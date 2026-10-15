//! Integration tests for the real local read path: sealed segments written through
//! io_uring are read back through [`LocalSegmentReader`] with aligned direct I/O.
//!
//! These tests need a working io_uring instance (Linux >= 6.1, sysctl and seccomp
//! permitting). When the ring cannot be created they print a skip note and succeed,
//! so the suite stays usable on restricted hosts.
//!
//! The local reader constructs its own ring on a dedicated I/O thread; the Tokio
//! runtime may move reader futures between workers.

use std::sync::Arc;

use bkxpro_codec::{
    AlignedSegmentEncoder, SegmentHeader, SegmentView, BLOCK_SIZE, HEADER_SIZE, RECORD_HEADER_SIZE,
};
use bkxpro_indexer::{EntryLocation, Index, IndexConfig, SegmentMeta, SegmentState};
use bkxpro_io::{RingConfig, UringSegmentIo};
use bkxpro_reader::{LocalSegmentReader, ReadCache, ReadCacheConfig, ReadSource, SegmentReader};
use bkxpro_segmenter::{AlignedBuffer, SegmentWriter, WriterConfig};
use bytes::Bytes;
use tempfile::TempDir;

const SEGMENT_ID: u64 = 1;
const LEDGER: u64 = 1;
const ENTRY_COUNT: u64 = 32;
const TARGET_DATA_BYTES: usize = 64 * 1024;

fn writer_config() -> WriterConfig {
    WriterConfig::default_for(TARGET_DATA_BYTES, ENTRY_COUNT as usize + 4)
}

fn ring() -> Option<UringSegmentIo> {
    match UringSegmentIo::new(RingConfig {
        registered_file_slots: 64,
        ..RingConfig::default()
    }) {
        Ok(io) => Some(io),
        Err(error) => {
            eprintln!("skipping io_uring integration test: failed to create ring: {error}");
            None
        }
    }
}

/// A sealed segment prepared for read-path tests: its temp dir, the index, and every
/// `(entry, payload)` pair in record order.
type SealedSegment = (TempDir, Arc<Index>, Vec<(u64, Bytes)>);

/// Write one sealed segment through io_uring, index its records, and return the
/// temp dir, the index, and every `(entry, payload)` pair in record order.
fn write_sealed_segment() -> Option<SealedSegment> {
    let mut io = ring()?;

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let index = Arc::new(
        Index::open(IndexConfig {
            db_path: temp.path().join("index"),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        })
        .unwrap(),
    );

    let config = writer_config();
    let mut buffer = AlignedBuffer::new_zeroed(config.buffer_capacity().unwrap()).unwrap();
    // Production leases the encoder buffer from the aligned buffer pool and registers
    // it with the active ring; this test allocates its own registered buffer.
    // SAFETY: `buffer` outlives every writer operation below and stays at a fixed
    // address while registered.
    unsafe { io.register_buffer(0, buffer.as_mut_ptr(), buffer.len()) }.unwrap();
    let mut writer = SegmentWriter::create(
        io,
        data_dir.join(format!("{SEGMENT_ID}.seg")),
        &data_dir,
        SegmentHeader {
            segment_id: SEGMENT_ID,
            writer_epoch: 1,
            target_size: (HEADER_SIZE + TARGET_DATA_BYTES) as u64,
            created_at_ns: 1_700_000_000,
        },
        config.clone(),
        &mut buffer,
    )
    .unwrap();

    let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
    for entry in 1..=ENTRY_COUNT {
        let payload = Bytes::from(format!("entry payload number {entry}"));
        writer
            .append(&mut encoder, LEDGER, entry, 0, &payload)
            .unwrap();
    }
    let sealed = writer.seal(encoder).unwrap();
    assert_eq!(sealed.segment_id, SEGMENT_ID);

    // Index the sealed segment: parse its footer/records exactly like recovery does.
    let segment_bytes = std::fs::read(&sealed.path).unwrap();
    let view = SegmentView::open_sealed(Bytes::from(segment_bytes)).unwrap();
    let mut payloads = Vec::new();
    let mut batch = index.batch();
    for record in view.records() {
        let record = record.unwrap();
        let mut payload = Vec::with_capacity(record.len as usize);
        record.copy_payload_to(&mut payload).unwrap();
        payloads.push((record.entry, Bytes::from(payload)));
        // `EntryLocation.offset` points at the payload, not at the record header.
        index
            .put_entry(
                &mut batch,
                record.ledger,
                record.entry,
                EntryLocation {
                    segment_id: SEGMENT_ID as u32,
                    offset: (record.offset + RECORD_HEADER_SIZE as u64) as u32,
                    len: record.len,
                },
            )
            .unwrap();
    }
    index
        .put_segment(
            &mut batch,
            SEGMENT_ID,
            SegmentMeta {
                state: SegmentState::Sealed,
                s3_key: None,
                min_ledger: LEDGER,
                max_ledger: LEDGER,
                byte_size: sealed.file_len as u64,
                writer_epoch: 1,
            },
        )
        .unwrap();
    batch.commit().unwrap();

    assert_eq!(payloads.len(), ENTRY_COUNT as usize);
    Some((temp, index, payloads))
}

fn segment_len(temp: &TempDir) -> u64 {
    std::fs::metadata(temp.path().join("data").join(format!("{SEGMENT_ID}.seg")))
        .unwrap()
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_segment_reader_reads_sealed_segment_without_resizing_it() {
    let Some((temp, index, payloads)) = write_sealed_segment() else {
        return;
    };
    let data_dir = temp.path().join("data");
    let written_len = segment_len(&temp);
    assert!(written_len.is_multiple_of(BLOCK_SIZE as u64));

    let reader = LocalSegmentReader::new(
        || UringSegmentIo::new(RingConfig::default()),
        data_dir,
        index.clone(),
        writer_config(),
    )
    .unwrap();

    for (entry, payload) in &payloads {
        let location = index
            .get_entry(LEDGER, *entry)
            .unwrap()
            .expect("entry is indexed");
        let read = reader
            .read_segment_range(
                location.segment_id as u64,
                location.offset as u64,
                location.len as usize,
            )
            .await
            .unwrap();
        assert_eq!(&read[..], &payload[..], "entry {entry} payload mismatch");
    }

    // Regression guard: the O_DIRECT read path must never ftruncate/fallocate the
    // sealed segment back to a preallocation capacity.
    assert_eq!(segment_len(&temp), written_len);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_cache_serves_real_segment_then_memory_hit() {
    let Some((temp, index, payloads)) = write_sealed_segment() else {
        return;
    };
    let data_dir = temp.path().join("data");
    let written_len = segment_len(&temp);

    let reader = Arc::new(
        LocalSegmentReader::new(
            || UringSegmentIo::new(RingConfig::default()),
            data_dir.clone(),
            index.clone(),
            writer_config(),
        )
        .unwrap(),
    );

    let config = ReadCacheConfig {
        data_dir,
        cache_dir: temp.path().join("cache"),
        memory_tier_bytes: 8 * 1024 * 1024,
        nvme_tier_bytes: 64 * 1024 * 1024,
        block_size: 16 * 1024 * 1024,
        cache_shards: 1,
        max_concurrent_reads: 8,
        enable_compression: false,
        ..Default::default()
    };
    let cache = ReadCache::new(config, index.clone(), reader).await.unwrap();

    let (first_entry, first_payload) = &payloads[0];
    let cold = cache.read_entry(LEDGER, *first_entry).await.unwrap();
    assert_eq!(&cold.payload[..], &first_payload[..]);
    assert_eq!(cold.source, ReadSource::LocalSegment);

    let warm = cache.read_entry(LEDGER, *first_entry).await.unwrap();
    assert_eq!(&warm.payload[..], &first_payload[..]);
    assert_eq!(warm.source, ReadSource::MemoryCache);

    let stats = cache.stats();
    assert_eq!(stats.segment_hits, 1);
    assert_eq!(stats.memory_hits, 1);
    assert_eq!(stats.cache_fills, 1);

    // Reading through the cache (entry read + block fill) must not resize the segment.
    assert_eq!(segment_len(&temp), written_len);

    cache.close().await.unwrap();
}

/// Exercise the reader independently of the writer's group-commit fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_reader_constructs_and_uses_its_ring_on_the_io_thread() {
    let Some(probe) = ring() else {
        return;
    };
    drop(probe);
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir).unwrap();
    let mut encoder = bkxpro_codec::SegmentEncoder::new(SegmentHeader {
        segment_id: SEGMENT_ID,
        writer_epoch: 1,
        target_size: 64 * 1024,
        created_at_ns: 0,
    });
    encoder
        .push_record(1, 1, 0, &Bytes::from(vec![0x5a; 4000]))
        .unwrap();
    let image = encoder.seal().unwrap();
    std::fs::write(data_dir.join("1.seg"), &image).unwrap();
    let index = Arc::new(
        Index::open(IndexConfig {
            db_path: temp.path().join("index"),
            ..Default::default()
        })
        .unwrap(),
    );
    let reader = Arc::new(
        LocalSegmentReader::new(
            || UringSegmentIo::new(RingConfig::default()),
            data_dir,
            index,
            writer_config(),
        )
        .unwrap(),
    );
    let offset = BLOCK_SIZE + 3;
    let length = BLOCK_SIZE + 91;
    let read = tokio::spawn(async move {
        reader
            .read_segment_range(SEGMENT_ID, offset as u64, length)
            .await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(read, image.slice(offset..offset + length));
}
