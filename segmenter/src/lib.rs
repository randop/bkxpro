//! Segment writer core.
//!
//! This crate deliberately owns the segment-writer algorithm, not the Linux I/O
//! runtime. The production adapter implements [`SegmentIo`] using its registered
//! buffers/files and io_uring ring. In particular, `commit_batch` must submit all
//! aligned writes and an explicitly ordered `fdatasync`, returning only after every
//! completion succeeds. This interface has no `pwrite`/buffered-I/O fallback.
//!
//! Buffers passed to [`SegmentWriter::create`] and `AlignedSegmentEncoder` must be
//! leases from the registered, 4 KiB-aligned buffer pool in production. The
//! small [`AlignedBuffer`] helper is intended for tests and standalone tooling; it
//! does not register memory with io_uring.

#[cfg(not(target_os = "linux"))]
compile_error!("bkxpro-segmenter supports Linux only; develop and test in a Linux VM/devcontainer");

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use bkxpro_codec::{
    AlignedSegmentEncoder, CodecError, RecordSpan, SegmentHeader, BLOCK_SIZE, HEADER_SIZE,
    RECORD_HEADER_SIZE, TRAILER_SIZE,
};
use bytes::Bytes;

const FOOTER_HEADER_SIZE: usize = 28;
const FOOTER_INDEX_ENTRY_SIZE: usize = 32;
const LEDGER_SUMMARY_SIZE: usize = 24;
const FOOTER_CRC_SIZE: usize = 4;
const ZERO_BATCH_DEFAULT_BLOCKS: usize = 256;

/// Required bytes for one segment buffer, including the segment header, the target
/// data region, the worst-case footer (each record may have its own ledger summary),
/// and the trailer/padding. `target_data_bytes` is rounded up to a 4 KiB boundary.
pub fn required_segment_buffer_capacity(
    target_data_bytes: usize,
    max_entries: usize,
) -> Result<usize, WriterError<std::io::Error>> {
    if target_data_bytes == 0 || max_entries == 0 {
        return Err(WriterError::InvalidConfig(
            "target_data_bytes and max_entries must be non-zero",
        ));
    }
    let data_bytes = checked_align_up(target_data_bytes, BLOCK_SIZE)?;
    let footer = FOOTER_HEADER_SIZE
        .checked_add(
            max_entries
                .checked_mul(FOOTER_INDEX_ENTRY_SIZE)
                .ok_or(WriterError::InvalidConfig("footer capacity overflow"))?,
        )
        .and_then(|n| n.checked_add(max_entries.checked_mul(LEDGER_SUMMARY_SIZE)?))
        .and_then(|n| n.checked_add(FOOTER_CRC_SIZE))
        .ok_or(WriterError::InvalidConfig("footer capacity overflow"))?;
    let tail = checked_align_up(
        footer
            .checked_add(TRAILER_SIZE)
            .ok_or(WriterError::InvalidConfig("footer capacity overflow"))?,
        BLOCK_SIZE,
    )?;
    HEADER_SIZE
        .checked_add(data_bytes)
        .and_then(|n| n.checked_add(tail))
        .ok_or(WriterError::InvalidConfig("segment capacity overflow"))
}

fn checked_align_up(value: usize, alignment: usize) -> Result<usize, WriterError<std::io::Error>> {
    value
        .checked_add(alignment - 1)
        .map(|n| n / alignment * alignment)
        .ok_or(WriterError::InvalidConfig("aligned size overflow"))
}

/// Writer tuning and hard limits. The buffer's full length must equal
/// [`required_segment_buffer_capacity`] for these settings; the same size is used
/// for preallocation, pre-zeroing, and the encoder output buffer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Physical data-region target, excluding the 4 KiB header and footer.
    pub target_data_bytes: usize,
    /// Maximum entries indexed in one segment; bounds footer and per-entry metadata.
    pub max_entries: usize,
    /// Completed block bytes required before an opportunistic group commit.
    pub group_commit_bytes: usize,
    /// Maximum bytes whose appends are awaiting durable acknowledgement.
    pub max_inflight_bytes: usize,
    /// Group-commit timer interval for the owning shard.
    pub flush_interval: Duration,
    /// Segment is eligible for sealing once this duration has elapsed.
    pub seal_interval: Duration,
    /// Number of 4 KiB zero blocks written per pre-zero I/O batch.
    pub prezero_batch_blocks: usize,
}

impl WriterConfig {
    pub fn default_for(target_data_bytes: usize, max_entries: usize) -> Self {
        Self {
            target_data_bytes,
            max_entries,
            group_commit_bytes: 256 * 1024,
            max_inflight_bytes: 16 * 1024 * 1024,
            flush_interval: Duration::from_millis(2),
            seal_interval: Duration::from_secs(5),
            prezero_batch_blocks: ZERO_BATCH_DEFAULT_BLOCKS,
        }
    }

    pub fn buffer_capacity(&self) -> Result<usize, WriterError<std::io::Error>> {
        required_segment_buffer_capacity(self.target_data_bytes, self.max_entries)
    }

    fn validate(&self) -> Result<(), WriterError<std::io::Error>> {
        if self.target_data_bytes == 0
            || self.max_entries == 0
            || self.group_commit_bytes == 0
            || self.max_inflight_bytes == 0
            || self.prezero_batch_blocks == 0
        {
            return Err(WriterError::InvalidConfig(
                "writer limits and batch sizes must be non-zero",
            ));
        }
        if self.max_entries > u32::MAX as usize {
            return Err(WriterError::InvalidConfig(
                "max_entries exceeds the codec's u32 footer count",
            ));
        }
        Ok(())
    }
}

/// One aligned write into a segment, identified by an absolute file offset.
#[derive(Debug, Clone, Copy)]
pub struct WriteOp<'a> {
    offset: u64,
    bytes: &'a [u8],
}

impl<'a> WriteOp<'a> {
    pub fn new(offset: u64, bytes: &'a [u8]) -> Result<Self, WriteOpError> {
        if offset % BLOCK_SIZE as u64 != 0 {
            return Err(WriteOpError::UnalignedOffset(offset));
        }
        if bytes.is_empty()
            || bytes.len() % BLOCK_SIZE != 0
            || (bytes.as_ptr() as usize) % BLOCK_SIZE != 0
        {
            return Err(WriteOpError::UnalignedBuffer {
                address: bytes.as_ptr() as usize,
                len: bytes.len(),
            });
        }
        Ok(Self { offset, bytes })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOpError {
    UnalignedOffset(u64),
    UnalignedBuffer { address: usize, len: usize },
}

impl fmt::Display for WriteOpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnalignedOffset(offset) => write!(f, "unaligned direct-I/O offset {offset}"),
            Self::UnalignedBuffer { address, len } => write!(
                f,
                "direct-I/O buffer must have a 4 KiB-aligned address and length (address={address:#x}, len={len})"
            ),
        }
    }
}
impl Error for WriteOpError {}

/// Result of acquiring a new or recycled segment file. Implementations should
/// prefer recycling a preallocated file of matching capacity over unlink/recreate.
pub struct AcquiredSegment<F> {
    pub file: F,
    /// True if create/rename changed a directory entry and therefore requires dir fsync.
    pub directory_entry_changed: bool,
}

/// Narrow io_uring integration seam. Implementations must use registered buffers/files
/// and io_uring for the production path. `write_batch` must wait for exact successful
/// completion of every write; `commit_batch` must order writes before `fdatasync` and
/// wait for both the writes and sync. No operation may silently retry a failed write.
pub trait SegmentIo {
    type File;
    type Error: Error + Send + Sync + 'static;

    /// Acquire a file at `path` with `capacity` already preallocated. A recycled file
    /// must be resized to this capacity before returning.
    fn acquire_preallocated_segment(
        &mut self,
        path: &Path,
        capacity: usize,
    ) -> Result<AcquiredSegment<Self::File>, Self::Error>;

    /// Open an existing segment for reading. The implementation must not create,
    /// truncate, extend, or allocate the file: the read path observes sealed bytes
    /// exactly as the writer left them. The returned handle is only used with
    /// [`SegmentIo::read_aligned`] and [`SegmentIo::file_size`].
    fn open_segment_for_read(&mut self, path: &Path) -> Result<Self::File, Self::Error>;

    /// Submit non-durable writes and wait until each reports the full requested length.
    /// Used for bulk zeroing; the caller issues one `sync_data` after all batches.
    fn write_batch(&mut self, file: &Self::File, writes: &[WriteOp<'_>])
        -> Result<(), Self::Error>;

    /// Wait for `fdatasync` completion.
    fn sync_data(&mut self, file: &Self::File) -> Result<(), Self::Error>;

    /// Durably commit a set of writes. Implementations should submit linked/drained
    /// writes plus `fdatasync` in one ring batch when supported, and must not return
    /// success until all writes and the sync complete. Short writes and errors are fatal.
    fn commit_batch(
        &mut self,
        file: &Self::File,
        writes: &[WriteOp<'_>],
    ) -> Result<(), Self::Error>;

    /// Read exactly one or more aligned 4 KiB blocks into a registered buffer.
    /// A short read is an error; callers must not expose a partial storage block.
    fn read_aligned(
        &mut self,
        file: &Self::File,
        offset: u64,
        buffer: &mut [u8],
    ) -> Result<(), Self::Error>;

    /// Truncate a sealed segment to its canonical codec length. The caller follows
    /// with `sync_data` before publishing the sealed state.
    fn truncate(&mut self, file: &Self::File, len: u64) -> Result<(), Self::Error>;

    /// Unlink a path and make that directory-entry change durable before returning.
    fn unlink(&mut self, path: &Path, directory: &Path) -> Result<(), Self::Error>;

    /// fsync the directory after create, rename, or unlink changes.
    fn sync_directory(&mut self, directory: &Path) -> Result<(), Self::Error>;

    /// Get the current file size (valid length) of a segment.
    /// For preallocated files, this returns the logical size, not the preallocated capacity.
    fn file_size(&self, file: &Self::File) -> Result<u64, Self::Error>;
}

#[derive(Debug)]
pub enum WriterError<E> {
    Io(E),
    Codec(CodecError),
    InvalidConfig(&'static str),
    InvalidBuffer(&'static str),
    InvalidState(&'static str),
    SegmentFull,
    EntryTooLarge,
    DuplicateEntry { ledger: u64, entry: u64 },
    InflightLimit { requested: usize, limit: usize },
    Poisoned,
}

impl<E: fmt::Display> fmt::Display for WriterError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "segment I/O failed: {error}"),
            Self::Codec(error) => write!(f, "segment codec failed: {error}"),
            Self::InvalidConfig(message) => write!(f, "invalid writer configuration: {message}"),
            Self::InvalidBuffer(message) => write!(f, "invalid segment buffer: {message}"),
            Self::InvalidState(message) => write!(f, "invalid writer state: {message}"),
            Self::SegmentFull => {
                write!(f, "segment target or entry budget reached; seal and rotate")
            }
            Self::EntryTooLarge => write!(f, "record cannot fit in an empty segment"),
            Self::DuplicateEntry { ledger, entry } => {
                write!(f, "duplicate entry ({ledger}, {entry})")
            }
            Self::InflightLimit { requested, limit } => write!(
                f,
                "record size {requested} exceeds the in-flight byte limit {limit}"
            ),
            Self::Poisoned => write!(f, "writer is fail-stop poisoned; restart and recover"),
        }
    }
}
impl<E: Error + 'static> Error for WriterError<E> {}
impl<E> From<CodecError> for WriterError<E> {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    /// Sequence number local to this segment.
    pub sequence: u64,
    /// Earlier/current sequences whose blocks became durable during this call.
    pub acknowledged: Vec<u64>,
    /// Caller should seal and rotate before accepting another entry.
    pub should_seal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealResult {
    pub segment_id: u64,
    pub path: PathBuf,
    pub file_len: usize,
    /// Entries not previously acknowledged that became durable while sealing.
    pub acknowledged: Vec<u64>,
}

#[derive(Debug)]
struct PendingAck {
    sequence: u64,
    last_block_end: usize,
    inflight_bytes: usize,
}

/// One-shard segment writer. It is intentionally driven by the owning shard's loop:
/// no worker-thread hop is added to the acknowledgement path. Append calls group
/// completed blocks by byte threshold; the reactor should call `should_seal_at` from
/// its timer and `seal` when the size target or timer expires.
pub struct SegmentWriter<I: SegmentIo> {
    io: I,
    file: I::File,
    path: PathBuf,
    directory: PathBuf,
    header: SegmentHeader,
    config: WriterConfig,
    file_capacity: usize,
    write_cursor: usize,
    header_durable: bool,
    pending: Vec<PendingAck>,
    seen_entries: HashSet<(u64, u64)>,
    inflight_bytes: usize,
    next_sequence: u64,
    opened_at: Instant,
    last_commit_at: Instant,
    poisoned: bool,
    sealed: bool,
}

impl<I: SegmentIo> SegmentWriter<I> {
    /// Create or reuse a preallocated segment, durably zero its complete capacity,
    /// and sync the directory if the path was created/renamed. `buffer` must be the
    /// exact-size registered aligned buffer that will also back `AlignedSegmentEncoder`.
    pub fn create(
        mut io: I,
        path: impl Into<PathBuf>,
        directory: impl Into<PathBuf>,
        header: SegmentHeader,
        mut config: WriterConfig,
        buffer: &mut [u8],
    ) -> Result<Self, WriterError<I::Error>> {
        config.validate().map_err(|e| match e {
            WriterError::InvalidConfig(message) => WriterError::InvalidConfig(message),
            _ => WriterError::InvalidConfig("invalid configuration"),
        })?;
        config.target_data_bytes =
            checked_align_up(config.target_data_bytes, BLOCK_SIZE).map_err(|e| match e {
                WriterError::InvalidConfig(message) => WriterError::InvalidConfig(message),
                _ => WriterError::InvalidConfig("target size overflow"),
            })?;
        let required = config.buffer_capacity().map_err(|e| match e {
            WriterError::InvalidConfig(message) => WriterError::InvalidConfig(message),
            _ => WriterError::InvalidConfig("segment capacity overflow"),
        })?;
        if buffer.len() != required || buffer.len() % BLOCK_SIZE != 0 {
            return Err(WriterError::InvalidBuffer(
                "length must equal required_segment_buffer_capacity and be 4 KiB aligned",
            ));
        }
        if (buffer.as_ptr() as usize) % BLOCK_SIZE != 0 {
            return Err(WriterError::InvalidBuffer(
                "base address is not 4 KiB aligned",
            ));
        }
        let path = path.into();
        let directory = directory.into();
        let acquired = io
            .acquire_preallocated_segment(&path, buffer.len())
            .map_err(WriterError::Io)?;
        if acquired.directory_entry_changed {
            io.sync_directory(&directory).map_err(WriterError::Io)?;
        }

        // Reused files are fully overwritten from offset zero. A single aligned zero
        // block is referenced by all writes in a batch; write_batch is a completion barrier.
        buffer[..BLOCK_SIZE].fill(0);
        let zero_block = &buffer[..BLOCK_SIZE];
        let mut offset = 0usize;
        while offset < buffer.len() {
            let count = config
                .prezero_batch_blocks
                .min((buffer.len() - offset) / BLOCK_SIZE);
            let mut writes = Vec::with_capacity(count);
            for i in 0..count {
                let block_offset = offset + i * BLOCK_SIZE;
                writes.push(
                    WriteOp::new(block_offset as u64, zero_block)
                        .map_err(|_| WriterError::InvalidBuffer("zero block alignment failed"))?,
                );
            }
            io.write_batch(&acquired.file, &writes)
                .map_err(WriterError::Io)?;
            offset += count * BLOCK_SIZE;
        }
        io.sync_data(&acquired.file).map_err(WriterError::Io)?;

        let now = Instant::now();
        let capacity = buffer.len();
        Ok(Self {
            io,
            file: acquired.file,
            path,
            directory,
            header,
            config: config.clone(),
            file_capacity: capacity,
            write_cursor: HEADER_SIZE,
            header_durable: false,
            pending: Vec::with_capacity(config.max_entries.min(4096)),
            seen_entries: HashSet::with_capacity(config.max_entries.min(4096)),
            inflight_bytes: 0,
            next_sequence: 1,
            opened_at: now,
            last_commit_at: now,
            poisoned: false,
            sealed: false,
        })
    }

    pub fn header(&self) -> SegmentHeader {
        self.header
    }

    pub fn file(&self) -> &I::File {
        &self.file
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes
    }

    pub fn should_seal_at(&self, now: Instant) -> bool {
        !self.sealed && (now.saturating_duration_since(self.opened_at) >= self.config.seal_interval)
    }

    /// The shard timer should call `flush_group` when this returns true.
    pub fn should_flush_at(&self, now: Instant) -> bool {
        !self.sealed
            && !self.pending.is_empty()
            && now.saturating_duration_since(self.last_commit_at) >= self.config.flush_interval
    }

    /// Append one entry into the caller-owned encoder buffer. No acknowledgement is
    /// returned until the entry's final block is part of a successful durable commit.
    pub fn append(
        &mut self,
        encoder: &mut AlignedSegmentEncoder<'_>,
        ledger: u64,
        entry: u64,
        flags: u8,
        payload: &Bytes,
    ) -> Result<AppendResult, WriterError<I::Error>> {
        self.ensure_open()?;
        let record_bytes = RECORD_HEADER_SIZE
            .checked_add(payload.len())
            .ok_or(WriterError::InvalidConfig("record size overflow"))?;
        if record_bytes > self.config.max_inflight_bytes {
            return Err(WriterError::InflightLimit {
                requested: record_bytes,
                limit: self.config.max_inflight_bytes,
            });
        }
        if self.seen_entries.contains(&(ledger, entry)) {
            return Err(WriterError::DuplicateEntry { ledger, entry });
        }
        if self.pending.len() >= self.config.max_entries
            || self.seen_entries.len() >= self.config.max_entries
        {
            return Err(WriterError::SegmentFull);
        }
        let projected = encoder.estimated_data_region_len_after_record(payload.len())?;
        if projected > self.config.target_data_bytes {
            if encoder.estimated_data_region_len() == 0 {
                return Err(WriterError::EntryTooLarge);
            }
            return Err(WriterError::SegmentFull);
        }

        let mut acknowledged = Vec::new();
        if self.inflight_bytes.saturating_add(record_bytes) > self.config.max_inflight_bytes {
            // Backpressure rather than allowing append payloads/acks to accumulate without bound.
            encoder.flush_pending_data_block()?;
            // Padding the partial block can make the next record exceed the target.
            // Reject before committing, leaving pending acks for flush_group/seal.
            if encoder.estimated_data_region_len_after_record(payload.len())?
                > self.config.target_data_bytes
            {
                return Err(WriterError::SegmentFull);
            }
            acknowledged.extend(self.commit_ready(encoder)?);
            if self.inflight_bytes.saturating_add(record_bytes) > self.config.max_inflight_bytes {
                return Err(WriterError::InflightLimit {
                    requested: record_bytes,
                    limit: self.config.max_inflight_bytes,
                });
            }
        }

        let sequence = self.next_sequence;
        let next = self
            .next_sequence
            .checked_add(1)
            .ok_or(WriterError::InvalidConfig("sequence counter overflow"))?;
        let span: RecordSpan = match encoder.push_record_with_span(ledger, entry, flags, payload) {
            Ok(span) => span,
            Err(error) => return Err(WriterError::Codec(error)),
        };
        self.next_sequence = next;
        self.seen_entries.insert((ledger, entry));
        self.inflight_bytes += record_bytes;
        self.pending.push(PendingAck {
            sequence,
            last_block_end: span
                .last_block_offset
                .checked_add(BLOCK_SIZE)
                .ok_or(WriterError::InvalidConfig("record block extent overflow"))?,
            inflight_bytes: record_bytes,
        });

        let ready_bytes = encoder
            .committed_prefix_len()
            .saturating_sub(self.write_cursor);
        if ready_bytes >= self.config.group_commit_bytes {
            acknowledged.extend(self.commit_ready(encoder)?);
        }
        acknowledged.sort_unstable();
        let should_seal = encoder.estimated_data_region_len() >= self.config.target_data_bytes
            || self.seen_entries.len() >= self.config.max_entries;
        Ok(AppendResult {
            sequence,
            acknowledged,
            should_seal,
        })
    }

    /// Force the current partial block out, then commit all completed blocks. Typically
    /// called by the shard timer before sealing; the return values are safe to ack.
    pub fn flush_group(
        &mut self,
        encoder: &mut AlignedSegmentEncoder<'_>,
    ) -> Result<Vec<u64>, WriterError<I::Error>> {
        self.ensure_open()?;
        encoder.flush_pending_data_block()?;
        self.commit_ready(encoder)
    }

    /// Seal the segment. The writer first durably commits all complete data blocks, then
    /// writes the footer/trailer, truncates the preallocated file to the codec's canonical
    /// length, and syncs that size change. On any write/sync error it becomes fail-stop
    /// poisoned and does not return acknowledgements from this call.
    pub fn seal(
        &mut self,
        encoder: AlignedSegmentEncoder<'_>,
    ) -> Result<SealResult, WriterError<I::Error>> {
        self.ensure_open()?;
        let mut acknowledged = Vec::new();
        // The encoder's final block must be submitted before its footer is computed.
        let mut encoder = encoder;
        encoder.flush_pending_data_block()?;
        acknowledged.extend(self.commit_ready(&encoder)?);
        let footer_start = encoder.committed_prefix_len();
        let (buffer, file_len) = match encoder.seal_and_return_buffer() {
            Ok(sealed) => sealed,
            Err(error) => {
                self.poisoned = true;
                return Err(WriterError::Codec(error));
            }
        };
        if file_len > self.file_capacity || footer_start % BLOCK_SIZE != 0 {
            self.poisoned = true;
            return Err(WriterError::InvalidBuffer(
                "sealed length exceeds preallocated capacity",
            ));
        }

        let mut writes = Vec::with_capacity(2);
        if !self.header_durable {
            writes.push(
                WriteOp::new(0, &buffer[..HEADER_SIZE])
                    .map_err(|_| WriterError::InvalidBuffer("header alignment failed"))?,
            );
        }
        writes.push(
            WriteOp::new(footer_start as u64, &buffer[footer_start..file_len])
                .map_err(|_| WriterError::InvalidBuffer("footer alignment failed"))?,
        );
        if let Err(error) = self.io.commit_batch(&self.file, &writes) {
            self.poisoned = true;
            return Err(WriterError::Io(error));
        }
        self.header_durable = true;
        if let Err(error) = self.io.truncate(&self.file, file_len as u64) {
            self.poisoned = true;
            return Err(WriterError::Io(error));
        }
        if let Err(error) = self.io.sync_data(&self.file) {
            self.poisoned = true;
            return Err(WriterError::Io(error));
        }
        self.sealed = true;
        // Everything in the segment has now had its final block durably committed.
        for pending in self.pending.drain(..) {
            acknowledged.push(pending.sequence);
            self.inflight_bytes = self.inflight_bytes.saturating_sub(pending.inflight_bytes);
        }
        Ok(SealResult {
            segment_id: self.header.segment_id,
            path: self.path.clone(),
            file_len,
            acknowledged,
        })
    }

    fn commit_ready(
        &mut self,
        encoder: &AlignedSegmentEncoder<'_>,
    ) -> Result<Vec<u64>, WriterError<I::Error>> {
        self.ensure_open()?;
        let ready_end = encoder.committed_prefix_len();
        if ready_end <= self.write_cursor {
            return Ok(Vec::new());
        }
        let block_bytes = ready_end - self.write_cursor;
        if block_bytes % BLOCK_SIZE != 0 {
            self.poisoned = true;
            return Err(WriterError::InvalidState(
                "encoder emitted a partial data block",
            ));
        }
        let block_count = block_bytes / BLOCK_SIZE;
        let header_write_count = if self.header_durable { 0 } else { 1 };
        let mut writes = Vec::with_capacity(block_count + header_write_count);
        if !self.header_durable {
            writes.push(
                WriteOp::new(0, encoder.header_bytes())
                    .map_err(|_| WriterError::InvalidBuffer("header alignment failed"))?,
            );
        }
        let mut offset = self.write_cursor;
        while offset < ready_end {
            writes.push(
                WriteOp::new(offset as u64, encoder.data_block_bytes(offset)?)
                    .map_err(|_| WriterError::InvalidBuffer("data block alignment failed"))?,
            );
            offset += BLOCK_SIZE;
        }
        if let Err(error) = self.io.commit_batch(&self.file, &writes) {
            self.poisoned = true;
            return Err(WriterError::Io(error));
        }
        self.header_durable = true;
        self.write_cursor = ready_end;
        self.last_commit_at = Instant::now();

        let mut acknowledged = Vec::new();
        let mut index = 0;
        while index < self.pending.len() {
            if self.pending[index].last_block_end <= ready_end {
                let pending = self.pending.swap_remove(index);
                self.inflight_bytes = self.inflight_bytes.saturating_sub(pending.inflight_bytes);
                acknowledged.push(pending.sequence);
            } else {
                index += 1;
            }
        }
        acknowledged.sort_unstable();
        Ok(acknowledged)
    }

    fn ensure_open(&self) -> Result<(), WriterError<I::Error>> {
        if self.poisoned {
            return Err(WriterError::Poisoned);
        }
        if self.sealed {
            return Err(WriterError::InvalidState("segment is already sealed"));
        }
        Ok(())
    }

    /// Borrow the I/O backend after sealing, e.g. to hand the sealed file to the uploader.
    pub fn io_mut(&mut self) -> &mut I {
        &mut self.io
    }

    /// The directory is retained so owner code can share it with the uploader/recycler.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Consume a sealed writer when moving to the next segment. The returned backend can
    /// be reused while the file handle is retained for upload/recycling. An open or poisoned
    /// writer is returned intact so pending acknowledgements cannot be silently discarded.
    pub fn try_into_parts(self) -> Result<(I, I::File, PathBuf), Self> {
        if !self.sealed {
            return Err(self);
        }
        Ok((self.io, self.file, self.path))
    }
}

/// A 4 KiB-aligned owning allocation for tests and local codec tooling. Production code
/// should instead lease a fixed buffer and register it with the active ring.
pub struct AlignedBuffer {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
    len: usize,
}

impl AlignedBuffer {
    pub fn new_zeroed(len: usize) -> Result<Self, std::io::Error> {
        if len == 0 || len % BLOCK_SIZE != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "aligned buffer length must be a non-zero multiple of 4 KiB",
            ));
        }
        let layout = std::alloc::Layout::from_size_align(len, BLOCK_SIZE)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid layout"))?;
        // SAFETY: `layout` is non-zero and valid; allocation is released in Drop.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(raw).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, "allocation failed")
        })?;
        Ok(Self { ptr, layout, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        // SAFETY: the allocation is valid for `len` bytes for the lifetime of self.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}
impl std::ops::DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the allocation is uniquely borrowed through `&mut self`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}
impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with exactly this layout in `new_zeroed`.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    struct MemoryFile(Arc<Mutex<Vec<u8>>>);

    #[derive(Default)]
    struct MemoryIo {
        files: HashMap<PathBuf, MemoryFile>,
        commit_count: usize,
        directory_sync_count: usize,
        fail_next_commit: bool,
    }

    impl SegmentIo for MemoryIo {
        type File = MemoryFile;
        type Error = io::Error;

        fn acquire_preallocated_segment(
            &mut self,
            path: &Path,
            capacity: usize,
        ) -> Result<AcquiredSegment<Self::File>, Self::Error> {
            let (file, changed) = match self.files.get(path) {
                Some(file) => (file.clone(), false),
                None => {
                    let file = MemoryFile(Arc::new(Mutex::new(vec![0; capacity])));
                    self.files.insert(path.to_path_buf(), file.clone());
                    (file, true)
                }
            };
            file.0.lock().unwrap().resize(capacity, 0);
            Ok(AcquiredSegment {
                file,
                directory_entry_changed: changed,
            })
        }

        fn open_segment_for_read(&mut self, path: &Path) -> Result<Self::File, Self::Error> {
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "test segment not found"))
        }

        fn write_batch(
            &mut self,
            file: &Self::File,
            writes: &[WriteOp<'_>],
        ) -> Result<(), Self::Error> {
            let mut bytes = file.0.lock().unwrap();
            for write in writes {
                let start = write.offset() as usize;
                let end = start
                    .checked_add(write.bytes().len())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "overflow"))?;
                if end > bytes.len() {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "short test write"));
                }
                bytes[start..end].copy_from_slice(write.bytes());
            }
            Ok(())
        }

        fn sync_data(&mut self, _file: &Self::File) -> Result<(), Self::Error> {
            Ok(())
        }

        fn commit_batch(
            &mut self,
            file: &Self::File,
            writes: &[WriteOp<'_>],
        ) -> Result<(), Self::Error> {
            if self.fail_next_commit {
                self.fail_next_commit = false;
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "injected commit failure",
                ));
            }
            self.commit_count += 1;
            self.write_batch(file, writes)?;
            self.sync_data(file)
        }

        fn read_aligned(
            &mut self,
            file: &Self::File,
            offset: u64,
            buffer: &mut [u8],
        ) -> Result<(), Self::Error> {
            let bytes = file.0.lock().unwrap();
            let start = offset as usize;
            let end = start
                .checked_add(buffer.len())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read overflow"))?;
            if offset % BLOCK_SIZE as u64 != 0
                || buffer.len() % BLOCK_SIZE != 0
                || (buffer.as_ptr() as usize) % BLOCK_SIZE != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unaligned test read",
                ));
            }
            if end > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "short test read",
                ));
            }
            buffer.copy_from_slice(&bytes[start..end]);
            Ok(())
        }

        fn truncate(&mut self, file: &Self::File, len: u64) -> Result<(), Self::Error> {
            file.0.lock().unwrap().truncate(len as usize);
            Ok(())
        }

        fn unlink(&mut self, path: &Path, directory: &Path) -> Result<(), Self::Error> {
            let _ = directory;
            self.files.remove(path);
            Ok(())
        }

        fn sync_directory(&mut self, _directory: &Path) -> Result<(), Self::Error> {
            self.directory_sync_count += 1;
            Ok(())
        }

        fn file_size(&self, file: &Self::File) -> Result<u64, Self::Error> {
            let bytes = file.0.lock().unwrap();
            Ok(bytes.len() as u64)
        }
    }

    fn header() -> SegmentHeader {
        SegmentHeader {
            segment_id: 77,
            writer_epoch: 2,
            target_size: (HEADER_SIZE + config().target_data_bytes) as u64,
            created_at_ns: 1_700_000_000,
        }
    }

    fn config() -> WriterConfig {
        WriterConfig {
            target_data_bytes: 32 * BLOCK_SIZE,
            max_entries: 64,
            group_commit_bytes: BLOCK_SIZE,
            max_inflight_bytes: 1024 * 1024,
            flush_interval: Duration::from_millis(2),
            seal_interval: Duration::from_secs(60),
            prezero_batch_blocks: 8,
        }
    }

    fn new_writer() -> (SegmentWriter<MemoryIo>, AlignedBuffer) {
        let cfg = config();
        let mut buffer = AlignedBuffer::new_zeroed(cfg.buffer_capacity().unwrap()).unwrap();
        let writer = SegmentWriter::create(
            MemoryIo::default(),
            "/data/77.seg",
            "/data",
            header(),
            cfg,
            &mut buffer,
        )
        .unwrap();
        (writer, buffer)
    }

    #[test]
    fn recycled_capacity_is_fully_zeroed_before_reuse() {
        let cfg = config();
        let capacity = cfg.buffer_capacity().unwrap();
        let path = PathBuf::from("/data/recycled.seg");
        let file = MemoryFile(Arc::new(Mutex::new(vec![0xA5; capacity])));
        let mut io = MemoryIo::default();
        io.files.insert(path.clone(), file.clone());
        let mut buffer = AlignedBuffer::new_zeroed(capacity).unwrap();
        let mut writer =
            SegmentWriter::create(io, path, "/data", header(), cfg, &mut buffer).unwrap();
        assert!(file.0.lock().unwrap().iter().all(|byte| *byte == 0));
        assert_eq!(writer.io_mut().directory_sync_count, 0);
    }

    #[test]
    fn empty_segment_seals_with_a_canonical_header_and_footer() {
        let (mut writer, mut buffer) = new_writer();
        let encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        let sealed = writer.seal(encoder).unwrap();
        assert_eq!(sealed.acknowledged, Vec::<u64>::new());
        let stored = writer
            .io_mut()
            .files
            .get(Path::new("/data/77.seg"))
            .unwrap()
            .0
            .lock()
            .unwrap()
            .clone();
        assert_eq!(stored.len(), sealed.file_len);
        let view = bkxpro_codec::SegmentView::open_sealed(Bytes::from(stored)).unwrap();
        assert_eq!(view.records().count(), 0);
    }

    #[test]
    fn entries_are_acknowledged_only_after_a_durable_group() {
        let (mut writer, mut buffer) = new_writer();
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        let first = writer
            .append(&mut encoder, 1, 1, 0, &Bytes::from_static(b"one"))
            .unwrap();
        assert!(first.acknowledged.is_empty());
        let sealed = writer.seal(encoder).unwrap();
        assert_eq!(sealed.acknowledged, vec![1]);
        assert_eq!(sealed.segment_id, 77);
        let stored = writer
            .io_mut()
            .files
            .get(Path::new("/data/77.seg"))
            .unwrap()
            .0
            .lock()
            .unwrap()
            .clone();
        assert_eq!(stored.len(), sealed.file_len);
        let view = bkxpro_codec::SegmentView::open_sealed(Bytes::from(stored)).unwrap();
        assert_eq!(view.records().count(), 1);
    }

    #[test]
    fn completed_block_batch_can_ack_multiple_records() {
        let (mut writer, mut buffer) = new_writer();
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        let a = writer
            .append(&mut encoder, 1, 10, 0, &Bytes::from(vec![1; 3000]))
            .unwrap();
        assert!(a.acknowledged.is_empty());
        let b = writer
            .append(&mut encoder, 1, 11, 0, &Bytes::from(vec![2; 1000]))
            .unwrap();
        assert!(b.acknowledged.is_empty());
        let c = writer
            .append(&mut encoder, 1, 12, 0, &Bytes::from(vec![3; 1000]))
            .unwrap();
        assert!(c.acknowledged.contains(&1));
        assert!(c.acknowledged.contains(&2));
        assert!(!c.acknowledged.contains(&3));
        // Seal after the first two records were durably committed as one batch.
        let sealed = writer.seal(encoder).unwrap();
        assert_eq!(sealed.acknowledged, vec![3]);
    }

    #[test]
    fn commit_error_poisoning_never_returns_an_ack() {
        let (mut writer, mut buffer) = new_writer();
        writer.io.fail_next_commit = true;
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        writer
            .append(&mut encoder, 1, 1, 0, &Bytes::from(vec![3; 4000]))
            .unwrap();
        let error = writer.seal(encoder).unwrap_err();
        assert!(matches!(error, WriterError::Io(_)));
        assert!(writer.is_poisoned());
    }

    #[test]
    fn writer_rejects_duplicate_entries_before_encoding() {
        let (mut writer, mut buffer) = new_writer();
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        writer
            .append(&mut encoder, 5, 9, 0, &Bytes::from_static(b"a"))
            .unwrap();
        let result = writer.append(&mut encoder, 5, 9, 0, &Bytes::from_static(b"b"));
        assert!(matches!(result, Err(WriterError::DuplicateEntry { .. })));
    }

    #[test]
    fn flush_and_seal_timers_are_independent_and_append_ignores_elapsed_seal_time() {
        let defaults = WriterConfig::default_for(BLOCK_SIZE, 4);
        assert_eq!(defaults.flush_interval, Duration::from_millis(2));
        assert_eq!(defaults.seal_interval, Duration::from_secs(5));
        let (mut writer, mut buffer) = new_writer();
        assert!(!writer.should_seal_at(writer.opened_at + writer.config.flush_interval));
        assert!(writer.should_seal_at(writer.opened_at + writer.config.seal_interval));
        writer.opened_at = Instant::now() - writer.config.seal_interval;
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        writer
            .append(&mut encoder, 1, 1, 0, &Bytes::from_static(b"one"))
            .unwrap();
        assert!(writer.should_flush_at(writer.last_commit_at + writer.config.flush_interval));
        assert_eq!(writer.flush_group(&mut encoder).unwrap(), [1]);
        assert!(!writer.should_flush_at(writer.last_commit_at + writer.config.flush_interval));
    }

    #[test]
    fn backpressure_padding_rechecks_target_and_preserves_pending_acknowledgments() {
        let mut cfg = config();
        cfg.target_data_bytes = BLOCK_SIZE;
        cfg.max_inflight_bytes = 1600;
        let mut buffer = AlignedBuffer::new_zeroed(cfg.buffer_capacity().unwrap()).unwrap();
        let mut writer = SegmentWriter::create(
            MemoryIo::default(),
            "/data/77.seg",
            "/data",
            header(),
            cfg,
            &mut buffer,
        )
        .unwrap();
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        let payload = Bytes::from(vec![1; 1000]);
        let first = writer.append(&mut encoder, 1, 1, 0, &payload).unwrap();
        assert!(first.acknowledged.is_empty());
        assert!(matches!(
            writer.append(&mut encoder, 1, 2, 0, &payload),
            Err(WriterError::SegmentFull)
        ));
        assert_eq!(encoder.estimated_data_region_len(), BLOCK_SIZE);
        assert_eq!(writer.pending.len(), 1);
        let sealed = writer.seal(encoder).unwrap();
        assert_eq!(sealed.acknowledged, [first.sequence]);
    }

    #[test]
    fn backpressure_accepts_a_record_when_padded_projection_fits() {
        let (mut writer, mut buffer) = new_writer();
        writer.config.max_inflight_bytes = 1600;
        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        let payload = Bytes::from(vec![1; 1000]);
        writer.append(&mut encoder, 1, 1, 0, &payload).unwrap();
        let second = writer.append(&mut encoder, 1, 2, 0, &payload).unwrap();
        assert_eq!(second.acknowledged, [1]);
        assert_eq!(writer.seal(encoder).unwrap().acknowledged, [2]);
    }
}
