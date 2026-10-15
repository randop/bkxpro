//! Foyer hybrid read cache and read path for bkxpro (RAN-131).
//!
//! Read path:
//! 1. Foyer memory tier (in-memory cache).
//! 2. Open segment in-memory index, then fjall index to locate segment and offset.
//! 3. Aligned O_DIRECT read from the local segment through the io_uring path (fixed buffers),
//!    or ranged S3 GET if the segment is deleted.
//! 4. Fill foyer. The NVMe tier is a separate directory from staging so eviction never touches staging data.
//!
//! Memory budget (example, 2 GB cap):
//! Write buffers 256 MB, fjall 128 MB, foyer memory tier 512 MB, foyer NVMe tier disk-sized, remainder headroom.
//! The cap is enforced and measured against cgroup v2 `memory.current` (RSS, page cache, and kernel memory).

#[cfg(not(target_os = "linux"))]
compile_error!("bkxpro-reader supports Linux only; develop and test in a Linux VM/devcontainer");

use std::{path::PathBuf, sync::Arc, time::Duration};

use bkxpro_codec::{BLOCK_SIZE, HEADER_SIZE, TRAILER_SIZE};
use bkxpro_indexer::{EntryLocation, Index, SegmentMeta, SegmentState};
use bkxpro_segmenter::{AlignedBuffer, SegmentIo, WriterConfig};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Compression, DeviceBuilder, FifoPicker, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, IoEngineConfig, LruConfig, NoopDeviceBuilder, NoopIoEngineConfig,
    PsyncIoEngineConfig, RecoverMode, Source, Throttle,
};
use foyer_storage::UringIoEngineConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{info, warn};

pub const DEFAULT_MEMORY_TIER_BYTES: usize = 512 * 1024 * 1024; // 512 MB
pub const DEFAULT_NVME_TIER_BYTES: usize = 16 * 1024 * 1024 * 1024; // 16 GB

/// Minimum number of foyer disk blocks required for the hybrid cache to make
/// progress. foyer's block engine uses `flushers(1) + clean_block_threshold(1)`
/// blocks and warns (then stalls) when `blocks / 2` is not comfortably above
/// that. With fewer than [`MIN_DISK_BLOCKS`] blocks, writes and even `close()`
/// can deadlock waiting for a clean block that can never be allocated.
pub const MIN_DISK_BLOCKS: usize = 4;

/// I/O engine type for foyer NVMe tier.
#[derive(Debug)]
pub enum IoEngineType {
    /// Synchronous I/O engine using pread/pwrite with tokio thread pool.
    Psync,
    /// io_uring based I/O engine (requires Linux).
    Uring(UringIoEngineConfig),
}

/// Read cache configuration.
#[derive(Debug)]
pub struct ReadCacheConfig {
    /// Path to the data directory (staging tier segments).
    pub data_dir: PathBuf,
    /// Path to the foyer NVMe cache directory (separate from staging).
    pub cache_dir: PathBuf,
    /// Memory tier capacity in bytes.
    pub memory_tier_bytes: usize,
    /// NVMe tier capacity in bytes.
    pub nvme_tier_bytes: usize,
    /// Number of shards for the hybrid cache.
    pub cache_shards: usize,
    /// Maximum concurrent read operations (for backpressure).
    pub max_concurrent_reads: usize,
    /// Block size for the disk cache engine.
    pub block_size: usize,
    /// Enable compression in NVMe tier.
    pub enable_compression: bool,
    /// I/O engine type for foyer NVMe tier.
    pub io_engine_type: IoEngineType,
    /// Read buffer pool capacity (4 KiB blocks).
    pub read_buffer_blocks: usize,
    /// S3 read timeout.
    pub s3_read_timeout: Duration,
}

impl Default for ReadCacheConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            cache_dir: PathBuf::from("./cache"),
            memory_tier_bytes: DEFAULT_MEMORY_TIER_BYTES,
            nvme_tier_bytes: DEFAULT_NVME_TIER_BYTES,
            cache_shards: 4,
            max_concurrent_reads: 128,
            block_size: 16 * 1024 * 1024, // 16 MB
            enable_compression: true,
            io_engine_type: IoEngineType::Psync,
            read_buffer_blocks: 256,
            s3_read_timeout: Duration::from_secs(30),
        }
    }
}

/// Statistics for the read cache.
#[derive(Debug, Default, Clone)]
pub struct ReadCacheStats {
    pub memory_hits: u64,
    pub nvme_hits: u64,
    pub segment_hits: u64,
    pub s3_reads: u64,
    pub misses: u64,
    pub read_latency_ns_total: u64,
    pub read_latency_ns_max: u64,
    pub read_bytes: u64,
    pub cache_fills: u64,
    pub evictions: u64,
}

/// Result of a read operation.
#[derive(Debug)]
pub struct ReadResult {
    pub payload: Bytes,
    pub source: ReadSource,
    pub latency_ns: u64,
}

/// Source of the data for a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSource {
    MemoryCache,
    NVMeCache,
    LocalSegment,
    S3,
}

/// Errors from read cache operations.
#[derive(Debug, Error)]
pub enum ReadCacheError {
    #[error("index error: {0}")]
    Index(#[from] bkxpro_indexer::IndexError),

    #[error("codec error: {0}")]
    Codec(#[from] bkxpro_codec::CodecError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("foyer error: {0}")]
    Foyer(#[from] foyer::Error),

    #[error("segment not found: {0}")]
    SegmentNotFound(u64),

    #[error("entry not found: ledger={0}, entry={1}")]
    EntryNotFound(u64, u64),

    #[error("segment is deleted, use S3 read path")]
    SegmentDeleted,

    #[error("S3 read error: {0}")]
    S3Read(String),

    #[error("read timed out")]
    Timeout,

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("cache is closed")]
    Closed,

    #[error("segment I/O error: {0}")]
    SegmentIo(String),
}

/// Trait for reading segment data from local storage or S3.
/// This allows the read cache to be storage-agnostic.
#[async_trait::async_trait]
pub trait SegmentReader: Send + Sync {
    /// Read a range of bytes from a segment file.
    async fn read_segment_range(
        &self,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError>;

    /// Check if a segment exists locally.
    async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError>;

    /// Get segment metadata.
    async fn get_segment_meta(
        &self,
        segment_id: u64,
    ) -> Result<Option<SegmentMeta>, ReadCacheError>;
}

/// Local segment reader using the RAN-134 I/O layer.
///
/// Local reads are serialized on a dedicated thread. The backend is constructed
/// there so thread-bound io_uring operations never run on Tokio workers. Requests
/// wait asynchronously; no I/O mutex is held across blocking operations.
pub struct LocalSegmentReader<I: SegmentIo> {
    requests: tokio::sync::mpsc::Sender<LocalReadRequest>,
    data_dir: PathBuf,
    index: Arc<Index>,
    _io: std::marker::PhantomData<fn() -> I>,
}

struct LocalReadRequest {
    path: PathBuf,
    segment_id: u64,
    offset: u64,
    length: usize,
    reply: tokio::sync::oneshot::Sender<Result<Bytes, ReadCacheError>>,
}

impl<I: SegmentIo + 'static> LocalSegmentReader<I> {
    /// Construct the backend on its owning I/O thread using `factory`.
    pub fn new<F>(
        factory: F,
        data_dir: PathBuf,
        index: Arc<Index>,
        _config: WriterConfig,
    ) -> Result<Self, ReadCacheError>
    where
        F: FnOnce() -> Result<I, I::Error> + Send + 'static,
    {
        let (requests, mut receiver) = tokio::sync::mpsc::channel::<LocalReadRequest>(1);
        let (ready, initialized) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("segment-reader".to_string())
            .spawn(move || {
                let mut io = match factory() {
                    Ok(io) => {
                        let _ = ready.send(Ok(()));
                        io
                    }
                    Err(error) => {
                        let _ = ready.send(Err(ReadCacheError::SegmentIo(error.to_string())));
                        return;
                    }
                };
                while let Some(request) = receiver.blocking_recv() {
                    let result = Self::read_local_range(
                        &mut io,
                        &request.path,
                        request.segment_id,
                        request.offset,
                        request.length,
                    );
                    let _ = request.reply.send(result);
                }
            })?;
        initialized
            .recv()
            .map_err(|error| ReadCacheError::SegmentIo(error.to_string()))??;
        Ok(Self {
            requests,
            data_dir,
            index,
            _io: std::marker::PhantomData,
        })
    }

    fn read_local_range(
        io: &mut I,
        segment_path: &std::path::Path,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError> {
        // The read path must observe the sealed file as-is: open it read-only and
        // never preallocate/truncate it (a write-capacity acquire would resize the
        // sealed segment and destroy its canonical length).
        let file = io
            .open_segment_for_read(segment_path)
            .map_err(|e| ReadCacheError::SegmentIo(e.to_string()))?;
        let file_size = io
            .file_size(&file)
            .map_err(|e| ReadCacheError::SegmentIo(e.to_string()))?;
        if offset >= file_size {
            return Err(ReadCacheError::SegmentIo(format!(
                "read of segment {segment_id} starts at {offset} past end of file ({file_size} bytes)"
            )));
        }

        // Align offset and length to 4 KiB block boundaries for O_DIRECT. The read
        // must also end on a block boundary and stay inside the file: floor the
        // file size to its last complete block so a partially written file cannot
        // trigger an unaligned O_DIRECT read.
        let aligned_offset = offset & !(BLOCK_SIZE as u64 - 1);
        let aligned_end =
            (offset + length as u64 + BLOCK_SIZE as u64 - 1) & !(BLOCK_SIZE as u64 - 1);
        let read_end = aligned_end.min(file_size & !(BLOCK_SIZE as u64 - 1));
        if read_end <= aligned_offset {
            return Err(ReadCacheError::SegmentIo(format!(
                "segment {segment_id} has no complete block at offset {offset} (file size {file_size})"
            )));
        }
        let read_len = (read_end - aligned_offset) as usize;

        let mut buffer = AlignedBuffer::new_zeroed(read_len)?;
        io.read_aligned(&file, aligned_offset, &mut buffer)
            .map_err(|error| ReadCacheError::SegmentIo(error.to_string()))?;
        let start = (offset - aligned_offset) as usize;
        let end = (start + length).min(read_len);
        Ok(Bytes::copy_from_slice(&buffer[start..end]))
    }
}

#[async_trait::async_trait]
impl<I: SegmentIo + 'static> SegmentReader for LocalSegmentReader<I> {
    async fn read_segment_range(
        &self,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ReadCacheError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let (reply, response) = tokio::sync::oneshot::channel();
        self.requests
            .send(LocalReadRequest {
                path: self.data_dir.join(format!("{segment_id}.seg")),
                segment_id,
                offset,
                length,
                reply,
            })
            .await
            .map_err(|_| ReadCacheError::SegmentIo("local I/O worker is closed".to_string()))?;
        response.await.map_err(|_| {
            ReadCacheError::SegmentIo("local I/O worker stopped during read".to_string())
        })?
    }

    async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
        let segment_path = self.data_dir.join(format!("{}.seg", segment_id));
        Ok(segment_path.exists())
    }

    async fn get_segment_meta(
        &self,
        segment_id: u64,
    ) -> Result<Option<SegmentMeta>, ReadCacheError> {
        Ok(self.index.get_segment(segment_id)?)
    }
}

/// Cached segment data in foyer.
/// Key: (segment_id << 32) | block_offset (4 KiB block index within segment)
/// Value: 4 KiB block of data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSegmentData {
    pub segment_id: u64,
    pub block_index: u32,
    pub data: Vec<u8>,
}

impl CachedSegmentData {
    fn cache_key(segment_id: u64, block_index: u32) -> u64 {
        (segment_id << 32) | block_index as u64
    }
}

/// The main read cache combining foyer hybrid cache with local segment reads.
pub struct ReadCache {
    hybrid_cache: HybridCache<u64, CachedSegmentData>,
    segment_reader: Arc<dyn SegmentReader>,
    index: Arc<Index>,
    config: ReadCacheConfig,
    read_semaphore: Arc<Semaphore>,
    stats: Arc<std::sync::RwLock<ReadCacheStats>>,
    closed: std::sync::atomic::AtomicBool,
}

impl ReadCache {
    /// Create a new read cache with the given configuration and segment reader.
    pub async fn new(
        config: ReadCacheConfig,
        index: Arc<Index>,
        segment_reader: Arc<dyn SegmentReader>,
    ) -> Result<Self, ReadCacheError> {
        Self::new_inner(config, index, segment_reader, false).await
    }

    /// Create a new read cache for testing (uses NoopDeviceBuilder for NVMe tier).
    #[cfg(test)]
    pub async fn new_for_test(
        config: ReadCacheConfig,
        index: Arc<Index>,
        segment_reader: Arc<dyn SegmentReader>,
    ) -> Result<Self, ReadCacheError> {
        Self::new_inner(config, index, segment_reader, true).await
    }

    async fn new_inner(
        mut config: ReadCacheConfig,
        index: Arc<Index>,
        segment_reader: Arc<dyn SegmentReader>,
        for_test: bool,
    ) -> Result<Self, ReadCacheError> {
        // Ensure cache directory exists
        std::fs::create_dir_all(&config.cache_dir)?;

        // Extract fields we need before moving config
        let max_concurrent_reads = config.max_concurrent_reads;

        // Clone other fields needed for building
        let memory_tier_bytes = config.memory_tier_bytes;
        let cache_shards = config.cache_shards;
        let block_size = config.block_size;
        let enable_compression = config.enable_compression;
        let cache_dir = config.cache_dir.clone();
        let nvme_tier_bytes = config.nvme_tier_bytes;

        // foyer's block engine silently ends up with zero blocks when the device
        // cannot fit a single block. A zero-block engine accepts cache inserts but
        // then deadlocks in `close()` (and in flush) waiting for a clean block.
        // Fail fast instead of hanging.
        let min_capacity = block_size.saturating_mul(MIN_DISK_BLOCKS);
        if nvme_tier_bytes < min_capacity {
            return Err(ReadCacheError::InvalidArgument(format!(
                "nvme_tier_bytes ({nvme_tier_bytes}) must be at least block_size * {MIN_DISK_BLOCKS} \
                 ({block_size} * {MIN_DISK_BLOCKS} = {min_capacity}); foyer creates no usable disk \
                 blocks below this and cache close would deadlock"
            )));
        }

        // Build the foyer device and I/O engine. Tests use no-op implementations
        // so they never touch real files or file descriptors.
        let (device, io_engine): (Arc<dyn foyer::Device>, Box<dyn IoEngineConfig>) = if for_test {
            (
                NoopDeviceBuilder::new(nvme_tier_bytes).build()?,
                Box::new(NoopIoEngineConfig),
            )
        } else {
            let device = FsDeviceBuilder::new(&cache_dir)
                .with_capacity(nvme_tier_bytes)
                .with_throttle(
                    Throttle::new()
                        .with_read_iops(10000)
                        .with_write_iops(5000)
                        .with_read_throughput(2 * 1024 * 1024 * 1024)
                        .with_write_throughput(1024 * 1024 * 1024),
                )
                .build()?;
            let io_engine: Box<dyn IoEngineConfig> =
                match std::mem::replace(&mut config.io_engine_type, IoEngineType::Psync) {
                    IoEngineType::Psync => Box::new(PsyncIoEngineConfig::new()),
                    IoEngineType::Uring(uring) => Box::new(uring),
                };
            (device, io_engine)
        };

        // Build the hybrid cache
        let hybrid_cache: HybridCache<u64, CachedSegmentData> = HybridCacheBuilder::new()
            .with_name("bkxpro-reader")
            .memory(memory_tier_bytes)
            .with_shards(cache_shards)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: 0.1,
            })
            .with_weighter(|_, value: &CachedSegmentData| value.data.len())
            .storage()
            .with_io_engine_config(io_engine)
            .with_engine_config(
                BlockEngineConfig::new(device)
                    .with_block_size(block_size)
                    .with_buffer_pool_size(memory_tier_bytes / 2)
                    .with_eviction_pickers(vec![Box::new(FifoPicker::default())]),
            )
            .with_recover_mode(RecoverMode::Quiet)
            .with_compression(if enable_compression {
                Compression::Lz4
            } else {
                Compression::None
            })
            .build()
            .await?;

        Ok(Self {
            hybrid_cache,
            segment_reader,
            index,
            config,
            read_semaphore: Arc::new(Semaphore::new(max_concurrent_reads)),
            stats: Arc::new(std::sync::RwLock::new(ReadCacheStats::default())),
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Read an entry by ledger and entry ID.
    /// Follows the read path: memory cache -> NVMe cache -> local segment -> S3.
    pub async fn read_entry(&self, ledger: u64, entry: u64) -> Result<ReadResult, ReadCacheError> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ReadCacheError::Closed);
        }

        let _permit = self
            .read_semaphore
            .acquire()
            .await
            .map_err(|_| ReadCacheError::Closed)?;
        let start = std::time::Instant::now();

        // 1. Check foyer memory tier first (fastest)
        // The cache key is based on the segment and block containing the entry
        if let Some(location) = self.index.get_entry(ledger, entry)? {
            let block_index = location.offset / BLOCK_SIZE as u32;
            let cache_key = CachedSegmentData::cache_key(location.segment_id as u64, block_index);

            // `HybridCache::get` serves from the memory or the NVMe tier and reports
            // which one answered, so the hit is attributed to the tier that actually
            // served it.
            if let Ok(Some(cached)) = self.hybrid_cache.get(&cache_key).await {
                let source = match cached.source() {
                    Source::Memory => Some(ReadSource::MemoryCache),
                    Source::Disk => Some(ReadSource::NVMeCache),
                    // An `Outer` value came from the caller that inserted it, not from a
                    // tier: fall through to the authoritative local segment read.
                    Source::Outer => None,
                };
                if let Some(source) = source {
                    if let Some(result) = self.cached_payload(&cached, &location, source, start) {
                        return Ok(result);
                    }
                }
            }

            // 2. Decide where the authoritative copy lives. The local segment is
            // only usable while its metadata is not the post-delete tombstone and
            // the file is still staged on disk; otherwise the uploader removed it
            // after committing it to the object store. A missing local file with a
            // non-deleted state (a crash between unlink and the state update) must
            // also fall through to S3 rather than fail the read.
            let segment_id = location.segment_id as u64;
            let segment_meta = self.segment_reader.get_segment_meta(segment_id).await?;
            let staged_locally = match &segment_meta {
                Some(meta) if meta.state != SegmentState::Deleted => {
                    self.segment_reader.segment_exists(segment_id).await?
                }
                _ => false,
            };

            if staged_locally {
                // 3. Read from local segment via io_uring
                let payload = self.read_from_segment(&location).await?;
                let latency_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;

                // Fill foyer cache with the block containing this entry
                self.fill_cache_block(segment_id, block_index).await?;

                self.record_stats(ReadSource::LocalSegment, latency_ns, location.len as u64);
                return Ok(ReadResult {
                    payload,
                    source: ReadSource::LocalSegment,
                    latency_ns,
                });
            }

            // 4. Fall back to a ranged GET through the segment reader, which routes
            // to the object store once the local copy is gone.
            let payload = match self.read_from_segment(&location).await {
                Ok(payload) => payload,
                Err(error) => {
                    let latency_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    self.record_stats(ReadSource::S3, latency_ns, location.len as u64);
                    return Err(ReadCacheError::S3Read(error.to_string()));
                }
            };
            let latency_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;

            // Warm the block cache from the remote copy. A fill failure must not
            // fail the read: the payload is already in hand and the next read will
            // simply fetch it again.
            if let Err(error) = self.fill_cache_block(segment_id, block_index).await {
                warn!("failed to cache block {block_index} of segment {segment_id} read from S3: {error}");
            }

            self.record_stats(ReadSource::S3, latency_ns, location.len as u64);
            Ok(ReadResult {
                payload,
                source: ReadSource::S3,
                latency_ns,
            })
        } else {
            // No index entry: count as a miss, not as an S3 read.
            {
                let mut stats = self.stats.write().unwrap();
                stats.misses += 1;
                stats.read_latency_ns_total = stats
                    .read_latency_ns_total
                    .saturating_add(start.elapsed().as_nanos().min(u64::MAX as u128) as u64);
            }
            Err(ReadCacheError::EntryNotFound(ledger, entry))
        }
    }

    /// Read a range of entries from a ledger.
    pub async fn read_range(
        &self,
        ledger: u64,
        start_entry: u64,
        end_entry: u64,
    ) -> Result<Vec<ReadResult>, ReadCacheError> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ReadCacheError::Closed);
        }

        let entries = self.index.scan_entries(ledger, start_entry, end_entry)?;
        let mut results = Vec::with_capacity(entries.len());

        for (_, entry, _location) in entries {
            match self.read_entry(ledger, entry).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    warn!("Failed to read entry: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(results)
    }

    /// Slice the payload of one entry out of a cached block and record the hit.
    ///
    /// Returns `None` when the cached block is shorter than the requested range (a
    /// truncated tail block or a stale entry), which makes the caller fall through to
    /// the authoritative local segment read.
    fn cached_payload(
        &self,
        cached: &CachedSegmentData,
        location: &EntryLocation,
        source: ReadSource,
        start: std::time::Instant,
    ) -> Option<ReadResult> {
        let entry_offset = (location.offset % BLOCK_SIZE as u32) as usize;
        let end = entry_offset + location.len as usize;
        let bytes = cached.data.get(entry_offset..end)?;
        let payload = Bytes::copy_from_slice(bytes);
        let latency_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.record_stats(source, latency_ns, location.len as u64);
        Some(ReadResult {
            payload,
            source,
            latency_ns,
        })
    }

    /// Read directly from a local segment using aligned O_DIRECT reads.
    async fn read_from_segment(&self, location: &EntryLocation) -> Result<Bytes, ReadCacheError> {
        self.segment_reader
            .read_segment_range(
                location.segment_id as u64,
                location.offset as u64,
                location.len as usize,
            )
            .await
    }

    /// Fill a 4 KiB block into the foyer cache.
    async fn fill_cache_block(
        &self,
        segment_id: u64,
        block_index: u32,
    ) -> Result<(), ReadCacheError> {
        let cache_key = CachedSegmentData::cache_key(segment_id, block_index);

        // Check if already cached
        if self.hybrid_cache.get(&cache_key).await?.is_some() {
            return Ok(());
        }

        // Read the full block from the segment
        let offset = (block_index as u64) * BLOCK_SIZE as u64;
        let block_data = self
            .segment_reader
            .read_segment_range(segment_id, offset, BLOCK_SIZE)
            .await?;

        let cached_data = CachedSegmentData {
            segment_id,
            block_index,
            data: block_data.to_vec(),
        };

        // Insert into foyer (will go to memory tier, then potentially NVMe tier on eviction)
        self.hybrid_cache.insert(cache_key, cached_data);

        let mut stats = self.stats.write().unwrap();
        stats.cache_fills += 1;

        Ok(())
    }

    /// Warm the cache by preloading blocks from a segment.
    pub async fn warm_segment(&self, segment_id: u64) -> Result<usize, ReadCacheError> {
        let segment_meta = self
            .segment_reader
            .get_segment_meta(segment_id)
            .await?
            .ok_or(ReadCacheError::SegmentNotFound(segment_id))?;

        if segment_meta.state == SegmentState::Deleted {
            return Err(ReadCacheError::SegmentDeleted);
        }

        // Get segment file size to determine number of blocks
        let segment_path = self.config.data_dir.join(format!("{}.seg", segment_id));
        let file_size = std::fs::metadata(&segment_path)?.len();
        if file_size < (HEADER_SIZE + TRAILER_SIZE) as u64 {
            return Err(ReadCacheError::InvalidArgument(format!(
                "segment {} file too small to contain header and trailer: {} bytes",
                segment_id, file_size
            )));
        }
        let data_region_end = file_size - TRAILER_SIZE as u64;
        let block_count = ((data_region_end - HEADER_SIZE as u64) / BLOCK_SIZE as u64) as u32;

        // Blocks are addressed by their global index in the file: the header occupies
        // block 0, so the data blocks are 1..=block_count. Warming must target the
        // data blocks, not the header block.
        let first_data_block = (HEADER_SIZE / BLOCK_SIZE) as u32;

        let mut loaded = 0;
        for block_index in first_data_block..first_data_block + block_count {
            if self.fill_cache_block(segment_id, block_index).await.is_ok() {
                loaded += 1;
            }
        }

        info!(
            "Warmed segment {}: loaded {} blocks into cache",
            segment_id, loaded
        );
        Ok(loaded)
    }

    /// Get cache statistics.
    pub fn stats(&self) -> ReadCacheStats {
        self.stats.read().unwrap().clone()
    }

    /// Get foyer internal statistics.
    pub fn foyer_stats(&self) -> Arc<foyer::Statistics> {
        self.hybrid_cache.statistics().clone()
    }

    /// Record read statistics.
    fn record_stats(&self, source: ReadSource, latency_ns: u64, bytes: u64) {
        let mut stats = self.stats.write().unwrap();
        stats.read_latency_ns_total = stats.read_latency_ns_total.saturating_add(latency_ns);
        stats.read_latency_ns_max = stats.read_latency_ns_max.max(latency_ns);
        stats.read_bytes = stats.read_bytes.saturating_add(bytes);

        match source {
            ReadSource::MemoryCache => stats.memory_hits += 1,
            ReadSource::NVMeCache => stats.nvme_hits += 1,
            ReadSource::LocalSegment => stats.segment_hits += 1,
            ReadSource::S3 => stats.s3_reads += 1,
        }
    }

    /// Close the read cache.
    pub async fn close(&self) -> Result<(), ReadCacheError> {
        if !self.closed.swap(true, std::sync::atomic::Ordering::Relaxed) {
            self.hybrid_cache.close().await?;
        }
        Ok(())
    }

    /// Check if the cache is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Builder for ReadCache with common defaults.
pub struct ReadCacheBuilder {
    config: ReadCacheConfig,
}

impl ReadCacheBuilder {
    pub fn new(data_dir: PathBuf, cache_dir: PathBuf) -> Self {
        Self {
            config: ReadCacheConfig {
                data_dir,
                cache_dir,
                ..Default::default()
            },
        }
    }

    /// Inspect the configuration assembled so far without building the cache.
    pub fn config(&self) -> &ReadCacheConfig {
        &self.config
    }

    pub fn memory_tier_bytes(mut self, bytes: usize) -> Self {
        self.config.memory_tier_bytes = bytes;
        self
    }

    pub fn nvme_tier_bytes(mut self, bytes: usize) -> Self {
        self.config.nvme_tier_bytes = bytes;
        self
    }

    pub fn cache_shards(mut self, shards: usize) -> Self {
        self.config.cache_shards = shards;
        self
    }

    pub fn max_concurrent_reads(mut self, max: usize) -> Self {
        self.config.max_concurrent_reads = max;
        self
    }

    pub fn block_size(mut self, size: usize) -> Self {
        self.config.block_size = size;
        self
    }

    pub fn enable_compression(mut self, enable: bool) -> Self {
        self.config.enable_compression = enable;
        self
    }

    pub fn io_engine_type(mut self, engine_type: IoEngineType) -> Self {
        self.config.io_engine_type = engine_type;
        self
    }

    pub fn read_buffer_blocks(mut self, blocks: usize) -> Self {
        self.config.read_buffer_blocks = blocks;
        self
    }

    pub fn s3_read_timeout(mut self, timeout: Duration) -> Self {
        self.config.s3_read_timeout = timeout;
        self
    }

    pub async fn build(
        self,
        index: Arc<Index>,
        segment_reader: Arc<dyn SegmentReader>,
    ) -> Result<ReadCache, ReadCacheError> {
        ReadCache::new(self.config, index, segment_reader).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bkxpro_indexer::{EntryLocation, Index, IndexConfig, SegmentMeta, SegmentState};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// Number of 4 KiB blocks reserved for the segment header (block 0).
    const HEADER_BLOCKS: usize = HEADER_SIZE / BLOCK_SIZE;

    /// Build a zeroed segment image with `data_blocks` data blocks plus the header
    /// block and the trailing trailer, mirroring the real on-disk segment layout.
    fn segment_image(data_blocks: usize) -> Vec<u8> {
        vec![0u8; HEADER_SIZE + data_blocks * BLOCK_SIZE + TRAILER_SIZE]
    }

    /// Write `payload` into the image at data block `block` (0-based within the data
    /// region) and in-block offset `in_block`, returning the absolute file offset.
    fn put_payload(image: &mut [u8], block: usize, in_block: usize, payload: &[u8]) -> u32 {
        let offset = HEADER_SIZE + block * BLOCK_SIZE + in_block;
        image[offset..offset + payload.len()].copy_from_slice(payload);
        offset as u32
    }

    struct MockSegment {
        /// Bytes of the staged local file; `None` models a file the uploader has
        /// already unlinked while the index still reports a non-deleted state.
        local: Option<Vec<u8>>,
        state: SegmentState,
    }

    /// In-memory segment storage used as the `SegmentReader` under test.
    ///
    /// Reads are served byte-for-byte from the fake segment image so the read path
    /// (index lookup -> segment range read -> block cache fill -> memory hit) can be
    /// exercised end to end without real disks. Like the real fallback reader, the
    /// mock routes a range read to the object-store copy once the local file is gone
    /// or the segment is deleted, and reports failures for segments that exist in
    /// neither place.
    #[derive(Default)]
    struct MockSegmentReader {
        segments: Mutex<HashMap<u64, MockSegment>>,
        s3_segments: Mutex<HashMap<u64, Vec<u8>>>,
        read_calls: AtomicU64,
    }

    impl MockSegmentReader {
        fn add_segment(&self, segment_id: u64, data: Vec<u8>) {
            self.add_segment_with_state(segment_id, data, SegmentState::Sealed);
        }

        fn add_segment_with_state(&self, segment_id: u64, data: Vec<u8>, state: SegmentState) {
            self.segments.lock().unwrap().insert(
                segment_id,
                MockSegment {
                    local: Some(data),
                    state,
                },
            );
        }

        /// Register a segment the uploader deleted locally without updating its
        /// state yet: the index row says `state`, but no local file exists.
        fn add_segment_without_local_file(&self, segment_id: u64, state: SegmentState) {
            self.segments
                .lock()
                .unwrap()
                .insert(segment_id, MockSegment { local: None, state });
        }

        /// Register the object-store copy of a segment.
        fn add_s3_segment(&self, segment_id: u64, data: Vec<u8>) {
            self.s3_segments.lock().unwrap().insert(segment_id, data);
        }

        fn read_calls(&self) -> u64 {
            self.read_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl SegmentReader for MockSegmentReader {
        async fn read_segment_range(
            &self,
            segment_id: u64,
            offset: u64,
            length: usize,
        ) -> Result<Bytes, ReadCacheError> {
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            // Serve the local file while it exists and is not deleted, then fall
            // back to the object-store copy.
            let local = {
                let segments = self.segments.lock().unwrap();
                segments.get(&segment_id).and_then(|segment| {
                    if segment.state == SegmentState::Deleted {
                        None
                    } else {
                        segment.local.clone()
                    }
                })
            };
            let data = match local {
                Some(data) => data,
                None => self
                    .s3_segments
                    .lock()
                    .unwrap()
                    .get(&segment_id)
                    .cloned()
                    .ok_or(ReadCacheError::SegmentNotFound(segment_id))?,
            };
            let start = (offset as usize).min(data.len());
            let end = (start + length).min(data.len());
            Ok(Bytes::copy_from_slice(&data[start..end]))
        }

        async fn segment_exists(&self, segment_id: u64) -> Result<bool, ReadCacheError> {
            Ok(self
                .segments
                .lock()
                .unwrap()
                .get(&segment_id)
                .is_some_and(|segment| segment.local.is_some()))
        }

        async fn get_segment_meta(
            &self,
            segment_id: u64,
        ) -> Result<Option<SegmentMeta>, ReadCacheError> {
            Ok(self
                .segments
                .lock()
                .unwrap()
                .get(&segment_id)
                .map(|segment| {
                    let byte_size = segment
                        .local
                        .as_ref()
                        .map(|data| data.len() as u64)
                        .or_else(|| {
                            self.s3_segments
                                .lock()
                                .unwrap()
                                .get(&segment_id)
                                .map(|data| data.len() as u64)
                        })
                        .unwrap_or(0);
                    SegmentMeta {
                        state: segment.state,
                        s3_key: None,
                        min_ledger: 1,
                        max_ledger: 1,
                        byte_size,
                        writer_epoch: 1,
                    }
                }))
        }
    }

    /// Self-contained test environment: owns the temp dirs for the index, the cache
    /// and the segment staging area for the whole test (so nothing is deleted while
    /// the index or cache still holds it open) plus the shared index and mock reader.
    struct TestEnv {
        _index_tmp: TempDir,
        _cache_tmp: TempDir,
        data_tmp: TempDir,
        index: Arc<Index>,
        reader: Arc<MockSegmentReader>,
    }

    impl TestEnv {
        fn new() -> Self {
            let index_tmp = TempDir::new().unwrap();
            let cache_tmp = TempDir::new().unwrap();
            let data_tmp = TempDir::new().unwrap();

            let index = Arc::new(
                Index::open(IndexConfig {
                    db_path: index_tmp.path().join("index"),
                    max_memory_bytes: 64 * 1024 * 1024,
                    ..Default::default()
                })
                .unwrap(),
            );

            Self {
                _index_tmp: index_tmp,
                _cache_tmp: cache_tmp,
                data_tmp,
                index,
                reader: Arc::new(MockSegmentReader::default()),
            }
        }

        /// A valid test configuration. `nvme_tier_bytes` comfortably exceeds
        /// `block_size * MIN_DISK_BLOCKS` so foyer allocates usable disk blocks.
        fn config(&self) -> ReadCacheConfig {
            ReadCacheConfig {
                data_dir: self.data_tmp.path().to_path_buf(),
                cache_dir: self._cache_tmp.path().join("cache"),
                memory_tier_bytes: 8 * 1024 * 1024,
                nvme_tier_bytes: 128 * 1024 * 1024,
                cache_shards: 2,
                max_concurrent_reads: 16,
                block_size: 16 * 1024 * 1024,
                enable_compression: false,
                ..Default::default()
            }
        }

        /// Build a cache backed by the no-op device + no-op IO engine.
        async fn build(&self) -> ReadCache {
            ReadCache::new_for_test(self.config(), self.index.clone(), self.reader.clone())
                .await
                .unwrap()
        }

        fn put_entry(&self, ledger: u64, entry: u64, segment_id: u64, offset: u32, len: u32) {
            let mut batch = self.index.batch();
            self.index
                .put_entry(
                    &mut batch,
                    ledger,
                    entry,
                    EntryLocation {
                        segment_id: segment_id as u32,
                        offset,
                        len,
                    },
                )
                .unwrap();
            batch.commit().unwrap();
        }

        fn put_segment(&self, segment_id: u64, state: SegmentState) {
            let mut batch = self.index.batch();
            self.index
                .put_segment(
                    &mut batch,
                    segment_id,
                    SegmentMeta {
                        state,
                        s3_key: None,
                        min_ledger: 1,
                        max_ledger: 1,
                        byte_size: 4096,
                        writer_epoch: 1,
                    },
                )
                .unwrap();
            batch.commit().unwrap();
        }
    }

    // ===== Configuration and key encoding =====

    struct BlockingIo {
        owner: std::thread::ThreadId,
        started: Option<tokio::sync::oneshot::Sender<std::thread::ThreadId>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl SegmentIo for BlockingIo {
        type File = ();
        type Error = std::io::Error;

        fn acquire_preallocated_segment(
            &mut self,
            _: &std::path::Path,
            _: usize,
        ) -> Result<bkxpro_segmenter::AcquiredSegment<()>, Self::Error> {
            unreachable!()
        }
        fn open_segment_for_read(&mut self, _: &std::path::Path) -> Result<(), Self::Error> {
            assert_eq!(std::thread::current().id(), self.owner);
            if let Some(started) = self.started.take() {
                let _ = started.send(self.owner);
            }
            Ok(())
        }
        fn write_batch(
            &mut self,
            _: &(),
            _: &[bkxpro_segmenter::WriteOp<'_>],
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
        fn commit_batch(
            &mut self,
            _: &(),
            _: &[bkxpro_segmenter::WriteOp<'_>],
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
        fn sync_data(&mut self, _: &()) -> Result<(), Self::Error> {
            unreachable!()
        }
        fn read_aligned(
            &mut self,
            _: &(),
            offset: u64,
            buffer: &mut [u8],
        ) -> Result<(), Self::Error> {
            assert_eq!(std::thread::current().id(), self.owner);
            assert_eq!(offset, 0);
            assert_eq!(buffer.len(), BLOCK_SIZE);
            assert_eq!(buffer.as_ptr() as usize % BLOCK_SIZE, 0);
            self.release
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?;
            buffer.fill(0x3d);
            Ok(())
        }
        fn truncate(&mut self, _: &(), _: u64) -> Result<(), Self::Error> {
            unreachable!()
        }
        fn unlink(&mut self, _: &std::path::Path, _: &std::path::Path) -> Result<(), Self::Error> {
            unreachable!()
        }
        fn sync_directory(&mut self, _: &std::path::Path) -> Result<(), Self::Error> {
            unreachable!()
        }
        fn file_size(&self, _: &()) -> Result<u64, Self::Error> {
            Ok(BLOCK_SIZE as u64)
        }
    }

    #[tokio::test]
    async fn local_io_preserves_its_owner_and_does_not_block_the_async_runtime() {
        let env = TestEnv::new();
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let caller = std::thread::current().id();
        let reader = LocalSegmentReader::new(
            move || {
                Ok(BlockingIo {
                    owner: std::thread::current().id(),
                    started: Some(started),
                    release: blocked,
                })
            },
            env.data_tmp.path().to_path_buf(),
            Arc::clone(&env.index),
            WriterConfig::default_for(64 * 1024, 10),
        )
        .unwrap();
        let mut read = Box::pin(reader.read_segment_range(1, 21, 10));
        tokio::select! {
            result = &mut read => panic!("read finished before release: {result:?}"),
            owner = start => assert_ne!(owner.unwrap(), caller),
        }
        // This timer can progress on a single-thread runtime while the I/O is blocked.
        tokio::time::sleep(Duration::from_millis(5)).await;
        release.send(()).unwrap();
        assert_eq!(read.await.unwrap(), Bytes::from_static(&[0x3d; 10]));
    }

    #[tokio::test]
    async fn local_io_factory_failure_is_a_read_cache_error() {
        let env = TestEnv::new();
        let result = LocalSegmentReader::<BlockingIo>::new(
            || Err(std::io::Error::other("factory failure")),
            env.data_tmp.path().to_path_buf(),
            Arc::clone(&env.index),
            WriterConfig::default_for(64 * 1024, 10),
        );
        assert!(
            matches!(result, Err(ReadCacheError::SegmentIo(message)) if message.contains("factory failure"))
        );
    }

    #[test]
    fn test_cache_key_encoding() {
        // Different segments must not collide.
        assert_ne!(
            CachedSegmentData::cache_key(1, 0),
            CachedSegmentData::cache_key(2, 0)
        );
        // Different blocks within one segment must not collide.
        assert_ne!(
            CachedSegmentData::cache_key(1, 0),
            CachedSegmentData::cache_key(1, 1)
        );
        // The key is deterministic and packs the block index into the low 32 bits.
        assert_eq!(
            CachedSegmentData::cache_key(7, 42),
            CachedSegmentData::cache_key(7, 42)
        );
        assert_eq!(CachedSegmentData::cache_key(1, 5), (1u64 << 32) | 5);
        assert_eq!(CachedSegmentData::cache_key(0, u32::MAX), u32::MAX as u64);
    }

    #[test]
    fn test_default_config_values() {
        let config = ReadCacheConfig::default();
        assert_eq!(config.memory_tier_bytes, DEFAULT_MEMORY_TIER_BYTES);
        assert_eq!(config.nvme_tier_bytes, DEFAULT_NVME_TIER_BYTES);
        assert_eq!(config.block_size, 16 * 1024 * 1024);
        // The defaults must leave room for enough foyer disk blocks to make progress.
        assert!(config.nvme_tier_bytes >= config.block_size * MIN_DISK_BLOCKS);
        assert!(matches!(config.io_engine_type, IoEngineType::Psync));
        assert_eq!(config.s3_read_timeout, Duration::from_secs(30));
        assert!(config.cache_shards > 0);
        assert!(config.max_concurrent_reads > 0);
    }

    #[test]
    fn test_builder_applies_settings() {
        let builder = ReadCacheBuilder::new(PathBuf::from("/data"), PathBuf::from("/cache"))
            .memory_tier_bytes(64 * 1024 * 1024)
            .nvme_tier_bytes(256 * 1024 * 1024)
            .cache_shards(8)
            .max_concurrent_reads(32)
            .block_size(8 * 1024 * 1024)
            .enable_compression(false)
            .read_buffer_blocks(64)
            .s3_read_timeout(Duration::from_secs(5));

        let config = builder.config();
        assert_eq!(config.data_dir, PathBuf::from("/data"));
        assert_eq!(config.cache_dir, PathBuf::from("/cache"));
        assert_eq!(config.memory_tier_bytes, 64 * 1024 * 1024);
        assert_eq!(config.nvme_tier_bytes, 256 * 1024 * 1024);
        assert_eq!(config.cache_shards, 8);
        assert_eq!(config.max_concurrent_reads, 32);
        assert_eq!(config.block_size, 8 * 1024 * 1024);
        assert!(!config.enable_compression);
        assert_eq!(config.read_buffer_blocks, 64);
        assert_eq!(config.s3_read_timeout, Duration::from_secs(5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_config_rejected_when_device_too_small() {
        let env = TestEnv::new();
        let mut config = env.config();
        // One block short of the minimum: foyer would create zero blocks and
        // `close()` would deadlock, so construction must fail fast instead.
        config.nvme_tier_bytes = config.block_size * (MIN_DISK_BLOCKS - 1);

        let result = ReadCache::new_for_test(config, env.index.clone(), env.reader.clone()).await;

        match result {
            Err(ReadCacheError::InvalidArgument(message)) => {
                assert!(
                    message.contains("nvme_tier_bytes"),
                    "unexpected message: {message}"
                );
            }
            Err(other) => panic!("expected InvalidArgument, got {other:?}"),
            Ok(_) => panic!("expected construction to fail, but a cache was built"),
        }
    }

    // ===== Read path: misses, hits, fallbacks =====

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_entry_not_found_counts_miss() {
        let env = TestEnv::new();
        let cache = env.build().await;

        let err = cache.read_entry(1, 999).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::EntryNotFound(1, 999)));

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.s3_reads, 0);
        assert_eq!(stats.segment_hits, 0);
        assert_eq!(stats.memory_hits, 0);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_read_then_memory_hit() {
        let env = TestEnv::new();
        let mut image = segment_image(1);
        let payload = b"hello world";
        let offset = put_payload(&mut image, 0, 0, payload);
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        env.put_entry(1, 1, 1, offset, payload.len() as u32);

        let cache = env.build().await;

        // First read: served from the local segment and fills the block cache.
        let first = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(first.payload, Bytes::from_static(payload));
        assert_eq!(first.source, ReadSource::LocalSegment);
        // One range read for the entry plus one full-block read to fill the cache.
        assert_eq!(env.reader.read_calls(), 2);

        // Second read: served entirely from the memory tier, no segment I/O.
        let second = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(second.payload, Bytes::from_static(payload));
        assert_eq!(second.source, ReadSource::MemoryCache);
        assert_eq!(env.reader.read_calls(), 2);

        let stats = cache.stats();
        assert_eq!(stats.segment_hits, 1);
        assert_eq!(stats.memory_hits, 1);
        assert_eq!(stats.s3_reads, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.cache_fills, 1);
        assert_eq!(stats.read_bytes, (payload.len() * 2) as u64);
        assert!(stats.read_latency_ns_total > 0);
        assert!(stats.read_latency_ns_max > 0);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s3_fallback_for_missing_segment() {
        let env = TestEnv::new();
        // Index knows the entry but the segment was never staged locally.
        env.put_entry(1, 1, 77, 4096, 16);

        let cache = env.build().await;
        let err = cache.read_entry(1, 1).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::S3Read(_)));

        let stats = cache.stats();
        assert_eq!(stats.s3_reads, 1);
        assert_eq!(stats.segment_hits, 0);
        assert_eq!(stats.memory_hits, 0);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s3_fallback_serves_deleted_segment_from_the_object_store() {
        let env = TestEnv::new();
        // The local file is gone and the object store holds the only copy. The
        // stale local image holds different bytes, so the payload also proves
        // which copy answered.
        let mut local = segment_image(1);
        put_payload(&mut local, 0, 0, b"stale!!!");
        env.reader
            .add_segment_with_state(1, local, SegmentState::Deleted);
        let mut remote = segment_image(1);
        let payload = b"from s3!";
        let offset = put_payload(&mut remote, 0, 0, payload);
        env.reader.add_s3_segment(1, remote);
        env.put_segment(1, SegmentState::Deleted);
        env.put_entry(1, 1, 1, offset, payload.len() as u32);

        let cache = env.build().await;
        let result = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(result.payload, Bytes::from_static(payload));
        assert_eq!(result.source, ReadSource::S3);

        let stats = cache.stats();
        assert_eq!(stats.s3_reads, 1);
        assert_eq!(stats.segment_hits, 0);
        assert_eq!(stats.cache_fills, 1, "the remote block warms the cache");

        // The S3 read filled the block cache, so the second read hits memory.
        let second = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(second.payload, Bytes::from_static(payload));
        assert_eq!(second.source, ReadSource::MemoryCache);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s3_fallback_when_the_local_file_is_already_unlinked() {
        let env = TestEnv::new();
        // Crash window: the uploader unlinked the file but has not written the
        // Deleted state yet. The state says Uploaded, yet no local file exists,
        // so the read must use the object store instead of failing.
        env.reader
            .add_segment_without_local_file(1, SegmentState::Uploaded);
        let mut remote = segment_image(1);
        let payload = b"recovered";
        let offset = put_payload(&mut remote, 0, 0, payload);
        env.reader.add_s3_segment(1, remote);
        env.put_segment(1, SegmentState::Uploaded);
        env.put_entry(1, 1, 1, offset, payload.len() as u32);
        // A second entry points at a segment that exists nowhere: the fallback
        // must surface the remote failure as an S3 error, not panic or hang.
        env.put_entry(1, 2, 9, HEADER_SIZE as u32, 5);

        let cache = env.build().await;
        let result = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(result.payload, Bytes::from_static(payload));
        assert_eq!(result.source, ReadSource::S3);

        let err = cache.read_entry(1, 2).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::S3Read(_)));
        assert_eq!(cache.stats().s3_reads, 2);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_truncated_cached_block_falls_back_to_segment() {
        let env = TestEnv::new();
        // Segment is only one truncated data block: a full-block cache fill returns
        // fewer than BLOCK_SIZE bytes.
        let mut image = segment_image(1);
        image.truncate(HEADER_SIZE + 100);
        put_payload(&mut image, 0, 96, b"WXYZ");
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        // Entry starts at in-block offset 96 and claims 8 bytes; the cached block only
        // holds 100 bytes so the cached slice would cross the truncation boundary.
        env.put_entry(1, 1, 1, (HEADER_SIZE + 96) as u32, 8);

        let cache = env.build().await;

        // First read fills the truncated block and then serves the entry from it.
        let first = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(first.payload, Bytes::from_static(b"WXYZ"));
        assert_eq!(first.source, ReadSource::LocalSegment);

        // Second read hits the truncated cached block: the bounds check must reject
        // the partial range and fall back to the segment instead of panicking.
        let second = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(second.payload, Bytes::from_static(b"WXYZ"));
        assert_eq!(second.source, ReadSource::LocalSegment);
        assert_eq!(cache.stats().memory_hits, 0);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_range_returns_entries_in_order() {
        let env = TestEnv::new();
        let mut image = segment_image(2);
        let payloads: [&[u8]; 3] = [b"one", b"two", b"three"];
        let offsets = [
            put_payload(&mut image, 0, 16, payloads[0]),
            put_payload(&mut image, 0, 64, payloads[1]),
            put_payload(&mut image, 1, 0, payloads[2]),
        ];
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        for (index, (payload, offset)) in payloads.iter().zip(offsets).enumerate() {
            env.put_entry(1, index as u64 + 1, 1, offset, payload.len() as u32);
        }

        let cache = env.build().await;
        let results = cache.read_range(1, 1, 3).await.unwrap();

        assert_eq!(results.len(), 3);
        for (result, payload) in results.iter().zip(payloads.iter()) {
            assert_eq!(result.payload, Bytes::copy_from_slice(payload));
        }

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_range_empty() {
        let env = TestEnv::new();
        let cache = env.build().await;

        let results = cache.read_range(1, 1, 10).await.unwrap();
        assert!(results.is_empty());

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_range_propagates_errors() {
        let env = TestEnv::new();
        let mut image = segment_image(1);
        let offset = put_payload(&mut image, 0, 0, b"first");
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        env.put_entry(1, 1, 1, offset, 5);
        // Entry 2 points at a segment that is not staged locally.
        env.put_entry(1, 2, 42, 4096, 5);

        let cache = env.build().await;
        let err = cache.read_range(1, 1, 2).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::S3Read(_)));

        cache.close().await.unwrap();
    }

    // ===== Block cache fill and warming =====

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fill_cache_block_is_idempotent() {
        let env = TestEnv::new();
        let mut image = segment_image(1);
        let payload = b"warm payload";
        let offset = put_payload(&mut image, 0, 32, payload);
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        env.put_entry(1, 1, 1, offset, payload.len() as u32);

        let cache = env.build().await;

        // Header lives in block 0, first data block is block 1.
        let data_block = HEADER_BLOCKS as u32;
        cache.fill_cache_block(1, data_block).await.unwrap();
        assert_eq!(cache.stats().cache_fills, 1);
        assert_eq!(env.reader.read_calls(), 1);

        // Filling again must be a no-op because the block is already cached.
        cache.fill_cache_block(1, data_block).await.unwrap();
        assert_eq!(cache.stats().cache_fills, 1);
        assert_eq!(env.reader.read_calls(), 1);

        // A read of the entry must now be served from the memory tier.
        let result = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(result.payload, Bytes::from_static(payload));
        assert_eq!(result.source, ReadSource::MemoryCache);
        assert_eq!(env.reader.read_calls(), 1);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_warm_segment_preloads_data_blocks() {
        let env = TestEnv::new();
        let mut image = segment_image(2);
        let payload = b"warmed entry";
        let offset = put_payload(&mut image, 1, 16, payload);
        env.reader.add_segment(1, image.clone());
        env.put_segment(1, SegmentState::Sealed);
        env.put_entry(1, 1, 1, offset, payload.len() as u32);
        // `warm_segment` derives the block count from the real file size.
        std::fs::write(env.data_tmp.path().join("1.seg"), &image).unwrap();

        let cache = env.build().await;

        let loaded = cache.warm_segment(1).await.unwrap();
        assert_eq!(loaded, 2, "both data blocks should be warmed");
        assert_eq!(cache.stats().cache_fills, 2);

        // The warmed entry is served from the memory tier without touching the segment.
        let reads_before = env.reader.read_calls();
        let result = cache.read_entry(1, 1).await.unwrap();
        assert_eq!(result.payload, Bytes::from_static(payload));
        assert_eq!(result.source, ReadSource::MemoryCache);
        assert_eq!(env.reader.read_calls(), reads_before);

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_warm_segment_missing() {
        let env = TestEnv::new();
        let cache = env.build().await;

        let err = cache.warm_segment(9).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::SegmentNotFound(9)));

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_warm_segment_deleted() {
        let env = TestEnv::new();
        env.reader
            .add_segment_with_state(1, segment_image(1), SegmentState::Deleted);
        let cache = env.build().await;

        let err = cache.warm_segment(1).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::SegmentDeleted));

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_warm_segment_rejects_tiny_file() {
        let env = TestEnv::new();
        env.reader.add_segment(1, segment_image(1));
        // File is smaller than header + trailer.
        std::fs::write(env.data_tmp.path().join("1.seg"), [0u8; 64]).unwrap();

        let cache = env.build().await;
        let err = cache.warm_segment(1).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::InvalidArgument(_)));

        cache.close().await.unwrap();
    }

    // ===== Concurrency and lifecycle =====

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_reads_of_same_entry() {
        let env = TestEnv::new();
        let mut image = segment_image(1);
        let payload = b"concurrent payload";
        let offset = put_payload(&mut image, 0, 0, payload);
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        env.put_entry(1, 1, 1, offset, payload.len() as u32);

        let cache = Arc::new(env.build().await);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move { cache.read_entry(1, 1).await }));
        }

        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            assert_eq!(result.payload, Bytes::from_static(payload));
        }

        // Every read must be accounted for exactly once, whatever the tier it was
        // served from.
        let stats = cache.stats();
        assert_eq!(
            stats.memory_hits + stats.nvme_hits + stats.segment_hits + stats.s3_reads,
            8
        );

        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_is_idempotent() {
        let env = TestEnv::new();
        let cache = env.build().await;

        assert!(!cache.is_closed());
        cache.close().await.unwrap();
        assert!(cache.is_closed());
        // Closing again is a no-op and must not hang or fail.
        cache.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_after_close_fails() {
        let env = TestEnv::new();
        let cache = env.build().await;
        cache.close().await.unwrap();

        let err = cache.read_entry(1, 1).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::Closed));

        let err = cache.read_range(1, 1, 2).await.unwrap_err();
        assert!(matches!(err, ReadCacheError::Closed));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_foyer_stats_smoke() {
        let env = TestEnv::new();
        let mut image = segment_image(1);
        let offset = put_payload(&mut image, 0, 0, b"stats");
        env.reader.add_segment(1, image);
        env.put_segment(1, SegmentState::Sealed);
        env.put_entry(1, 1, 1, offset, 5);

        let cache = env.build().await;
        cache.read_entry(1, 1).await.unwrap();

        // Statistics handle is always available and reflects hybrid cache activity.
        let foyer_stats = cache.foyer_stats();
        assert!(Arc::strong_count(&foyer_stats) >= 1);

        cache.close().await.unwrap();
    }
}
