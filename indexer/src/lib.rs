//! fjall-backed index and rebuild-from-segments for bkxpro.
//!
//! The index is derived data; segments are the source of truth. This crate provides:
//! - Three fjall keyspaces: `entries`, `ledgers`, `segments`
//! - Rebuild index by scanning sealed segment footers plus the open segment
//! - Cleanup on segment deletion via one write batch bounded by the `segments` row

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use bkxpro_codec::{
    FooterView, SegmentView, TrailerWire, BLOCK_SIZE, RECORD_HEADER_SIZE, TRAILER_SIZE,
};
use bkxpro_segmenter::{AlignedBuffer, SegmentIo, WriterConfig};
use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{info, warn};

pub const ENTRIES_KEYSPACE: &str = "entries";
pub const LEDGERS_KEYSPACE: &str = "ledgers";
pub const SEGMENTS_KEYSPACE: &str = "segments";

/// Segment lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SegmentState {
    Open = 0,
    Sealed = 1,
    Uploading = 2,
    Uploaded = 3,
    Deleted = 4,
}

impl From<SegmentState> for u8 {
    fn from(s: SegmentState) -> Self {
        s as u8
    }
}

impl TryFrom<u8> for SegmentState {
    type Error = IndexError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SegmentState::Open),
            1 => Ok(SegmentState::Sealed),
            2 => Ok(SegmentState::Uploading),
            3 => Ok(SegmentState::Uploaded),
            4 => Ok(SegmentState::Deleted),
            _ => Err(IndexError::InvalidSegmentState(value)),
        }
    }
}

/// Ledger state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum LedgerState {
    Open = 0,
    Fenced = 1,
    Closed = 2,
}

impl From<LedgerState> for u8 {
    fn from(s: LedgerState) -> Self {
        s as u8
    }
}

impl TryFrom<u8> for LedgerState {
    type Error = IndexError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(LedgerState::Open),
            1 => Ok(LedgerState::Fenced),
            2 => Ok(LedgerState::Closed),
            _ => Err(IndexError::InvalidLedgerState(value)),
        }
    }
}

/// Ledger metadata stored in the `ledgers` keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerMeta {
    pub epoch: u64,
    pub lac: u64,
    pub state: LedgerState,
}

/// Segment metadata stored in the `segments` keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub state: SegmentState,
    pub s3_key: Option<String>,
    pub min_ledger: u64,
    pub max_ledger: u64,
    pub byte_size: u64,
    pub writer_epoch: u64,
}

/// Entry location stored in the `entries` keyspace.
/// Key: ledger (u64 BE) || entry (u64 BE)
/// Value: segment (u32 BE) || offset (u32 BE) || len (u32 BE)
///
/// `offset` is the absolute byte offset of the entry's **payload** in the segment:
/// the packed 21-byte record header is not part of it. The read path slices the
/// payload at this offset, so both rebuild paths translate the record-header
/// offsets stored in the footer/records by [`RECORD_HEADER_SIZE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryLocation {
    pub segment_id: u32,
    pub offset: u32,
    pub len: u32,
}

impl EntryLocation {
    pub fn encode(&self) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[0..4].copy_from_slice(&self.segment_id.to_be_bytes());
        buf[4..8].copy_from_slice(&self.offset.to_be_bytes());
        buf[8..12].copy_from_slice(&self.len.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 12 {
            return Err(IndexError::InvalidEntryLocationFormat(bytes.len()));
        }
        Ok(Self {
            segment_id: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            offset: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            len: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        })
    }
}

/// Encode a composite key for the `entries` keyspace: ledger BE || entry BE
fn encode_entry_key(ledger: u64, entry: u64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&ledger.to_be_bytes());
    buf[8..16].copy_from_slice(&entry.to_be_bytes());
    buf
}

/// Encode a key for the `ledgers` keyspace: ledger BE
fn encode_ledger_key(ledger: u64) -> [u8; 8] {
    ledger.to_be_bytes()
}

/// Encode a key for the `segments` keyspace: segment BE
fn encode_segment_key(segment_id: u64) -> [u8; 8] {
    segment_id.to_be_bytes()
}

/// Index configuration.
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Path to the fjall database directory.
    pub db_path: PathBuf,
    /// Maximum memory budget for fjall cache + write buffers (bytes).
    /// Example: ~128 MB.
    pub max_memory_bytes: usize,
    /// Maximum number of open files for fjall.
    pub max_open_files: usize,
    /// Sync interval for fjall journal (ms). Relaxed durability for derived data.
    pub journal_sync_interval_ms: u64,
    /// Whether to enable fjall's compaction.
    pub enable_compaction: bool,
    /// Compaction job threads.
    pub compaction_threads: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("/var/lib/bkxpro/index"),
            max_memory_bytes: 128 * 1024 * 1024, // 128 MB
            max_open_files: 256,
            journal_sync_interval_ms: 1000,
            enable_compaction: true,
            compaction_threads: 2,
        }
    }
}

/// Errors from the index operations.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    #[error("codec error: {0}")]
    Codec(#[from] bkxpro_codec::CodecError),

    #[error("segment writer error: {0}")]
    SegmentWriter(#[from] bkxpro_segmenter::WriterError<std::io::Error>),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("invalid segment state: {0}")]
    InvalidSegmentState(u8),

    #[error("invalid ledger state: {0}")]
    InvalidLedgerState(u8),

    #[error("invalid entry location format: expected 12 bytes, got {0}")]
    InvalidEntryLocationFormat(usize),

    #[error("segment not found: {0}")]
    SegmentNotFound(u64),

    #[error("ledger not found: {0}")]
    LedgerNotFound(u64),

    #[error("entry not found: ledger={0}, entry={1}")]
    EntryNotFound(u64, u64),

    #[error("duplicate entry: ledger={0}, entry={1}")]
    DuplicateEntry(u64, u64),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("index is poisoned")]
    Poisoned,
}

/// Result type for index operations.
pub type IndexResult<T> = Result<T, IndexError>;

/// The main index structure providing access to the three keyspaces.
pub struct Index {
    db: Database,
    entries: Arc<Keyspace>,
    ledgers: Arc<Keyspace>,
    segments: Arc<Keyspace>,
    _config: IndexConfig,
    poisoned: bool,
}

impl Index {
    /// Open or create the index database with the given configuration.
    pub fn open(config: IndexConfig) -> IndexResult<Self> {
        std::fs::create_dir_all(&config.db_path)?;

        let keyspace_opts =
            KeyspaceCreateOptions::default().max_memtable_size(config.max_memory_bytes as u64 / 4);

        let db = Database::builder(&config.db_path)
            .cache_size(config.max_memory_bytes as u64 / 2)
            .max_cached_files(Some(config.max_open_files))
            .worker_threads(config.compaction_threads)
            .manual_journal_persist(true)
            .open()?;

        let entries = Arc::new(db.keyspace(ENTRIES_KEYSPACE, || keyspace_opts.clone())?);
        let ledgers = Arc::new(db.keyspace(LEDGERS_KEYSPACE, || keyspace_opts.clone())?);
        let segments = Arc::new(db.keyspace(SEGMENTS_KEYSPACE, || keyspace_opts)?);

        Ok(Self {
            db,
            entries,
            ledgers,
            segments,
            _config: config,
            poisoned: false,
        })
    }

    /// Get a reference to the entries keyspace.
    pub fn entries(&self) -> &Keyspace {
        &self.entries
    }

    /// Get a reference to the ledgers keyspace.
    pub fn ledgers(&self) -> &Keyspace {
        &self.ledgers
    }

    /// Get a reference to the segments keyspace.
    pub fn segments(&self) -> &Keyspace {
        &self.segments
    }

    /// Check if the index is poisoned (fail-stop state).
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Poison the index (fail-stop).
    pub fn poison(&mut self) {
        self.poisoned = true;
    }

    /// Persist all pending writes to disk.
    pub fn persist(&self) -> IndexResult<()> {
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Create a new write batch.
    pub fn batch(&self) -> OwnedWriteBatch {
        self.db.batch()
    }

    // ===== Entry operations =====

    /// Insert or update an entry location. Uses a write batch for atomicity with other operations.
    pub fn put_entry(
        &self,
        batch: &mut OwnedWriteBatch,
        ledger: u64,
        entry: u64,
        location: EntryLocation,
    ) -> IndexResult<()> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_entry_key(ledger, entry);
        let value = location.encode();
        batch.insert(&self.entries, key, value);
        Ok(())
    }

    /// Get an entry location.
    pub fn get_entry(&self, ledger: u64, entry: u64) -> IndexResult<Option<EntryLocation>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_entry_key(ledger, entry);
        match self.entries.get(key)? {
            Some(bytes) => Ok(Some(EntryLocation::decode(bytes.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Remove an entry (used during segment deletion cleanup).
    pub fn remove_entry(
        &self,
        batch: &mut OwnedWriteBatch,
        ledger: u64,
        entry: u64,
    ) -> IndexResult<()> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_entry_key(ledger, entry);
        batch.remove(&self.entries, key);
        Ok(())
    }

    /// Iterate entries for a ledger range.
    pub fn scan_entries(
        &self,
        ledger: u64,
        start_entry: u64,
        end_entry: u64,
    ) -> IndexResult<Vec<(u64, u64, EntryLocation)>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let start_key = encode_entry_key(ledger, start_entry);
        let end_key = encode_entry_key(ledger, end_entry);

        let mut results = Vec::new();
        for guard in self.entries.range(start_key..=end_key) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 16 {
                continue;
            }
            let ledger = u64::from_be_bytes(key[0..8].try_into().unwrap());
            let entry = u64::from_be_bytes(key[8..16].try_into().unwrap());
            let location = EntryLocation::decode(value.as_ref())?;
            results.push((ledger, entry, location));
        }
        Ok(results)
    }

    // ===== Ledger operations =====

    /// Insert or update ledger metadata.
    pub fn put_ledger(
        &self,
        batch: &mut OwnedWriteBatch,
        ledger: u64,
        meta: LedgerMeta,
    ) -> IndexResult<()> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_ledger_key(ledger);
        let value = bincode::serialize(&meta)?;
        batch.insert(&self.ledgers, key, value);
        Ok(())
    }

    /// Get ledger metadata.
    pub fn get_ledger(&self, ledger: u64) -> IndexResult<Option<LedgerMeta>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_ledger_key(ledger);
        match self.ledgers.get(key)? {
            Some(bytes) => Ok(Some(bincode::deserialize(bytes.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Remove a ledger.
    pub fn remove_ledger(&self, batch: &mut OwnedWriteBatch, ledger: u64) -> IndexResult<()> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_ledger_key(ledger);
        batch.remove(&self.ledgers, key);
        Ok(())
    }

    /// Iterate all ledgers.
    pub fn iter_ledgers(&self) -> IndexResult<Vec<(u64, LedgerMeta)>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let mut results = Vec::new();
        for guard in self.ledgers.iter() {
            let (key, value) = guard.into_inner()?;
            if key.len() != 8 {
                continue;
            }
            let ledger = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            let meta: LedgerMeta = bincode::deserialize(value.as_ref())?;
            results.push((ledger, meta));
        }
        Ok(results)
    }

    // ===== Segment operations =====

    /// Insert or update segment metadata.
    pub fn put_segment(
        &self,
        batch: &mut OwnedWriteBatch,
        segment_id: u64,
        meta: SegmentMeta,
    ) -> IndexResult<()> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_segment_key(segment_id);
        let value = bincode::serialize(&meta)?;
        batch.insert(&self.segments, key, value);
        Ok(())
    }

    /// Get segment metadata.
    pub fn get_segment(&self, segment_id: u64) -> IndexResult<Option<SegmentMeta>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_segment_key(segment_id);
        match self.segments.get(key)? {
            Some(bytes) => Ok(Some(bincode::deserialize(bytes.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Update segment state (atomic with other operations in a batch).
    pub fn update_segment_state(
        &self,
        batch: &mut OwnedWriteBatch,
        segment_id: u64,
        state: SegmentState,
    ) -> IndexResult<()> {
        if let Some(mut meta) = self.get_segment(segment_id)? {
            meta.state = state;
            self.put_segment(batch, segment_id, meta)?;
        }
        Ok(())
    }

    /// Remove a segment (used during cleanup after S3 upload + manifest commit).
    pub fn remove_segment(&self, batch: &mut OwnedWriteBatch, segment_id: u64) -> IndexResult<()> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let key = encode_segment_key(segment_id);
        batch.remove(&self.segments, key);
        Ok(())
    }

    /// Iterate all segments.
    pub fn iter_segments(&self) -> IndexResult<Vec<(u64, SegmentMeta)>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let mut results = Vec::new();
        for guard in self.segments.iter() {
            let (key, value) = guard.into_inner()?;
            if key.len() != 8 {
                continue;
            }
            let segment_id = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            let meta: SegmentMeta = bincode::deserialize(value.as_ref())?;
            results.push((segment_id, meta));
        }
        Ok(results)
    }

    /// Get segments in a state.
    pub fn segments_by_state(&self, state: SegmentState) -> IndexResult<Vec<(u64, SegmentMeta)>> {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }
        let mut results = Vec::new();
        for (id, meta) in self.iter_segments()? {
            if meta.state == state {
                results.push((id, meta));
            }
        }
        Ok(results)
    }

    // ===== Rebuild from segments =====

    /// Rebuild the index by scanning all segment files in the data directory.
    /// This scans sealed segment footers and the open segment.
    pub fn rebuild_from_segments<I: SegmentIo>(
        &mut self,
        io: &mut I,
        data_dir: &Path,
        writer_config: &WriterConfig,
    ) -> IndexResult<RebuildStats>
    where
        IndexError: From<I::Error>,
    {
        self.rebuild_from_segments_with_options(io, data_dir, writer_config, false)
    }

    /// Rebuild with explicit permission to skip failed segments. Each failed
    /// segment contributes no rows; without `allow_partial`, nothing is committed
    /// when any segment fails.
    pub fn rebuild_from_segments_with_options<I: SegmentIo>(
        &mut self,
        io: &mut I,
        data_dir: &Path,
        writer_config: &WriterConfig,
        allow_partial: bool,
    ) -> IndexResult<RebuildStats>
    where
        IndexError: From<I::Error>,
    {
        if self.poisoned {
            return Err(IndexError::Poisoned);
        }

        info!("Starting index rebuild from segments in {:?}", data_dir);

        let mut stats = RebuildStats::default();
        let mut first_error = None;
        let mut batch = self.db.batch();

        // Find all segment files
        let segment_files = Self::discover_segment_files(data_dir)?;
        info!("Found {} segment files", segment_files.len());

        // Process each segment file
        for (segment_id, path) in segment_files {
            let mut rows = Vec::new();
            match Self::process_segment_file(
                io,
                &path,
                segment_id,
                &mut rows,
                &self.entries,
                &self.ledgers,
                &self.segments,
                writer_config,
            ) {
                Ok(segment_stats) => {
                    for (keyspace, key, value) in rows {
                        batch.insert(&keyspace, key, value);
                    }
                    stats.segments_processed += 1;
                    stats.entries_indexed += segment_stats.entries_indexed;
                    stats.ledgers_updated += segment_stats.ledgers_updated;
                }
                Err(e) => {
                    warn!("Failed to process segment {}: {}", segment_id, e);
                    stats.segments_failed += 1;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        if !allow_partial {
            if let Some(error) = first_error {
                return Err(error);
            }
        }

        // Commit only rows from fully processed segments.
        batch.commit()?;
        self.db.persist(PersistMode::SyncAll)?;

        info!("Index rebuild complete: {} segments processed, {} entries indexed, {} ledgers updated, {} failed",
              stats.segments_processed, stats.entries_indexed, stats.ledgers_updated, stats.segments_failed);
        Ok(stats)
    }

    /// Discover all segment files in the data directory.
    fn discover_segment_files(data_dir: &Path) -> IndexResult<Vec<(u64, PathBuf)>> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(stripped) = name.strip_suffix(".seg") {
                    if let Ok(segment_id) = stripped.parse::<u64>() {
                        files.push((segment_id, path));
                    }
                }
            }
        }
        files.sort_by_key(|(id, _)| *id);
        Ok(files)
    }

    /// Read an arbitrary range through one aligned request, then discard padding.
    fn read_range<I: SegmentIo>(
        io: &mut I,
        file: &I::File,
        offset: u64,
        len: usize,
    ) -> IndexResult<Vec<u8>>
    where
        IndexError: From<I::Error>,
    {
        let aligned_offset = offset & !(BLOCK_SIZE as u64 - 1);
        let start = (offset - aligned_offset) as usize;
        let end = start
            .checked_add(len)
            .ok_or_else(|| IndexError::InvalidArgument("read range overflow".to_string()))?;
        let aligned_len = end.checked_add(BLOCK_SIZE - 1).ok_or_else(|| {
            IndexError::InvalidArgument("aligned read length overflow".to_string())
        })? & !(BLOCK_SIZE - 1);
        let mut buffer = AlignedBuffer::new_zeroed(aligned_len)?;
        io.read_aligned(file, aligned_offset, &mut buffer)?;
        Ok(buffer[start..end].to_vec())
    }

    /// Process a single segment file: read footer, index entries, update ledger/segment metadata.
    fn process_segment_file<I: SegmentIo>(
        io: &mut I,
        path: &Path,
        segment_id: u64,
        rows: &mut Vec<(Keyspace, Vec<u8>, Vec<u8>)>,
        entries: &Keyspace,
        ledgers: &Keyspace,
        segments: &Keyspace,
        _writer_config: &WriterConfig,
    ) -> IndexResult<SegmentRebuildStats>
    where
        IndexError: From<I::Error>,
    {
        let mut stats = SegmentRebuildStats::default();

        // Open read-only; rebuild must never resize source-of-truth segments.
        let file = io.open_segment_for_read(path)?;

        // Get the file size (valid length) using the SegmentIo trait
        let file_size = io.file_size(&file)?;
        if file_size < BLOCK_SIZE as u64 {
            return Err(IndexError::InvalidArgument(format!(
                "Segment file too small: {} bytes",
                file_size
            )));
        }

        // Read the last block to check for trailer
        let trailer_offset = file_size - BLOCK_SIZE as u64;
        let trailer_block = Self::read_range(io, &file, trailer_offset, BLOCK_SIZE)?;

        // Trailer is at the end of the block
        let trailer_start_in_block = BLOCK_SIZE - TRAILER_SIZE;
        let trailer_bytes = &trailer_block[trailer_start_in_block..];

        // Try to parse trailer to determine if segment is sealed
        let is_sealed = TrailerWire::parse(trailer_bytes).is_ok();

        if is_sealed {
            // Process as sealed segment with footer
            Self::process_sealed_segment(
                io, &file, file_size, segment_id, rows, entries, ledgers, segments, &mut stats,
            )?;
        } else {
            // Process as open segment using recovery
            Self::process_open_segment(
                io, &file, file_size, segment_id, rows, entries, ledgers, segments, &mut stats,
            )?;
        }

        Ok(stats)
    }

    /// Process a sealed segment with valid footer and trailer.
    fn process_sealed_segment<I: SegmentIo>(
        io: &mut I,
        file: &I::File,
        file_size: u64,
        segment_id: u64,
        rows: &mut Vec<(Keyspace, Vec<u8>, Vec<u8>)>,
        entries: &Keyspace,
        ledgers: &Keyspace,
        segments: &Keyspace,
        stats: &mut SegmentRebuildStats,
    ) -> IndexResult<()>
    where
        IndexError: From<I::Error>,
    {
        // Read the last block which contains the trailer
        let trailer_offset = file_size - BLOCK_SIZE as u64;
        let trailer_block = Self::read_range(io, file, trailer_offset, BLOCK_SIZE)?;

        // Trailer is at the end of the block
        let trailer_start_in_block = BLOCK_SIZE - TRAILER_SIZE;
        let trailer_bytes = &trailer_block[trailer_start_in_block..];

        // Parse trailer
        let trailer = TrailerWire::parse(trailer_bytes)?;
        let footer_start = trailer.footer_start;
        let footer_len = trailer.footer_len as usize;
        let footer_end = footer_start
            .checked_add(footer_len as u64)
            .ok_or_else(|| IndexError::InvalidArgument("footer range overflow".to_string()))?;
        if footer_end > file_size - TRAILER_SIZE as u64 {
            return Err(IndexError::InvalidArgument(
                "footer extends past trailer".to_string(),
            ));
        }
        let footer_buf = Self::read_range(io, file, footer_start, footer_len)?;

        let footer = FooterView::parse(Bytes::from(footer_buf))?;

        // Index entries from footer
        let mut ledger_entries: HashMap<u64, (u64, u64)> = HashMap::new(); // ledger -> (min_entry, max_entry)
        let mut min_ledger = u64::MAX;
        let mut max_ledger = 0u64;

        for entry in footer.index_entries() {
            let location = EntryLocation {
                segment_id: entry_u32(segment_id, "segment id")?,
                // The footer stores the physical record-header offset; readers want
                // the payload offset.
                offset: payload_offset(entry.offset)?,
                len: entry.len,
            };
            let key = encode_entry_key(entry.ledger, entry.entry);
            let value = location.encode();
            rows.push((entries.clone(), key.to_vec(), value.to_vec()));
            stats.entries_indexed += 1;

            min_ledger = min_ledger.min(entry.ledger);
            max_ledger = max_ledger.max(entry.ledger);

            let (min_e, max_e) = ledger_entries
                .entry(entry.ledger)
                .or_insert((entry.entry, entry.entry));
            *min_e = (*min_e).min(entry.entry);
            *max_e = (*max_e).max(entry.entry);
        }

        // Update ledger metadata from footer summaries
        for summary in footer.ledger_summaries() {
            let meta = LedgerMeta {
                epoch: 0, // Will be updated from header or segment metadata
                lac: summary.max_entry,
                state: LedgerState::Open,
            };
            let key = encode_ledger_key(summary.ledger);
            let value = bincode::serialize(&meta)?;
            rows.push((ledgers.clone(), key.to_vec(), value.to_vec()));
            stats.ledgers_updated += 1;
        }

        // Update segment metadata
        let segment_meta = SegmentMeta {
            state: SegmentState::Sealed,
            s3_key: None,
            min_ledger,
            max_ledger,
            byte_size: file_size,
            writer_epoch: 0, // TODO: read from header
        };
        let key = encode_segment_key(segment_id);
        let value = bincode::serialize(&segment_meta)?;
        rows.push((segments.clone(), key.to_vec(), value.to_vec()));

        stats.segments_processed = 1;
        Ok(())
    }

    /// Process an open segment using the recovery scanner.
    fn process_open_segment<I: SegmentIo>(
        io: &mut I,
        file: &I::File,
        file_size: u64,
        segment_id: u64,
        rows: &mut Vec<(Keyspace, Vec<u8>, Vec<u8>)>,
        entries: &Keyspace,
        ledgers: &Keyspace,
        segments: &Keyspace,
        stats: &mut SegmentRebuildStats,
    ) -> IndexResult<()>
    where
        IndexError: From<I::Error>,
    {
        // Read the entire segment for recovery
        let segment_len = usize::try_from(file_size).map_err(|_| {
            IndexError::InvalidArgument("segment size exceeds address space".to_string())
        })?;
        let segment_buf = Self::read_range(io, file, 0, segment_len)?;

        // Use recovery scanner to get valid records
        let recovery_result = SegmentView::recover_open(Bytes::from(segment_buf));
        let view = recovery_result.view.ok_or_else(|| {
            IndexError::InvalidArgument("Failed to recover open segment".to_string())
        })?;

        // Index entries from recovered records
        let mut ledger_entries: HashMap<u64, (u64, u64)> = HashMap::new(); // ledger -> (min_entry, max_entry)
        let mut min_ledger = u64::MAX;
        let mut max_ledger = 0u64;

        for record in view.records() {
            let record = record?;
            let location = EntryLocation {
                segment_id: entry_u32(segment_id, "segment id")?,
                // `RecordRef::offset` is the record header; readers want the payload.
                offset: payload_offset(record.offset)?,
                len: record.len,
            };
            let key = encode_entry_key(record.ledger, record.entry);
            let value = location.encode();
            rows.push((entries.clone(), key.to_vec(), value.to_vec()));
            stats.entries_indexed += 1;

            min_ledger = min_ledger.min(record.ledger);
            max_ledger = max_ledger.max(record.ledger);

            let (min_e, max_e) = ledger_entries
                .entry(record.ledger)
                .or_insert((record.entry, record.entry));
            *min_e = (*min_e).min(record.entry);
            *max_e = (*max_e).max(record.entry);
        }

        // Update ledger metadata from recovered records
        for (ledger, (_, max_entry)) in ledger_entries {
            let meta = LedgerMeta {
                epoch: 0,
                lac: max_entry,
                state: LedgerState::Open,
            };
            let key = encode_ledger_key(ledger);
            let value = bincode::serialize(&meta)?;
            rows.push((ledgers.clone(), key.to_vec(), value.to_vec()));
            stats.ledgers_updated += 1;
        }

        // Update segment metadata
        let segment_meta = SegmentMeta {
            state: SegmentState::Open,
            s3_key: None,
            min_ledger,
            max_ledger,
            byte_size: file_size,
            writer_epoch: 0, // TODO: read from header
        };
        let key = encode_segment_key(segment_id);
        let value = bincode::serialize(&segment_meta)?;
        rows.push((segments.clone(), key.to_vec(), value.to_vec()));

        stats.segments_processed = 1;
        Ok(())
    }
}

fn entry_u32(value: u64, field: &str) -> IndexResult<u32> {
    u32::try_from(value)
        .map_err(|_| IndexError::InvalidArgument(format!("{field} {value} exceeds u32")))
}

fn payload_offset(record_offset: u64) -> IndexResult<u32> {
    let offset = record_offset
        .checked_add(RECORD_HEADER_SIZE as u64)
        .ok_or_else(|| IndexError::InvalidArgument("payload offset overflow".to_string()))?;
    entry_u32(offset, "payload offset")
}

/// Statistics from a full index rebuild.
#[derive(Debug, Default, Clone)]
pub struct RebuildStats {
    pub segments_processed: usize,
    pub segments_failed: usize,
    pub entries_indexed: usize,
    pub ledgers_updated: usize,
}

/// Statistics from processing a single segment during rebuild.
#[derive(Debug, Default, Clone)]
struct SegmentRebuildStats {
    segments_processed: usize,
    entries_indexed: usize,
    ledgers_updated: usize,
}

/// Batch delete entries for a segment (used when segment is deleted after S3 upload + manifest commit).
/// This is done in a single write batch bounded by the segments row.
pub fn delete_segment_entries(index: &Index, segment_id: u64) -> IndexResult<()> {
    if index.is_poisoned() {
        return Err(IndexError::Poisoned);
    }

    let entry_segment_id = entry_u32(segment_id, "segment id")?;

    // First, get the segment metadata to know the ledger range
    let segment_meta = index
        .get_segment(segment_id)?
        .ok_or(IndexError::SegmentNotFound(segment_id))?;

    let mut batch = index.db.batch();

    // Delete all entries for this segment's ledger range
    // We need to scan entries and delete those belonging to this segment
    // Since entries are keyed by (ledger, entry), we scan each ledger in range
    if segment_meta.min_ledger <= segment_meta.max_ledger {
        let start = encode_entry_key(segment_meta.min_ledger, 0);
        let end = encode_entry_key(segment_meta.max_ledger, u64::MAX);
        for guard in index.entries.range(start..=end) {
            let (key, value) = guard.into_inner()?;
            if EntryLocation::decode(value.as_ref())?.segment_id == entry_segment_id {
                batch.remove(&index.entries, key);
            }
        }
    }

    // Remove the segment itself
    index.remove_segment(&mut batch, segment_id)?;

    batch.commit()?;
    index.db.persist(PersistMode::SyncAll)?;

    Ok(())
}

/// Batch delete entries for a ledger (used when ledger is fenced/closed).
pub fn delete_ledger_entries(index: &Index, ledger: u64) -> IndexResult<()> {
    if index.is_poisoned() {
        return Err(IndexError::Poisoned);
    }

    let mut batch = index.db.batch();
    let entries = index.scan_entries(ledger, 0, u64::MAX)?;
    for (l, e, _) in entries {
        index.remove_entry(&mut batch, l, e)?;
    }
    // Also remove the ledger metadata
    index.remove_ledger(&mut batch, ledger)?;
    batch.commit()?;
    index.db.persist(PersistMode::SyncAll)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bkxpro_segmenter::{SegmentIo, WriteOp};
    use bytes::Bytes;
    use std::{
        collections::HashMap,
        io,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    };
    use tempfile::TempDir;

    #[derive(Clone)]
    struct MemoryFile(Arc<Mutex<Vec<u8>>>);

    #[derive(Default, Clone)]
    struct MemoryIo {
        files: HashMap<PathBuf, MemoryFile>,
        logical_sizes: HashMap<PathBuf, u64>,
        commit_count: usize,
        directory_sync_count: usize,
        reads: Vec<(u64, usize)>,
    }

    impl SegmentIo for MemoryIo {
        type File = MemoryFile;
        type Error = io::Error;

        fn acquire_preallocated_segment(
            &mut self,
            path: &Path,
            capacity: usize,
        ) -> Result<bkxpro_segmenter::AcquiredSegment<Self::File>, Self::Error> {
            let (file, changed) = match self.files.get(path) {
                Some(file) => (file.clone(), false),
                None => {
                    let file = MemoryFile(Arc::new(Mutex::new(vec![0; capacity])));
                    self.files.insert(path.to_path_buf(), file.clone());
                    (file, true)
                }
            };
            // Only resize when creating a new file (changed == true)
            // For existing files, preserve the data
            if changed {
                file.0.lock().unwrap().resize(capacity, 0);
                self.logical_sizes.insert(path.to_path_buf(), 0);
            }
            Ok(bkxpro_segmenter::AcquiredSegment {
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
            self.commit_count += 1;
            // Update logical size based on the highest write
            for write in writes {
                let end = write.offset() + write.bytes().len() as u64;
                if let Some(path) = self
                    .files
                    .iter()
                    .find(|(_, f)| Arc::ptr_eq(&f.0, &file.0))
                    .map(|(p, _)| p.clone())
                {
                    self.logical_sizes
                        .entry(path)
                        .and_modify(|s| {
                            *s = (*s).max(end);
                        })
                        .or_insert(end);
                }
            }
            self.write_batch(file, writes)?;
            self.sync_data(file)
        }

        fn read_aligned(
            &mut self,
            file: &Self::File,
            offset: u64,
            buffer: &mut [u8],
        ) -> Result<(), Self::Error> {
            self.reads.push((offset, buffer.len()));
            let bytes = file.0.lock().unwrap();
            let start = offset as usize;
            let end = start
                .checked_add(buffer.len())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read overflow"))?;
            if offset % BLOCK_SIZE as u64 != 0
                || buffer.len() % BLOCK_SIZE != 0
                || buffer.as_ptr() as usize % BLOCK_SIZE != 0
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
            // Find the path for this file and return its logical size
            for (path, f) in &self.files {
                if Arc::ptr_eq(&f.0, &file.0) {
                    return Ok(self.logical_sizes.get(path).copied().unwrap_or(0));
                }
            }
            // Fallback to full capacity if not found
            let bytes = file.0.lock().unwrap();
            Ok(bytes.len() as u64)
        }
    }

    #[test]
    fn test_entry_location_encode_decode() {
        let loc = EntryLocation {
            segment_id: 42,
            offset: 1024,
            len: 512,
        };
        let encoded = loc.encode();
        let decoded = EntryLocation::decode(&encoded).unwrap();
        assert_eq!(loc, decoded);
    }

    #[test]
    fn test_encode_entry_key() {
        let key = encode_entry_key(1, 2);
        assert_eq!(key[0..8], 1u64.to_be_bytes());
        assert_eq!(key[8..16], 2u64.to_be_bytes());
    }

    #[test]
    fn test_index_open_and_basic_ops() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        let index = Index::open(config).unwrap();

        // Test put/get entry
        let loc = EntryLocation {
            segment_id: 1,
            offset: 4096,
            len: 100,
        };
        let mut batch = index.db.batch();
        index.put_entry(&mut batch, 10, 20, loc).unwrap();
        batch.commit().unwrap();

        let retrieved = index.get_entry(10, 20).unwrap();
        assert_eq!(retrieved, Some(loc));

        // Test put/get ledger
        let ledger_meta = LedgerMeta {
            epoch: 1,
            lac: 100,
            state: LedgerState::Open,
        };
        let mut batch = index.db.batch();
        index
            .put_ledger(&mut batch, 10, ledger_meta.clone())
            .unwrap();
        batch.commit().unwrap();

        let retrieved = index.get_ledger(10).unwrap();
        assert_eq!(retrieved, Some(ledger_meta));

        // Test put/get segment
        let segment_meta = SegmentMeta {
            state: SegmentState::Sealed,
            s3_key: Some("s3://bucket/key".to_string()),
            min_ledger: 10,
            max_ledger: 20,
            byte_size: 1024 * 1024,
            writer_epoch: 1,
        };
        let mut batch = index.db.batch();
        index
            .put_segment(&mut batch, 1, segment_meta.clone())
            .unwrap();
        batch.commit().unwrap();

        let retrieved = index.get_segment(1).unwrap();
        assert_eq!(retrieved, Some(segment_meta));
    }

    #[test]
    fn test_scan_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        let index = Index::open(config).unwrap();

        let mut batch = index.db.batch();
        for i in 1..=10u64 {
            let loc = EntryLocation {
                segment_id: 1,
                offset: (i * 100) as u32,
                len: 50,
            };
            index.put_entry(&mut batch, 1, i, loc).unwrap();
        }
        batch.commit().unwrap();

        let scanned = index.scan_entries(1, 3, 7).unwrap();
        assert_eq!(scanned.len(), 5);
        assert_eq!(scanned[0].1, 3);
        assert_eq!(scanned[4].1, 7);
    }

    #[test]
    fn test_iter_ledgers() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        let index = Index::open(config).unwrap();

        let mut batch = index.db.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 10,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        index
            .put_ledger(
                &mut batch,
                2,
                LedgerMeta {
                    epoch: 1,
                    lac: 20,
                    state: LedgerState::Fenced,
                },
            )
            .unwrap();
        index
            .put_ledger(
                &mut batch,
                3,
                LedgerMeta {
                    epoch: 1,
                    lac: 30,
                    state: LedgerState::Closed,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        let ledgers = index.iter_ledgers().unwrap();
        assert_eq!(ledgers.len(), 3);
    }

    #[test]
    fn test_segments_by_state() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        let index = Index::open(config).unwrap();

        let mut batch = index.db.batch();
        index
            .put_segment(
                &mut batch,
                1,
                SegmentMeta {
                    state: SegmentState::Open,
                    s3_key: None,
                    min_ledger: 1,
                    max_ledger: 10,
                    byte_size: 100,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        index
            .put_segment(
                &mut batch,
                2,
                SegmentMeta {
                    state: SegmentState::Sealed,
                    s3_key: None,
                    min_ledger: 11,
                    max_ledger: 20,
                    byte_size: 200,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        index
            .put_segment(
                &mut batch,
                3,
                SegmentMeta {
                    state: SegmentState::Sealed,
                    s3_key: None,
                    min_ledger: 21,
                    max_ledger: 30,
                    byte_size: 300,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        index
            .put_segment(
                &mut batch,
                4,
                SegmentMeta {
                    state: SegmentState::Uploaded,
                    s3_key: Some("s3://b/k".to_string()),
                    min_ledger: 31,
                    max_ledger: 40,
                    byte_size: 400,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        let sealed = index.segments_by_state(SegmentState::Sealed).unwrap();
        assert_eq!(sealed.len(), 2);
        assert!(sealed.iter().all(|(_, m)| m.state == SegmentState::Sealed));
    }

    #[test]
    fn test_delete_segment_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        let index = Index::open(config).unwrap();

        let mut batch = index.db.batch();
        index
            .put_segment(
                &mut batch,
                1,
                SegmentMeta {
                    state: SegmentState::Sealed,
                    s3_key: Some("s3://b/k".to_string()),
                    min_ledger: 1,
                    max_ledger: 5,
                    byte_size: 1000,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        for i in 1..=5 {
            index
                .put_entry(
                    &mut batch,
                    1,
                    i,
                    EntryLocation {
                        segment_id: 1,
                        offset: i as u32 * 100,
                        len: 50,
                    },
                )
                .unwrap();
        }
        batch.commit().unwrap();

        // Verify entries exist
        assert!(index.get_entry(1, 1).unwrap().is_some());
        assert!(index.get_entry(1, 5).unwrap().is_some());

        // Delete segment entries
        delete_segment_entries(&index, 1).unwrap();

        // Verify entries are gone
        assert!(index.get_entry(1, 1).unwrap().is_none());
        assert!(index.get_entry(1, 5).unwrap().is_none());

        // Verify segment is gone
        assert!(index.get_segment(1).unwrap().is_none());
    }

    #[test]
    fn test_delete_ledger_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        let index = Index::open(config).unwrap();

        let mut batch = index.db.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 10,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        for i in 1..=10 {
            index
                .put_entry(
                    &mut batch,
                    1,
                    i,
                    EntryLocation {
                        segment_id: 1,
                        offset: i as u32 * 100,
                        len: 50,
                    },
                )
                .unwrap();
        }
        batch.commit().unwrap();

        // Verify entries exist
        assert!(index.get_entry(1, 1).unwrap().is_some());
        assert!(index.get_entry(1, 10).unwrap().is_some());

        // Delete ledger entries
        delete_ledger_entries(&index, 1).unwrap();

        // Verify entries are gone
        assert!(index.get_entry(1, 1).unwrap().is_none());
        assert!(index.get_entry(1, 10).unwrap().is_none());

        // Verify ledger is gone
        assert!(index.get_ledger(1).unwrap().is_none());
    }

    // ===== Comprehensive Tests =====

    #[test]
    fn test_batch_atomicity() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        // Single batch with multiple operations
        let mut batch = index.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 5,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        index
            .put_entry(
                &mut batch,
                1,
                1,
                EntryLocation {
                    segment_id: 1,
                    offset: 100,
                    len: 50,
                },
            )
            .unwrap();
        index
            .put_entry(
                &mut batch,
                1,
                2,
                EntryLocation {
                    segment_id: 1,
                    offset: 200,
                    len: 50,
                },
            )
            .unwrap();
        index
            .put_segment(
                &mut batch,
                1,
                SegmentMeta {
                    state: SegmentState::Sealed,
                    s3_key: None,
                    min_ledger: 1,
                    max_ledger: 1,
                    byte_size: 1000,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        assert_eq!(index.get_ledger(1).unwrap().unwrap().lac, 5);
        assert!(index.get_entry(1, 1).unwrap().is_some());
        assert!(index.get_entry(1, 2).unwrap().is_some());
        assert!(index.get_segment(1).unwrap().is_some());
    }

    #[test]
    fn test_batch_rollback_on_error() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        // Put initial data
        let mut batch = index.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 5,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        // Try to create a batch but don't commit - should not persist
        let _batch = index.batch();
        // Don't commit - data should not be visible
        assert!(index.get_ledger(1).unwrap().is_some());
    }

    #[test]
    fn test_concurrent_batch_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        // Multiple sequential batches
        for i in 1..=5 {
            let mut batch = index.batch();
            index
                .put_ledger(
                    &mut batch,
                    i,
                    LedgerMeta {
                        epoch: 1,
                        lac: i * 10,
                        state: LedgerState::Open,
                    },
                )
                .unwrap();
            index
                .put_segment(
                    &mut batch,
                    i,
                    SegmentMeta {
                        state: SegmentState::Sealed,
                        s3_key: None,
                        min_ledger: i,
                        max_ledger: i,
                        byte_size: 1000,
                        writer_epoch: 1,
                    },
                )
                .unwrap();
            batch.commit().unwrap();
        }

        for i in 1..=5 {
            assert!(index.get_ledger(i).unwrap().is_some());
            assert!(index.get_segment(i).unwrap().is_some());
        }
    }

    #[test]
    fn test_large_number_of_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        const NUM_ENTRIES: usize = 10000;
        let mut batch = index.batch();
        for i in 1..=NUM_ENTRIES {
            index
                .put_entry(
                    &mut batch,
                    1,
                    i as u64,
                    EntryLocation {
                        segment_id: 1,
                        offset: i as u32 * 100,
                        len: 50,
                    },
                )
                .unwrap();
        }
        batch.commit().unwrap();

        // Verify all entries
        for i in 1..=NUM_ENTRIES {
            let entry = index.get_entry(1, i as u64).unwrap();
            assert!(entry.is_some());
            assert_eq!(entry.unwrap().offset, (i as u32 * 100));
        }

        // Test scan range
        let scanned = index.scan_entries(1, 5000, 5100).unwrap();
        assert_eq!(scanned.len(), 101);
    }

    #[test]
    fn test_multiple_ledgers_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        // Ledger 1
        for i in 1..=100 {
            index
                .put_entry(
                    &mut batch,
                    1,
                    i,
                    EntryLocation {
                        segment_id: 1,
                        offset: i as u32 * 100,
                        len: 50,
                    },
                )
                .unwrap();
        }
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 100,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        // Ledger 2
        for i in 1..=50 {
            index
                .put_entry(
                    &mut batch,
                    2,
                    i,
                    EntryLocation {
                        segment_id: 2,
                        offset: i as u32 * 200,
                        len: 100,
                    },
                )
                .unwrap();
        }
        index
            .put_ledger(
                &mut batch,
                2,
                LedgerMeta {
                    epoch: 2,
                    lac: 50,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        // Verify isolation
        let ledger1_entries = index.scan_entries(1, 1, 100).unwrap();
        assert_eq!(ledger1_entries.len(), 100);
        let ledger2_entries = index.scan_entries(2, 1, 50).unwrap();
        assert_eq!(ledger2_entries.len(), 50);

        // Scan ledger 1 should not return ledger 2 entries
        let cross_scan = index.scan_entries(1, 1, 200).unwrap();
        assert_eq!(cross_scan.len(), 100);
    }

    #[test]
    fn test_segment_state_transitions() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        index
            .put_segment(
                &mut batch,
                1,
                SegmentMeta {
                    state: SegmentState::Open,
                    s3_key: None,
                    min_ledger: 1,
                    max_ledger: 10,
                    byte_size: 1000,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        assert_eq!(
            index.get_segment(1).unwrap().unwrap().state,
            SegmentState::Open
        );

        // Transition to Sealed
        let mut batch = index.batch();
        index
            .update_segment_state(&mut batch, 1, SegmentState::Sealed)
            .unwrap();
        batch.commit().unwrap();
        assert_eq!(
            index.get_segment(1).unwrap().unwrap().state,
            SegmentState::Sealed
        );

        // Transition to Uploading
        let mut batch = index.batch();
        index
            .update_segment_state(&mut batch, 1, SegmentState::Uploading)
            .unwrap();
        batch.commit().unwrap();
        assert_eq!(
            index.get_segment(1).unwrap().unwrap().state,
            SegmentState::Uploading
        );

        // Transition to Uploaded
        let mut batch = index.batch();
        index
            .update_segment_state(&mut batch, 1, SegmentState::Uploaded)
            .unwrap();
        batch.commit().unwrap();
        assert_eq!(
            index.get_segment(1).unwrap().unwrap().state,
            SegmentState::Uploaded
        );

        // Transition to Deleted
        let mut batch = index.batch();
        index
            .update_segment_state(&mut batch, 1, SegmentState::Deleted)
            .unwrap();
        batch.commit().unwrap();
        assert_eq!(
            index.get_segment(1).unwrap().unwrap().state,
            SegmentState::Deleted
        );
    }

    #[test]
    fn test_ledger_state_transitions() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 100,
                    state: LedgerState::Open,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        assert_eq!(
            index.get_ledger(1).unwrap().unwrap().state,
            LedgerState::Open
        );

        // Fence the ledger
        let mut batch = index.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 2,
                    lac: 100,
                    state: LedgerState::Fenced,
                },
            )
            .unwrap();
        batch.commit().unwrap();
        assert_eq!(
            index.get_ledger(1).unwrap().unwrap().state,
            LedgerState::Fenced
        );

        // Close the ledger
        let mut batch = index.batch();
        index
            .put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 2,
                    lac: 100,
                    state: LedgerState::Closed,
                },
            )
            .unwrap();
        batch.commit().unwrap();
        assert_eq!(
            index.get_ledger(1).unwrap().unwrap().state,
            LedgerState::Closed
        );
    }

    #[test]
    fn test_persist_and_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };

        // Write data
        {
            let index = Index::open(config.clone()).unwrap();
            let mut batch = index.batch();
            index
                .put_ledger(
                    &mut batch,
                    1,
                    LedgerMeta {
                        epoch: 1,
                        lac: 50,
                        state: LedgerState::Open,
                    },
                )
                .unwrap();
            for i in 1..=50 {
                index
                    .put_entry(
                        &mut batch,
                        1,
                        i,
                        EntryLocation {
                            segment_id: 1,
                            offset: i as u32 * 100,
                            len: 50,
                        },
                    )
                    .unwrap();
            }
            index
                .put_segment(
                    &mut batch,
                    1,
                    SegmentMeta {
                        state: SegmentState::Sealed,
                        s3_key: None,
                        min_ledger: 1,
                        max_ledger: 1,
                        byte_size: 5000,
                        writer_epoch: 1,
                    },
                )
                .unwrap();
            batch.commit().unwrap();
            index.persist().unwrap();
        }

        // Reopen and verify
        {
            let index = Index::open(config).unwrap();
            assert_eq!(index.get_ledger(1).unwrap().unwrap().lac, 50);
            assert!(index.get_entry(1, 25).unwrap().is_some());
            assert_eq!(
                index.get_segment(1).unwrap().unwrap().state,
                SegmentState::Sealed
            );
        }
    }

    #[test]
    fn test_poisoned_index() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let mut index = Index::open(config).unwrap();

        // Poison the index
        index.poison();
        assert!(index.is_poisoned());

        // All operations should fail
        let mut batch = index.batch();
        assert!(matches!(
            index.put_ledger(
                &mut batch,
                1,
                LedgerMeta {
                    epoch: 1,
                    lac: 0,
                    state: LedgerState::Open
                }
            ),
            Err(IndexError::Poisoned)
        ));
        assert!(matches!(index.get_ledger(1), Err(IndexError::Poisoned)));
        assert!(matches!(
            index.put_entry(
                &mut batch,
                1,
                1,
                EntryLocation {
                    segment_id: 1,
                    offset: 100,
                    len: 50
                }
            ),
            Err(IndexError::Poisoned)
        ));
        assert!(matches!(index.get_entry(1, 1), Err(IndexError::Poisoned)));
    }

    fn rebuild_image(segment_id: u64) -> Bytes {
        let mut encoder = bkxpro_codec::SegmentEncoder::new(bkxpro_codec::SegmentHeader {
            segment_id,
            writer_epoch: 1,
            target_size: 64 * 1024,
            created_at_ns: 0,
        });
        for entry in 1..=3 {
            encoder
                .push_record(segment_id, entry, 0, &Bytes::from(vec![entry as u8; 4000]))
                .unwrap();
        }
        encoder.seal().unwrap()
    }

    fn install_rebuild_image(io: &mut MemoryIo, path: &Path, image: &[u8], logical_size: usize) {
        std::fs::write(path, image).unwrap();
        io.files.insert(
            path.to_path_buf(),
            MemoryFile(Arc::new(Mutex::new(image.to_vec()))),
        );
        io.logical_sizes
            .insert(path.to_path_buf(), logical_size as u64);
    }

    #[test]
    fn failed_rebuild_rolls_back_and_partial_rebuild_discards_failed_segment_rows() {
        let temp = TempDir::new().unwrap();
        let data_dir = temp.path().join("segments");
        std::fs::create_dir(&data_dir).unwrap();
        let mut index = Index::open(IndexConfig {
            db_path: temp.path().join("index"),
            ..Default::default()
        })
        .unwrap();
        let good = rebuild_image(1);
        let mut bad = rebuild_image(2).to_vec();
        let trailer = TrailerWire::parse(&bad[bad.len() - TRAILER_SIZE..]).unwrap();
        let footer_start = trailer.footer_start as usize;
        let crc_offset = footer_start + trailer.footer_len as usize - 4;
        // The first footer row is valid; the second overflows the payload offset.
        // Wire layout: 28-byte footer header, 32-byte index rows, offset at byte 16.
        let second_offset = footer_start + 28 + 32 + 16;
        bad[second_offset..second_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        let crc = crc32c::crc32c(&bad[footer_start..crc_offset]);
        bad[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());
        let mut io = MemoryIo::default();
        install_rebuild_image(&mut io, &data_dir.join("1.seg"), &good, good.len());
        install_rebuild_image(&mut io, &data_dir.join("2.seg"), &bad, bad.len());
        let config = WriterConfig::default_for(64 * 1024, 10);
        assert!(matches!(
            index.rebuild_from_segments(&mut io, &data_dir, &config),
            Err(IndexError::InvalidArgument(_))
        ));
        assert!(index.get_entry(1, 1).unwrap().is_none());
        assert!(index.get_segment(1).unwrap().is_none());
        let stats = index
            .rebuild_from_segments_with_options(&mut io, &data_dir, &config, true)
            .unwrap();
        assert_eq!(
            (
                stats.segments_processed,
                stats.segments_failed,
                stats.entries_indexed
            ),
            (1, 1, 3)
        );
        assert!(index.get_entry(1, 1).unwrap().is_some());
        assert!(index.get_entry(2, 1).unwrap().is_none());
        assert!(index.get_ledger(2).unwrap().is_none());
        assert!(index.get_segment(2).unwrap().is_none());
    }

    #[test]
    fn open_rebuild_uses_one_aligned_range_and_ignores_partial_tail() {
        let temp = TempDir::new().unwrap();
        let data_dir = temp.path().join("segments");
        std::fs::create_dir(&data_dir).unwrap();
        let mut index = Index::open(IndexConfig {
            db_path: temp.path().join("index"),
            ..Default::default()
        })
        .unwrap();
        let sealed = rebuild_image(1);
        let trailer = TrailerWire::parse(&sealed[sealed.len() - TRAILER_SIZE..]).unwrap();
        let open_len = trailer.footer_start as usize;
        let logical_size = open_len - 7;
        let mut io = MemoryIo::default();
        install_rebuild_image(
            &mut io,
            &data_dir.join("1.seg"),
            &sealed[..open_len],
            logical_size,
        );
        let stats = index
            .rebuild_from_segments(
                &mut io,
                &data_dir,
                &WriterConfig::default_for(64 * 1024, 10),
            )
            .unwrap();
        assert_eq!(stats.entries_indexed, 2);
        assert_eq!(
            io.reads.len(),
            2,
            "trailer probe plus one whole-segment read"
        );
        assert_eq!(io.reads[1], (0, open_len));
        assert!(index.get_entry(1, 3).unwrap().is_none());
        let loc = index.get_entry(1, 2).unwrap().unwrap();
        assert_eq!(
            &sealed[loc.offset as usize..loc.offset as usize + loc.len as usize],
            &[2; 4000]
        );
    }

    #[test]
    fn entry_locations_reject_overflow_and_accept_u32_boundaries() {
        assert_eq!(entry_u32(u32::MAX as u64, "segment id").unwrap(), u32::MAX);
        assert_eq!(
            payload_offset(u32::MAX as u64 - RECORD_HEADER_SIZE as u64).unwrap(),
            u32::MAX
        );
        for offset in [u32::MAX as u64, u64::MAX] {
            assert!(matches!(
                payload_offset(offset),
                Err(IndexError::InvalidArgument(_))
            ));
        }
        let temp = TempDir::new().unwrap();
        let index = Index::open(IndexConfig {
            db_path: temp.path().join("index"),
            ..Default::default()
        })
        .unwrap();
        let mut batch = index.batch();
        index
            .put_entry(
                &mut batch,
                1,
                1,
                EntryLocation {
                    segment_id: 1,
                    offset: 4096,
                    len: 1,
                },
            )
            .unwrap();
        batch.commit().unwrap();
        assert!(matches!(
            delete_segment_entries(&index, (1u64 << 32) + 1),
            Err(IndexError::InvalidArgument(_))
        ));
        assert!(index.get_entry(1, 1).unwrap().is_some());
    }

    #[test]
    fn test_rebuild_from_segments_integration() {
        use bkxpro_codec::{AlignedSegmentEncoder, SegmentHeader, BLOCK_SIZE, HEADER_SIZE};
        use bkxpro_segmenter::{AlignedBuffer, SegmentWriter, WriterConfig};

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let index_dir = temp_dir.path().join("index");
        std::fs::create_dir_all(&data_dir).unwrap();

        let index_config = IndexConfig {
            db_path: index_dir,
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let mut index = Index::open(index_config).unwrap();

        // Create a segment with data
        let io = MemoryIo::default();
        let mut buffer = AlignedBuffer::new_zeroed(
            WriterConfig::default_for(32 * BLOCK_SIZE, 100)
                .buffer_capacity()
                .unwrap(),
        )
        .unwrap();
        let mut writer = SegmentWriter::create(
            io,
            data_dir.join("1.seg"),
            &data_dir,
            SegmentHeader {
                segment_id: 1,
                writer_epoch: 1,
                target_size: (HEADER_SIZE + 32 * BLOCK_SIZE) as u64,
                created_at_ns: 1_700_000_000,
            },
            WriterConfig::default_for(32 * BLOCK_SIZE, 100),
            &mut buffer,
        )
        .unwrap();

        let mut encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        for i in 1..=10 {
            writer
                .append(&mut encoder, 1, i, 0, &Bytes::from(format!("entry {}", i)))
                .unwrap();
        }
        let _ = writer.seal(encoder).unwrap();

        // Get the io back from the sealed writer
        let (mut io, _, _) = match writer.try_into_parts() {
            Ok(parts) => parts,
            Err(_) => panic!("writer should be sealed"),
        };

        // Also write the segment file to disk so discover_segment_files can find it
        let segment_path = data_dir.join("1.seg");
        std::fs::write(&segment_path, &*buffer).unwrap();

        // Rebuild index from segments using the SAME MemoryIo instance
        let stats = index
            .rebuild_from_segments(
                &mut io,
                &data_dir,
                &WriterConfig::default_for(32 * BLOCK_SIZE, 100),
            )
            .unwrap();

        assert_eq!(stats.segments_processed, 1);
        assert_eq!(stats.entries_indexed, 10);
        assert_eq!(stats.ledgers_updated, 1);

        // Verify rebuilt index
        for i in 1..=10 {
            assert!(index.get_entry(1, i).unwrap().is_some());
        }
        assert_eq!(index.get_ledger(1).unwrap().unwrap().lac, 10);
        assert!(index.get_segment(1).unwrap().is_some());

        // Rebuilt locations point at the payload, not at the record header: reading
        // `len` bytes at `offset` out of the sealed segment reproduces the entry.
        let image = std::fs::read(&segment_path).unwrap();
        for i in 1..=10 {
            let location = index.get_entry(1, i).unwrap().unwrap();
            let start = location.offset as usize;
            let end = start + location.len as usize;
            assert_eq!(
                &image[start..end],
                format!("entry {i}").as_bytes(),
                "payload offset of entry {i}"
            );
        }
    }

    #[test]
    fn test_empty_segment_rebuild() {
        use bkxpro_codec::{AlignedSegmentEncoder, SegmentHeader, BLOCK_SIZE, HEADER_SIZE};
        use bkxpro_segmenter::{AlignedBuffer, SegmentWriter, WriterConfig};

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let index_dir = temp_dir.path().join("index");
        std::fs::create_dir_all(&data_dir).unwrap();

        let index_config = IndexConfig {
            db_path: index_dir,
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let mut index = Index::open(index_config).unwrap();

        // Create empty segment - use the same MemoryIo for writing and rebuilding
        let io = MemoryIo::default();
        let mut buffer = AlignedBuffer::new_zeroed(
            WriterConfig::default_for(32 * BLOCK_SIZE, 10)
                .buffer_capacity()
                .unwrap(),
        )
        .unwrap();
        let mut writer = SegmentWriter::create(
            io,
            data_dir.join("1.seg"),
            &data_dir,
            SegmentHeader {
                segment_id: 1,
                writer_epoch: 1,
                target_size: (HEADER_SIZE + 32 * BLOCK_SIZE) as u64,
                created_at_ns: 1_700_000_000,
            },
            WriterConfig::default_for(32 * BLOCK_SIZE, 10),
            &mut buffer,
        )
        .unwrap();

        let encoder = AlignedSegmentEncoder::new(writer.header(), &mut buffer).unwrap();
        let _ = writer.seal(encoder).unwrap();

        // Get the io back from the sealed writer
        let (mut io, _, _) = match writer.try_into_parts() {
            Ok(parts) => parts,
            Err(_) => panic!("writer should be sealed"),
        };

        // Also write the segment file to disk so discover_segment_files can find it
        let segment_path = data_dir.join("1.seg");
        std::fs::write(&segment_path, &*buffer).unwrap();

        // Rebuild index using the SAME MemoryIo instance
        let stats = index
            .rebuild_from_segments(
                &mut io,
                &data_dir,
                &WriterConfig::default_for(32 * BLOCK_SIZE, 10),
            )
            .unwrap();

        assert_eq!(stats.segments_processed, 1);
        assert_eq!(stats.entries_indexed, 0);
    }

    #[test]
    fn test_scan_entries_empty_range() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let scanned = index.scan_entries(1, 1, 10).unwrap();
        assert_eq!(scanned.len(), 0);
    }

    #[test]
    fn test_scan_entries_out_of_bounds() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        index
            .put_entry(
                &mut batch,
                1,
                100,
                EntryLocation {
                    segment_id: 1,
                    offset: 10000,
                    len: 50,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        // Scan range that doesn't include entry 100
        let scanned = index.scan_entries(1, 1, 50).unwrap();
        assert_eq!(scanned.len(), 0);

        // Scan range that includes entry 100
        let scanned = index.scan_entries(1, 1, 150).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].1, 100);
    }

    #[test]
    fn test_duplicate_entry_rejection() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        index
            .put_entry(
                &mut batch,
                1,
                1,
                EntryLocation {
                    segment_id: 1,
                    offset: 100,
                    len: 50,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        // Overwrite should work (same key)
        let mut batch = index.batch();
        index
            .put_entry(
                &mut batch,
                1,
                1,
                EntryLocation {
                    segment_id: 2,
                    offset: 200,
                    len: 100,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        let entry = index.get_entry(1, 1).unwrap().unwrap();
        assert_eq!(entry.segment_id, 2);
        assert_eq!(entry.offset, 200);
    }

    #[test]
    fn test_segment_with_s3_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        index
            .put_segment(
                &mut batch,
                1,
                SegmentMeta {
                    state: SegmentState::Uploaded,
                    s3_key: Some("s3://my-bucket/segments/1.seg".to_string()),
                    min_ledger: 1,
                    max_ledger: 10,
                    byte_size: 1024 * 1024,
                    writer_epoch: 1,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        let segment = index.get_segment(1).unwrap().unwrap();
        assert_eq!(
            segment.s3_key,
            Some("s3://my-bucket/segments/1.seg".to_string())
        );
        assert_eq!(segment.state, SegmentState::Uploaded);
    }

    #[test]
    fn test_large_segment_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        index
            .put_segment(
                &mut batch,
                1,
                SegmentMeta {
                    state: SegmentState::Sealed,
                    s3_key: None,
                    min_ledger: 1,
                    max_ledger: 10000,
                    byte_size: 64 * 1024 * 1024, // 64 MB
                    writer_epoch: 1,
                },
            )
            .unwrap();
        batch.commit().unwrap();

        let segment = index.get_segment(1).unwrap().unwrap();
        assert_eq!(segment.min_ledger, 1);
        assert_eq!(segment.max_ledger, 10000);
        assert_eq!(segment.byte_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_iter_segments() {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_memory_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let index = Index::open(config).unwrap();

        let mut batch = index.batch();
        for i in 1..=10 {
            index
                .put_segment(
                    &mut batch,
                    i,
                    SegmentMeta {
                        state: SegmentState::Sealed,
                        s3_key: None,
                        min_ledger: i,
                        max_ledger: i,
                        byte_size: 1000,
                        writer_epoch: 1,
                    },
                )
                .unwrap();
        }
        batch.commit().unwrap();

        let segments = index.iter_segments().unwrap();
        assert_eq!(segments.len(), 10);
        for (seg_id, meta) in segments {
            assert_eq!(seg_id, meta.min_ledger);
        }
    }

    #[test]
    fn test_entry_location_edge_cases() {
        // Max values
        let loc = EntryLocation {
            segment_id: u32::MAX,
            offset: u32::MAX,
            len: u32::MAX,
        };
        let encoded = loc.encode();
        let decoded = EntryLocation::decode(&encoded).unwrap();
        assert_eq!(loc, decoded);

        // Zero values
        let loc = EntryLocation {
            segment_id: 0,
            offset: 0,
            len: 0,
        };
        let encoded = loc.encode();
        let decoded = EntryLocation::decode(&encoded).unwrap();
        assert_eq!(loc, decoded);
    }

    #[test]
    fn test_invalid_entry_location_decode() {
        // Wrong length
        assert!(EntryLocation::decode(&[0u8; 11]).is_err());
        assert!(EntryLocation::decode(&[0u8; 13]).is_err());
        assert!(EntryLocation::decode(&[]).is_err());
    }
}
