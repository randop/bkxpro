//! Fixed-format segment, block, record, and footer codec for bkxpro (RAN-126).
//!
//! Payload bytes are copied once into the output segment during encoding. Decoding
//! borrows the segment through `bytes::Bytes`; `RecordRef::payload_chunks` yields
//! cheap `Bytes::slice` views and does not allocate a per-record payload buffer,
//! including when a payload crosses block boundaries.
//!
//! Platform: Linux only. Uses Linux-specific alignment and O_DIRECT assumptions.

#[cfg(not(target_os = "linux"))]
compile_error!("bkxpro-codec supports Linux only; develop and test in a Linux VM/devcontainer");

use bytes::Bytes;
use core::{fmt, marker::PhantomData};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub const HEADER_SIZE: usize = 4096;
pub const BLOCK_SIZE: usize = 4096;
pub const BLOCK_HEADER_SIZE: usize = 16;
pub const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;
pub const RECORD_HEADER_SIZE: usize = 21;
pub const TRAILER_SIZE: usize = 32;
pub const FORMAT_VERSION: u16 = 1;

const SEGMENT_MAGIC: &[u8; 8] = b"BKXPROSG";
const BLOCK_MAGIC: &[u8; 4] = b"BLK1";
const FOOTER_MAGIC: &[u8; 8] = b"BKXPROFT";
const TRAILER_MAGIC: &[u8; 8] = b"BKXPROTR";

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct SegmentHeaderWire {
    magic: [u8; 8],
    version: u16,
    header_len: u16,
    flags: u32,
    segment_id: u64,
    writer_epoch: u64,
    target_size: u64,
    created_at_ns: u64,
    crc32c: u32,
    reserved: u32,
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct BlockHeaderWire {
    magic: [u8; 4],
    record_count: u16,
    flags: u16,
    used_len: u32,
    crc32c: u32,
}

/// Byte-array integer fields keep this `repr(C)` wire frame exactly 21 bytes
/// without ABI padding; integer conversion is explicit and little-endian.
#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct RecordHeaderWire {
    len: [u8; 4],
    ledger: [u8; 8],
    entry: [u8; 8],
    flags: u8,
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct FooterHeaderWire {
    magic: [u8; 8],
    version: u16,
    header_len: u16,
    index_count: u32,
    ledger_count: u32,
    data_blocks: u32,
    flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct FooterIndexWire {
    ledger: u64,
    entry: u64,
    offset: u64,
    len: u32,
    reserved: u32,
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct LedgerSummaryWire {
    ledger: u64,
    min_entry: u64,
    max_entry: u64,
}

/// Fixed 32-byte trailer. Field order avoids implicit C padding.
#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct TrailerWire {
    pub magic: [u8; 8],
    pub footer_start: u64,
    pub footer_len: u32,
    pub footer_crc32c: u32,
    pub version: u16,
    pub trailer_len: u16,
    pub trailer_crc32c: u32,
}

impl TrailerWire {
    /// Parse a trailer from the last 32 bytes of a segment.
    pub fn parse(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() < 32 {
            return Err(CodecError::Truncated("trailer"));
        }
        let wire =
            Self::read_from_bytes(&bytes[..32]).map_err(|_| CodecError::Truncated("trailer"))?;
        if &wire.magic != TRAILER_MAGIC {
            return Err(CodecError::BadMagic("trailer"));
        }
        let version = u16::from_le(wire.version);
        if version != FORMAT_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        if u16::from_le(wire.trailer_len) as usize != 32 {
            return Err(CodecError::InvalidField("trailer length"));
        }
        let stored_crc = u32::from_le(wire.trailer_crc32c);
        // CRC with the CRC field zeroed
        let mut bytes_with_zero_crc = [0u8; 32];
        bytes_with_zero_crc.copy_from_slice(&bytes[..32]);
        bytes_with_zero_crc[28..32].fill(0);
        if crc32c::crc32c(&bytes_with_zero_crc) != stored_crc {
            return Err(CodecError::BadCrc("trailer"));
        }
        Ok(wire)
    }
}

// Fail the build if an ABI/layout change alters any wire size.
const _: [(); 56] = [(); core::mem::size_of::<SegmentHeaderWire>()];
const _: [(); 16] = [(); core::mem::size_of::<BlockHeaderWire>()];
const _: [(); 21] = [(); core::mem::size_of::<RecordHeaderWire>()];
const _: [(); 28] = [(); core::mem::size_of::<FooterHeaderWire>()];
const _: [(); 32] = [(); core::mem::size_of::<FooterIndexWire>()];
const _: [(); 24] = [(); core::mem::size_of::<LedgerSummaryWire>()];
const _: [(); 32] = [(); core::mem::size_of::<TrailerWire>()];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    Truncated(&'static str),
    BadMagic(&'static str),
    UnsupportedVersion(u16),
    BadCrc(&'static str),
    InvalidField(&'static str),
    IntegerOverflow,
    DuplicateEntry { ledger: u64, entry: u64 },
    UnsortedFooter,
    IncompleteRecord,
    RecordCountMismatch,
    NotSealed,
    OutputBufferTooSmall { required: usize, available: usize },
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated(what) => write!(f, "truncated {what}"),
            Self::BadMagic(what) => write!(f, "invalid {what} magic"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported codec version {v}"),
            Self::BadCrc(what) => write!(f, "invalid {what} CRC32C"),
            Self::InvalidField(what) => write!(f, "invalid {what}"),
            Self::IntegerOverflow => write!(f, "integer overflow in codec length"),
            Self::DuplicateEntry { ledger, entry } => {
                write!(f, "duplicate entry ({ledger}, {entry})")
            }
            Self::UnsortedFooter => write!(f, "footer index or ledger summaries are not sorted"),
            Self::IncompleteRecord => write!(f, "incomplete record at recovery boundary"),
            Self::RecordCountMismatch => {
                write!(f, "record count does not match block/footer metadata")
            }
            Self::NotSealed => write!(f, "segment has no valid footer and trailer"),
            Self::OutputBufferTooSmall {
                required,
                available,
            } => {
                write!(
                    f,
                    "output buffer too small: need {required} bytes, have {available}"
                )
            }
        }
    }
}
impl std::error::Error for CodecError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub segment_id: u64,
    pub writer_epoch: u64,
    pub target_size: u64,
    pub created_at_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub len: u32,
    pub ledger: u64,
    pub entry: u64,
    pub flags: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedRecord<'a> {
    pub header: RecordHeader,
    /// Borrowed payload view; no entry-sized allocation is made.
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FooterIndexEntry {
    pub ledger: u64,
    pub entry: u64,
    /// Absolute byte offset of the packed 21-byte record header in the segment.
    pub offset: u64,
    /// Payload length, excluding the record header.
    pub len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerSummary {
    pub ledger: u64,
    pub min_entry: u64,
    pub max_entry: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockInfo {
    pub record_count: u16,
    pub used_len: usize,
}

/// Physical block extent occupied by a record in an in-progress aligned segment.
/// Offsets are absolute byte offsets from the beginning of the segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordSpan {
    /// Block containing the record header.
    pub first_block_offset: usize,
    /// Block containing the final payload byte. This block may still be open.
    pub last_block_offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStop {
    EndOfFile,
    PartialBlock,
    ZeroBlock,
    InvalidBlock,
    IncompleteRecord,
    InvalidHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    /// Last safe physical truncation point. 0 means the segment header itself was invalid.
    pub valid_len: usize,
    pub valid_blocks: usize,
    pub complete_records: usize,
    pub stop: RecoveryStop,
}

/// CRC32C for a byte slice with one four-byte checksum field treated as zero.
/// `crc32c` dispatches to hardware acceleration where supported; incremental
/// checksumming avoids copying an entire 4 KiB block just to clear its CRC field.
#[inline]
fn crc32c_with_zeroed_u32(bytes: &[u8], field_offset: usize) -> u32 {
    debug_assert!(field_offset + 4 <= bytes.len());
    let crc = crc32c::crc32c(&bytes[..field_offset]);
    let crc = crc32c::crc32c_append(crc, &[0u8; 4]);
    crc32c::crc32c_append(crc, &bytes[field_offset + 4..])
}

#[inline]
fn align_up(value: usize, alignment: usize) -> Result<usize, CodecError> {
    let add = alignment
        .checked_sub(1)
        .ok_or(CodecError::IntegerOverflow)?;
    value
        .checked_add(add)
        .map(|v| v / alignment * alignment)
        .ok_or(CodecError::IntegerOverflow)
}

/// True when the buffer starts at a 4 KiB boundary and its length is a multiple
/// of 4 KiB. This checks memory geometry only; RAN-134 performs host/device DIO
/// capability preflight and RAN-127 owns io_uring registration and I/O.
pub fn is_4k_aligned_buffer(buffer: &[u8]) -> bool {
    (buffer.as_ptr() as usize) % BLOCK_SIZE == 0 && buffer.len() % BLOCK_SIZE == 0
}

/// Parse and validate the 4 KiB segment header.
pub fn parse_segment_header(input: &[u8]) -> Result<SegmentHeader, CodecError> {
    if input.len() < HEADER_SIZE {
        return Err(CodecError::Truncated("segment header"));
    }
    let sector = &input[..HEADER_SIZE];
    let wire =
        SegmentHeaderWire::read_from_bytes(&sector[..core::mem::size_of::<SegmentHeaderWire>()])
            .map_err(|_| CodecError::Truncated("segment header fields"))?;
    if &wire.magic != SEGMENT_MAGIC {
        return Err(CodecError::BadMagic("segment header"));
    }
    let version = u16::from_le(wire.version);
    if version != FORMAT_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    if u16::from_le(wire.header_len) as usize != HEADER_SIZE {
        return Err(CodecError::InvalidField("segment header length"));
    }
    if u32::from_le(wire.flags) != 0 || u32::from_le(wire.reserved) != 0 {
        return Err(CodecError::InvalidField("segment header reserved bits"));
    }
    if sector[core::mem::size_of::<SegmentHeaderWire>()..]
        .iter()
        .any(|b| *b != 0)
    {
        return Err(CodecError::InvalidField("segment header padding"));
    }
    let stored_crc = u32::from_le(wire.crc32c);
    if crc32c_with_zeroed_u32(sector, 48) != stored_crc {
        return Err(CodecError::BadCrc("segment header"));
    }
    Ok(SegmentHeader {
        segment_id: u64::from_le(wire.segment_id),
        writer_epoch: u64::from_le(wire.writer_epoch),
        target_size: u64::from_le(wire.target_size),
        created_at_ns: u64::from_le(wire.created_at_ns),
    })
}

/// Parse a record header from a contiguous byte slice. Payload bytes are not copied.
pub fn parse_record_header(input: &[u8]) -> Result<RecordHeader, CodecError> {
    if input.len() < RECORD_HEADER_SIZE {
        return Err(CodecError::Truncated("record header"));
    }
    let wire = RecordHeaderWire::read_from_bytes(&input[..RECORD_HEADER_SIZE])
        .map_err(|_| CodecError::Truncated("record header"))?;
    Ok(RecordHeader {
        len: u32::from_le_bytes(wire.len),
        ledger: u64::from_le_bytes(wire.ledger),
        entry: u64::from_le_bytes(wire.entry),
        flags: wire.flags,
    })
}

/// Parse one contiguous record and return its payload as a borrowed slice. Any bytes
/// after this record are left to the caller as the next record in the stream.
pub fn parse_record(input: &[u8]) -> Result<ParsedRecord<'_>, CodecError> {
    let header = parse_record_header(input)?;
    let end = RECORD_HEADER_SIZE
        .checked_add(header.len as usize)
        .ok_or(CodecError::IntegerOverflow)?;
    if input.len() < end {
        return Err(CodecError::Truncated("record payload"));
    }
    Ok(ParsedRecord {
        header,
        payload: &input[RECORD_HEADER_SIZE..end],
    })
}

/// Parse one 4 KiB block, checking its checksum and canonical zero padding.
pub fn parse_block(input: &[u8]) -> Result<BlockInfo, CodecError> {
    if input.len() < BLOCK_SIZE {
        return Err(CodecError::Truncated("block"));
    }
    let block = &input[..BLOCK_SIZE];
    if block.iter().all(|b| *b == 0) {
        return Err(CodecError::InvalidField("zero block"));
    }
    let wire = BlockHeaderWire::read_from_bytes(&block[..BLOCK_HEADER_SIZE])
        .map_err(|_| CodecError::Truncated("block header"))?;
    if &wire.magic != BLOCK_MAGIC {
        return Err(CodecError::BadMagic("block"));
    }
    let record_count = u16::from_le(wire.record_count);
    let flags = u16::from_le(wire.flags);
    let used_len = u32::from_le(wire.used_len) as usize;
    if flags != 0 || used_len > BLOCK_DATA_SIZE {
        return Err(CodecError::InvalidField("block header"));
    }
    if block[BLOCK_HEADER_SIZE + used_len..]
        .iter()
        .any(|b| *b != 0)
    {
        return Err(CodecError::InvalidField("block zero padding"));
    }
    let stored_crc = u32::from_le(wire.crc32c);
    if crc32c_with_zeroed_u32(block, 12) != stored_crc {
        return Err(CodecError::BadCrc("block"));
    }
    if used_len == 0 && record_count != 0 {
        return Err(CodecError::InvalidField("empty block record count"));
    }
    Ok(BlockInfo {
        record_count,
        used_len,
    })
}

fn encode_segment_header_into(sector: &mut [u8], header: SegmentHeader) {
    debug_assert!(sector.len() >= HEADER_SIZE);
    sector[..HEADER_SIZE].fill(0);
    let wire = SegmentHeaderWire {
        magic: *SEGMENT_MAGIC,
        version: FORMAT_VERSION.to_le(),
        header_len: (HEADER_SIZE as u16).to_le(),
        flags: 0,
        segment_id: header.segment_id.to_le(),
        writer_epoch: header.writer_epoch.to_le(),
        target_size: header.target_size.to_le(),
        created_at_ns: header.created_at_ns.to_le(),
        crc32c: 0,
        reserved: 0,
    };
    sector[..core::mem::size_of::<SegmentHeaderWire>()].copy_from_slice(wire.as_bytes());
    let crc = crc32c_with_zeroed_u32(&sector[..HEADER_SIZE], 48);
    sector[48..52].copy_from_slice(&crc.to_le_bytes());
}

fn encode_segment_header(header: SegmentHeader) -> [u8; HEADER_SIZE] {
    let mut sector = [0u8; HEADER_SIZE];
    encode_segment_header_into(&mut sector, header);
    sector
}

fn append_record_header(out: &mut [u8], header: RecordHeader) {
    debug_assert!(out.len() >= RECORD_HEADER_SIZE);
    let wire = RecordHeaderWire {
        len: header.len.to_le_bytes(),
        ledger: header.ledger.to_le_bytes(),
        entry: header.entry.to_le_bytes(),
        flags: header.flags,
    };
    out[..RECORD_HEADER_SIZE].copy_from_slice(wire.as_bytes());
}

#[derive(Debug, Clone, Copy)]
struct BlockSpan {
    physical_block_start: usize,
    data_start: usize,
    stream_start: usize,
    len: usize,
    record_count: u16,
}

#[derive(Debug, Clone)]
pub struct FooterView {
    bytes: Bytes,
    index_count: usize,
    ledger_count: usize,
    data_blocks: usize,
}

impl FooterView {
    /// Parse a footer section (including its trailing CRC, excluding the trailer).
    pub fn parse(bytes: Bytes) -> Result<Self, CodecError> {
        if bytes.len() < core::mem::size_of::<FooterHeaderWire>() + 4 {
            return Err(CodecError::Truncated("footer"));
        }
        let wire =
            FooterHeaderWire::read_from_bytes(&bytes[..core::mem::size_of::<FooterHeaderWire>()])
                .map_err(|_| CodecError::Truncated("footer header"))?;
        if &wire.magic != FOOTER_MAGIC {
            return Err(CodecError::BadMagic("footer"));
        }
        let version = u16::from_le(wire.version);
        if version != FORMAT_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        if u16::from_le(wire.header_len) as usize != core::mem::size_of::<FooterHeaderWire>()
            || u32::from_le(wire.flags) != 0
        {
            return Err(CodecError::InvalidField("footer header"));
        }
        let index_count = u32::from_le(wire.index_count) as usize;
        let ledger_count = u32::from_le(wire.ledger_count) as usize;
        let data_blocks = u32::from_le(wire.data_blocks) as usize;
        let expected_len = core::mem::size_of::<FooterHeaderWire>()
            .checked_add(
                index_count
                    .checked_mul(core::mem::size_of::<FooterIndexWire>())
                    .ok_or(CodecError::IntegerOverflow)?,
            )
            .and_then(|n| {
                n.checked_add(ledger_count.checked_mul(core::mem::size_of::<LedgerSummaryWire>())?)
            })
            .and_then(|n| n.checked_add(4))
            .ok_or(CodecError::IntegerOverflow)?;
        if expected_len != bytes.len() {
            return Err(CodecError::InvalidField("footer length/counts"));
        }
        for i in 0..index_count {
            let start = core::mem::size_of::<FooterHeaderWire>()
                + i * core::mem::size_of::<FooterIndexWire>();
            let item = FooterIndexWire::read_from_bytes(
                &bytes[start..start + core::mem::size_of::<FooterIndexWire>()],
            )
            .map_err(|_| CodecError::Truncated("footer index entry"))?;
            if u32::from_le(item.reserved) != 0 {
                return Err(CodecError::InvalidField("footer index reserved bits"));
            }
        }
        let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        if crc32c::crc32c(&bytes[..bytes.len() - 4]) != stored_crc {
            return Err(CodecError::BadCrc("footer"));
        }
        let footer = Self {
            bytes,
            index_count,
            ledger_count,
            data_blocks,
        };
        footer.validate_sorted()?;
        Ok(footer)
    }

    pub fn index_count(&self) -> usize {
        self.index_count
    }
    pub fn ledger_count(&self) -> usize {
        self.ledger_count
    }
    pub fn data_blocks(&self) -> usize {
        self.data_blocks
    }

    pub fn index_entries(&self) -> FooterEntries<'_> {
        FooterEntries {
            footer: self,
            next: 0,
            _marker: PhantomData,
        }
    }

    pub fn ledger_summaries(&self) -> LedgerSummaries<'_> {
        LedgerSummaries {
            footer: self,
            next: 0,
            _marker: PhantomData,
        }
    }

    fn index_at(&self, i: usize) -> FooterIndexEntry {
        let start =
            core::mem::size_of::<FooterHeaderWire>() + i * core::mem::size_of::<FooterIndexWire>();
        let wire = FooterIndexWire::read_from_bytes(
            &self.bytes[start..start + core::mem::size_of::<FooterIndexWire>()],
        )
        .unwrap();
        FooterIndexEntry {
            ledger: u64::from_le(wire.ledger),
            entry: u64::from_le(wire.entry),
            offset: u64::from_le(wire.offset),
            len: u32::from_le(wire.len),
        }
    }

    fn summary_at(&self, i: usize) -> LedgerSummary {
        let start = core::mem::size_of::<FooterHeaderWire>()
            + self.index_count * core::mem::size_of::<FooterIndexWire>()
            + i * core::mem::size_of::<LedgerSummaryWire>();
        let wire = LedgerSummaryWire::read_from_bytes(
            &self.bytes[start..start + core::mem::size_of::<LedgerSummaryWire>()],
        )
        .unwrap();
        LedgerSummary {
            ledger: u64::from_le(wire.ledger),
            min_entry: u64::from_le(wire.min_entry),
            max_entry: u64::from_le(wire.max_entry),
        }
    }

    fn find_index(&self, ledger: u64, entry: u64) -> Option<FooterIndexEntry> {
        let mut lo = 0usize;
        let mut hi = self.index_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let item = self.index_at(mid);
            match (item.ledger, item.entry).cmp(&(ledger, entry)) {
                core::cmp::Ordering::Less => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Some(item),
            }
        }
        None
    }

    fn validate_sorted(&self) -> Result<(), CodecError> {
        let mut previous: Option<(u64, u64)> = None;
        for item in self.index_entries() {
            let key = (item.ledger, item.entry);
            if previous.is_some_and(|p| p >= key) {
                return Err(CodecError::UnsortedFooter);
            }
            previous = Some(key);
        }
        let mut previous_ledger = None;
        for s in self.ledger_summaries() {
            if s.min_entry > s.max_entry || previous_ledger.is_some_and(|p| p >= s.ledger) {
                return Err(CodecError::UnsortedFooter);
            }
            previous_ledger = Some(s.ledger);
        }
        // Validate the per-ledger range summaries without building an index-sized allocation.
        let mut si = 0usize;
        let mut i = 0usize;
        while i < self.index_count {
            let first = self.index_at(i);
            let mut min_entry = first.entry;
            let mut max_entry = first.entry;
            i += 1;
            while i < self.index_count {
                let item = self.index_at(i);
                if item.ledger != first.ledger {
                    break;
                }
                min_entry = min_entry.min(item.entry);
                max_entry = max_entry.max(item.entry);
                i += 1;
            }
            if si >= self.ledger_count {
                return Err(CodecError::InvalidField("missing ledger summary"));
            }
            let summary = self.summary_at(si);
            if summary.ledger != first.ledger
                || summary.min_entry != min_entry
                || summary.max_entry != max_entry
            {
                return Err(CodecError::InvalidField("ledger min/max summary"));
            }
            si += 1;
        }
        if si != self.ledger_count {
            return Err(CodecError::InvalidField("extra ledger summary"));
        }
        if self.index_count == 0 && self.ledger_count != 0 {
            return Err(CodecError::InvalidField("empty footer summaries"));
        }
        Ok(())
    }
}

pub struct FooterEntries<'a> {
    footer: &'a FooterView,
    next: usize,
    _marker: PhantomData<&'a FooterView>,
}
impl Iterator for FooterEntries<'_> {
    type Item = FooterIndexEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.footer.index_count {
            return None;
        }
        let item = self.footer.index_at(self.next);
        self.next += 1;
        Some(item)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.footer.index_count - self.next;
        (n, Some(n))
    }
}
impl ExactSizeIterator for FooterEntries<'_> {}

pub struct LedgerSummaries<'a> {
    footer: &'a FooterView,
    next: usize,
    _marker: PhantomData<&'a FooterView>,
}
impl Iterator for LedgerSummaries<'_> {
    type Item = LedgerSummary;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.footer.ledger_count {
            return None;
        }
        let item = self.footer.summary_at(self.next);
        self.next += 1;
        Some(item)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.footer.ledger_count - self.next;
        (n, Some(n))
    }
}
impl ExactSizeIterator for LedgerSummaries<'_> {}

#[derive(Debug, Clone)]
pub struct SegmentView {
    bytes: Bytes,
    header: SegmentHeader,
    spans: Vec<BlockSpan>,
    stream_len: usize,
    footer: Option<FooterView>,
    data_end: usize,
}

impl SegmentView {
    /// Open a sealed segment. Every data block, footer, trailer, and record boundary is checked.
    pub fn open_sealed(bytes: Bytes) -> Result<Self, CodecError> {
        let header = parse_segment_header(&bytes)?;
        if bytes.len() < HEADER_SIZE + TRAILER_SIZE {
            return Err(CodecError::NotSealed);
        }
        let trailer_start = bytes.len() - TRAILER_SIZE;
        let trailer_bytes = &bytes[trailer_start..];
        let trailer = TrailerWire::read_from_bytes(trailer_bytes)
            .map_err(|_| CodecError::Truncated("segment trailer"))?;
        if &trailer.magic != TRAILER_MAGIC {
            return Err(CodecError::NotSealed);
        }
        let version = u16::from_le(trailer.version);
        if version != FORMAT_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        if u16::from_le(trailer.trailer_len) as usize != TRAILER_SIZE {
            return Err(CodecError::InvalidField("trailer length"));
        }
        let stored_trailer_crc = u32::from_le(trailer.trailer_crc32c);
        if crc32c_with_zeroed_u32(trailer_bytes, 28) != stored_trailer_crc {
            return Err(CodecError::BadCrc("trailer"));
        }
        let footer_start = usize::try_from(u64::from_le(trailer.footer_start))
            .map_err(|_| CodecError::IntegerOverflow)?;
        let footer_len = u32::from_le(trailer.footer_len) as usize;
        let footer_end = footer_start
            .checked_add(footer_len)
            .ok_or(CodecError::IntegerOverflow)?;
        if bytes.len() % BLOCK_SIZE != 0
            || footer_start < HEADER_SIZE
            || footer_start % BLOCK_SIZE != 0
            || footer_end > trailer_start
            || footer_len < 4
        {
            return Err(CodecError::InvalidField("trailer footer bounds/alignment"));
        }
        let expected_tail = align_up(
            footer_len
                .checked_add(TRAILER_SIZE)
                .ok_or(CodecError::IntegerOverflow)?,
            BLOCK_SIZE,
        )?;
        if footer_start
            .checked_add(expected_tail)
            .ok_or(CodecError::IntegerOverflow)?
            != bytes.len()
        {
            return Err(CodecError::InvalidField(
                "non-canonical aligned footer tail",
            ));
        }
        if bytes[footer_end..trailer_start].iter().any(|b| *b != 0) {
            return Err(CodecError::InvalidField("footer tail padding"));
        }
        let footer_bytes = bytes.slice(footer_start..footer_end);
        let footer = FooterView::parse(footer_bytes)?;
        let stored_footer_crc =
            u32::from_le_bytes(bytes[footer_end - 4..footer_end].try_into().unwrap());
        if stored_footer_crc != u32::from_le(trailer.footer_crc32c) {
            return Err(CodecError::BadCrc("footer/trailer agreement"));
        }
        let block_count = (footer_start - HEADER_SIZE) / BLOCK_SIZE;
        if block_count != footer.data_blocks {
            return Err(CodecError::InvalidField("footer block count"));
        }
        let spans = parse_all_blocks(&bytes, footer_start)?;
        let stream_len = spans.iter().map(|s| s.len).sum();
        let view = Self {
            bytes,
            header,
            spans,
            stream_len,
            footer: Some(footer),
            data_end: footer_start,
        };
        let (scanned_count, rollback) = view.scan_complete_records();
        if rollback.is_some() {
            return Err(CodecError::RecordCountMismatch);
        }
        let mut count = 0usize;
        for record in view.records() {
            let record = record?;
            let indexed = view
                .footer
                .as_ref()
                .unwrap()
                .find_index(record.ledger, record.entry)
                .ok_or(CodecError::RecordCountMismatch)?;
            if indexed.offset != record.offset || indexed.len != record.len {
                return Err(CodecError::InvalidField(
                    "footer entry does not match record",
                ));
            }
            count += 1;
        }
        if scanned_count != count {
            return Err(CodecError::RecordCountMismatch);
        }
        if count != view.footer.as_ref().unwrap().index_count() {
            return Err(CodecError::RecordCountMismatch);
        }
        if view
            .spans
            .iter()
            .map(|s| s.record_count as usize)
            .sum::<usize>()
            != count
        {
            return Err(CodecError::RecordCountMismatch);
        }
        Ok(view)
    }

    /// Scan an open segment. Bad, zero, or partial blocks end the recoverable prefix.
    /// An incomplete multi-block record rolls recovery back to its first block; the
    /// encoder starts every multi-block record in a fresh block to preserve prior records.
    pub fn recover_open(bytes: Bytes) -> RecoveryResult {
        if parse_segment_header(&bytes).is_err() {
            return RecoveryResult {
                point: RecoveryPoint {
                    valid_len: 0,
                    valid_blocks: 0,
                    complete_records: 0,
                    stop: RecoveryStop::InvalidHeader,
                },
                view: None,
            };
        }
        let header = parse_segment_header(&bytes).unwrap();
        let mut spans = Vec::new();
        let mut stream_len = 0usize;
        let mut pos = HEADER_SIZE;
        let mut stop = loop {
            if pos == bytes.len() {
                break RecoveryStop::EndOfFile;
            }
            if bytes.len() - pos < BLOCK_SIZE {
                break RecoveryStop::PartialBlock;
            }
            let raw = &bytes[pos..pos + BLOCK_SIZE];
            if raw.iter().all(|b| *b == 0) {
                break RecoveryStop::ZeroBlock;
            }
            let info = match parse_block(raw) {
                Ok(info) => info,
                Err(_) => break RecoveryStop::InvalidBlock,
            };
            spans.push(BlockSpan {
                physical_block_start: pos,
                data_start: pos + BLOCK_HEADER_SIZE,
                stream_start: stream_len,
                len: info.used_len,
                record_count: info.record_count,
            });
            stream_len += info.used_len;
            pos += BLOCK_SIZE;
        };
        let mut view = SegmentView {
            bytes,
            header,
            spans,
            stream_len,
            footer: None,
            data_end: pos,
        };
        let (complete_records, rollback_block) = view.scan_complete_records();
        if let Some(block_start) = rollback_block {
            view.spans.retain(|s| s.physical_block_start < block_start);
            view.stream_len = view.spans.iter().map(|s| s.len).sum();
            view.data_end = block_start;
            stop = RecoveryStop::IncompleteRecord;
        }
        let point = RecoveryPoint {
            valid_len: view.data_end,
            valid_blocks: view.spans.len(),
            complete_records,
            stop,
        };
        RecoveryResult {
            point,
            view: Some(view),
        }
    }

    pub fn header(&self) -> SegmentHeader {
        self.header
    }
    pub fn footer(&self) -> Option<&FooterView> {
        self.footer.as_ref()
    }
    pub fn data_end(&self) -> usize {
        self.data_end
    }
    pub fn records(&self) -> RecordIter<'_> {
        RecordIter {
            segment: self,
            next_stream: 0,
            failed: false,
        }
    }

    fn span_for_stream(&self, at: usize) -> Option<(usize, &BlockSpan)> {
        // First span whose end is past `at`; zero-length spans are skipped naturally.
        let i = self.spans.partition_point(|s| s.stream_start + s.len <= at);
        let s = self.spans.get(i)?;
        (at >= s.stream_start).then_some((i, s))
    }

    fn physical_for_stream(&self, at: usize) -> Option<usize> {
        self.span_for_stream(at)
            .map(|(_, s)| s.data_start + at - s.stream_start)
    }

    fn read_header_at(&self, stream_at: usize) -> Result<(RecordHeader, usize), CodecError> {
        let (span_index, span) = self
            .span_for_stream(stream_at)
            .ok_or(CodecError::Truncated("record header"))?;
        let local = stream_at - span.stream_start;
        if local
            .checked_add(RECORD_HEADER_SIZE)
            .ok_or(CodecError::IntegerOverflow)?
            > span.len
        {
            return Err(CodecError::IncompleteRecord);
        }
        let physical = span.data_start + local;
        let header = parse_record_header(&self.bytes[physical..physical + RECORD_HEADER_SIZE])?;
        Ok((header, span_index))
    }

    fn scan_complete_records(&self) -> (usize, Option<usize>) {
        let mut stream_at = 0usize;
        let mut records = 0usize;
        let mut starts_per_span = vec![0usize; self.spans.len()];
        while stream_at < self.stream_len {
            let (header, span_index) = match self.read_header_at(stream_at) {
                Ok(v) => v,
                Err(_) => {
                    let failed_span = self
                        .span_for_stream(stream_at)
                        .map(|(i, _)| i)
                        .unwrap_or(self.spans.len().saturating_sub(1));
                    let rollback = self.spans.get(failed_span).map(|s| s.physical_block_start);
                    let safe_records = starts_per_span.iter().take(failed_span).sum();
                    return (safe_records, rollback);
                }
            };
            let record_end = match stream_at
                .checked_add(RECORD_HEADER_SIZE)
                .and_then(|n| n.checked_add(header.len as usize))
            {
                Some(end) => end,
                None => {
                    let rollback = self.spans[span_index].physical_block_start;
                    let safe_records = starts_per_span.iter().take(span_index).sum();
                    return (safe_records, Some(rollback));
                }
            };
            if record_end > self.stream_len {
                // Large records are guaranteed to start in an otherwise-empty block.
                let rollback = self.spans[span_index].physical_block_start;
                let safe_records = starts_per_span.iter().take(span_index).sum();
                return (safe_records, Some(rollback));
            }
            starts_per_span[span_index] += 1;
            stream_at = record_end;
            records += 1;
        }
        for (i, span) in self.spans.iter().enumerate() {
            if starts_per_span[i] != span.record_count as usize {
                let safe_records = starts_per_span.iter().take(i).sum();
                return (safe_records, Some(span.physical_block_start));
            }
        }
        (records, None)
    }
}

fn parse_all_blocks(bytes: &Bytes, end: usize) -> Result<Vec<BlockSpan>, CodecError> {
    if end < HEADER_SIZE || (end - HEADER_SIZE) % BLOCK_SIZE != 0 {
        return Err(CodecError::InvalidField("data block alignment"));
    }
    let mut spans = Vec::new();
    let mut pos = HEADER_SIZE;
    let mut stream_len = 0usize;
    while pos < end {
        let block = &bytes[pos..pos + BLOCK_SIZE];
        let info = parse_block(block)?;
        spans.push(BlockSpan {
            physical_block_start: pos,
            data_start: pos + BLOCK_HEADER_SIZE,
            stream_start: stream_len,
            len: info.used_len,
            record_count: info.record_count,
        });
        stream_len = stream_len
            .checked_add(info.used_len)
            .ok_or(CodecError::IntegerOverflow)?;
        pos += BLOCK_SIZE;
    }
    Ok(spans)
}

pub struct RecoveryResult {
    pub point: RecoveryPoint,
    pub view: Option<SegmentView>,
}

#[derive(Debug, Clone, Copy)]
pub struct RecordRef<'a> {
    segment: &'a SegmentView,
    pub ledger: u64,
    pub entry: u64,
    pub flags: u8,
    pub len: u32,
    pub offset: u64,
    payload_stream_start: usize,
}

impl RecordRef<'_> {
    /// Iterate through zero-copy slices of the payload. Multi-block payloads yield
    /// one slice per physical block data area; no Vec or contiguous payload is built.
    pub fn payload_chunks(&self) -> PayloadChunks<'_> {
        PayloadChunks {
            segment: self.segment,
            next_span: 0,
            start: self.payload_stream_start,
            end: self.payload_stream_start + self.len as usize,
        }
    }

    pub fn copy_payload_to(&self, out: &mut impl std::io::Write) -> std::io::Result<()> {
        for chunk in self.payload_chunks() {
            out.write_all(&chunk)?;
        }
        Ok(())
    }
}

pub struct PayloadChunks<'a> {
    segment: &'a SegmentView,
    next_span: usize,
    start: usize,
    end: usize,
}
impl Iterator for PayloadChunks<'_> {
    type Item = Bytes;
    fn next(&mut self) -> Option<Self::Item> {
        while self.next_span < self.segment.spans.len() {
            let s = self.segment.spans[self.next_span];
            self.next_span += 1;
            let span_end = s.stream_start + s.len;
            let start = self.start.max(s.stream_start);
            let end = self.end.min(span_end);
            if start < end {
                let physical_start = s.data_start + (start - s.stream_start);
                let physical_end = physical_start + (end - start);
                return Some(self.segment.bytes.slice(physical_start..physical_end));
            }
        }
        None
    }
}

pub struct RecordIter<'a> {
    segment: &'a SegmentView,
    next_stream: usize,
    failed: bool,
}
impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<RecordRef<'a>, CodecError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.next_stream >= self.segment.stream_len {
            return None;
        }
        let start = self.next_stream;
        let (header, span_index) = match self.segment.read_header_at(start) {
            Ok(v) => v,
            Err(e) => {
                self.failed = true;
                return Some(Err(e));
            }
        };
        let payload_stream_start = match start.checked_add(RECORD_HEADER_SIZE) {
            Some(v) => v,
            None => {
                self.failed = true;
                return Some(Err(CodecError::IntegerOverflow));
            }
        };
        let end = match payload_stream_start.checked_add(header.len as usize) {
            Some(v) => v,
            None => {
                self.failed = true;
                return Some(Err(CodecError::IntegerOverflow));
            }
        };
        if end > self.segment.stream_len {
            self.failed = true;
            return Some(Err(CodecError::IncompleteRecord));
        }
        let offset = self.segment.physical_for_stream(start).unwrap_or(0) as u64;
        self.next_stream = end;
        let _ = span_index;
        Some(Ok(RecordRef {
            segment: self.segment,
            ledger: header.ledger,
            entry: header.entry,
            flags: header.flags,
            len: header.len,
            offset,
            payload_stream_start,
        }))
    }
}

/// Build a segment in memory. This is a codec/buffer abstraction, not the aligned
/// O_DIRECT writer; RAN-127 owns filesystem durability and group commit.
pub struct SegmentEncoder {
    header: SegmentHeader,
    output: Vec<u8>,
    block: [u8; BLOCK_SIZE],
    used_len: usize,
    record_count: u16,
    index: Vec<FooterIndexEntry>,
    sealed: bool,
}

impl SegmentEncoder {
    pub fn new(header: SegmentHeader) -> Self {
        let mut output = Vec::with_capacity(HEADER_SIZE + BLOCK_SIZE);
        output.extend_from_slice(&encode_segment_header(header));
        Self {
            header,
            output,
            block: [0; BLOCK_SIZE],
            used_len: 0,
            record_count: 0,
            index: Vec::new(),
            sealed: false,
        }
    }

    pub fn push_record(
        &mut self,
        ledger: u64,
        entry: u64,
        flags: u8,
        payload: &Bytes,
    ) -> Result<(), CodecError> {
        if self.sealed {
            return Err(CodecError::InvalidField("encoder already sealed"));
        }
        let len = u32::try_from(payload.len()).map_err(|_| CodecError::IntegerOverflow)?;
        let record_size = RECORD_HEADER_SIZE
            .checked_add(payload.len())
            .ok_or(CodecError::IntegerOverflow)?;
        // Start each record in a fresh block if it will span blocks. This makes
        // truncation rollback of a torn multi-block record preserve earlier records.
        let spans_blocks = record_size > BLOCK_DATA_SIZE;
        if spans_blocks || BLOCK_DATA_SIZE - self.used_len < record_size {
            self.flush_block()?;
        }
        if BLOCK_DATA_SIZE - self.used_len < RECORD_HEADER_SIZE {
            self.flush_block()?;
        }
        let record_offset = self.output.len() + BLOCK_HEADER_SIZE + self.used_len;
        let mut raw = [0u8; RECORD_HEADER_SIZE];
        append_record_header(
            &mut raw,
            RecordHeader {
                len,
                ledger,
                entry,
                flags,
            },
        );
        self.write_data(&raw)?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or(CodecError::InvalidField("block record count overflow"))?;
        self.index.push(FooterIndexEntry {
            ledger,
            entry,
            offset: record_offset as u64,
            len,
        });
        let mut copied = 0usize;
        while copied < payload.len() {
            if self.used_len == BLOCK_DATA_SIZE {
                self.flush_block()?;
            }
            let count = (BLOCK_DATA_SIZE - self.used_len).min(payload.len() - copied);
            let target_start = BLOCK_HEADER_SIZE + self.used_len;
            self.block[target_start..target_start + count]
                .copy_from_slice(&payload[copied..copied + count]);
            self.used_len += count;
            copied += count;
        }
        Ok(())
    }

    fn write_data(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        if bytes.len() > BLOCK_DATA_SIZE - self.used_len {
            return Err(CodecError::InvalidField("record header crosses block"));
        }
        let start = BLOCK_HEADER_SIZE + self.used_len;
        self.block[start..start + bytes.len()].copy_from_slice(bytes);
        self.used_len += bytes.len();
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), CodecError> {
        if self.used_len == 0 && self.record_count == 0 {
            return Ok(());
        }
        let header = BlockHeaderWire {
            magic: *BLOCK_MAGIC,
            record_count: self.record_count.to_le(),
            flags: 0,
            used_len: (self.used_len as u32).to_le(),
            crc32c: 0,
        };
        self.block[..BLOCK_HEADER_SIZE].copy_from_slice(header.as_bytes());
        let crc = crc32c::crc32c(&self.block);
        self.block[12..16].copy_from_slice(&crc.to_le_bytes());
        self.output.extend_from_slice(&self.block);
        self.block = [0; BLOCK_SIZE];
        self.used_len = 0;
        self.record_count = 0;
        Ok(())
    }

    pub fn seal(mut self) -> Result<Bytes, CodecError> {
        if self.sealed {
            return Err(CodecError::InvalidField("encoder already sealed"));
        }
        self.flush_block()?;
        self.sealed = true;
        self.index.sort_by_key(|i| (i.ledger, i.entry));
        for pair in self.index.windows(2) {
            if pair[0].ledger == pair[1].ledger && pair[0].entry == pair[1].entry {
                return Err(CodecError::DuplicateEntry {
                    ledger: pair[0].ledger,
                    entry: pair[0].entry,
                });
            }
        }
        let mut summaries = Vec::new();
        let mut i = 0usize;
        while i < self.index.len() {
            let ledger = self.index[i].ledger;
            let mut min_entry = self.index[i].entry;
            let mut max_entry = min_entry;
            i += 1;
            while i < self.index.len() && self.index[i].ledger == ledger {
                min_entry = min_entry.min(self.index[i].entry);
                max_entry = max_entry.max(self.index[i].entry);
                i += 1;
            }
            summaries.push(LedgerSummary {
                ledger,
                min_entry,
                max_entry,
            });
        }
        let blocks = (self.output.len() - HEADER_SIZE) / BLOCK_SIZE;
        let mut footer = Vec::new();
        let footer_header = FooterHeaderWire {
            magic: *FOOTER_MAGIC,
            version: FORMAT_VERSION.to_le(),
            header_len: (core::mem::size_of::<FooterHeaderWire>() as u16).to_le(),
            index_count: u32::try_from(self.index.len())
                .map_err(|_| CodecError::IntegerOverflow)?
                .to_le(),
            ledger_count: u32::try_from(summaries.len())
                .map_err(|_| CodecError::IntegerOverflow)?
                .to_le(),
            data_blocks: u32::try_from(blocks)
                .map_err(|_| CodecError::IntegerOverflow)?
                .to_le(),
            flags: 0,
        };
        footer.extend_from_slice(footer_header.as_bytes());
        for item in &self.index {
            let wire = FooterIndexWire {
                ledger: item.ledger.to_le(),
                entry: item.entry.to_le(),
                offset: item.offset.to_le(),
                len: item.len.to_le(),
                reserved: 0,
            };
            footer.extend_from_slice(wire.as_bytes());
        }
        for item in &summaries {
            let wire = LedgerSummaryWire {
                ledger: item.ledger.to_le(),
                min_entry: item.min_entry.to_le(),
                max_entry: item.max_entry.to_le(),
            };
            footer.extend_from_slice(wire.as_bytes());
        }
        let footer_crc = crc32c::crc32c(&footer);
        footer.extend_from_slice(&footer_crc.to_le_bytes());
        let footer_start = self.output.len();
        let footer_len = u32::try_from(footer.len()).map_err(|_| CodecError::IntegerOverflow)?;
        let tail_len = align_up(
            footer
                .len()
                .checked_add(TRAILER_SIZE)
                .ok_or(CodecError::IntegerOverflow)?,
            BLOCK_SIZE,
        )?;
        let tail_end = footer_start
            .checked_add(tail_len)
            .ok_or(CodecError::IntegerOverflow)?;
        let trailer_start = tail_end
            .checked_sub(TRAILER_SIZE)
            .ok_or(CodecError::IntegerOverflow)?;
        self.output.extend_from_slice(&footer);
        self.output.resize(trailer_start, 0);
        let trailer = TrailerWire {
            magic: *TRAILER_MAGIC,
            footer_start: (footer_start as u64).to_le(),
            footer_len: footer_len.to_le(),
            footer_crc32c: footer_crc.to_le(),
            version: FORMAT_VERSION.to_le(),
            trailer_len: (TRAILER_SIZE as u16).to_le(),
            trailer_crc32c: 0,
        };
        let mut trailer_bytes = [0u8; TRAILER_SIZE];
        trailer_bytes.copy_from_slice(trailer.as_bytes());
        let trailer_crc = crc32c::crc32c(&trailer_bytes);
        trailer_bytes[28..32].copy_from_slice(&trailer_crc.to_le_bytes());
        if self.output.len() != trailer_start {
            return Err(CodecError::InvalidField("trailer placement"));
        }
        self.output.extend_from_slice(&trailer_bytes);
        let _ = self.header;
        Ok(Bytes::from(self.output))
    }
}

/// Streaming encoder which writes the segment directly into caller-owned, 4 KiB-aligned
/// storage. It never builds an intermediate segment `Vec` and never copies a finished
/// block into the output: the caller's buffer is the encoder's working/output area.
/// Payload bytes are copied once from their source `Bytes` into their final block.
///
/// The caller must provide enough capacity for the complete segment and retain the
/// storage until writes submitted against it have completed. Buffer registration and
/// asynchronous I/O ownership remain the responsibility of RAN-127.
pub struct AlignedSegmentEncoder<'a> {
    output: &'a mut [u8],
    header: SegmentHeader,
    cursor: usize,
    used_len: usize,
    record_count: u16,
    block_open: bool,
    index: Vec<FooterIndexEntry>,
}

impl<'a> AlignedSegmentEncoder<'a> {
    /// Start writing at the beginning of `output`. Both its base address and total
    /// length must be 4 KiB-aligned; `output` must also hold at least header + tail.
    pub fn new(header: SegmentHeader, output: &'a mut [u8]) -> Result<Self, CodecError> {
        if !is_4k_aligned_buffer(output) {
            return Err(CodecError::InvalidField(
                "output buffer must be 4 KiB aligned",
            ));
        }
        if output.len() < HEADER_SIZE + BLOCK_SIZE {
            return Err(CodecError::OutputBufferTooSmall {
                required: HEADER_SIZE + BLOCK_SIZE,
                available: output.len(),
            });
        }
        encode_segment_header_into(output, header);
        Ok(Self {
            output,
            header,
            cursor: HEADER_SIZE,
            used_len: 0,
            record_count: 0,
            block_open: false,
            index: Vec::new(),
        })
    }

    /// Number of complete bytes currently emitted (header and flushed data blocks).
    /// An unflushed data block is deliberately excluded from this recovery-safe prefix.
    pub fn committed_prefix_len(&self) -> usize {
        self.cursor
    }

    /// Physical data-region length including the reserved space for an open block.
    /// This is useful for enforcing a segment target before appending another entry.
    pub fn estimated_data_region_len(&self) -> usize {
        self.cursor.saturating_sub(HEADER_SIZE)
            + if self.block_open || self.used_len != 0 || self.record_count != 0 {
                BLOCK_SIZE
            } else {
                0
            }
    }

    /// Conservative physical data-region length after adding a record with this payload size.
    /// It mirrors the codec's record placement rules without mutating the encoder.
    pub fn estimated_data_region_len_after_record(
        &self,
        payload_len: usize,
    ) -> Result<usize, CodecError> {
        let record_size = RECORD_HEADER_SIZE
            .checked_add(payload_len)
            .ok_or(CodecError::IntegerOverflow)?;
        let spans_blocks = record_size > BLOCK_DATA_SIZE;
        let flush_first =
            spans_blocks || BLOCK_DATA_SIZE.saturating_sub(self.used_len) < record_size;
        let has_pending = self.used_len != 0 || self.record_count != 0;
        let extra_block = if flush_first && has_pending {
            BLOCK_SIZE
        } else {
            0
        };
        let start_cursor = self
            .cursor
            .checked_add(extra_block)
            .ok_or(CodecError::IntegerOverflow)?;
        let start_used = if flush_first { 0 } else { self.used_len };
        let blocks = start_used
            .checked_add(record_size)
            .and_then(|n| n.checked_add(BLOCK_DATA_SIZE - 1))
            .ok_or(CodecError::IntegerOverflow)?
            / BLOCK_DATA_SIZE;
        let end = start_cursor
            .checked_add(
                blocks
                    .checked_mul(BLOCK_SIZE)
                    .ok_or(CodecError::IntegerOverflow)?,
            )
            .ok_or(CodecError::IntegerOverflow)?;
        Ok(end.saturating_sub(HEADER_SIZE))
    }

    /// Borrow the 4 KiB segment header in the encoder's caller-owned buffer.
    pub fn header_bytes(&self) -> &[u8] {
        &self.output[..HEADER_SIZE]
    }

    /// Borrow one complete, finalized 4 KiB data block by its absolute aligned offset.
    /// The block must be before `committed_prefix_len()`; open blocks are rejected.
    pub fn data_block_bytes(&self, offset: usize) -> Result<&[u8], CodecError> {
        if offset < HEADER_SIZE || offset % BLOCK_SIZE != 0 {
            return Err(CodecError::InvalidField("data block offset/alignment"));
        }
        let end = offset
            .checked_add(BLOCK_SIZE)
            .ok_or(CodecError::IntegerOverflow)?;
        if end > self.cursor {
            return Err(CodecError::InvalidField("data block is not finalized"));
        }
        Ok(&self.output[offset..end])
    }

    /// Finalize the currently open data block, if any, and return its byte offset.
    /// No I/O is performed; RAN-127 submits this block through its I/O backend.
    pub fn flush_pending_data_block(&mut self) -> Result<Option<usize>, CodecError> {
        if self.used_len == 0 && self.record_count == 0 {
            return Ok(None);
        }
        let offset = self.cursor;
        self.flush_block()?;
        Ok(Some(offset))
    }

    fn ensure_block_open(&mut self) -> Result<(), CodecError> {
        if self.block_open {
            return Ok(());
        }
        let end = self
            .cursor
            .checked_add(BLOCK_SIZE)
            .ok_or(CodecError::IntegerOverflow)?;
        if end > self.output.len() {
            return Err(CodecError::OutputBufferTooSmall {
                required: end,
                available: self.output.len(),
            });
        }
        self.output[self.cursor..end].fill(0);
        self.block_open = true;
        Ok(())
    }

    fn write_data(&mut self, data: &[u8]) -> Result<(), CodecError> {
        if data.len() > BLOCK_DATA_SIZE.saturating_sub(self.used_len) {
            return Err(CodecError::InvalidField("record header crosses block"));
        }
        self.ensure_block_open()?;
        let start = self.cursor + BLOCK_HEADER_SIZE + self.used_len;
        self.output[start..start + data.len()].copy_from_slice(data);
        self.used_len += data.len();
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), CodecError> {
        if self.used_len == 0 && self.record_count == 0 {
            return Ok(());
        }
        self.ensure_block_open()?;
        let end = self.cursor + BLOCK_SIZE;
        let wire = BlockHeaderWire {
            magic: *BLOCK_MAGIC,
            record_count: self.record_count.to_le(),
            flags: 0,
            used_len: (self.used_len as u32).to_le(),
            crc32c: 0,
        };
        self.output[self.cursor..self.cursor + BLOCK_HEADER_SIZE].copy_from_slice(wire.as_bytes());
        let crc = crc32c_with_zeroed_u32(&self.output[self.cursor..end], 12);
        self.output[self.cursor + 12..self.cursor + 16].copy_from_slice(&crc.to_le_bytes());
        self.cursor = end;
        self.used_len = 0;
        self.record_count = 0;
        self.block_open = false;
        Ok(())
    }

    pub fn push_record(
        &mut self,
        ledger: u64,
        entry: u64,
        flags: u8,
        payload: &Bytes,
    ) -> Result<(), CodecError> {
        self.push_record_with_span(ledger, entry, flags, payload)
            .map(|_| ())
    }

    /// Append a record and report the physical blocks containing it. The record header
    /// is never split across blocks; a multi-block payload may continue through later blocks.
    pub fn push_record_with_span(
        &mut self,
        ledger: u64,
        entry: u64,
        flags: u8,
        payload: &Bytes,
    ) -> Result<RecordSpan, CodecError> {
        let len = u32::try_from(payload.len()).map_err(|_| CodecError::IntegerOverflow)?;
        let record_size = RECORD_HEADER_SIZE
            .checked_add(payload.len())
            .ok_or(CodecError::IntegerOverflow)?;
        let spans_blocks = record_size > BLOCK_DATA_SIZE;
        let flush_first = spans_blocks || BLOCK_DATA_SIZE - self.used_len < record_size;
        let has_pending = self.used_len != 0 || self.record_count != 0;
        let (start_cursor, start_used) = match (flush_first, has_pending) {
            (true, true) => (self.cursor + BLOCK_SIZE, 0),
            (true, false) => (self.cursor, 0),
            (false, _) => (self.cursor, self.used_len),
        };
        let blocks = (start_used + record_size).div_ceil(BLOCK_DATA_SIZE);
        let required = blocks
            .checked_mul(BLOCK_SIZE)
            .and_then(|n| start_cursor.checked_add(n))
            .ok_or(CodecError::IntegerOverflow)?;
        if required > self.output.len() {
            return Err(CodecError::OutputBufferTooSmall {
                required,
                available: self.output.len(),
            });
        }
        if flush_first {
            self.flush_block()?;
        }
        self.ensure_block_open()?;
        let record_offset = self.cursor + BLOCK_HEADER_SIZE + self.used_len;
        let mut raw = [0u8; RECORD_HEADER_SIZE];
        append_record_header(
            &mut raw,
            RecordHeader {
                len,
                ledger,
                entry,
                flags,
            },
        );
        self.write_data(&raw)?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or(CodecError::InvalidField("block record count overflow"))?;
        self.index.push(FooterIndexEntry {
            ledger,
            entry,
            offset: record_offset as u64,
            len,
        });

        let mut copied = 0usize;
        while copied < payload.len() {
            if self.used_len == BLOCK_DATA_SIZE {
                self.flush_block()?;
            }
            self.ensure_block_open()?;
            let count = (BLOCK_DATA_SIZE - self.used_len).min(payload.len() - copied);
            let start = self.cursor + BLOCK_HEADER_SIZE + self.used_len;
            self.output[start..start + count].copy_from_slice(&payload[copied..copied + count]);
            self.used_len += count;
            copied += count;
        }
        Ok(RecordSpan {
            first_block_offset: (record_offset / BLOCK_SIZE) * BLOCK_SIZE,
            last_block_offset: self.cursor,
        })
    }

    /// Finalize the footer and trailer in-place. The returned file length is always
    /// a multiple of 4 KiB; zero padding is placed between footer and trailer so the
    /// complete tail can be issued with O_DIRECT without a partial final write.
    pub fn seal(self) -> Result<usize, CodecError> {
        self.seal_and_return_buffer().map(|(_, len)| len)
    }

    /// Seal in-place and return the caller-owned buffer as well as the final file length.
    /// Returning the buffer lets the segment writer submit the footer/trailer with O_DIRECT
    /// after all data blocks have been durably committed.
    pub fn seal_and_return_buffer(mut self) -> Result<(&'a mut [u8], usize), CodecError> {
        self.flush_block()?;
        self.index.sort_by_key(|item| (item.ledger, item.entry));
        for pair in self.index.windows(2) {
            if pair[0].ledger == pair[1].ledger && pair[0].entry == pair[1].entry {
                return Err(CodecError::DuplicateEntry {
                    ledger: pair[0].ledger,
                    entry: pair[0].entry,
                });
            }
        }
        let mut ledger_count = 0usize;
        let mut last_ledger = None;
        for item in &self.index {
            if last_ledger != Some(item.ledger) {
                ledger_count = ledger_count
                    .checked_add(1)
                    .ok_or(CodecError::IntegerOverflow)?;
                last_ledger = Some(item.ledger);
            }
        }
        let footer_len = core::mem::size_of::<FooterHeaderWire>()
            .checked_add(
                self.index
                    .len()
                    .checked_mul(core::mem::size_of::<FooterIndexWire>())
                    .ok_or(CodecError::IntegerOverflow)?,
            )
            .and_then(|n| {
                n.checked_add(ledger_count.checked_mul(core::mem::size_of::<LedgerSummaryWire>())?)
            })
            .and_then(|n| n.checked_add(4))
            .ok_or(CodecError::IntegerOverflow)?;
        let footer_len_u32 = u32::try_from(footer_len).map_err(|_| CodecError::IntegerOverflow)?;
        let tail_len = align_up(
            footer_len
                .checked_add(TRAILER_SIZE)
                .ok_or(CodecError::IntegerOverflow)?,
            BLOCK_SIZE,
        )?;
        let tail_end = self
            .cursor
            .checked_add(tail_len)
            .ok_or(CodecError::IntegerOverflow)?;
        if tail_end > self.output.len() {
            return Err(CodecError::OutputBufferTooSmall {
                required: tail_end,
                available: self.output.len(),
            });
        }
        let footer_start = self.cursor;
        let trailer_start = tail_end - TRAILER_SIZE;
        self.output[footer_start..tail_end].fill(0);

        let footer_header = FooterHeaderWire {
            magic: *FOOTER_MAGIC,
            version: FORMAT_VERSION.to_le(),
            header_len: (core::mem::size_of::<FooterHeaderWire>() as u16).to_le(),
            index_count: u32::try_from(self.index.len())
                .map_err(|_| CodecError::IntegerOverflow)?
                .to_le(),
            ledger_count: u32::try_from(ledger_count)
                .map_err(|_| CodecError::IntegerOverflow)?
                .to_le(),
            data_blocks: u32::try_from((footer_start - HEADER_SIZE) / BLOCK_SIZE)
                .map_err(|_| CodecError::IntegerOverflow)?
                .to_le(),
            flags: 0,
        };
        let mut at = footer_start;
        self.output[at..at + core::mem::size_of::<FooterHeaderWire>()]
            .copy_from_slice(footer_header.as_bytes());
        at += core::mem::size_of::<FooterHeaderWire>();
        for item in &self.index {
            let wire = FooterIndexWire {
                ledger: item.ledger.to_le(),
                entry: item.entry.to_le(),
                offset: item.offset.to_le(),
                len: item.len.to_le(),
                reserved: 0,
            };
            self.output[at..at + core::mem::size_of::<FooterIndexWire>()]
                .copy_from_slice(wire.as_bytes());
            at += core::mem::size_of::<FooterIndexWire>();
        }
        let mut i = 0usize;
        while i < self.index.len() {
            let ledger = self.index[i].ledger;
            let mut min_entry = self.index[i].entry;
            let mut max_entry = min_entry;
            i += 1;
            while i < self.index.len() && self.index[i].ledger == ledger {
                min_entry = min_entry.min(self.index[i].entry);
                max_entry = max_entry.max(self.index[i].entry);
                i += 1;
            }
            let wire = LedgerSummaryWire {
                ledger: ledger.to_le(),
                min_entry: min_entry.to_le(),
                max_entry: max_entry.to_le(),
            };
            self.output[at..at + core::mem::size_of::<LedgerSummaryWire>()]
                .copy_from_slice(wire.as_bytes());
            at += core::mem::size_of::<LedgerSummaryWire>();
        }
        debug_assert_eq!(at + 4, footer_start + footer_len);
        let footer_crc = crc32c::crc32c(&self.output[footer_start..at]);
        self.output[at..at + 4].copy_from_slice(&footer_crc.to_le_bytes());
        let trailer = TrailerWire {
            magic: *TRAILER_MAGIC,
            footer_start: (footer_start as u64).to_le(),
            footer_len: footer_len_u32.to_le(),
            footer_crc32c: footer_crc.to_le(),
            version: FORMAT_VERSION.to_le(),
            trailer_len: (TRAILER_SIZE as u16).to_le(),
            trailer_crc32c: 0,
        };
        self.output[trailer_start..tail_end].copy_from_slice(trailer.as_bytes());
        let trailer_crc = crc32c_with_zeroed_u32(&self.output[trailer_start..tail_end], 28);
        self.output[trailer_start + 28..tail_end].copy_from_slice(&trailer_crc.to_le_bytes());
        let _ = self.header;
        Ok((self.output, tail_end))
    }
}

/// Return a valid prefix for an open or truncated segment. This never treats bytes
/// after the first invalid/zero block as committed codec data.
pub fn scan_recovery(bytes: Bytes) -> RecoveryPoint {
    SegmentView::recover_open(bytes).point
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn header() -> SegmentHeader {
        SegmentHeader {
            segment_id: 42,
            writer_epoch: 7,
            target_size: 16 * 1024 * 1024,
            created_at_ns: 1_700_000_000,
        }
    }

    #[test]
    fn round_trip_records_and_footer() {
        let mut enc = SegmentEncoder::new(header());
        enc.push_record(2, 9, 1, &Bytes::from_static(b"later ledger"))
            .unwrap();
        enc.push_record(1, 3, 0, &Bytes::from_static(b"first"))
            .unwrap();
        enc.push_record(1, 8, 2, &Bytes::from(vec![0x5a; 11_000]))
            .unwrap();
        let bytes = enc.seal().unwrap();
        assert_eq!(
            bytes.len() % BLOCK_SIZE,
            0,
            "sealed file length is direct-I/O aligned"
        );
        let view = SegmentView::open_sealed(bytes.clone()).unwrap();
        assert_eq!(view.header(), header());
        let records: Vec<_> = view.records().map(Result::unwrap).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(
            (records[0].ledger, records[0].entry, records[0].flags),
            (2, 9, 1)
        );
        assert_eq!((records[1].ledger, records[1].entry), (1, 3));
        let mut big = Vec::new();
        records[2].copy_payload_to(&mut big).unwrap();
        assert_eq!(big, vec![0x5a; 11_000]);
        let footer = view.footer().unwrap();
        let keys: Vec<_> = footer
            .index_entries()
            .map(|e| (e.ledger, e.entry))
            .collect();
        assert_eq!(keys, vec![(1, 3), (1, 8), (2, 9)]);
        assert_eq!(
            footer.ledger_summaries().collect::<Vec<_>>(),
            vec![
                LedgerSummary {
                    ledger: 1,
                    min_entry: 3,
                    max_entry: 8
                },
                LedgerSummary {
                    ledger: 2,
                    min_entry: 9,
                    max_entry: 9
                },
            ]
        );
    }

    #[test]
    fn aligned_encoder_writes_in_place_and_matches_reference_encoder() {
        let payloads = [
            (1u64, 4u64, 0u8, Bytes::from_static(b"small payload")),
            (1u64, 2u64, 3u8, Bytes::from(vec![0x7bu8; 12_000])),
            (2u64, 9u64, 1u8, Bytes::from_static(b"tail")),
        ];
        let mut reference = SegmentEncoder::new(header());
        for (ledger, entry, flags, payload) in &payloads {
            reference
                .push_record(*ledger, *entry, *flags, payload)
                .unwrap();
        }
        let reference_bytes = reference.seal().unwrap();

        let capacity = 128 * 1024;
        let mut storage = vec![0u8; capacity + BLOCK_SIZE];
        let base = storage.as_ptr() as usize;
        let offset = (BLOCK_SIZE - base % BLOCK_SIZE) % BLOCK_SIZE;
        let encoded_len = {
            let output = &mut storage[offset..offset + capacity];
            assert!(is_4k_aligned_buffer(output));
            let mut encoder = AlignedSegmentEncoder::new(header(), output).unwrap();
            for (ledger, entry, flags, payload) in &payloads {
                encoder
                    .push_record(*ledger, *entry, *flags, payload)
                    .unwrap();
            }
            encoder.seal().unwrap()
        };
        assert_eq!(encoded_len % BLOCK_SIZE, 0);
        assert_eq!(
            &storage[offset..offset + encoded_len],
            reference_bytes.as_ref()
        );
        let view = SegmentView::open_sealed(Bytes::copy_from_slice(
            &storage[offset..offset + encoded_len],
        ))
        .unwrap();
        assert_eq!(view.records().count(), payloads.len());
    }

    #[test]
    fn aligned_encoder_rejects_unaligned_output() {
        let mut storage = vec![0u8; BLOCK_SIZE * 3 + 1];
        let base = storage.as_ptr() as usize;
        let aligned_start = (BLOCK_SIZE - base % BLOCK_SIZE) % BLOCK_SIZE;
        assert!(matches!(
            AlignedSegmentEncoder::new(
                header(),
                &mut storage[aligned_start + 1..aligned_start + 1 + BLOCK_SIZE * 2]
            ),
            Err(CodecError::InvalidField(_))
        ));
    }

    #[test]
    fn record_parser_borrows_payload_without_copying() {
        let payload = b"zero-copy-payload";
        let mut encoded = vec![0u8; RECORD_HEADER_SIZE + payload.len()];
        append_record_header(
            &mut encoded[..RECORD_HEADER_SIZE],
            RecordHeader {
                len: payload.len() as u32,
                ledger: 11,
                entry: 12,
                flags: 3,
            },
        );
        encoded[RECORD_HEADER_SIZE..].copy_from_slice(payload);
        let parsed = parse_record(&encoded).unwrap();
        assert_eq!(
            parsed.header,
            RecordHeader {
                len: payload.len() as u32,
                ledger: 11,
                entry: 12,
                flags: 3
            }
        );
        assert_eq!(parsed.payload, payload);
        assert_eq!(
            parsed.payload.as_ptr(),
            encoded[RECORD_HEADER_SIZE..].as_ptr()
        );
    }

    #[test]
    fn rejects_duplicate_key_on_seal() {
        let mut enc = SegmentEncoder::new(header());
        enc.push_record(1, 4, 0, &Bytes::new()).unwrap();
        enc.push_record(1, 4, 0, &Bytes::new()).unwrap();
        assert!(matches!(enc.seal(), Err(CodecError::DuplicateEntry { .. })));
    }

    #[test]
    fn recovery_accepts_only_complete_blocks_and_records() {
        let mut enc = SegmentEncoder::new(header());
        enc.push_record(1, 0, 0, &Bytes::from_static(b"one"))
            .unwrap();
        enc.push_record(1, 1, 0, &Bytes::from(vec![9u8; 10_000]))
            .unwrap();
        let sealed = enc.seal().unwrap();
        let trailer_start = sealed.len() - TRAILER_SIZE;
        let footer_start = u64::from_le_bytes(
            sealed[trailer_start + 8..trailer_start + 16]
                .try_into()
                .unwrap(),
        ) as usize;
        let data_end = footer_start;
        for cut in 0..=sealed.len() {
            let truncated = sealed.slice(..cut);
            let result = SegmentView::recover_open(truncated);
            assert!(result.point.valid_len <= cut);
            if result.point.valid_len != 0 {
                assert!(result.point.valid_len >= HEADER_SIZE);
                assert_eq!((result.point.valid_len - HEADER_SIZE) % BLOCK_SIZE, 0);
            }
            let mut decoded = 0usize;
            if let Some(view) = result.view {
                for item in view.records() {
                    item.unwrap();
                    decoded += 1;
                }
            }
            assert_eq!(decoded, result.point.complete_records);
        }
        assert!(data_end < sealed.len());
    }

    #[test]
    fn sealed_tail_padding_is_zero_and_file_length_is_aligned() {
        let mut enc = SegmentEncoder::new(header());
        enc.push_record(1, 1, 0, &Bytes::from_static(b"payload"))
            .unwrap();
        let sealed = enc.seal().unwrap();
        assert_eq!(sealed.len() % BLOCK_SIZE, 0);
        let trailer_start = sealed.len() - TRAILER_SIZE;
        let footer_start = u64::from_le_bytes(
            sealed[trailer_start + 8..trailer_start + 16]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_len = u32::from_le_bytes(
            sealed[trailer_start + 16..trailer_start + 20]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_end = footer_start + footer_len;
        assert!(footer_end < trailer_start);
        let mut corrupted = sealed.to_vec();
        corrupted[footer_end] = 1;
        assert!(SegmentView::open_sealed(Bytes::from(corrupted)).is_err());
        assert!(SegmentView::open_sealed(sealed.slice(..sealed.len() - 1)).is_err());
    }

    #[test]
    fn detects_block_crc_damage() {
        let mut enc = SegmentEncoder::new(header());
        enc.push_record(1, 1, 0, &Bytes::from_static(b"payload"))
            .unwrap();
        let mut data = enc.seal().unwrap().to_vec();
        data[HEADER_SIZE + BLOCK_HEADER_SIZE + RECORD_HEADER_SIZE] ^= 0x40;
        assert!(SegmentView::open_sealed(Bytes::from(data)).is_err());
    }

    proptest! {
        #[test]
        fn record_header_parser_never_panics(data in proptest::collection::vec(any::<u8>(), 0..128)) {
            let _ = parse_record_header(&data);
            let _ = parse_record(&data);
        }

        #[test]
        fn block_parser_never_panics(data in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let _ = parse_block(&data);
        }
    }
}
