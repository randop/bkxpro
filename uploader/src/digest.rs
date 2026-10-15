//! Sealed-segment digest: the validated facts the uploader needs about a segment
//! file, derived only from its header, footer, and trailer.
//!
//! Both the local worker (aligned `SegmentIo` reads) and the S3 reader (ranged
//! GETs) build the digest through this one function, so a segment that validates
//! locally validates when it is read back from the object store.

use bkxpro_codec::{
    parse_segment_header, FooterView, TrailerWire, BLOCK_SIZE, HEADER_SIZE, TRAILER_SIZE,
};
use bytes::Bytes;
use thiserror::Error;

/// Errors from digest parsing. Every variant means "these bytes are not a sealed
/// segment"; callers must fail-stop rather than delete or overwrite anything.
#[derive(Debug, Error)]
pub enum DigestError {
    #[error("segment is {len} bytes; a sealed segment is a 4 KiB multiple of at least {minimum}")]
    TooSmall { len: u64, minimum: usize },
    #[error("segment length {0} is not a 4 KiB multiple")]
    Misaligned(u64),
    #[error("segment header: {0}")]
    Header(String),
    #[error("segment trailer: {0}")]
    Trailer(String),
    #[error("segment footer: {0}")]
    Footer(String),
    #[error("footer bounds are invalid: start {start}, len {len}, trailer at {tail}")]
    FooterBounds { start: u64, len: u32, tail: u64 },
    #[error("footer length {found} does not match the trailer's {declared}")]
    FooterLength { found: usize, declared: usize },
    #[error(
        "trailer footer CRC {trailer:#010x} does not match the footer's stored CRC {stored:#010x}"
    )]
    FooterCrc { trailer: u32, stored: u32 },
}

/// Parse the trailer of a sealed segment and validate the footer bounds it
/// declares. `tail_block` is the final 4 KiB block of the segment.
///
/// Layout: `[header block][data blocks][footer][zero padding][trailer]`, where
/// the footer starts at a block boundary and the 32-byte trailer ends the file.
/// The footer therefore may extend into the final block; only the trailer itself
/// is guaranteed to be outside it.
///
/// Readers that fetch the footer over the network call this first to learn the
/// byte range they must fetch; `SegmentDigest::parse` re-validates the same
/// bounds when the footer arrives.
pub fn footer_bounds(tail_block: &[u8], file_len: u64) -> Result<(u64, u32), DigestError> {
    if file_len < (HEADER_SIZE + BLOCK_SIZE) as u64 {
        return Err(DigestError::TooSmall {
            len: file_len,
            minimum: HEADER_SIZE + BLOCK_SIZE,
        });
    }
    if file_len % BLOCK_SIZE as u64 != 0 {
        return Err(DigestError::Misaligned(file_len));
    }
    if tail_block.len() < BLOCK_SIZE {
        return Err(DigestError::Trailer(format!(
            "trailer block is {} bytes, expected {BLOCK_SIZE}",
            tail_block.len()
        )));
    }
    let trailer = TrailerWire::parse(&tail_block[BLOCK_SIZE - TRAILER_SIZE..])
        .map_err(|error| DigestError::Trailer(error.to_string()))?;
    let trailer_start = file_len - TRAILER_SIZE as u64;
    let footer_start = u64::from_le(trailer.footer_start);
    let footer_len = u32::from_le(trailer.footer_len);
    // The encoder pads the tail so that the footer and trailer plus padding fill
    // a whole number of blocks; anything else is not a canonical sealed segment.
    let tail_len =
        (footer_len as u64 + TRAILER_SIZE as u64).div_ceil(BLOCK_SIZE as u64) * BLOCK_SIZE as u64;
    let footer_end = footer_start.checked_add(footer_len as u64);
    let tail_end = footer_start.checked_add(tail_len);
    if footer_len < 4
        || footer_start < HEADER_SIZE as u64
        || footer_start % BLOCK_SIZE as u64 != 0
        || footer_end.map_or(true, |end| end > trailer_start)
        || tail_end != Some(file_len)
    {
        return Err(DigestError::FooterBounds {
            start: footer_start,
            len: footer_len,
            tail: trailer_start,
        });
    }
    Ok((footer_start, footer_len))
}

/// Validated metadata of one sealed segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDigest {
    pub segment_id: u64,
    /// Writer epoch from the segment header (authoritative, unlike the index copy).
    pub writer_epoch: u64,
    /// Canonical sealed length in bytes, always a 4 KiB multiple.
    pub file_len: u64,
    /// Absolute offset of the footer section (block aligned).
    pub footer_start: u64,
    /// Footer length in bytes including its trailing CRC.
    pub footer_len: u32,
    pub min_ledger: u64,
    pub max_ledger: u64,
    pub entry_count: u64,
}

impl SegmentDigest {
    /// Last byte offset of the record data region; the footer and trailer live at
    /// or after this offset.
    pub fn data_end(&self) -> u64 {
        self.footer_start
    }

    /// Parse a digest from the first 4 KiB block, the last 4 KiB block, and the
    /// footer section (including its trailing CRC) of a sealed segment.
    pub fn parse(
        file_len: u64,
        header_block: &[u8],
        tail_block: &[u8],
        footer_bytes: Bytes,
    ) -> Result<Self, DigestError> {
        if file_len < (HEADER_SIZE + BLOCK_SIZE) as u64 {
            return Err(DigestError::TooSmall {
                len: file_len,
                minimum: HEADER_SIZE + BLOCK_SIZE,
            });
        }
        if file_len % BLOCK_SIZE as u64 != 0 {
            return Err(DigestError::Misaligned(file_len));
        }
        let header = parse_segment_header(header_block)
            .map_err(|error| DigestError::Header(error.to_string()))?;
        let (footer_start, footer_len) = footer_bounds(tail_block, file_len)?;
        if footer_bytes.len() != footer_len as usize {
            return Err(DigestError::FooterLength {
                found: footer_bytes.len(),
                declared: footer_len as usize,
            });
        }
        let trailer = TrailerWire::parse(&tail_block[BLOCK_SIZE - TRAILER_SIZE..])
            .map_err(|error| DigestError::Trailer(error.to_string()))?;
        let stored_crc = u32::from_le_bytes(
            footer_bytes[footer_bytes.len() - 4..]
                .try_into()
                .map_err(|_| DigestError::Footer("footer CRC is truncated".to_string()))?,
        );
        let trailer_crc = u32::from_le(trailer.footer_crc32c);
        if stored_crc != trailer_crc {
            return Err(DigestError::FooterCrc {
                trailer: trailer_crc,
                stored: stored_crc,
            });
        }
        let footer = FooterView::parse(footer_bytes)
            .map_err(|error| DigestError::Footer(error.to_string()))?;

        let (min_ledger, max_ledger) = footer
            .ledger_summaries()
            .fold(None, |range: Option<(u64, u64)>, summary| {
                Some(match range {
                    Some((min, max)) => (min.min(summary.ledger), max.max(summary.ledger)),
                    None => (summary.ledger, summary.ledger),
                })
            })
            .unwrap_or((0, 0));

        Ok(Self {
            segment_id: header.segment_id,
            writer_epoch: header.writer_epoch,
            file_len,
            footer_start,
            footer_len,
            min_ledger,
            max_ledger,
            entry_count: footer.index_count() as u64,
        })
    }

    /// Block-aligned read length that covers the footer section.
    pub fn footer_read_len(&self) -> usize {
        let end = self.footer_start + self.footer_len as u64;
        (end.div_ceil(BLOCK_SIZE as u64) * BLOCK_SIZE as u64 - self.footer_start) as usize
    }
}

#[cfg(test)]
mod tests {
    use bkxpro_codec::{SegmentEncoder, SegmentHeader};

    use super::*;

    fn sealed(segment_id: u64, epoch: u64, entries: u64) -> Bytes {
        let header = SegmentHeader {
            segment_id,
            writer_epoch: epoch,
            target_size: 64 * 1024,
            created_at_ns: 0,
        };
        let mut encoder = SegmentEncoder::new(header);
        for entry in 1..=entries {
            encoder
                .push_record(1, entry, 0, &Bytes::from(vec![7u8; 32]))
                .unwrap();
        }
        encoder.seal().unwrap()
    }

    /// Slice one image the way a reader would: first block, last block, footer.
    fn digest_of(image: &Bytes) -> Result<SegmentDigest, DigestError> {
        let file_len = image.len() as u64;
        let trailer_start = image.len() - TRAILER_SIZE;
        let trailer = TrailerWire::parse(&image[trailer_start..]).expect("valid image trailer");
        let footer_start = u64::from_le(trailer.footer_start) as usize;
        let footer_len = u32::from_le(trailer.footer_len) as usize;
        SegmentDigest::parse(
            file_len,
            &image[..BLOCK_SIZE],
            &image[image.len() - BLOCK_SIZE..],
            image.slice(footer_start..footer_start + footer_len),
        )
    }

    #[test]
    fn parses_a_valid_sealed_segment() {
        let image = sealed(5, 42, 4);
        let digest = digest_of(&image).unwrap();
        assert_eq!(digest.segment_id, 5);
        assert_eq!(digest.writer_epoch, 42);
        assert_eq!(digest.file_len, image.len() as u64);
        assert_eq!(digest.entry_count, 4);
        assert_eq!((digest.min_ledger, digest.max_ledger), (1, 1));
        assert_eq!(digest.footer_start % BLOCK_SIZE as u64, 0);
        assert!(digest.footer_read_len() >= digest.footer_len as usize);
    }

    #[test]
    fn rejects_corruption() {
        let mut image = sealed(6, 1, 2).to_vec();
        let digest = digest_of(&Bytes::from(image.clone())).unwrap();

        // Flip a bit inside the footer: the footer CRC no longer matches.
        let target = digest.footer_start as usize + 8;
        image[target] ^= 0xff;
        let error = digest_of(&Bytes::from(image.clone())).unwrap_err();
        assert!(matches!(
            error,
            DigestError::FooterCrc { .. } | DigestError::Footer(_)
        ));

        // Truncate the image to break the trailer.
        let mut truncated = sealed(7, 1, 2).to_vec();
        truncated.truncate(truncated.len() - BLOCK_SIZE);
        let file_len = truncated.len() as u64;
        let error = SegmentDigest::parse(
            file_len,
            &truncated[..BLOCK_SIZE],
            &truncated[truncated.len() - BLOCK_SIZE..],
            Bytes::copy_from_slice(&truncated[truncated.len() - 64..]),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DigestError::Trailer(_)
                | DigestError::FooterCrc { .. }
                | DigestError::FooterBounds { .. }
        ));

        // A file shorter than one block is not a segment at all.
        let error =
            SegmentDigest::parse(1024, &[0u8; 1024], &[0u8; 1024], Bytes::new()).unwrap_err();
        assert!(matches!(error, DigestError::TooSmall { .. }));
    }
}
