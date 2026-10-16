//! Deterministic RNG for reproducible seeds.
//!
//! SplitMix64 (public-domain algorithm by Sebastiano Vigna) with a per-call
//! derivation helper. Nothing here uses OS entropy or the global clock, so a seed
//! always replays the same decision sequence. Decisions that must not depend on
//! thread interleaving are derived from *content* (`seed ^ kind ^ key ^ attempt`)
//! rather than from a shared stream cursor; see [`DetRng::derive`].

/// Deterministic 64-bit generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetRng {
    state: u64,
}

impl DetRng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Derive an independent stream from this one plus a salt. The parent stream
    /// is not advanced, so derived streams are stable under any call order.
    pub fn derive(&self, salt: u64) -> Self {
        Self::new(self.state ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15))
    }

    /// Derive a stream from arbitrary bytes (stable across runs and platforms).
    pub fn derive_from(&self, parts: &[&[u8]]) -> Self {
        let mut hash = self.state;
        for part in parts {
            hash = fnv1a_extend(hash, part);
        }
        Self::new(hash)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Uniform in `0..bounds` (returns 0 when `bounds == 0`).
    pub fn below(&mut self, bounds: u64) -> u64 {
        if bounds == 0 {
            return 0;
        }
        self.next_u64() % bounds
    }

    /// Uniform in `low..=high`. Panics when the range is empty.
    pub fn range_inclusive(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "empty inclusive range {low}..={high}");
        low + self.below(high - low + 1)
    }

    /// True with probability `percent/100`.
    pub fn chance(&mut self, percent: u8) -> bool {
        percent > 0 && self.below(100) < u64::from(percent)
    }

    /// Pick one element, or `None` for an empty slice.
    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            Some(&items[self.below(items.len() as u64) as usize])
        }
    }

    /// Fill `len` deterministic bytes.
    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            out.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        out.truncate(len);
        out
    }
}

pub fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_extend(0xCBF2_9CE4_8422_2325, bytes)
}

pub fn fnv1a_extend(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

/// Stable 64-bit hash of a trace-like byte stream.
pub fn digest_bytes(chunks: &[impl AsRef<[u8]>]) -> u64 {
    let mut hash = 0xCBF2_9CE4_8422_2325;
    for chunk in chunks {
        hash = fnv1a_extend(hash, chunk.as_ref());
        hash = fnv1a_extend(hash, &[0xFF]);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences_are_reproducible() {
        let mut a = DetRng::new(42);
        let mut b = DetRng::new(42);
        for _ in 0..64 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn derived_streams_are_order_independent() {
        let rng = DetRng::new(7);
        let mut first = rng.derive(1);
        // Advancing one derived stream must not affect a sibling derivation.
        for _ in 0..8 {
            first.next_u64();
        }
        let a = rng.derive(2).next_u64();
        let b = rng.derive(2).next_u64();
        assert_eq!(a, b);
    }

    #[test]
    fn ranges_and_chances_stay_in_bounds() {
        let mut rng = DetRng::new(99);
        for _ in 0..1000 {
            let value = rng.range_inclusive(5, 9);
            assert!((5..=9).contains(&value));
        }
        assert!(!rng.chance(0));
        assert!(rng.chance(100));
    }
}
