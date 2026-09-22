//! A tiny deterministic PRNG (SplitMix64). Both engine workers generate each
//! phase's operation stream from the same seed, so they replay byte-for-byte
//! identical operations — which is what lets the harness assert that the two
//! engines produce the same cross-engine checksum.

pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform index in `0..n` (returns 0 when `n == 0`).
    pub fn index(&mut self, n: u32) -> u32 {
        if n == 0 {
            0
        } else {
            (self.next_u64() >> 32) as u32 % n
        }
    }

    /// A uniform roll in `0..100`, for percentage-based choices.
    pub fn percent(&mut self) -> u32 {
        self.index(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_a_given_seed() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn index_stays_in_range() {
        let mut rng = SplitMix64::new(7);
        for _ in 0..10_000 {
            assert!(rng.index(13) < 13);
        }
        assert_eq!(rng.index(0), 0);
    }
}
