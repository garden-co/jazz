use xxhash_rust::xxh32;

pub(crate) struct Hasher {
    state: xxh32::Xxh32,
}

impl Hasher {
    pub(crate) fn new() -> Self {
        Self {
            state: xxh32::Xxh32::new(0),
        }
    }

    pub(crate) fn update(&mut self, data: &[u8]) {
        self.state.update(data);
    }

    pub(crate) fn finalize(self) -> u32 {
        self.state.digest()
    }
}

#[inline]
pub(crate) fn hash(bytes: &[u8]) -> u32 {
    xxh32::xxh32(bytes, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxh32_standard_test_vectors() {
        assert_eq!(hash(b""), 0x02CC_5D05);
        assert_eq!(hash(b"123456789"), 0x937B_AD67);
    }

    #[test]
    fn streaming_matches_one_shot() {
        let data = b"hello world, this is a test of streaming xxhash32";
        let one_shot = hash(data);

        for split in 0..=data.len() {
            let mut h = Hasher::new();
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finalize(), one_shot, "split={split}");
        }
    }

    #[test]
    fn streaming_matches_one_shot_large() {
        let mut data = vec![0u8; 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        let one_shot = hash(&data);

        for split in (0..=128).chain([256, 512, 513, 1023, 1024].iter().copied()) {
            let split = split.min(data.len());
            let mut h = Hasher::new();
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finalize(), one_shot, "split={split}");
        }
    }

    #[test]
    fn streaming_skip_segment_differs() {
        let full = b"AAAAbbbbCCCCCCCC";
        let full_hash = hash(full);

        let mut h = Hasher::new();
        h.update(&full[..4]);
        h.update(&full[8..]);
        let skip_hash = h.finalize();

        assert_ne!(full_hash, skip_hash);
    }

    #[test]
    fn hash_deterministic() {
        assert_eq!(hash(b"determinism"), hash(b"determinism"));
    }
}
