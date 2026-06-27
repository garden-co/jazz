use xxhash_rust::xxh3;

pub(crate) struct Hasher {
    state: xxh3::Xxh3,
}

impl Hasher {
    pub(crate) fn new() -> Self {
        Self {
            state: xxh3::Xxh3::new(),
        }
    }

    pub(crate) fn update(&mut self, data: &[u8]) {
        self.state.update(data);
    }

    pub(crate) fn finalize(self) -> u32 {
        self.state.digest() as u32
    }
}

#[inline]
pub(crate) fn hash(bytes: &[u8]) -> u32 {
    xxh3::xxh3_64(bytes) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_matches_one_shot() {
        let data = b"hello world, this is a test of streaming xxh3";
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
