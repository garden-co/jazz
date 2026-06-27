//! Deterministic rand-free distributions for fixture and workload generation.

/// Small deterministic pseudo-random generator used by benchmark fixtures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lcg {
    state: u64,
}

impl Lcg {
    /// Construct a generator from a stable seed.
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    /// Return the next `u64`.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    /// Uniform integer in `0..upper`.
    pub fn usize(&mut self, upper: usize) -> usize {
        assert!(upper > 0, "upper bound must be non-zero");
        (self.next_u64() % upper as u64) as usize
    }

    /// Uniform integer in `start..=end`.
    pub fn range_usize(&mut self, start: usize, end: usize) -> usize {
        assert!(start <= end, "invalid inclusive range");
        start + self.usize(end - start + 1)
    }

    /// Return true with probability `numerator / denominator`.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        assert!(denominator > 0, "denominator must be non-zero");
        (self.next_u64() % denominator) < numerator
    }

    /// Choose one item uniformly.
    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.usize(items.len())]
    }

    /// Choose an index using integer weights.
    pub fn weighted_index(&mut self, weights: &[u64]) -> usize {
        let total = weights
            .iter()
            .copied()
            .reduce(u64::saturating_add)
            .expect("weights must be non-empty");
        assert!(total > 0, "at least one weight must be positive");
        let mut draw = self.next_u64() % total;
        for (idx, weight) in weights.iter().copied().enumerate() {
            if draw < weight {
                return idx;
            }
            draw -= weight;
        }
        weights.len() - 1
    }
}

/// Table-based Zipf sampler over ranks `0..n`.
#[derive(Clone, Debug)]
pub struct Zipf {
    cumulative: Vec<u64>,
}

impl Zipf {
    /// Precompute a deterministic Zipf sampler with exponent `s`.
    pub fn new(n: usize, s: f64) -> Self {
        assert!(n > 0, "zipf n must be non-zero");
        assert!(s > 0.0, "zipf exponent must be positive");
        let weights = (0..n)
            .map(|idx| 1.0 / ((idx + 1) as f64).powf(s))
            .collect::<Vec<_>>();
        let total = weights.iter().sum::<f64>();
        let mut cumulative = Vec::with_capacity(n);
        let mut acc = 0_u64;
        for weight in weights {
            let scaled = ((weight / total) * u64::MAX as f64).max(1.0) as u64;
            acc = acc.saturating_add(scaled);
            cumulative.push(acc);
        }
        if let Some(last) = cumulative.last_mut() {
            *last = u64::MAX;
        }
        Self { cumulative }
    }

    /// Draw a rank in `0..n`.
    pub fn sample(&self, rng: &mut Lcg) -> usize {
        let draw = rng.next_u64();
        self.cumulative.partition_point(|cut| *cut < draw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lcg_repeats_for_same_seed() {
        let mut a = Lcg::new(7);
        let mut b = Lcg::new(7);
        for _ in 0..32 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn zipf_is_deterministic_and_skewed() {
        let zipf = Zipf::new(10, 1.2);
        let mut a = Lcg::new(99);
        let mut b = Lcg::new(99);
        let mut counts = [0_u64; 10];
        for _ in 0..1_000 {
            let left = zipf.sample(&mut a);
            assert_eq!(left, zipf.sample(&mut b));
            counts[left] += 1;
        }
        assert!(counts[0] > counts[1]);
        assert!(counts[1] > counts[9]);
    }
}
