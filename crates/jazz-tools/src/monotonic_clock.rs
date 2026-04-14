use web_time::{SystemTime, UNIX_EPOCH};

/// Monotonic microsecond clock for write ordering.
#[derive(Debug, Clone, Default)]
pub struct MonotonicClock {
    last_timestamp: u64,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Guarantees each reservation is strictly greater than the previous one.
    pub fn reserve_timestamp(&mut self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_micros() as u64;

        self.last_timestamp = if now > self.last_timestamp {
            now
        } else {
            self.last_timestamp + 1
        };

        self.last_timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::MonotonicClock;

    #[test]
    fn reserved_timestamps_are_strictly_monotonic() {
        let mut clock = MonotonicClock::new();
        let first = clock.reserve_timestamp();
        let second = clock.reserve_timestamp();
        assert!(second > first);
    }
}
