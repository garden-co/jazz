//! Distinct monotone ordering values used by Jazz. This module owns transaction
//! HLC packing (`TxTime`) and authority serialization positions (`GlobalSeq`);
//! clock mutation and skew checks live in [`crate::node::ingest`] and
//! [`crate::node::open_tx`], while merge/currency interpretation lives in
//! [`crate::node::currency`]. The types flow from facade writes through protocol
//! records down into groove storage keys.

use crate::ids::NodeUuid;

/// Core-assigned serialization point for globally accepted transactions.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct GlobalSeq(pub u64);

impl GlobalSeq {
    /// Return the next global sequence value.
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Hybrid logical timestamp packed as physical milliseconds plus logical counter.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct TxTime(pub u64);

impl TxTime {
    const COUNTER_BITS: u64 = 16;
    const COUNTER_MASK: u64 = (1 << Self::COUNTER_BITS) - 1;
    const MAX_PHYSICAL_MS: u64 = (1 << 48) - 1;

    /// Construct a hybrid logical clock value.
    pub fn new(physical_ms: u64, counter: u32) -> Self {
        assert!(
            physical_ms <= Self::MAX_PHYSICAL_MS,
            "HLC physical component exceeds 48-bit packed range"
        );
        assert!(
            counter <= Self::COUNTER_MASK as u32,
            "HLC logical counter exceeds 16-bit packed range"
        );
        Self((physical_ms << Self::COUNTER_BITS) | u64::from(counter))
    }

    /// Physical milliseconds component.
    pub fn physical_ms(self) -> u64 {
        self.0 >> Self::COUNTER_BITS
    }

    /// Logical counter component.
    pub fn counter(self) -> u16 {
        (self.0 & Self::COUNTER_MASK) as u16
    }

    /// Return a clock value immediately after this one.
    pub fn tick_after(self) -> Self {
        let counter = self
            .counter()
            .checked_add(1)
            .expect("HLC logical counter saturated while ticking after parent");
        Self::new(self.physical_ms(), u32::from(counter))
    }

    /// Mint the next local HLC from a register and abstract wall clock.
    pub fn tick(register: Self, now_ms: u64) -> Self {
        if now_ms > register.physical_ms() {
            Self::new(now_ms, 0)
        } else {
            let counter = register
                .counter()
                .checked_add(1)
                .expect("HLC logical counter saturated while minting transaction id");
            Self::new(register.physical_ms(), u32::from(counter))
        }
    }

    /// Return a total ordering key using the node as tie-breaker.
    pub fn sort_key(self, node: NodeUuid) -> TxTimeSortKey {
        TxTimeSortKey { time: self, node }
    }
}

impl From<u64> for TxTime {
    fn from(physical_ms: u64) -> Self {
        Self::new(physical_ms, 0)
    }
}

/// Total-order comparison key for domination's HLC-LWW tie break.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxTimeSortKey {
    /// Packed HLC time.
    pub time: TxTime,
    /// Node tie-breaker.
    pub node: NodeUuid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_time_packs_physical_millis_and_logical_counter() {
        let time = TxTime::new(0x1234_5678_9abc, 0xdef0);
        assert_eq!(time.0, 0x1234_5678_9abc_def0);
        assert_eq!(time.physical_ms(), 0x1234_5678_9abc);
        assert_eq!(time.counter(), 0xdef0);

        assert_eq!(
            TxTime::tick(time, 0x1234_5678_9abd),
            TxTime::new(0x1234_5678_9abd, 0)
        );
        assert_eq!(
            TxTime::tick(time, 0x1234_5678_9abb),
            TxTime::new(0x1234_5678_9abc, 0xdef1)
        );
        assert_eq!(time.tick_after(), TxTime::new(0x1234_5678_9abc, 0xdef1));
    }

    #[test]
    #[should_panic(expected = "HLC physical component exceeds 48-bit packed range")]
    fn tx_time_rejects_physical_millis_outside_packed_range() {
        TxTime::new(1 << 48, 0);
    }

    #[test]
    #[should_panic(expected = "HLC logical counter exceeds 16-bit packed range")]
    fn tx_time_rejects_counter_outside_packed_range() {
        TxTime::new(0, 1 << 16);
    }
}
