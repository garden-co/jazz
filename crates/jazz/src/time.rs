//! Distinct monotone ordering values used by Jazz. This module owns transaction
//! HLC packing (`TxTime`) and authority serialization positions (`GlobalTime`);
//! clock mutation and skew checks live in [`crate::node::ingest`] and
//! [`crate::node::open_tx`], while merge/currency interpretation lives in
//! [`crate::node::currency`]. The types flow from facade writes through protocol
//! records down into groove storage keys.

use crate::ids::NodeUuid;

/// Core-assigned hybrid logical timestamp for globally accepted transactions.
///
/// The high 48 bits contain physical milliseconds and the low 16 bits contain
/// a logical counter, matching [`TxTime`]'s compact representation while
/// remaining a distinct domain.
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
pub struct GlobalTime(pub u64);

impl GlobalTime {
    const COUNTER_BITS: u64 = 16;
    const COUNTER_MASK: u64 = (1 << Self::COUNTER_BITS) - 1;
    const MAX_PHYSICAL_MS: u64 = (1 << 48) - 1;

    /// Pack physical milliseconds and a logical counter into one value.
    pub fn new(physical_ms: u64, counter: u16) -> Option<Self> {
        (physical_ms <= Self::MAX_PHYSICAL_MS).then_some(Self(
            (physical_ms << Self::COUNTER_BITS) | u64::from(counter),
        ))
    }

    /// Return the physical-millisecond component.
    pub fn physical_ms(self) -> u64 {
        self.0 >> Self::COUNTER_BITS
    }

    /// Return the logical-counter component.
    pub fn counter(self) -> u16 {
        (self.0 & Self::COUNTER_MASK) as u16
    }

    /// Mint a timestamp strictly after `register` using the supplied wall time.
    pub fn tick(register: Self, now_ms: u64) -> Option<Self> {
        if now_ms > register.physical_ms() {
            Self::new(now_ms, 0)
        } else {
            Self::new(register.physical_ms(), register.counter().checked_add(1)?)
        }
    }

    pub(crate) fn authority_now_ms(now_ms: u64, fallback_ms: u64) -> u64 {
        if now_ms <= Self::MAX_PHYSICAL_MS {
            now_ms
        } else {
            fallback_ms.min(Self::MAX_PHYSICAL_MS)
        }
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
    fn global_time_packs_and_ticks_monotonically() {
        let initial = GlobalTime::new(0x1234_5678_9abc, 0xdef0).unwrap();
        assert_eq!(initial.physical_ms(), 0x1234_5678_9abc);
        assert_eq!(initial.counter(), 0xdef0);

        assert_eq!(
            GlobalTime::tick(initial, 0x1234_5678_9abd),
            GlobalTime::new(0x1234_5678_9abd, 0)
        );
        assert_eq!(
            GlobalTime::tick(initial, 0x1234_5678_9abb),
            GlobalTime::new(0x1234_5678_9abc, 0xdef1)
        );
    }

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
