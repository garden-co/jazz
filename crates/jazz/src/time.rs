//! Distinct monotone ordering values used by Jazz. This module owns transaction
//! HLC packing (`TxTime`) and authority serialization positions (`GlobalTime`);
//! clock mutation and skew checks live in [`crate::node::ingest`] and
//! [`crate::node::open_tx`], while merge/currency interpretation lives in
//! [`crate::node::currency`]. The types flow from facade writes through protocol
//! records down into groove storage keys.

use crate::ids::NodeUuid;

/// The packed HLC has 46 physical Unix-millisecond bits and 18 logical bits.
/// This reaches year 4200 while allowing 262,144 causally ordered ticks in one
/// millisecond on one node.
pub const HLC_PHYSICAL_BITS: u64 = 46;
/// Number of logical-counter bits in a packed HLC.
pub const HLC_COUNTER_BITS: u64 = 64 - HLC_PHYSICAL_BITS;
/// Maximum physical Unix millisecond representable by a packed HLC.
pub const HLC_MAX_PHYSICAL_MS: u64 = (1 << HLC_PHYSICAL_BITS) - 1;
/// Maximum logical counter representable by a packed HLC.
pub const HLC_MAX_LOGICAL_COUNTER: u32 = (1 << HLC_COUNTER_BITS) - 1;

/// The packed HLC exhausted both its physical and logical components.
///
/// This is a typed failure rather than a clock panic. It can occur only at the
/// year-4200 physical horizon after all logical positions in that millisecond
/// have also been consumed.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("packed HLC exhausted at physical millisecond {physical_ms}")]
pub struct HlcOverflow {
    /// The final representable physical Unix millisecond.
    pub physical_ms: u64,
}

/// A public Unix-millisecond value that cannot be represented by the packed
/// HLC's 46-bit physical component.
///
/// Unlike [`HlcOverflow`], this is an ingress/decoding error: no clock position
/// has been allocated.  Wire provenance is public Unix milliseconds, so every
/// conversion back into an internal [`TxTime`] must use this fallible boundary.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("physical millisecond {physical_ms} exceeds the packed HLC maximum {max_physical_ms}")]
pub struct HlcPhysicalMsOutOfRange {
    /// The unrepresentable public Unix-millisecond value.
    pub physical_ms: u64,
    /// The greatest physical value representable by the packed HLC.
    pub max_physical_ms: u64,
}

/// Core-assigned hybrid logical timestamp for globally accepted transactions.
///
/// The high 46 bits contain physical milliseconds and the low 18 bits contain
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
    /// Pack physical milliseconds and a logical counter into one value.
    pub fn new(physical_ms: u64, counter: u32) -> Option<Self> {
        (physical_ms <= HLC_MAX_PHYSICAL_MS && counter <= HLC_MAX_LOGICAL_COUNTER)
            .then_some(Self((physical_ms << HLC_COUNTER_BITS) | u64::from(counter)))
    }

    /// Return the physical-millisecond component.
    pub fn physical_ms(self) -> u64 {
        self.0 >> HLC_COUNTER_BITS
    }

    /// Return the logical-counter component.
    pub fn counter(self) -> u32 {
        (self.0 & u64::from(HLC_MAX_LOGICAL_COUNTER)) as u32
    }

    /// Mint a timestamp strictly after `register` using the supplied wall time.
    pub fn tick(register: Self, now_ms: u64) -> Result<Self, HlcOverflow> {
        tick_packed(register.physical_ms(), register.counter(), now_ms).map(
            |(physical_ms, counter)| Self((physical_ms << HLC_COUNTER_BITS) | u64::from(counter)),
        )
    }

    pub(crate) fn authority_now_ms(now_ms: u64, fallback_ms: u64) -> u64 {
        if now_ms <= HLC_MAX_PHYSICAL_MS {
            now_ms
        } else {
            fallback_ms.min(HLC_MAX_PHYSICAL_MS)
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
    /// Construct a hybrid logical clock value.
    pub fn new(physical_ms: u64, counter: u32) -> Self {
        assert!(
            physical_ms <= HLC_MAX_PHYSICAL_MS,
            "HLC physical component exceeds 46-bit packed range"
        );
        assert!(
            counter <= HLC_MAX_LOGICAL_COUNTER,
            "HLC logical counter exceeds 18-bit packed range"
        );
        Self((physical_ms << HLC_COUNTER_BITS) | u64::from(counter))
    }

    /// Reconstruct a provenance HLC from a public Unix-millisecond value.
    ///
    /// Wire provenance deliberately omits the private logical counter, so the
    /// reconstructed timestamp always has counter zero.  This is fallible at
    /// the 46-bit physical horizon; callers handling peer input must reject
    /// rather than panic or truncate it.
    pub fn from_physical_ms(physical_ms: u64) -> Result<Self, HlcPhysicalMsOutOfRange> {
        (physical_ms <= HLC_MAX_PHYSICAL_MS)
            .then_some(Self(physical_ms << HLC_COUNTER_BITS))
            .ok_or(HlcPhysicalMsOutOfRange {
                physical_ms,
                max_physical_ms: HLC_MAX_PHYSICAL_MS,
            })
    }

    /// Physical milliseconds component.
    pub fn physical_ms(self) -> u64 {
        self.0 >> HLC_COUNTER_BITS
    }

    /// Logical counter component.
    pub fn counter(self) -> u32 {
        (self.0 & u64::from(HLC_MAX_LOGICAL_COUNTER)) as u32
    }

    /// Return a clock value immediately after this one.
    pub fn tick_after(self) -> Result<Self, HlcOverflow> {
        tick_packed(self.physical_ms(), self.counter(), self.physical_ms()).map(
            |(physical_ms, counter)| Self((physical_ms << HLC_COUNTER_BITS) | u64::from(counter)),
        )
    }

    /// Mint the next local HLC from a register and abstract wall clock.
    pub fn tick(register: Self, now_ms: u64) -> Result<Self, HlcOverflow> {
        tick_packed(register.physical_ms(), register.counter(), now_ms).map(
            |(physical_ms, counter)| Self((physical_ms << HLC_COUNTER_BITS) | u64::from(counter)),
        )
    }

    /// Return a total ordering key using the node as tie-breaker.
    pub fn sort_key(self, node: NodeUuid) -> TxTimeSortKey {
        TxTimeSortKey { time: self, node }
    }
}

impl From<u64> for TxTime {
    fn from(physical_ms: u64) -> Self {
        Self::from_physical_ms(physical_ms)
            .expect("TxTime::from requires a physical millisecond in the packed HLC range")
    }
}

fn tick_packed(
    registered_physical_ms: u64,
    registered_counter: u32,
    now_ms: u64,
) -> Result<(u64, u32), HlcOverflow> {
    if now_ms > registered_physical_ms {
        if now_ms > HLC_MAX_PHYSICAL_MS {
            return Err(HlcOverflow {
                physical_ms: HLC_MAX_PHYSICAL_MS,
            });
        }
        return Ok((now_ms, 0));
    }
    if registered_counter < HLC_MAX_LOGICAL_COUNTER {
        return Ok((registered_physical_ms, registered_counter + 1));
    }
    let physical_ms = registered_physical_ms.checked_add(1).ok_or(HlcOverflow {
        physical_ms: HLC_MAX_PHYSICAL_MS,
    })?;
    if physical_ms > HLC_MAX_PHYSICAL_MS {
        return Err(HlcOverflow {
            physical_ms: HLC_MAX_PHYSICAL_MS,
        });
    }
    Ok((physical_ms, 0))
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
        let initial = GlobalTime::new(0x1234_5678_9abc, 0x2_def0).unwrap();
        assert_eq!(initial.physical_ms(), 0x1234_5678_9abc);
        assert_eq!(initial.counter(), 0x2_def0);

        assert_eq!(
            GlobalTime::tick(initial, 0x1234_5678_9abd),
            Ok(GlobalTime::new(0x1234_5678_9abd, 0).unwrap())
        );
        assert_eq!(
            GlobalTime::tick(initial, 0x1234_5678_9abb),
            Ok(GlobalTime::new(0x1234_5678_9abc, 0x2_def1).unwrap())
        );
    }

    #[test]
    fn tx_time_packs_physical_millis_and_logical_counter() {
        let time = TxTime::new(0x1234_5678_9abc, 0x2_def0);
        assert_eq!(time.0, (0x1234_5678_9abc << HLC_COUNTER_BITS) | 0x2_def0);
        assert_eq!(time.physical_ms(), 0x1234_5678_9abc);
        assert_eq!(time.counter(), 0x2_def0);

        assert_eq!(
            TxTime::tick(time, 0x1234_5678_9abd),
            Ok(TxTime::new(0x1234_5678_9abd, 0))
        );
        assert_eq!(
            TxTime::tick(time, 0x1234_5678_9abb),
            Ok(TxTime::new(0x1234_5678_9abc, 0x2_def1))
        );
        assert_eq!(
            time.tick_after(),
            Ok(TxTime::new(0x1234_5678_9abc, 0x2_def1))
        );
    }

    #[test]
    fn packed_hlc_golden_boundaries_are_big_endian_ordered_u64s() {
        assert_eq!(TxTime::new(0, 0).0, 0x0000_0000_0000_0000);
        assert_eq!(
            TxTime::new(0, HLC_MAX_LOGICAL_COUNTER).0,
            0x0000_0000_0003_ffff
        );
        assert_eq!(TxTime::new(1, 0).0, 0x0000_0000_0004_0000);
        assert_eq!(
            TxTime::new(HLC_MAX_PHYSICAL_MS, HLC_MAX_LOGICAL_COUNTER).0,
            u64::MAX
        );
        assert!(TxTime::new(0, HLC_MAX_LOGICAL_COUNTER) < TxTime::new(1, 0));

        // These are the actual ordered-primary-key bytes, not merely the
        // in-memory integer ordering.  The leading tag is Groove's U64 key
        // arm; the remaining bytes must preserve packed HLC order.
        let first = groove::db::PrimaryKeyValue::U64(TxTime::new(0, HLC_MAX_LOGICAL_COUNTER).0)
            .into_bytes();
        let second = groove::db::PrimaryKeyValue::U64(TxTime::new(1, 0).0).into_bytes();
        assert_eq!(first, vec![3, 0, 0, 0, 0, 0, 3, 255, 255]);
        assert_eq!(second, vec![3, 0, 0, 0, 0, 0, 4, 0, 0]);
        assert!(first < second);
    }

    #[test]
    fn global_time_has_the_same_packed_boundary_and_overflow_contract() {
        assert_eq!(GlobalTime::new(0, 0).unwrap().0, 0);
        assert_eq!(
            GlobalTime::new(0, HLC_MAX_LOGICAL_COUNTER).unwrap().0,
            0x0000_0000_0003_ffff
        );
        assert_eq!(GlobalTime::new(1, 0).unwrap().0, 0x0000_0000_0004_0000);
        assert_eq!(
            GlobalTime::new(HLC_MAX_PHYSICAL_MS, HLC_MAX_LOGICAL_COUNTER)
                .unwrap()
                .0,
            u64::MAX
        );
        assert_eq!(
            GlobalTime::tick(GlobalTime::new(17, HLC_MAX_LOGICAL_COUNTER).unwrap(), 17,),
            Ok(GlobalTime::new(18, 0).unwrap())
        );
        assert_eq!(
            GlobalTime::tick(
                GlobalTime::new(HLC_MAX_PHYSICAL_MS, HLC_MAX_LOGICAL_COUNTER).unwrap(),
                HLC_MAX_PHYSICAL_MS,
            ),
            Err(HlcOverflow {
                physical_ms: HLC_MAX_PHYSICAL_MS,
            })
        );
        assert_eq!(GlobalTime::new(HLC_MAX_PHYSICAL_MS + 1, 0), None);
        assert_eq!(GlobalTime::new(0, HLC_MAX_LOGICAL_COUNTER + 1), None);
    }

    /// Public Unix-millisecond provenance reconstructs only the zero logical
    /// counter and rejects values that cannot fit the 46-bit HLC layout.
    #[test]
    fn physical_millisecond_reconstruction_has_an_explicit_46_bit_boundary() {
        assert_eq!(
            TxTime::from_physical_ms(HLC_MAX_PHYSICAL_MS),
            Ok(TxTime::new(HLC_MAX_PHYSICAL_MS, 0))
        );
        assert_eq!(
            TxTime::from_physical_ms(HLC_MAX_PHYSICAL_MS + 1),
            Err(HlcPhysicalMsOutOfRange {
                physical_ms: HLC_MAX_PHYSICAL_MS + 1,
                max_physical_ms: HLC_MAX_PHYSICAL_MS,
            })
        );
    }

    #[test]
    #[should_panic(expected = "HLC physical component exceeds 46-bit packed range")]
    fn tx_time_rejects_physical_millis_outside_packed_range() {
        TxTime::new(1 << HLC_PHYSICAL_BITS, 0);
    }

    #[test]
    #[should_panic(expected = "HLC logical counter exceeds 18-bit packed range")]
    fn tx_time_rejects_counter_outside_packed_range() {
        TxTime::new(0, 1 << HLC_COUNTER_BITS);
    }

    #[test]
    /// Logical exhaustion moves into the next physical millisecond without wrapping.
    fn logical_exhaustion_advances_physical_time_without_a_clock_panic() {
        let saturated = TxTime::new(123, HLC_MAX_LOGICAL_COUNTER);
        assert_eq!(
            TxTime::tick(saturated, 123),
            Ok(TxTime::new(124, 0)),
            "a stalled or rolled-back wall clock must remain monotone"
        );
    }

    #[test]
    /// Only the final packed position reports a typed clock overflow.
    fn packed_hlc_reports_typed_overflow_only_at_its_final_position() {
        let final_time = TxTime::new(HLC_MAX_PHYSICAL_MS, HLC_MAX_LOGICAL_COUNTER);
        assert_eq!(
            TxTime::tick(final_time, HLC_MAX_PHYSICAL_MS),
            Err(HlcOverflow {
                physical_ms: HLC_MAX_PHYSICAL_MS,
            })
        );
    }

    #[test]
    /// A burst larger than one logical range remains strictly ordered.
    fn high_same_millisecond_burst_remains_strictly_monotone() {
        let now_ms = 1_777_777_777_777;
        let mut current = TxTime::new(now_ms, 0);
        for _ in 0..300_000 {
            let next = TxTime::tick(current, now_ms).expect("year-4200 horizon is not reached");
            assert!(next > current);
            current = next;
        }
        assert_eq!(current.physical_ms(), now_ms + 1);
        assert_eq!(current.counter(), 37_856);
    }
}
