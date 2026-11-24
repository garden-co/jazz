//! Spaces (`E2EE_PROTOCOL.md` §3): replay of a space's rows into its root,
//! its chain of key versions, the keys a device obtains, who lacks a
//! version and whether the space needs rotation, and the records each
//! space publication writes. Everything is a pure function of the rows of
//! one ledger read: the space's rows and the public account histories they
//! name, at one cut.
//!
//! - [`ledger`]: one space's rows, and the account histories the replay
//!   checks authors against and seals deliveries to.
//! - [`chain`]: the space's root among the roots of its address (§3.2) and
//!   the chain rule (§3.3).
//! - [`keys`]: acquiring and authenticating keys, and deliveries that do
//!   not open (§3.3, §3.4).
//! - [`members`]: key holders, who lacks a version, the needs-rotation mark
//!   and a rotation's recipients (§1.1, §3.4, §3.5).
//! - [`build`]: roots, successors (ordinary and recovery) and deliveries
//!   (§3.1, §3.2, §3.4, §3.5).

pub mod build;
pub mod chain;
pub mod keys;
pub mod ledger;
pub mod members;

pub use chain::{InvalidSpaceRoot, LostRoot, SpaceChain, SpaceRoots, SpaceVersion, Unresolved};
pub use keys::{
    Acquisition, BrokenHistory, HeldVersion, InvalidDelivery, KeyCheck, NonDerivedDelivery,
    SpaceKeys, acquire,
};
pub use ledger::{AccountDirectory, AuthorCheck, Recipient, SpaceLedger};
pub use members::{
    BrokenMember, Lacking, LackingMembers, RotationMark, RotationRecipients, UnsupportedMember,
    VersionEnvelope,
};

use super::position::by_position_then_id;
