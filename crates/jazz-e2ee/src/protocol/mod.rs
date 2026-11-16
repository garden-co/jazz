//! The E2EE protocol's pure foundations (`crates/jazz/SPEC/E2EE_PROTOCOL.md`):
//! typed rows of the internal tables, their canonical encodings and signed
//! bytes, derived identifiers and keys, and account replay. Nothing here does
//! I/O: callers pass the rows of one ledger read, with their positions.
//!
//! - [`domain`]: application domains.
//! - [`ids`]: UUID forms and public derived IDs.
//! - [`json`]: canonical JSON and the ID list, revision, member map and
//!   delivery map encodings.
//! - [`records`]: one type per internal table row, with validation and the
//!   exact bytes each record signs or binds.
//! - [`derive`]: recovery roots from a recovery secret, group epoch key
//!   pairs and recipient commitments.
//! - [`signing`]: signing and verification over the [`crate::agility::Signer`]
//!   role, keeping provider failures distinct from invalid signatures.
//! - [`account`]: account and device lifecycle (§2): record builders, public
//!   replay, state as of a position, private verification and account key
//!   authentication.

pub mod account;
pub mod derive;
pub mod domain;
pub mod error;
pub mod ids;
pub mod json;
pub mod position;
pub mod records;
pub mod signing;

pub use error::{ProtocolError, ProtocolResult};
pub use position::{Accepted, Position};
