//! The E2EE protocol's pure foundations (`crates/jazz/SPEC/E2EE_PROTOCOL.md`):
//! typed rows of the internal tables, their canonical encodings, signed
//! bytes and contexts, recovery and account sealing key derivations, and
//! account replay. Nothing here does I/O: callers pass the rows of one
//! ledger read, with their positions.
//!
//! - [`domain`]: application domains.
//! - [`ids`]: UUID forms, random IDs and the version 8 form of derived
//!   recovery root IDs.
//! - [`json`]: canonical JSON and the ID list, revision and delivery map
//!   encodings.
//! - [`records`]: one type per internal table row (the account and device
//!   tables, the members table and the space key tables), with validation
//!   and the exact bytes each record signs or binds.
//! - [`derive`](mod@derive): recovery roots from a recovery secret, and
//!   account sealing key pairs from account epoch keys.
//! - [`local_first`]: the local-first identity that signs a local-first
//!   account's root, and the account ID its key derives.
//! - [`signing`]: signing and verification over the [`crate::agility::Signer`]
//!   role, keeping provider failures distinct from invalid signatures.
//! - [`account`]: account and device lifecycle (§2): record builders, public
//!   replay, state as of a position, private verification and account key
//!   authentication.
//! - [`position`]: accepted positions and the checks of one ledger read.
//! - [`error`]: the protocol's failures and their codes (§7).

pub mod account;
pub mod derive;
pub mod domain;
pub mod error;
pub mod ids;
pub mod json;
pub mod local_first;
pub mod position;
pub mod records;
pub mod signing;

pub use error::{ProtocolError, ProtocolResult};
pub use position::{Accepted, Position};
