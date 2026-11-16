//! Account and device lifecycle (`E2EE_PROTOCOL.md` §2).
//!
//! - [`ledger`]: one account's public and private rows from one ledger read.
//! - [`replay`]: public replay, the current state and the state as of a
//!   position, including generations and recovery root validity.
//! - [`private`]: verification of the private rows against public replay.
//! - [`key`]: acquiring and authenticating the account epoch key (§2.6,
//!   §2.7).
//! - [`build`]: the records each account publication writes, and the
//!   preconditions that refuse it.

pub mod build;
pub mod key;
pub mod ledger;
pub mod private;
pub mod replay;

pub use key::{AccountKey, AccountKeyEvidence, AuthenticatedAccountKey};
pub use ledger::{PrivateAccountLedger, PublicAccountLedger};
pub use private::{PrivateEvidence, VerifiedApproval, verify_private};
pub use replay::{AccountEpoch, AccountReplay, AccountState};
