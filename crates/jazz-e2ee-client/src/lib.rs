//! Jazz's end-to-end encryption client runtime (`crates/jazz/SPEC/E2EE_PROTOCOL.md`;
//! core hooks note `crates/jazz/SPEC/E2EE_CORE_HOOKS.md`).
//!
//! The database stays free of cryptography and calls its encryption hooks
//! wherever a value becomes stored or leaves for a reader. This crate
//! implements those hooks over `jazz-e2ee`, which holds the formats and
//! the protocol as pure functions, and does the I/O around them:
//!
//! - [`store`]: the host's private storage of one device's encryption
//!   state ([`PrivateStore`]), with the platform's
//!   protection of secrets at rest.
//! - [`E2eeClient`]: the runtime of one account on this device. It loads
//!   and stores the device (generated on first use), the keys it holds per
//!   space root, and its floors, and gives the database its hooks.
//! - The hooks seal and open cells, rewrite equality filters into token
//!   filters and seal streamed values with the keys this device holds. A
//!   held key permits the write; nothing else is checked on the content
//!   path.
//!
//! - Spaces: [`E2eeClient::acquire`] reads a space's rows at one cut and
//!   keeps the keys they give; [`E2eeClient::write`] and
//!   [`E2eeClient::read`] wrap a call so that a key the hooks lack is
//!   acquired, or a space's root prepared and staged with the write, and
//!   reports settle to `key-pending`, `key-unavailable` or `space-pending`
//!   ([`E2eeClient::on_report`]); [`E2eeClient::create_space`] and
//!   [`E2eeClient::reconcile`] create a space and settle creations by their
//!   root's fate; [`E2eeClient::watch_spaces`] and
//!   [`E2eeClient::sync_spaces`] acquire when watched rows change.
//!
//! The application's mechanism selection and allow-list come from its
//! `encryption.mechanisms` settings (see [`ClientConfig`]): readers route
//! only allowed mechanisms, and writers select only allowed ones.

mod account;
mod client;
mod error;
mod hooks;
mod mechanisms;
mod spaces;
mod state;
pub mod store;
mod values;

pub use account::{AccountStatus, AccountWatch, OwnAccount};
pub use client::{ClientConfig, E2eeClient};
pub use error::ClientError;
pub use spaces::{SpaceCreation, SpaceKeyState, SpaceWatch};
pub use state::{MAX_REPORTS, Report};
pub use store::{MemoryStore, PrivateStore, StoreError, Stored};
pub use values::CELL_TYPE_ENCODING;
