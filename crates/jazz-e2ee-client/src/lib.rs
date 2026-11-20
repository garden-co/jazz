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
//! Key acquisition, space creation, deliveries and rotation build on this.
//!
//! The application's mechanism selection and allow-list come from its
//! `encryption.mechanisms` settings (see [`ClientConfig`]): readers route
//! only allowed mechanisms, and writers select only allowed ones.

mod account;
mod client;
mod error;
mod hooks;
mod mechanisms;
mod state;
pub mod store;
mod values;

pub use account::{AccountStatus, AccountWatch, OwnAccount};
pub use client::{ClientConfig, E2eeClient};
pub use error::ClientError;
pub use state::{MAX_REPORTS, Report};
pub use store::{MemoryStore, PrivateStore, StoreError, Stored};
pub use values::CELL_TYPE_ENCODING;
