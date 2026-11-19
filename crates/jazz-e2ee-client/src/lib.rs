//! Private storage for the local state of a device using Jazz end-to-end encryption.

pub mod store;

pub use store::{MemoryStore, PrivateStore, StoreError, Stored};
