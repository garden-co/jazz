//! What a binding's host needs to run the runtime for an app, the same for
//! every binding that exposes it to JavaScript (Node's native module and the
//! browser's WebAssembly module):
//!
//! - [`bridge`]: the private store an app supplies from JavaScript, as a
//!   queue of requests that JavaScript answers.
//! - [`runtime`]: the runtime built from an app's own declarations over that
//!   store, its long-lived loop (account verification, key watches, the
//!   delivery pass), the calls the TypeScript handle makes, and the reports.
//!
//! - [`phrase`]: the recovery phrase, 24 words of a 32-byte recovery secret.
//!
//! A binding adds only what is its own: how a future is polled and how a
//! result becomes a promise.

pub mod bridge;
pub mod phrase;
pub mod runtime;

/// The device account's secret as native hosts pass it (see
/// [`runtime::Options`]): its bytes are zeroized when it is dropped.
pub use jazz_e2ee::protocol::derive::RecoverySecret;
