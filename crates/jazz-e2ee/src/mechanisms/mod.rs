//! The built-in mechanisms, each identified by its envelope ID and version.
//!
//! Every mechanism binds its own envelope header into what it authenticates
//! or derives, so its ID and version cannot be swapped for another's.

pub mod cell;
pub mod equality;
pub mod key_envelope;
pub mod key_seal;
pub mod signature;
pub mod stream;

fn concat(parts: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(parts.iter().map(|part| part.len()).sum());
    for part in parts {
        bytes.extend_from_slice(part);
    }
    bytes
}
