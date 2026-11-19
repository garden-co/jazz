//! The crate's one error type.
//!
//! Messages are fixed strings: no variant carries key material, plaintext or
//! ciphertext, so an error can be logged or shown without leaking data.

/// Why an E2EE operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The bytes are a well-formed envelope for another mechanism or version.
    /// A reader may route them elsewhere; they are not known to be corrupt.
    #[error("The encrypted data format is not supported")]
    UnsupportedFormat,
    /// The bytes are malformed, truncated, carry trailing data or fail
    /// authentication (including a wrong key or context). No plaintext from
    /// a failed operation is ever returned.
    #[error("Encrypted data could not be authenticated or decoded")]
    InvalidCiphertext,
    /// A caller-supplied argument is outside what the format allows: an
    /// invalid identity, context field, mechanism, key or record size.
    #[error("Invalid E2EE input")]
    InvalidInput,
    /// The random source failed. Nothing was encrypted.
    #[error("Secure randomness is unavailable")]
    RandomUnavailable,
    /// The cryptographic provider failed: an internal error of a primitive
    /// or of a mechanism implementation, as opposed to input that fails
    /// authentication. Never reported as [`Error::InvalidCiphertext`] or as a
    /// failed verification, and never retried as one.
    #[error("E2EE cryptographic operation failed")]
    CryptoFailure,
    /// The mechanism a configuration or lookup names is not registered.
    #[error("The E2EE mechanism is not registered")]
    UnknownMechanism,
    /// The recipient advertises no public key for the mechanism the sender
    /// selected, or a key was given to a mechanism it does not belong to.
    /// There is no fallback to another mechanism.
    #[error("The recipient does not support the selected E2EE mechanism")]
    RecipientUnsupported,
    /// The envelope's mechanism is registered, but this reader holds no key
    /// pair of that mechanism to open it with.
    #[error("No key is held for the E2EE mechanism")]
    NoKeyForMechanism,
}

impl Error {
    /// The error a reader reports once routing has chosen a mechanism: an
    /// inner unsupported format means the routed bytes are corrupt, and
    /// every other error, provider failures included, passes through.
    pub(crate) fn after_routing(self) -> Self {
        match self {
            Error::UnsupportedFormat => Error::InvalidCiphertext,
            other => other,
        }
    }
}

pub type Result<T> = core::result::Result<T, Error>;
