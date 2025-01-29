mod error;
pub use error::CryptoError;

pub mod hash {
    pub mod blake3;
    pub use blake3::*;
}

pub mod crypto {
    pub mod ed25519;
    pub mod seal;
    pub mod x25519;
    pub mod xsalsa20;

    pub use ed25519::*;
    pub use seal::*;
    pub use x25519::*;
    pub use xsalsa20::*;
}
