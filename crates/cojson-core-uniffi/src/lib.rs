
uniffi::setup_scaffolding!();

pub mod crypto {    
    pub mod ed25519;
    pub use ed25519::*;
}

