//! Mechanism and signature digest used by account admissions.

use sha2::{Digest, Sha256};

use crate::envelope::{self, Mechanism};
use crate::primitives::slh_dsa;

use super::error::{ProtocolError, ProtocolResult, UnsupportedCapability};

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.commit.slh-dsa-sha2-128s", 1);
/// The endorsement policy and FIPS 205 context string.
pub const ENDORSEMENT_CONTEXT: &str = "jazz.e2ee.endorsement.v1";

/// Hash the entire signature column, including its common envelope header.
pub fn endorsement_digest(signature: &[u8]) -> [u8; 32] {
    Sha256::new()
        .chain_update(b"jazz.e2ee.endorsement-digest.v1\0")
        .chain_update(signature)
        .finalize()
        .into()
}

/// Whether this reader can use a committed key or signature mechanism.
pub fn commitment_support(mechanism: Mechanism<'_>) -> ProtocolResult<()> {
    if mechanism == MECHANISM {
        Ok(())
    } else {
        Err(ProtocolError::Unsupported(
            UnsupportedCapability::CommitmentMechanism,
        ))
    }
}

/// Check a signature column against the endorser's committed public key.
/// Unknown mechanisms stay distinguishable from invalid signatures and keys.
pub fn verify_endorsement(
    mechanism: Mechanism<'_>,
    commitment: &[u8],
    message: &[u8],
    signature: &[u8],
) -> ProtocolResult<bool> {
    commitment_support(mechanism)?;
    let Ok((signature_mechanism, payload)) = envelope::parse(signature) else {
        return Ok(false);
    };
    commitment_support(signature_mechanism)?;
    Ok(slh_dsa::verify(
        commitment,
        message,
        ENDORSEMENT_CONTEXT.as_bytes(),
        payload,
    )?)
}
