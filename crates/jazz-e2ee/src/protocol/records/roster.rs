//! Public account commitments, endorsement headers and endorsement signatures.

use crate::context::CryptoContext;
use crate::envelope::{self, Mechanism};
use crate::primitives::slh_dsa;
use crate::protocol::commitment::{ENDORSEMENT_CONTEXT, MECHANISM};
use crate::protocol::domain::AccountDomain;
use crate::protocol::error::{ProtocolError, ProtocolResult};
use crate::protocol::ids::IdForm;
use crate::protocol::json::MAX_DELIVERY_BYTES;

use super::{
    KeyColumns, append_framed, context, ensure, ensure_account, ensure_bytes, ensure_id, env,
    row_id, string_enum,
};

string_enum!(SubjectKind { Device => "device", RecoveryRoot => "recovery-root" });

impl SubjectKind {
    fn check_id(self, id: &str) -> ProtocolResult<()> {
        let form = match self {
            Self::Device => IdForm::Random,
            Self::RecoveryRoot => IdForm::RandomOrDerived,
        };
        ensure_id(id, form, "invalid subject ID")
    }
}

/// A subject's classically signed row in `__e2ee_commitments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commitment {
    pub id: String,
    pub account_id: String,
    pub subject_kind: SubjectKind,
    pub subject_id: String,
    pub mechanism: String,
    pub version: u32,
    pub commitment: Vec<u8>,
    pub signature: Vec<u8>,
}

impl Commitment {
    fn check_coordinates(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid commitment ID")?;
        ensure_account(&self.account_id, "invalid account ID")?;
        self.subject_kind.check_id(&self.subject_id)
    }

    /// A known mechanism's key has its exact length. An unknown mechanism's
    /// key is opaque: it is unsupported, not invalid, so only the generic
    /// bound applies.
    fn check_key(&self) -> ProtocolResult<()> {
        super::ensure_mechanism(&self.mechanism, self.version)?;
        let valid = if Mechanism::new(&self.mechanism, self.version) == MECHANISM {
            self.commitment.len() == slh_dsa::PUBLIC_KEY_BYTES
        } else {
            (1..=MAX_DELIVERY_BYTES).contains(&self.commitment.len())
        };
        ensure(valid, "invalid commitment length")
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_coordinates()?;
        self.check_key()?;
        ensure_bytes(&self.signature, "empty commitment signature")
    }

    /// The exact key bytes signed and pinned, including for an unknown mechanism.
    pub fn envelope(&self) -> ProtocolResult<Vec<u8>> {
        self.check_key()?;
        env(&self.mechanism, self.version, &self.commitment)
    }

    /// The generation of the subject's admission occupies the context's epoch field.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        generation: u64,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_coordinates()?;
        let prefix = context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.commitment.v1",
            scope: "account",
            identifier: &self.account_id,
            table: "__e2ee_commitments",
            row: &self.id,
            column: "signature",
            epoch: &generation.to_string(),
            recipient: &self.subject_id,
        })?;
        append_framed(
            prefix,
            &[self.subject_kind.as_str().as_bytes(), &self.envelope()?],
        )
    }
}

/// The header in `__e2ee_endorsements`, with the ID of the admitting record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endorsement {
    pub id: String,
    pub account_id: String,
    pub endorser_kind: SubjectKind,
    pub endorser_id: String,
    pub subject_kind: SubjectKind,
    pub subject_id: String,
    pub signature_digest: Vec<u8>,
}

impl Endorsement {
    fn check_coordinates(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::RandomOrDerived, "invalid endorsement ID")?;
        ensure_account(&self.account_id, "invalid account ID")?;
        self.endorser_kind.check_id(&self.endorser_id)?;
        self.subject_kind.check_id(&self.subject_id)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_coordinates()?;
        ensure(
            self.signature_digest.len() == 32,
            "invalid endorsement digest length",
        )
    }

    /// Rebuild the message from the admitting record's epoch and generation,
    /// and the admitted subject's commitment and classical key columns.
    pub fn message(
        &self,
        application: &AccountDomain,
        generation: u64,
        epoch_id: &str,
        commitment: &Commitment,
        keys: &KeyColumns,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_coordinates()?;
        ensure_id(epoch_id, IdForm::Random, "invalid admission epoch ID")?;
        keys.validate()?;
        let prefix = context(CryptoContext {
            application: application.as_str(),
            policy: ENDORSEMENT_CONTEXT,
            scope: "account",
            identifier: &self.account_id,
            table: "__e2ee_endorsements",
            row: &self.id,
            column: &format!(
                "endorsement:{}:{}",
                self.endorser_kind.as_str(),
                self.endorser_id
            ),
            epoch: epoch_id,
            recipient: &self.subject_id,
        })?;
        append_framed(
            prefix,
            &[
                generation.to_string().as_bytes(),
                self.subject_kind.as_str().as_bytes(),
                &commitment.envelope()?,
                &keys.signing_envelope()?,
                &keys.sealing_envelope()?,
            ],
        )
    }
}

/// The signature envelope in `__e2ee_endorsement_signatures`, at the header's ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndorsementSignature {
    pub id: String,
    pub account_id: String,
    pub signature: Vec<u8>,
}

impl EndorsementSignature {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(
            &self.id,
            IdForm::RandomOrDerived,
            "invalid endorsement signature ID",
        )?;
        ensure_account(&self.account_id, "invalid account ID")?;
        let (_, payload) = envelope::parse(&self.signature)
            .map_err(|_| ProtocolError::InvalidRecord("invalid endorsement signature envelope"))?;
        ensure_bytes(payload, "empty endorsement signature")
    }
}

row_id!(Commitment, Endorsement, EndorsementSignature);
