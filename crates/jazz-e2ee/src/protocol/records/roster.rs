//! Public account commitments.

use crate::agility::PublicKey;
use crate::context::CryptoContext;
use crate::envelope::Mechanism;
use crate::primitives::slh_dsa;
use crate::protocol::commitment::MECHANISM;
use crate::protocol::domain::AccountDomain;
use crate::protocol::error::ProtocolResult;
use crate::protocol::ids::IdForm;
use crate::protocol::json::MAX_DELIVERY_BYTES;
use crate::protocol::position::{Accepted, Position, by_position_then_id};
use crate::protocol::signing;
use crate::registry::Registry;

use super::{
    append_framed, context, ensure, ensure_account, ensure_bytes, ensure_id, env, row_id,
    string_enum,
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

row_id!(Commitment);

/// A valid recovery root, as replay accepted it: what a reader needs to
/// decide which of its commitment rows counts.
#[derive(Debug, Clone, Copy)]
pub struct CommittedRoot<'a> {
    pub account_id: &'a str,
    pub root_id: &'a str,
    pub generation: u64,
    /// The root's accepted position.
    pub position: Position,
    /// The root's own signing key.
    pub signing: &'a PublicKey,
}

/// The commitment that counts for a recovery root (§2.9, "Which commitment
/// counts").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counted<'a> {
    /// The counting row is of the mechanism this version knows.
    Known(&'a Commitment),
    /// The counting row is of a mechanism or version this reader does not
    /// know. No later row is considered in its place.
    Unsupported(&'a Commitment),
}

impl Commitment {
    /// Whether this row is a valid commitment of `root` (§2.9, "Which
    /// commitment counts"): of its account and subject, with a well-formed
    /// record (known mechanisms at their exact key length) and a signature
    /// by the root's own key that verifies. A malformed row is invalid; a
    /// failure to verify (an unsupported signing mechanism, a provider
    /// failure) is an error, never "invalid".
    pub fn is_valid_for(
        &self,
        registry: &Registry,
        application: &AccountDomain,
        root: &CommittedRoot<'_>,
    ) -> ProtocolResult<bool> {
        if self.account_id != root.account_id
            || self.subject_kind != SubjectKind::RecoveryRoot
            || self.subject_id != root.root_id
            || self.validate().is_err()
        {
            return Ok(false);
        }
        let bytes = self.signed_bytes(application, root.generation)?;
        signing::verify(registry, root.signing, &bytes, &self.signature)
    }
}

/// The commitment that counts for `root` among the commitment rows of a
/// ledger read: the earliest valid one by accepted position, ties by
/// smallest row ID, no earlier than the root itself (§2.9, "Which
/// commitment counts"). `None` if no row is valid. Rows that are not about
/// `root` are ignored, so `rows` can be a whole read.
pub fn counting_commitment<'a>(
    registry: &Registry,
    application: &AccountDomain,
    root: &CommittedRoot<'_>,
    rows: &'a [Accepted<Commitment>],
) -> ProtocolResult<Option<Counted<'a>>> {
    let mut ordered: Vec<&Accepted<Commitment>> = rows
        .iter()
        .filter(|accepted| accepted.position >= root.position)
        .collect();
    by_position_then_id(&mut ordered);
    for accepted in ordered {
        let row = &accepted.row;
        if row.is_valid_for(registry, application, root)? {
            return Ok(Some(
                if Mechanism::new(&row.mechanism, row.version) == MECHANISM {
                    Counted::Known(row)
                } else {
                    Counted::Unsupported(row)
                },
            ));
        }
    }
    Ok(None)
}
