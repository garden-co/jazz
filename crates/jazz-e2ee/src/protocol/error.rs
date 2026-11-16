//! Protocol failures (`E2EE_PROTOCOL.md` §7).
//!
//! Each failure keeps its cause: a provider failure is
//! [`ProtocolError::ProviderFailure`] and is never reported as an invalid
//! record, an unusable candidate or a refusal.

use crate::error::Error;

/// A capability this client lacks. The dependent state stays unresolved; the
/// record is never treated as invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnsupportedCapability {
    /// A ledger version other than 1.
    LedgerVersion,
    /// A signing mechanism with no registered signer.
    SigningMechanism,
    /// A registered signing mechanism at an unregistered version.
    SigningVersion,
}

impl UnsupportedCapability {
    pub const fn code(self) -> &'static str {
        match self {
            Self::LedgerVersion => "ledger-version",
            Self::SigningMechanism => "signing-mechanism",
            Self::SigningVersion => "signing-version",
        }
    }
}

/// Accepted rows that contradict each other. Current replay fails; no
/// conflicting row is picked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntrinsicContradiction {
    ConflictingRoots,
    ConflictingDeviceKeys,
    MissingRootKey,
    MissingRecipientKey,
    MissingSignerKey,
}

impl IntrinsicContradiction {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ConflictingRoots => "conflicting-roots",
            Self::ConflictingDeviceKeys => "conflicting-device-keys",
            Self::MissingRootKey => "missing-root-key",
            Self::MissingRecipientKey => "missing-recipient-key",
            Self::MissingSignerKey => "missing-signer-key",
        }
    }
}

/// The refusal codes of §7, carried by publication errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RefusalCode {
    AccountHasHistory,
    DeviceIdentityConflict,
    DeviceKeysMismatch,
    NoDeviceRequest,
    ResetDeviceActive,
    DeviceNotActive,
    DeviceRemoved,
    StaleChallenge,
    ApprovalClosed,
    InvalidPossessionProof,
    TargetNotActive,
    UnsupportedRecipient,
    SpaceSealed,
    GroupSealed,
    RecipientAbsent,
    RecipientNeedsMaintenance,
    GroupUnavailable,
    AccountResetNotAllowed,
    SpacePending,
    ConfirmationStale,
    RecipientStillDeclared,
    RecipientsDeclared,
    LastManager,
}

impl RefusalCode {
    pub const fn code(self) -> &'static str {
        match self {
            Self::AccountHasHistory => "account-has-history",
            Self::DeviceIdentityConflict => "device-identity-conflict",
            Self::DeviceKeysMismatch => "device-keys-mismatch",
            Self::NoDeviceRequest => "no-device-request",
            Self::ResetDeviceActive => "reset-device-active",
            Self::DeviceNotActive => "device-not-active",
            Self::DeviceRemoved => "device-removed",
            Self::StaleChallenge => "stale-challenge",
            Self::ApprovalClosed => "approval-closed",
            Self::InvalidPossessionProof => "invalid-possession-proof",
            Self::TargetNotActive => "target-not-active",
            Self::UnsupportedRecipient => "unsupported-recipient",
            Self::SpaceSealed => "space-sealed",
            Self::GroupSealed => "group-sealed",
            Self::RecipientAbsent => "recipient-absent",
            Self::RecipientNeedsMaintenance => "recipient-needs-maintenance",
            Self::GroupUnavailable => "group-unavailable",
            Self::AccountResetNotAllowed => "account-reset-not-allowed",
            Self::SpacePending => "space-pending",
            Self::ConfirmationStale => "confirmation-stale",
            Self::RecipientStillDeclared => "recipient-still-declared",
            Self::RecipientsDeclared => "recipients-declared",
            Self::LastManager => "last-manager",
        }
    }
}

/// Recovery failures (§2.9, §7). Only the candidate failures permit trying
/// another delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryFailure {
    Cancelled,
    MaterialUnusable,
    RootMismatch,
    DeliveryMissing,
    DeliveryUnusable,
}

impl RecoveryFailure {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Cancelled => "recovery-cancelled",
            Self::MaterialUnusable => "recovery-material-unusable",
            Self::RootMismatch => "recovery-root-mismatch",
            Self::DeliveryMissing => "recovery-delivery-missing",
            Self::DeliveryUnusable => "recovery-delivery-unusable",
        }
    }
}

/// Why a protocol operation failed. Messages are fixed strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The cryptographic provider failed ([`Error::CryptoFailure`]), or a
    /// writer could not read back what it just produced. Stops the
    /// operation; never a property of the input.
    #[error("E2EE cryptographic provider failed")]
    ProviderFailure,
    /// A cryptographic operation refused its input: malformed or
    /// unauthentic bytes, an invalid argument, an unregistered mechanism, no
    /// key for a mechanism, or no secure randomness. Never a provider
    /// failure.
    #[error("E2EE cryptographic operation failed: {0}")]
    Crypto(Error),
    #[error("Unsupported E2EE capability")]
    Unsupported(UnsupportedCapability),
    #[error("Contradictory E2EE account history")]
    Intrinsic(IntrinsicContradiction),
    /// A record does not satisfy its own format: wrong ID form, enum value,
    /// target columns or canonical encoding. `what` names the rule.
    #[error("Invalid E2EE record")]
    InvalidRecord(&'static str),
    /// Evidence that does not come from one ledger read: rows beyond the cut,
    /// duplicate row IDs or reads at different cuts.
    #[error("E2EE evidence is inconsistent")]
    InconsistentEvidence,
    #[error("E2EE operation refused")]
    Refused(RefusalCode),
    /// The account key is not available from the evidence: a required proof
    /// is provably invalid or missing (§2.7). Not a provider failure.
    #[error("The E2EE account key is unavailable")]
    AccountKeyUnavailable,
    #[error("E2EE recovery failed")]
    Recovery(RecoveryFailure),
}

impl From<Error> for ProtocolError {
    fn from(error: Error) -> Self {
        match error {
            Error::CryptoFailure => Self::ProviderFailure,
            other => Self::Crypto(other),
        }
    }
}

impl ProtocolError {
    /// Whether the provider failed, as opposed to the input or evidence.
    pub const fn is_provider_failure(&self) -> bool {
        matches!(self, Self::ProviderFailure)
    }

    /// The specific code, when the failure has one.
    pub const fn code(&self) -> Option<&'static str> {
        match self {
            Self::Unsupported(kind) => Some(kind.code()),
            Self::Intrinsic(kind) => Some(kind.code()),
            Self::Refused(kind) => Some(kind.code()),
            Self::Recovery(kind) => Some(kind.code()),
            _ => None,
        }
    }
}

pub type ProtocolResult<T> = core::result::Result<T, ProtocolError>;
