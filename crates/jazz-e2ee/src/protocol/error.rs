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
    /// An account key envelope names an unregistered mechanism or version.
    AccountKeyEnvelope,
}

impl UnsupportedCapability {
    pub const fn code(self) -> &'static str {
        match self {
            Self::LedgerVersion => "ledger-version",
            Self::SigningMechanism => "signing-mechanism",
            Self::SigningVersion => "signing-version",
            Self::AccountKeyEnvelope => "account-key-envelope",
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
    MissingSignerKey,
}

impl IntrinsicContradiction {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ConflictingRoots => "conflicting-roots",
            Self::ConflictingDeviceKeys => "conflicting-device-keys",
            Self::MissingRootKey => "missing-root-key",
            Self::MissingSignerKey => "missing-signer-key",
        }
    }
}

/// Why an account root does not count (§2.4 step 1). Such a root is
/// reported and otherwise ignored; it never fails replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RootRejection {
    /// The row does not satisfy its own format.
    Malformed,
    /// The row names another account than the one replayed.
    ForeignAccount,
    /// A `device-account` root whose public key does not derive the account ID.
    KeyMismatch,
    /// A `device-account` root whose device has no published keys at or
    /// before the root, so its signature cannot verify.
    DeviceKeysMissing,
    /// A `device-account` root whose signature does not verify.
    SignatureInvalid,
    /// An `app` root of an account that has a valid `device-account` root.
    KindMismatch,
    /// A root of generation `g > 0` not accepted after a counting root of
    /// generation `g − 1`.
    Unchained,
}

impl RootRejection {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Malformed => "root-malformed",
            Self::ForeignAccount => "root-foreign-account",
            Self::KeyMismatch => "root-key-mismatch",
            Self::DeviceKeysMissing => "root-device-keys-missing",
            Self::SignatureInvalid => "root-signature-invalid",
            Self::KindMismatch => "root-kind-mismatch",
            Self::Unchained => "root-unchained",
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
    /// A challenge not signed, over this device's own keys, by an active
    /// device of the account (§2.5).
    InvalidChallenge,
    TargetNotActive,
    /// A device cannot remove itself (§2.8): another device does it.
    SelfRemoval,
    /// A removal request with no other active device to complete it
    /// (§2.8): the account would be left with no device.
    NoOtherDevice,
    UnsupportedRecipient,
    AccountResetNotAllowed,
    DeviceAccountRootRequired,
    DeviceAccountSecretRequired,
    SpacePending,
    SpaceCreateRefused,
    RotationNeedsKey,
    RotationKeyStateUnconfirmed,
    NotAMember,
    DeviceAccountIdentityMismatch,
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
            Self::InvalidChallenge => "invalid-challenge",
            Self::TargetNotActive => "target-not-active",
            Self::SelfRemoval => "self-removal",
            Self::NoOtherDevice => "no-other-device",
            Self::UnsupportedRecipient => "unsupported-recipient",
            Self::DeviceAccountRootRequired => "device-account-root-required",
            Self::DeviceAccountSecretRequired => "device-account-secret-required",
            Self::AccountResetNotAllowed => "account-reset-not-allowed",
            Self::SpacePending => "space-pending",
            Self::SpaceCreateRefused => "space-create-refused",
            Self::RotationNeedsKey | Self::RotationKeyStateUnconfirmed => "rotation-needs-key",
            Self::NotAMember => "not-a-member",
            Self::DeviceAccountIdentityMismatch => "device-account-identity-mismatch",
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
    #[error("{}", match .0 {
        RefusalCode::RotationNeedsKey => "This device does not hold the space's current key yet",
        RefusalCode::RotationKeyStateUnconfirmed => "This device cannot confirm the space's key state",
        RefusalCode::DeviceAccountRootRequired => "Retiring this root would prevent recovery with the device account secret",
        RefusalCode::DeviceAccountSecretRequired => "Sign in with the account secret to retire a derived recovery root",
        RefusalCode::NotAMember => "Your account is not a member of this space",
        _ => "E2EE operation refused",
    })]
    Refused(RefusalCode),
    /// The account key is not available from the evidence: a required proof
    /// is provably invalid or missing (§2.7). Not a provider failure.
    #[error("The E2EE account key is unavailable")]
    AccountKeyUnavailable,
    #[error("E2EE recovery failed")]
    Recovery(RecoveryFailure),
    /// The account's canonical root is not the one this device knows its
    /// account wrote (`root-not-mine`, §2.4): the server, or whoever can
    /// write as the account, replaced it. Surfaced to the user; never
    /// treated as an ordinary failure.
    #[error("The E2EE account root is not this account's own")]
    RootNotMine,
    /// The evidence is older than a state this device already verified
    /// (`stale-state`, §2.4 "Floors"): a rollback or withheld rows.
    #[error("E2EE evidence is older than state already verified")]
    StaleState,
    /// This device was active in the account state it last verified and is
    /// not active in the newer one (`device-no-longer-active`, §2.4
    /// "Floors"): it was removed, or the account was reset to a generation
    /// it is not part of. The device wipes its E2EE state for the account
    /// and enrols again (a new device ID, approved as in §2.5). Not a
    /// security alert.
    #[error("This device is no longer an active E2EE device of the account")]
    DeviceNoLongerActive,
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
            Self::RootNotMine => Some("root-not-mine"),
            Self::StaleState => Some("stale-state"),
            Self::DeviceNoLongerActive => Some("device-no-longer-active"),
            _ => None,
        }
    }
}

pub type ProtocolResult<T> = core::result::Result<T, ProtocolError>;
