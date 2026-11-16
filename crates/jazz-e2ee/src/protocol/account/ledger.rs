//! One account's rows from one ledger read (`E2EE_PROTOCOL.md` §0 item 3,
//! §2.4).

use super::super::error::{ProtocolError, ProtocolResult};
use super::super::position::{Accepted, Position, before, check_read};
use super::super::records::account::{
    AccountIdentity, AccountRoot, AccountSuccessor, DeviceApproval, DeviceChallenge,
    DeviceDelivery, DeviceKeysRow, DeviceProof, DeviceRequest, PublicAccountSuccessor,
    PublicDeviceApproval, RecoveryDelivery, RecoveryRoot,
};

/// The public rows of one account at cut `through`: everything account
/// replay reads (R10). Device keys are the account's `__e2ee_device_keys`
/// rows; the other tables may hold rows naming other accounts, which replay
/// ignores or, for roots, treats as contradictions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAccountLedger {
    pub account_id: String,
    pub through: Position,
    pub roots: Vec<Accepted<AccountRoot>>,
    pub keys: Vec<Accepted<DeviceKeysRow>>,
    pub approvals: Vec<Accepted<PublicDeviceApproval>>,
    pub successors: Vec<Accepted<PublicAccountSuccessor>>,
    pub recovery_roots: Vec<Accepted<RecoveryRoot>>,
}

impl PublicAccountLedger {
    /// An empty ledger read of `account_id` at `through`.
    pub fn new(account_id: &str, through: Position) -> Self {
        Self {
            account_id: account_id.to_owned(),
            through,
            roots: Vec::new(),
            keys: Vec::new(),
            approvals: Vec::new(),
            successors: Vec::new(),
            recovery_roots: Vec::new(),
        }
    }

    /// Every row is at or before the cut, and no table repeats a row ID.
    pub fn check(&self) -> ProtocolResult<()> {
        if self.account_id.is_empty() {
            return Err(ProtocolError::InvalidRecord("empty account ID"));
        }
        check_read(&self.roots, self.through)?;
        check_read(&self.keys, self.through)?;
        check_read(&self.approvals, self.through)?;
        check_read(&self.successors, self.through)?;
        check_read(&self.recovery_roots, self.through)
    }

    /// The rows accepted strictly before `cut` (§2.4, "State as of a
    /// position"). Device keys are filtered like every other row.
    pub fn before(&self, cut: Position) -> Self {
        Self {
            account_id: self.account_id.clone(),
            through: Position(cut.0.saturating_sub(1)).min(self.through),
            roots: before(&self.roots, cut),
            keys: before(&self.keys, cut),
            approvals: before(&self.approvals, cut),
            successors: before(&self.successors, cut),
            recovery_roots: before(&self.recovery_roots, cut),
        }
    }

    /// Whether the account has any public evidence at all.
    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
            && self.approvals.is_empty()
            && self.successors.is_empty()
            && self.recovery_roots.is_empty()
    }
}

/// The private rows of one account, read at the same cut as its public
/// ledger (§2.4, "Private verification").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateAccountLedger {
    pub through: Position,
    pub identities: Vec<Accepted<AccountIdentity>>,
    pub requests: Vec<Accepted<DeviceRequest>>,
    pub challenges: Vec<Accepted<DeviceChallenge>>,
    pub proofs: Vec<Accepted<DeviceProof>>,
    pub approvals: Vec<Accepted<DeviceApproval>>,
    pub deliveries: Vec<Accepted<DeviceDelivery>>,
    pub successors: Vec<Accepted<AccountSuccessor>>,
    pub recovery_deliveries: Vec<Accepted<RecoveryDelivery>>,
}

impl PrivateAccountLedger {
    pub fn new(through: Position) -> Self {
        Self {
            through,
            identities: Vec::new(),
            requests: Vec::new(),
            challenges: Vec::new(),
            proofs: Vec::new(),
            approvals: Vec::new(),
            deliveries: Vec::new(),
            successors: Vec::new(),
            recovery_deliveries: Vec::new(),
        }
    }

    pub fn check(&self) -> ProtocolResult<()> {
        check_read(&self.identities, self.through)?;
        check_read(&self.requests, self.through)?;
        check_read(&self.challenges, self.through)?;
        check_read(&self.proofs, self.through)?;
        check_read(&self.approvals, self.through)?;
        check_read(&self.deliveries, self.through)?;
        check_read(&self.successors, self.through)?;
        check_read(&self.recovery_deliveries, self.through)
    }

    /// Whether the account has any private evidence besides requests.
    pub fn has_history(&self) -> bool {
        !(self.identities.is_empty()
            && self.challenges.is_empty()
            && self.proofs.is_empty()
            && self.approvals.is_empty()
            && self.deliveries.is_empty()
            && self.successors.is_empty())
    }
}
