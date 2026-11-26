//! Approving, removing and recovering devices (`E2EE_PROTOCOL.md` §2.5,
//! §2.8, §2.9; core hooks note H8).
//!
//! **Approval.** An active device [challenges](E2eeClient::challenge_device)
//! a device that published a request; that device
//! [answers](E2eeClient::answer_challenge) with a proof of possession that
//! reveals its approval nonce, and shows an approval code; the active
//! device [approves](E2eeClient::approve_device) once the user confirmed
//! that its own code matches. The nonce answers one challenge only: before
//! the proof reveals it the runtime rewrites the device document with the
//! nonce marked spent and waits for the store to confirm, a device whose
//! document says spent refuses every challenge, and a device that sees a
//! proof of its own that it did not make in this run refuses too.
//! A device whose nonce is spent but whose proof the authority never
//! showed (the publish failed, or the runtime restarted between the mark and
//! the publish) can answer nothing: the next answer or recovery wipes it
//! and it enrols again as a new device, which the approver challenges anew.
//! A proof is never published twice.
//!
//! **Removal.** [`E2eeClient::remove_device`] rotates the account without
//! the device; a device that wants to leave writes a
//! [removal request](E2eeClient::request_removal) another device
//! [completes](E2eeClient::complete_removal_requests).
//!
//! **Recovery.** [`E2eeClient::start_device_account`] registers the root a
//! device account's secret derives whenever the device starts active in
//! it; [`E2eeClient::register_recovery`] registers the root of any
//! recovery secret, and [`E2eeClient::recover_device`] approves
//! this device with it. The secret and the keys derived from it live in
//! memory for the call only and are never stored.

use jazz::db::Db;
use jazz::query::{Query, col, eq, lit};
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::account::build::{
    AccountWriter, Challenge, LocalDevice, RecoveryApprovalRecords, RecoveryCommitmentKey,
    RotationTarget, approve, challenge, recovery_approve, recovery_commitment, recovery_delivery,
    recovery_root, removal_request, respond, rotate,
};
use jazz_e2ee::protocol::account::{
    AccountKey, AccountKeyEvidence, AccountReplay, AccountState, verify_private,
};
use jazz_e2ee::protocol::derive::{RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::device_account::DeviceAccountIdentity;
use jazz_e2ee::protocol::error::{RecoveryFailure, RefusalCode, UnsupportedCapability};
use jazz_e2ee::protocol::position::Position;
use jazz_e2ee::protocol::records::account::{DeviceProof, DeviceRequest, approval_code};
use jazz_e2ee::protocol::{Accepted, ProtocolError};

use super::{ATTEMPTS, AccountRead, AccountStatus, FloorCheck, rows};
use crate::client::{E2eeClient, SharedRandom};
use crate::error::ClientError;
use crate::state::WatchedRows;

/// What a device that answers or recovers finds: a proof to build, or a
/// nonce spent without a proof of its own at the authority.
enum Attempt<T> {
    Ready(T),
    Stranded,
}

/// A challenge this device made to another, with the secret it seals, kept
/// in memory only and never printed (protocol §2.5).
pub struct PendingApproval {
    challenge: Challenge,
}

impl std::fmt::Debug for PendingApproval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingApproval")
            .field("device", &self.challenge.request.id)
            .finish_non_exhaustive()
    }
}

/// A device of the account, with what its records say about it, so an
/// application can show the user which device they are approving or
/// removing. The records hold no name and no time: a device is told apart
/// by when its request arrived, relative to the others, and by who approved
/// it. More than one device waiting is worth showing as such: a code
/// someone else asks the user to confirm may be theirs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeviceInfo {
    pub id: String,
    pub status: DeviceStatus,
    /// Whether it is the device this runtime runs on.
    pub this_device: bool,
    /// The accepted position of its request: a later request has a greater
    /// one. `None` if the read shows no request for it.
    pub requested_at: Option<Position>,
    /// How an active device entered the account; `None` for one waiting.
    pub approved_by: Option<ApprovedBy>,
    /// Whether it asked to be removed and waits for another device to
    /// complete the removal (protocol §2.8).
    pub removal_requested: bool,
}

/// How an active device entered the account, by its latest approval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovedBy {
    /// It created the account (or reset it) with its root.
    AccountRoot,
    /// The active device of this ID approved it by challenge (§2.5).
    Device(String),
    /// It restored itself with the recovery root of this ID (§2.9).
    Recovery(String),
}

/// Whether a device is approved or waits for approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceStatus {
    Active,
    Waiting,
}

/// This device in its account as one read shows it: the state, the
/// authenticated account key and the device.
struct View {
    state: AccountState,
    key: AccountKey,
    device: LocalDevice,
}

impl E2eeClient {
    pub(super) fn writer(&self) -> Result<AccountWriter<'_>, ClientError> {
        Ok(AccountWriter {
            account_secret_root_id: self.account_secret_root_id.as_deref(),
            suite: self.shared.registry.select(&self.shared.selected)?,
            registry: &self.shared.registry,
            domain: &self.shared.domain,
            account_id: &self.shared.account_id,
        })
    }

    /// Runs `step` in serializable transactions, reading again after a
    /// conflict.
    pub(super) async fn serializable<T>(
        &self,
        db: &Db,
        mut step: impl AsyncFnMut(OpenTransactionId) -> Result<T, ClientError>,
    ) -> Result<T, ClientError> {
        super::serializable(db, ATTEMPTS, async |tx| {
            self.ensure_open()?;
            step(tx).await
        })
        .await
        .map_err(|error| self.forget_account_on(error))
    }

    /// This device as an active device of its account at `read`'s cut, with
    /// the authenticated account key. A read that shows less than the
    /// device's floor is refused first (protocol §2.4).
    pub(super) async fn account_state_and_device<'a>(
        &'a self,
        read: &'a AccountRead,
    ) -> Result<(AccountReplay<'a>, AccountState, LocalDevice), ClientError> {
        let device = self.shared.device()?.clone();
        if self.check_floor_first(read, &device.id).await? == FloorCheck::NoLongerActive {
            return Err(ProtocolError::DeviceNoLongerActive.into());
        }
        let replay = AccountReplay::new(&read.public, &self.shared.domain, &self.shared.registry)?;
        let state = replay
            .current()?
            .ok_or(ProtocolError::InconsistentEvidence)?;
        Ok((replay, state, device))
    }

    async fn view(&self, read: &AccountRead) -> Result<View, ClientError> {
        let (replay, state, device) = self.account_state_and_device(read).await?;
        if !state.is_active(&device.id) {
            return Err(ProtocolError::Refused(RefusalCode::DeviceNotActive).into());
        }
        let evidence = verify_private(&replay, &state, &read.private)?;
        let keys = AccountKeyEvidence::new(&replay, &state, &evidence)?;
        let key = keys.acquire(&device.id, std::slice::from_ref(&device.sealing))?;
        Ok(View { state, key, device })
    }

    /// The approval code this challenge and proof give the requester's keys
    /// (protocol §2.5).
    fn code(
        &self,
        challenge: &jazz_e2ee::protocol::records::account::DeviceChallenge,
        keys: &jazz_e2ee::protocol::records::KeyColumns,
        proof_nonce: &[u8],
    ) -> Result<String, ClientError> {
        Ok(approval_code(
            &self.shared.domain.account(&self.shared.account_id)?,
            &self.shared.account_id,
            challenge.coordinates(),
            keys,
            &challenge.nonce,
            proof_nonce,
        )?)
    }

    // ---- approval -------------------------------------------------------

    /// Challenges the device `device_id`, which published a request (protocol
    /// §2.5 step 1): this device is active and holds the account key, and the
    /// request matches the keys the server shows. The caller (the user, in
    /// the application) asked for this one approval: the runtime never
    /// re-challenges silently. Needs the authority.
    pub async fn challenge_device(
        &self,
        db: &Db,
        device_id: &str,
    ) -> Result<PendingApproval, ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let view = self.view(&read).await?;
            let request: DeviceRequest = read
                .private
                .requests
                .iter()
                .find(|request| request.row.id == device_id)
                .map(|request| request.row.clone())
                .ok_or(ProtocolError::Refused(RefusalCode::NoDeviceRequest))?;
            let made = challenge(
                &self.writer()?,
                &view.state,
                &view.device,
                &request,
                &mut SharedRandom(&self.shared.random),
            )?;
            self.stage(db, tx, &made.challenge).await?;
            self.publish(db, tx).await?;
            Ok(PendingApproval { challenge: made })
        })
        .await
    }

    /// The proof the device answered `pending` with, if it did.
    async fn proof_of(
        &self,
        db: &Db,
        pending: &PendingApproval,
    ) -> Result<Option<Accepted<DeviceProof>>, ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            Ok(read
                .private
                .proofs
                .into_iter()
                .find(|proof| proof.row.challenge_id == pending.challenge.challenge.id))
        })
        .await
    }

    /// The approval code this device shows once the device answered the
    /// challenge: `None` while no proof is there (protocol §2.5). The user
    /// compares it with the code the other device shows.
    pub async fn approval_code_of(
        &self,
        db: &Db,
        pending: &PendingApproval,
    ) -> Result<Option<String>, ClientError> {
        let Some(proof) = self.proof_of(db, pending).await? else {
            return Ok(None);
        };
        Ok(Some(self.code(
            &pending.challenge.challenge,
            &pending.challenge.request.keys,
            &proof.row.nonce,
        )?))
    }

    /// Answers the challenge made to this device (protocol §2.5 step 2) and
    /// returns the approval code to show: it checks that an active device of
    /// the account signed the challenge over this device's own keys
    /// (`invalid-challenge` otherwise: the approver was shown other keys),
    /// marks the approval nonce spent in the private store and waits for
    /// the store to confirm, and only then publishes the proof that reveals
    /// it. A device that sees a proof of its own refuses
    /// ([`ClientError::ApprovalAnswered`]); one whose document says spent
    /// but that sees none never published it, and enrols again as a new
    /// device ([`ClientError::NoChallenge`]). Needs the authority.
    pub async fn answer_challenge(&self, db: &Db) -> Result<String, ClientError> {
        let attempt = self
            .serializable(db, async |tx| {
                let read = self.read_account(db, tx).await?;
                let (_, state, device) = self.account_state_and_device(&read).await?;
                let mine: Vec<_> = read
                    .private
                    .challenges
                    .iter()
                    .filter(|challenge| challenge.row.device_id == device.id)
                    .collect();
                // A proof of this device's own: its nonce was revealed.
                if read.private.proofs.iter().any(|proof| {
                    mine.iter()
                        .any(|challenge| challenge.row.id == proof.row.challenge_id)
                }) {
                    return Err(ClientError::ApprovalAnswered);
                }
                // Spent, and no proof of its own reached the authority: a
                // device still waiting for approval starts again. One the
                // account lists as active, or that verified it, refuses: a
                // server hiding its proof must not wipe the keys it holds.
                if device.approval_nonce.is_none() {
                    return if state.is_active(&device.id) || self.account.borrow().is_some() {
                        Err(ClientError::ApprovalAnswered)
                    } else {
                        Ok(Attempt::Stranded)
                    };
                }
                // The newest valid challenge of the epoch: one that is not
                // (junk the server shows, one signed over other keys, or one
                // whose answer proves it invalid, see `newest_answered`)
                // never hides a valid one.
                let mut candidates: Vec<_> = mine
                    .iter()
                    .filter(|challenge| challenge.row.epoch_id == state.epoch_id())
                    .collect();
                candidates.sort_by_key(|challenge| std::cmp::Reverse(challenge.position));
                let writer = self.writer()?;
                let (proof, candidate) = newest_answered(candidates, |candidate| {
                    // Built, not revealed: the nonce is still unspent.
                    let mut scratch = device.clone();
                    respond(
                        &writer,
                        &state,
                        &mut scratch,
                        &candidate.row,
                        &mut SharedRandom(&self.shared.random),
                    )
                })?;
                Ok(Attempt::Ready((proof, candidate.row.clone())))
            })
            .await?;
        let Attempt::Ready((proof, challenge)) = attempt else {
            self.reenrol().await?;
            return Err(ClientError::NoChallenge);
        };
        // Spent in the store, and confirmed, before the proof reveals it.
        let keys = self.shared.device()?.keys();
        let code = self.code(&challenge, &keys, &proof.nonce)?;
        self.mark_nonce_spent().await?;
        self.publish_proof(db, &proof).await?;
        Ok(code)
    }

    /// Publishes `proof`.
    async fn publish_proof(&self, db: &Db, proof: &DeviceProof) -> Result<(), ClientError> {
        self.serializable(db, async |tx| {
            self.stage(db, tx, proof).await?;
            self.publish(db, tx).await?;
            Ok(())
        })
        .await
    }

    /// Approves the device `pending` was made for, once the user confirmed
    /// that the approval code it shows is `code` (protocol §2.5 step 3): the
    /// proof must open the request's nonce commitment and verify, the request
    /// must be the one the challenge was signed over, and `code` must be the
    /// one this device computes; a mismatch means someone between the
    /// devices substituted keys, and nothing is written
    /// ([`ClientError::ApprovalCodeMismatch`]). Inserts the approval, the
    /// public approval and the delivery of the account key in one
    /// transaction. Needs the authority.
    pub async fn approve_device(
        &self,
        db: &Db,
        pending: &PendingApproval,
        code: &str,
    ) -> Result<(), ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let view = self.view(&read).await?;
            let proof = read
                .private
                .proofs
                .iter()
                .find(|proof| proof.row.challenge_id == pending.challenge.challenge.id)
                .map(|proof| proof.row.clone())
                .ok_or(ClientError::NoChallenge)?;
            let request = read
                .private
                .requests
                .iter()
                .find(|request| request.row.id == pending.challenge.request.id)
                .map(|request| request.row.clone())
                .ok_or(ProtocolError::Refused(RefusalCode::NoDeviceRequest))?;
            let shown = self.code(&pending.challenge.challenge, &request.keys, &proof.nonce)?;
            if shown != code {
                return Err(ClientError::ApprovalCodeMismatch);
            }
            let records = approve(
                &self.writer()?,
                &view.state,
                &view.device,
                &pending.challenge,
                &proof,
                &request,
                &view.key,
                &mut SharedRandom(&self.shared.random),
            )?;
            self.stage(db, tx, &records.approval).await?;
            self.stage(db, tx, &records.public).await?;
            self.stage(db, tx, &records.delivery).await?;
            self.publish(db, tx).await?;
            Ok(())
        })
        .await
    }

    // ---- reading ---------------------------------------------------------

    /// The devices of this account as one read shows them: those active, and
    /// those whose request waits for approval (a removed device is not
    /// listed). The read is of the authority's rows, like any other call.
    pub async fn devices(&self, db: &Db) -> Result<Vec<DeviceInfo>, ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let view = self.view(&read).await?;
            let this = &view.device.id;
            let requested_at = |id: &str| {
                read.private
                    .requests
                    .iter()
                    .find(|request| request.row.id == id)
                    .map(|request| request.position)
            };
            // The latest approval counts: a device carried into a later
            // epoch keeps the one that admitted it.
            let approved_by = |id: &str| {
                let latest = view
                    .state
                    .approvals
                    .iter()
                    .rev()
                    .find(|approval| approval.row.device_id == id);
                match latest {
                    Some(approval) => Some(match &approval.row.recovery_root_id {
                        Some(root) => ApprovedBy::Recovery(root.clone()),
                        None => ApprovedBy::Device(approval.row.signer_id.clone()),
                    }),
                    None if view.state.root.row.device_id == id => Some(ApprovedBy::AccountRoot),
                    None => None,
                }
            };
            let mut listed: Vec<DeviceInfo> = view
                .state
                .active
                .iter()
                .map(|id| DeviceInfo {
                    id: id.clone(),
                    status: DeviceStatus::Active,
                    this_device: id == this,
                    requested_at: requested_at(id),
                    approved_by: approved_by(id),
                    removal_requested: view.state.removal_requested.contains(id),
                })
                .collect();
            for request in &read.private.requests {
                let id = &request.row.id;
                if !view.state.active.contains(id) && !view.state.removed.contains(id) {
                    listed.push(DeviceInfo {
                        id: id.clone(),
                        status: DeviceStatus::Waiting,
                        this_device: id == this,
                        requested_at: Some(request.position),
                        approved_by: None,
                        removal_requested: false,
                    });
                }
            }
            Ok(listed)
        })
        .await
    }

    /// Watch challenges for this device and the subset with a proof. Both
    /// queries start at the device index; proofs share their challenge's row ID.
    pub(crate) async fn watch_approval(
        &self,
        db: &Db,
    ) -> Result<(String, [WatchedRows<()>; 2]), ClientError> {
        let device_id = self.shared.device()?.id.clone();
        let device = rows::uuid_cell(&device_id).ok_or(ProtocolError::InvalidRecord(
            "a device ID that is not a UUID",
        ))?;
        let challenges =
            Query::from(rows::DEVICE_CHALLENGES).filter(eq(col("deviceId"), lit(device)));
        let proved = challenges
            .clone()
            .join_via_row_id(rows::DEVICE_PROOFS, "id", []);
        let challenges = db
            .subscribe(&db.prepare_query(&challenges)?, Default::default())
            .await?;
        let proved = db
            .subscribe(&db.prepare_query(&proved)?, Default::default())
            .await?;
        Ok((
            device_id,
            [WatchedRows::new(challenges), WatchedRows::new(proved)],
        ))
    }

    /// Whether a challenge for this device of the account's current epoch is
    /// waiting for its answer: what a device awaiting approval looks for, so
    /// it can answer without polling blindly.
    pub async fn challenged(&self, db: &Db) -> Result<bool, ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let device = self.shared.device()?.clone();
            let replay =
                AccountReplay::new(&read.public, &self.shared.domain, &self.shared.registry)?;
            let state = replay
                .current()?
                .ok_or(ProtocolError::InconsistentEvidence)?;
            let mine = |challenge: &&Accepted<
                jazz_e2ee::protocol::records::account::DeviceChallenge,
            >| {
                challenge.row.device_id == device.id && challenge.row.epoch_id == state.epoch_id()
            };
            Ok(read
                .private
                .challenges
                .iter()
                .filter(mine)
                .any(|challenge| {
                    !read
                        .private
                        .proofs
                        .iter()
                        .any(|proof| proof.row.challenge_id == challenge.row.id)
                }))
        })
        .await
    }

    // ---- removal --------------------------------------------------------

    /// Removes the device `device_id` and rotates the account (protocol
    /// §2.8): a fresh account epoch key delivered to the remaining devices
    /// and to the unretired recovery roots, and the recovery roots the
    /// device held retired in the same rotation. The device never removes
    /// itself. Verifies the account again, then runs the delivery pass over
    /// every space this device holds keys of, which marks those whose
    /// current version the removed device could open (§3.5): this device's
    /// next write to each rotates it first. A space whose pass fails is
    /// marked anyway (fail closed). Needs the authority.
    pub async fn remove_device(&self, db: &Db, device_id: &str) -> Result<(), ClientError> {
        self.rotate_account(db, RotationTarget::RemoveDevice(device_id.to_owned()))
            .await?;
        self.ensure_account(db).await?;
        self.mark_after_device_removal(db).await;
        Ok(())
    }

    /// Retires the recovery root `root_id` in an account rotation (protocol
    /// §2.8, §2.9).
    pub async fn retire_recovery_root(&self, db: &Db, root_id: &str) -> Result<(), ClientError> {
        self.rotate_account(db, RotationTarget::RetireRecoveryRoot(root_id.to_owned()))
            .await?;
        self.ensure_account(db).await?;
        Ok(())
    }

    async fn rotate_account(&self, db: &Db, target: RotationTarget) -> Result<(), ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let view = self.view(&read).await?;
            let records = rotate(
                &self.writer()?,
                &view.state,
                &view.device,
                &view.key,
                &target,
                &mut SharedRandom(&self.shared.random),
            )?;
            self.stage_rotation(db, tx, &records).await?;
            self.publish(db, tx).await?;
            Ok(())
        })
        .await
    }

    /// Writes this device's removal request (protocol §2.8): refused with
    /// `no-other-device` unless another active device could complete it. The
    /// device stays active and keeps its keys until another device removes
    /// it. Needs the authority.
    pub async fn request_removal(&self, db: &Db) -> Result<(), ClientError> {
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let view = self.view(&read).await?;
            let request = removal_request(
                &self.writer()?,
                &view.state,
                &view.device,
                &mut SharedRandom(&self.shared.random),
            )?;
            self.stage(db, tx, &request).await?;
            self.publish(db, tx).await?;
            Ok(())
        })
        .await
    }

    /// Whether verification found a removal request this device can complete.
    pub(crate) fn may_have_other_removal_requests(&self) -> bool {
        let Ok(device) = self.shared.device() else {
            return true;
        };
        self.with_account(|account| {
            account
                .state
                .removal_requested
                .iter()
                .any(|id| id != &device.id)
        })
        .unwrap_or(true)
    }

    /// Completes the pending removal requests of this account's other
    /// devices, one rotation each (protocol §2.8): any active device that
    /// holds the current key does, so the requesting device never holds the
    /// new epoch's key. One request that cannot be completed does not stop
    /// the others: each request is tried once per call and reported, in the
    /// order tried, with its outcome; the account is verified again after
    /// each rotation. A failure of the account itself (this device is not
    /// active, the evidence is refused, the database fails) is the call's
    /// error. Call it after verifying the account, and when
    /// [`E2eeClient::watch_account`] reports a change.
    pub async fn complete_removal_requests(
        &self,
        db: &Db,
    ) -> Result<Vec<(String, Result<(), ClientError>)>, ClientError> {
        let mut tried: Vec<String> = Vec::new();
        let mut outcomes = Vec::new();
        loop {
            let next = self
                .serializable(db, async |tx| {
                    let read = self.read_account(db, tx).await?;
                    let view = self.view(&read).await?;
                    let Some(device) = view
                        .state
                        .removal_requested
                        .iter()
                        .find(|device| **device != view.device.id && !tried.contains(device))
                        .cloned()
                    else {
                        return Ok(None);
                    };
                    let item = async {
                        let records = rotate(
                            &self.writer()?,
                            &view.state,
                            &view.device,
                            &view.key,
                            &RotationTarget::RemoveDevice(device.clone()),
                            &mut SharedRandom(&self.shared.random),
                        )?;
                        self.stage_rotation(db, tx, &records).await?;
                        self.publish(db, tx).await?;
                        Ok::<(), ClientError>(())
                    }
                    .await;
                    match item {
                        // A conflict reads again, as for any other call.
                        Err(ClientError::Database(error)) if error.is_conflict() => {
                            Err(ClientError::Database(error))
                        }
                        other => Ok(Some((device, other))),
                    }
                })
                .await?;
            let Some((device, outcome)) = next else {
                return Ok(outcomes);
            };
            tried.push(device.clone());
            let done = outcome.is_ok();
            outcomes.push((device, outcome));
            if done {
                self.ensure_account(db).await?;
                self.mark_after_device_removal(db).await;
            }
        }
    }

    // ---- recovery -------------------------------------------------------

    /// Registers the recovery root `secret` derives for the account's
    /// current generation, unless a valid root with that ID exists (protocol
    /// §2.9, "Register"): the root, signed by this active device, and a
    /// delivery of the account key sealed to it. A root that was retired
    /// stays retired (§2.9, "Retiring a derived root"): registering its
    /// secret writes nothing and is refused with `recovery-root-mismatch`,
    /// the refusal a restore with it gets, so a caller never believes a
    /// secret that cannot restore is registered. When the root and its
    /// delivery for the current epoch are there and the delivery opens with the
    /// secret's key, nothing is written. What was written is not read back:
    /// a server that altered the rows can leave recovery broken whatever
    /// this call checked, so a restore may find a delivery that does not
    /// open (a known limit). The secret and the keys derived from it are in
    /// memory for this call only.
    pub async fn register_recovery(
        &self,
        db: &Db,
        secret: &RecoverySecret,
    ) -> Result<(), ClientError> {
        self.register(db, secret, false).await
    }

    /// Starts a device account on this device (protocol §2.9, "Register"):
    /// verifies the account ([`Self::ensure_account`]) and, once this
    /// device is active in it (right after bootstrap, or after approval),
    /// registers the recovery root the account secret derives, so every
    /// device account has recovery by its secret without the application
    /// asking for it. A root of that secret that was retired stays retired:
    /// nothing is written and the device still starts. A device awaiting
    /// approval registers nothing; it registers at the first start that finds
    /// it active. Verifies the account again after a registration, so
    /// [`Self::with_account`] lists the root.
    ///
    /// `secret` is the device account's account secret, as the runtime was
    /// opened with its identity (`ClientConfig::device_account`): a secret
    /// of another identity is refused with `recovery-root-mismatch` and
    /// nothing is written, and a runtime of an account of the application's
    /// own sign-in is refused as unsupported (it registers its separate
    /// secret with [`Self::register_recovery`]). The secret is in memory for
    /// this call only. Needs the authority.
    pub async fn start_device_account(
        &self,
        db: &Db,
        secret: &RecoverySecret,
    ) -> Result<AccountStatus, ClientError> {
        self.ensure_open()?;
        if self.device_account.borrow().is_none() {
            return Err(ClientError::Unsupported(
                "start_device_account is for a device account: this runtime was opened \
                 without its identity"
                    .to_owned(),
            ));
        }
        self.refuse_foreign_device_secret(secret)?;
        let status = self.ensure_account(db).await?;
        if status != AccountStatus::Active {
            return Ok(status);
        }
        self.register_recovery_at_start(db, secret).await?;
        self.ensure_account(db).await
    }

    /// Refuses `secret` with `recovery-root-mismatch` when this runtime was
    /// opened as a device account (`ClientConfig::device_account`) and the
    /// secret derives another identity: a start-up registration takes only
    /// the device account's own secret.
    pub(crate) fn refuse_foreign_device_secret(
        &self,
        secret: &RecoverySecret,
    ) -> Result<(), ClientError> {
        if let Some(identity) = self.device_account.borrow().as_ref()
            && DeviceAccountIdentity::from_secret(secret)?.public_key() != identity.public_key()
        {
            return Err(ProtocolError::Recovery(RecoveryFailure::RootMismatch).into());
        }
        Ok(())
    }

    /// The registration a start-up runs with a device account's own secret:
    /// as [`Self::register_recovery`], except that a retired root writes
    /// nothing and is not an error, so the device still starts.
    pub(crate) async fn register_recovery_at_start(
        &self,
        db: &Db,
        secret: &RecoverySecret,
    ) -> Result<(), ClientError> {
        self.register(db, secret, true).await
    }

    async fn register(
        &self,
        db: &Db,
        secret: &RecoverySecret,
        at_start: bool,
    ) -> Result<(), ClientError> {
        let domain = self.shared.domain.account(&self.shared.account_id)?;
        // Slow to derive: kept across the transaction's retries, and derived
        // only when a new root is written.
        let mut commitment_key: Option<RecoveryCommitmentKey> = None;
        self.serializable(db, async |tx| {
            let read = self.read_account(db, tx).await?;
            let view = self.view(&read).await?;
            let keys = RecoveryKeys::derive(secret, &domain, view.state.generation)?;
            // Retired: it stays retired (§2.9, "Retiring a derived root"), so
            // there is nothing to register. A start with this secret goes on;
            // an explicit call is refused as a restore with it would be.
            if view
                .state
                .retired_recovery_roots
                .contains_key(&keys.root_id)
            {
                return if at_start {
                    Ok(())
                } else {
                    Err(ProtocolError::Recovery(RecoveryFailure::RootMismatch).into())
                };
            }
            // Registered already, with its delivery for the current epoch:
            // nothing to write, so a binding may call this at every start.
            // The delivery must open with the recovery key: a row that is
            // merely there (junk from the server) would leave recovery
            // silently broken. Only a delivery that is missing or does not
            // open for this client is written again; a provider failure, or
            // any other error, stops the registration and writes nothing.
            if view.state.recovery_root(&keys.root_id).is_some() {
                let replay =
                    AccountReplay::new(&read.public, &self.shared.domain, &self.shared.registry)?;
                let evidence = verify_private(&replay, &view.state, &read.private)?;
                match AccountKeyEvidence::new(&replay, &view.state, &evidence)?
                    .acquire_with_recovery(&keys, &read.private.recovery_deliveries)
                {
                    Ok(_) => return Ok(()),
                    Err(
                        ProtocolError::Recovery(
                            RecoveryFailure::DeliveryMissing | RecoveryFailure::DeliveryUnusable,
                        )
                        | ProtocolError::Unsupported(UnsupportedCapability::AccountKeyEnvelope),
                    ) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let writer = self.writer()?;
            let root = match view.state.recovery_root(&keys.root_id) {
                Some(existing) => existing.row.clone(),
                None => {
                    let root = recovery_root(&writer, &view.state, &view.device, &keys)?;
                    let key = match commitment_key.take() {
                        Some(key) if key.generation() == view.state.generation => key,
                        _ => RecoveryCommitmentKey::derive(secret, &domain, view.state.generation)?,
                    };
                    let commitment = recovery_commitment(
                        &writer,
                        secret,
                        &key,
                        &mut SharedRandom(&self.shared.random),
                    )?;
                    commitment_key = Some(key);
                    self.stage(db, tx, &root).await?;
                    self.stage(db, tx, &commitment).await?;
                    root
                }
            };
            let delivery = recovery_delivery(
                &writer,
                &view.state,
                &root,
                &view.key,
                &mut SharedRandom(&self.shared.random),
            )?;
            self.stage(db, tx, &delivery).await?;
            self.publish(db, tx).await?;
            Ok(())
        })
        .await
    }

    /// Restores this device's access with `secret` (protocol §2.9,
    /// "Restore"): it derives the root of the account's current generation,
    /// obtains the account key from a recovery delivery, and runs the
    /// approval with itself as both approver and requester, the root
    /// authorising it. Like any answer to a challenge it reveals the
    /// approval nonce, so it is marked spent first. Verifies the account
    /// afterwards. A root that was retired is refused
    /// (`recovery-root-mismatch`). The secret is in memory for this call only.
    pub async fn recover_device(
        &self,
        db: &Db,
        secret: &RecoverySecret,
    ) -> Result<AccountStatus, ClientError> {
        self.restore_device(db, secret, false)
            .await
            .map(|status| status.expect("explicit recovery requires evidence"))
    }

    /// Automatic account-secret recovery can wait while the authority does
    /// not yet show a root or delivery, or while a newly enrolled device
    /// still needs to recover. A retired root ends the wait: the device
    /// stays awaiting approval (`Some`). Evidence that is present but
    /// refused still fails the operation.
    pub(crate) async fn recover_device_when_available(
        &self,
        db: &Db,
        secret: &RecoverySecret,
    ) -> Result<Option<AccountStatus>, ClientError> {
        self.restore_device(db, secret, true).await
    }

    async fn restore_device(
        &self,
        db: &Db,
        secret: &RecoverySecret,
        automatic: bool,
    ) -> Result<Option<AccountStatus>, ClientError> {
        let attempt = self
            .serializable(db, async |tx| {
                let read = self.read_account(db, tx).await?;
                let (replay, state, mut device) = self.account_state_and_device(&read).await?;
                if device.approval_nonce.is_none() {
                    // Spent: a device that is active, or whose proof the
                    // authority shows, recovered already; any other never
                    // published its proof.
                    let published = read.private.proofs.iter().any(|proof| {
                        read.private.challenges.iter().any(|challenge| {
                            challenge.row.device_id == device.id
                                && challenge.row.id == proof.row.challenge_id
                        })
                    });
                    return if published || state.is_active(&device.id) {
                        Err(ClientError::ApprovalAnswered)
                    } else {
                        Ok(Some(Attempt::Stranded))
                    };
                }
                let keys = RecoveryKeys::derive(
                    secret,
                    &self.shared.domain.account(&self.shared.account_id)?,
                    state.generation,
                )?;
                let evidence = verify_private(&replay, &state, &read.private)?;
                let account_keys = AccountKeyEvidence::new(&replay, &state, &evidence)?;
                let key = match account_keys
                    .acquire_with_recovery(&keys, &read.private.recovery_deliveries)
                {
                    Err(ProtocolError::Recovery(RecoveryFailure::RootMismatch))
                        if automatic
                            && !state.retired_recovery_roots.contains_key(&keys.root_id)
                            && !read
                                .public
                                .recovery_roots
                                .iter()
                                .any(|root| root.row.id == keys.root_id) =>
                    {
                        return Ok(None);
                    }
                    // A retired root never comes back: automatic recovery
                    // stops waiting and the device stays awaiting approval.
                    // Explicit recovery is refused.
                    Err(ProtocolError::Recovery(RecoveryFailure::RootMismatch))
                        if automatic
                            && state.retired_recovery_roots.contains_key(&keys.root_id) =>
                    {
                        return Ok(Some(Attempt::Ready(None)));
                    }
                    Err(ProtocolError::Recovery(RecoveryFailure::DeliveryMissing))
                        if automatic
                            && !read.private.recovery_deliveries.iter().any(|delivery| {
                                delivery.row.root_id == keys.root_id
                                    && delivery.row.epoch_id == state.epoch_id()
                            }) =>
                    {
                        return Ok(None);
                    }
                    result => result?,
                };
                Ok(recovery_approve(
                    &self.writer()?,
                    &state,
                    &mut device,
                    &keys,
                    &key,
                    &mut SharedRandom(&self.shared.random),
                )
                .map(|records| Some(Attempt::Ready(records)))?)
            })
            .await?;
        match attempt {
            None => return Ok(None),
            Some(Attempt::Stranded) => {
                self.reenrol().await?;
                let status = self.ensure_account(db).await?;
                // Retain the account secret for a new automatic attempt
                // with this device's fresh approval nonce.
                if automatic && status == AccountStatus::AwaitingApproval {
                    return Ok(None);
                }
                return Ok(Some(status));
            }
            Some(Attempt::Ready(None)) => {}
            Some(Attempt::Ready(Some(records))) => {
                self.clear_verified_history().await?;
                self.mark_nonce_spent().await?;
                self.publish_recovery(db, &records).await?;
            }
        }
        self.ensure_account(db).await.map(Some)
    }

    /// Publishes the records of a recovery in one transaction.
    async fn publish_recovery(
        &self,
        db: &Db,
        records: &RecoveryApprovalRecords,
    ) -> Result<(), ClientError> {
        self.serializable(db, async |tx| {
            self.stage(db, tx, &records.challenge).await?;
            self.stage(db, tx, &records.proof).await?;
            self.stage(db, tx, &records.records.approval).await?;
            self.stage(db, tx, &records.records.public).await?;
            self.stage(db, tx, &records.records.delivery).await?;
            self.publish(db, tx).await?;
            Ok(())
        })
        .await
    }
}

/// The first of `candidates` (challenges, newest first) that `answer`
/// accepts, with its answer. One whose answer fails in a way that proves
/// the challenge itself invalid (see [`proves_challenge_invalid`]) is
/// skipped: it never hides a valid one. Any other failure (a mechanism this
/// build does not support, a provider or random-source failure) says
/// nothing against the challenge, which may be the one the approver waits
/// on, and stops the search with that error, before any nonce is spent.
/// With none accepted: the first skipped failure that is not
/// `invalid-challenge`, else `invalid-challenge`, else
/// [`ClientError::NoChallenge`].
fn newest_answered<C, T>(
    candidates: impl IntoIterator<Item = C>,
    mut answer: impl FnMut(&C) -> Result<T, ProtocolError>,
) -> Result<(T, C), ClientError> {
    let mut invalid = None;
    let mut failed = None;
    for candidate in candidates {
        match answer(&candidate) {
            Ok(answered) => return Ok((answered, candidate)),
            Err(error) if !proves_challenge_invalid(&error) => return Err(error.into()),
            Err(error @ ProtocolError::Refused(RefusalCode::InvalidChallenge)) => {
                invalid.get_or_insert(error);
            }
            Err(error) => {
                failed.get_or_insert(error);
            }
        }
    }
    Err(failed
        .or(invalid)
        .map_or(ClientError::NoChallenge, ClientError::from))
}

/// Whether answering a challenge failed because of the challenge itself:
/// it is not for this device's keys or epoch (`invalid-challenge`,
/// `stale-challenge`), its record is malformed, or its signature or sealed
/// envelope is junk (malformed, unauthentic, or sealed to a key of a
/// mechanism this device does not hold). Unsupported mechanisms or formats,
/// provider and random-source failures, and contradictions of the account
/// state are not.
fn proves_challenge_invalid(error: &ProtocolError) -> bool {
    matches!(
        error,
        ProtocolError::Refused(RefusalCode::InvalidChallenge | RefusalCode::StaleChallenge)
            | ProtocolError::InvalidRecord(_)
            | ProtocolError::Crypto(
                jazz_e2ee::Error::InvalidCiphertext
                    | jazz_e2ee::Error::InvalidInput
                    | jazz_e2ee::Error::NoKeyForMechanism
            )
    )
}

#[cfg(test)]
mod tests {
    //! Unit tests of the challenge choice: a challenge whose answer fails
    //! with an error (not a refusal) needs one an active member signed over
    //! a junk envelope, or a mechanism the build does not route, which the
    //! integration harness cannot produce (it holds no device's signing key
    //! and routes every mechanism). The refusal case is covered end to end
    //! by `an_invalid_newer_challenge_does_not_hide_a_valid_one`. A failure
    //! that stops the choice returns before `answer_challenge` spends the
    //! nonce.
    use super::*;
    use jazz_e2ee::protocol::error::UnsupportedCapability;

    fn unsupported() -> ProtocolError {
        ProtocolError::Unsupported(UnsupportedCapability::AccountKeyEnvelope)
    }

    fn junk() -> ProtocolError {
        ProtocolError::Crypto(jazz_e2ee::Error::InvalidCiphertext)
    }

    /// Contract: each failure that proves a challenge invalid (a bad
    /// signature or junk envelope, a wrong account or epoch, a malformed
    /// record, a sealed key of a mechanism this device lacks) is skipped,
    /// and the older valid challenge is answered.
    ///
    /// ```text
    /// newest: junk ──answer──✗ <proof of invalid> (skipped)
    /// older:  valid ──answer──► answered
    /// ```
    #[test]
    fn each_failure_that_proves_a_challenge_invalid_is_skipped() {
        for proof in [
            ProtocolError::Refused(RefusalCode::InvalidChallenge),
            ProtocolError::Refused(RefusalCode::StaleChallenge),
            ProtocolError::InvalidRecord("junk record"),
            ProtocolError::Crypto(jazz_e2ee::Error::InvalidCiphertext),
            ProtocolError::Crypto(jazz_e2ee::Error::InvalidInput),
            ProtocolError::Crypto(jazz_e2ee::Error::NoKeyForMechanism),
        ] {
            let answered = newest_answered(["junk", "valid"], |candidate| match *candidate {
                "valid" => Ok(7),
                _ => Err(proof),
            });
            assert!(
                matches!(answered, Ok((7, "valid"))),
                "{proof:?}: {answered:?}"
            );
        }
    }

    /// Contract: only a failure that proves the challenge invalid is
    /// skipped. A newer challenge this device cannot judge (an unsupported
    /// mechanism), a provider failure and a random-source failure say
    /// nothing against it: the choice stops with that error, no older
    /// challenge is answered, and the nonce stays unspent.
    ///
    /// ```text
    /// newest: genuine ──answer──✗ unsupported / provider / random ──► stop
    /// older:  valid   (not answered)
    /// ```
    #[test]
    fn a_failure_that_does_not_prove_the_challenge_invalid_stops_the_choice() {
        for stop in [
            unsupported(),
            ProtocolError::ProviderFailure,
            ProtocolError::from(jazz_e2ee::Error::RandomUnavailable),
        ] {
            let stopped = newest_answered(["newer", "valid"], |candidate| match *candidate {
                "valid" => Ok(7),
                _ => Err(stop),
            });
            assert!(
                matches!(&stopped, Err(ClientError::Protocol(error)) if *error == stop),
                "{stop:?}: {stopped:?}"
            );
        }
    }

    /// Contract: with no challenge answered, a skipped failure other than
    /// `invalid-challenge` is reported over it; with none, no challenge.
    #[test]
    fn with_none_answered_the_most_specific_failure_is_reported() {
        let none: Result<(u8, _), _> = newest_answered(["invalid", "junk"], |candidate| {
            Err(match *candidate {
                "invalid" => ProtocolError::Refused(RefusalCode::InvalidChallenge),
                _ => junk(),
            })
        });
        assert!(
            matches!(&none, Err(ClientError::Protocol(error)) if *error == junk()),
            "{none:?}"
        );
        let none: Result<(u8, &str), _> = newest_answered([], |_: &&str| Ok(0));
        assert!(matches!(none, Err(ClientError::NoChallenge)), "{none:?}");
    }
}
