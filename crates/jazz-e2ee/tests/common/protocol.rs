//! An account's ledger built with the crate's own record builders, accepted
//! row by row at increasing positions, for protocol tests.

use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::envelope::Mechanism;
use jazz_e2ee::protocol::account::build::{
    self, AccountWriter, ApprovalRecords, LocalDevice, ResetPolicy, RootAuthority, RotationRecords,
    RotationTarget,
};
use jazz_e2ee::protocol::account::{
    AccountKey, AccountKeyEvidence, AccountReplay, AccountState, PrivateAccountLedger,
    PrivateEvidence, PublicAccountLedger, verify_private,
};
use jazz_e2ee::protocol::derive::{RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::device_account::DeviceAccountIdentity;
use jazz_e2ee::protocol::domain::{AccountDomain, AppDomain};
use jazz_e2ee::protocol::records::account::{AccountSealingKey, RecoveryRoot};
use jazz_e2ee::protocol::{Accepted, Position, ProtocolResult, ids};
use jazz_e2ee::registry::Registry;

use super::TestRandom;

pub const APP_ID: &str = "0f8e3a4c-1d2b-4c5d-8e6f-7a8b9c0d1e2f";
/// An account ID: a UUID, as every account ID is (version 5, as Jazz's
/// account registry mints them).
pub const ALICE: &str = "a11ce000-0000-5000-8000-00000000a11c";

/// The mechanisms, domain and account under test.
pub struct Env {
    pub registry: Registry,
    pub config: MechanismConfig,
    pub domain: AppDomain,
    pub account: String,
    /// The device-account identity of a device account.
    pub identity: Option<DeviceAccountIdentity>,
}

impl Env {
    pub fn new(account: &str) -> Self {
        Self::with(Registry::builtin(), MechanismConfig::default(), account)
    }

    pub fn with(registry: Registry, config: MechanismConfig, account: &str) -> Self {
        Self {
            registry,
            config,
            domain: AppDomain::new(APP_ID, Some("test")).unwrap(),
            account: account.to_owned(),
            identity: None,
        }
    }

    /// A device account: its ID is the one the secret's identity
    /// founds, and its roots are signed by that identity.
    pub fn device_account(secret: [u8; 32]) -> Self {
        let identity =
            DeviceAccountIdentity::from_secret(&RecoverySecret::from_bytes(secret)).unwrap();
        let mut env = Self::new("pending");
        env.account = identity.account_id(&env.domain).unwrap();
        env.identity = Some(identity);
        env
    }

    pub fn authority(&self) -> RootAuthority<'_> {
        match &self.identity {
            Some(identity) => RootAuthority::DeviceAccount(identity),
            None => RootAuthority::App,
        }
    }

    pub fn writer(&self) -> AccountWriter<'_> {
        AccountWriter {
            account_secret_root_id: None,
            suite: self.registry.select(&self.config).unwrap(),
            registry: &self.registry,
            domain: &self.domain,
            account_id: &self.account,
        }
    }

    pub fn application(&self) -> AccountDomain {
        self.domain.account(&self.account).unwrap()
    }

    pub fn device(&self, rng: &mut TestRandom) -> LocalDevice {
        LocalDevice::generate(
            &self.registry.select(&self.config).unwrap(),
            &self.registry,
            &self.application(),
            rng,
        )
        .unwrap()
    }
}

/// Builds a signed sealing-key fixture for the supplied public key and mechanism.
pub fn sealing_key_record(
    env: &Env,
    signer: &LocalDevice,
    epoch_id: &str,
    mechanism: Mechanism<'_>,
    public_key: Vec<u8>,
    rng: &mut TestRandom,
) -> AccountSealingKey {
    let mut record = AccountSealingKey {
        id: ids::random_v4(rng).unwrap(),
        account_id: env.account.clone(),
        epoch_id: epoch_id.to_owned(),
        signer_id: signer.id.clone(),
        mechanism: mechanism.id.to_owned(),
        version: mechanism.version,
        public_key,
        signature: Vec::new(),
    };
    let bytes = record.signed_bytes(&env.application()).unwrap();
    let signer_impl = env
        .registry
        .signer(signer.signing.public.mechanism.as_mechanism())
        .unwrap();
    record.signature = signer_impl.sign(&signer.signing, &bytes).unwrap();
    record
}

/// One account's public and private rows, with the next position to accept
/// at.
#[derive(Clone)]
pub struct Ledger {
    pub public: PublicAccountLedger,
    pub private: PrivateAccountLedger,
    next: u64,
}

impl Ledger {
    pub fn new(account: &str) -> Self {
        Self {
            public: PublicAccountLedger::new(account, Position(0)),
            private: PrivateAccountLedger::new(Position(0)),
            next: 1,
        }
    }

    /// The position the next transaction will take.
    pub fn next_position(&self) -> u64 {
        self.next
    }

    /// Moves the next transaction's position forward to `next`, so that
    /// several accounts' ledgers share one sequence of positions.
    pub fn advance_to(&mut self, next: u64) {
        assert!(next >= self.next, "positions only move forward");
        self.next = next;
    }

    /// A new transaction's position; the cut moves to it.
    pub fn next(&mut self) -> Position {
        let position = Position(self.next);
        self.next += 1;
        self.public.through = position;
        self.private.through = position;
        position
    }

    /// Accepts the device's request and key projection.
    pub fn request(&mut self, device: &LocalDevice, rng: &mut TestRandom) -> Position {
        let at = self.next();
        self.private
            .requests
            .push(Accepted::new(at, device.request()));
        self.public
            .keys
            .push(Accepted::new(at, device.keys_row(rng).unwrap()));
        at
    }

    /// Bootstraps the account with `device` (generation 0), or resets it to
    /// the next generation, after the builders' own precondition checks.
    pub fn root(
        &mut self,
        env: &Env,
        device: &LocalDevice,
        generation: u64,
        rng: &mut TestRandom,
    ) -> (Position, AccountKeyBytes) {
        let writer = env.writer();
        let permit = if generation == 0 {
            build::check_bootstrap(&writer, device, &self.public, &self.private).unwrap()
        } else {
            let policy = ResetPolicy {
                device_account: false,
                allow_account_reset: true,
            };
            let state = self.state(env);
            build::check_reset(&writer, policy, &state, device, &self.public, &self.private)
                .unwrap()
        };
        assert_eq!(permit.generation(), generation);
        let records = build::account_root(&writer, device, &permit, env.authority(), rng).unwrap();
        let at = self.next();
        self.private
            .identities
            .push(Accepted::new(at, records.identity));
        self.public.roots.push(Accepted::new(at, records.root));
        self.public
            .account_keys
            .push(Accepted::new(at, records.account_key));
        (at, *records.key)
    }

    pub fn replay<'a>(&'a self, env: &'a Env) -> AccountReplay<'a> {
        AccountReplay::new(&self.public, &env.domain, &env.registry).unwrap()
    }

    pub fn state(&self, env: &Env) -> AccountState {
        self.replay(env).current().unwrap().expect("a root")
    }

    pub fn evidence(&self, env: &Env) -> (AccountState, PrivateEvidence) {
        let replay = self.replay(env);
        let state = replay.current().unwrap().expect("a root");
        let evidence = verify_private(&replay, &state, &self.private).unwrap();
        (state, evidence)
    }

    /// The account key `device` obtains at the cut.
    pub fn acquire(&self, env: &Env, device: &LocalDevice) -> ProtocolResult<AccountKey> {
        let replay = self.replay(env);
        let state = replay.current()?.expect("a root");
        let private = verify_private(&replay, &state, &self.private)?;
        AccountKeyEvidence::new(&replay, &state, &private)
            .unwrap()
            .acquire(&device.id, std::slice::from_ref(&device.sealing))
    }

    /// The challenge, proof and approval of `requester` by `approver`, each
    /// accepted in its own transaction. Returns the approval rows and the
    /// position they were accepted at.
    pub fn approve(
        &mut self,
        env: &Env,
        approver: &LocalDevice,
        requester: &mut LocalDevice,
        rng: &mut TestRandom,
    ) -> (Position, ApprovalRecords) {
        let key = self.acquire(env, approver).unwrap();
        let writer = env.writer();
        let state = self.state(env);
        let challenge =
            build::challenge(&writer, &state, approver, &requester.request(), rng).unwrap();
        let at = self.next();
        self.private
            .challenges
            .push(Accepted::new(at, challenge.challenge.clone()));
        let proof = build::respond(&writer, &state, requester, &challenge.challenge, rng).unwrap();
        let at = self.next();
        self.private.proofs.push(Accepted::new(at, proof.clone()));
        let records = build::approve(
            &writer,
            &state,
            approver,
            &challenge,
            &proof,
            &requester.request(),
            &key,
            rng,
        )
        .unwrap();
        let at = self.next();
        self.accept_approval(at, &records);
        (at, records)
    }

    pub fn accept_approval(&mut self, at: Position, records: &ApprovalRecords) {
        self.private
            .approvals
            .push(Accepted::new(at, records.approval.clone()));
        self.public
            .approvals
            .push(Accepted::new(at, records.public.clone()));
        self.private
            .deliveries
            .push(Accepted::new(at, records.delivery.clone()));
    }

    /// A rotation by `author`, with the key it acquires, accepted in one
    /// transaction.
    pub fn rotate(
        &mut self,
        env: &Env,
        author: &LocalDevice,
        target: RotationTarget,
        rng: &mut TestRandom,
    ) -> (Position, RotationRecords) {
        let key = self.acquire(env, author).unwrap();
        let state = self.state(env);
        let records = build::rotate(&env.writer(), &state, author, &key, &target, rng).unwrap();
        let at = self.next();
        self.accept_rotation(at, &records);
        (at, records)
    }

    pub fn accept_rotation(&mut self, at: Position, records: &RotationRecords) {
        self.private
            .successors
            .push(Accepted::new(at, records.private.clone()));
        self.public
            .successors
            .push(Accepted::new(at, records.public.clone()));
        for delivery in &records.recovery_deliveries {
            self.private
                .recovery_deliveries
                .push(Accepted::new(at, delivery.clone()));
        }
        self.public
            .account_keys
            .push(Accepted::new(at, records.account_key.clone()));
    }

    /// Registers `recovery` by `signer` and delivers `key` to it, in two
    /// transactions.
    pub fn register_recovery(
        &mut self,
        env: &Env,
        signer: &LocalDevice,
        recovery: &RecoveryKeys,
        rng: &mut TestRandom,
    ) -> RecoveryRoot {
        let key = self.acquire(env, signer).unwrap();
        let writer = env.writer();
        let state = self.state(env);
        let root = build::recovery_root(&writer, &state, signer, recovery).unwrap();
        let at = self.next();
        self.public
            .recovery_roots
            .push(Accepted::new(at, root.clone()));
        let delivery = build::recovery_delivery(&writer, &state, &root, &key, rng).unwrap();
        let at = self.next();
        self.private
            .recovery_deliveries
            .push(Accepted::new(at, delivery));
        root
    }
}

pub type AccountKeyBytes = [u8; 32];
