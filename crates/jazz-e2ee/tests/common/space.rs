//! Several accounts and one space, built with the crate's own record
//! builders and accepted row by row on one shared sequence of positions,
//! for space protocol tests.

use std::collections::BTreeMap;

use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::account::build::{LocalDevice, RotationTarget};
use jazz_e2ee::protocol::account::{
    AccountEpochKeys, AccountKey, AccountKeyEvidence, AccountState, PublicAccountLedger,
    verify_private,
};
use jazz_e2ee::protocol::records::space::{SpaceAddress, SpaceDelivery, SpaceMember};
use jazz_e2ee::protocol::space::build::{self, Author, SpaceWriter};
use jazz_e2ee::protocol::space::members::DELIVERY_BATCH;
use jazz_e2ee::protocol::space::{
    AccountDirectory, Acquisition, SpaceChain, SpaceKeys, SpaceLedger, acquire,
};
use jazz_e2ee::protocol::{Accepted, Position, ProtocolResult, ids};
use jazz_e2ee::registry::Registry;

use super::TestRandom;
use super::protocol::{Env, Ledger};

pub const ALICE: &str = "a11ce000-0000-5000-8000-00000000a11c";
pub const BOB: &str = "b0b00000-0000-5000-8000-00000000b0b0";
pub const CAROL: &str = "ca401000-0000-5000-8000-0000000ca401";
pub const DAVE: &str = "da7e0000-0000-5000-8000-00000000da7e";
pub const MALLORY: &str = "3a1103e0-0000-5000-8000-0000003a1103";

/// The space row's table identity and the space row's ID.
pub const SCOPE: &str = "5c0e1d2a-3b4c-7d5e-8f60-718293a4b5c6";
pub const ROW: &str = "018f2e3d-4c5b-7a69-8877-665544332211";
pub const OTHER_ROW: &str = "028f2e3d-4c5b-7a69-8877-665544332222";

pub fn address() -> SpaceAddress {
    SpaceAddress::new(SCOPE, ROW).unwrap()
}

/// One account with its ledger and devices.
pub struct Account {
    pub env: Env,
    pub ledger: Ledger,
    /// The device every operation of the account uses unless a test picks
    /// another.
    pub device: LocalDevice,
}

/// One account's view of itself, for writing.
pub struct Own {
    pub state: AccountState,
    pub key: AccountKey,
    pub epoch_keys: AccountEpochKeys,
}

/// A rotation as its rotator publishes it (§3.5): the successor with the
/// deliveries to the first batch of members, and the remaining members,
/// delivered to in later transactions once the successor is accepted.
pub struct Rotation {
    pub rotator: String,
    pub records: build::RotationRecords,
    /// The members delivered to after the successor is accepted.
    pub rest: Vec<String>,
}

/// A ledger read at one cut: every account's public rows and the space's.
pub struct Read {
    pub ledgers: Vec<PublicAccountLedger>,
    pub space: SpaceLedger,
}

impl Read {
    pub fn directory<'a>(&'a self, world: &'a World) -> AccountDirectory<'a> {
        AccountDirectory::new(
            &self.ledgers,
            &world.shared.domain,
            &world.shared.registry,
            self.space.through,
        )
        .unwrap()
    }

    pub fn chain(&self, world: &World) -> Option<SpaceChain> {
        SpaceChain::replay(&self.space, &self.directory(world)).unwrap()
    }
}

/// The registry and domain every reader uses.
pub struct Shared {
    pub registry: Registry,
    pub domain: jazz_e2ee::protocol::domain::AppDomain,
}

/// Several accounts and spaces on one sequence of positions.
pub struct World {
    pub shared: Shared,
    pub accounts: BTreeMap<String, Account>,
    /// The rows of every space, of every address.
    pub space: SpaceLedger,
    pub rng: TestRandom,
    next: u64,
}

impl World {
    pub fn new(seed: u64) -> Self {
        Self::with_registry(seed, Registry::builtin())
    }

    pub fn with_registry(seed: u64, registry: Registry) -> Self {
        let env = Env::new(ALICE);
        Self {
            shared: Shared {
                registry,
                domain: env.domain,
            },
            accounts: BTreeMap::new(),
            space: SpaceLedger::whole_space(address(), Position(0)),
            rng: TestRandom(seed),
            next: 1,
        }
    }

    /// A new transaction's position.
    pub fn next(&mut self) -> Position {
        let position = Position(self.next);
        self.next += 1;
        position
    }

    pub fn cut(&self) -> Position {
        Position(self.next - 1)
    }

    /// Bootstraps `account` with the built-in mechanisms.
    pub fn account(&mut self, account: &str) {
        self.account_with(account, MechanismConfig::default());
    }

    /// Bootstraps `account` whose devices write with `config`.
    pub fn account_with(&mut self, account: &str, config: MechanismConfig) {
        let env = Env::with(self.shared.registry.clone(), config, account);
        let device = env.device(&mut self.rng);
        let mut ledger = Ledger::new(account);
        ledger.advance_to(self.next);
        ledger.request(&device, &mut self.rng);
        ledger.root(&env, &device, 0, &mut self.rng);
        self.next = ledger.next_position();
        self.accounts.insert(
            account.to_owned(),
            Account {
                env,
                ledger,
                device,
            },
        );
    }

    /// Runs an account operation on the shared sequence of positions.
    pub fn with_account<T>(
        &mut self,
        account: &str,
        operation: impl FnOnce(&mut Account, &mut TestRandom) -> T,
    ) -> T {
        let entry = self.accounts.get_mut(account).expect("a known account");
        entry.ledger.advance_to(self.next);
        let out = operation(entry, &mut self.rng);
        self.next = entry.ledger.next_position();
        out
    }

    /// A read at the current cut.
    pub fn read(&self) -> Read {
        self.read_of(address())
    }

    /// A read of the space at `address` at the current cut.
    pub fn read_of(&self, address: SpaceAddress) -> Read {
        let cut = self.cut();
        let ledgers = self
            .accounts
            .values()
            .map(|account| {
                let mut ledger = account.ledger.public.clone();
                ledger.through = cut;
                ledger
            })
            .collect();
        let mut space = self.space.clone();
        space.address = address;
        space.through = cut;
        Read { ledgers, space }
    }

    /// The account's current state, key and epoch keys, for `device`.
    pub fn own_with(&self, account: &str, device: &LocalDevice) -> ProtocolResult<Own> {
        let entry = &self.accounts[account];
        let replay = entry.ledger.replay(&entry.env);
        let state = replay.current()?.expect("a root");
        let private = verify_private(&replay, &state, &entry.ledger.private)?;
        let evidence = AccountKeyEvidence::new(&replay, &state, &private)?;
        let key = evidence.acquire(&device.id, std::slice::from_ref(&device.sealing))?;
        let epoch_keys = evidence.epoch_keys(&key)?;
        Ok(Own {
            state,
            key,
            epoch_keys,
        })
    }

    pub fn own(&self, account: &str) -> Own {
        self.own_with(account, &self.accounts[account].device)
            .unwrap()
    }

    pub fn writer(&self, account: &str) -> SpaceWriter<'_> {
        let entry = &self.accounts[account];
        SpaceWriter {
            suite: self.shared.registry.select(&entry.env.config).unwrap(),
            registry: &self.shared.registry,
            domain: &self.shared.domain,
            account_id: &entry.env.account,
        }
    }

    /// Adds a member row for `account` to the space at `identifier`, in its
    /// own transaction.
    pub fn add_member_of(&mut self, identifier: &str, account: &str) -> Position {
        let at = self.next();
        let id = ids::random_v4(&mut self.rng).unwrap();
        self.space.members.push(Accepted::new(
            at,
            SpaceMember {
                id,
                scope_id: SCOPE.into(),
                identifier: identifier.into(),
                account_id: account.into(),
            },
        ));
        at
    }

    pub fn add_member(&mut self, account: &str) -> Position {
        self.add_member_of(ROW, account)
    }

    /// Deletes every member row of `account` for the space (§1.3).
    pub fn remove_member(&mut self, account: &str) {
        self.space
            .members
            .retain(|member| !(member.row.identifier == ROW && member.row.account_id == account));
    }

    /// A root of the space at `identifier` by `device` of `account`, not
    /// yet accepted.
    pub fn build_root(
        &mut self,
        identifier: &str,
        account: &str,
        device: &LocalDevice,
    ) -> ProtocolResult<build::RootRecords> {
        let own = self.own_with(account, device)?;
        let read = self.read();
        let author = Author::new(&read.directory(self), account, &own.key, device)?;
        let mut rng = TestRandom(self.rng.next_u64());
        build::space_root(
            &self.writer(account),
            &SpaceAddress::new(SCOPE, identifier).unwrap(),
            &author,
            &mut rng,
        )
    }

    /// `account` creates the space at `identifier` with its default device;
    /// the root is accepted in its own transaction.
    pub fn create_of(&mut self, identifier: &str, account: &str) -> Position {
        let device = self.accounts[account].device.clone();
        let records = self.build_root(identifier, account, &device).unwrap();
        let at = self.next();
        self.space.roots.push(Accepted::new(at, records.root));
        at
    }

    /// A second device of `account`, requested and approved by its default
    /// device.
    pub fn approve_device(&mut self, account: &str) -> LocalDevice {
        self.with_account(account, |entry, rng| {
            let mut device = entry.env.device(rng);
            entry.ledger.request(&device, rng);
            let approver = entry.device.clone();
            entry
                .ledger
                .approve(&entry.env, &approver, &mut device, rng);
            device
        })
    }

    /// `account`'s default device removes `device_id` from the account.
    pub fn remove_device(&mut self, account: &str, device_id: &str) -> String {
        self.with_account(account, |entry, rng| {
            let author = entry.device.clone();
            let (_, rotation) = entry.ledger.rotate(
                &entry.env,
                &author,
                RotationTarget::RemoveDevice(device_id.to_owned()),
                rng,
            );
            rotation.public.columns.id.clone()
        })
    }

    /// Resets `account` with a new device, which becomes its default
    /// device (§2.11).
    pub fn reset(&mut self, account: &str) {
        self.with_account(account, |entry, rng| {
            let device = entry.env.device(rng);
            entry.ledger.request(&device, rng);
            let generation = entry.ledger.state(&entry.env).generation + 1;
            entry.ledger.root(&entry.env, &device, generation, rng);
            entry.device = device;
        });
    }

    pub fn create(&mut self, account: &str) -> Position {
        self.create_of(ROW, account)
    }

    /// What `account`'s default device acquires at the current cut.
    pub fn acquire(&self, account: &str) -> Acquisition {
        self.try_acquire(account).unwrap()
    }

    pub fn try_acquire(&self, account: &str) -> ProtocolResult<Acquisition> {
        let read = self.read();
        let chain = read.chain(self).expect("a space root");
        let own = self.own(account);
        acquire(&chain, &read.space, &read.directory(self), &own.epoch_keys)
    }

    /// `from` delivers version `epoch_id` of the space to `to`, in its own
    /// transaction.
    pub fn deliver(&mut self, from: &str, to: &str, epoch_id: &str) -> SpaceDelivery {
        let delivery = self.build_delivery(from, to, epoch_id).unwrap();
        let at = self.next();
        self.space
            .deliveries
            .push(Accepted::new(at, delivery.clone()));
        delivery
    }

    pub fn build_delivery(
        &mut self,
        from: &str,
        to: &str,
        epoch_id: &str,
    ) -> ProtocolResult<SpaceDelivery> {
        let mut rng = TestRandom(self.rng.next_u64());
        let acquisition = self.acquire(from);
        let read = self.read();
        let chain = read.chain(self).expect("a space root");
        let directory = read.directory(self);
        let writer = self.writer(from);
        let recipient = directory
            .recipient(to, writer.suite.key_seal.mechanism())?
            .ok_or(jazz_e2ee::protocol::ProtocolError::Refused(
                jazz_e2ee::protocol::error::RefusalCode::UnsupportedRecipient,
            ))?;
        let version = acquisition
            .keys
            .version(epoch_id)
            .expect("the sender holds the version");
        build::deliver(&writer, &chain, &version, &recipient, &mut rng)
    }

    /// `account` rotates the space with its default device (§3.5): the
    /// successor and the deliveries to the first batch of members in one
    /// transaction, then the deliveries to the remaining members in later
    /// transactions. Returns the new version's epoch.
    pub fn rotate(&mut self, account: &str) -> String {
        let rotation = self.build_rotation(account, false).unwrap();
        self.accept_rotation(rotation)
    }

    /// A rotation by `account`'s default device at the current cut, not
    /// yet accepted: an ordinary one, or with `recovery` a recovery
    /// rotation.
    pub fn build_rotation(&mut self, account: &str, recovery: bool) -> ProtocolResult<Rotation> {
        let mut rng = TestRandom(self.rng.next_u64());
        let read = self.read();
        let directory = read.directory(self);
        let chain = read.chain(self).expect("a space root");
        let keys = self.try_acquire(account)?.keys;
        self.build_rotation_with(
            account, &read, &chain, &keys, &directory, recovery, &mut rng,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_rotation_with(
        &self,
        account: &str,
        read: &Read,
        chain: &SpaceChain,
        keys: &SpaceKeys,
        directory: &AccountDirectory<'_>,
        recovery: bool,
        rng: &mut TestRandom,
    ) -> ProtocolResult<Rotation> {
        let own = self.own(account);
        let writer = self.writer(account);
        let recipients = jazz_e2ee::protocol::space::members::rotation_recipients(
            chain,
            &read.space,
            directory,
            account,
            writer.suite.key_seal.mechanism(),
            rng,
        )?;
        let author = Author::new(directory, account, &own.key, &self.accounts[account].device)?;
        let records = if recovery {
            build::recover_rotation(&writer, chain, keys, &author, &recipients.first, rng)
        } else {
            build::rotate(&writer, chain, keys, &author, &recipients.first, rng)
        }?;
        Ok(Rotation {
            rotator: account.to_owned(),
            records,
            rest: recipients.rest,
        })
    }

    /// Accepts the rotation's successor and its deliveries in one
    /// transaction, then, if the successor became the next version, the
    /// rotator's deliveries of it to the remaining members in transactions
    /// of up to [`DELIVERY_BATCH`] each. Returns the new version's epoch.
    pub fn accept_rotation(&mut self, rotation: Rotation) -> String {
        let Rotation {
            rotator,
            records,
            rest,
        } = rotation;
        let at = self.next();
        let epoch = records.successor.epoch_id.clone();
        self.space
            .successors
            .push(Accepted::new(at, records.successor));
        for delivery in records.deliveries {
            self.space.deliveries.push(Accepted::new(at, delivery));
        }
        for batch in rest.chunks(DELIVERY_BATCH) {
            let Some(deliveries) = self.build_deliveries(&rotator, &epoch, batch).unwrap() else {
                break;
            };
            let at = self.next();
            for delivery in deliveries {
                self.space.deliveries.push(Accepted::new(at, delivery));
            }
        }
        epoch
    }

    /// `from`'s deliveries of version `epoch_id` to each of `accounts`,
    /// whether or not a delivery to it already exists, built from a read
    /// at the current cut and what `from` acquires there. `None` when
    /// `from` does not hold the version (a successor that did not become
    /// the next version).
    pub fn build_deliveries(
        &mut self,
        from: &str,
        epoch_id: &str,
        accounts: &[String],
    ) -> ProtocolResult<Option<Vec<SpaceDelivery>>> {
        let mut rng = TestRandom(self.rng.next_u64());
        let acquisition = self.try_acquire(from)?;
        let Some(version) = acquisition.keys.version(epoch_id) else {
            return Ok(None);
        };
        let read = self.read();
        let chain = read.chain(self).expect("a space root");
        let directory = read.directory(self);
        let writer = self.writer(from);
        accounts
            .iter()
            .map(|account| {
                let recipient = directory
                    .recipient(account, writer.suite.key_seal.mechanism())?
                    .expect("a recipient with a sealing key");
                build::deliver(&writer, &chain, &version, &recipient, &mut rng)
            })
            .collect::<ProtocolResult<_>>()
            .map(Some)
    }

    /// The current version's epoch at the cut.
    pub fn current(&self) -> String {
        let chain = self.read().chain(self).expect("a space root");
        chain.current().unwrap().epoch_id.clone()
    }

    /// The account's current account epoch.
    pub fn account_epoch(&self, account: &str) -> String {
        let entry = &self.accounts[account];
        entry.ledger.state(&entry.env).epoch_id().to_owned()
    }
}
