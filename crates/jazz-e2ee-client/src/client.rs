//! The runtime of one account on one device (`E2EE_PROTOCOL.md` §6): it
//! opens the private store, keeps the device, held keys and floors in step
//! with it, and gives the database its hooks.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use futures::lock::Mutex;
use jazz::db::Db;
use jazz::e2ee::{E2eeDeclarations, E2eeHooks};
use jazz_e2ee::RandomSource;
use jazz_e2ee::agility::SecretKey;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::account::AccountFloor;
use jazz_e2ee::protocol::account::build::LocalDevice;
use jazz_e2ee::protocol::content::HeldSpace;
use jazz_e2ee::protocol::content::local::{
    DeviceSecrets, FLOOR_FORMAT, FloorRecord, FloorVerifier, StoredApproval, StoredDevice,
    decode_device_document,
};
use jazz_e2ee::protocol::content::store::{
    HeldKeyRecord, Rejected, RejectedRecord, SealedKeys, StoreOwner,
};
use jazz_e2ee::protocol::derive;
use jazz_e2ee::protocol::device_account::DeviceAccountIdentity;
use jazz_e2ee::protocol::domain::AppDomain;
#[cfg(any(test, feature = "testing"))]
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::registry::Registry;

use crate::account::OwnAccount;
use crate::error::ClientError;
use crate::hooks::{Hooks, plans};
use crate::mechanisms;
use crate::state::{Catalogue, KeyState, Report, Shared};
use crate::store::{PrivateStore, ScopeDocuments, StoreError, Stored};

const DEVICE_DOCUMENT: &str = "device";
const HELD_PREFIX: &str = "held-";
const FLOOR_PREFIX: &str = "floor-";
/// Attempts of a save that keeps meeting another writer's save.
const SAVE_ATTEMPTS: usize = 3;

struct DeviceLoad<'a> {
    store: &'a dyn PrivateStore,
    incarnation: u64,
    registry: &'a Registry,
    selected: &'a jazz_e2ee::agility::MechanismConfig,
    domain: &'a AppDomain,
    account_id: &'a str,
    scope: &'a str,
}

/// What a runtime opens with.
pub struct ClientConfig<'a> {
    pub store: Rc<dyn PrivateStore>,
    /// The application's encryption declarations, as the database opens
    /// with them.
    pub declarations: E2eeDeclarations,
    /// The signed-in account.
    pub account_id: String,
    /// Every mechanism this build includes (`Registry::builtin()`, or one
    /// with custom mechanisms); the application's allow-list restricts it.
    pub registry: Registry,
    /// The operating system's random source outside tests.
    pub random: Box<dyn RandomSource>,
    /// The device-account secret, when available. Opening derives the signing
    /// identity and the ID of the recovery root that must stay active (§2.9).
    pub device_account: Option<&'a derive::RecoverySecret>,
}

/// One held-key document as last read or written.
#[derive(Default)]
struct HeldDocumentState {
    /// The store's revision of the document, `None` before it exists.
    revision: Option<u64>,
    /// Bytes confirmed by a save whose held state still matches memory.
    confirmed_bytes: Option<Vec<u8>>,
    /// The document's own removal counter (§6.4).
    removals: u64,
    /// Records skipped as unsupported, carried over unchanged (§6.2).
    carried: Vec<HeldKeyRecord>,
    /// The checked lineage when no key of the document loaded.
    lineage: Option<HeldSpace>,
    /// The key records last read or written, reused by the next rewrite
    /// for keys that did not change.
    sealed: SealedKeys,
}

#[derive(Default)]
struct Documents {
    held: BTreeMap<String, HeldDocumentState>,
    floors: BTreeMap<String, (Option<AccountFloor>, u64)>,
    /// Documents of a format this runtime does not read: never written.
    unrecognised: BTreeSet<String>,
}

/// The encryption runtime of one account on this device.
pub struct E2eeClient {
    pub(crate) shared: Rc<Shared>,
    hooks: Rc<Hooks>,
    store: Rc<dyn PrivateStore>,
    scope: String,
    /// The scope's incarnation this runtime writes into: a save after the
    /// scope was wiped elsewhere fails, and closes the runtime. Re-enrolment
    /// moves it to a new incarnation.
    incarnation: Cell<u64>,
    pub(crate) device_account: RefCell<Option<DeviceAccountIdentity>>,
    pub(crate) account_secret_root_id: Option<String>,
    /// The account as this device last verified it.
    pub(crate) account: RefCell<Option<OwnAccount>>,
    /// Whether this run reconciled the creations it found stored.
    pub(crate) reconciled: Cell<bool>,
    /// Every store write and the wipe run under this lock, so a wipe waits
    /// for writes in flight and no write follows it (§6.4).
    documents: Mutex<Documents>,
}

/// The runtime's random source, borrowed only for each fill, so that it
/// is never held across an await.
pub(crate) struct SharedRandom<'a>(pub(crate) &'a RefCell<Box<dyn RandomSource>>);

impl RandomSource for SharedRandom<'_> {
    fn fill(&mut self, dest: &mut [u8]) -> jazz_e2ee::Result<()> {
        self.0.borrow_mut().fill(dest)
    }
}

/// The `format` a stored record names, if it is a JSON object with one.
fn floor_format(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    Some(value.get("format")?.as_str()?.to_owned())
}

/// Saves retry when another writer changed the document first.
fn retry_save<T>(result: Result<T, StoreError>) -> Result<Option<T>, StoreError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(StoreError::Conflict) => Ok(None),
        Err(error) => Err(error),
    }
}

fn rejection(record: &Rejected) -> String {
    match &record.record {
        RejectedRecord::Lineage => format!("lineage: {:?}", record.reason),
        RejectedRecord::Key { index, .. } => format!("key {index}: {:?}", record.reason),
    }
}

/// The local check of a stored device (§2.1): its mechanisms are allowed,
/// and its key pairs seal and open, sign and verify.
fn check_device(
    registry: &Registry,
    device: &LocalDevice,
    scope: &str,
    random: &mut dyn RandomSource,
) -> Result<(), ClientError> {
    let unsupported = |_| {
        ClientError::Unsupported(
            "the stored device's mechanisms are not allowed by this application".to_owned(),
        )
    };
    let bytes = derive::local_device_check(scope);
    let sealer = registry
        .key_sealer(device.sealing.public.mechanism.as_mechanism())
        .map_err(unsupported)?;
    let signer = registry
        .signer(device.signing.public.mechanism.as_mechanism())
        .map_err(unsupported)?;
    let mut probe = [0u8; 32];
    random.fill(&mut probe)?;
    let sealed = sealer.seal(&device.sealing.public, &bytes, &probe, random)?;
    let opened = match registry.open_sealed(std::slice::from_ref(&device.sealing), &bytes, &sealed)
    {
        Ok(key) => *key == probe,
        Err(jazz_e2ee::Error::InvalidCiphertext) => false,
        Err(error) => return Err(error.into()),
    };
    let signature = signer.sign(&device.signing, &bytes)?;
    let verified = registry.verify(&device.signing.public, &bytes, &signature)?;
    if !opened || !verified {
        return Err(ClientError::Protocol(ProtocolError::InvalidRecord(
            "the stored device does not pass its local check",
        )));
    }
    Ok(())
}

impl E2eeClient {
    /// Opens the runtime: checks the declarations and settings, loads the
    /// device (generating and storing one on first use, §2.1), the held
    /// keys and the floors of the account's scope. A record that fails its
    /// checks is skipped and reported ([`E2eeClient::on_report`]); a
    /// document of a format this runtime does not read fails the open if
    /// it is the device document, and is otherwise reported and never
    /// overwritten. A store or provider failure fails the open.
    pub async fn open(config: ClientConfig<'_>) -> Result<Self, ClientError> {
        let ClientConfig {
            store,
            declarations,
            account_id,
            registry: host,
            random,
            device_account,
        } = config;
        let plans = plans(&declarations)?;
        let (registry, selected) =
            mechanisms::configure(&host, declarations.settings.mechanisms.as_ref())?;
        let environment =
            (!declarations.environment.is_empty()).then_some(declarations.environment.as_str());
        let domain = AppDomain::new(
            &declarations.app_id.as_object_id().uuid().to_string(),
            environment,
        )?;
        let application = domain.account(&account_id)?;
        // Device accounts cannot reset, so their recovery root is in generation 0.
        let (identity, account_secret_root_id) = match device_account {
            Some(secret) => {
                let identity = DeviceAccountIdentity::from_secret(secret)?;
                let root_id = if identity.account_id(&domain).as_deref() == Some(&account_id) {
                    Some(derive::RecoveryKeys::derive(secret, &application, 0)?.root_id)
                } else {
                    None
                };
                (Some(identity), root_id)
            }
            None => (None, None),
        };
        let scope = application.as_str().to_owned();
        // A wipe that lands while the scope opens deletes it under this
        // runtime: open the new scope and load again.
        let mut opened = None;
        let random = RefCell::new(random);
        for _ in 0..SAVE_ATTEMPTS {
            let incarnation = store.open_scope(&scope).await?;
            let mut bulk = match store.load_all(&scope, incarnation) {
                Some(read) => match read.await {
                    Ok(documents) => Some(documents),
                    Err(StoreError::Wiped) => continue,
                    Err(error) => return Err(error.into()),
                },
                None => None,
            };
            let initial_device = bulk.as_mut().map(|documents| {
                documents
                    .iter()
                    .position(|(name, _)| name == DEVICE_DOCUMENT)
                    .map(|index| documents.remove(index).1)
            });
            match Self::load_device(
                DeviceLoad {
                    store: &*store,
                    incarnation,
                    registry: &registry,
                    selected: &selected,
                    domain: &domain,
                    account_id: &account_id,
                    scope: &scope,
                },
                initial_device,
                &mut SharedRandom(&random),
            )
            .await
            {
                Ok(device) => {
                    opened = Some((incarnation, device, bulk));
                    break;
                }
                Err(ClientError::Closed) => continue,
                Err(error) => return Err(error),
            }
        }
        let (incarnation, device, bulk) = opened.ok_or(ClientError::Store(StoreError::Wiped))?;
        let shared = Rc::new(Shared::new(
            registry,
            selected,
            domain,
            account_id,
            plans,
            device,
            random.into_inner(),
        ));
        let client = Self {
            hooks: Rc::new(Hooks {
                declarations,
                shared: shared.clone(),
            }),
            shared,
            store,
            scope,
            incarnation: Cell::new(incarnation),
            device_account: identity.into(),
            account_secret_root_id,
            account: RefCell::new(None),
            reconciled: Cell::new(false),
            documents: Mutex::new(Documents::default()),
        };
        client.load_documents(bulk).await?;
        Ok(client)
    }

    async fn load_device(
        load: DeviceLoad<'_>,
        mut initial: Option<Option<Stored>>,
        random: &mut dyn RandomSource,
    ) -> Result<LocalDevice, ClientError> {
        let DeviceLoad {
            store,
            incarnation,
            registry,
            selected,
            domain,
            account_id,
            scope,
        } = load;
        let unrecognised = || ClientError::UnrecognisedDocument(DEVICE_DOCUMENT.to_owned());
        for _ in 0..SAVE_ATTEMPTS {
            let stored = match initial.take() {
                Some(stored) => stored,
                None => store.load(scope, DEVICE_DOCUMENT).await?,
            };
            let expected = match &stored {
                None => None,
                Some(stored) => {
                    match decode_device_document(&stored.bytes).map_err(|_| unrecognised())? {
                        Some(device) if device.scope == scope => {
                            let secrets = DeviceSecrets {
                                sealing: SecretKey(
                                    store.unprotect(&device.protected_sealing_key).await?,
                                ),
                                signing: SecretKey(
                                    store.unprotect(&device.protected_signing_key).await?,
                                ),
                                approval_nonce: match &device.approval {
                                    StoredApproval::Unspent { protected_nonce } => {
                                        Some(store.unprotect(protected_nonce).await?)
                                    }
                                    StoredApproval::Spent => None,
                                },
                            };
                            let device = device.into_local(secrets)?;
                            check_device(registry, &device, scope, random)?;
                            return Ok(device);
                        }
                        Some(_) => return Err(unrecognised()),
                        None => Some(stored.revision),
                    }
                }
            };
            let suite = registry.select(selected)?;
            let device =
                LocalDevice::generate(&suite, registry, &domain.account(account_id)?, random)?;
            let nonce = device
                .approval_nonce
                .as_ref()
                .expect("a new device has its approval nonce");
            let document = StoredDevice {
                scope: scope.to_owned(),
                id: device.id.clone(),
                sealing: device.sealing.public.clone(),
                protected_sealing_key: store.protect(device.sealing.secret.as_bytes()).await?,
                signing: device.signing.public.clone(),
                protected_signing_key: store.protect(device.signing.secret.as_bytes()).await?,
                approval_commitment: device.approval_commitment,
                approval: StoredApproval::Unspent {
                    protected_nonce: store.protect(&nonce.0[..]).await?,
                },
            }
            .encode_document()?;
            match retry_save(
                store
                    .save(scope, incarnation, DEVICE_DOCUMENT, &document, expected)
                    .await,
            ) {
                Ok(Some(_)) => return Ok(device),
                Ok(None) => continue,
                // Another writer stored a device first: the store keeps one
                // device per scope, so load that one.
                Err(StoreError::Wiped) => return Err(ClientError::Closed),
                Err(error) => return Err(error.into()),
            }
        }
        Err(StoreError::Conflict.into())
    }

    fn owner_of<'a>(shared: &'a Shared, device: &'a LocalDevice) -> StoreOwner<'a> {
        StoreOwner {
            registry: &shared.registry,
            domain: &shared.domain,
            account_id: &shared.account_id,
            device,
        }
    }

    async fn load_documents(&self, bulk: Option<ScopeDocuments>) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
        let mut floor_verifier = None;
        if let Some(bulk) = bulk {
            for (name, stored) in bulk {
                self.accept_document(&mut documents, &name, stored, &mut floor_verifier)
                    .await?;
            }
            return Ok(());
        }
        for name in self.store.list(&self.scope).await? {
            if (name.starts_with(HELD_PREFIX) || name.starts_with(FLOOR_PREFIX))
                && let Some(stored) = self.store.load(&self.scope, &name).await?
            {
                self.accept_document(&mut documents, &name, stored, &mut floor_verifier)
                    .await?;
            }
        }
        Ok(())
    }

    async fn accept_document<'a>(
        &'a self,
        documents: &mut Documents,
        name: &str,
        stored: Stored,
        floor_verifier: &mut Option<FloorVerifier<'a>>,
    ) -> Result<(), ClientError> {
        if let Some(root_id) = name.strip_prefix(HELD_PREFIX) {
            if let Some(space) = self
                .read_held(documents, name, root_id, stored, true)
                .await?
            {
                self.shared.keys.borrow_mut().held.hold(space)?;
            }
        } else if let Some(account_id) = name.strip_prefix(FLOOR_PREFIX) {
            self.load_floor(documents, name, account_id, stored, floor_verifier)?;
        }
        Ok(())
    }

    fn skipped(&self, document: &str, reason: impl Into<String>) {
        self.shared.report(Report::SkippedRecord {
            document: document.to_owned(),
            reason: reason.into(),
        });
    }

    /// Reads one held-key document: records its state and returns the keys
    /// that passed their checks, for the caller to hold.
    async fn read_held(
        &self,
        documents: &mut Documents,
        name: &str,
        root_id: &str,
        stored: Stored,
        advance_verification: bool,
    ) -> Result<Option<HeldSpace>, ClientError> {
        let loaded = {
            let device = self.shared.device()?;
            Self::owner_of(&self.shared, &device).load_stored_document(&stored.bytes)?
        };
        let Some(loaded) = loaded else {
            documents.unrecognised.insert(name.to_owned());
            self.skipped(
                name,
                "a held-key document of a format this runtime does not read",
            );
            return Ok(None);
        };
        for rejected in &loaded.rejected {
            self.skipped(name, rejection(rejected));
        }
        let mut state = HeldDocumentState {
            revision: Some(stored.revision),
            confirmed_bytes: None,
            removals: loaded.revision,
            carried: loaded.unsupported,
            lineage: None,
            sealed: loaded.sealed,
        };
        if advance_verification
            && let Some(bytes) = loaded.updated_document
            && loaded
                .space
                .as_ref()
                .is_some_and(|space| space.root_id() == root_id)
        {
            match self
                .store
                .save(
                    &self.scope,
                    self.incarnation.get(),
                    name,
                    &bytes,
                    Some(stored.revision),
                )
                .await
            {
                Ok(revision) => state.revision = Some(revision),
                Err(StoreError::Wiped) => return Err(self.close()),
                Err(_) => {}
            }
        }
        let space = match loaded.space {
            Some(space) if space.is_empty() => {
                state.lineage = (space.root_id() == root_id).then_some(space);
                None
            }
            Some(space) if space.root_id() != root_id => {
                self.skipped(name, "the document holds another root's keys");
                state.carried.clear();
                None
            }
            space => space,
        };
        documents.held.insert(root_id.to_owned(), state);
        Ok(space)
    }

    /// Loads one floor record.
    fn load_floor<'a>(
        &'a self,
        documents: &mut Documents,
        name: &str,
        account_id: &str,
        stored: Stored,
        verifier: &mut Option<FloorVerifier<'a>>,
    ) -> Result<(), ClientError> {
        let floor = match FloorRecord::decode(&stored.bytes) {
            Ok(record) => {
                let device = self.shared.device()?;
                let verifier = match verifier {
                    Some(verifier) => verifier,
                    none => none.insert(FloorVerifier::new(
                        &self.shared.registry,
                        &self.shared.domain,
                        &self.shared.account_id,
                        &device,
                    )?),
                };
                match verifier.load_floor(account_id, &record)? {
                    Ok(floor) => Some(floor),
                    Err(reason) => {
                        self.skipped(name, format!("{reason:?}"));
                        None
                    }
                }
            }
            // A record of this format that does not decode is damaged and
            // may be replaced; anything else is of a format this runtime
            // does not read, refused and never overwritten (§6.1).
            //
            // Replacing it does not fail open. A floor guards against an
            // older account state, not against whoever can write this
            // store: that writer could as well delete the record, and a
            // damaged one carries no state this device signed, so it is
            // treated as absent (§6.3a: skipped and reported, protection
            // restarting at the next verified state, §8). Refusing to
            // advance instead would block the account's floor for good,
            // until a wipe.
            Err(_) if floor_format(&stored.bytes).as_deref() == Some(FLOOR_FORMAT) => {
                self.skipped(name, "a floor record that does not decode");
                None
            }
            Err(_) => {
                documents.unrecognised.insert(name.to_owned());
                self.skipped(
                    name,
                    "a floor record of a format this runtime does not read",
                );
                None
            }
        };
        documents
            .floors
            .insert(account_id.to_owned(), (floor, stored.revision));
        Ok(())
    }

    /// Whether the application allows account resets (its encryption
    /// settings).
    pub(crate) fn allow_account_reset(&self) -> bool {
        self.hooks.declarations.settings.allow_account_reset
    }

    /// The hooks to open the account's database with
    /// (`DbConfig::with_e2ee`).
    pub fn hooks(&self) -> Rc<dyn E2eeHooks> {
        self.hooks.clone()
    }

    /// Learns the catalogue identities the cell contexts name from `db`
    /// (protocol §4.1). `false` while the database has not observed them:
    /// until then encrypted tables can be neither read nor written, so a
    /// client that has never synced cannot encrypt or decrypt.
    pub async fn prepare(&self, db: &Db) -> Result<bool, ClientError> {
        // A refused creation is found among the refusals the store keeps.
        db.track_upstream_rejections();
        let catalogue = Catalogue::resolve(db, &self.shared.plans).await?;
        let known = catalogue.is_some();
        if known {
            self.shared.keys.borrow_mut().catalogue = catalogue;
        }
        Ok(known)
    }

    /// Whether the client is online: offline, equality queries compute no
    /// tokens and match every row of the space instead (protocol §4.5).
    pub fn set_online(&self, online: bool) {
        let mut keys = self.shared.keys.borrow_mut();
        if online && !keys.online {
            self.shared
                .member_watch
                .borrow_mut()
                .invalidate_rotation_checks(&mut keys);
        }
        keys.online = online;
    }

    /// How many staged member rows are still waited for.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn pending_member_changes(&self) -> usize {
        self.shared.member_changes.borrow().len()
    }

    /// How many member removals this client wrote whose space's pass has not
    /// run yet. Only for tests of that queue.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn pending_member_removals(&self) -> usize {
        self.shared.member_removals.borrow().len()
    }

    /// Content-only test fixtures create accepted roots in memory without
    /// an authority ledger. Record the check made by their fixture setup.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn record_fixture_rotation_check(&self, address: &SpaceAddress) {
        self.shared.note_write(address);
        self.shared
            .keys
            .borrow_mut()
            .rotation_evaluated
            .insert(address.clone());
    }

    /// The table and account pairs covered by live account streams for this
    /// space. Exposed to tests because an unnecessary subscription has no
    /// observable effect on decrypted rows.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn watched_member_accounts(
        &self,
        space: &jazz::e2ee::SpaceAddress,
    ) -> Vec<(&'static str, String)> {
        let space = crate::state::unchecked_address(space);
        let watch = self.shared.member_watch.borrow();
        watch
            .by_account
            .iter()
            .filter(|((table, account), spaces)| {
                spaces.contains(&space)
                    && watch
                        .streams
                        .get(table)
                        .is_some_and(|sub| sub.accounts.contains(account))
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn write_check_reads(&self) -> (usize, usize) {
        let watch = self.shared.member_watch.borrow();
        (watch.last_check_rows, watch.chain_reads)
    }

    /// Delivery rows returned, queries lacking an epoch filter, and total delivery queries since the last call.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn take_delivery_reads(&self) -> (usize, usize, usize) {
        std::mem::take(&mut self.shared.member_watch.borrow_mut().delivery_reads)
    }

    /// Runs the rest of this device's last rotation of `space` now, as the
    /// runtime's background pass would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub async fn finish_rotation_for_test(
        &self,
        db: &jazz::db::Db,
        space: &jazz::e2ee::SpaceAddress,
    ) -> Result<usize, ClientError> {
        self.finish_rotation(db, &crate::spaces::protocol_address(space)?)
            .await
    }

    /// Leaves the rest of a rotation of `space` pending, with a retry time
    /// already past: what a failed batch leaves when its wait ends while
    /// the client is offline, where nothing serves it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn stale_rotation_retry_for_test(
        &self,
        space: &jazz::e2ee::SpaceAddress,
    ) -> Result<(), ClientError> {
        let now = self.shared.now();
        self.shared
            .member_watch
            .borrow_mut()
            .pending_deliveries
            .insert(
                crate::spaces::protocol_address(space)?,
                crate::state::PendingRotation {
                    accounts: vec![uuid::Uuid::new_v4().to_string()],
                    failures: 1,
                    retry_at: Some(
                        now.checked_sub(std::time::Duration::from_secs(1))
                            .unwrap_or(now),
                    ),
                },
            );
        Ok(())
    }

    /// Makes the next `count` full acquisitions of a space fail, as a
    /// failed read of the space would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn fail_acquisitions_for_test(&self, count: u32) {
        self.shared.member_watch.borrow_mut().fail_acquisitions = count;
    }

    /// The rest of this device's last rotation of `space`, if any is
    /// pending: its failed attempts in a row, and how long until its next
    /// attempt may run (`None`: no wait).
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn pending_rotation_for_test(
        &self,
        space: &jazz::e2ee::SpaceAddress,
    ) -> Result<Option<(u32, Option<std::time::Duration>)>, ClientError> {
        let now = self.shared.now();
        let space = crate::spaces::protocol_address(space)?;
        Ok(self
            .shared
            .member_watch
            .borrow()
            .pending_deliveries
            .get(&space)
            .map(|pending| {
                (
                    pending.failures,
                    pending.retry_at.map(|at| at.saturating_duration_since(now)),
                )
            }))
    }

    /// When the runtime is next woken to retry the rest of a rotation:
    /// how long from now (`None`: no retry is scheduled).
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn next_rotation_retry_for_test(&self) -> Option<std::time::Duration> {
        let now = self.shared.now();
        self.next_rotation_retry()
            .map(|at| at.saturating_duration_since(now))
    }

    /// Makes the next [`E2eeClient::sync_spaces`] fail before it serves
    /// anything, as a failed read of the local store would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn fail_next_sync_for_test(&self) {
        self.shared.member_watch.borrow_mut().fail_next_sync = true;
    }

    /// Public account histories requested since the last call.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn take_public_account_reads(&self) -> usize {
        std::mem::take(&mut self.shared.member_watch.borrow_mut().public_account_reads)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn delivery_mark_checks(&self) -> usize {
        self.shared.member_watch.borrow().delivery_mark_checks
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn delivery_chain_replays(&self) -> usize {
        self.shared.member_watch.borrow().delivery_chain_replays
    }

    /// Holds the runtime clock still, or moves it on by `by` when
    /// already held (tests: a wait is stepped, never slept).
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn advance_settle_clock(&self, by: std::time::Duration) {
        let now = self.shared.now();
        self.shared.held_clock.set(Some(now + by));
    }

    /// This device: its ID and key pairs. [`ClientError::Closed`] once the
    /// store was wiped, here or by another writer: the device's secrets
    /// are dropped then.
    pub fn device(&self) -> Result<std::cell::Ref<'_, LocalDevice>, ClientError> {
        self.shared.device()
    }

    /// The account this runtime is for.
    pub fn account_id(&self) -> &str {
        &self.shared.account_id
    }

    /// Marks this device's approval nonce spent in the private store, and
    /// returns once the store confirmed the save (protocol §2.5, §6.1): the
    /// proof that reveals the nonce is only published after this, so a
    /// device restored from an older copy of its store, whose document
    /// says spent, answers no challenge. Fails if the document already says
    /// spent.
    pub(crate) async fn mark_nonce_spent(&self) -> Result<(), ClientError> {
        let documents = self.documents.lock().await;
        self.ensure_open()?;
        let unrecognised = || ClientError::UnrecognisedDocument(DEVICE_DOCUMENT.to_owned());
        for _ in 0..SAVE_ATTEMPTS {
            let stored = self
                .store
                .load(&self.scope, DEVICE_DOCUMENT)
                .await?
                .ok_or_else(unrecognised)?;
            let mut device = decode_device_document(&stored.bytes)
                .map_err(|_| unrecognised())?
                .ok_or_else(unrecognised)?;
            if matches!(device.approval, StoredApproval::Spent) {
                return Err(ClientError::ApprovalAnswered);
            }
            device.approval = StoredApproval::Spent;
            let bytes = device.encode_document()?;
            match retry_save(
                self.store
                    .save(
                        &self.scope,
                        self.incarnation.get(),
                        DEVICE_DOCUMENT,
                        &bytes,
                        Some(stored.revision),
                    )
                    .await,
            ) {
                Ok(Some(_)) => {
                    self.ensure_open()?;
                    if let Some(device) = self.shared.device.borrow_mut().as_mut() {
                        device.approval_nonce.take();
                    }
                    drop(documents);
                    return Ok(());
                }
                Ok(None) => continue,
                Err(StoreError::Wiped) => return Err(self.close()),
                Err(error) => return Err(error.into()),
            }
        }
        Err(StoreError::Conflict.into())
    }

    /// Calls `listener` with every report from now on, each distinct report
    /// once: `(table, row, column, code)` identifies a report, a
    /// per-space one `(table, space, code)`; a report is withdrawn
    /// ([`Report::Withdrawn`]) when the key arrives (protocol §4.3). The
    /// reports still in the bounded set of ten thousand are delivered first.
    /// Hooks never call out while the database is busy: listeners are
    /// called by the runtime's own operations and [`E2eeClient::flush_reports`].
    pub fn on_report(&self, listener: impl Fn(&Report) + 'static) {
        self.shared.listen(Rc::new(listener));
    }

    /// Delivers the reports reads left since the last call to the
    /// listeners. The runtime's reads and writes do it themselves; a host
    /// that reads the database directly calls it afterwards.
    pub fn flush_reports(&self) {
        self.shared.flush(Vec::new());
    }

    /// Test observation: takes every report reads made since the last
    /// call, oldest first, whether or not it was delivered and before
    /// listeners' de-duplication, so a test can tell which read made one.
    /// Listeners still get them as usual.
    #[cfg(any(test, feature = "testing"))]
    pub fn take_reports(&self) -> Vec<Report> {
        self.shared.taken.borrow_mut().drain(..).collect()
    }

    /// Calls `f` with the keys this device holds.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_held<T>(&self, f: impl FnOnce(&jazz_e2ee::protocol::content::HeldKeys) -> T) -> T {
        f(&self.shared.keys.borrow().held)
    }

    /// Test observation: refused space-root creations this client remembers.
    #[cfg(any(test, feature = "testing"))]
    pub fn refused_creations(&self) -> usize {
        self.shared.creations.borrow().refused.len()
    }

    /// Test setup: remember a refused space-root creation, as the refusal hook
    /// does when the host collects it.
    #[cfg(any(test, feature = "testing"))]
    pub fn note_refused_creation(&self, root_id: &str, error: jazz::db::Error) {
        self.shared
            .creations
            .borrow_mut()
            .refused
            .insert(root_id.to_owned(), error);
    }

    /// Test observation for a host retaining its client through shutdown.
    #[cfg(any(test, feature = "testing"))]
    pub fn has_device_account_identity(&self) -> bool {
        self.device_account.borrow().is_some()
    }

    /// Closes the client and drops the device, account and held space keys.
    pub(crate) fn close(&self) -> ClientError {
        self.shared.forget_secrets();
        self.device_account.borrow_mut().take();
        self.account.borrow_mut().take();
        // Refused creations belong to this run's device and account.
        self.shared.creations.borrow_mut().refused.clear();
        ClientError::Closed
    }

    /// Refuses new writes, waits for the current store operation to settle,
    /// and clears secrets while the host can still answer store requests.
    pub(crate) async fn shutdown(&self) {
        self.shared.keys.borrow_mut().closed = true;
        let mut documents = self.documents.lock().await;
        self.close();
        *documents = Documents::default();
    }

    pub(crate) fn ensure_open(&self) -> Result<(), ClientError> {
        if self.shared.keys.borrow().closed {
            Err(ClientError::Closed)
        } else {
            Ok(())
        }
    }

    /// Holds `space`'s keys (from an acquisition, or this device's own
    /// creation) once they are stored: they are kept with what is already
    /// held under the same root (§6.2), and only that root's document is
    /// rewritten. The keys become usable only after the store confirms the
    /// save, so a key the device could lose on a restart never seals a
    /// write; a failed save holds nothing. A save that meets another
    /// writer's reads its document again and merges, this device's keys
    /// winning except over a `refused` mark.
    pub async fn keep(&self, space: HeldSpace) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
        self.ensure_open()?;
        let root_id = space.root_id().to_owned();
        let name = format!("{HELD_PREFIX}{root_id}");
        if documents.unrecognised.contains(&name) {
            return Err(ClientError::UnrecognisedDocument(name));
        }
        let mut staged = {
            let keys = self.shared.keys.borrow();
            let base = keys.held.space(&root_id).cloned().or_else(|| {
                documents
                    .held
                    .get(&root_id)
                    .and_then(|state| state.lineage.clone())
            });
            match base {
                Some(mut base) => {
                    base.keep(space)?;
                    base
                }
                None => space,
            }
        };
        for _ in 0..SAVE_ATTEMPTS {
            // A wipe that began while a save was in flight wins: nothing
            // more is written or held.
            self.ensure_open()?;
            let state = documents.held.entry(root_id.clone()).or_default();
            let unchanged = || {
                self.shared
                    .keys
                    .borrow()
                    .held
                    .space(&root_id)
                    .is_some_and(|held| held.same_stored_state(&staged))
            };
            let (bytes, sealed) =
                if let Some(bytes) = state.confirmed_bytes.take().filter(|_| unchanged()) {
                    (bytes, state.sealed.clone())
                } else {
                    let device = self.shared.device()?;
                    Self::owner_of(&self.shared, &device).seal_stored_document_reusing(
                        &staged,
                        &state.carried,
                        state.removals,
                        &state.sealed,
                        &mut **self.shared.random.borrow_mut(),
                    )?
                };
            // The store still checks the incarnation and revision for reused bytes.
            match retry_save(
                self.store
                    .save(
                        &self.scope,
                        self.incarnation.get(),
                        &name,
                        &bytes,
                        state.revision,
                    )
                    .await,
            ) {
                Ok(Some(revision)) => {
                    state.revision = Some(revision);
                    state.lineage = None;
                    state.sealed = sealed;
                    self.ensure_open()?;
                    self.shared.keys.borrow_mut().held.hold(staged.clone())?;
                    state.confirmed_bytes = unchanged().then_some(bytes);
                    return Ok(());
                }
                Ok(None) => {
                    // Another writer saved this root's document: take its
                    // keys too, then write both.
                    documents.held.remove(&root_id);
                    let theirs = match self.store.load(&self.scope, &name).await? {
                        Some(stored) => {
                            self.read_held(&mut documents, &name, &root_id, stored, false)
                                .await?
                        }
                        None => None,
                    };
                    if documents.unrecognised.contains(&name) {
                        return Err(ClientError::UnrecognisedDocument(name));
                    }
                    if let Some(state) = documents.held.get_mut(&root_id) {
                        state.lineage = None;
                    }
                    if let Some(mut theirs) = theirs {
                        theirs.keep(staged)?;
                        staged = theirs;
                    }
                }
                Err(StoreError::Wiped) => return Err(self.close()),
                Err(error) => return Err(error.into()),
            }
        }
        Err(StoreError::Conflict.into())
    }

    /// Removes verification hints before publishing an account reset or
    /// recovering an account with the same local device.
    pub(crate) async fn clear_verified_history(&self) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
        self.ensure_open()?;
        self.shared.keys.borrow_mut().held.clear_verified_history();
        for state in documents.held.values_mut() {
            state.confirmed_bytes = None;
        }
        for name in self.store.list(&self.scope).await? {
            let Some(root) = name.strip_prefix(HELD_PREFIX) else {
                continue;
            };
            let mut cleared = false;
            for _ in 0..SAVE_ATTEMPTS {
                self.ensure_open()?;
                let Some(stored) = self.store.load(&self.scope, &name).await? else {
                    cleared = true;
                    break;
                };
                let Some(bytes) = StoreOwner::clear_document_verification(&stored.bytes) else {
                    cleared = true;
                    break;
                };
                let saved = self
                    .store
                    .save(
                        &self.scope,
                        self.incarnation.get(),
                        &name,
                        &bytes,
                        Some(stored.revision),
                    )
                    .await;
                match retry_save(saved) {
                    Ok(Some(revision)) => {
                        if let Some(state) = documents.held.get_mut(root)
                            && state.revision == Some(stored.revision)
                        {
                            state.revision = Some(revision);
                        }
                        cleared = true;
                        break;
                    }
                    Ok(None) => {}
                    Err(StoreError::Wiped) => return Err(self.close()),
                    Err(error) => return Err(error.into()),
                }
            }
            if !cleared {
                return Err(StoreError::Conflict.into());
            }
        }
        Ok(())
    }

    /// The newest state of `account_id` this device verified, if any
    /// (§2.4, "Floors").
    pub async fn floor(&self, account_id: &str) -> Option<AccountFloor> {
        let documents = self.documents.lock().await;
        documents
            .floors
            .get(account_id)
            .and_then(|(floor, _)| floor.clone())
    }

    /// Stores `floor` as `account_id`'s floor, signed by the device. A floor
    /// only moves forward: one of an earlier cut, a lower generation or an
    /// earlier epoch position than the stored floor is `stale-state`, and
    /// nothing is written. The caller has checked the newer state against
    /// the stored floor first (`AccountState::check_floor`).
    pub async fn advance_floor(
        &self,
        account_id: &str,
        floor: &AccountFloor,
    ) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
        self.ensure_open()?;
        let name = format!("{FLOOR_PREFIX}{account_id}");
        for _ in 0..SAVE_ATTEMPTS {
            if documents.unrecognised.contains(&name) {
                return Err(ClientError::UnrecognisedDocument(name));
            }
            let (current, revision) = match documents.floors.get(account_id) {
                Some((current, revision)) => (current.as_ref(), Some(*revision)),
                None => (None, None),
            };
            // Every field that orders states moves forward: the cut, the
            // generation and the current epoch's position (§6.3a). A later
            // generation's epochs follow the earlier one's on the ledger.
            if current.is_some_and(|current| {
                floor.through < current.through
                    || floor.generation < current.generation
                    || floor.epoch_position < current.epoch_position
            }) {
                return Err(ProtocolError::StaleState.into());
            }
            let bytes = {
                let device = self.shared.device()?;
                Self::owner_of(&self.shared, &device)
                    .seal_floor(account_id, floor)?
                    .encode()?
            };
            match retry_save(
                self.store
                    .save(&self.scope, self.incarnation.get(), &name, &bytes, revision)
                    .await,
            ) {
                Ok(Some(revision)) => {
                    self.ensure_open()?;
                    documents
                        .floors
                        .insert(account_id.to_owned(), (Some(floor.clone()), revision));
                    return Ok(());
                }
                Ok(None) => {
                    documents.floors.remove(account_id);
                    if let Some(stored) = self.store.load(&self.scope, &name).await? {
                        self.load_floor(&mut documents, &name, account_id, stored, &mut None)?;
                    }
                }
                Err(StoreError::Wiped) => return Err(self.close()),
                Err(error) => return Err(error.into()),
            }
        }
        Err(StoreError::Conflict.into())
    }

    /// Wipes this account's private state (held keys, floors, the device)
    /// and enrolls a new local device in a new incarnation of the scope,
    /// keeping the runtime open (§2.4, "Floors"): what a device the account
    /// no longer has does before it requests approval again. If the old
    /// incarnation cannot be deleted or the new device cannot be stored, the
    /// runtime is closed and the error returned.
    pub(crate) async fn reenrol(&self) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
        self.ensure_open()?;
        {
            // Everything but what describes the open database starts again.
            let mut keys = self.shared.keys.borrow_mut();
            *keys = KeyState {
                catalogue: keys.catalogue.take(),
                online: keys.online,
                closed: keys.closed,
                ..Default::default()
            };
        }
        *self.shared.creations.borrow_mut() = Default::default();
        *self.shared.member_watch.borrow_mut() = Default::default();
        self.shared.written.borrow_mut().clear();
        // What belongs to the device and account that are gone: versions its
        // reads found missing.
        self.shared.missing.borrow_mut().clear();
        // Its reports, and the spaces watched for them.
        self.shared.reset_reports();
        self.reconciled.set(false);
        *self.account.borrow_mut() = None;
        *documents = Documents::default();
        // Once the old incarnation is deleted, the runtime has no device
        // until the new one is stored: any failure from here closes it, so
        // nothing runs on as a device the store no longer has.
        let renewed = async {
            self.store.delete(&self.scope).await?;
            let incarnation = self.store.open_scope(&self.scope).await?;
            let device = Self::load_device(
                DeviceLoad {
                    store: &*self.store,
                    incarnation,
                    registry: &self.shared.registry,
                    selected: &self.shared.selected,
                    domain: &self.shared.domain,
                    account_id: &self.shared.account_id,
                    scope: &self.scope,
                },
                None,
                &mut SharedRandom(&self.shared.random),
            )
            .await?;
            Ok::<_, ClientError>((incarnation, device))
        };
        let (incarnation, device) = match renewed.await {
            Ok(renewed) => renewed,
            Err(error) => {
                self.close();
                return Err(error);
            }
        };
        self.ensure_open()?;
        self.incarnation.set(incarnation);
        *self.shared.device.borrow_mut() = Some(device);
        Ok(())
    }

    /// Wipes this account's private state from the store: the device, every
    /// held key and every floor (§6.4). The runtime is closed first, so no
    /// write still in flight or made later recreates the store; it waits
    /// for writes in flight, then deletes. Closing drops the device's
    /// secrets, its approval nonce and the held keys from memory (each
    /// zeroed on drop), and [`E2eeClient::device`] fails from then on. A
    /// failed deletion is returned: the wipe is unfinished, and the runtime
    /// stays closed.
    pub async fn wipe(&self) -> Result<(), ClientError> {
        self.close();
        let mut documents = self.documents.lock().await;
        *documents = Documents::default();
        self.store.delete(&self.scope).await?;
        Ok(())
    }
}
