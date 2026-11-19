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
    DeviceSecrets, FLOOR_FORMAT, FloorRecord, StoredApproval, StoredDevice, decode_device_document,
};
use jazz_e2ee::protocol::content::store::{
    HeldKeyRecord, Rejected, RejectedRecord, StoreOwner, decode_document,
};
use jazz_e2ee::protocol::derive;
use jazz_e2ee::protocol::device_account::DeviceAccountIdentity;
use jazz_e2ee::protocol::domain::AppDomain;
use jazz_e2ee::registry::Registry;

use crate::account::OwnAccount;
use crate::error::ClientError;
use crate::hooks::{Hooks, plans};
use crate::mechanisms;
use crate::state::{Catalogue, KeyState, Report, Shared};
use crate::store::{PrivateStore, StoreError};

const DEVICE_DOCUMENT: &str = "device";
const HELD_PREFIX: &str = "held-";
const FLOOR_PREFIX: &str = "floor-";
/// Attempts of a save that keeps meeting another writer's save.
const SAVE_ATTEMPTS: usize = 3;

/// What a runtime opens with.
pub struct ClientConfig {
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
    /// For a device-account account, the identity its account secret derives:
    /// it signs the account's roots, and the device checks the account's
    /// root against it (§2.3, §2.4). `None` for an account of the
    /// application's own sign-in.
    pub device_account: Option<DeviceAccountIdentity>,
}

/// One held-key document as last read or written.
#[derive(Default)]
struct HeldDocumentState {
    /// The store's revision of the document, `None` before it exists.
    revision: Option<u64>,
    /// The document's own removal counter (§6.4).
    removals: u64,
    /// Records skipped as unsupported, carried over unchanged (§6.2).
    carried: Vec<HeldKeyRecord>,
    /// The checked lineage when no key of the document loaded.
    lineage: Option<HeldSpace>,
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
    pub(crate) device_account: Option<DeviceAccountIdentity>,
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
    let opened = registry.open_sealed(std::slice::from_ref(&device.sealing), &bytes, &sealed);
    let signature = signer.sign(&device.signing, &bytes)?;
    let verified = registry.verify(&device.signing.public, &bytes, &signature);
    if opened.ok().as_deref().map(|key| &key[..]) != Some(&probe[..]) || verified != Ok(true) {
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
    /// checks is skipped and reported ([`E2eeClient::take_reports`]); a
    /// document of a format this runtime does not read fails the open if
    /// it is the device document, and is otherwise reported and never
    /// overwritten. A store or provider failure fails the open.
    pub async fn open(config: ClientConfig) -> Result<Self, ClientError> {
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
        let scope = domain.account(&account_id)?.as_str().to_owned();
        // A wipe that lands while the scope opens deletes it under this
        // runtime: open the new scope and load again.
        let mut opened = None;
        let random = RefCell::new(random);
        for _ in 0..SAVE_ATTEMPTS {
            let incarnation = store.open_scope(&scope).await?;
            match Self::load_device(
                &*store,
                incarnation,
                &registry,
                &selected,
                &domain,
                &account_id,
                &scope,
                &mut SharedRandom(&random),
            )
            .await
            {
                Ok(device) => {
                    opened = Some((incarnation, device));
                    break;
                }
                Err(ClientError::Closed) => continue,
                Err(error) => return Err(error),
            }
        }
        let (incarnation, device) = opened.ok_or(ClientError::Store(StoreError::Wiped))?;
        let shared = Rc::new(Shared {
            content: domain.shared(),
            registry,
            selected,
            domain,
            account_id,
            plans,
            device: device.into(),
            keys: KeyState {
                online: true,
                ..KeyState::default()
            }
            .into(),
            random,
            reports: Default::default(),
            outbox: Default::default(),
            published: Default::default(),
            creations: Default::default(),
            missing: Default::default(),
            member_changes: Default::default(),
            member_change_wait: std::cell::Cell::new(crate::state::MEMBER_CHANGE_WAIT),
            collectors: Default::default(),
        });
        let client = Self {
            hooks: Rc::new(Hooks {
                declarations,
                shared: shared.clone(),
            }),
            shared,
            store,
            scope,
            incarnation: Cell::new(incarnation),
            device_account,
            account: RefCell::new(None),
            reconciled: Cell::new(false),
            documents: Mutex::new(Documents::default()),
        };
        client.load_documents().await?;
        Ok(client)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_device(
        store: &dyn PrivateStore,
        incarnation: u64,
        registry: &Registry,
        selected: &jazz_e2ee::agility::MechanismConfig,
        domain: &AppDomain,
        account_id: &str,
        scope: &str,
        random: &mut dyn RandomSource,
    ) -> Result<LocalDevice, ClientError> {
        let unrecognised = || ClientError::UnrecognisedDocument(DEVICE_DOCUMENT.to_owned());
        for _ in 0..SAVE_ATTEMPTS {
            let stored = store.load(scope, DEVICE_DOCUMENT).await?;
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
            match store
                .save(scope, incarnation, DEVICE_DOCUMENT, &document, expected)
                .await
            {
                Ok(_) => return Ok(device),
                // Another writer stored a device first: the store keeps one
                // device per scope, so load that one.
                Err(StoreError::Conflict) => continue,
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

    async fn load_documents(&self) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
        for name in self.store.list(&self.scope).await? {
            if let Some(root_id) = name.strip_prefix(HELD_PREFIX) {
                if let Some(space) = self.read_held(&mut documents, &name, root_id).await? {
                    self.shared.keys.borrow_mut().held.hold(space)?;
                }
            } else if let Some(account_id) = name.strip_prefix(FLOOR_PREFIX) {
                self.load_floor(&mut documents, &name, account_id).await?;
            }
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
    ) -> Result<Option<HeldSpace>, ClientError> {
        let Some(stored) = self.store.load(&self.scope, name).await? else {
            return Ok(None);
        };
        let Ok(decoded) = decode_document(&stored.bytes) else {
            documents.unrecognised.insert(name.to_owned());
            self.skipped(
                name,
                "a held-key document of a format this runtime does not read",
            );
            return Ok(None);
        };
        let loaded = {
            let device = self.shared.device.borrow();
            Self::owner_of(&self.shared, &device).load(&decoded)?
        };
        for rejected in &loaded.rejected {
            self.skipped(name, rejection(rejected));
        }
        let mut state = HeldDocumentState {
            revision: Some(stored.revision),
            removals: loaded.revision,
            carried: loaded.unsupported,
            lineage: loaded.lineage,
        };
        let mut space = loaded.space;
        if space
            .as_ref()
            .is_some_and(|space| space.root_id() != root_id)
        {
            self.skipped(name, "the document holds another root's keys");
            state.carried.clear();
            space = None;
        }
        if state
            .lineage
            .as_ref()
            .is_some_and(|lineage| lineage.root_id() != root_id)
        {
            state.lineage = None;
        }
        documents.held.insert(root_id.to_owned(), state);
        Ok(space)
    }

    /// Loads one floor record.
    async fn load_floor(
        &self,
        documents: &mut Documents,
        name: &str,
        account_id: &str,
    ) -> Result<(), ClientError> {
        let Some(stored) = self.store.load(&self.scope, name).await? else {
            return Ok(());
        };
        let floor = match FloorRecord::decode(&stored.bytes) {
            Ok(record) => {
                let device = self.shared.device.borrow();
                match Self::owner_of(&self.shared, &device).load_floor(account_id, &record)? {
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
    pub fn prepare(&self, db: &Db) -> Result<bool, ClientError> {
        // A refused creation is found among the refusals the store keeps.
        db.track_upstream_rejections();
        let catalogue = Catalogue::resolve(db, &self.shared.plans)?;
        let known = catalogue.is_some();
        if known {
            self.shared.keys.borrow_mut().catalogue = catalogue;
        }
        Ok(known)
    }

    /// Whether the client is online: offline, equality queries compute no
    /// tokens and match every row of the space instead (protocol §4.5).
    pub fn set_online(&self, online: bool) {
        self.shared.keys.borrow_mut().online = online;
    }

    /// How many staged member rows are still waited for, and how long to wait
    /// for one. Only for tests of that queue.
    #[doc(hidden)]
    pub fn pending_member_changes(&self) -> usize {
        self.shared.member_changes.borrow().len()
    }

    #[doc(hidden)]
    pub fn set_member_change_wait(&self, wait: std::time::Duration) {
        self.shared.member_change_wait.set(wait);
    }

    /// This device: its ID and key pairs.
    pub fn device(&self) -> std::cell::Ref<'_, LocalDevice> {
        self.shared.device.borrow()
    }

    /// The account this runtime is for.
    pub fn account_id(&self) -> &str {
        &self.shared.account_id
    }

    /// Takes the reports reads and loads left since the last call, oldest
    /// first.
    pub fn take_reports(&self) -> Vec<Report> {
        self.shared.reports.borrow_mut().drain(..).collect()
    }

    /// Marks this device's approval nonce spent in the private store, and
    /// returns once the store confirmed the save (protocol §2.5, §6.1): the
    /// proof that reveals the nonce is only published after this, so a
    /// device restored from an older copy of its store, whose document
    /// says spent, answers no challenge. Fails if the document already says
    /// spent.
    pub(crate) async fn mark_nonce_spent(&self) -> Result<(), ClientError> {
        let mut documents = self.documents.lock().await;
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
            match self
                .store
                .save(
                    &self.scope,
                    self.incarnation.get(),
                    DEVICE_DOCUMENT,
                    &bytes,
                    Some(stored.revision),
                )
                .await
            {
                Ok(_) => {
                    drop(documents);
                    return Ok(());
                }
                Err(StoreError::Conflict) => continue,
                Err(StoreError::Wiped) => return Err(self.close()),
                Err(error) => return Err(error.into()),
            }
        }
        let _ = &mut documents;
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

    /// Calls `f` with the keys this device holds.
    pub fn with_held<T>(&self, f: impl FnOnce(&jazz_e2ee::protocol::content::HeldKeys) -> T) -> T {
        f(&self.shared.keys.borrow().held)
    }

    /// Closes the runtime after finding its scope wiped by another writer:
    /// the held keys are dropped and nothing more is written.
    fn close(&self) -> ClientError {
        let mut keys = self.shared.keys.borrow_mut();
        keys.closed = true;
        keys.held = Default::default();
        ClientError::Closed
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
            let bytes = {
                let device = self.shared.device.borrow();
                Self::owner_of(&self.shared, &device)
                    .seal_document(
                        &staged,
                        &state.carried,
                        state.removals,
                        &mut **self.shared.random.borrow_mut(),
                    )?
                    .encode()?
            };
            match self
                .store
                .save(
                    &self.scope,
                    self.incarnation.get(),
                    &name,
                    &bytes,
                    state.revision,
                )
                .await
            {
                Ok(revision) => {
                    state.revision = Some(revision);
                    state.lineage = None;
                    self.ensure_open()?;
                    self.shared.keys.borrow_mut().held.hold(staged)?;
                    return Ok(());
                }
                Err(StoreError::Conflict) => {
                    // Another writer saved this root's document: take its
                    // keys too, then write both.
                    documents.held.remove(&root_id);
                    let theirs = self.read_held(&mut documents, &name, &root_id).await?;
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
    /// only moves forward: one of an earlier cut or a lower generation than
    /// the stored floor is `stale-state`, and nothing is written. The
    /// caller has checked the newer state against the stored floor first
    /// (`AccountState::check_floor`).
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
            if current.is_some_and(|current| {
                floor.through < current.through || floor.generation < current.generation
            }) {
                return Err(ProtocolError::StaleState.into());
            }
            let bytes = {
                let device = self.shared.device.borrow();
                Self::owner_of(&self.shared, &device)
                    .seal_floor(account_id, floor)?
                    .encode()?
            };
            match self
                .store
                .save(&self.scope, self.incarnation.get(), &name, &bytes, revision)
                .await
            {
                Ok(revision) => {
                    documents
                        .floors
                        .insert(account_id.to_owned(), (Some(floor.clone()), revision));
                    return Ok(());
                }
                Err(StoreError::Conflict) => {
                    documents.floors.remove(account_id);
                    self.load_floor(&mut documents, &name, account_id).await?;
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
    /// no longer has does before it requests approval again.
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
        // What belongs to the device and account that are gone: versions its
        // reads found missing.
        self.shared.missing.borrow_mut().clear();
        self.reconciled.set(false);
        *self.account.borrow_mut() = None;
        *documents = Documents::default();
        self.store.delete(&self.scope).await?;
        let incarnation = self.store.open_scope(&self.scope).await?;
        let device = Self::load_device(
            &*self.store,
            incarnation,
            &self.shared.registry,
            &self.shared.selected,
            &self.shared.domain,
            &self.shared.account_id,
            &self.scope,
            &mut SharedRandom(&self.shared.random),
        )
        .await?;
        self.incarnation.set(incarnation);
        *self.shared.device.borrow_mut() = device;
        Ok(())
    }

    /// Wipes this account's private state from the store: the device, every
    /// held key and every floor (§6.4). The runtime is closed first, so no
    /// write still in flight or made later recreates the store; it waits
    /// for writes in flight, then deletes. A failed deletion is returned:
    /// the wipe is unfinished, and the runtime stays closed.
    pub async fn wipe(&self) -> Result<(), ClientError> {
        {
            let mut keys = self.shared.keys.borrow_mut();
            keys.closed = true;
            keys.held = Default::default();
        }
        let mut documents = self.documents.lock().await;
        *documents = Documents::default();
        self.store.delete(&self.scope).await?;
        Ok(())
    }
}
