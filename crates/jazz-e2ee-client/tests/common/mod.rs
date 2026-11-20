//! A real authority `Db` and client `Db`s opened with the runtime's hooks,
//! linked in memory, and the keys a device holds for a space, made with
//! `jazz-e2ee`'s own record builders.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use futures::future::LocalBoxFuture;
use jazz::app_id::AppId;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, ReadOpts, ReadTier, SyncMessage, Transport,
    TransportError, Upstream, block_on, encode_frames,
};
use jazz::e2ee::{
    E2eeDeclarations, EncryptedColumn, EncryptionEntry, EncryptionSettings, PlainType,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_e2ee::OsRandom;
use jazz_e2ee::Zeroizing;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::account::build::{self, AccountWriter, LocalDevice, RootAuthority};
use jazz_e2ee::protocol::account::{
    AccountKeyEvidence, AccountReplay, PrivateAccountLedger, PublicAccountLedger, verify_private,
};
use jazz_e2ee::protocol::content::{CreationFate, HeldSpace};
use jazz_e2ee::protocol::domain::AppDomain;
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::protocol::space::AccountDirectory;
use jazz_e2ee::protocol::space::build::{Author, RootRecords, SpaceWriter, space_root};
use jazz_e2ee::protocol::{Accepted, Position};
use jazz_e2ee::registry::Registry;
use jazz_e2ee_client::{ClientConfig, E2eeClient, MemoryStore, PrivateStore, StoreError, Stored};

pub const ALICE: &str = "a11ce000-0000-5000-8000-00000000a11c";
pub const SECRET: &str = "PLAINTEXT-SECRET";
pub const ENVIRONMENT: &str = "test";

pub fn app_id() -> AppId {
    AppId::from_name("jazz-e2ee-client-tests")
}

pub fn allow_all() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

/// `projects(name)`, the space table; `notes(projectId, title, body,
/// __e2ee_eq_body, note, attachment, rank)` whose `body` (text, with an
/// equality index), `note` (optional text), `attachment` (optional bytes)
/// and `rank` (optional integer) are encrypted, each stored as required
/// bytes, as the protocol prescribes.
pub fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Text)
                    .column("body", ColumnType::Bytea)
                    .nullable_column("__e2ee_eq_body", ColumnType::Bytea)
                    .nullable_column("__e2ee_eq_note", ColumnType::Bytea)
                    .column("note", ColumnType::Bytea)
                    .column("attachment", ColumnType::Bytea)
                    .column("rank", ColumnType::Bytea)
                    .nullable_column("plainNote", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("vaults")
                    .column("label", ColumnType::Text)
                    .column("secret", ColumnType::Bytea)
                    .policies(allow_all()),
            )
            .build(),
    )
    .unwrap()
}

/// The schema before a migration added `notes`' encrypted columns.
pub fn schema_before_contents() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Text)
                    .nullable_column("plainNote", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("vaults")
                    .column("label", ColumnType::Text)
                    .column("secret", ColumnType::Bytea)
                    .policies(allow_all()),
            )
            .build(),
    )
    .unwrap()
}

/// The migration that adds the encrypted columns (and the equality column)
/// with the empty value, as the protocol's lens does.
pub fn add_contents() -> MigrationLens {
    let add = |column: &str| LensOp::AddColumn {
        column: column.into(),
        default: Value::Bytes(Vec::new()),
    };
    MigrationLens::new(
        schema_before_contents().version_id(),
        schema().version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: [
                "body",
                "__e2ee_eq_body",
                "__e2ee_eq_note",
                "note",
                "attachment",
                "rank",
            ]
            .into_iter()
            .map(add)
            .collect(),
        }],
    )
    .unwrap()
}

/// A database config of the current schema that knows the migration.
pub fn migrated(config: DbConfig) -> DbConfig {
    config.with_migrations(vec![schema_before_contents()], vec![add_contents()])
}

pub fn encrypted(column_type: ColumnType, nullable: bool) -> EncryptedColumn {
    EncryptedColumn {
        plain: PlainType {
            column_type,
            nullable,
            default: None,
        },
        sealed_type: None,
        from: None,
        to: None,
        convert: None,
        reverse: None,
    }
}

pub fn declarations() -> E2eeDeclarations {
    declarations_with(EncryptionSettings::default())
}

pub fn declarations_with(settings: EncryptionSettings) -> E2eeDeclarations {
    E2eeDeclarations {
        app_id: app_id(),
        environment: ENVIRONMENT.to_owned(),
        settings,
        tables: BTreeMap::from([
            (
                "vaults".to_owned(),
                EncryptionEntry {
                    space: "id".to_owned(),
                    scope: "vaults".to_owned(),
                    columns: BTreeMap::from([(
                        "secret".to_owned(),
                        encrypted(ColumnType::Text, true),
                    )]),
                    equality: Vec::new(),
                    backfill: Vec::new(),
                    rehome: None,
                    sweep: Default::default(),
                },
            ),
            (
                "notes".to_owned(),
                EncryptionEntry {
                    space: "projectId".to_owned(),
                    scope: "projects".to_owned(),
                    columns: BTreeMap::from([
                        ("body".to_owned(), encrypted(ColumnType::Text, false)),
                        ("note".to_owned(), encrypted(ColumnType::Text, true)),
                        ("attachment".to_owned(), encrypted(ColumnType::Bytea, true)),
                        ("rank".to_owned(), encrypted(ColumnType::Integer, true)),
                    ]),
                    equality: vec!["body".to_owned()],
                    backfill: Vec::new(),
                    rehome: None,
                    sweep: Default::default(),
                },
            ),
        ]),
    }
}

/// Opens alice's runtime on `store`.
pub fn open_client(store: Rc<dyn PrivateStore>) -> E2eeClient {
    open_client_with(store, declarations())
}

pub fn open_client_with(store: Rc<dyn PrivateStore>, declarations: E2eeDeclarations) -> E2eeClient {
    block_on(E2eeClient::open(ClientConfig {
        store,
        declarations,
        account_id: ALICE.to_owned(),
        registry: Registry::builtin(),
        random: Box::new(OsRandom),
    }))
    .unwrap()
}

pub fn identity(node: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorSubject::SYSTEM,
    }
}

pub fn authority() -> Db {
    let server = block_on(Db::open(DbConfig::new(schema(), identity(0)))).unwrap();
    server.set_authority().unwrap();
    server
}

/// A client database with the runtime's hooks, in memory or at `path`.
pub fn client_db(client: &E2eeClient, path: Option<&Path>) -> Db {
    let config = DbConfig::new(schema(), identity(1)).with_e2ee(client.hooks());
    match path {
        Some(path) => block_on(Db::open_at(config, path, FileOptions::default())).unwrap(),
        None => block_on(Db::open(config)).unwrap(),
    }
}

/// A transport that keeps every frame it sends, as bytes on the wire.
struct Recorded {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    wire: Rc<RefCell<Vec<u8>>>,
}

impl Transport for Recorded {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        for frame in encode_frames(message.clone())? {
            self.wire.borrow_mut().extend(frame);
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// `client` connected to `server`; every frame either sends is recorded.
pub fn link(client: &Db, server: &Db) -> (Upstream, Rc<RefCell<Vec<u8>>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let wire = Rc::new(RefCell::new(Vec::new()));
    let link = block_on(client.connect_upstream(Box::new(Recorded {
        out: up.clone(),
        inbound: down.clone(),
        wire: wire.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Recorded {
                out: down,
                inbound: up,
                wire: wire.clone(),
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    (link, wire)
}

pub fn pump(server: &Db, client: &Db) {
    for _ in 0..10 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

/// A client database linked to `server` and synced once, so the runtime
/// Runs `future` while the clients and `server` exchange what it waits
/// for.
pub fn drive<T>(server: &Db, clients: &[&Db], future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..2_000 {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    panic!("the operation did not finish within 2,000 turns");
}

/// knows the catalogue identities.
pub fn online(
    client: &E2eeClient,
    server: &Db,
    path: Option<&Path>,
) -> (Db, Upstream, Rc<RefCell<Vec<u8>>>) {
    let db = client_db(client, path);
    let (upstream, wire) = link(&db, server);
    pump(server, &db);
    assert!(client.prepare(&db).unwrap(), "identities after one sync");
    (db, upstream, wire)
}

pub fn local() -> ReadOpts {
    ReadOpts::for_read_tier(ReadTier::LocalOnly)
}

pub fn all(db: &Db, query: Query) -> Result<Vec<jazz::db::CurrentRow>, jazz::db::Error> {
    let prepared = db.prepare_query(&query)?;
    block_on(db.all(&prepared, local()))
}

pub fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

pub fn project(db: &Db, name: &str) -> uuid::Uuid {
    let row = block_on(db.insert(
        "projects",
        BTreeMap::from([("name".to_owned(), text(name))]),
        Default::default(),
    ))
    .unwrap();
    row.row_uuid().0
}

pub fn note(space: uuid::Uuid, body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("projectId".to_owned(), Value::Uuid(space)),
        ("title".to_owned(), text("title")),
        ("body".to_owned(), text(body)),
    ])
}

pub fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// The space address of project `space` as the runtime sees it.
pub fn address(db: &Db, space: uuid::Uuid) -> SpaceAddress {
    address_in(db, "projects", space)
}

/// The address of the space that row `space` of space table `scope` is.
pub fn address_in(db: &Db, scope: &str, space: uuid::Uuid) -> SpaceAddress {
    let scope = db.table_identity(scope).unwrap().unwrap().0;
    SpaceAddress::new(&scope.to_string(), &space.to_string()).unwrap()
}

/// Alice's account bootstrapped with `device` and a new root of the space
/// at `address`, with its key, built and checked with the protocol's own
/// builders. The rows stay in this fixture: the runtime's hooks only need
/// the held key.
pub fn new_root(device: &LocalDevice, address: &SpaceAddress) -> RootRecords {
    let registry = Registry::builtin();
    let config = MechanismConfig::default();
    let domain = AppDomain::new(
        &app_id().as_object_id().uuid().to_string(),
        Some(ENVIRONMENT),
    )
    .unwrap();
    let mut random = OsRandom;
    let suite = registry.select(&config).unwrap();
    let writer = AccountWriter {
        suite,
        registry: &registry,
        domain: &domain,
        account_id: ALICE,
    };
    let mut public = PublicAccountLedger::new(ALICE, Position(1));
    let mut private = PrivateAccountLedger::new(Position(1));
    private
        .requests
        .push(Accepted::new(Position(1), device.request()));
    public.keys.push(Accepted::new(
        Position(1),
        device.keys_row(&mut random).unwrap(),
    ));
    let permit = build::check_bootstrap(&writer, device, &public, &private).unwrap();
    let records =
        build::account_root(&writer, device, &permit, RootAuthority::App, &mut random).unwrap();
    public.through = Position(2);
    private.through = Position(2);
    private
        .identities
        .push(Accepted::new(Position(2), records.identity));
    public.roots.push(Accepted::new(Position(2), records.root));
    public
        .account_keys
        .push(Accepted::new(Position(2), records.account_key));
    let replay = AccountReplay::new(&public, &domain, &registry).unwrap();
    let state = replay.current().unwrap().unwrap();
    let evidence = verify_private(&replay, &state, &private).unwrap();
    let key = AccountKeyEvidence::new(&replay, &state, &evidence)
        .unwrap()
        .acquire(&device.id, std::slice::from_ref(&device.sealing))
        .unwrap();
    let ledgers = vec![public.clone()];
    let directory = AccountDirectory::new(&ledgers, &domain, &registry, Position(2)).unwrap();
    let author = Author::new(&directory, ALICE, &key, device).unwrap();
    let space_writer = SpaceWriter {
        suite,
        registry: &registry,
        domain: &domain,
        account_id: ALICE,
    };
    space_root(&space_writer, address, &author, &mut random).unwrap()
}

/// The device's key of `root` as an accepted creation leaves it.
pub fn held(root: &RootRecords) -> HeldSpace {
    let domain = AppDomain::new(
        &app_id().as_object_id().uuid().to_string(),
        Some(ENVIRONMENT),
    )
    .unwrap();
    let mut held =
        HeldSpace::from_creation(&Registry::builtin(), &domain, root, "creation").unwrap();
    held.settle(CreationFate::Accepted, Some(&root.root.id));
    held
}

/// A new space root's key, held as an accepted creation leaves it.
pub fn new_space_key(device: &LocalDevice, address: &SpaceAddress) -> HeldSpace {
    held(&new_root(device, address))
}

/// Keeps a new key for project `space`, as an accepted creation would,
/// and returns its root's ID.
pub fn hold_new_space(client: &E2eeClient, db: &Db, space: uuid::Uuid) -> String {
    let held = new_space_key(&client.device().unwrap(), &address(db, space));
    let root_id = held.root_id().to_owned();
    block_on(client.keep(held)).unwrap();
    root_id
}

/// A memory store whose protection of secrets stands in for an operating
/// system key store: it scrambles them with a key held only in this
/// process, so what the store keeps cannot be unprotected without it.
#[derive(Clone)]
pub struct ProtectedStore {
    pub inner: MemoryStore,
    pub key: u8,
}

impl PrivateStore for ProtectedStore {
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> LocalBoxFuture<'a, Result<Option<Stored>, StoreError>> {
        self.inner.load(scope, doc)
    }

    fn open_scope<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        self.inner.open_scope(scope)
    }

    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        self.inner.save(scope, incarnation, doc, bytes, expected)
    }

    fn list<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<Vec<String>, StoreError>> {
        self.inner.list(scope)
    }

    fn delete<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<(), StoreError>> {
        self.inner.delete(scope)
    }

    fn protect<'a>(&'a self, secret: &'a [u8]) -> LocalBoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let mut out = b"protected:".to_vec();
            out.extend(secret.iter().map(|byte| byte ^ self.key));
            Ok(out)
        })
    }

    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            let body = blob
                .strip_prefix(b"protected:")
                .ok_or_else(|| StoreError::Failed("not a protected blob".to_owned()))?;
            Ok(Zeroizing::new(
                body.iter().map(|byte| byte ^ self.key).collect(),
            ))
        })
    }
}

/// A memory store whose next saves can be made to fail, or to yield to
/// other work and then meet another writer's save: what a full disk or a
/// second tab looks like.
#[derive(Clone, Default)]
pub struct FaultyStore {
    pub inner: MemoryStore,
    /// Saves fail while set.
    pub failing: Rc<std::cell::Cell<bool>>,
    /// The next save yields once, then reports a conflict.
    pub contended: Rc<std::cell::Cell<bool>>,
    /// The next save first deletes its scope, as another tab's wipe would.
    pub wiped_first: Rc<std::cell::Cell<bool>>,
}

/// Returns pending once, waking itself, so that other futures run.
struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            std::task::Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

impl PrivateStore for FaultyStore {
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> LocalBoxFuture<'a, Result<Option<Stored>, StoreError>> {
        self.inner.load(scope, doc)
    }

    fn open_scope<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        self.inner.open_scope(scope)
    }

    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            if self.failing.get() {
                return Err(StoreError::Failed("the disk is full".to_owned()));
            }
            if self.wiped_first.replace(false) {
                self.inner.delete(scope).await?;
            }
            if self.contended.replace(false) {
                YieldOnce(false).await;
                return Err(StoreError::Conflict);
            }
            self.inner
                .save(scope, incarnation, doc, bytes, expected)
                .await
        })
    }

    fn list<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<Vec<String>, StoreError>> {
        self.inner.list(scope)
    }

    fn delete<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<(), StoreError>> {
        self.inner.delete(scope)
    }

    fn protect<'a>(&'a self, secret: &'a [u8]) -> LocalBoxFuture<'a, Result<Vec<u8>, StoreError>> {
        self.inner.protect(secret)
    }

    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>> {
        self.inner.unprotect(blob)
    }
}

/// Every cell of every table of the SQLite file at `path`, as bytes.
pub fn stored_bytes(path: &std::path::Path) -> Vec<u8> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut out = Vec::new();
    for table in tables {
        let mut statement = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
        let columns = statement.column_count();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for i in 0..columns {
                if let rusqlite::types::ValueRef::Text(bytes)
                | rusqlite::types::ValueRef::Blob(bytes) = row.get_ref(i).unwrap()
                {
                    out.extend_from_slice(bytes);
                }
            }
        }
    }
    out
}
