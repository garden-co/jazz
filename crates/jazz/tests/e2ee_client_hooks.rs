//! The client's encryption hooks are called wherever a value becomes stored
//! or leaves for a reader (E2EE protocol §4–§5): writes are sealed before
//! anything stores, chunks or uploads them, reads are opened before rows
//! reach a reader, equality filters become token filters before a query is
//! validated or sent, and a refusal surfaces on the call that needs the key.
//!
//! The hooks here are a reversible stand-in, not cryptography: a sealed
//! cell is the plaintext's bytes XORed with a constant behind a header
//! naming its space, so a test can tell sealed bytes from plaintext and
//! prove that plaintext never reaches storage or the wire.

use crate::common::{allow_all_policies, compile_schema};

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::app_id::AppId;
use jazz::db::CurrentRow;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, InsertOptions, ReadOpts, ReadTier, Reason,
    SubscriptionEvent, SyncMessage, Transport, TransportError, block_on, encode_frames,
};
use jazz::e2ee::{
    E2eeDeclarations, E2eeHooks, E2eeRefusal, EncryptedColumn, EncryptionEntry, EncryptionSettings,
    EpochSetId, OpenOutcome, PlainType, Provisional, QueryPlan, ReadSite, Residual, ResidualPred,
    SealError, SpaceAddress, StreamSealer, StreamTarget, TransactionFate, Unreadable,
    UnreadableCode, WriteCells, WriteKind,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::{ArraySubquery, Operand, Predicate, Query, col, eq, lit};
use jazz::records::{Value, ValueType};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

const SECRET: &str = "PLAINTEXT-SECRET";
const KEY: u8 = 0x5a;
const TOKEN_KEY: u8 = 0x33;

/// `projects(name)`, the space table; `notes(projectId, title, body,
/// __e2ee_eq_body)`, whose `body` is encrypted with equality tokens; and
/// `space_roots(space)`, the internal rows a first write to a space stages;
/// and `closed(space)`, which nobody may write.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Text)
                    .column("body", ColumnType::Bytea)
                    .nullable_column("__e2ee_eq_body", ColumnType::Bytea)
                    .nullable_column("attachment", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("noteId", "notes")
                    .column("text", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("vaults")
                    .nullable_column("secret", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("space_roots")
                    .column("space", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("closed")
                    .column("space", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
}

fn declarations() -> E2eeDeclarations {
    E2eeDeclarations {
        app_id: AppId::from_name("e2ee-client-hooks"),
        environment: "test".to_owned(),
        settings: EncryptionSettings::default(),
        tables: BTreeMap::from([
            (
                "vaults".to_owned(),
                EncryptionEntry {
                    space: "id".to_owned(),
                    scope: "vaults".to_owned(),
                    columns: BTreeMap::from([(
                        "secret".to_owned(),
                        encrypted(ColumnType::Bytea, true),
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
                        ("attachment".to_owned(), encrypted(ColumnType::Bytea, true)),
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

fn encrypted(column_type: ColumnType, nullable: bool) -> EncryptedColumn {
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

/// A streamed value: `st`, its space, the bytes XORed, then `END`.
struct FakeSealer {
    space: uuid::Uuid,
}

impl StreamSealer for FakeSealer {
    fn header(&mut self) -> Result<Vec<u8>, jazz::db::Error> {
        let mut out = b"st".to_vec();
        out.extend_from_slice(self.space.as_bytes());
        Ok(out)
    }

    fn push(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, jazz::db::Error> {
        Ok(plaintext.iter().map(|byte| byte ^ KEY).collect())
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>, jazz::db::Error> {
        Ok(b"END".to_vec())
    }
}

fn address(space: uuid::Uuid) -> SpaceAddress {
    SpaceAddress {
        scope_id: "projects".to_owned(),
        identifier: space.to_string(),
    }
}

/// `plain` sealed for `space`: `ct`, the space, then the bytes XORed.
fn seal(space: uuid::Uuid, plain: &[u8]) -> Vec<u8> {
    let mut out = b"ct".to_vec();
    out.extend_from_slice(space.as_bytes());
    out.extend(plain.iter().map(|byte| byte ^ KEY));
    out
}

fn token(plain: &str) -> Vec<u8> {
    let mut out = b"tk".to_vec();
    out.extend(plain.bytes().map(|byte| byte ^ TOKEN_KEY));
    out
}

enum Opened {
    Plain(Vec<u8>),
    MissingKey(uuid::Uuid),
    Invalid,
}

/// The stand-in hooks: a set of spaces whose key this device holds, the
/// spaces it staged a root for, and a record of each call.
#[derive(Default)]
struct Fake {
    declarations: Option<E2eeDeclarations>,
    held: RefCell<BTreeSet<uuid::Uuid>>,
    rooted: RefCell<BTreeSet<uuid::Uuid>>,
    /// Seal nothing: a writer that stores whatever it likes.
    hostile: Cell<bool>,
    epoch: Cell<u8>,
    seals: Cell<usize>,
    plans: Cell<usize>,
    /// Each read's strictness, in call order.
    opens: RefCell<Vec<bool>>,
    /// Each write's kind and the stored space core supplied.
    writes: RefCell<Vec<(WriteKind, Option<Value>)>>,
    lens_defaults: Cell<usize>,
    /// The streams it began sealing.
    streams: Cell<usize>,
    /// Fail every read of an encrypted table: a hook that cannot open
    /// anything at all (its own state lost, say).
    failing: Cell<bool>,
    /// Plan as offline: no tokens, the space filter alone, the residual
    /// doing all the filtering (protocol §4.5).
    offline: Cell<bool>,
    /// Leave pagination on the physical query as well as the residual.
    keep_physical_window: Cell<bool>,
    /// Rely on the physical token filter alone.
    omit_residual_predicates: Cell<bool>,
    /// Return a query that still filters the encrypted column: 1 keeps the
    /// filter in a rewritten query, 2 plans nothing at all.
    leak: Cell<u8>,
    /// Built from an encrypted app's declarations even when they name no
    /// table (a version that keeps the system tables and declares nothing).
    encrypted_app: Cell<bool>,
}

impl Fake {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            declarations: Some(declarations()),
            ..Self::default()
        })
    }

    fn hold(&self, space: uuid::Uuid) {
        self.held.borrow_mut().insert(space);
    }

    fn forget(&self, space: uuid::Uuid) {
        self.held.borrow_mut().remove(&space);
    }

    /// A streamed value back, or why not.
    fn open_streamed(&self, cell: &[u8]) -> Result<Vec<u8>, Opened> {
        let body = cell
            .strip_prefix(b"st")
            .and_then(|rest| rest.strip_suffix(b"END"))
            .filter(|body| body.len() >= 16)
            .ok_or(Opened::Invalid)?;
        let space = uuid::Uuid::from_slice(&body[..16]).unwrap();
        if !self.held.borrow().contains(&space) {
            return Err(Opened::MissingKey(space));
        }
        Ok(body[16..].iter().map(|byte| byte ^ KEY).collect())
    }

    fn open(&self, cell: &[u8]) -> Opened {
        let Some(rest) = cell.strip_prefix(b"ct") else {
            return Opened::Invalid;
        };
        if rest.len() < 16 {
            return Opened::Invalid;
        }
        let space = uuid::Uuid::from_slice(&rest[..16]).unwrap();
        if !self.held.borrow().contains(&space) {
            return Opened::MissingKey(space);
        }
        Opened::Plain(rest[16..].iter().map(|byte| byte ^ KEY).collect())
    }
}

fn uuid_of(value: Option<&Value>) -> Option<uuid::Uuid> {
    match value? {
        Value::Uuid(id) => Some(*id),
        _ => None,
    }
}

impl E2eeHooks for Fake {
    fn declarations(&self) -> &E2eeDeclarations {
        self.declarations.as_ref().unwrap()
    }

    fn built_from_encrypted_app(&self) -> bool {
        self.encrypted_app.get() || !self.declarations().tables.is_empty()
    }

    fn seal_write(&self, write: &mut WriteCells<'_>) -> Result<(), SealError> {
        self.seals.set(self.seals.get() + 1);
        self.writes
            .borrow_mut()
            .push((write.kind.clone(), write.stored_space.cloned()));
        let Some(entry) = self.declarations().tables.get(write.table) else {
            return Ok(());
        };
        if self.hostile.get() {
            return Ok(());
        }
        let given = uuid_of(write.cells.get(&entry.space));
        let stored = uuid_of(write.stored_space);
        if let (Some(given), Some(stored)) = (given, stored)
            && given != stored
            && !matches!(write.kind, WriteKind::Move { .. })
        {
            return Err(SealError::Refused(jazz::db::Error::new(
                ErrorCode::WriteRejected,
                "an encrypted row cannot change its space",
            )));
        }
        let encrypted: Vec<String> = write
            .cells
            .keys()
            .filter(|column| entry.columns.contains_key(*column))
            .cloned()
            .collect();
        if encrypted.is_empty() {
            return Ok(());
        }
        let space = given
            .or(stored)
            .expect("an encrypted write names its space");
        if !self.held.borrow().contains(&space) {
            return Err(SealError::NeedsKey {
                space: address(space),
            });
        }
        if self.rooted.borrow_mut().insert(space) {
            write.companion_rows.push((
                "space_roots".to_owned(),
                RowUuid(uuid::Uuid::new_v4()),
                BTreeMap::from([("space".to_owned(), Value::Uuid(space))]),
            ));
        }
        for column in encrypted {
            let plain = match write.cells.get(&column).cloned() {
                Some(Value::String(plain)) => plain.into_bytes(),
                Some(Value::Bytes(plain)) => plain,
                _ => {
                    return Err(SealError::Refused(jazz::db::Error::new(
                        ErrorCode::WriteRejected,
                        "an encrypted cell takes its plaintext type",
                    )));
                }
            };
            write
                .cells
                .insert(column.clone(), Value::Bytes(seal(space, &plain)));
            if entry.equality.contains(&column) {
                write.cells.insert(
                    format!("__e2ee_eq_{column}"),
                    Value::Bytes(token(&String::from_utf8_lossy(&plain))),
                );
            }
        }
        Ok(())
    }

    fn open_rows(
        &self,
        site: &ReadSite<'_>,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<OpenOutcome, jazz::db::Error> {
        self.opens.borrow_mut().push(site.strict);
        if self.failing.get() && self.declarations().tables.contains_key(site.table) {
            return Err(jazz::db::Error::new(
                ErrorCode::Storage,
                "the stand-in hook cannot open anything",
            ));
        }
        let mut outcome = OpenOutcome::default();
        let Some(entry) = self.declarations().tables.get(site.table) else {
            return Ok(outcome);
        };
        let mut kept = Vec::new();
        for mut row in rows.drain(..) {
            let mut readable = true;
            for column in entry.columns.keys() {
                if site.is_lens_default(&row, column) {
                    self.lens_defaults.set(self.lens_defaults.get() + 1);
                    row.set_cell(
                        column,
                        Some(Value::String(String::new())),
                        ValueType::String,
                    );
                    continue;
                }
                let cell = match row.cell(column) {
                    Some(Value::Bytes(cell)) => cell,
                    Some(Value::Nullable(Some(inner))) => match *inner {
                        Value::Bytes(cell) => cell,
                        _ => continue,
                    },
                    _ => continue,
                };
                let opened = match cell.starts_with(b"st") {
                    true => match self.open_streamed(&cell) {
                        Ok(plain) => {
                            row.set_cell(column, Some(Value::Bytes(plain)), ValueType::Bytes);
                            continue;
                        }
                        Err(why) => why,
                    },
                    false => self.open(&cell),
                };
                match opened {
                    Opened::Plain(plain)
                        if entry.columns[column].plain.column_type == ColumnType::Bytea =>
                    {
                        row.set_cell(column, Some(Value::Bytes(plain)), ValueType::Bytes)
                    }
                    Opened::Plain(plain) => match String::from_utf8(plain) {
                        Ok(plain) => {
                            row.set_cell(column, Some(Value::String(plain)), ValueType::String)
                        }
                        Err(_) => {
                            outcome.unreadable.push(Unreadable {
                                table: site.table.to_owned(),
                                id: Some(row.row_uuid()),
                                column: Some(column.clone()),
                                code: UnreadableCode::InvalidCiphertext,
                                space: None,
                            });
                            readable = false;
                        }
                    },
                    Opened::MissingKey(space) => {
                        outcome
                            .missing_epochs
                            .push((address(space), format!("epoch-{}", self.epoch.get())));
                        readable = false;
                    }
                    Opened::Invalid => {
                        outcome.unreadable.push(Unreadable {
                            table: site.table.to_owned(),
                            id: Some(row.row_uuid()),
                            column: Some(column.clone()),
                            code: UnreadableCode::InvalidCiphertext,
                            space: None,
                        });
                        readable = false;
                    }
                }
                row.remove_cell(&format!("__e2ee_eq_{column}"));
            }
            if readable {
                kept.push(row);
            }
        }
        *rows = kept;
        Ok(outcome)
    }

    fn plan_query(&self, query: &Query) -> Result<QueryPlan, jazz::db::Error> {
        self.plans.set(self.plans.get() + 1);
        if query.table != "notes" {
            return Ok(QueryPlan::Plain);
        }
        let searched = query.filters.iter().position(|filter| {
            matches!(filter, Predicate::Eq(Operand::Column(column), Operand::Literal(Value::String(_))) if column == "body")
        });
        let Some(searched) = searched else {
            return Ok(QueryPlan::Plain);
        };
        let Predicate::Eq(_, Operand::Literal(Value::String(plain))) = &query.filters[searched]
        else {
            unreachable!()
        };
        let space = query.filters.iter().find_map(|filter| match filter {
            Predicate::Eq(Operand::Column(column), Operand::Literal(Value::Uuid(space)))
                if column == "projectId" =>
            {
                Some(*space)
            }
            _ => None,
        });
        let Some(space) = space else {
            return Err(jazz::db::Error::new(
                ErrorCode::Query,
                "an encrypted filter needs its space",
            ));
        };
        if !self.held.borrow().contains(&space) {
            return Ok(QueryPlan::NeedsKey {
                space: address(space),
            });
        }
        if self.leak.get() == 2 {
            return Ok(QueryPlan::Plain);
        }
        let mut physical = query.clone();
        if self.leak.get() == 3 {
            physical.includes.push(jazz::query::Include {
                path: "projectId".to_owned(),
                join_mode: jazz::query::JoinMode::Holes,
                require: false,
            });
        }
        if self.leak.get() == 1 {
            // The filter stays as written.
        } else if self.offline.get() {
            physical.filters.remove(searched);
        } else {
            physical.filters[searched] = Predicate::In(
                Operand::Column("__e2ee_eq_body".to_owned()),
                vec![Operand::Literal(Value::Bytes(token(plain)))],
            );
        }
        if !self.keep_physical_window.get() {
            physical.limit = None;
            physical.offset = 0;
        }
        Ok(QueryPlan::Rewritten {
            physical: Box::new(physical),
            residual: Residual {
                preds: if self.omit_residual_predicates.get() {
                    Vec::new()
                } else {
                    vec![ResidualPred {
                        column: "body".to_owned(),
                        any_of: vec![Value::String(plain.clone())],
                    }]
                },
                limit: query.limit,
                offset: query.offset,
            },
            epochs: EpochSetId([self.epoch.get(); 32]),
            incomplete: None,
        })
    }

    fn seal_stream(&self, target: &StreamTarget<'_>) -> Result<Box<dyn StreamSealer>, SealError> {
        self.streams.set(self.streams.get() + 1);
        let Some(space) = uuid_of(target.space) else {
            return Err(SealError::Refused(jazz::db::Error::new(
                ErrorCode::Query,
                "a streamed value needs its space",
            )));
        };
        if !self.held.borrow().contains(&space) {
            return Err(SealError::NeedsKey {
                space: address(space),
            });
        }
        Ok(Box::new(FakeSealer { space }))
    }
}

fn identity(node: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorSubject::SYSTEM,
    }
}

fn client(hooks: &Rc<Fake>) -> Db {
    block_on(Db::open(
        DbConfig::new(schema(), identity(1)).with_e2ee(hooks.clone()),
    ))
    .unwrap()
}

fn client_at(hooks: &Rc<Fake>, path: &Path) -> Db {
    client_as(hooks, path, AuthorSubject::SYSTEM)
}

/// A durable client at `path` whose writes are `author`'s.
fn client_as(hooks: &Rc<Fake>, path: &Path, author: AuthorSubject) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author,
            },
        )
        .with_e2ee(hooks.clone()),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn local() -> ReadOpts {
    ReadOpts::for_read_tier(ReadTier::LocalOnly)
}

fn note(space: uuid::Uuid, body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("projectId".to_owned(), Value::Uuid(space)),
        ("title".to_owned(), Value::String("title".to_owned())),
        ("body".to_owned(), Value::String(body.to_owned())),
    ])
}

fn bodies(rows: &[CurrentRow]) -> Vec<Value> {
    rows.iter().map(|row| row.cell("body").unwrap()).collect()
}

fn all(db: &Db, table: &str) -> Vec<CurrentRow> {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&query, local())).unwrap()
}

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
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

/// `client` connected to `server` as SYSTEM; every frame either sends.
fn link(client: &Db, server: &Db) -> (jazz::db::Upstream, Rc<RefCell<Vec<u8>>>) {
    link_as(client, server, AuthorSubject::SYSTEM)
}

/// `client` connected to `server`, which serves it as `author`.
fn link_as(
    client: &Db,
    server: &Db,
    author: AuthorSubject,
) -> (jazz::db::Upstream, Rc<RefCell<Vec<u8>>>) {
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
            author,
        )
        .unwrap();
    (link, wire)
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..10 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

/// Run `future` while `client` and `server` exchange what it waits for.
fn drive<T>(server: &Db, client: &Db, future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..100 {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    panic!("the operation did not finish within 100 turns");
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Every cell of every table of the SQLite file at `path`, as bytes.
fn stored_bytes(path: &Path) -> Vec<u8> {
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
                match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Text(bytes)
                    | rusqlite::types::ValueRef::Blob(bytes) => out.extend_from_slice(bytes),
                    _ => {}
                }
            }
        }
    }
    out
}

/// Contract: every way alice writes an encrypted column (a write, a
/// callback transaction, a staged mergeable handle, a handle transaction)
/// stores and uploads only sealed cells and tokens, and her equality query
/// reaches the server as a token filter. She reads plaintext back.
///
/// ```text
/// alice ──seal──► her store ──upload──► server
///   ▲                                      │
///   └──────────open◄── her reads           └── stores ciphertext
/// ```
#[test]
fn plaintext_reaches_neither_storage_nor_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let hooks = Fake::new();
    let alice = client_at(&hooks, &path);
    let server = block_on(Db::open(DbConfig::new(schema(), identity(0)))).unwrap();
    server.set_authority().unwrap();
    let (_link, wire) = link(&alice, &server);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);

    let written = block_on(alice.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    block_on(alice.update(
        "notes",
        written,
        BTreeMap::from([("body".to_owned(), text(&format!("{SECRET}-updated")))]),
        Default::default(),
    ))
    .unwrap();
    block_on(alice.transaction(async |tx| {
        tx.insert(
            "notes",
            note(space, &format!("{SECRET}-callback")),
            Default::default(),
        )
        .await
    }))
    .unwrap();
    let handle = block_on(alice.mergeable_tx()).unwrap();
    block_on(handle.insert(
        "notes",
        note(space, &format!("{SECRET}-handle")),
        Default::default(),
    ))
    .unwrap();
    block_on(handle.commit()).unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    block_on(alice.tx_ref(id).insert(
        "notes",
        note(space, &format!("{SECRET}-staged")),
        Default::default(),
    ))
    .unwrap();
    block_on(alice.commit_mergeable_handle(id)).unwrap();

    let searched = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text(&format!("{SECRET}-handle")))));
    let prepared = alice.prepare_query(&searched).unwrap();
    let _live = block_on(alice.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &alice);

    let mut read = bodies(&all(&alice, "notes"));
    read.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        read,
        [
            text(&format!("{SECRET}-callback")),
            text(&format!("{SECRET}-handle")),
            text(&format!("{SECRET}-staged")),
            text(&format!("{SECRET}-updated")),
        ]
    );
    assert_eq!(
        bodies(&block_on(alice.all(&prepared, local())).unwrap()),
        [text(&format!("{SECRET}-handle"))]
    );
    // The server holds what alice uploaded: sealed cells, one root.
    let stored = all(&server, "notes");
    assert_eq!(stored.len(), 4);
    for row in &stored {
        let Some(Value::Bytes(cell)) = row.cell("body") else {
            panic!("the server stores a sealed cell");
        };
        assert!(cell.starts_with(b"ct") && !contains(&cell, SECRET.as_bytes()));
        let Some(Value::Nullable(Some(token))) = row.cell("__e2ee_eq_body") else {
            panic!("the server stores the equality token");
        };
        assert!(matches!(*token, Value::Bytes(_)));
    }
    assert_eq!(all(&server, "space_roots").len(), 1);
    let wire = wire.borrow();
    assert!(contains(&wire, b"__e2ee_eq_body"), "the query went up");
    assert!(!contains(&wire, SECRET.as_bytes()), "plaintext on the wire");
    drop(alice);
    assert!(
        !contains(&stored_bytes(&path), SECRET.as_bytes()),
        "plaintext in alice's store"
    );
}

/// Contract: a write that needs a key alice does not hold is refused on
/// the call itself, applies nothing, and says which space it needs; the
/// same call succeeds once the key is held, its space's root staged in the
/// same commit.
#[test]
fn a_write_without_its_key_is_refused_and_applies_nothing() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    let error =
        block_on(alice.insert("notes", note(space, SECRET), Default::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(error.message.starts_with("key-pending"));
    assert_eq!(
        E2eeRefusal::of(&error),
        Some(E2eeRefusal::NeedsKey {
            space: address(space)
        })
    );
    assert_eq!(error.reason, Some(Reason::KeyPending));
    assert_eq!(error.space, Some(address(space)), "the space is a field");
    assert!(all(&alice, "notes").is_empty());
    assert!(all(&alice, "space_roots").is_empty());

    hooks.hold(space);
    block_on(alice.insert("notes", note(space, SECRET), Default::default())).unwrap();
    let notes = all(&alice, "notes");
    let roots = all(&alice, "space_roots");
    assert_eq!(bodies(&notes), [text(SECRET)]);
    assert_eq!(roots.len(), 1);
    assert_eq!(notes[0].seq(), roots[0].seq(), "one commit");
    // A delete needs no key, nor an update of plaintext columns only.
    hooks.forget(space);
    block_on(alice.update(
        "notes",
        notes[0].row_uuid(),
        BTreeMap::from([("title".to_owned(), text("renamed"))]),
        Default::default(),
    ))
    .unwrap();
    block_on(alice.delete("notes", notes[0].row_uuid(), Default::default())).unwrap();
}

/// Contract: in a handle transaction the refusal surfaces on the staging
/// call, which can be repeated once the key is held; the transaction goes
/// on and commits. A staged write that fails leaves none of the internal
/// rows staged with it.
#[test]
fn a_handle_transaction_refuses_on_the_staging_call() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (first, second) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    let tx = alice.tx_ref(id);
    let row = RowUuid(uuid::Uuid::now_v7());
    let opts = || InsertOptions {
        row_id: Some(row),
        ..Default::default()
    };
    let error = block_on(tx.insert("notes", note(first, SECRET), opts())).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(matches!(
        E2eeRefusal::of(&error),
        Some(E2eeRefusal::NeedsKey { space }) if space == address(first)
    ));
    hooks.hold(first);
    block_on(tx.insert("notes", note(first, SECRET), opts())).unwrap();
    // The same row id again, in a fresh space: its root is staged, then
    // the insert fails, and the root goes with it.
    hooks.hold(second);
    let error = block_on(tx.insert("notes", note(second, SECRET), opts())).unwrap_err();
    assert_eq!(E2eeRefusal::of(&error), None);
    block_on(alice.commit_serializable_handle(id)).unwrap();
    assert_eq!(bodies(&all(&alice, "notes")), [text(SECRET)]);
    let roots = all(&alice, "space_roots");
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].cell("space"), Some(Value::Uuid(first)));
}

/// Contract: an update or upsert hands the hooks the space of the row it
/// changes; an ordinary write that changes an encrypted row's space is
/// refused, and only a move, in a serializable transaction, makes it.
#[test]
fn only_a_move_changes_an_encrypted_row_s_space() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (from, to) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(from);
    hooks.hold(to);
    let row = block_on(alice.insert("notes", note(from, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    block_on(alice.update(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), text("edited"))]),
        Default::default(),
    ))
    .unwrap();
    block_on(alice.upsert(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), text("upserted"))]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        hooks.writes.borrow()[1..],
        [
            (WriteKind::Update, Some(Value::Uuid(from))),
            (WriteKind::Upsert, Some(Value::Uuid(from))),
        ]
    );
    let error =
        block_on(alice.update("notes", row, note(to, "moved"), Default::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);

    let mergeable = OpenTransactionId::new();
    block_on(alice.begin_mergeable(mergeable)).unwrap();
    let refused = block_on(alice.tx_ref(mergeable).move_to_space(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), text("moved"))]),
        Value::Uuid(to),
        Default::default(),
    ));
    assert_eq!(refused.unwrap_err().code, ErrorCode::Query);
    alice.abandon_transaction_handle(mergeable).unwrap();

    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    block_on(alice.tx_ref(id).move_to_space(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), text("moved"))]),
        Value::Uuid(to),
        Default::default(),
    ))
    .unwrap();
    block_on(alice.commit_serializable_handle(id)).unwrap();
    assert_eq!(
        hooks.writes.borrow().last().unwrap().0,
        WriteKind::Move {
            target_space: Value::Uuid(to)
        }
    );
    let rows = all(&alice, "notes");
    assert_eq!(rows[0].cell("projectId"), Some(Value::Uuid(to)));
    assert_eq!(rows[0].cell("body"), Some(text("moved")));
    // Sealed under the new space: without its key the row is unreadable.
    hooks.forget(to);
    assert!(all(&alice, "notes").is_empty());
}

/// Contract: an ordinary read leaves out the rows the hooks cannot open
/// (a key alice lacks, a cell mallory wrote unsealed) and returns the rest;
/// a strict read (inside a transaction, or the row a write is built on)
/// fails instead, and a missing key says which space it needs.
#[test]
fn strict_reads_fail_rather_than_leave_rows_out() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (kept, lost) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(kept);
    hooks.hold(lost);
    block_on(alice.insert("notes", note(kept, "kept"), Default::default())).unwrap();
    block_on(alice.insert("notes", note(lost, "lost"), Default::default())).unwrap();
    hooks.forget(lost);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        bodies(&block_on(alice.all(&query, local())).unwrap()),
        [text("kept")]
    );

    let error = block_on(alice.all_strict(&query, local())).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert_eq!(
        E2eeRefusal::of(&error),
        Some(E2eeRefusal::NeedsKey {
            space: address(lost)
        })
    );
    let in_callback = block_on(alice.transaction(async |tx| tx.all_prepared(&query).await));
    assert!(in_callback.is_err());
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    assert!(block_on(alice.tx_ref(id).all_prepared(&query)).is_err());
    alice.abandon_transaction_handle(id).unwrap();

    // Mallory's unsealed cell: left out of ordinary reads, fatal to strict.
    hooks.hold(lost);
    hooks.hostile.set(true);
    let mut forged = note(kept, "forged");
    forged.insert("body".to_owned(), Value::Bytes(b"junk".to_vec()));
    block_on(alice.insert("notes", forged, Default::default())).unwrap();
    hooks.hostile.set(false);
    let mut read = bodies(&block_on(alice.all(&query, local())).unwrap());
    read.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(read, [text("kept"), text("lost")]);
    let error = block_on(alice.all_strict(&query, local())).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert_eq!(E2eeRefusal::of(&error), None);
    let opens = hooks.opens.borrow();
    assert!(opens.contains(&true) && opens.contains(&false));
}

fn added(event: Option<SubscriptionEvent>) -> Vec<Value> {
    match event {
        Some(SubscriptionEvent::Delta { added, .. }) => {
            added.iter().map(|row| row.cell("body").unwrap()).collect()
        }
        other => panic!("expected a delta, got {other:?}"),
    }
}

/// Contract: a subscription's opening and deltas are opened rows; a row
/// whose key alice lacks is left out, and arrives as an addition once the
/// key is held and the runtime refreshes the reads of its table.
#[test]
fn subscriptions_open_rows_and_refresh_when_a_key_arrives() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (held, late) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(held);
    hooks.hold(late);
    block_on(alice.insert("notes", note(held, "first"), Default::default())).unwrap();
    block_on(alice.insert("notes", note(late, "late"), Default::default())).unwrap();
    hooks.forget(late);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(alice.subscribe(&query, local())).unwrap();
    assert_eq!(added(stream.try_next_event()), [text("first")]);
    block_on(alice.insert("notes", note(held, "second"), Default::default())).unwrap();
    assert_eq!(added(stream.try_next_event()), [text("second")]);

    hooks.hold(late);
    assert!(stream.try_next_event().is_none());
    alice.refresh_reads_of(&["notes"]).unwrap();
    assert_eq!(added(stream.try_next_event()), [text("late")]);
}

/// Contract: an equality filter on an encrypted column is planned by the
/// hooks: the prepared query compares tokens, its window applies after the
/// rows are opened and compared (in a live result too), a new held epoch
/// makes a new query, and a filter on a space without a key is refused on
/// preparation.
#[test]
fn equality_filters_are_planned_into_token_filters() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (space, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(space);
    for body in ["x", "y", "x"] {
        block_on(alice.insert("notes", note(space, body), Default::default())).unwrap();
    }
    let searched = |space: uuid::Uuid| {
        Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text("x"))))
    };
    let prepared = alice.prepare_query(&searched(space)).unwrap();
    assert!(!format!("{:?}", prepared.query()).contains("String(\"x\")"));
    assert_eq!(prepared.e2ee_epochs(), Some(EpochSetId([0; 32])));
    assert_eq!(
        bodies(&block_on(alice.all(&prepared, local())).unwrap()),
        [text("x"), text("x")]
    );
    let first = alice.prepare_query(&searched(space).limit(1)).unwrap();
    assert_eq!(
        bodies(&block_on(alice.all(&first, local())).unwrap()),
        [text("x")]
    );
    // A live result keeps the window too: a new match past it changes
    // nothing.
    let mut live = block_on(alice.subscribe(&first, local())).unwrap();
    assert_eq!(added(live.try_next_event()), [text("x")]);
    block_on(alice.insert("notes", note(space, "x"), Default::default())).unwrap();
    let past_window = match live.try_next_event() {
        None => Vec::new(),
        event => added(event),
    };
    assert!(past_window.is_empty(), "{past_window:?}");

    hooks.epoch.set(1);
    let rotated = alice.prepare_query(&searched(space)).unwrap();
    assert_ne!(rotated.shape(), prepared.shape());

    let error = alice.prepare_query(&searched(other)).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert_eq!(
        E2eeRefusal::of(&error),
        Some(E2eeRefusal::NeedsKey {
            space: address(other)
        })
    );
}

/// Contract: an encrypted column is sealed and opened whole: a splice, a
/// range read or a streamed value of it is refused, and nothing is stored.
#[test]
fn encrypted_columns_are_only_read_and_written_whole() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let row = block_on(alice.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    let splice = block_on(alice.splice_value(
        "notes",
        row,
        "body",
        jazz::db::LargeValueUpdateSplice {
            at: 0,
            delete: 1,
            insert: b"x".to_vec(),
        },
        Default::default(),
    ));
    assert_eq!(splice.unwrap_err().code, ErrorCode::Query);
    let range = block_on(alice.read_value_range("notes", row, "body", 0..2));
    assert_eq!(range.unwrap_err().code, ErrorCode::Query);
    // Without a row, nothing names the stream's key: refused.
    let streamed = alice.begin_streaming_value_upload("notes", &BTreeMap::new(), "attachment");
    assert_eq!(
        streamed.err().map(|error| error.code),
        Some(ErrorCode::Query)
    );
    assert_eq!(bodies(&all(&alice, "notes")), [text(SECRET)]);
}

/// Contract: Alice's database returns a protocol error when asked to
/// serve as the authority with encryption hooks installed.
#[test]
fn a_database_with_hooks_cannot_become_the_authority() {
    let alice = client(&Fake::new());
    let error = alice.set_authority().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert_eq!(
        error.message,
        "a database with encryption hooks cannot serve as the authority"
    );
}

/// Contract: opening an authority store with encryption hooks returns a
/// protocol error to the server.
#[test]
fn the_authority_cannot_open_with_hooks() {
    let config = DbConfig::new(schema(), identity(0)).with_e2ee(Fake::new());
    let error = block_on(Db::open_history_complete(config))
        .err()
        .expect("authority open should be refused");
    assert_eq!(error.code, ErrorCode::Protocol);
    assert_eq!(
        error.message,
        "a database with encryption hooks cannot serve as the authority"
    );
}

fn root(space: uuid::Uuid) -> BTreeMap<String, Value> {
    BTreeMap::from([("space".to_owned(), Value::Uuid(space))])
}

fn authority() -> Db {
    let server = block_on(Db::open(DbConfig::new(schema(), identity(0)))).unwrap();
    server.set_authority().unwrap();
    server
}

/// Contract: a ledger read in alice's serializable transaction returns
/// each row with the position the authority accepted it at, none past the
/// common cut. Offline, a row of hers still pending fails the read rather
/// than be left out; a ledger read outside a serializable transaction, or
/// on a store that has never had an upstream, is refused.
#[test]
fn ledger_reads_return_accepted_positions_at_one_cut() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let hooks = Fake::new();
    let server = authority();
    let roots = Query::from("space_roots");
    {
        let alice = client_at(&hooks, &path);
        let _link = link(&alice, &server);
        for _ in 0..2 {
            block_on(alice.insert(
                "space_roots",
                root(uuid::Uuid::new_v4()),
                Default::default(),
            ))
            .unwrap();
        }
        pump(&server, &alice);
        let roots = alice.prepare_query(&roots).unwrap();
        let id = OpenTransactionId::new();
        block_on(alice.begin_serializable(id)).unwrap();
        let ledger = drive(&server, &alice, alice.tx_ref(id).ledger_read(&roots)).unwrap();
        assert_eq!(ledger.rows.len(), 2);
        for (row, at) in &ledger.rows {
            assert!(*at > 0 && *at <= ledger.through);
            assert_eq!(row.seq().seq, *at);
        }
        let server_positions: BTreeSet<u64> = all(&server, "space_roots")
            .iter()
            .map(|row| row.seq().seq)
            .collect();
        let positions: BTreeSet<u64> = ledger.rows.iter().map(|(_, at)| *at).collect();
        assert_eq!(positions, server_positions);
        alice.abandon_transaction_handle(id).unwrap();
        let id = OpenTransactionId::new();
        block_on(alice.begin_mergeable(id)).unwrap();
        let error = drive(&server, &alice, alice.tx_ref(id).ledger_read(&roots)).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
    }
    // Offline: her new row is pending.
    let alice = client_at(&hooks, &path);
    block_on(alice.insert(
        "space_roots",
        root(uuid::Uuid::new_v4()),
        Default::default(),
    ))
    .unwrap();
    let prepared = alice.prepare_query(&roots).unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    let error = block_on(alice.tx_ref(id).ledger_read(&prepared)).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);

    let fresh = client(&Fake::new());
    let prepared = fresh.prepare_query(&roots).unwrap();
    let id = OpenTransactionId::new();
    block_on(fresh.begin_serializable(id)).unwrap();
    let error = block_on(fresh.tx_ref(id).ledger_read(&prepared)).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
}

/// Contract: alice's provisional read marks each row accepted, at its
/// position, or hers and pending, by her commit; accepted rows come first.
/// A read through other tables is refused.
#[test]
fn provisional_reads_mark_own_pending_rows() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let server = authority();
    let _link = link(&alice, &server);
    let (accepted, pending) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    block_on(alice.insert("space_roots", root(accepted), Default::default())).unwrap();
    pump(&server, &alice);
    let written = block_on(alice.insert("space_roots", root(pending), Default::default())).unwrap();
    let roots = alice.prepare_query(&Query::from("space_roots")).unwrap();
    let read = block_on(alice.provisional_read(&roots)).unwrap();
    assert_eq!(read.owner, identity(1).node);
    assert_eq!(read.rows.len(), 2);
    let (first, at) = &read.rows[0];
    assert_eq!(first.cell("space"), Some(Value::Uuid(accepted)));
    assert!(matches!(at, Provisional::Accepted(p) if *p <= read.accepted_through));
    let (second, mark) = &read.rows[1];
    assert_eq!(second.cell("space"), Some(Value::Uuid(pending)));
    assert_eq!(*mark, Provisional::Own(written.mergeable_tx_id()));

    let joined = alice
        .prepare_query(&Query::from("notes").join_via_row_id("projects", "projectId", []))
        .unwrap();
    let error = block_on(alice.provisional_read(&joined)).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert!(error.message.contains("provisional read"));
}

/// Contract: the fate of each of alice's commits is kept across a restart:
/// pending until the authority answers, then accepted at its position or
/// refused (with the refusal while she keeps refusals); a commit the store
/// never made is unknown.
#[test]
fn transaction_fates_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let hooks = Fake::new();
    let server = authority();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let (accepted, refused, kept) = {
        let alice = client_as(&hooks, &path, author);
        alice.track_upstream_rejections();
        let _link = link_as(&alice, &server, author);
        let accepted = block_on(alice.insert(
            "space_roots",
            root(uuid::Uuid::new_v4()),
            Default::default(),
        ))
        .unwrap()
        .mergeable_tx_id();
        assert!(matches!(
            alice.transaction_fate(accepted).unwrap(),
            TransactionFate::Pending
        ));
        pump(&server, &alice);
        let refused =
            block_on(alice.insert("closed", root(uuid::Uuid::new_v4()), Default::default()))
                .unwrap()
                .mergeable_tx_id();
        pump(&server, &alice);
        let kept = match alice.transaction_fate(refused).unwrap() {
            TransactionFate::Refused(Some(error)) => error,
            other => panic!("expected a kept refusal, got {other:?}"),
        };
        (accepted, refused, kept)
    };
    let alice = client_as(&hooks, &path, author);
    let server_position = all(&server, "space_roots")[0].seq().seq;
    assert!(matches!(
        alice.transaction_fate(accepted).unwrap(),
        TransactionFate::Accepted(at) if at == server_position
    ));
    match alice.transaction_fate(refused).unwrap() {
        TransactionFate::Refused(Some(error)) => assert_eq!(error.message, kept.message),
        other => panic!("expected the kept refusal, got {other:?}"),
    }
    alice.discard_rejected_transaction(refused).unwrap();
    assert!(matches!(
        alice.transaction_fate(refused).unwrap(),
        TransactionFate::Refused(None)
    ));
    assert!(matches!(
        alice.transaction_fate(refused + 1_000).unwrap(),
        TransactionFate::Unknown
    ));
}

/// `schema()` before `notes` had `body` and its tokens.
fn without_body() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Text)
                    .nullable_column("attachment", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("noteId", "notes")
                    .column("text", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("vaults")
                    .nullable_column("secret", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("space_roots")
                    .column("space", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("closed")
                    .column("space", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
}

/// The migration that adds `body`, giving the rows stored before it the
/// empty value, and its tokens.
fn add_body() -> MigrationLens {
    MigrationLens::new(
        without_body().version_id(),
        schema().version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![LensOp::AddColumn {
                column: "body".into(),
                default: Value::Bytes(Vec::new()),
            }],
        }],
    )
    .unwrap()
}

/// Contract: a row stored before a migration added an encrypted column
/// reaches the hooks marked as holding the migration's value for it, so
/// they read it as never written; a sealed cell is not marked.
#[test]
fn rows_older_than_an_encrypted_column_are_marked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let space = uuid::Uuid::new_v4();
    {
        let before = block_on(Db::open_at(
            DbConfig::new(without_body(), identity(1)),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        let mut old = note(space, "");
        old.remove("body");
        block_on(before.insert("notes", old, Default::default())).unwrap();
    }
    let hooks = Fake::new();
    hooks.hold(space);
    let alice = block_on(Db::open_at(
        DbConfig::new(schema(), identity(1))
            .with_migrations(vec![without_body()], vec![add_body()])
            .with_e2ee(hooks.clone()),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    block_on(alice.insert("notes", note(space, SECRET), Default::default())).unwrap();
    let mut read = bodies(&all(&alice, "notes"));
    read.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(read, [text(""), text(SECRET)]);
    assert_eq!(hooks.lens_defaults.get(), 1);
}

/// A source that counts the reads made of it.
struct Counted<'a> {
    bytes: &'a [u8],
    reads: &'a Cell<usize>,
}

impl std::io::Read for Counted<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.reads.set(self.reads.get() + 1);
        let n = out.len().min(self.bytes.len());
        out[..n].copy_from_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(n)
    }
}

/// Contract: a value alice streams into an encrypted column is sealed as
/// it streams: the hooks choose its key before the first source byte is
/// read (without the key the stream is refused and nothing is read), the
/// chunker and the store see only sealed bytes, and the row's other cells
/// are sealed by the write as usual. It reads back whole.
#[test]
fn streamed_values_are_sealed_as_they_stream() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let hooks = Fake::new();
    let alice = client_at(&hooks, &path);
    let space = uuid::Uuid::new_v4();
    let value = SECRET.repeat(20_000).into_bytes();
    let reads = Cell::new(0);
    let row = RowUuid(uuid::Uuid::now_v7());
    let refused = block_on(alice.insert_streaming_value_with_id(
        "notes",
        row,
        note(space, SECRET),
        "attachment",
        Counted {
            bytes: &value,
            reads: &reads,
        },
    ));
    let error = refused.unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert_eq!(reads.get(), 0, "no source byte is read before the key");
    assert_eq!(
        E2eeRefusal::of(&error),
        Some(E2eeRefusal::NeedsKey {
            space: address(space)
        })
    );

    hooks.hold(space);
    block_on(alice.insert_streaming_value_with_id(
        "notes",
        row,
        note(space, SECRET),
        "attachment",
        Counted {
            bytes: &value,
            reads: &reads,
        },
    ))
    .unwrap();
    assert!(reads.get() > 1);
    assert_eq!(hooks.streams.get(), 2);
    let rows = all(&alice, "notes");
    assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    assert_eq!(rows[0].cell("attachment"), Some(Value::Bytes(value)));
    drop(alice);
    assert!(
        !contains(&stored_bytes(&path), SECRET.as_bytes()),
        "plaintext in alice's store"
    );
}

/// Contract: a relation read that nests rows of an encrypted table is
/// refused when it is prepared, naming the table: nested rows are not
/// opened, so alice reads them with a query of their own. Nesting a plain
/// table under an encrypted one stays allowed.
#[test]
fn relation_reads_nesting_encrypted_rows_are_refused() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let nested = Query::from("projects").array_subquery(ArraySubquery::new(
        "notes",
        "notes",
        "projectId",
        "id",
    ));
    let error = alice.prepare_query(&nested).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert_eq!(
        error.message,
        "Reading encrypted table `notes` nested in a relation read (`notes`) is not supported yet"
    );
    let plain = Query::from("notes").array_subquery(ArraySubquery::new(
        "project",
        "projects",
        "id",
        "projectId",
    ));
    alice.prepare_query(&plain).unwrap();
}

/// Contract: a value sealed as it streamed is left as it is only by the
/// one write that publishes it. Another write of the same column in the
/// same transaction, staged right after it, is sealed as usual. The column
/// a stream sealed travels with that write alone, so no other write can
/// see it whatever runs in between.
#[test]
fn only_the_streamed_write_leaves_its_column_unsealed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let hooks = Fake::new();
    let alice = client_at(&hooks, &path);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    let tx = alice.tx_ref(id);
    let streamed = RowUuid(uuid::Uuid::now_v7());
    let mut upload = alice
        .begin_streaming_value_upload_for(
            "notes",
            streamed,
            &note(space, "streamed"),
            "attachment",
            &jazz::db::WriteTarget::Root,
        )
        .unwrap();
    block_on(alice.push_streaming_value_upload(&mut upload, b"streamed bytes")).unwrap();
    block_on(tx.finish_streaming_value_upload(
        upload,
        jazz::db::StreamingMutationKind::Insert,
        "notes",
        streamed,
        note(space, "streamed"),
        "attachment",
        Default::default(),
        None,
        None,
        None,
    ))
    .unwrap();
    let mut written = note(space, "written");
    written.insert(
        "attachment".to_owned(),
        Value::Bytes(SECRET.as_bytes().to_vec()),
    );
    block_on(tx.insert("notes", written, Default::default())).unwrap();
    block_on(alice.commit_mergeable_handle(id)).unwrap();
    let mut attachments: Vec<Value> = all(&alice, "notes")
        .iter()
        .map(|row| row.cell("attachment").unwrap())
        .collect();
    attachments.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        attachments,
        [
            Value::Bytes(b"streamed bytes".to_vec()),
            Value::Bytes(SECRET.as_bytes().to_vec()),
        ]
    );
    drop(alice);
    assert!(
        !contains(&stored_bytes(&path), SECRET.as_bytes()),
        "plaintext in alice's store"
    );
}

/// Contract: a restore keeps an encrypted row in the space it was deleted
/// from. Restored content naming another space is refused, as an update's
/// would be; restored content in the same space is sealed and reads back.
#[test]
fn a_restore_cannot_move_an_encrypted_row() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (home, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(home);
    hooks.hold(other);
    let row = block_on(alice.insert("notes", note(home, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    block_on(alice.delete("notes", row, Default::default())).unwrap();
    let moved =
        block_on(alice.restore("notes", row, Some(note(other, "moved")), Default::default()));
    assert_eq!(moved.unwrap_err().code, ErrorCode::WriteRejected);
    assert!(all(&alice, "notes").is_empty());
    assert_eq!(
        hooks.writes.borrow().last().unwrap(),
        &(WriteKind::Restore, Some(Value::Uuid(home)))
    );
    block_on(alice.restore(
        "notes",
        row,
        Some(note(home, "restored")),
        Default::default(),
    ))
    .unwrap();
    let rows = all(&alice, "notes");
    assert_eq!(rows[0].cell("projectId"), Some(Value::Uuid(home)));
    assert_eq!(rows[0].cell("body"), Some(text("restored")));
}

/// Contract: a searched plaintext value never shows in the debug output of
/// the prepared query or of the hooks' plan, which logs and panics print.
#[test]
fn search_values_stay_out_of_debug_output() {
    const SEARCHED: &str = "SEARCHED-PLAINTEXT";
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let query = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text(SEARCHED))));
    let prepared = alice.prepare_query(&query).unwrap();
    assert!(!format!("{prepared:?}").contains(SEARCHED));
    let plan = hooks.plan_query(&query).unwrap();
    assert!(matches!(plan, QueryPlan::Rewritten { .. }));
    assert!(!format!("{plan:?}").contains(SEARCHED));
}

/// Contract: the row a binding builds a mutation on is read strictly: a row
/// whose key alice no longer holds fails the read instead of reading as
/// absent, so the binding never builds a write on a row it could not see.
#[test]
fn a_binding_reads_the_row_it_mutates_strictly() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let row = block_on(alice.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    let binding = jazz::binding_host::Binding::new(alice, identity(1), false);
    assert!(binding.local_current_row("notes", row.0.as_bytes()).is_ok());
    hooks.forget(space);
    assert!(
        binding
            .local_current_row("notes", row.0.as_bytes())
            .is_err()
    );
}

/// Contract: a provisional read never leaves out a row it cannot open:
/// replay must not rest on skipped rows, so it fails instead.
#[test]
fn a_provisional_read_fails_rather_than_leave_rows_out() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let server = authority();
    let _link = link(&alice, &server);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    block_on(alice.insert("notes", note(space, SECRET), Default::default())).unwrap();
    pump(&server, &alice);
    let notes = alice.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(alice.provisional_read(&notes)).unwrap().rows.len(),
        1
    );
    hooks.forget(space);
    let error = block_on(alice.provisional_read(&notes)).unwrap_err();
    assert_eq!(error.reason, Some(Reason::KeyPending));
}

/// Contract: when the hooks fail to open a read, the read fails with their
/// error; it never returns the rows as if the hooks had left them out.
#[test]
fn a_failing_read_hook_fails_the_read() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    block_on(alice.insert("notes", note(space, SECRET), Default::default())).unwrap();
    hooks.failing.set(true);
    let notes = alice.prepare_query(&Query::from("notes")).unwrap();
    let error = block_on(alice.all(&notes, local())).unwrap_err();
    assert_eq!(error.code, ErrorCode::Storage);
    assert_eq!(error.message, "the stand-in hook cannot open anything");
    let subscribed = block_on(alice.subscribe(&notes, local()));
    assert_eq!(
        subscribed.err().map(|error| error.code),
        Some(ErrorCode::Storage)
    );
}

/// Contract: only a read's own rows are opened, so a read that would return
/// cells of an encrypted table any other way is refused when it is
/// prepared, naming the table: an include of an encrypted row, a flat join
/// over an encrypted table (whose cells are named by source, the root's
/// included), and the history of one. The same reads of a
/// plain table stay allowed.
#[test]
fn reads_that_would_return_unopened_encrypted_rows_are_refused() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let note_id = uuid::Uuid::new_v4();
    for (query, message) in [
        (
            Query::from("comments").include("noteId"),
            "Reading encrypted table `notes` through an include (`noteId`) is not supported yet",
        ),
        (
            Query::from("comments").flat_join("notes", "comments.noteId", "notes.id"),
            "Reading encrypted table `notes` in a flat join is not supported yet",
        ),
        (
            Query::from("history.notes").filter(eq(col("id"), lit(Value::Uuid(note_id)))),
            "Reading the history of encrypted table `notes` is not supported yet",
        ),
    ] {
        let error = alice.prepare_query(&query).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert_eq!(error.message, message);
    }
    alice
        .prepare_query(&Query::from("notes").include("projectId"))
        .unwrap();
    let rooted = Query::from("notes").flat_join("projects", "notes.projectId", "projects.id");
    assert_eq!(
        alice.prepare_query(&rooted).unwrap_err().message,
        "Reading encrypted table `notes` in a flat join is not supported yet"
    );
    alice
        .prepare_query(&Query::from("projects").flat_join(
            "space_roots",
            "projects.id",
            "space_roots.space",
        ))
        .unwrap();
    alice
        .prepare_query(
            &Query::from("history.projects").filter(eq(col("id"), lit(Value::Uuid(note_id)))),
        )
        .unwrap();
}

/// Contract: core refuses, from the declarations alone, every read shape
/// on an encrypted column except a top-level equality on an equality
/// column, which the hook plans: aggregates, ORDER BY, ranges, IN, other
/// comparisons, and an equality nested in Any, Not or All. The error names
/// the table and column; the hook is never asked.
#[test]
fn reads_that_would_compare_or_return_sealed_cells_are_refused() {
    use jazz::query::{Aggregate, AggregateQuery, OrderBy, OrderDirection};
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    let in_space = || eq(col("projectId"), lit(Value::Uuid(space)));
    let plain = || Operand::Literal(text("x"));
    let body = || Operand::Column("body".to_owned());
    let nested = |predicate: Predicate| Query::from("notes").filter(in_space()).filter(predicate);
    let mut max = Query::from("notes").filter(in_space());
    max.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::max("body")])));
    let mut grouped = Query::from("notes");
    grouped.aggregate = Some(Box::new(AggregateQuery {
        aggregates: vec![Aggregate::count()],
        group_by: Some("body".to_owned()),
    }));
    let mut ordered = Query::from("notes").filter(in_space());
    ordered.order_by.push(OrderBy {
        column: "body".to_owned(),
        direction: OrderDirection::Asc,
    });
    let mut ordered_attachment = Query::from("vaults");
    ordered_attachment.order_by.push(OrderBy {
        column: "secret".to_owned(),
        direction: OrderDirection::Asc,
    });
    let refused = [
        (max, "notes.body"),
        (grouped, "notes.body"),
        (ordered, "notes.body"),
        (ordered_attachment, "vaults.secret"),
        (nested(Predicate::Gt(body(), plain())), "notes.body"),
        (nested(Predicate::Lte(body(), plain())), "notes.body"),
        (nested(Predicate::In(body(), vec![plain()])), "notes.body"),
        (nested(Predicate::Ne(body(), plain())), "notes.body"),
        (nested(Predicate::Contains(body(), plain())), "notes.body"),
        (nested(Predicate::IsNull(body())), "notes.body"),
        (nested(Predicate::Eq(plain(), body())), "notes.body"),
        (
            nested(Predicate::Any(vec![Predicate::Eq(body(), plain())])),
            "notes.body",
        ),
        (
            nested(Predicate::Not(Box::new(Predicate::Eq(body(), plain())))),
            "notes.body",
        ),
        (
            nested(Predicate::All(vec![Predicate::Eq(body(), plain())])),
            "notes.body",
        ),
        // An encrypted column with no equality index has no plannable shape.
        (
            nested(Predicate::Eq(
                Operand::Column("attachment".to_owned()),
                plain(),
            )),
            "notes.attachment",
        ),
    ];
    for (query, column) in refused {
        let plans = hooks.plans.get();
        let error = alice.prepare_query(&query).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query, "{query:?}");
        assert!(
            error
                .message
                .starts_with(&format!("Reading encrypted column `{column}`"))
                && error.message.ends_with("is not supported yet"),
            "{query:?}: {}",
            error.message
        );
        assert_eq!(hooks.plans.get(), plans, "the hook was not asked");
    }
    // Counting rows names no column; an equality the hook plans is allowed;
    // the same shapes on a plain column of the table stay allowed.
    let mut count = Query::from("notes").filter(in_space());
    count.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::count()])));
    alice.prepare_query(&count).unwrap();
    hooks.held.borrow_mut().insert(space);
    alice
        .prepare_query(&nested(eq(col("body"), lit(text("x")))))
        .unwrap();
    let mut by_title = Query::from("notes").filter(in_space());
    by_title.order_by.push(OrderBy {
        column: "title".to_owned(),
        direction: OrderDirection::Asc,
    });
    alice.prepare_query(&by_title).unwrap();
}

/// Contract: a plain branch of an app that also has an encrypting version
/// (the current version leads from a plain one, and carries no system
/// tables) opens as a plain app, with no hooks.
#[test]
fn a_plain_branch_after_an_encrypting_version_opens_without_hooks() {
    use jazz::tools::AppId;
    use jazz::tools::deployment::artifact::load_client_encrypted;
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "done": "boolean", "extra": "boolean?" } } },
        "history": {
            "v1": { "tables": { "notes": { "columns": { "done": "boolean" } } } },
            "v2": { "tables": { "notes": {
                "columns": { "done": "boolean", "title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [
            { "from": "v1", "migrate": { "notes": { "extra": { "add": "boolean?", "default": null } } } }] }"#;
    let (schema, _, declarations) =
        load_client_encrypted(file.as_bytes(), AppId::from_name("plain"), "test").unwrap();
    assert!(declarations.is_none(), "the current version is plain");
    let config = DbConfig::new(jazz::schema::JazzSchema::new(&schema).unwrap(), identity(1));
    block_on(jazz::binding_host::open_memory(config)).unwrap();
}

/// Hooks that leave `built_from_encrypted_app` at its default: they count
/// as built from an encrypted app when their declarations name a table.
struct Bare(E2eeDeclarations);

impl E2eeHooks for Bare {
    fn declarations(&self) -> &E2eeDeclarations {
        &self.0
    }

    fn seal_write(&self, _: &mut WriteCells<'_>) -> Result<(), SealError> {
        Ok(())
    }

    fn open_rows(
        &self,
        _: &ReadSite<'_>,
        _: &mut Vec<CurrentRow>,
    ) -> Result<OpenOutcome, jazz::db::Error> {
        Ok(OpenOutcome::default())
    }

    fn plan_query(&self, _: &Query) -> Result<QueryPlan, jazz::db::Error> {
        Ok(QueryPlan::Plain)
    }

    fn seal_stream(&self, _: &StreamTarget<'_>) -> Result<Box<dyn StreamSealer>, SealError> {
        Err(SealError::Refused(jazz::db::Error::new(
            ErrorCode::Query,
            "no streams",
        )))
    }
}

/// Contract: hooks that do not say whether they were built from an encrypted
/// app are taken to be when their declarations name an encrypted table, and
/// not otherwise: so with no declared table they are refused on a schema
/// holding Jazz's system tables, and with one they open it.
#[test]
fn hooks_default_to_built_from_an_encrypted_app_when_they_declare_a_table() {
    use jazz::tools::AppId;
    use jazz::tools::deployment::artifact::load_client_encrypted;
    let app = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {
            "projects": { "columns": { "name": "string" } },
            "notes": {
                "columns": { "projectId": "uuid", "body": "bytes", "__e2ee_eq_body": "bytes" },
                "relations": { "project": { "rel": "projects", "column": "projectId" } },
                "encryption": { "space": "projectId", "scope": "projects",
                    "columns": { "body": "string" }, "equality": ["body"] }
            }
        } }"#;
    let (schema, _, declarations) =
        load_client_encrypted(app.as_bytes(), AppId::from_name("bare"), "test").unwrap();
    let declarations = declarations.expect("the app declares encryption");
    let config = || DbConfig::new(jazz::schema::JazzSchema::new(&schema).unwrap(), identity(1));
    let nothing = E2eeDeclarations {
        tables: BTreeMap::new(),
        ..declarations.clone()
    };
    let error = block_on(jazz::binding_host::open_memory(
        config().with_e2ee(Rc::new(Bare(nothing))),
    ))
    .err()
    .expect("refused");
    assert!(
        error.message.contains("declares end-to-end encryption"),
        "{}",
        error.message
    );
    block_on(jazz::binding_host::open_memory(
        config().with_e2ee(Rc::new(Bare(declarations))),
    ))
    .unwrap();
}

/// A union query of `arms` over `notes`, each `(label, predicate)` reading
/// the whole table under its predicate, ordered by `order` when given.
fn union_of(arms: Vec<(&str, jazz::query::RelationPredicate)>, order: Option<&str>) -> Query {
    use jazz::query::{
        RelationExpr, RelationOrderBy, RelationProjectColumn, RelationProjectExpr, RelationQuery,
        RelationRowIdRef, RelationUnionArm,
    };
    let mut rel = RelationExpr::Union {
        inputs: arms
            .into_iter()
            .map(|(label, predicate)| RelationUnionArm {
                label: label.to_owned(),
                input: RelationExpr::Project {
                    input: Box::new(RelationExpr::Filter {
                        input: Box::new(RelationExpr::TableScan {
                            table: "notes".to_owned(),
                            alias: None,
                        }),
                        predicate,
                    }),
                    columns: std::iter::once(RelationProjectColumn {
                        alias: "id".to_owned(),
                        expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                    })
                    .chain(
                        ["projectId", "title", "body", "__e2ee_eq_body", "attachment"].map(
                            |column| RelationProjectColumn {
                                alias: column.to_owned(),
                                expr: RelationProjectExpr::Column(jazz::query::RelationColumnRef {
                                    scope: Some("notes".to_owned()),
                                    column: column.to_owned(),
                                }),
                            },
                        ),
                    )
                    .collect(),
                },
            })
            .collect(),
    };
    if let Some(column) = order {
        rel = RelationExpr::OrderBy {
            input: Box::new(rel),
            terms: vec![RelationOrderBy {
                column: jazz::query::RelationColumnRef {
                    scope: Some("notes".to_owned()),
                    column: column.to_owned(),
                },
                direction: jazz::query::OrderDirection::Asc,
            }],
        };
    }
    let mut query = Query::from("notes");
    query.relation = Some(RelationQuery { rel });
    query
}

/// Contract: a union relation query is read like any other: every
/// condition in each of its arms that names an encrypted column is refused
/// (the plaintext literal would go upstream, or a comparison would run on
/// sealed cells), as is ordering the union by one; a union on plain columns
/// of an encrypted table still prepares.
#[test]
fn union_arms_on_encrypted_columns_are_refused() {
    use jazz::query::{RelationCmpOp, RelationPredicate, RelationValueRef};
    let hooks = Fake::new();
    let alice = client(&hooks);
    let scoped = |column: &str| jazz::query::RelationColumnRef {
        scope: Some("notes".to_owned()),
        column: column.to_owned(),
    };
    let cmp = |column: &str, op: RelationCmpOp, value: &str| RelationPredicate::Cmp {
        left: scoped(column),
        op,
        right: RelationValueRef::Literal(serde_json::json!(value)),
    };
    let refused = [
        union_of(
            vec![("a", cmp("body", RelationCmpOp::Eq, "UNION-SECRET"))],
            None,
        ),
        union_of(
            vec![
                ("a", cmp("title", RelationCmpOp::Eq, "t")),
                ("b", cmp("body", RelationCmpOp::Ne, "UNION-SECRET")),
            ],
            None,
        ),
        union_of(
            vec![(
                "a",
                RelationPredicate::Not(Box::new(cmp("body", RelationCmpOp::Eq, "x"))),
            )],
            None,
        ),
        union_of(
            vec![("a", cmp("title", RelationCmpOp::Eq, "t"))],
            Some("body"),
        ),
    ];
    for query in refused {
        let error = alice.prepare_query(&query).unwrap_err();
        assert!(
            error
                .message
                .starts_with("Reading encrypted column `notes.body`")
                && error.message.ends_with("is not supported yet"),
            "{query:?}: {}",
            error.message
        );
    }
    alice
        .prepare_query(&union_of(
            vec![
                ("a", cmp("title", RelationCmpOp::Eq, "t")),
                ("b", cmp("title", RelationCmpOp::Ne, "u")),
            ],
            Some("title"),
        ))
        .unwrap();
}

/// Contract: a query the hook returns may differ from the one it was given
/// only in its conditions and the window it takes over: one that also adds
/// an include (or anything else) is refused.
#[test]
fn a_hook_cannot_return_a_query_that_changes_more_than_its_conditions() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let query = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text("x"))));
    alice.prepare_query(&query).unwrap();
    hooks.leak.set(3);
    let error = alice.prepare_query(&query).unwrap_err();
    assert_eq!(
        error.message,
        "the encryption hooks returned a query that changes more than its conditions"
    );
}

/// Contract: Alice's client refuses a hook's physical LIMIT or OFFSET when
/// rows still need to pass a filter after decryption.
#[test]
fn a_hook_cannot_window_rows_before_residual_filtering() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    hooks.offline.set(true);
    hooks.keep_physical_window.set(true);
    let query = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text("x"))));

    alice.prepare_query(&query).unwrap();
    for windowed in [query.clone().limit(1), query.offset(1)] {
        let error = alice.prepare_query(&windowed).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert_eq!(
            error.message,
            "Applying LIMIT or OFFSET before post-decryption filtering on `notes` is not supported yet"
        );
    }
}

/// Contract: Alice's client refuses a hook that applies LIMIT or OFFSET to
/// both the physical query and the residual, even with no residual predicates.
#[test]
fn a_hook_cannot_window_rows_twice_without_residual_predicates() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    hooks.omit_residual_predicates.set(true);
    let query = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text("x"))));

    for windowed in [query.clone().limit(1), query.offset(1)] {
        hooks.keep_physical_window.set(false);
        alice.prepare_query(&windowed).unwrap();

        hooks.keep_physical_window.set(true);
        let error = alice.prepare_query(&windowed).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert_eq!(
            error.message,
            "Applying LIMIT or OFFSET before post-decryption filtering on `notes` is not supported yet"
        );
    }
}

/// Contract: hooks are not installed on a schema that gives an encrypted
/// column a stored default, which would be stored as plaintext where its
/// ciphertext belongs.
#[test]
fn a_stored_default_on_an_encrypted_column_refuses_the_hooks() {
    let hooks = Fake::new();
    let with_default = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Text)
                    .column_with_default(
                        "body",
                        ColumnType::Bytea,
                        jazz::tools::public_schema::Value::Bytea(b"plain".to_vec()),
                    )
                    .nullable_column("__e2ee_eq_body", ColumnType::Bytea)
                    .nullable_column("attachment", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let error = block_on(Db::open(
        DbConfig::new(with_default, identity(1)).with_e2ee(hooks.clone()),
    ))
    .err()
    .expect("refused");
    assert_eq!(
        error.message,
        "encrypted column `notes.body` has a stored default, which would be stored as plaintext \
         where its ciphertext belongs"
    );
    client(&hooks);
}

/// Contract: an application's query never names a reserved token column
/// (`__e2ee_eq_*`): in a filter, an ordering, a selection or a union arm,
/// its literal would go upstream as written. The hook's own token filter
/// in the query it returns is accepted: an ordinary equality on `body`
/// still prepares.
#[test]
fn an_application_query_cannot_name_a_token_column() {
    use jazz::query::{
        OrderBy, OrderDirection, RelationCmpOp, RelationPredicate, RelationValueRef,
    };
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let in_space = || eq(col("projectId"), lit(Value::Uuid(space)));
    let mut ordered = Query::from("notes").filter(in_space());
    ordered.order_by.push(OrderBy {
        column: "__e2ee_eq_body".to_owned(),
        direction: OrderDirection::Asc,
    });
    let mut selected = Query::from("notes").filter(in_space());
    selected.select = Some(vec!["__e2ee_eq_body".to_owned()]);
    let arm = union_of(
        vec![(
            "a",
            RelationPredicate::Cmp {
                left: jazz::query::RelationColumnRef {
                    scope: Some("notes".to_owned()),
                    column: "__e2ee_eq_body".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::Literal(serde_json::json!("x")),
            },
        )],
        None,
    );
    for query in [
        Query::from("notes").filter(eq(col("__e2ee_eq_body"), lit(text("x")))),
        Query::from("notes").filter(Predicate::Ne(
            Operand::Column("__e2ee_eq_body".to_owned()),
            lit(text("x")),
        )),
        ordered,
        selected,
        arm,
    ] {
        let error = alice.prepare_query(&query).unwrap_err();
        assert!(
            error.message.contains("__e2ee_eq_body")
                && error.message.ends_with("not supported yet")
                || error.message.starts_with("Reading reserved column"),
            "{query:?}: {}",
            error.message
        );
    }
    alice
        .prepare_query(
            &Query::from("notes")
                .filter(in_space())
                .filter(eq(col("body"), lit(text("x")))),
        )
        .unwrap();
}

/// Contract: JSON and array columns are encrypted columns like any other
/// to the refusal walk: a top-level equality on an equality-indexed one of
/// the query's own table is left to the hook, and every other condition on
/// them is refused before the hook is asked.
#[test]
fn json_and_array_columns_are_refused_like_any_encrypted_column() {
    use jazz::query::{OrderBy, OrderDirection};
    let mut declared = declarations();
    let notes = declared.tables.get_mut("notes").unwrap();
    notes.columns.insert(
        "meta".to_owned(),
        encrypted(ColumnType::Json { schema: None }, true),
    );
    notes.columns.insert(
        "tags".to_owned(),
        encrypted(
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
            true,
        ),
    );
    notes
        .equality
        .extend(["meta".to_owned(), "tags".to_owned()]);
    let hooks = Rc::new(Fake {
        declarations: Some(declared),
        ..Fake::default()
    });
    let alice = client(&hooks);
    let value = || Operand::Literal(text("x"));
    let on = |column: &str| Operand::Column(column.to_owned());
    for column in ["meta", "tags"] {
        let asked = hooks.plans.get();
        // The one plannable shape reaches the hook (which here plans
        // nothing for it, and its answer is then refused).
        let _ = alice.prepare_query(&Query::from("notes").filter(eq(col(column), lit(text("x")))));
        assert_eq!(hooks.plans.get(), asked + 1, "{column}: the hook is asked");

        let mut ordered = Query::from("notes");
        ordered.order_by.push(OrderBy {
            column: column.to_owned(),
            direction: OrderDirection::Asc,
        });
        for query in [
            Query::from("notes").filter(Predicate::Contains(on(column), value())),
            Query::from("notes").filter(Predicate::Ne(on(column), value())),
            Query::from("notes").filter(Predicate::IsNull(on(column))),
            Query::from("notes").filter(Predicate::In(on(column), vec![value()])),
            Query::from("notes").filter(Predicate::Any(vec![Predicate::Eq(on(column), value())])),
            Query::from("projects").join_via(
                "notes",
                "projectId",
                [Predicate::Eq(on(column), value())],
            ),
            ordered,
        ] {
            let asked = hooks.plans.get();
            let error = alice.prepare_query(&query).unwrap_err();
            assert!(
                error
                    .message
                    .contains(&format!("encrypted column `notes.{column}`")),
                "{query:?}: {}",
                error.message
            );
            assert_eq!(hooks.plans.get(), asked, "{column}: the hook is not asked");
        }
    }
}

/// Contract: a condition on an encrypted column is refused wherever a
/// query runs one, not only on the query's own table: in a join, a nested
/// join, a correlation, a join's `on` column, reachability and a policy
/// branch. Each would send the plaintext literal upstream or compare
/// sealed cells. The same conditions on a plain column stay allowed.
#[test]
fn conditions_of_joins_and_reachability_on_encrypted_columns_are_refused() {
    use jazz::query::{JoinCorrelation, PolicyBranch};
    let hooks = Fake::new();
    let alice = client(&hooks);
    let secret = || eq(col("body"), lit(text("SECRET-JOIN")));
    let plain = || eq(col("title"), lit(text("t")));
    let nested = |filter: Predicate| {
        Query::from("projects").join_via_with_nested_joins(
            "comments",
            "noteId",
            [],
            Query::from("x")
                .join_via("notes", "projectId", [filter])
                .joins,
        )
    };
    let mut branch = Query::from("projects");
    branch.policy_branches.push(PolicyBranch {
        filters: Vec::new(),
        joins: Query::from("x")
            .join_via("notes", "projectId", [secret()])
            .joins,
        reachable: Vec::new(),
        inherits: Vec::new(),
    });
    let refused = [
        (
            Query::from("projects").join_via("notes", "projectId", [secret()]),
            "notes.body",
        ),
        (nested(secret()), "notes.body"),
        (
            Query::from("projects").join_via_column_with_correlations(
                "notes",
                "projectId",
                "id",
                [JoinCorrelation {
                    join_column: "body".to_owned(),
                    source_column: "name".to_owned(),
                }],
                [],
            ),
            "notes.body",
        ),
        (
            Query::from("projects").join_via("notes", "body", []),
            "notes.body",
        ),
        (
            Query::from("projects").reachable_via_with_access_filters(
                "notes",
                "projectId",
                "projectId",
                Operand::Claim("team".to_owned()),
                [secret()],
                "projects",
                "id",
                "id",
                [],
            ),
            "notes.body",
        ),
        (branch, "notes.body"),
        (
            Query::from("notes").reachable_via(
                "projects",
                "id",
                "id",
                Operand::Column("body".to_owned()),
                "projects",
                "id",
                "id",
                [],
            ),
            "notes.body",
        ),
        (
            Query::from("notes")
                .array_subquery(ArraySubquery::new("mine", "comments", "noteId", "body")),
            "notes.body",
        ),
    ];
    for (query, column) in refused {
        let error = alice.prepare_query(&query).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query, "{query:?}");
        assert!(
            error
                .message
                .starts_with(&format!("Reading encrypted column `{column}`"))
                && error.message.ends_with("is not supported yet"),
            "{query:?}: {}",
            error.message
        );
    }
    alice
        .prepare_query(&Query::from("projects").join_via("notes", "projectId", [plain()]))
        .unwrap();
}

/// Contract: whatever a hook returns, a query that still filters an
/// encrypted column is refused, so the plaintext never goes upstream: a
/// rewritten query that kept the filter, and a plan that is no plan at all.
#[test]
fn a_hook_cannot_return_a_query_that_still_filters_an_encrypted_column() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let query = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text("SECRET-LEAK"))));
    alice.prepare_query(&query).unwrap();
    for leak in [1, 2] {
        hooks.leak.set(leak);
        let error = alice.prepare_query(&query).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query, "leak {leak}");
        assert!(
            error
                .message
                .starts_with("Reading encrypted column `notes.body`"),
            "leak {leak}: {}",
            error.message
        );
    }
}

/// Contract: an app that declares encryption (its schema holds Jazz's
/// system tables) opens through the bindings' open only when encryption
/// hooks built from its declarations are installed on that same open;
/// without hooks, or with plain hooks that declare nothing, it is refused,
/// and a database it opens seals its writes. A version that declares
/// nothing but keeps the tables is the same app: refused without hooks,
/// opened with hooks built from its (empty) declarations, which still
/// cannot name a token column.
#[test]
fn an_encrypted_app_opens_only_with_encryption_hooks() {
    use jazz::tools::AppId;
    use jazz::tools::deployment::artifact::load_client_encrypted;
    let app = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {
            "projects": { "columns": { "name": "string" } },
            "space_roots": { "columns": { "space": "uuid" } },
            "notes": {
                "columns": { "projectId": "uuid", "title": "string", "body": "bytes",
                    "__e2ee_eq_body": "bytes" },
                "relations": { "project": { "rel": "projects", "column": "projectId" } },
                "encryption": { "space": "projectId", "scope": "projects",
                    "columns": { "body": "string" },
                    "equality": ["body"] }
            }
        } }"#;
    let (schema, _, declarations) =
        load_client_encrypted(app.as_bytes(), AppId::from_name("hooks"), "test").unwrap();
    let declarations = declarations.expect("the app declares encryption");
    let config = || DbConfig::new(jazz::schema::JazzSchema::new(&schema).unwrap(), identity(1));

    let refused = |config: DbConfig| {
        let error = block_on(jazz::binding_host::open_memory(config))
            .err()
            .expect("refused");
        assert_eq!(error.code, ErrorCode::Schema);
        assert!(
            error.message.contains("declares end-to-end encryption"),
            "{}",
            error.message
        );
    };
    refused(config());
    let empty = Rc::new(Fake {
        declarations: Some(E2eeDeclarations {
            tables: BTreeMap::new(),
            ..declarations.clone()
        }),
        ..Fake::default()
    });
    // Hooks that declare nothing and were not built from an encrypted app
    // are refused; built from one, they open the tables-only version.
    refused(config().with_e2ee(empty.clone()));
    empty.encrypted_app.set(true);
    let quiet = block_on(jazz::binding_host::open_memory(config().with_e2ee(empty))).unwrap();
    let token = Query::from("notes").filter(eq(col("__e2ee_eq_body"), lit(text("x"))));
    assert!(quiet.prepare_query(&token).is_err());
    quiet.prepare_query(&Query::from("notes")).unwrap();
    let path = tempfile::tempdir().unwrap();
    let error = block_on(jazz::binding_host::open_file(
        config(),
        &path.path().join("refused.sqlite"),
        FileOptions::default(),
    ))
    .err()
    .expect("refused");
    assert_eq!(error.code, ErrorCode::Schema);

    let hooks = Rc::new(Fake {
        declarations: Some(declarations),
        ..Fake::default()
    });
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let db = block_on(jazz::binding_host::open_memory(
        config().with_e2ee(hooks.clone()),
    ))
    .unwrap();
    block_on(db.insert("notes", note(space, SECRET), Default::default())).unwrap();
    assert_eq!(hooks.seals.get(), 1, "the write was sealed");
    assert_eq!(bodies(&all(&db, "notes")), [text(SECRET)]);
    block_on(jazz::binding_host::open_file(
        config().with_e2ee(hooks),
        &path.path().join("opened.sqlite"),
        FileOptions::default(),
    ))
    .unwrap();
}

/// The bodies a live result holds after its opening.
fn opened_bodies(stream: &mut jazz::db::SubscriptionStream) -> Vec<Value> {
    let mut held = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            held.extend(added.iter().map(|row| row.cell("body").unwrap()));
        }
    }
    held
}

/// Contract: live equality queries that differ only in what applies after
/// opening (their window, or the value they compare) each get their own
/// rows, the same as a one-shot read of the same query.
///
/// ```text
/// alice ──subscribe(body = x, limit 1)──► [x]
///       ──subscribe(body = x, limit 2)──► [x, x]   (not the first's [x])
///       ──subscribe(body = y)──────────► [y]
/// ```
#[test]
fn live_queries_differing_after_opening_keep_their_own_rows() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    for body in ["x", "x", "y"] {
        block_on(alice.insert("notes", note(space, body), Default::default())).unwrap();
    }
    let searched = |body: &str| {
        Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text(body))))
    };
    let queries = [
        searched("x").limit(1),
        searched("x").limit(2),
        searched("x"),
        searched("y"),
    ];
    let mut streams = Vec::new();
    for query in &queries {
        let prepared = alice.prepare_query(query).unwrap();
        streams.push((
            block_on(alice.subscribe(&prepared, local())).unwrap(),
            bodies(&block_on(alice.all(&prepared, local())).unwrap()),
        ));
    }
    let expected = [
        vec![text("x")],
        vec![text("x"), text("x")],
        vec![text("x"), text("x")],
        vec![text("y")],
    ];
    for ((stream, one_shot), expected) in streams.iter_mut().zip(expected) {
        assert_eq!(*one_shot, expected);
        assert_eq!(opened_bodies(stream), expected);
    }
}

/// Contract: a value sealed as it streamed is published only to the row,
/// target and space it was sealed for. Finishing it at another row, or with
/// cells naming another space, is refused and stores nothing, as a standalone
/// write and in a transaction; the matching finish publishes it.
#[test]
fn a_sealed_stream_is_published_only_where_it_was_sealed_for() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (home, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(home);
    hooks.hold(other);
    let (sealed_row, other_row) = (RowUuid(uuid::Uuid::now_v7()), RowUuid(uuid::Uuid::now_v7()));
    let begin = || {
        let mut upload = alice
            .begin_streaming_value_upload_for(
                "notes",
                sealed_row,
                &note(home, "body"),
                "attachment",
                &jazz::db::WriteTarget::Root,
            )
            .unwrap();
        block_on(alice.push_streaming_value_upload(&mut upload, b"sealed bytes")).unwrap();
        upload
    };
    let finish = |row: RowUuid, space: uuid::Uuid| {
        block_on(alice.finish_streaming_value_upload(
            begin(),
            jazz::db::StreamingMutationKind::Insert,
            "notes",
            row,
            note(space, "body"),
            "attachment",
            Default::default(),
            None,
            None,
            None,
        ))
    };
    assert_eq!(finish(other_row, home).unwrap_err().code, ErrorCode::Schema);
    assert_eq!(
        finish(sealed_row, other).unwrap_err().code,
        ErrorCode::Schema
    );
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    let staged = block_on(alice.tx_ref(id).finish_streaming_value_upload(
        begin(),
        jazz::db::StreamingMutationKind::Insert,
        "notes",
        other_row,
        note(home, "body"),
        "attachment",
        Default::default(),
        None,
        None,
        None,
    ));
    assert_eq!(staged.unwrap_err().code, ErrorCode::Schema);
    block_on(alice.commit_mergeable_handle(id)).unwrap();
    assert!(all(&alice, "notes").is_empty());

    finish(sealed_row, home).unwrap();
    let rows = all(&alice, "notes");
    assert_eq!(rows[0].row_uuid(), sealed_row);
    assert_eq!(
        rows[0].cell("attachment"),
        Some(Value::Bytes(b"sealed bytes".to_vec()))
    );
}

/// The digest of a residual comparing `body` with `value`, unwindowed, as
/// an unkeyed hash would give it: what a log reader could check a guess
/// against.
fn unkeyed_residual_digest(value: &str) -> [u8; 32] {
    let mut hasher = jazz_types::sha256::Hasher::new();
    hasher.update(b"jazz e2ee residual v1\0");
    let mut field = |bytes: &[u8]| {
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };
    field(&1u64.to_be_bytes());
    field(b"body");
    field(&postcard::to_allocvec(&vec![text(value)]).unwrap());
    field(&u64::MAX.to_be_bytes());
    field(&0u64.to_be_bytes());
    *hasher.finalize().as_bytes()
}

/// Contract: what tells rewritten queries apart is keyed per database, so
/// a log of one confirms no guessed search value: the prepared query's
/// debug output holds neither the value nor an unkeyed digest of it, and
/// the same query prepared by two databases gets two shapes. Within one
/// database, queries that differ only after opening (the compared value,
/// offline, where the physical query is the same, or the window) get
/// shapes of their own.
#[test]
fn rewritten_query_shapes_are_keyed_per_database() {
    const SEARCHED: &str = "GUESSABLE";
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    hooks.offline.set(true);
    let searched = |body: &str| {
        Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text(body))))
    };
    let prepared = alice.prepare_query(&searched(SEARCHED)).unwrap();
    let debug = format!("{prepared:?}");
    let digest = unkeyed_residual_digest(SEARCHED);
    assert!(!debug.contains(SEARCHED));
    assert!(!debug.contains(&format!("{digest:?}")));
    assert!(!debug.contains(&hex::encode(digest)));
    assert!(!debug.contains("residual_key"));

    let other = alice.prepare_query(&searched("OTHER")).unwrap();
    assert_eq!(
        other.query(),
        prepared.query(),
        "offline: one physical query"
    );
    assert_ne!(other.shape(), prepared.shape());
    let first = alice.prepare_query(&searched(SEARCHED).limit(1)).unwrap();
    assert_ne!(first.shape(), prepared.shape());

    let bob = client(&hooks);
    let elsewhere = bob.prepare_query(&searched(SEARCHED)).unwrap();
    assert_ne!(elsewhere.shape(), prepared.shape());
}

/// Begin a sealed upload of `bytes` into `attachment` of `row`, through the
/// transaction `tx` when given.
fn sealed_upload(
    db: &Db,
    tx: Option<OpenTransactionId>,
    row: RowUuid,
    cells: &BTreeMap<String, Value>,
    bytes: &[u8],
) -> Result<jazz::db::StreamingValueUpload, jazz::db::Error> {
    let target = jazz::db::WriteTarget::Root;
    let mut upload = match tx {
        Some(id) => db.tx_ref(id).begin_streaming_value_upload_for(
            "notes",
            row,
            cells,
            "attachment",
            &target,
        )?,
        None => db.begin_streaming_value_upload_for("notes", row, cells, "attachment", &target)?,
    };
    block_on(db.push_streaming_value_upload(&mut upload, bytes))?;
    Ok(upload)
}

/// Finish `upload` in transaction `tx` as an update of `row` with no other
/// cells: the row's space is the one the transaction sees.
fn finish_in(
    db: &Db,
    tx: OpenTransactionId,
    upload: jazz::db::StreamingValueUpload,
    row: RowUuid,
) -> Result<jazz::db::WriteHandle, jazz::db::Error> {
    block_on(db.tx_ref(tx).finish_streaming_value_upload(
        upload,
        jazz::db::StreamingMutationKind::Update,
        "notes",
        row,
        BTreeMap::new(),
        "attachment",
        Default::default(),
        None,
        None,
        None,
    ))
}

/// Contract: a sealed stream is bound to its row's space as the write that
/// publishes it finds it, the transaction's own writes included. Moved to
/// another space earlier in the same transaction, the row refuses the
/// stream sealed for its old space, and the value never lands; in a
/// transaction that leaves the row where it was, the stream publishes.
///
/// ```text
/// begin upload (row in A) ─► tx: move row to B ─► finish ✗ ─► commit: no value
/// begin upload (row in A) ─► tx: (row stays in A) ─► finish ✓ ─► value reads back
/// ```
#[test]
fn a_sealed_stream_is_checked_against_its_transaction() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (home, other) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    hooks.hold(home);
    hooks.hold(other);
    let row = block_on(alice.insert("notes", note(home, SECRET), Default::default()))
        .unwrap()
        .row_uuid();

    let upload = sealed_upload(&alice, None, row, &BTreeMap::new(), b"sealed for home").unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    block_on(alice.tx_ref(id).move_to_space(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), text("moved"))]),
        Value::Uuid(other),
        Default::default(),
    ))
    .unwrap();
    let error = finish_in(&alice, id, upload, row).unwrap_err();
    assert_eq!(error.code, ErrorCode::Schema);
    block_on(alice.commit_serializable_handle(id)).unwrap();
    let rows = all(&alice, "notes");
    assert_eq!(rows[0].cell("projectId"), Some(Value::Uuid(other)));
    assert_eq!(
        rows[0].cell("attachment"),
        Some(Value::Nullable(None)),
        "the value never lands"
    );

    let upload = sealed_upload(&alice, None, row, &BTreeMap::new(), b"sealed for other").unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    finish_in(&alice, id, upload, row).unwrap();
    block_on(alice.commit_mergeable_handle(id)).unwrap();
    assert_eq!(
        all(&alice, "notes")[0].cell("attachment"),
        Some(Value::Bytes(b"sealed for other".to_vec()))
    );
}

/// Contract: a stream begun in a transaction finds the space of a row the
/// same transaction inserted, so a value can be streamed into it, and it
/// publishes and opens once the transaction commits.
#[test]
fn a_stream_begun_in_a_transaction_sees_its_rows() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let space = uuid::Uuid::new_v4();
    hooks.hold(space);
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    let row = block_on(
        alice
            .tx_ref(id)
            .insert("notes", note(space, SECRET), Default::default()),
    )
    .unwrap()
    .row_uuid();
    assert!(sealed_upload(&alice, None, row, &BTreeMap::new(), b"no").is_err());
    let upload = sealed_upload(&alice, Some(id), row, &BTreeMap::new(), b"streamed in").unwrap();
    finish_in(&alice, id, upload, row).unwrap();
    block_on(alice.commit_mergeable_handle(id)).unwrap();
    let rows = all(&alice, "notes");
    assert_eq!(rows[0].cell("body"), Some(text(SECRET)));
    assert_eq!(
        rows[0].cell("attachment"),
        Some(Value::Bytes(b"streamed in".to_vec()))
    );
}

/// Contract: in a table whose rows are their own spaces, a value streamed
/// into a new row is sealed for that row's own space, and publishes and
/// opens, as a standalone write and in a transaction.
#[test]
fn a_stream_into_a_new_self_space_row_publishes() {
    let hooks = Fake::new();
    let alice = client(&hooks);
    let (standalone, staged) = (RowUuid(uuid::Uuid::now_v7()), RowUuid(uuid::Uuid::now_v7()));
    hooks.hold(standalone.0);
    hooks.hold(staged.0);
    block_on(alice.insert_streaming_value_with_id(
        "vaults",
        standalone,
        BTreeMap::new(),
        "secret",
        &b"standalone secret"[..],
    ))
    .unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_mergeable(id)).unwrap();
    let tx = alice.tx_ref(id);
    let mut upload = tx
        .begin_streaming_value_upload_for(
            "vaults",
            staged,
            &BTreeMap::new(),
            "secret",
            &jazz::db::WriteTarget::Root,
        )
        .unwrap();
    block_on(alice.push_streaming_value_upload(&mut upload, b"staged secret")).unwrap();
    block_on(tx.finish_streaming_value_upload(
        upload,
        jazz::db::StreamingMutationKind::Insert,
        "vaults",
        staged,
        BTreeMap::new(),
        "secret",
        Default::default(),
        None,
        None,
        None,
    ))
    .unwrap();
    block_on(alice.commit_mergeable_handle(id)).unwrap();
    let mut secrets: Vec<(RowUuid, Option<Value>)> = all(&alice, "vaults")
        .iter()
        .map(|row| (row.row_uuid(), row.cell("secret")))
        .collect();
    secrets.sort_by_key(|(row, _)| *row);
    let mut expected = vec![
        (
            standalone,
            Some(Value::Bytes(b"standalone secret".to_vec())),
        ),
        (staged, Some(Value::Bytes(b"staged secret".to_vec()))),
    ];
    expected.sort_by_key(|(row, _)| *row);
    assert_eq!(secrets, expected);
}
