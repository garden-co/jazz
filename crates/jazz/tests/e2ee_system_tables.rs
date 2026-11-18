//! Jazz's E2EE system tables under the policies Jazz derives for them
//! (E2EE protocol §1.2–§1.4): the fragment every encrypted app's schema
//! gains, `derive_e2ee_policies`, the write guard that keeps system rows
//! immutable for every writer, and the deployment's check that only Jazz
//! defines system tables.
//!
//! ```text
//! projects ◄──(scopeId = table_id('projects'), identifier = id)── __e2ee_members
//!    │  read:   M(projects)          (a member row of the project)
//!    │  update: ownerId = session    (the owner manages members)
//!    ▼
//! __e2ee_spaces, __e2ee_space_successors, __e2ee_space_deliveries
//! ```
//!
//! Writes are staged at the authority as each account's session, so the
//! derived policies decide them.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::{BTreeMap, HashMap};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, ReadOpts, RestoreOptions,
    UpdateOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::deployment::artifact::DeclaredEncryption;
use jazz::tools::deployment::artifact::system_tables::{
    self, MEMBERS, SPACE_DELIVERIES, SPACE_SUCCESSORS, SPACES, derive_e2ee_policies,
    with_e2ee_fragment,
};
use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_encrypted_deployment,
};
use jazz::tools::public_schema::{
    EncryptedColumn, EncryptionEntry, OperationPolicy, PlainType, SweepBy,
};
use jazz::tools::{
    CmpOp, ColumnType, OpenTransactionId, PolicyExpr, PolicyValue, Schema, SchemaBuilder,
    SchemaHash, TableName, TablePolicies, TableSchemaBuilder,
};

const DEVICE_REQUESTS: &str = system_tables::DEVICE_REQUESTS;
const DEVICE_KEYS: &str = system_tables::DEVICE_KEYS;

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec!["user".to_owned()])
}

fn owner_is_me() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "ownerId".to_owned(),
        op: CmpOp::Eq,
        value: me(),
    }
}

/// `M(projects)`: the session has a member row of this project.
fn member_of_this_project() -> PolicyExpr {
    let cmp = |column: &str, value: PolicyValue| PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    };
    PolicyExpr::Exists {
        table: MEMBERS.to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp(
                "scopeId",
                PolicyValue::TableIdentity {
                    table: "projects".to_owned(),
                },
            ),
            cmp(
                "identifier",
                PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), "id".to_owned()]),
            ),
            cmp("accountId", me()),
        ])),
    }
}

/// The app's own tables: projects are spaces, tasks encrypt their title
/// in their project's space. Boards are spaces too, still read by their
/// owner alone, before any column is encrypted in them (migrations §4.1).
fn app() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("boards")
                .column("ownerId", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(owner_is_me())
                        .with_insert(common::session_eq("$createdBy.account", &["user"]))
                        .with_update(Some(owner_is_me()), owner_is_me()),
                ),
        )
        .table(
            TableSchemaBuilder::new("projects")
                .column("ownerId", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(member_of_this_project())
                        .with_insert(common::session_eq("$createdBy.account", &["user"]))
                        .with_update(Some(owner_is_me()), owner_is_me())
                        .with_delete(owner_is_me()),
                ),
        )
        .table(
            TableSchemaBuilder::new("tasks")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Bytea)
                .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                    operation: jazz::tools::Operation::Select,
                    via_column: "projectId".to_owned(),
                    max_depth: None,
                })),
        )
        .build()
}

fn entries() -> BTreeMap<String, EncryptionEntry> {
    BTreeMap::from([
        (
            "boards".to_owned(),
            EncryptionEntry {
                space: "id".to_owned(),
                scope: "boards".to_owned(),
                columns: BTreeMap::new(),
                equality: Vec::new(),
                backfill: Vec::new(),
                rehome: None,
                sweep: SweepBy::Updaters,
            },
        ),
        (
            "tasks".to_owned(),
            EncryptionEntry {
                space: "projectId".to_owned(),
                scope: "projects".to_owned(),
                columns: BTreeMap::from([(
                    "title".to_owned(),
                    EncryptedColumn {
                        plain: PlainType {
                            column_type: ColumnType::Text,
                            nullable: false,
                            default: None,
                        },
                        sealed_type: None,
                        from: None,
                        to: None,
                        convert: None,
                        reverse: None,
                    },
                )]),
                equality: Vec::new(),
                backfill: Vec::new(),
                rehome: None,
                sweep: SweepBy::Updaters,
            },
        ),
    ])
}

/// The app's first version, before it turned encryption on: the same
/// tables without the system tables, and policies that need none.
fn first_version() -> JazzSchema {
    common::compile_schema(&first_version_public())
}

fn first_version_public() -> Schema {
    {
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("boards")
                    .column("ownerId", ColumnType::Uuid)
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("projects")
                    .column("ownerId", ColumnType::Uuid)
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("tasks")
                    .fk_column("projectId", "projects")
                    .column("title", ColumnType::Bytea)
                    .policies(common::allow_all_policies()),
            )
            .build()
    }
}

/// The server's or a client's config for the app whose encryption came
/// with a later version: it ships both versions and the lens between them.
fn late_config(node: NodeUuid, author: AuthorSubject) -> DbConfig {
    let (first, second) = (first_version(), schema_with(&late_settings()));
    let lens = jazz::protocol::MigrationLens::new(first.version_id(), second.version_id(), vec![])
        .unwrap();
    DbConfig::new(second, DbIdentity { node, author }).with_migrations(vec![first], vec![lens])
}

fn late_settings() -> jazz::tools::public_schema::EncryptionSettings {
    jazz::tools::public_schema::EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    }
}

/// The app's schema with the system tables and their derived policies.
fn schema() -> JazzSchema {
    schema_with(&Default::default())
}

/// [`schema`] under the app-wide encryption `settings`.
fn schema_with(settings: &jazz::tools::public_schema::EncryptionSettings) -> JazzSchema {
    common::compile_schema(&public_schema_with(settings))
}

fn public_schema_with(settings: &jazz::tools::public_schema::EncryptionSettings) -> Schema {
    let mut schema = with_e2ee_fragment(&app());
    for (table, policies) in derive_e2ee_policies(&schema, &entries(), settings).unwrap() {
        schema.get_mut(&table).unwrap().policies = policies;
    }
    schema
}

type Cells = BTreeMap<String, Value>;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn carol() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xc0; 16])
}

fn account(who: AuthorSubject) -> Value {
    Value::Uuid(who.account_id().expect("a test account").0)
}

fn new_row() -> RowUuid {
    RowUuid(uuid::Uuid::new_v4())
}

fn bytes(text: &str) -> Value {
    Value::Bytes(text.as_bytes().to_vec())
}

struct World {
    server: Db,
    projects: Value,
    replicas: std::cell::RefCell<HashMap<AuthorSubject, std::rc::Rc<Db>>>,
    streams: std::cell::RefCell<Vec<jazz::db::SubscriptionStream>>,
    next_node: std::cell::Cell<u8>,
    /// Whether the encryption fragment came with a later migration, so the
    /// system tables are stored under `name~hash`.
    late_fragment: bool,
    /// The bundle the replicas' sessions are pinned to, when the fragment
    /// was deployed to a running server, whose owner schema then lacks it.
    bundle: Option<jazz::catalogue::BundleId>,
    /// Whether writes are made on the writer's own replica and uploaded,
    /// not staged at the authority.
    uploading: std::cell::Cell<bool>,
}

impl World {
    fn new() -> Self {
        Self::with_settings(&Default::default())
    }

    fn with_settings(settings: &jazz::tools::public_schema::EncryptionSettings) -> Self {
        let server = block_on(Db::open_history_complete(DbConfig::new(
            schema_with(settings),
            DbIdentity {
                node: NodeUuid::from_bytes([0xee; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        Self::around(server, false)
    }

    /// An app that turned encryption on after its first version: the server
    /// holds the version without the fragment and the lens into the one
    /// with it, so the system tables are stored under `name~hash`.
    fn late_fragment() -> Self {
        let server = block_on(Db::open_history_complete(late_config(
            NodeUuid::from_bytes([0xee; 16]),
            AuthorSubject::SYSTEM,
        )))
        .unwrap();
        let world = Self::around(server, true);
        let (stored, _) = world.server.stored_column_for_test(MEMBERS, "scopeId");
        assert!(
            stored.contains('~'),
            "the fragment's tables are stored under later names: {stored}"
        );
        world
    }

    /// The same app, but the server opened on the version without the
    /// fragment and had the one with it deployed while running, through the
    /// server's own pipeline: the catalogue change admits the version and
    /// publishes its permissions as a bundle the replicas' sessions are
    /// pinned to, and the server's first schema stays its owner. Every
    /// write is then an upload from a replica.
    fn late_fragment_deployed() -> Self {
        use jazz::catalogue::CataloguePut;
        use jazz::tools::deployment::{DeploymentRequest, prepare_catalogue_change};
        use jazz::tools::public_schema::{SchemaHash, TablePolicies};
        let structural = |schema: &Schema| {
            let mut schema = schema.clone();
            for table in schema.values_mut() {
                table.policies = TablePolicies::default();
            }
            schema
        };
        let (first, second) = (first_version_public(), public_schema_with(&late_settings()));
        let (first_hash, second_hash) = (
            SchemaHash::compute(&structural(&first)),
            SchemaHash::compute(&structural(&second)),
        );
        let server = block_on(Db::open_history_complete(DbConfig::new(
            common::compile_schema(&structural(&first)),
            DbIdentity {
                node: NodeUuid::from_bytes([0xee; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let rows = server.catalogue().unwrap();
        let permissions: HashMap<_, _> = second
            .iter()
            .map(|(name, table)| (*name, table.policies.clone()))
            .collect();
        let declared = DeclaredEncryption {
            settings: late_settings(),
            versions: BTreeMap::from([(second_hash.to_string(), entries())]),
        };
        let (prepared, _) = prepare_encrypted_deployment(
            &DeploymentCatalogue {
                schemas: rows.schemas.iter().map(|s| s.schema.clone()).collect(),
                migrations: Vec::new(),
                ..Default::default()
            },
            DeploymentRequest {
                target_schema_hash: second_hash,
                schemas: vec![
                    (first_hash, structural(&first)),
                    (second_hash, structural(&second)),
                ],
                migrations: vec![jazz::tools::Lens::new(
                    first_hash,
                    second_hash,
                    jazz::tools::LensTransform {
                        ops: structural(&second)
                            .iter()
                            .filter(|(name, _)| system_tables::is_system_table(name.as_str()))
                            .map(|(name, table)| jazz::tools::LensOp::AddTable {
                                table: name.to_string(),
                                schema: table.clone(),
                            })
                            .collect(),
                        draft_ops: Vec::new(),
                    },
                )],
                // The app's own permissions: Jazz derives the system
                // tables' and the bundle carries both.
                permissions: permissions
                    .iter()
                    .filter(|(name, _)| !system_tables::is_system_table(name.as_str()))
                    .map(|(name, policies)| (*name, policies.clone()))
                    .collect(),
                allow_dropping_history: false,
            },
            &declared,
        )
        .unwrap();
        let permissions: jazz::catalogue::Permissions = permissions.into_iter().collect();
        let change = prepare_catalogue_change(
            &prepared,
            &rows,
            CataloguePut {
                schemas: prepared.schemas().iter().map(|s| (s.clone(), 1)).collect(),
                bundles: vec![(second_hash, permissions.clone(), 1)],
                ..Default::default()
            },
        )
        .unwrap();
        block_on(server.apply_catalogue(change)).unwrap();
        let mut world = Self::around(server, true);
        world.bundle = Some(jazz::catalogue::bundle_id(&second_hash, &permissions).unwrap());
        world.uploading.set(true);
        world
    }

    /// Tick the authority and every replica until nothing more moves.
    fn pump(&self) {
        for _ in 0..20 {
            for replica in self.replicas.borrow().values() {
                block_on(replica.tick()).unwrap();
            }
            block_on(self.server.tick()).unwrap();
        }
    }

    /// Whether the authority holds a live row `row` of `table`.
    fn holds(&self, table: &str, row: RowUuid) -> bool {
        if self.bundle.is_some() {
            return self.server.holds_row_for_test(table, row);
        }
        let prepared = self.server.prepare_query(&Query::from(table)).unwrap();
        block_on(self.server.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .any(|found| found.row_uuid() == row)
    }

    fn around(server: Db, late_fragment: bool) -> Self {
        let projects = Value::Uuid(server.table_identity("projects").unwrap().unwrap().0);
        Self {
            server,
            projects,
            replicas: Default::default(),
            streams: Default::default(),
            next_node: std::cell::Cell::new(1),
            late_fragment,
            bundle: None,
            uploading: Default::default(),
        }
    }

    /// Commit `writes` as one unit by `who`, staged at the authority as
    /// `who`'s session.
    fn commit(&self, who: AuthorSubject, writes: &[(&str, RowUuid, Cells)]) -> Result<(), String> {
        if !self.uploading.get() {
            return commit_on(&self.server, WriteIdentity::Session(who), writes);
        }
        // An upload is accepted if the authority ends up holding every row.
        let replica = self.replica(who);
        commit_on(&replica, WriteIdentity::Database, writes)?;
        self.pump();
        if writes.iter().all(|(table, row, _)| self.holds(table, *row)) {
            Ok(())
        } else {
            Err("the authority refused the upload".to_owned())
        }
    }

    fn address(&self, project: RowUuid) -> Cells {
        BTreeMap::from([
            ("scopeId".to_owned(), self.projects.clone()),
            ("identifier".to_owned(), Value::Uuid(project.0)),
        ])
    }

    fn member(&self, project: RowUuid, who: AuthorSubject) -> Cells {
        let mut cells = self.address(project);
        cells.insert("accountId".to_owned(), account(who));
        cells
    }

    fn root(&self, project: RowUuid, creator: AuthorSubject) -> Cells {
        let mut cells = self.address(project);
        cells.extend([
            ("creatorAccountId".to_owned(), account(creator)),
            ("deviceId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            (
                "accountEpochId".to_owned(),
                Value::Uuid(uuid::Uuid::new_v4()),
            ),
            ("epochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            ("mechanism".to_owned(), Value::String("m".to_owned())),
            ("version".to_owned(), Value::I64(1)),
            ("verification".to_owned(), bytes("v")),
            ("accountEnvelope".to_owned(), bytes("e")),
            ("signature".to_owned(), bytes("s")),
        ]);
        cells
    }

    fn successor(&self, project: RowUuid, author: AuthorSubject) -> Cells {
        let mut cells = self.address(project);
        cells.extend([
            ("predecessor".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            (
                "historyEpochId".to_owned(),
                Value::Uuid(uuid::Uuid::new_v4()),
            ),
            ("epochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            ("authorAccountId".to_owned(), account(author)),
            (
                "authorDeviceId".to_owned(),
                Value::Uuid(uuid::Uuid::new_v4()),
            ),
            (
                "authorEpochId".to_owned(),
                Value::Uuid(uuid::Uuid::new_v4()),
            ),
            ("verification".to_owned(), bytes("v")),
            ("history".to_owned(), bytes("h")),
            ("accountEnvelope".to_owned(), bytes("e")),
            ("signature".to_owned(), bytes("s")),
        ]);
        cells
    }

    fn delivery(&self, project: RowUuid, from: AuthorSubject, to: AuthorSubject) -> Cells {
        let mut cells = self.address(project);
        cells.extend([
            ("keyEpochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            ("senderAccountId".to_owned(), account(from)),
            ("recipientAccountId".to_owned(), account(to)),
            (
                "recipientAccountEpochId".to_owned(),
                Value::Uuid(uuid::Uuid::new_v4()),
            ),
            ("envelope".to_owned(), bytes("e")),
        ]);
        cells
    }

    /// `who`'s replica, connected to the authority as `who`, subscribed to
    /// the space tables.
    fn replica(&self, who: AuthorSubject) -> std::rc::Rc<Db> {
        if let Some(db) = self.replicas.borrow().get(&who) {
            return db.clone();
        }
        let node = self.next_node.get() + 1;
        self.next_node.set(node);
        let db = std::rc::Rc::new(
            block_on(Db::open(if self.late_fragment {
                late_config(NodeUuid::from_bytes([node; 16]), who)
            } else {
                DbConfig::new(
                    schema(),
                    DbIdentity {
                        node: NodeUuid::from_bytes([node; 16]),
                        author: who,
                    },
                )
            }))
            .unwrap(),
        );
        let (client_side, server_side) = duplex();
        block_on(db.connect_upstream(client_side));
        match self.bundle {
            Some(bundle) => {
                self.server
                    .accept_pinned_subscriber(server_side, who, None, Some(bundle))
                    .unwrap();
            }
            None => {
                self.server.accept_subscriber(server_side, who).unwrap();
            }
        }
        for table in [MEMBERS, SPACES, "projects"] {
            let prepared = db.prepare_query(&Query::from(table)).unwrap();
            let stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
            self.streams.borrow_mut().push(stream);
        }
        self.replicas.borrow_mut().insert(who, db.clone());
        db
    }

    /// The rows of `table` that reach `who`'s replica.
    fn visible(&self, who: AuthorSubject, table: &str) -> Vec<RowUuid> {
        let replica = self.replica(who);
        for _ in 0..10 {
            block_on(replica.tick()).unwrap();
            block_on(self.server.tick()).unwrap();
        }
        let prepared = replica.prepare_query(&Query::from(table)).unwrap();
        block_on(replica.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect()
    }

    fn system_insert(&self, table: &str, row: RowUuid, cells: Cells) {
        block_on(self.server.insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
    }
}

/// Commit `writes` as one unit on `db`, written as `identity`.
fn commit_on(
    db: &Db,
    identity: WriteIdentity,
    writes: &[(&str, RowUuid, Cells)],
) -> Result<(), String> {
    let id = OpenTransactionId::new();
    block_on(db.begin_mergeable(id)).unwrap();
    let tx = db.tx_ref(id);
    for (table, row, cells) in writes {
        let staged = block_on(tx.insert(
            table,
            cells.clone(),
            InsertOptions {
                row_id: Some(*row),
                identity,
                ..Default::default()
            },
        ));
        if let Err(error) = staged {
            let _ = db.abandon_transaction_handle(id);
            return Err(error.message);
        }
    }
    block_on(db.commit_mergeable_handle(id))
        .map(drop)
        .map_err(|error| error.message)
}

fn project(owner: AuthorSubject) -> Cells {
    BTreeMap::from([("ownerId".to_owned(), account(owner))])
}

/// A project's owner opens its space in one transaction (the project, her
/// own member row and the space root), then manages members; only members
/// read and deliver keys, and only the owner rotates.
///
/// ```text
/// alice ──{project, member(alice), root}──► authority ✓ (creator clause)
/// bob   ──member(bob)──────────────────────► ✗ (neither creator nor updater)
/// alice ──member(bob)──────────────────────► ✓ (updater)
/// bob   ──delivery to carol────────────────► ✗ (carol is no member)
/// bob   ──successor────────────────────────► ✗ (bob may not update the project)
/// ```
#[test]
fn derived_policies_admit_the_creator_and_updaters_and_refuse_everyone_else() {
    let world = World::new();
    let p = new_row();
    world
        .commit(
            alice(),
            &[
                ("projects", p, project(alice())),
                (MEMBERS, new_row(), world.member(p, alice())),
                (SPACES, new_row(), world.root(p, alice())),
            ],
        )
        .expect("the creator opens the space with its first member and root");

    world
        .commit(bob(), &[(MEMBERS, new_row(), world.member(p, bob()))])
        .expect_err("a non-member who may not update the project can't add itself");
    // Carol creates a project alice owns: the creator clause admits
    // carol's own member row in that transaction, and never again.
    let q = new_row();
    world
        .commit(
            carol(),
            &[
                ("projects", q, project(alice())),
                (MEMBERS, new_row(), world.member(q, carol())),
            ],
        )
        .expect("the creator adds itself while creating the project");
    world
        .commit(carol(), &[(MEMBERS, new_row(), world.member(q, carol()))])
        .expect_err("the creator clause holds only in the transaction creating the project");
    world
        .commit(alice(), &[(MEMBERS, new_row(), world.member(p, bob()))])
        .expect("the owner adds bob");

    assert_eq!(
        world.visible(bob(), SPACES).len(),
        1,
        "a member reads the root"
    );
    assert!(
        world.visible(carol(), SPACES).is_empty(),
        "a non-member reads no root"
    );
    assert_eq!(
        world.visible(carol(), MEMBERS).len(),
        1,
        "carol sees only her own member row, of the project she created"
    );

    world
        .commit(
            bob(),
            &[(
                SPACE_DELIVERIES,
                new_row(),
                world.delivery(p, bob(), carol()),
            )],
        )
        .expect_err("a delivery goes to members only");
    world
        .commit(
            bob(),
            &[(
                SPACE_DELIVERIES,
                new_row(),
                world.delivery(p, bob(), alice()),
            )],
        )
        .expect("a member delivers to a member");
    world
        .commit(
            bob(),
            &[(
                SPACE_DELIVERIES,
                new_row(),
                world.delivery(p, alice(), alice()),
            )],
        )
        .expect_err("the sender column is the session's account");
    world
        .commit(
            bob(),
            &[(SPACE_SUCCESSORS, new_row(), world.successor(p, bob()))],
        )
        .expect_err("a member who may not update the project can't rotate");
    world
        .commit(
            alice(),
            &[(SPACE_SUCCESSORS, new_row(), world.successor(p, alice()))],
        )
        .expect("the owner rotates");
}

/// No writer updates or restores a system row or deletes one other than a member row:
/// not a session, and not the system, which policies don't bind.
#[test]
fn system_rows_are_immutable_for_every_writer() {
    let world = World::new();
    let p = new_row();
    world.system_insert("projects", p, project(alice()));
    let member = new_row();
    world.system_insert(MEMBERS, member, world.member(p, alice()));
    let root = new_row();
    world.system_insert(SPACES, root, world.root(p, alice()));

    let update = block_on(world.server.update(
        MEMBERS,
        member,
        BTreeMap::from([("accountId".to_owned(), account(bob()))]),
        UpdateOptions::default(),
    ))
    .expect_err("the system can't update a member row");
    assert!(update.message.contains("never updated"), "{update:?}");
    let delete = block_on(world.server.delete(SPACES, root, DeleteOptions::default()))
        .expect_err("the system can't delete a space root");
    assert!(delete.message.contains("never deleted"), "{delete:?}");
    let session_update = block_on(world.server.update(
        SPACES,
        root,
        BTreeMap::from([("version".to_owned(), Value::I64(2))]),
        UpdateOptions {
            identity: WriteIdentity::Session(alice()),
            ..Default::default()
        },
    ))
    .expect_err("nor can a session");
    assert!(session_update.message.contains("never updated"));

    block_on(
        world
            .server
            .delete(MEMBERS, member, DeleteOptions::default()),
    )
    .expect("member rows are deleted under their policies, and the system bypasses them");

    // Nor is a deleted member row written back, with other content or as it was.
    for content in [
        Some(BTreeMap::from([("accountId".to_owned(), account(bob()))])),
        None,
    ] {
        let restore = block_on(world.server.restore(
            MEMBERS,
            member,
            content,
            RestoreOptions::default(),
        ))
        .expect_err("a deleted system row stays deleted");
        assert!(restore.message.contains("never restored"), "{restore:?}");
    }
}

/// A public device approval is admitted only with the private approval
/// that shares its row id, inserted in the same transaction (R8).
///
/// ```text
/// alice ──{private approval X, public approval X}──► ✓
/// alice ──{public approval Y} (no private Y)───────► ✗
/// alice ──{private approval Z}, later {public Z}───► ✗ (not this transaction)
/// ```
#[test]
fn a_public_approval_pairs_with_its_private_approval_by_row_id() {
    let world = World::new();
    let device = new_row();
    let mut request = BTreeMap::from([
        ("publicKey".to_owned(), bytes("pk")),
        ("mechanism".to_owned(), Value::String("m".to_owned())),
        ("version".to_owned(), Value::I64(1)),
        ("signingPublicKey".to_owned(), bytes("spk")),
        ("signingMechanism".to_owned(), Value::String("s".to_owned())),
        ("signingVersion".to_owned(), Value::I64(1)),
    ]);
    request.insert("nonceCommitment".to_owned(), bytes("n"));
    world
        .commit(alice(), &[(DEVICE_REQUESTS, device, request)])
        .expect("a device request");
    let challenge = new_row();
    world
        .commit(
            alice(),
            &[(
                system_tables::DEVICE_CHALLENGES,
                challenge,
                BTreeMap::from([
                    ("deviceId".to_owned(), Value::Uuid(device.0)),
                    ("epochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
                    ("envelope".to_owned(), bytes("e")),
                    ("nonce".to_owned(), bytes("n")),
                    ("signerId".to_owned(), Value::Uuid(device.0)),
                    ("signature".to_owned(), bytes("s")),
                ]),
            )],
        )
        .expect("a challenge");
    let private = || {
        BTreeMap::from([
            ("challengeId".to_owned(), Value::Uuid(challenge.0)),
            ("signerId".to_owned(), Value::Uuid(device.0)),
            ("verification".to_owned(), bytes("v")),
            ("signature".to_owned(), bytes("s")),
        ])
    };
    let public = || {
        BTreeMap::from([
            ("accountId".to_owned(), account(alice())),
            ("epochId".to_owned(), Value::Uuid(uuid::Uuid::new_v4())),
            ("deviceId".to_owned(), Value::Uuid(device.0)),
            ("signerId".to_owned(), Value::Uuid(device.0)),
            ("signature".to_owned(), bytes("s")),
        ])
    };
    let private_table = system_tables::DEVICE_APPROVALS;
    let public_table = system_tables::PUBLIC_DEVICE_APPROVALS;
    let x = new_row();
    world
        .commit(
            alice(),
            &[(private_table, x, private()), (public_table, x, public())],
        )
        .expect("the halves of one approval, together");
    world
        .commit(alice(), &[(public_table, new_row(), public())])
        .expect_err("a public approval without its private one");
    world
        .commit(
            alice(),
            &[
                (private_table, new_row(), private()),
                (public_table, new_row(), public()),
            ],
        )
        .expect_err("a public approval beside a private one of another id");
    let z = new_row();
    world
        .commit(alice(), &[(private_table, z, private())])
        .expect("a private approval alone");
    world
        .commit(alice(), &[(public_table, z, public())])
        .expect_err("its public half in a later transaction");
}

/// The account registry as a test sets it: which accounts a local-first
/// identity founded.
struct Founders(Vec<jazz::account_registry::AccountId>);

impl jazz::db::AccountRegistryView for Founders {
    fn founded_local_first(
        &self,
        account: jazz::account_registry::AccountId,
    ) -> Result<Option<bool>, String> {
        Ok(Some(self.0.contains(&account)))
    }
}

/// A local-first session: its account is the one its key founded.
fn lena() -> AuthorSubject {
    let subject = uuid::Uuid::from_bytes([0x1f; 16]).to_string();
    AuthorSubject::reserved(jazz::identity::DEVICE_ACCOUNT_ISSUER, &subject)
        .unwrap()
        .with_account(jazz::account_registry::device_account_id(
            uuid::Uuid::from_bytes([0xaa; 16]),
            &subject,
        ))
}

impl World {
    /// `who`'s device request, as the device id.
    fn device(&self, who: AuthorSubject) -> RowUuid {
        let device = new_row();
        self.commit(
            who,
            &[(
                DEVICE_REQUESTS,
                device,
                BTreeMap::from([
                    ("publicKey".to_owned(), bytes("pk")),
                    ("mechanism".to_owned(), Value::String("m".to_owned())),
                    ("version".to_owned(), Value::I64(1)),
                    ("signingPublicKey".to_owned(), bytes("spk")),
                    ("signingMechanism".to_owned(), Value::String("s".to_owned())),
                    ("signingVersion".to_owned(), Value::I64(1)),
                    ("nonceCommitment".to_owned(), bytes("n")),
                ]),
            )],
        )
        .expect("a device request");
        device
    }

    /// Insert `who`'s account identity of `generation` with its root of
    /// `kind`, in one transaction.
    fn identity(
        &self,
        who: AuthorSubject,
        device: RowUuid,
        generation: i64,
        kind: &str,
    ) -> Result<(), String> {
        let epoch = Value::Uuid(uuid::Uuid::new_v4());
        let id = if generation == 0 {
            RowUuid(who.account_id().unwrap().0)
        } else {
            new_row()
        };
        self.commit(
            who,
            &[
                (
                    system_tables::ACCOUNT_IDENTITIES,
                    id,
                    BTreeMap::from([
                        ("ledgerVersion".to_owned(), Value::I64(1)),
                        ("generation".to_owned(), Value::I64(generation)),
                        ("deviceId".to_owned(), Value::Uuid(device.0)),
                        ("epochId".to_owned(), epoch.clone()),
                        ("envelope".to_owned(), bytes("e")),
                        ("verification".to_owned(), bytes("v")),
                    ]),
                ),
                (
                    system_tables::ACCOUNT_ROOTS,
                    new_row(),
                    BTreeMap::from([
                        ("ledgerVersion".to_owned(), Value::I64(1)),
                        ("generation".to_owned(), Value::I64(generation)),
                        ("accountId".to_owned(), account(who)),
                        ("deviceId".to_owned(), Value::Uuid(device.0)),
                        ("epochId".to_owned(), epoch),
                        ("kind".to_owned(), Value::String(kind.to_owned())),
                    ]),
                ),
            ],
        )
    }
}

/// An account bootstraps at generation 0 with a root naming how it was
/// founded, and resets to the next generation only when the app allows
/// resets, the session isn't local-first and the account wasn't founded by
/// a local-first identity (R5, R6).
///
/// ```text
/// alice (external) ── gen 0 + root "app" ──► ✓     root "local-first" ✗
/// alice ── gen 1 ──► ✓ with allowAccountReset, ✗ without; gen 3 ✗ (no gen 2)
/// lena (local-first) ── gen 0 + root "local-first" ✓; gen 1 ✗
/// bob (external, account founded local-first) ── gen 1 ✗; root "app" ✗
/// ```
#[test]
fn account_identities_follow_the_founder_and_the_reset_setting() {
    let resets = jazz::tools::public_schema::EncryptionSettings {
        allow_account_reset: true,
        ..Default::default()
    };
    let world = World::with_settings(&resets);
    world
        .server
        .set_account_registry(std::rc::Rc::new(Founders(vec![
            bob().account_id().unwrap(),
            lena().account_id().unwrap(),
        ])));
    let device = world.device(alice());
    world
        .identity(alice(), device, 0, "local-first")
        .expect_err("a root's kind names how its account was founded");
    world
        .identity(alice(), device, 0, "app")
        .expect("an account bootstraps at generation 0");
    world
        .identity(alice(), device, 0, "app")
        .expect_err("an account has one first identity");
    world
        .identity(alice(), device, 2, "app")
        .expect_err("a reset follows the generation before it");
    world
        .identity(alice(), device, 1, "app")
        .expect("an account founded by its app resets");
    world
        .identity(alice(), device, 2, "app")
        .expect("and resets again");

    let device = world.device(lena());
    world
        .identity(lena(), device, 0, "local-first")
        .expect("a local-first account bootstraps with a local-first root");
    world
        .identity(lena(), device, 1, "local-first")
        .expect_err("a local-first session never resets");

    let device = world.device(bob());
    world
        .identity(bob(), device, 0, "app")
        .expect_err("bob's account was founded by a local-first identity");
    world
        .identity(bob(), device, 0, "local-first")
        .expect("so its root says so");
    world
        .identity(bob(), device, 1, "local-first")
        .expect_err("an account founded by a local-first identity never resets");

    let closed = World::new();
    let device = closed.device(alice());
    closed.identity(alice(), device, 0, "app").unwrap();
    closed
        .identity(alice(), device, 1, "app")
        .expect_err("no reset unless the app allows resets");
}

/// A device's public keys are the account's own request's keys (R4).
#[test]
fn device_keys_must_repeat_the_accounts_own_request() {
    let world = World::new();
    let request = new_row();
    let keys = BTreeMap::from([
        ("publicKey".to_owned(), bytes("pk")),
        ("mechanism".to_owned(), Value::String("m".to_owned())),
        ("version".to_owned(), Value::I64(1)),
        ("signingPublicKey".to_owned(), bytes("spk")),
        ("signingMechanism".to_owned(), Value::String("s".to_owned())),
        ("signingVersion".to_owned(), Value::I64(1)),
    ]);
    let mut request_cells = keys.clone();
    request_cells.insert("nonceCommitment".to_owned(), bytes("n"));
    world
        .commit(alice(), &[(DEVICE_REQUESTS, request, request_cells)])
        .expect("an account requests a device");
    let projection = |cells: &Cells| {
        let mut cells = cells.clone();
        cells.insert("deviceId".to_owned(), Value::Uuid(request.0));
        cells
    };
    world
        .commit(bob(), &[(DEVICE_KEYS, new_row(), projection(&keys))])
        .expect_err("another account can't publish keys for alice's device");
    let mut other = keys.clone();
    other.insert("publicKey".to_owned(), bytes("other"));
    world
        .commit(alice(), &[(DEVICE_KEYS, new_row(), projection(&other))])
        .expect_err("the projection must repeat the request's keys");
    world
        .commit(alice(), &[(DEVICE_KEYS, new_row(), projection(&keys))])
        .expect("the account publishes its device's keys");
}

/// The app's own permissions.
fn app_permissions() -> HashMap<TableName, TablePolicies> {
    app()
        .iter()
        .map(|(table, schema)| (*table, schema.policies.clone()))
        .collect()
}

/// Deploy `schema` as the target, declaring [`entries`] for it when it
/// holds Jazz's system tables (unless `declare` says otherwise).
fn deploy_declaring(
    schema: &Schema,
    permissions: HashMap<TableName, TablePolicies>,
    declare: bool,
) -> Result<Vec<String>, DeploymentError> {
    let hash = SchemaHash::compute(schema);
    let encryption = DeclaredEncryption {
        versions: if declare {
            BTreeMap::from([(hash.to_string(), entries())])
        } else {
            BTreeMap::new()
        },
        ..Default::default()
    };
    prepare_encrypted_deployment(
        &DeploymentCatalogue::default(),
        DeploymentRequest {
            target_schema_hash: hash,
            schemas: vec![(hash, schema.clone())],
            migrations: Vec::new(),
            permissions,
            allow_dropping_history: false,
        },
        &encryption,
    )
    .map(|(_, warnings)| warnings)
}

fn deploy(
    schema: &Schema,
    permissions: HashMap<TableName, TablePolicies>,
) -> Result<Vec<String>, DeploymentError> {
    deploy_declaring(
        schema,
        permissions,
        schema.contains_key(&TableName::from(MEMBERS)),
    )
}

/// A deployment accepts Jazz's system tables only as Jazz defines them, in
/// exactly the versions that declare encryption, and never the app's own
/// policies for them; it derives those itself.
#[test]
fn deployment_accepts_only_jazzs_own_system_tables() {
    let structural = |mut schema: Schema| {
        for table in schema.values_mut() {
            table.policies = TablePolicies::default();
        }
        schema
    };
    let fragment = structural(with_e2ee_fragment(&app()));
    let warnings = deploy(&fragment, app_permissions()).expect("the encrypted app deploys");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("space table boards"),
        "only the boards still read without members: {warnings:?}"
    );
    deploy_declaring(&fragment, app_permissions(), false)
        .expect_err("system tables in a version that declares no encryption");
    deploy_declaring(&structural(app()), app_permissions(), true)
        .expect_err("declarations for a version without the system tables");

    let mut wrong_rule = app_permissions();
    wrong_rule
        .get_mut(&TableName::from("projects"))
        .unwrap()
        .select = OperationPolicy::using(owner_is_me());
    let error = deploy(&fragment, wrong_rule).expect_err("a space table read without members");
    assert!(
        error
            .to_string()
            .contains("space table projects: its read rule must come from the members table"),
        "{error}"
    );

    let mut tampered = fragment.clone();
    tampered
        .get_mut(&TableName::from(MEMBERS))
        .unwrap()
        .indexed_columns = None;
    let error = deploy(&tampered, app_permissions()).expect_err("a changed system table");
    assert!(error.to_string().contains("not the definition"), "{error}");

    let mut partial = fragment.clone();
    partial.remove(&TableName::from(SPACES));
    deploy(&partial, app_permissions()).expect_err("a missing system table");

    let foreign = structural(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("__e2ee_notes").column("body", ColumnType::Text))
            .build(),
    );
    let error = deploy(&foreign, HashMap::new()).expect_err("an app table with the prefix");
    assert!(error.to_string().contains("reserved prefix"), "{error}");

    let mut referencing = fragment.clone();
    referencing.insert(
        TableName::from("invites"),
        TableSchemaBuilder::new("invites")
            .fk_column("member", MEMBERS)
            .build(),
    );
    deploy(&referencing, app_permissions()).expect_err("an app reference to a system table");

    let mut own = app_permissions();
    own.insert(TableName::from(MEMBERS), common::allow_all_policies());
    let error = deploy(&fragment, own).expect_err("app permissions for a system table");
    assert!(
        matches!(error, DeploymentError::InvalidPermissions { .. }),
        "{error}"
    );
}

/// Core finds a system table by the name the admitted versions give it and
/// reads its columns by name, so a migration that renames one, or changes
/// its columns, is refused at deploy: it would give an older version's
/// uploads another name or layout than the newest version's.
///
/// ```text
/// v1: __e2ee_members  ──RenameTable──►  v2: members_old    ✗
/// v1: old_members     ──RenameTable──►  v2: __e2ee_members ✗
/// scopeId ⇄ identifier swapped through a temporary name    ✗
/// a column added to __e2ee_members                         ✗
/// ```
#[test]
fn a_migration_may_not_change_a_system_table() {
    use jazz::tools::{Lens, LensOp, LensTransform};
    let structural = |mut schema: Schema| {
        for table in schema.values_mut() {
            table.policies = TablePolicies::default();
        }
        schema
    };
    let fragment = structural(with_e2ee_fragment(&app()));
    let plain = |table: &str| {
        let mut schema = structural(app());
        schema.insert(
            TableName::from(table),
            TableSchemaBuilder::new(table)
                .column("body", ColumnType::Text)
                .build(),
        );
        schema
    };
    let try_ops = |from_schema: Schema, to_schema: Schema, ops: Vec<LensOp>| {
        let (from, to) = (
            SchemaHash::compute(&from_schema),
            SchemaHash::compute(&to_schema),
        );
        let declares = |schema: &Schema| schema.contains_key(&TableName::from(MEMBERS));
        let versions = [(from, &from_schema), (to, &to_schema)]
            .into_iter()
            .filter(|(_, schema)| declares(schema))
            .map(|(hash, _)| (hash.to_string(), entries()))
            .collect();
        prepare_encrypted_deployment(
            &DeploymentCatalogue::default(),
            DeploymentRequest {
                target_schema_hash: to,
                schemas: vec![(from, from_schema), (to, to_schema)],
                migrations: vec![Lens::new(
                    from,
                    to,
                    LensTransform {
                        ops,
                        draft_ops: Vec::new(),
                    },
                )],
                permissions: app_permissions(),
                allow_dropping_history: false,
            },
            &DeclaredEncryption {
                versions,
                ..Default::default()
            },
        )
    };
    let rename_table = |old: &str, new: &str| LensOp::RenameTable {
        old_name: old.to_owned(),
        new_name: new.to_owned(),
    };
    let rename_column = |table: &str, old: &str, new: &str| LensOp::RenameColumn {
        table: table.to_owned(),
        old_name: old.to_owned(),
        new_name: new.to_owned(),
    };
    // Both versions hold the fragment exactly, so only the lens differs.
    let mut next = fragment.clone();
    next.insert(
        TableName::from("notes"),
        TableSchemaBuilder::new("notes")
            .column("body", ColumnType::Text)
            .build(),
    );
    // The lens introduces `notes`, so that apart from its column operations
    // it is a valid migration between the two versions.
    let add_notes = LensOp::AddTable {
        table: "notes".to_owned(),
        schema: next[&TableName::from("notes")].clone(),
    };
    let cases = [
        (
            fragment.clone(),
            plain("members_old"),
            vec![rename_table(MEMBERS, "members_old")],
        ),
        (
            plain("old_members"),
            fragment.clone(),
            vec![rename_table("old_members", MEMBERS)],
        ),
        (
            // `scopeId` and `identifier` swapped through a temporary name.
            fragment.clone(),
            next.clone(),
            vec![
                add_notes.clone(),
                rename_column(MEMBERS, "scopeId", "swap"),
                rename_column(MEMBERS, "identifier", "scopeId"),
                rename_column(MEMBERS, "swap", "identifier"),
            ],
        ),
        (
            fragment.clone(),
            next.clone(),
            vec![
                add_notes,
                LensOp::AddColumn {
                    table: MEMBERS.to_owned(),
                    column: "extra".to_owned(),
                    column_type: ColumnType::Text,
                    default: jazz::tools::Value::Text("x".to_owned()),
                },
            ],
        ),
    ];
    for (from, to, ops) in cases {
        let error = try_ops(from, to, ops).expect_err("a lens that changes a system table");
        assert!(
            matches!(error, DeploymentError::InvalidMigration { .. })
                && error.to_string().contains("may not change"),
            "{error}"
        );
    }
    // A lens on an app table is not this rule's business.
    let outcome = try_ops(
        fragment.clone(),
        next,
        vec![rename_column("boards", "ownerId", "owner")],
    );
    assert!(
        !outcome
            .err()
            .is_some_and(|error| error.to_string().contains("may not change")),
        "the rule names system tables only"
    );
}

// ---------------------------------------------------------------------------
// Each derived rule, one by one (protocol R3–R15)
// ---------------------------------------------------------------------------

fn fresh() -> Value {
    Value::Uuid(uuid::Uuid::new_v4())
}

fn id_of(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn cells(pairs: &[(&str, Value)]) -> Cells {
    pairs
        .iter()
        .map(|(column, value)| ((*column).to_owned(), value.clone()))
        .collect()
}

/// A session with no account, signed in anonymously.
fn anonymous() -> AuthorSubject {
    AuthorSubject::reserved(AuthorSubject::ANONYMOUS_ISSUER, "visitor").unwrap()
}

fn challenge(device: RowUuid) -> Cells {
    cells(&[
        ("deviceId", id_of(device)),
        ("epochId", fresh()),
        ("envelope", bytes("e")),
        ("nonce", bytes("n")),
        ("signerId", id_of(device)),
        ("signature", bytes("s")),
    ])
}

fn proof(challenge: RowUuid) -> Cells {
    cells(&[
        ("challengeId", id_of(challenge)),
        ("proof", bytes("p")),
        ("nonce", bytes("n")),
        ("signature", bytes("s")),
    ])
}

fn approval(challenge: RowUuid, signer: RowUuid) -> Cells {
    cells(&[
        ("challengeId", id_of(challenge)),
        ("signerId", id_of(signer)),
        ("verification", bytes("v")),
        ("signature", bytes("s")),
    ])
}

fn device_delivery(challenge: RowUuid) -> Cells {
    cells(&[
        ("challengeId", id_of(challenge)),
        ("envelope", bytes("e")),
        ("verification", bytes("v")),
    ])
}

fn public_approval(
    who: AuthorSubject,
    device: RowUuid,
    signer: RowUuid,
    recovery: Option<RowUuid>,
) -> Cells {
    let mut row = cells(&[
        ("accountId", account(who)),
        ("epochId", fresh()),
        ("deviceId", id_of(device)),
        ("signerId", id_of(signer)),
        ("signature", bytes("s")),
    ]);
    if let Some(root) = recovery {
        row.insert("recoveryRootId".to_owned(), id_of(root));
        row.insert("recoverySignature".to_owned(), bytes("r"));
    }
    row
}

/// An account successor's columns; the private one adds its key history.
fn account_successor(
    who: AuthorSubject,
    signer: RowUuid,
    action: &str,
    removed: Option<RowUuid>,
    private: bool,
) -> Cells {
    let mut row = cells(&[
        ("accountId", account(who)),
        ("predecessor", fresh()),
        ("epochId", fresh()),
        ("signerId", id_of(signer)),
        ("action", text(action)),
        ("retiredRecoveryRootIds", bytes("[]")),
        ("membership", bytes("[]")),
        ("revision", bytes("[]")),
        ("signature", bytes("s")),
    ]);
    if let Some(device) = removed {
        row.insert("removedDeviceId".to_owned(), id_of(device));
    }
    if private {
        row.insert("verification".to_owned(), bytes("v"));
        row.insert("history".to_owned(), bytes("h"));
        row.insert("deliveries".to_owned(), bytes("[]"));
    }
    row
}

fn recovery_root(who: AuthorSubject, signer: RowUuid) -> Cells {
    cells(&[
        ("accountId", account(who)),
        ("signerId", id_of(signer)),
        ("epochId", fresh()),
        ("publicKey", bytes("pk")),
        ("mechanism", text("m")),
        ("version", Value::I64(1)),
        ("signingPublicKey", bytes("spk")),
        ("signingMechanism", text("s")),
        ("signingVersion", Value::I64(1)),
        ("signature", bytes("s")),
    ])
}

fn recovery_delivery(root: RowUuid) -> Cells {
    cells(&[
        ("rootId", id_of(root)),
        ("epochId", fresh()),
        ("envelope", bytes("e")),
    ])
}

fn removal_request(who: AuthorSubject, device: RowUuid) -> Cells {
    cells(&[
        ("accountId", account(who)),
        ("epochId", fresh()),
        ("deviceId", id_of(device)),
        ("signature", bytes("s")),
    ])
}

fn account_key(who: AuthorSubject, signer: RowUuid) -> Cells {
    cells(&[
        ("accountId", account(who)),
        ("epochId", fresh()),
        ("signerId", id_of(signer)),
        ("mechanism", text("m")),
        ("version", Value::I64(1)),
        ("publicKey", bytes("pk")),
        ("signature", bytes("s")),
    ])
}

fn identity_row(device: RowUuid, generation: i64, ledger: i64) -> Cells {
    cells(&[
        ("ledgerVersion", Value::I64(ledger)),
        ("generation", Value::I64(generation)),
        ("deviceId", id_of(device)),
        ("epochId", fresh()),
        ("envelope", bytes("e")),
        ("verification", bytes("v")),
    ])
}

fn root_row(who: AuthorSubject, device: RowUuid, epoch: Value, kind: &str) -> Cells {
    cells(&[
        ("ledgerVersion", Value::I64(1)),
        ("generation", Value::I64(0)),
        ("accountId", account(who)),
        ("deviceId", id_of(device)),
        ("epochId", epoch),
        ("kind", text(kind)),
    ])
}

impl World {
    /// The rows of `table` a read by `who` at the authority returns.
    fn reads(&self, who: AuthorSubject, table: &str) -> Vec<RowUuid> {
        let prepared = self.server.prepare_query(&Query::from(table)).unwrap();
        block_on(
            self.server
                .all_for_identity(&prepared, ReadOpts::default(), who),
        )
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
    }

    /// Delete `row` of `table` as `who`'s session, alone in its unit.
    fn delete(&self, who: AuthorSubject, table: &str, row: RowUuid) -> Result<(), String> {
        if self.uploading.get() {
            let replica = self.replica(who);
            self.pump();
            block_on(replica.delete(table, row, DeleteOptions::default()))
                .map_err(|error| error.message)?;
            self.pump();
            return if self.holds(table, row) {
                Err("the authority refused the delete".to_owned())
            } else {
                Ok(())
            };
        }
        let id = OpenTransactionId::new();
        block_on(self.server.begin_mergeable(id)).unwrap();
        let staged = block_on(self.server.tx_ref(id).delete(
            table,
            row,
            DeleteOptions {
                identity: WriteIdentity::Session(who),
                ..Default::default()
            },
        ));
        if let Err(error) = staged {
            let _ = self.server.abandon_transaction_handle(id);
            return Err(error.message);
        }
        block_on(self.server.commit_mergeable_handle(id))
            .map(drop)
            .map_err(|error| error.message)
    }

    fn insert(&self, who: AuthorSubject, table: &str, cells: Cells) -> Result<RowUuid, String> {
        let row = new_row();
        self.commit(who, &[(table, row, cells)]).map(|()| row)
    }
}

/// Every account record of alice's, one of each kind, by table.
struct Ledger {
    rows: Vec<(&'static str, RowUuid)>,
}

/// Alice's account records: a bootstrap, an approved second device, a
/// recovery root and its delivery, a device removal (successor and
/// request) and an account key.
fn alices_ledger(world: &World) -> Ledger {
    use system_tables::*;
    let mut rows = Vec::new();
    let device = world.device(alice());
    rows.push((DEVICE_REQUESTS, device));
    world.identity(alice(), device, 0, "app").unwrap();
    rows.push((ACCOUNT_IDENTITIES, RowUuid(alice().account_id().unwrap().0)));
    rows.extend(
        world
            .reads(alice(), ACCOUNT_ROOTS)
            .into_iter()
            .map(|row| (ACCOUNT_ROOTS, row)),
    );
    let second = world.device(alice());
    let keys = world
        .insert(
            alice(),
            DEVICE_KEYS,
            cells(&[
                ("deviceId", id_of(second)),
                ("publicKey", bytes("pk")),
                ("mechanism", text("m")),
                ("version", Value::I64(1)),
                ("signingPublicKey", bytes("spk")),
                ("signingMechanism", text("s")),
                ("signingVersion", Value::I64(1)),
            ]),
        )
        .unwrap();
    rows.push((DEVICE_KEYS, keys));
    let challenge_row = world
        .insert(alice(), DEVICE_CHALLENGES, challenge(second))
        .unwrap();
    rows.push((DEVICE_CHALLENGES, challenge_row));
    rows.push((
        DEVICE_PROOFS,
        world
            .insert(alice(), DEVICE_PROOFS, proof(challenge_row))
            .unwrap(),
    ));
    rows.push((
        DEVICE_DELIVERIES,
        world
            .insert(alice(), DEVICE_DELIVERIES, device_delivery(challenge_row))
            .unwrap(),
    ));
    let root = world
        .insert(alice(), RECOVERY_ROOTS, recovery_root(alice(), device))
        .unwrap();
    rows.push((RECOVERY_ROOTS, root));
    rows.push((
        RECOVERY_DELIVERIES,
        world
            .insert(alice(), RECOVERY_DELIVERIES, recovery_delivery(root))
            .unwrap(),
    ));
    let approved = new_row();
    world
        .commit(
            alice(),
            &[
                (DEVICE_APPROVALS, approved, approval(challenge_row, device)),
                (
                    PUBLIC_DEVICE_APPROVALS,
                    approved,
                    public_approval(alice(), second, device, Some(root)),
                ),
            ],
        )
        .expect("an approval with its recovery root");
    rows.push((DEVICE_APPROVALS, approved));
    rows.push((PUBLIC_DEVICE_APPROVALS, approved));
    let removal = new_row();
    world
        .commit(
            alice(),
            &[
                (
                    ACCOUNT_SUCCESSORS,
                    removal,
                    account_successor(alice(), device, "remove-device", Some(second), true),
                ),
                (
                    PUBLIC_ACCOUNT_SUCCESSORS,
                    removal,
                    account_successor(alice(), device, "remove-device", Some(second), false),
                ),
            ],
        )
        .expect("a device removal");
    rows.push((ACCOUNT_SUCCESSORS, removal));
    rows.push((PUBLIC_ACCOUNT_SUCCESSORS, removal));
    rows.push((
        DEVICE_REMOVAL_REQUESTS,
        world
            .insert(
                alice(),
                DEVICE_REMOVAL_REQUESTS,
                removal_request(alice(), second),
            )
            .unwrap(),
    ));
    rows.push((
        ACCOUNT_KEYS,
        world
            .insert(alice(), ACCOUNT_KEYS, account_key(alice(), device))
            .unwrap(),
    ));
    Ledger { rows }
}

/// Private account records reach only their own account (R3, R5, R7, R9);
/// public ones every local-first or external session, never an anonymous
/// one (R10).
#[test]
fn account_records_are_read_by_their_account_or_by_signed_in_sessions() {
    use system_tables::*;
    let world = World::new();
    let ledger = alices_ledger(&world);
    let public = [
        DEVICE_KEYS,
        ACCOUNT_ROOTS,
        PUBLIC_DEVICE_APPROVALS,
        PUBLIC_ACCOUNT_SUCCESSORS,
        RECOVERY_ROOTS,
        DEVICE_REMOVAL_REQUESTS,
        ACCOUNT_KEYS,
    ];
    for (table, row) in &ledger.rows {
        assert!(
            world.reads(alice(), table).contains(row),
            "alice reads her own {table}"
        );
        assert_eq!(
            world.reads(bob(), table).contains(row),
            public.contains(table),
            "bob reads alice's {table} exactly when it is public"
        );
        assert!(
            !world.reads(anonymous(), table).contains(row),
            "an anonymous session reads no {table}"
        );
    }
}

/// R5: the first identity is the account's own row id, made from the
/// account's own device request, at ledger version 1.
#[test]
fn a_first_identity_is_the_accounts_own() {
    use system_tables::ACCOUNT_IDENTITIES;
    let world = World::new();
    let device = world.device(alice());
    let bobs = world.device(bob());
    let own_id = RowUuid(alice().account_id().unwrap().0);
    world
        .commit(
            alice(),
            &[(ACCOUNT_IDENTITIES, new_row(), identity_row(device, 0, 1))],
        )
        .expect_err("a first identity under another row id");
    world
        .commit(
            alice(),
            &[(ACCOUNT_IDENTITIES, own_id, identity_row(bobs, 0, 1))],
        )
        .expect_err("a first identity from another account's device");
    world
        .commit(
            alice(),
            &[(ACCOUNT_IDENTITIES, own_id, identity_row(device, 0, 2))],
        )
        .expect_err("another ledger version");
    world
        .commit(
            alice(),
            &[(ACCOUNT_IDENTITIES, own_id, identity_row(device, 0, 1))],
        )
        .expect("the account's first identity");
}

/// R6: a root names the session's account and matches an identity the
/// same transaction inserts.
#[test]
fn a_root_comes_with_its_identity() {
    use system_tables::{ACCOUNT_IDENTITIES, ACCOUNT_ROOTS};
    let world = World::new();
    let device = world.device(alice());
    let own_id = RowUuid(alice().account_id().unwrap().0);
    let identity = identity_row(device, 0, 1);
    let epoch = identity["epochId"].clone();
    world
        .commit(
            alice(),
            &[
                (ACCOUNT_IDENTITIES, own_id, identity.clone()),
                (
                    ACCOUNT_ROOTS,
                    new_row(),
                    root_row(alice(), device, fresh(), "app"),
                ),
            ],
        )
        .expect_err("a root whose epoch matches no identity");
    world
        .commit(
            alice(),
            &[
                (ACCOUNT_IDENTITIES, own_id, identity.clone()),
                (
                    ACCOUNT_ROOTS,
                    new_row(),
                    root_row(bob(), device, epoch.clone(), "app"),
                ),
            ],
        )
        .expect_err("a root naming another account");
    world
        .commit(alice(), &[(ACCOUNT_IDENTITIES, own_id, identity)])
        .expect("the identity alone");
    world
        .commit(
            alice(),
            &[(
                ACCOUNT_ROOTS,
                new_row(),
                root_row(alice(), device, epoch, "app"),
            )],
        )
        .expect_err("its root in a later transaction");
}

/// R7: challenges, proofs, approvals and device deliveries reference the
/// account's own request or challenge.
#[test]
fn device_approval_records_reference_the_accounts_own() {
    use system_tables::*;
    let world = World::new();
    let device = world.device(alice());
    let bobs_device = world.device(bob());
    let bobs_challenge = world
        .insert(bob(), DEVICE_CHALLENGES, challenge(bobs_device))
        .unwrap();
    world
        .insert(alice(), DEVICE_CHALLENGES, challenge(bobs_device))
        .expect_err("a challenge to another account's device");
    let own = world
        .insert(alice(), DEVICE_CHALLENGES, challenge(device))
        .expect("a challenge to the account's own device");
    for (table, of) in [
        (DEVICE_PROOFS, proof as fn(RowUuid) -> Cells),
        (DEVICE_APPROVALS, |c| {
            approval(c, RowUuid(uuid::Uuid::nil()))
        }),
        (DEVICE_DELIVERIES, device_delivery),
    ] {
        world
            .insert(alice(), table, of(bobs_challenge))
            .expect_err(table);
        world
            .insert(alice(), table, of(own))
            .unwrap_or_else(|e| panic!("{table}: {e}"));
    }
}

/// R8: a public approval names the session's account, its own signer and
/// device requests, and no recovery root or one of its own.
#[test]
fn a_public_approval_names_the_accounts_own_records() {
    use system_tables::*;
    let world = World::new();
    let device = world.device(alice());
    let second = world.device(alice());
    let challenge_row = world
        .insert(alice(), DEVICE_CHALLENGES, challenge(second))
        .unwrap();
    let bobs_device = world.device(bob());
    let own_root = world
        .insert(alice(), RECOVERY_ROOTS, recovery_root(alice(), device))
        .unwrap();
    let bobs_root = world
        .insert(bob(), RECOVERY_ROOTS, recovery_root(bob(), bobs_device))
        .unwrap();
    let pair = |public: Cells| {
        let row = new_row();
        world.commit(
            alice(),
            &[
                (DEVICE_APPROVALS, row, approval(challenge_row, device)),
                (PUBLIC_DEVICE_APPROVALS, row, public),
            ],
        )
    };
    for (refused, public) in [
        (
            "another account",
            public_approval(bob(), second, device, None),
        ),
        (
            "another account's signer",
            public_approval(alice(), second, bobs_device, None),
        ),
        (
            "another account's device",
            public_approval(alice(), bobs_device, device, None),
        ),
        (
            "another account's recovery root",
            public_approval(alice(), second, device, Some(bobs_root)),
        ),
        (
            "no recovery root at all",
            public_approval(alice(), second, device, Some(new_row())),
        ),
    ] {
        pair(public).expect_err(refused);
    }
    pair(public_approval(alice(), second, device, None)).expect("without a recovery root");
    pair(public_approval(alice(), second, device, Some(own_root)))
        .expect("with the account's own recovery root");
}

/// R8: an account successor names the session's account and its own
/// signer, and a target that matches its action; its public half shares
/// its row id and transaction.
#[test]
fn an_account_successor_matches_its_action_and_its_public_half() {
    use system_tables::{ACCOUNT_SUCCESSORS, PUBLIC_ACCOUNT_SUCCESSORS};
    let world = World::new();
    let device = world.device(alice());
    let second = world.device(alice());
    let bobs_device = world.device(bob());
    let both = |private: Cells, public: Cells| {
        let row = new_row();
        world.commit(
            alice(),
            &[
                (ACCOUNT_SUCCESSORS, row, private),
                (PUBLIC_ACCOUNT_SUCCESSORS, row, public),
            ],
        )
    };
    let good =
        |public: bool| account_successor(alice(), device, "remove-device", Some(second), !public);
    let bad: [(&str, Box<dyn Fn(bool) -> Cells>); 6] = [
        (
            "another account",
            Box::new(|p| account_successor(bob(), device, "remove-device", Some(second), !p)),
        ),
        (
            "another account's signer",
            Box::new(|p| {
                account_successor(alice(), bobs_device, "remove-device", Some(second), !p)
            }),
        ),
        (
            "another account's device removed",
            Box::new(|p| {
                account_successor(alice(), device, "remove-device", Some(bobs_device), !p)
            }),
        ),
        (
            "a recovery root retired with a device",
            Box::new(|p| {
                account_successor(alice(), device, "retire-recovery-root", Some(second), !p)
            }),
        ),
        (
            "an unknown action",
            Box::new(|p| account_successor(alice(), device, "rename", None, !p)),
        ),
        (
            "an unknown action removing a device",
            Box::new(|p| account_successor(alice(), device, "rename", Some(second), !p)),
        ),
    ];
    for (refused, row) in &bad {
        world
            .insert(alice(), ACCOUNT_SUCCESSORS, row(false))
            .expect_err(refused);
        both(good(false), row(true)).expect_err(refused);
    }
    both(good(false), good(true)).expect("a device removal, both halves");
    both(
        account_successor(alice(), device, "retire-recovery-root", None, true),
        account_successor(alice(), device, "retire-recovery-root", None, false),
    )
    .expect("a recovery root retired");
    world
        .insert(alice(), PUBLIC_ACCOUNT_SUCCESSORS, good(true))
        .expect_err("a public half alone");
    world
        .commit(
            alice(),
            &[
                (ACCOUNT_SUCCESSORS, new_row(), good(false)),
                (PUBLIC_ACCOUNT_SUCCESSORS, new_row(), good(true)),
            ],
        )
        .expect_err("a public half beside a private one of another id");
    let earlier = world
        .insert(alice(), ACCOUNT_SUCCESSORS, good(false))
        .expect("a private half alone");
    world
        .commit(alice(), &[(PUBLIC_ACCOUNT_SUCCESSORS, earlier, good(true))])
        .expect_err("its public half in a later transaction");
}

/// R8, R9, R11: removal requests, recovery roots and account keys name the
/// session's account and its own device or signer; a recovery delivery
/// names its own recovery root.
#[test]
fn removal_requests_recovery_and_account_keys_are_the_accounts_own() {
    use system_tables::*;
    let world = World::new();
    let device = world.device(alice());
    let bobs_device = world.device(bob());
    let bobs_root = world
        .insert(bob(), RECOVERY_ROOTS, recovery_root(bob(), bobs_device))
        .unwrap();
    for (table, refused, row) in [
        (
            DEVICE_REMOVAL_REQUESTS,
            "another account",
            removal_request(bob(), device),
        ),
        (
            DEVICE_REMOVAL_REQUESTS,
            "another account's device",
            removal_request(alice(), bobs_device),
        ),
        (
            RECOVERY_ROOTS,
            "another account",
            recovery_root(bob(), device),
        ),
        (
            RECOVERY_ROOTS,
            "another account's signer",
            recovery_root(alice(), bobs_device),
        ),
        (
            RECOVERY_DELIVERIES,
            "another account's recovery root",
            recovery_delivery(bobs_root),
        ),
        (ACCOUNT_KEYS, "another account", account_key(bob(), device)),
        (
            ACCOUNT_KEYS,
            "another account's signer",
            account_key(alice(), bobs_device),
        ),
    ] {
        world
            .insert(alice(), table, row)
            .expect_err(&format!("{table}: {refused}"));
    }
    world
        .insert(
            alice(),
            DEVICE_REMOVAL_REQUESTS,
            removal_request(alice(), device),
        )
        .unwrap();
    let root = world
        .insert(alice(), RECOVERY_ROOTS, recovery_root(alice(), device))
        .unwrap();
    world
        .insert(alice(), RECOVERY_DELIVERIES, recovery_delivery(root))
        .unwrap();
    world
        .insert(alice(), ACCOUNT_KEYS, account_key(alice(), device))
        .unwrap();
}

/// R12: one member-row delete per transaction: a member leaves, an
/// updater removes another member, and a member who may not update the
/// project removes nobody else.
#[test]
fn members_leave_and_updaters_remove_members() {
    let world = World::new();
    let p = new_row();
    let own = new_row();
    world
        .commit(
            alice(),
            &[
                ("projects", p, project(alice())),
                (MEMBERS, own, world.member(p, alice())),
            ],
        )
        .unwrap();
    let add = |who: AuthorSubject| {
        let row = new_row();
        world
            .commit(alice(), &[(MEMBERS, row, world.member(p, who))])
            .unwrap();
        row
    };
    let (bobs, carols) = (add(bob()), add(carol()));
    world
        .delete(bob(), MEMBERS, carols)
        .expect_err("a member who may not update the project removes nobody else");
    world.delete(bob(), MEMBERS, bobs).expect("a member leaves");
    world
        .delete(alice(), MEMBERS, carols)
        .expect("the owner removes a member");
    assert_eq!(world.reads(alice(), MEMBERS), vec![own]);
}

/// R14: a space's successors and deliveries are read by its members only.
#[test]
fn members_read_successors_and_deliveries() {
    let world = World::new();
    let p = new_row();
    world
        .commit(
            alice(),
            &[
                ("projects", p, project(alice())),
                (MEMBERS, new_row(), world.member(p, alice())),
            ],
        )
        .unwrap();
    world
        .commit(alice(), &[(MEMBERS, new_row(), world.member(p, bob()))])
        .unwrap();
    let successor = world
        .insert(alice(), SPACE_SUCCESSORS, world.successor(p, alice()))
        .unwrap();
    let delivery = world
        .insert(alice(), SPACE_DELIVERIES, world.delivery(p, alice(), bob()))
        .unwrap();
    for (table, row) in [(SPACE_SUCCESSORS, successor), (SPACE_DELIVERIES, delivery)] {
        assert!(
            world.reads(bob(), table).contains(&row),
            "a member reads {table}"
        );
        assert!(
            !world.reads(carol(), table).contains(&row),
            "a non-member doesn't read {table}"
        );
    }
}

/// R15, R16: creating or rotating a space takes a member who may read and
/// update the space row. A board's owner reads and updates it without a
/// member row (its read rule doesn't come from members yet), so membership
/// is what admits it; a member who may not update it is refused.
#[test]
fn only_member_updaters_create_and_rotate_a_space() {
    let world = World::new();
    let boards = Value::Uuid(world.server.table_identity("boards").unwrap().unwrap().0);
    let board = new_row();
    world
        .commit(alice(), &[("boards", board, project(alice()))])
        .unwrap();
    let on_board = |mut row: Cells| {
        row.insert("scopeId".to_owned(), boards.clone());
        row.insert("identifier".to_owned(), id_of(board));
        row
    };
    let root = |who| on_board(world.root(board, who));
    let successor = |who| on_board(world.successor(board, who));
    let member = |who| on_board(world.member(board, who));
    world
        .insert(alice(), SPACES, root(alice()))
        .expect_err("the owner is no member yet");
    world
        .insert(alice(), SPACE_SUCCESSORS, successor(alice()))
        .expect_err("nor may it rotate");
    world.insert(alice(), MEMBERS, member(alice())).unwrap();
    world.insert(alice(), MEMBERS, member(bob())).unwrap();
    world
        .insert(bob(), SPACES, root(bob()))
        .expect_err("a member who may not update the board");
    world
        .insert(bob(), SPACE_SUCCESSORS, successor(bob()))
        .expect_err("nor may it rotate");
    world
        .insert(alice(), SPACES, root(alice()))
        .expect("a member who updates the board creates its space");
    world
        .insert(alice(), SPACE_SUCCESSORS, successor(alice()))
        .expect("and rotates it");
}

/// An account bootstraps from its own client, not only from writes staged
/// at the authority: the checks beside the identity and root policies
/// read the uploaded rows through the stored view the authority checks
/// them in, so alice's first identity and root sync and are accepted.
///
/// ```text
/// alice's replica ──request, then identity + root (one commit)──► server ✓
/// ```
#[test]
fn an_account_bootstraps_from_its_own_client() {
    let world = World::new();
    let replica = world.replica(alice());
    let device = new_row();
    let request = world.device_request_cells();
    block_on(replica.insert(
        DEVICE_REQUESTS,
        request,
        InsertOptions {
            row_id: Some(device),
            ..Default::default()
        },
    ))
    .unwrap();
    world.visible(alice(), DEVICE_REQUESTS);
    let epoch = Value::Uuid(uuid::Uuid::new_v4());
    let id = OpenTransactionId::new();
    block_on(replica.begin_mergeable(id)).unwrap();
    let tx = replica.tx_ref(id);
    block_on(tx.insert(
        system_tables::ACCOUNT_IDENTITIES,
        BTreeMap::from([
            ("ledgerVersion".to_owned(), Value::I64(1)),
            ("generation".to_owned(), Value::I64(0)),
            ("deviceId".to_owned(), Value::Uuid(device.0)),
            ("epochId".to_owned(), epoch.clone()),
            ("envelope".to_owned(), bytes("e")),
            ("verification".to_owned(), bytes("v")),
        ]),
        InsertOptions {
            row_id: Some(RowUuid(alice().account_id().unwrap().0)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(tx.insert(
        system_tables::ACCOUNT_ROOTS,
        BTreeMap::from([
            ("ledgerVersion".to_owned(), Value::I64(1)),
            ("generation".to_owned(), Value::I64(0)),
            ("accountId".to_owned(), account(alice())),
            ("deviceId".to_owned(), Value::Uuid(device.0)),
            ("epochId".to_owned(), epoch),
            ("kind".to_owned(), Value::String("app".to_owned())),
        ]),
        InsertOptions {
            row_id: Some(new_row()),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(replica.commit_mergeable_handle(id)).unwrap();
    world.visible(alice(), system_tables::ACCOUNT_ROOTS);
    let at_server = |table: &str| {
        let prepared = world.server.prepare_query(&Query::from(table)).unwrap();
        block_on(world.server.all(&prepared, ReadOpts::default()))
            .unwrap()
            .len()
    };
    assert_eq!(at_server(system_tables::ACCOUNT_IDENTITIES), 1);
    assert_eq!(at_server(system_tables::ACCOUNT_ROOTS), 1);
}

impl World {
    /// The cells of a device request, as `device` writes them.
    fn device_request_cells(&self) -> Cells {
        BTreeMap::from([
            ("publicKey".to_owned(), bytes("pk")),
            ("mechanism".to_owned(), Value::String("m".to_owned())),
            ("version".to_owned(), Value::I64(1)),
            ("signingPublicKey".to_owned(), bytes("spk")),
            ("signingMechanism".to_owned(), Value::String("s".to_owned())),
            ("signingVersion".to_owned(), Value::I64(1)),
            ("nonceCommitment".to_owned(), bytes("n")),
        ])
    }
}

/// Core recognises Jazz's system tables by the name the admitted versions
/// give them, whatever name a client's upload carries: an app that turns
/// encryption on in a later version stores them as `name~hash`, and an
/// uploaded identity or root still gets the account checks, and a member
/// still leaves over sync. Whether the server opened on the version with
/// the fragment or had it deployed while running, which leaves the first
/// schema its owner.
fn uploaded_records_are_checked(world: World) {
    world.uploading.set(true);
    let device = world.device(alice());
    world
        .identity(alice(), device, 0, "local-first")
        .expect_err("a root's kind names how its account was founded");
    world
        .identity(alice(), device, 0, "app")
        .expect("the right kind");
    world
        .identity(alice(), device, 2, "app")
        .expect_err("a reset follows the generation before it");
    world
        .identity(alice(), device, 1, "app")
        .expect("a reset of an app-founded account");
}

fn a_member_leaves_over_sync(world: World) {
    world.uploading.set(true);
    let p = new_row();
    world
        .commit(
            alice(),
            &[
                ("projects", p, project(alice())),
                (MEMBERS, new_row(), world.member(p, alice())),
            ],
        )
        .unwrap();
    let bobs = new_row();
    world
        .commit(alice(), &[(MEMBERS, bobs, world.member(p, bob()))])
        .unwrap();
    world.delete(bob(), MEMBERS, bobs).expect("a member leaves");
}

#[test]
fn uploaded_account_records_to_a_later_fragment_get_the_same_checks() {
    uploaded_records_are_checked(World::late_fragment());
}

#[test]
fn uploaded_account_records_after_a_deployed_fragment_get_the_same_checks() {
    uploaded_records_are_checked(World::late_fragment_deployed());
}

#[test]
fn a_member_leaves_over_sync_from_a_later_fragment() {
    a_member_leaves_over_sync(World::late_fragment());
}

#[test]
fn a_member_leaves_over_sync_after_a_deployed_fragment() {
    a_member_leaves_over_sync(World::late_fragment_deployed());
}

/// The account registry as a test sets it for an encrypting app: which
/// accounts the app's own namespace founded, and which another audience did.
struct Namespaces {
    own: Vec<jazz::account_registry::AccountId>,
    other: Vec<jazz::account_registry::AccountId>,
}

impl jazz::db::AccountRegistryView for Namespaces {
    fn founded_local_first(
        &self,
        account: jazz::account_registry::AccountId,
    ) -> Result<Option<bool>, String> {
        Ok(Some(self.own.contains(&account)))
    }

    fn founded_for_another_audience(
        &self,
        account: jazz::account_registry::AccountId,
    ) -> Result<bool, String> {
        Ok(self.other.contains(&account))
    }
}

/// An encrypting app serves local-first accounts founded for its own app id
/// only (R5, R6): for an account founded for another audience, neither an
/// honest `local-first` root nor an `app` one is accepted. The app's own
/// local-first accounts and app-founded accounts still bootstrap.
#[test]
fn an_account_founded_for_another_audience_cannot_bootstrap() {
    let world = World::new();
    world
        .server
        .set_account_registry(std::rc::Rc::new(Namespaces {
            own: vec![bob().account_id().unwrap()],
            other: vec![alice().account_id().unwrap()],
        }));
    let device = world.device(alice());
    for kind in ["local-first", "app"] {
        world
            .identity(alice(), device, 0, kind)
            .expect_err("no root of either kind for an account of another audience");
    }
    let device = world.device(bob());
    world
        .identity(bob(), device, 0, "local-first")
        .expect("an account the app's own audience founded");
    let device = world.device(carol());
    world
        .identity(carol(), device, 0, "app")
        .expect("an app-founded account");
}

/// A lens that renames a system table, or changes its columns, is not
/// admitted by core either, whatever ships it: ordinary tables' lenses still
/// are, and a system table carried unchanged needs no entry.
#[test]
fn core_admits_no_lens_that_renames_a_system_table() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    let one_table = |name: &str| {
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new(name)
                        .column("body", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        )
    };
    let (before, ordinary, system) = (
        one_table("old_members"),
        one_table("renamed_members"),
        one_table(MEMBERS),
    );
    let rename = |to: &JazzSchema, target: &str| {
        MigrationLens::new(
            before.version_id(),
            to.version_id(),
            vec![TableLens {
                source_table: "old_members".into(),
                target_table: target.into(),
                ops: Vec::<LensOp>::new(),
            }],
        )
        .unwrap()
    };
    let (to_ordinary, to_system) = (
        rename(&ordinary, "renamed_members"),
        rename(&system, MEMBERS),
    );
    let server = block_on(Db::open_history_complete(DbConfig::new(
        before.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(server.admit_schema_with_lenses(ordinary, &[to_ordinary]))
        .expect("an ordinary rename is admitted");
    let error = block_on(server.admit_schema_with_lenses(system, &[to_system]))
        .expect_err("a rename to a system table's name");
    assert!(
        error.message.contains("carried between versions unchanged"),
        "{error:?}"
    );

    // A column op on a system table, and the same op on an ordinary table.
    let with_column = |table: &str, column: &str| {
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new(table)
                        .column(column, ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        )
    };
    for (table, admitted) in [("plain_table", true), (MEMBERS, false)] {
        let (first, second) = (with_column(table, "body"), with_column(table, "text"));
        let lens = MigrationLens::new(
            first.version_id(),
            second.version_id(),
            vec![TableLens {
                source_table: table.into(),
                target_table: table.into(),
                ops: vec![LensOp::RenameColumn {
                    from: "body".into(),
                    to: "text".into(),
                }],
            }],
        )
        .unwrap();
        let server = block_on(Db::open_history_complete(DbConfig::new(
            first,
            DbIdentity {
                node: NodeUuid::from_bytes([0xee; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let result = block_on(server.admit_schema_with_lenses(second, &[lens]));
        match (admitted, result) {
            (true, result) => {
                result.expect("a column rename on an ordinary table is admitted");
            }
            (false, result) => {
                let error = result.expect_err("a column rename on a system table");
                assert!(
                    error.message.contains("carried between versions unchanged"),
                    "{error:?}"
                );
            }
        }
    }
}

/// A version history as a deployment: `versions` oldest first, each with
/// whether it declares encryption, and the operations of the lens from each
/// version to the next. The last version is the target.
fn deploy_history(
    versions: Vec<(Schema, bool)>,
    ops: Vec<Vec<jazz::tools::LensOp>>,
) -> Result<jazz::tools::deployment::PreparedDeployment, DeploymentError> {
    use jazz::tools::{Lens, LensTransform};
    let hashes: Vec<SchemaHash> = versions
        .iter()
        .map(|(schema, _)| SchemaHash::compute(schema))
        .collect();
    let declared = DeclaredEncryption {
        versions: versions
            .iter()
            .zip(&hashes)
            .filter(|((_, declares), _)| *declares)
            .map(|(_, hash)| (hash.to_string(), entries()))
            .collect(),
        ..Default::default()
    };
    let migrations = ops
        .into_iter()
        .enumerate()
        .map(|(n, ops)| {
            Lens::new(
                hashes[n],
                hashes[n + 1],
                LensTransform {
                    ops,
                    draft_ops: Vec::new(),
                },
            )
        })
        .collect();
    prepare_encrypted_deployment(
        &DeploymentCatalogue::default(),
        DeploymentRequest {
            target_schema_hash: *hashes.last().unwrap(),
            schemas: hashes
                .iter()
                .copied()
                .zip(versions.into_iter().map(|(schema, _)| schema))
                .collect(),
            migrations,
            permissions: app_permissions(),
            allow_dropping_history: false,
        },
        &declared,
    )
    .map(|(prepared, _)| prepared)
}

/// The structural form of a schema, as a deployment carries it.
fn structural_of(mut schema: Schema) -> Schema {
    for table in schema.values_mut() {
        table.policies = TablePolicies::default();
    }
    schema
}

/// The lens operations that add, or remove, every system table of `schema`.
fn fragment_ops(schema: &Schema, add: bool) -> Vec<jazz::tools::LensOp> {
    schema
        .iter()
        .filter(|(name, _)| system_tables::is_system_table(name.as_str()))
        .map(|(name, table)| {
            if add {
                jazz::tools::LensOp::AddTable {
                    table: name.to_string(),
                    schema: table.clone(),
                }
            } else {
                jazz::tools::LensOp::RemoveTable {
                    table: name.to_string(),
                    schema: table.clone(),
                }
            }
        })
        .collect()
}

/// Once any version of an app carries the system tables, every later version
/// does: a history in which a version without them follows one with them is
/// refused (bringing them back would store them as new tables and strand the
/// system rows of the first), while a later version that keeps them and
/// declares no encryption, as an app that stops encrypting does, is accepted.
///
/// ```text
/// v1 fragment ─► v2 no fragment ─► v3 fragment    ✗ (v2 drops them)
/// v1 fragment ─► v2 fragment, declares nothing    ✓
/// ```
#[test]
fn a_later_version_carries_the_system_tables_its_predecessor_has() {
    let fragment = structural_of(with_e2ee_fragment(&app()));
    let mut with_notes = fragment.clone();
    with_notes.insert(
        TableName::from("notes"),
        TableSchemaBuilder::new("notes")
            .column("body", ColumnType::Text)
            .build(),
    );
    let without = structural_of(app());
    let add_notes = vec![jazz::tools::LensOp::AddTable {
        table: "notes".to_owned(),
        schema: with_notes[&TableName::from("notes")].clone(),
    }];

    let error = deploy_history(
        vec![
            (fragment.clone(), true),
            (without.clone(), false),
            (fragment.clone(), true),
        ],
        vec![
            fragment_ops(&fragment, false),
            fragment_ops(&fragment, true),
        ],
    )
    .expect_err("the system tables dropped, then brought back");
    assert!(
        matches!(error, DeploymentError::InvalidMigration { .. })
            && error.to_string().contains("every later version does"),
        "{error}"
    );
    deploy_history(
        vec![(fragment.clone(), true), (without, false)],
        vec![fragment_ops(&fragment, false)],
    )
    .expect_err("a later version without them");

    let prepared = deploy_history(
        vec![(fragment.clone(), true), (with_notes, false)],
        vec![add_notes],
    )
    .expect("a later version that keeps them and declares no encryption");
    // Its system tables get their derived policies, from the declarations
    // of the version it follows: its spaces are still there.
    let members = &prepared.active_schema().public_schema()[&TableName::from(MEMBERS)];
    assert!(
        members.policies.select.using.is_some(),
        "{:?}",
        members.policies
    );
}

/// An app whose later version declares no encryption keeps the system tables
/// under their own stored names, so the system rows written under the first
/// version stay readable.
#[test]
fn a_later_version_without_declarations_keeps_the_system_rows() {
    use jazz::protocol::MigrationLens;
    let world = World::new();
    let p = new_row();
    let member = new_row();
    world
        .commit(
            alice(),
            &[
                ("projects", p, project(alice())),
                (MEMBERS, member, world.member(p, alice())),
            ],
        )
        .unwrap();
    let before = world.server.stored_column_for_test(MEMBERS, "scopeId");
    let first = schema();
    let mut next = public_schema_with(&Default::default());
    next.insert(
        TableName::from("notes"),
        TableSchemaBuilder::new("notes")
            .column("body", ColumnType::Text)
            .policies(common::allow_all_policies())
            .build(),
    );
    let second = common::compile_schema(&next);
    let lens = MigrationLens::new(first.version_id(), second.version_id(), vec![]).unwrap();
    block_on(world.server.admit_schema_with_lenses(second, &[lens])).unwrap();
    assert_eq!(
        world.server.stored_column_for_test(MEMBERS, "scopeId"),
        before,
        "the system table keeps its stored name"
    );
    assert!(world.server.holds_row_for_test(MEMBERS, member));
    assert_eq!(world.reads(alice(), MEMBERS), vec![member]);
}
