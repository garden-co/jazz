//! Policies that reach a row by a two-column address (a table identity and
//! a row id), as Jazz's system tables do: `InheritsFrom`, `table_id('T')`,
//! the creator clause's `InsertedByThisTransaction`, and deletes judged on
//! the unit's pre-state (SPEC 7, "Policy evidence inside a transaction").
//!
//! The tables are ordinary tables declaring such policies. The members table
//! carries the system-table prefix only because schema validation accepts an
//! undeclared `uuid` address correlation for that table class alone.
//!
//! ```text
//! projects ◄──(scopeId = table_id('projects'), identifier = id)── __e2ee_members
//!    │  read:   member(session) and not hidden
//!    │  update: an admin (USING), and the name is not "frozen" (WITH CHECK)
//!    ▼
//! admins (project, account)
//! ```
//!
//! Every unit is decided twice over: staged at the authority as a session
//! (the eager path) and uploaded from a replica (the authority's upload
//! path), in each transaction kind and both write orders.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, ReadOpts, ReadTier, RestoreOptions,
    UpsertOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, OpenTransactionId, Operation, PolicyExpr, PolicyValue, SchemaBuilder,
    TablePolicies, TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema, session_eq};

const MEMBERS: &str = "__e2ee_members";

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec!["user".to_owned()])
}

fn this(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), column.to_owned()])
}

fn table_id(table: &str) -> PolicyValue {
    PolicyValue::TableIdentity {
        table: table.to_owned(),
    }
}

fn cmp(column: &str, value: PolicyValue) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    }
}

fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::And(conditions)),
    }
}

/// `M(projects)`: a member row of this project for the session.
fn member_of_this_project() -> PolicyExpr {
    exists(
        MEMBERS,
        vec![
            cmp("scopeId", table_id("projects")),
            cmp("identifier", this("id")),
            cmp("accountId", me()),
        ],
    )
}

/// `MEMBER(session)` on a member row: the session's member row of the
/// same space.
fn member_of_same_space() -> PolicyExpr {
    exists(
        MEMBERS,
        vec![
            cmp("scopeId", this("scopeId")),
            cmp("identifier", this("identifier")),
            cmp("accountId", me()),
        ],
    )
}

fn inherits_from(operation: Operation) -> PolicyExpr {
    PolicyExpr::InheritsFrom {
        operation,
        table_column: "scopeId".to_owned(),
        id_column: "identifier".to_owned(),
        tables: vec!["projects".to_owned()],
    }
}

/// read(row) and update(row).
fn manages() -> PolicyExpr {
    PolicyExpr::And(vec![
        inherits_from(Operation::Select),
        inherits_from(Operation::Update),
    ])
}

/// The creator clause: the session adds itself to a project it creates in
/// this very unit.
fn creator_clause() -> PolicyExpr {
    PolicyExpr::And(vec![
        cmp("accountId", me()),
        cmp("scopeId", table_id("projects")),
        exists(
            "projects",
            vec![
                cmp("id", this("identifier")),
                session_eq("$createdBy.account", &["user"]),
                PolicyExpr::InsertedByThisTransaction,
            ],
        ),
    ])
}

fn is_admin() -> PolicyExpr {
    exists(
        "admins",
        vec![cmp("project", this("id")), cmp("account", me())],
    )
}

fn schema(creator: bool) -> JazzSchema {
    let member_insert = if creator {
        PolicyExpr::Or(vec![manages(), creator_clause()])
    } else {
        manages()
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .column_with_default("hidden", ColumnType::Boolean, false.into())
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::And(vec![
                                member_of_this_project(),
                                PolicyExpr::eq_literal("hidden", false.into()),
                            ]))
                            .with_insert(PolicyExpr::True)
                            .with_update(
                                Some(is_admin()),
                                PolicyExpr::And(vec![
                                    is_admin(),
                                    PolicyExpr::Cmp {
                                        column: "name".to_owned(),
                                        op: CmpOp::Ne,
                                        value: PolicyValue::Literal("frozen".into()),
                                    },
                                ]),
                            )
                            .with_delete(is_admin()),
                    ),
            )
            // Not a candidate: its rows never stand for a project.
            .table(
                TableSchemaBuilder::new("boards")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("admins")
                    .fk_column("project", "projects")
                    .column("account", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(MEMBERS)
                    .column("scopeId", ColumnType::Uuid)
                    .column("identifier", ColumnType::Uuid)
                    .column("accountId", ColumnType::Uuid)
                    .composite_index(["scopeId", "identifier", "accountId"])
                    .index_only(["accountId"])
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::Or(vec![
                                cmp("accountId", me()),
                                member_of_same_space(),
                            ]))
                            .with_insert(member_insert)
                            .with_delete(PolicyExpr::Or(vec![cmp("accountId", me()), manages()])),
                    ),
            )
            .build(),
    )
}

/// How a unit reaches the authority's decision.
#[derive(Clone, Copy, Debug)]
enum Via {
    /// Staged at the authority as the session, through a mergeable handle.
    AuthorityMergeable,
    /// Staged at the authority as the session, through a serializable handle.
    AuthoritySerializable,
    /// A replica's `Db::transaction`, uploaded.
    UploadCallback,
    /// A replica's mergeable handle, uploaded.
    UploadMergeable,
    /// A replica's serializable handle, uploaded.
    UploadSerializable,
}

const EVERY_VIA: [Via; 5] = [
    Via::AuthorityMergeable,
    Via::AuthoritySerializable,
    Via::UploadCallback,
    Via::UploadMergeable,
    Via::UploadSerializable,
];

type Cells = BTreeMap<String, Value>;

#[derive(Clone)]
enum Write {
    Insert(&'static str, RowUuid, Cells),
    Upsert(&'static str, RowUuid, Cells),
    Delete(&'static str, RowUuid),
    /// Staged at the authority only: a replica restores only a row it holds.
    Restore(&'static str, RowUuid),
}

struct World {
    server: Db,
    creator: bool,
    replicas: RefCell<HashMap<AuthorSubject, std::rc::Rc<Db>>>,
    /// Each replica's subscriptions, kept open so it holds the rows it acts
    /// on (a serializable delete names the version it read).
    streams: RefCell<Vec<jazz::db::SubscriptionStream>>,
    next_node: RefCell<u8>,
    projects: Value,
    boards: Value,
}

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

fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

impl World {
    fn new(creator: bool) -> Self {
        let server = block_on(Db::open_history_complete(DbConfig::new(
            schema(creator),
            DbIdentity {
                node: NodeUuid::from_bytes([0xee; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let identity = |table: &str| Value::Uuid(server.table_identity(table).unwrap().unwrap().0);
        let (projects, boards) = (identity("projects"), identity("boards"));
        Self {
            server,
            creator,
            replicas: RefCell::default(),
            streams: RefCell::default(),
            next_node: RefCell::new(1),
            projects,
            boards,
        }
    }

    /// `who`'s replica, connected to the authority as `who`.
    fn replica(&self, who: AuthorSubject) -> std::rc::Rc<Db> {
        if let Some(db) = self.replicas.borrow().get(&who) {
            return db.clone();
        }
        let node = {
            let mut next = self.next_node.borrow_mut();
            *next += 1;
            *next
        };
        let db = std::rc::Rc::new(
            block_on(Db::open(DbConfig::new(
                schema(self.creator),
                DbIdentity {
                    node: NodeUuid::from_bytes([node; 16]),
                    author: who,
                },
            )))
            .unwrap(),
        );
        let (client_side, server_side) = duplex();
        block_on(db.connect_upstream(client_side));
        self.server.accept_subscriber(server_side, who).unwrap();
        for table in [MEMBERS, "projects"] {
            let prepared = db.prepare_query(&Query::from(table)).unwrap();
            let stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
            self.streams.borrow_mut().push(stream);
        }
        self.replicas.borrow_mut().insert(who, db.clone());
        db
    }

    /// Let `replica` catch up with the authority.
    fn pump(&self, replica: &Db) {
        for _ in 0..10 {
            block_on(replica.tick()).unwrap();
            block_on(self.server.tick()).unwrap();
        }
    }

    /// A project row, written by the system: nobody created it.
    fn seed_project(&self, row: RowUuid) {
        self.system_insert("projects", row, project_cells("p"));
    }

    fn seed_admin(&self, project: RowUuid, who: AuthorSubject) {
        self.system_insert(
            "admins",
            RowUuid(uuid::Uuid::new_v4()),
            BTreeMap::from([
                ("project".to_owned(), uuid(project)),
                ("account".to_owned(), account(who)),
            ]),
        );
    }

    fn seed_member(&self, project: RowUuid, who: AuthorSubject) -> RowUuid {
        let row = RowUuid(uuid::Uuid::new_v4());
        self.system_insert(MEMBERS, row, self.member_cells(project, who));
        row
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

    fn member_cells(&self, project: RowUuid, who: AuthorSubject) -> Cells {
        self.address_cells(&self.projects, project, who)
    }

    fn address_cells(&self, scope: &Value, identifier: RowUuid, who: AuthorSubject) -> Cells {
        BTreeMap::from([
            ("scopeId".to_owned(), scope.clone()),
            ("identifier".to_owned(), uuid(identifier)),
            ("accountId".to_owned(), account(who)),
        ])
    }

    /// The member rows of `project` the authority holds, by account.
    fn members_of(&self, project: RowUuid) -> Vec<Value> {
        let prepared = self.server.prepare_query(&Query::from(MEMBERS)).unwrap();
        let mut accounts: Vec<Value> = block_on(self.server.all(&prepared, ReadOpts::default()))
            .unwrap()
            .into_iter()
            .filter(|row| row.cell("identifier") == Some(uuid(project)))
            .filter_map(|row| row.cell("accountId"))
            .collect();
        accounts.sort_by_key(|value| format!("{value:?}"));
        accounts
    }

    fn has_row(&self, table: &str, row: RowUuid) -> bool {
        let prepared = self.server.prepare_query(&Query::from(table)).unwrap();
        block_on(self.server.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .any(|found| found.row_uuid() == row)
    }

    /// Commit `writes` as one unit by `who`, decided by the authority as
    /// `via` says.
    fn commit(
        &self,
        who: AuthorSubject,
        via: Via,
        writes: &[Write],
    ) -> Result<(), jazz::db::Error> {
        match via {
            Via::AuthorityMergeable | Via::AuthoritySerializable => {
                let id = OpenTransactionId::new();
                if matches!(via, Via::AuthorityMergeable) {
                    block_on(self.server.begin_mergeable(id)).unwrap();
                } else {
                    block_on(self.server.begin_serializable(id)).unwrap();
                }
                let identity = WriteIdentity::Session(who);
                for write in writes {
                    let staged = stage(&self.server, id, write, identity);
                    if let Err(error) = staged {
                        let _ = self.server.abandon_transaction_handle(id);
                        return Err(error);
                    }
                }
                if matches!(via, Via::AuthorityMergeable) {
                    block_on(self.server.commit_mergeable_handle(id)).map(drop)
                } else {
                    block_on(self.server.commit_serializable_handle(id)).map(drop)
                }
            }
            Via::UploadCallback => {
                let replica = self.replica(who);
                self.pump(&replica);
                let writes = writes.to_vec();
                let ((), receipt) = block_on(replica.transaction(async move |tx| {
                    for write in &writes {
                        match write.clone() {
                            Write::Insert(table, row, cells) => {
                                tx.insert(table, cells, insert_options(row)).await?;
                            }
                            Write::Upsert(table, row, cells) => {
                                tx.upsert(table, row, cells, UpsertOptions::default())
                                    .await?;
                            }
                            Write::Delete(table, row) => {
                                tx.delete(table, row, DeleteOptions::default()).await?;
                            }
                            Write::Restore(..) => unreachable!("staged at the authority only"),
                        }
                    }
                    Ok(())
                }))?;
                self.settle(&replica, receipt.0)
            }
            Via::UploadMergeable | Via::UploadSerializable => {
                let replica = self.replica(who);
                self.pump(&replica);
                let id = OpenTransactionId::new();
                if matches!(via, Via::UploadMergeable) {
                    block_on(replica.begin_mergeable(id)).unwrap();
                } else {
                    block_on(replica.begin_serializable(id)).unwrap();
                }
                for write in writes {
                    stage(&replica, id, write, WriteIdentity::Database)?;
                }
                let receipt = if matches!(via, Via::UploadMergeable) {
                    block_on(replica.commit_mergeable_handle(id))?
                } else {
                    block_on(replica.commit_serializable_handle(id))?
                };
                self.settle(&replica, receipt.0)
            }
        }
    }

    /// The authority's verdict on the replica's commit `tx`.
    fn settle(&self, replica: &Db, tx: u64) -> Result<(), jazz::db::Error> {
        drive(
            &[replica, &self.server, replica],
            replica.wait_for_transaction(tx, DurabilityTier::Global),
        )
        .map(drop)
    }
}

/// Whether `via` stages the unit write by write at the authority.
fn staged_at_authority(via: Via) -> bool {
    matches!(via, Via::AuthorityMergeable | Via::AuthoritySerializable)
}

fn insert_options(row: RowUuid) -> InsertOptions {
    InsertOptions {
        row_id: Some(row),
        ..Default::default()
    }
}

fn stage(
    db: &Db,
    id: OpenTransactionId,
    write: &Write,
    identity: WriteIdentity,
) -> Result<(), jazz::db::Error> {
    let tx = db.tx_ref(id);
    match write.clone() {
        Write::Insert(table, row, cells) => block_on(tx.insert(
            table,
            cells,
            InsertOptions {
                row_id: Some(row),
                identity,
                ..Default::default()
            },
        ))
        .map(drop),
        Write::Upsert(table, row, cells) => block_on(tx.upsert(
            table,
            row,
            cells,
            UpsertOptions {
                identity,
                ..Default::default()
            },
        ))
        .map(drop),
        Write::Delete(table, row) => block_on(tx.delete(
            table,
            row,
            DeleteOptions {
                identity,
                ..Default::default()
            },
        ))
        .map(drop),
        Write::Restore(table, row) => block_on(tx.restore(
            table,
            row,
            None,
            RestoreOptions {
                identity,
                ..Default::default()
            },
        ))
        .map(drop),
    }
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn project_cells(name: &str) -> Cells {
    BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))])
}

/// The refusal is the authority's policy decision, not anything else.
#[track_caller]
fn assert_denied(result: Result<(), jazz::db::Error>, what: &str) {
    let error = result.expect_err(what);
    assert!(
        error.message.contains("policy") || error.message.contains("denied"),
        "{what}: {error:?}"
    );
}

/// An updater of a project, who reads it through their member row, adds a
/// member; a member who may not update the project cannot. The address
/// must name the project by its table's identity: the same row id under
/// another table's identity, or a missing or deleted project, admits
/// nothing.
///
/// ```text
/// alice (member, admin) ──add bob──► authority ── IF(Select) ∧ IF(Update) ──✓
/// bob (member)          ──add carol─► authority ── IF(Update) ──✗
/// alice ──(scopeId = table_id('boards'), identifier = project)──► ✗
/// ```
#[test]
fn a_member_row_is_admitted_through_the_project_s_read_and_update_policies() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = id(0x10);
        world.seed_project(project);
        world.seed_admin(project, alice());
        world.seed_member(project, alice());

        let add = |who: AuthorSubject| {
            Write::Insert(
                MEMBERS,
                RowUuid(uuid::Uuid::new_v4()),
                world.member_cells(project, who),
            )
        };
        world
            .commit(alice(), via, &[add(bob())])
            .unwrap_or_else(|e| panic!("{via:?}: an updater adds a member: {e:?}"));
        assert_denied(
            world.commit(bob(), via, &[add(carol())]),
            &format!("{via:?}: a member who may not update adds nobody"),
        );

        // The project's id under another table's identity names no project.
        let elsewhere = Write::Insert(
            MEMBERS,
            RowUuid(uuid::Uuid::new_v4()),
            world.address_cells(&world.boards, project, carol()),
        );
        assert_denied(
            world.commit(alice(), via, &[elsewhere]),
            &format!("{via:?}: another table's identity"),
        );
        // A row of a table that is no candidate, whatever its policies.
        let board = id(0x20);
        world.system_insert("boards", board, project_cells("b"));
        let board_member = Write::Insert(
            MEMBERS,
            RowUuid(uuid::Uuid::new_v4()),
            world.address_cells(&world.boards, board, carol()),
        );
        assert_denied(
            world.commit(alice(), via, &[board_member]),
            &format!("{via:?}: a board is no project"),
        );
        // A missing project, and a deleted one.
        assert_denied(
            world.commit(
                alice(),
                via,
                &[Write::Insert(
                    MEMBERS,
                    RowUuid(uuid::Uuid::new_v4()),
                    world.member_cells(id(0x11), carol()),
                )],
            ),
            &format!("{via:?}: a missing project"),
        );
        block_on(
            world
                .server
                .delete("projects", project, DeleteOptions::default()),
        )
        .unwrap();
        assert_denied(
            world.commit(alice(), via, &[add(carol())]),
            &format!("{via:?}: a deleted project"),
        );
        assert_eq!(
            world.members_of(project),
            {
                let mut accounts = vec![account(alice()), account(bob())];
                accounts.sort_by_key(|value| format!("{value:?}"));
                accounts
            },
            "{via:?}"
        );
    }
}

/// update(row) is both clauses of the project's update policy on the same
/// image, a save that changes nothing: an admin of a frozen project passes
/// its `USING` but not its `WITH CHECK`, so manages no members of it.
#[test]
fn update_means_both_clauses_of_the_project_s_update_policy() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = id(0x10);
        world.system_insert("projects", project, project_cells("frozen"));
        world.seed_admin(project, alice());
        world.seed_member(project, alice());
        assert_denied(
            world.commit(
                alice(),
                via,
                &[Write::Insert(
                    MEMBERS,
                    RowUuid(uuid::Uuid::new_v4()),
                    world.member_cells(project, bob()),
                )],
            ),
            &format!("{via:?}: the WITH CHECK clause fails"),
        );
        // Unfrozen, the same admin adds the member.
        block_on(world.server.update(
            "projects",
            project,
            project_cells("open"),
            Default::default(),
        ))
        .unwrap();
        world
            .commit(
                alice(),
                via,
                &[Write::Insert(
                    MEMBERS,
                    RowUuid(uuid::Uuid::new_v4()),
                    world.member_cells(project, bob()),
                )],
            )
            .unwrap_or_else(|e| panic!("{via:?}: {e:?}"));
    }
}

/// A new project and its creator's member row in one unit are accepted in
/// either order and every kind of transaction: once the project row's own
/// insert check passes it is evidence, and the creator clause reads it as
/// a row this unit inserted. Read(row) alone could never admit the first
/// member, whose own row is what read(row) needs.
///
/// ```text
/// alice ──{ insert project P, insert member(P, alice) }──► authority
///            round 1: P ✓ (grounded)   member ✗ (P not yet evidence)
///            round 2: member ✓ (creator clause: P inserted by this unit)
/// ```
///
/// A unit staged write by write at the authority checks each write as it
/// is staged, on the writes staged before it, so there the member row must
/// come second: staged first, it is refused at once (as any write staged
/// before its evidence is, `transaction_policy_exists`). An uploaded unit
/// is decided as a whole, in either order.
#[test]
fn a_creator_adds_itself_in_the_unit_that_creates_the_project() {
    for via in EVERY_VIA {
        for member_first in [false, true] {
            let world = World::new(true);
            let project = RowUuid(uuid::Uuid::new_v4());
            let mut writes = vec![
                Write::Insert("projects", project, project_cells("new")),
                Write::Insert(
                    MEMBERS,
                    RowUuid(uuid::Uuid::new_v4()),
                    world.member_cells(project, alice()),
                ),
            ];
            if member_first {
                writes.reverse();
            }
            if member_first && staged_at_authority(via) {
                assert_denied(
                    world.commit(alice(), via, &writes),
                    &format!("{via:?}: staged before its project"),
                );
                continue;
            }
            world
                .commit(alice(), via, &writes)
                .unwrap_or_else(|e| panic!("{via:?} member first {member_first}: {e:?}"));
            assert_eq!(world.members_of(project), vec![account(alice())]);
        }
    }
}

/// A project created by an upsert in the unit counts as inserted by it.
#[test]
fn an_upsert_that_creates_the_project_admits_its_creator() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = RowUuid(uuid::Uuid::new_v4());
        world
            .commit(
                alice(),
                via,
                &[
                    Write::Upsert("projects", project, project_cells("new")),
                    Write::Insert(
                        MEMBERS,
                        RowUuid(uuid::Uuid::new_v4()),
                        world.member_cells(project, alice()),
                    ),
                ],
            )
            .unwrap_or_else(|e| panic!("{via:?}: {e:?}"));
    }
}

/// Without the creator clause, the same unit is refused in either order and
/// every kind: no write is evidence for its own check, and the project's
/// read rule needs the very member row being checked.
#[test]
fn without_the_creator_clause_the_first_member_is_refused() {
    for via in EVERY_VIA {
        for member_first in [false, true] {
            let world = World::new(false);
            let project = RowUuid(uuid::Uuid::new_v4());
            let mut writes = vec![
                Write::Insert("projects", project, project_cells("new")),
                Write::Insert(
                    MEMBERS,
                    RowUuid(uuid::Uuid::new_v4()),
                    world.member_cells(project, alice()),
                ),
            ];
            if member_first {
                writes.reverse();
            }
            assert_denied(
                world.commit(alice(), via, &writes),
                &format!("{via:?} member first {member_first}"),
            );
            assert!(!world.has_row("projects", project), "{via:?}");
        }
    }
}

/// The creator clause holds only in the unit that inserts the project: not
/// in a later one (also straight after, after removal, and after the
/// project is restored), not for another account, and never for a session
/// that neither created nor may update the project.
#[test]
fn the_creator_clause_holds_only_in_the_creating_unit() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = RowUuid(uuid::Uuid::new_v4());
        let join = |who: AuthorSubject| {
            Write::Insert(
                MEMBERS,
                RowUuid(uuid::Uuid::new_v4()),
                world.member_cells(project, who),
            )
        };
        // Created alone: the creator is not a member yet, and a later unit
        // (straight after) cannot add it.
        world
            .commit(
                alice(),
                via,
                &[Write::Insert("projects", project, project_cells("new"))],
            )
            .unwrap_or_else(|e| panic!("{via:?}: {e:?}"));
        assert_denied(
            world.commit(alice(), via, &[join(alice())]),
            &format!("{via:?}: a later unit"),
        );
        // The creator may add only itself by the clause.
        let other = RowUuid(uuid::Uuid::new_v4());
        assert_denied(
            world.commit(
                alice(),
                via,
                &[
                    Write::Insert("projects", other, project_cells("other")),
                    Write::Insert(
                        MEMBERS,
                        RowUuid(uuid::Uuid::new_v4()),
                        world.member_cells(other, bob()),
                    ),
                ],
            ),
            &format!("{via:?}: the creator adding another account"),
        );
        // Neither creator nor updater.
        assert_denied(
            world.commit(bob(), via, &[join(bob())]),
            &format!("{via:?}: a stranger adding itself"),
        );
        // Removed (by the system), the creator cannot come back.
        let row = world.seed_member(project, alice());
        block_on(world.server.delete(MEMBERS, row, DeleteOptions::default())).unwrap();
        assert_denied(
            world.commit(alice(), via, &[join(alice())]),
            &format!("{via:?}: after removal"),
        );
        // A restored project is no fresh insert.
        block_on(
            world
                .server
                .delete("projects", project, DeleteOptions::default()),
        )
        .unwrap();
        block_on(
            world
                .server
                .restore("projects", project, None, RestoreOptions::default()),
        )
        .unwrap();
        assert_denied(
            world.commit(alice(), via, &[join(alice())]),
            &format!("{via:?}: after a restore"),
        );
        assert!(world.members_of(project).is_empty(), "{via:?}");
    }
}

/// A project that existed before its creator's member row gives its
/// creator nothing; an updater adds the creator.
#[test]
fn an_existing_project_s_creator_is_added_by_an_updater() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = RowUuid(uuid::Uuid::new_v4());
        // Alice created it, earlier.
        block_on(world.server.insert(
            "projects",
            project_cells("old"),
            InsertOptions {
                row_id: Some(project),
                identity: WriteIdentity::Session(alice()),
                ..Default::default()
            },
        ))
        .unwrap();
        let join = Write::Insert(
            MEMBERS,
            RowUuid(uuid::Uuid::new_v4()),
            world.member_cells(project, alice()),
        );
        assert_denied(
            world.commit(alice(), via, std::slice::from_ref(&join)),
            &format!("{via:?}: an earlier creator"),
        );
        world.seed_admin(project, bob());
        world.seed_member(project, bob());
        world
            .commit(bob(), via, &[join])
            .unwrap_or_else(|e| panic!("{via:?}: {e:?}"));
    }
}

/// Deletes are judged on the unit's pre-state: an updater deletes its own
/// member row and another member's in one unit, in either order. Judged as
/// the unit goes, the second delete would read a project the first one
/// already hid from it.
///
/// ```text
/// alice (admin, member) ──{ delete alice's row, delete bob's row }──► ✓
///                          both read the members as before the unit
/// ```
#[test]
#[ignore = "needs pre-state delete checks from main"]
fn an_updater_removes_itself_and_another_member_in_one_unit() {
    for via in EVERY_VIA {
        for own_first in [true, false] {
            let world = World::new(true);
            let project = id(0x10);
            world.seed_project(project);
            world.seed_admin(project, alice());
            let own = world.seed_member(project, alice());
            let other = world.seed_member(project, bob());
            let mut writes = vec![Write::Delete(MEMBERS, own), Write::Delete(MEMBERS, other)];
            if !own_first {
                writes.reverse();
            }
            world
                .commit(alice(), via, &writes)
                .unwrap_or_else(|e| panic!("{via:?} own first {own_first}: {e:?}"));
            assert!(world.members_of(project).is_empty(), "{via:?}");
        }
    }
}

/// A member who may not update the project deleting its own row and
/// another's is refused in either order: its own delete is allowed (anyone
/// leaves), the other's never is, and neither is judged on what the other
/// did.
#[test]
fn a_non_updater_cannot_remove_another_member_in_either_order() {
    for via in EVERY_VIA {
        for own_first in [true, false] {
            let world = World::new(true);
            let project = id(0x10);
            world.seed_project(project);
            let own = world.seed_member(project, bob());
            let other = world.seed_member(project, carol());
            let mut writes = vec![Write::Delete(MEMBERS, own), Write::Delete(MEMBERS, other)];
            if !own_first {
                writes.reverse();
            }
            assert_denied(
                world.commit(bob(), via, &writes),
                &format!("{via:?} own first {own_first}"),
            );
            assert_eq!(world.members_of(project).len(), 2, "{via:?}");
        }
    }
}

/// Any member leaves, also one the project's further read conjunct hides it
/// from; such a member still removes nobody else.
#[test]
fn a_member_leaves_even_when_it_may_not_read_the_project() {
    for via in EVERY_VIA {
        for hidden in [false, true] {
            let world = World::new(true);
            let project = id(0x10);
            world.system_insert(
                "projects",
                project,
                BTreeMap::from([
                    ("name".to_owned(), Value::String("p".into())),
                    ("hidden".to_owned(), Value::Bool(hidden)),
                ]),
            );
            let own = world.seed_member(project, bob());
            let other = world.seed_member(project, carol());
            assert_denied(
                world.commit(bob(), via, &[Write::Delete(MEMBERS, other)]),
                &format!("{via:?} hidden {hidden}: removing another"),
            );
            world
                .commit(bob(), via, &[Write::Delete(MEMBERS, own)])
                .unwrap_or_else(|e| panic!("{via:?} hidden {hidden}: leaving: {e:?}"));
            assert_eq!(world.members_of(project), vec![account(carol())]);
        }
    }
}

/// A table identity is the authority's: a replica binds none until its
/// catalogue mirror confirms the names, and a policy comparing with one
/// grants nothing meanwhile (fails closed). Once confirmed, the same read
/// shows the row.
#[test]
fn an_unknown_table_identity_grants_nothing_until_the_names_are_confirmed() {
    let world = World::new(true);
    let replica = block_on(Db::open(DbConfig::new(
        schema(true),
        DbIdentity {
            node: NodeUuid::from_bytes([0x33; 16]),
            author: alice(),
        },
    )))
    .unwrap();
    replica.become_replica();
    let project = id(0x10);
    // The replica holds the project and alice's member row, written in one
    // unit before it ever met its authority.
    let member = world.member_cells(project, alice());
    block_on(replica.transaction(async move |tx| {
        tx.insert("projects", project_cells("p"), insert_options(project))
            .await?;
        tx.insert(
            MEMBERS,
            member,
            insert_options(RowUuid(uuid::Uuid::new_v4())),
        )
        .await?;
        Ok(())
    }))
    .unwrap();
    let projects = replica.prepare_query(&Query::from("projects")).unwrap();
    let read = |db: &Db| {
        block_on(db.all_for_identity(
            &projects,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            alice(),
        ))
        .unwrap()
        .len()
    };
    assert_eq!(
        read(&replica),
        0,
        "no identity known: M(projects) holds for no row"
    );
    let (client_side, server_side) = duplex();
    let _upstream = block_on(replica.connect_upstream(client_side));
    world
        .server
        .accept_subscriber(server_side, alice())
        .unwrap();
    let _stream = block_on(replica.subscribe(&projects, ReadOpts::default())).unwrap();
    for _ in 0..40 {
        block_on(replica.tick()).unwrap();
        block_on(world.server.tick()).unwrap();
    }
    assert!(
        world.has_row("projects", project),
        "the authority accepted the unit"
    );
    assert!(replica.table_identity("projects").unwrap().is_some());
    assert_eq!(read(&replica), 1, "the confirmed identity binds");
}

/// A project brought back in the unit is no fresh insert: deleted and
/// upserted again, or restored, it does not admit its creator by the
/// creator clause in that unit.
///
/// ```text
/// alice creates P (an admin of it, not a member)
/// alice ──{ delete P, upsert P, insert member(P, alice) }──► ✗
/// system deletes P
/// alice ──{ restore P, insert member(P, alice) }──► ✗   (staged at the authority)
/// ```
#[test]
fn a_project_brought_back_in_the_unit_does_not_admit_its_creator() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = RowUuid(uuid::Uuid::new_v4());
        world
            .commit(
                alice(),
                via,
                &[Write::Insert("projects", project, project_cells("new"))],
            )
            .unwrap_or_else(|e| panic!("{via:?}: {e:?}"));
        // An admin may delete and restore it (a restore is checked as an
        // update).
        world.seed_admin(project, alice());
        let join = Write::Insert(
            MEMBERS,
            RowUuid(uuid::Uuid::new_v4()),
            world.member_cells(project, alice()),
        );
        let again = world.commit(
            alice(),
            via,
            &[
                Write::Delete("projects", project),
                Write::Upsert("projects", project, project_cells("again")),
                join.clone(),
            ],
        );
        if matches!(via, Via::UploadSerializable) {
            // Alice's replica cannot hold the project (she may not read
            // it), so her serializable delete conflicts with it before any
            // policy is read.
            assert!(again.is_err(), "{via:?}");
        } else {
            assert_denied(again, &format!("{via:?}: deleted and upserted again"));
        }
        if staged_at_authority(via) {
            block_on(
                world
                    .server
                    .delete("projects", project, DeleteOptions::default()),
            )
            .unwrap();
            assert_denied(
                world.commit(alice(), via, &[Write::Restore("projects", project), join]),
                &format!("{via:?}: a restore"),
            );
        }
        assert!(world.members_of(project).is_empty(), "{via:?}");
    }
}

/// read(row) is the project's read policy exactly as a direct read of it:
/// an updater the project's further read conjunct hides it from manages
/// none of its members.
#[test]
fn an_updater_who_may_not_read_the_project_adds_nobody() {
    for via in EVERY_VIA {
        let world = World::new(true);
        let project = id(0x10);
        world.system_insert(
            "projects",
            project,
            BTreeMap::from([
                ("name".to_owned(), Value::String("p".into())),
                ("hidden".to_owned(), Value::Bool(true)),
            ]),
        );
        world.seed_admin(project, alice());
        world.seed_member(project, alice());
        assert_denied(
            world.commit(
                alice(),
                via,
                &[Write::Insert(
                    MEMBERS,
                    RowUuid(uuid::Uuid::new_v4()),
                    world.member_cells(project, bob()),
                )],
            ),
            &format!("{via:?}"),
        );
    }
}

/// Under `NOT` too, a table identity this store does not know yet grants
/// nothing: `NOT(scopeId = table_id('notes'))` holds for no row until the
/// names are confirmed, then for a row naming another table.
///
/// ```text
/// replica (names unconfirmed): note(scopeId = projects) ── NOT(= notes) ──✗
/// replica (names confirmed):   note(scopeId = projects) ── NOT(= notes) ──✓
/// ```
#[test]
fn an_unknown_table_identity_grants_nothing_under_not() {
    let schema = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("projects")
                        .column("name", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("scopeId", ColumnType::Uuid)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::Not(Box::new(cmp(
                                    "scopeId",
                                    table_id("notes"),
                                ))))
                                .with_insert(PolicyExpr::True),
                        ),
                )
                .build(),
        )
    };
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let projects = Value::Uuid(server.table_identity("projects").unwrap().unwrap().0);
    let replica = block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x33; 16]),
            author: alice(),
        },
    )))
    .unwrap();
    replica.become_replica();
    block_on(replica.insert(
        "notes",
        BTreeMap::from([("scopeId".to_owned(), projects)]),
        insert_options(id(0x20)),
    ))
    .unwrap();
    let notes = replica.prepare_query(&Query::from("notes")).unwrap();
    let read = |db: &Db| {
        block_on(db.all_for_identity(
            &notes,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            alice(),
        ))
        .unwrap()
        .len()
    };
    assert!(!matches!(replica.table_identity("notes"), Ok(Some(_))));
    assert_eq!(
        read(&replica),
        0,
        "no identity known: the negation holds for no row"
    );
    let (client_side, server_side) = duplex();
    let _upstream = block_on(replica.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let _stream = block_on(replica.subscribe(&notes, ReadOpts::default())).unwrap();
    for _ in 0..40 {
        block_on(replica.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(replica.table_identity("notes").unwrap().is_some());
    assert_eq!(read(&replica), 1, "the confirmed identity binds");
}

/// The creator clause's fresh insert is one row on one branch: a unit that
/// inserts project P on branch A does not count as inserting P on branch
/// B, which alice created in an earlier unit.
///
/// ```text
/// earlier: alice inserts P@B
/// alice ──{ insert P@A, insert member(P, ws = B) }──► ✗
/// alice ──{ insert P@A, insert member(P, ws = A) }──► ✓
/// ```
#[test]
fn the_creator_clause_names_the_branch_the_unit_inserted() {
    let ws = |byte: u8| Value::Uuid(uuid::Uuid::from_bytes([byte; 16]));
    let branch = |byte: u8| {
        jazz::db::ExactWriteTarget::Branch(jazz::protocol::BranchSelector::new([("ws", ws(byte))]))
    };
    let creator = PolicyExpr::And(vec![
        cmp("accountId", me()),
        exists(
            "projects",
            vec![
                cmp("id", this("identifier")),
                cmp("ws", this("ws")),
                session_eq("$createdBy.account", &["user"]),
                PolicyExpr::InsertedByThisTransaction,
            ],
        ),
    ]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("ws", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .branch_by("ws")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(MEMBERS)
                    .column("scopeId", ColumnType::Uuid)
                    .column("identifier", ColumnType::Uuid)
                    .column("accountId", ColumnType::Uuid)
                    .column("ws", ColumnType::Uuid)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(creator),
                    ),
            )
            .build(),
    );
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let insert_project = |tx: OpenTransactionId, project: RowUuid, on: u8| {
        block_on(server.tx_ref(tx).insert(
            "projects",
            project_cells("p"),
            InsertOptions {
                row_id: Some(project),
                identity: WriteIdentity::Session(alice()),
                target: branch(on),
                ..Default::default()
            },
        ))
        .map(drop)
    };
    let insert_member = |tx: OpenTransactionId, project: RowUuid, on: u8| {
        block_on(server.tx_ref(tx).insert(
            MEMBERS,
            BTreeMap::from([
                ("scopeId".to_owned(), ws(0)),
                ("identifier".to_owned(), uuid(project)),
                ("accountId".to_owned(), account(alice())),
                ("ws".to_owned(), ws(on)),
            ]),
            InsertOptions {
                identity: WriteIdentity::Session(alice()),
                ..Default::default()
            },
        ))
        .map(drop)
    };
    let unit = |writes: &dyn Fn(OpenTransactionId) -> Result<(), jazz::db::Error>| {
        let tx = OpenTransactionId::new();
        block_on(server.begin_mergeable(tx)).unwrap();
        let staged = writes(tx);
        if staged.is_err() {
            let _ = server.abandon_transaction_handle(tx);
            return staged;
        }
        block_on(server.commit_mergeable_handle(tx)).map(drop)
    };
    let (a, b) = (0x0a, 0x0b);
    // Alone, and among enough other fresh projects that the unit's inserts
    // are bound as one list rather than one by one.
    for (project, others) in [(id(0x30), 0), (id(0x31), 20)] {
        let unit_on = |member_on: u8| {
            unit(&|tx| {
                for _ in 0..others {
                    insert_project(tx, RowUuid(uuid::Uuid::new_v4()), a)?;
                }
                insert_project(tx, project, a)?;
                insert_member(tx, project, member_on)
            })
        };
        unit(&|tx| insert_project(tx, project, b)).unwrap();
        assert_denied(
            unit_on(b),
            &format!("{others} others: P@B was not inserted by this unit"),
        );
        unit_on(a).unwrap_or_else(|e| panic!("{others} others: P@A was: {e:?}"));
    }
}

/// A delete's policy reads the deleted row as it stood before the unit
/// (SPEC 7: `USING` reads committed state): an earlier write of the same
/// unit to that row cannot grant the delete. A row the unit itself inserted
/// has no earlier version and is judged as the unit left it.
#[test]
#[ignore = "needs pre-state delete checks from main"]
fn a_delete_judges_the_row_as_it_stood_before_the_unit() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("owner", ColumnType::Uuid)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(session_eq("owner", &["user"])),
                    ),
            )
            .build(),
    );
    let db = block_on(Db::open_history_complete(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x99; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let as_alice = || WriteIdentity::Session(alice());
    let owned_by = |who| BTreeMap::from([("owner".to_owned(), account(who))]);

    // Bob owns the row before the unit; Alice makes herself the owner and
    // then deletes it in the same unit: refused.
    let row = id(0x77);
    block_on(db.insert("docs", owned_by(bob()), insert_options(row))).unwrap();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    let staged = db.tx_ref(tx);
    block_on(staged.upsert(
        "docs",
        row,
        owned_by(alice()),
        UpsertOptions {
            identity: as_alice(),
            ..Default::default()
        },
    ))
    .unwrap();
    let deleted = block_on(staged.delete(
        "docs",
        row,
        DeleteOptions {
            identity: as_alice(),
            ..Default::default()
        },
    ));
    let _ = db.abandon_transaction_handle(tx);
    assert!(
        deleted.is_err(),
        "the delete is judged on the Bob-owned row, not Alice's staged one"
    );

    // A row Alice inserts and deletes in one unit has no earlier version:
    // judged as the unit left it, the delete is accepted.
    let fresh = id(0x78);
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    let staged = db.tx_ref(tx);
    block_on(staged.insert(
        "docs",
        owned_by(alice()),
        InsertOptions {
            row_id: Some(fresh),
            identity: as_alice(),
            ..Default::default()
        },
    ))
    .unwrap();
    let deleted = block_on(staged.delete(
        "docs",
        fresh,
        DeleteOptions {
            identity: as_alice(),
            ..Default::default()
        },
    ));
    let _ = db.abandon_transaction_handle(tx);
    assert!(deleted.is_ok(), "{deleted:?}");
}
