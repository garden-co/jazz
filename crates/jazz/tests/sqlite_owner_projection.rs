//! Scope-isolated owner pending projections and original transaction aliases.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::{collections::BTreeMap, path::Path};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn bob() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "bob").unwrap()
}
fn config(node: u8, author: AuthorSubject) -> DbConfig {
    versioned_config(node, author, false)
}
fn versioned_config(node: u8, author: AuthorSubject, extended: bool) -> DbConfig {
    let mut builder = SchemaBuilder::new().table(
        TableSchemaBuilder::new("notes")
            .column("text", ColumnType::Text)
            .column("allowed", ColumnType::Boolean)
            .policies(
                TablePolicies::new()
                    .with_select(PolicyExpr::True)
                    .with_insert(PolicyExpr::eq_literal(
                        "allowed",
                        jazz::tools::Value::Boolean(true),
                    ))
                    .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                    .with_delete(PolicyExpr::True),
            ),
    );
    if extended {
        builder = builder.table(
            TableSchemaBuilder::new("labels")
                .column("name", ColumnType::Text)
                .policies(common::allow_all_policies()),
        );
    }
    let schema = common::compile_schema(&builder.build());
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}
fn open(node: u8, author: AuthorSubject, path: Option<&Path>) -> Db {
    let config = config(node, author);
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn authority() -> Db {
    let db = open(90, AuthorSubject::SYSTEM, None);
    db.set_authority();
    db
}
fn connect(client: &Db, server: &Db, identity: AuthorSubject) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, identity);
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn insert(db: &Db, text: &str, allowed: bool) -> u64 {
    block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("text".into(), Value::String(text.into())),
            ("allowed".into(), Value::Bool(allowed)),
        ]),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id()
}
fn texts(db: &Db) -> Vec<String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let mut values = block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            match row
                .cells()
                .find(|(name, _)| *name == "text")
                .unwrap()
                .1
                .unwrap()
            {
                Value::String(text) => text.clone(),
                other => panic!("{other:?}"),
            }
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

/// Alice's private owner can serve her pending write; Bob cannot obtain it even
/// when the application SELECT policy would otherwise allow every session.
#[test]
fn scope_owner_keeps_pending_rows_private() {
    let owner = open(50, alice(), None);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = open(1, alice(), None);
    connect(&tab, &owner, alice());
    insert(&tab, "private", true);
    turns(&[&tab, &owner]);
    assert_eq!(texts(&owner), vec!["private"]);
    let query = owner.prepare_query(&Query::from("notes")).unwrap();
    assert!(block_on(owner.all_for_identity(&query, ReadOpts::default(), bob())).is_err());
    let (mut intruder, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, bob());
    intruder
        .send(jazz::db::SyncMessage::Fetch {
            view: jazz::protocol::ReadViewSourceSpec::Current,
            tier: jazz::db::ReadTier::LocalOnly,
            wait: None,
            id: 7,
            query: Query::from("notes"),
            binding: BTreeMap::new(),
            at: None,
            include_deleted: false,
        })
        .unwrap();
    block_on(owner.tick()).unwrap();
    let replies: Vec<_> = std::iter::from_fn(|| intruder.try_recv()).collect();
    assert!(
        replies
            .iter()
            .any(|r| matches!(r, jazz::db::SyncMessage::Refused { id: 7, .. }))
    );
    assert!(
        !replies
            .iter()
            .any(|r| matches!(r,jazz::db::SyncMessage::Rows{images} if !images.is_empty()))
    );
}

/// Alice reopens her owner with two old tab nodes that allocated equal numeric
/// transaction times. Both private pending rows survive and later settle by
/// their original identities; the authority rejects only the disallowed row.
#[test]
fn owner_reopen_preserves_equal_time_writers_and_independent_fates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner.sqlite");
    let a = open(1, alice(), None);
    let b = open(2, alice(), None);
    let (x, y) = {
        let owner = open(50, alice(), Some(&path));
        owner.set_scope_isolated_storage_owner().unwrap();
        owner.set_remote_link_hint(RemoteLinkHint::Failed);
        connect(&a, &owner, alice());
        connect(&b, &owner, alice());
        let x = insert(&a, "accepted", true);
        let y = insert(&b, "rejected", false);
        assert_eq!(x, y);
        turns(&[&a, &b, &owner]);
        assert_eq!(texts(&owner), vec!["accepted", "rejected"]);
        (x, y)
    };
    let owner = open(50, alice(), Some(&path));
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(texts(&owner), vec!["accepted", "rejected"]);
    let server = authority();
    connect(&owner, &server, alice());
    connect(&a, &owner, alice());
    connect(&b, &owner, alice());
    turns(&[&a, &b, &owner, &server, &owner, &a, &b]);
    assert_eq!(a.write_state(x).unwrap().fate, Fate::Accepted);
    assert!(matches!(b.write_state(y).unwrap().fate, Fate::Rejected(_)));
    assert_eq!(texts(&owner), vec!["accepted"]);
    assert_eq!(texts(&server), vec!["accepted"]);
    block_on(a.wait_for_transaction(x, DurabilityTier::Global)).unwrap();
    assert!(block_on(b.wait_for_transaction(y, DurabilityTier::Global)).is_err());
}

/// Bob cannot turn an authority or SYSTEM backend into Alice's ordinary private
/// owner, nor activate ownership after another session has already attached.
#[test]
fn owner_capability_requires_an_unshared_ordinary_replica() {
    assert!(authority().set_scope_isolated_storage_owner().is_err());
    assert!(
        open(50, AuthorSubject::SYSTEM, None)
            .set_scope_isolated_storage_owner()
            .is_err()
    );
    let tab = open(1, alice(), None);
    tab.set_private_owner_client().unwrap();
    assert!(tab.set_scope_isolated_storage_owner().is_err());
    let owner = open(50, alice(), None);
    let (_, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, bob());
    assert!(owner.set_scope_isolated_storage_owner().is_err());
}

/// Alice's file remembers its private owner role across reopen. Bob cannot open
/// it as his own cache, and Alice need not call the activation method again.
#[test]
fn persisted_scope_refuses_another_subject_before_opening_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scope.sqlite");
    let tab = open(1, alice(), None);
    {
        let owner = open(50, alice(), Some(&path));
        owner.set_scope_isolated_storage_owner().unwrap();
        owner.set_remote_link_hint(RemoteLinkHint::Failed);
        connect(&tab, &owner, alice());
        insert(&tab, "private durable", true);
        turns(&[&tab, &owner]);
    }
    assert!(block_on(Db::open_at(config(51, bob()), &path, Default::default())).is_err());
    let owner = open(50, alice(), Some(&path));
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(texts(&owner), vec!["private durable"]);
    let q = owner.prepare_query(&Query::from("notes")).unwrap();
    assert!(block_on(owner.all_for_identity(&q, ReadOpts::default(), bob())).is_err());
}

/// Bob's injected storage failure happens after Alice's fate was applied in
/// memory but before its enclosing transaction commits. The owner fails closed;
/// reopening restores the still-pending journal instead of publishing half a fate.
/// Raw SQL is used only to inject the persistence failure absent from public APIs.
#[test]
fn failed_fate_application_requires_owner_reopen_and_recovers_the_projection() {
    use jazz::db::SyncMessage;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("failed-fate.sqlite");
    let tab = open(1, alice(), None);
    let owner = open(50, alice(), Some(&path));
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    connect(&tab, &owner, alice());
    let tx = insert(&tab, "pending", true);
    turns(&[&tab, &owner]);
    let (link, mut upstream) = duplex_transport::duplex();
    block_on(owner.connect_upstream(link));
    turns(&[&owner]);
    while upstream.try_recv().is_some() {}
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_owner_fate BEFORE INSERT ON _jazz_meta WHEN NEW.k='known' BEGIN SELECT RAISE(ABORT,'injected owner fate failure'); END").unwrap();
    upstream
        .send(SyncMessage::FateV1 {
            node: [1; 16],
            tx,
            through: 7,
            result: Ok(()),
        })
        .unwrap();
    assert!(block_on(owner.tick()).is_err());
    let q = owner.prepare_query(&Query::from("notes")).unwrap();
    assert!(block_on(owner.all(&q, ReadOpts::default())).is_err());
    assert_eq!(
        sql.query_row("SELECT pending FROM _jazz_relay_upload_v1", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    sql.execute_batch("DROP TRIGGER fail_owner_fate").unwrap();
    drop(owner);
    drop(upstream);
    drop(sql);
    let owner = open(50, alice(), Some(&path));
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(texts(&owner), vec!["pending"]);
    let server = authority();
    connect(&owner, &server, alice());
    connect(&tab, &owner, alice());
    turns(&[&tab, &owner, &server, &owner, &tab]);
    assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Accepted);
    assert_eq!(texts(&owner), vec!["pending"]);
}

/// Alice cannot turn her private pending cache into Bob's global authority or
/// an ephemeral tab after admission; both role changes stop the runtime closed.
#[test]
fn private_owner_role_cannot_be_repurposed() {
    for foreground in [false, true] {
        let owner = open(50, alice(), None);
        owner.set_scope_isolated_storage_owner().unwrap();
        if foreground {
            owner.set_non_durable_client();
        } else {
            owner.set_authority();
        }
        let q = owner.prepare_query(&Query::from("notes")).unwrap();
        assert!(block_on(owner.all(&q, ReadOpts::default())).is_err());
        assert!(owner.set_scope_isolated_storage_owner().is_err());
    }
}

/// Alice's owner creates a serializable transaction after reading her tab's
/// pending insert. Bob's authority must receive that earlier tab unit before
/// the owner's dependent commit, preserving their different original nodes.
#[test]
fn owner_serializable_reads_keep_pending_origin_order() {
    let tab = open(1, alice(), None);
    let owner = open(50, alice(), None);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    connect(&tab, &owner, alice());
    let parent = insert(&tab, "parent", true);
    turns(&[&tab, &owner]);
    let id = jazz::tools::OpenTransactionId([9; 16]);
    block_on(owner.begin_serializable(id)).unwrap();
    let query = owner.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(owner.tx_ref(id).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    let child = block_on(owner.commit_serializable_handle(id)).unwrap().0;
    let server = authority();
    connect(&owner, &server, alice());
    turns(&[&tab, &owner, &server, &owner, &tab]);
    assert_eq!(tab.write_state(parent).unwrap().fate, Fate::Accepted);
    assert_eq!(owner.write_state(child).unwrap().fate, Fate::Accepted);
}

/// Bob fails Alice's private projection after its journal has committed. The
/// sender must stay Pending, since reopening can still forward that real unit.
/// SQL triggers inject both alias allocation and allocator-watermark failures.
#[test]
fn projection_failure_never_rejects_an_already_journalled_upload() {
    for (table, condition) in [
        ("_jazz_owner_positions_v1", ""),
        ("_jazz_meta", "WHEN NEW.k='issued-seq-v1'"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projection-failure.sqlite");
        let owner = open(50, alice(), Some(&path));
        owner.set_scope_isolated_storage_owner().unwrap();
        owner.set_remote_link_hint(RemoteLinkHint::Failed);
        let tab = open(1, alice(), None);
        tab.set_remote_link_hint(RemoteLinkHint::Failed);
        tab.set_private_owner_client().unwrap();
        connect(&tab, &owner, alice());
        turns(&[&tab, &owner]);
        let tx = insert(&tab, "recover me", true);
        let sql = rusqlite::Connection::open(&path).unwrap();
        sql.execute_batch(&format!("CREATE TRIGGER fail_projection BEFORE INSERT ON {table} {condition} BEGIN SELECT RAISE(ABORT,'injected projection failure'); END")).unwrap();
        block_on(tab.tick()).unwrap();
        assert!(block_on(owner.tick()).is_err());
        block_on(tab.tick()).unwrap();
        assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Pending);
        assert_eq!(
            tab.write_state(tx).unwrap().durability,
            DurabilityTier::None
        );
        assert_eq!(
            sql.query_row("SELECT pending FROM _jazz_relay_upload_v1", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(block_on(owner.tick()).is_err());
        sql.execute_batch("DROP TRIGGER fail_projection").unwrap();
        drop(owner);
        drop(sql);
        let owner = open(50, alice(), Some(&path));
        owner.set_remote_link_hint(RemoteLinkHint::Failed);
        assert_eq!(texts(&owner), vec!["recover me"]);
        let own = insert(&owner, "owner's next", true);
        assert!(own > jazz::db::LOCAL_BASE + 1);
        let server = authority();
        connect(&owner, &server, alice());
        connect(&tab, &owner, alice());
        turns(&[&tab, &owner, &server, &owner, &tab]);
        assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Accepted);
        assert_eq!(texts(&server), vec!["owner's next", "recover me"]);
    }
}

/// Alice's queued writes keep their original schema, whether published by her
/// worker or a tab. Bob cannot make reopening or admission reinterpret them.
/// SQL inspection verifies that a refused open did not mutate physical names.
#[test]
fn owner_schema_changes_wait_for_pending_writes_before_mutating_storage() {
    for relayed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending-schema.sqlite");
        let tab = open(1, alice(), None);
        {
            let owner = open(50, alice(), Some(&path));
            owner.set_scope_isolated_storage_owner().unwrap();
            owner.set_remote_link_hint(RemoteLinkHint::Failed);
            if relayed {
                connect(&tab, &owner, alice());
                insert(&tab, "pending", true);
                turns(&[&tab, &owner]);
            } else {
                insert(&owner, "pending", true);
            }
            let next = versioned_config(50, alice(), true).schema;
            for make_current in [false, true] {
                let result = if make_current {
                    block_on(owner.activate_schema(next.clone()))
                } else {
                    block_on(owner.admit_schema_with_lenses(next.clone(), &[]))
                };
                let error = result.unwrap_err();
                assert_eq!(error.code, jazz::db::ErrorCode::Query);
                assert!(error.message.contains("pending"), "{error:?}");
            }
            block_on(owner.activate_schema(config(50, alice()).schema)).unwrap();
            assert_eq!(texts(&owner), vec!["pending"]);
        }
        let result = block_on(Db::open_at(
            versioned_config(50, alice(), true),
            &path,
            Default::default(),
        ));
        let error = match result {
            Ok(_) => panic!("reopened with a different pending schema"),
            Err(e) => e,
        };
        assert_eq!(error.code, jazz::db::ErrorCode::Query);
        let conn = rusqlite::Connection::open(&path).unwrap();
        let mutated: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='labels') OR EXISTS(SELECT 1 FROM _jazz_names WHERE tbl='labels')",
            [], |r| r.get(0),
        ).unwrap();
        assert!(!mutated);
        drop(conn);
        let owner = open(50, alice(), Some(&path));
        assert_eq!(texts(&owner), vec!["pending"]);
        let server = authority();
        connect(&owner, &server, alice());
        turns(&[&owner, &server]);
        assert_eq!(texts(&server), vec!["pending"]);
        block_on(owner.activate_schema(versioned_config(50, alice(), true).schema)).unwrap();
        drop(owner);
        let reopened = block_on(Db::open_at(
            versioned_config(50, alice(), true),
            &path,
            Default::default(),
        ))
        .unwrap();
        assert_eq!(texts(&reopened), vec!["pending"]);
    }
}

/// Alice's original pending coordinates survive a terminal receipt for later
/// reconnect reconciliation, but terminal writes leave the active lookup index.
/// Physical SQL inspection is necessary to verify this durable index contract.
#[test]
fn owner_retains_terminal_coordinates_outside_the_active_input_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("footprints.sqlite");
    let owner = open(50, alice(), Some(&path));
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = open(1, alice(), None);
    connect(&tab, &owner, alice());
    let tx = insert(&tab, "rejected", false);
    turns(&[&tab, &owner]);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let coordinate: (String, Vec<u8>, Vec<u8>) = conn
        .query_row(
            "SELECT t,id,k FROM _jazz_owner_input_rows_v1 WHERE node=?1 AND tx=?2 AND pending=1",
            rusqlite::params![[1u8; 16].as_slice(), tx as i64],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let server = authority();
    connect(&owner, &server, alice());
    turns(&[&tab, &owner, &server]);
    assert!(matches!(
        tab.write_state(tx).unwrap().fate,
        Fate::Rejected(_)
    ));
    let pending: bool = conn
        .query_row(
            "SELECT pending FROM _jazz_owner_input_rows_v1 WHERE node=?1 AND tx=?2",
            rusqlite::params![[1u8; 16].as_slice(), tx as i64],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!pending);
    let active: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM _jazz_owner_input_rows_v1 WHERE t=?1 AND id=?2 AND k=?3 AND pending=1 AND writes=1)",
        rusqlite::params![coordinate.0,coordinate.1,coordinate.2], |r|r.get(0),
    ).unwrap();
    assert!(!active);
    let plan: String = conn.query_row(
        "EXPLAIN QUERY PLAN SELECT node,tx FROM _jazz_owner_input_rows_v1 WHERE t=?1 AND id=?2 AND k=?3 AND pending=1 AND writes=1",
        rusqlite::params![coordinate.0,coordinate.1,coordinate.2], |r|r.get(3),
    ).unwrap();
    assert!(plan.contains("_jazz_owner_input_pending_v1"), "{plan}");
}

/// Bob's authority rejects Alice's first tab write with only a terminal receipt.
/// Her owner removes that exact pending insert and preserves the later one.
#[test]
fn a_rejection_without_an_eviction_removes_only_the_original_pending_insert() {
    use jazz::db::SyncMessage;
    let owner = open(50, alice(), None);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = open(1, alice(), None);
    connect(&tab, &owner, alice());
    let tx = insert(&tab, "rejected", true);
    insert(&tab, "still pending", true);
    turns(&[&tab, &owner]);
    assert_eq!(texts(&owner), vec!["rejected", "still pending"]);
    let (upstream, mut authority) = duplex_transport::duplex();
    block_on(owner.connect_upstream(upstream));
    authority
        .send(SyncMessage::FateV1 {
            node: [1; 16],
            tx,
            through: 0,
            result: Err(jazz::db::WireError::Rejected("permission denied".into())),
        })
        .unwrap();
    turns(&[&owner, &tab]);
    assert_eq!(texts(&owner), vec!["still pending"]);
    assert!(matches!(
        tab.write_state(tx).unwrap().fate,
        Fate::Rejected(_)
    ));
}

/// Alice's draft is a new branch head, copied from her pending base. Bob's
/// rejection removes only that head; replay and reopen preserve the base row.
#[test]
fn rejected_copy_on_write_removes_new_head_without_touching_pending_base() {
    use jazz::db::{ExactWriteTarget, InsertOptions, SyncMessage, UpdateOptions, WriteTarget};
    use jazz::ids::RowUuid;
    use jazz::protocol::{BranchSelector, BranchViewBase};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .column("branch", ColumnType::Uuid)
                    .branch_by("branch")
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let configured = |node| {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: alice(),
            },
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("branch.sqlite");
    let owner = block_on(Db::open_at(configured(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = block_on(Db::open(configured(1))).unwrap();
    connect(&tab, &owner, alice());
    let branch =
        |n| BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([n; 16])))]);
    let base = branch(3);
    let head = branch(4);
    let row = RowUuid::from_bytes([8; 16]);
    block_on(tab.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("base".into()))]),
        InsertOptions {
            row_id: Some(row),
            target: ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    ))
    .unwrap();
    let draft = block_on(tab.update(
        "notes",
        row,
        BTreeMap::from([("text".into(), Value::String("draft".into()))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id();
    turns(&[&tab, &owner]);
    let count = |db: &Db, branch: BranchSelector| {
        let query = db.prepare_query(&Query::from("notes")).unwrap();
        block_on(db.all(&query, ReadOpts::default().branch_view(branch, None)))
            .unwrap()
            .len()
    };
    assert_eq!(count(&owner, base.clone()), 1);
    assert_eq!(count(&owner, head.clone()), 1);
    let (upstream, mut authority) = duplex_transport::duplex();
    block_on(owner.connect_upstream(upstream));
    authority
        .send(SyncMessage::FateV1 {
            node: [1; 16],
            tx: draft,
            through: 0,
            result: Err(jazz::db::WireError::Rejected("draft denied".into())),
        })
        .unwrap();
    turns(&[&owner, &tab]);
    assert_eq!(count(&owner, base.clone()), 1);
    assert_eq!(count(&owner, head.clone()), 0);
    drop(owner);
    let owner = block_on(Db::open_at(configured(50), &path, Default::default())).unwrap();
    assert_eq!(count(&owner, base), 1);
    assert_eq!(count(&owner, head), 0);
}

/// Alice's persisted scope, original writer alias and pending row coordinate
/// keep their named SQL layouts. Physical inspection is needed to pin raw UUID
/// and canonical-subject bytes independently of query presentation.
#[test]
fn owner_projection_storage_v1_pins_types_identity_bytes_and_schema_binding() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_owner_projection_v1.json")).unwrap();
    assert_eq!(fixture["version"], 1);
    assert_eq!(
        fixture["roles"],
        serde_json::json!([
            "jazz.sqlite.owner-position.v1",
            "jazz.sqlite.owner-scope.v1",
            "jazz.sqlite.owner-input-row.v1"
        ])
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("format.sqlite");
    let owner = open(50, alice(), Some(&path));
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let writer = open(1, alice(), None);
    connect(&writer, &owner, alice());
    block_on(writer.insert(
        "notes",
        BTreeMap::from([
            ("text".into(), Value::String("fixture".into())),
            ("allowed".into(), Value::Bool(true)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(jazz::ids::RowUuid::from_bytes([0x44; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    turns(&[&writer, &owner]);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let (seq, node, tx, node_type, tx_type): (i64, Vec<u8>, i64, String, String) = conn
        .query_row(
            "SELECT seq,node,tx,typeof(node),typeof(tx) FROM _jazz_owner_positions_v1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(seq, fixture["position"].as_i64().unwrap());
    assert_eq!(tx, fixture["tx"].as_i64().unwrap());
    assert_eq!(hex::encode(node), fixture["nodeHex"].as_str().unwrap());
    assert_eq!((node_type.as_str(), tx_type.as_str()), ("blob", "integer"));
    let (subject,schema,subject_type,schema_type):(String,Vec<u8>,String,String)=conn.query_row("SELECT subject,schema_id,typeof(subject),typeof(schema_id) FROM _jazz_owner_scope_v1 WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(subject, fixture["subject"].as_str().unwrap());
    assert_eq!(
        hex::encode(subject.as_bytes()),
        fixture["subjectUtf8Hex"].as_str().unwrap()
    );
    assert_eq!(
        schema.len(),
        fixture["schemaByteLength"].as_u64().unwrap() as usize
    );
    assert_eq!(schema, config(50, alice()).schema.version_id().as_bytes());
    assert_eq!(
        (subject_type.as_str(), schema_type.as_str()),
        ("text", "blob")
    );
    let (row,key,pending,row_type,key_type):(Vec<u8>,Vec<u8>,i64,String,String)=conn.query_row("SELECT id,k,pending,typeof(id),typeof(k) FROM _jazz_owner_input_rows_v1 WHERE node=?1 AND tx=?2",rusqlite::params![[1u8;16].as_slice(),tx],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
    assert_eq!(hex::encode(row), fixture["rowHex"].as_str().unwrap());
    assert_eq!(hex::encode(key), fixture["rootBranchHex"].as_str().unwrap());
    assert_eq!(pending, fixture["pending"].as_i64().unwrap());
    let (writes, kind): (i64, String) = conn
        .query_row(
            "SELECT writes,typeof(writes) FROM _jazz_owner_input_rows_v1 WHERE node=?1 AND tx=?2",
            rusqlite::params![[1u8; 16].as_slice(), tx],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(writes, fixture["writes"].as_i64().unwrap());
    assert_eq!(kind, "integer");
    assert_eq!((row_type.as_str(), key_type.as_str()), ("blob", "blob"));
}
