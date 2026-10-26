//! Private pending input delivery through ordinary foreground queries.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, RemoteLinkHint, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::{collections::BTreeMap, path::Path};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn config(node: u8) -> DbConfig {
    DbConfig::new(
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
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
                                .with_update(
                                    Some(PolicyExpr::True),
                                    PolicyExpr::eq_literal(
                                        "allowed",
                                        jazz::tools::Value::Boolean(true),
                                    ),
                                )
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn open(node: u8, path: Option<&Path>) -> Db {
    match path {
        Some(path) => block_on(Db::open_at(config(node), path, Default::default())),
        None => block_on(Db::open(config(node))),
    }
    .unwrap()
}
fn owner(path: Option<&Path>) -> Db {
    let db = open(50, path);
    db.set_scope_isolated_storage_owner().unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    db
}
fn foreground(node: u8) -> Db {
    let db = open(node, None);
    db.set_private_owner_client().unwrap();
    db
}

fn cold_owner(path: Option<&Path>) -> Db {
    let mut config = config(50);
    config.schema = config.schema.without_permissions();
    let db = match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    db.set_scope_isolated_storage_owner().unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    db
}

fn cold_foreground(node: u8) -> Db {
    let mut config = config(node);
    config.schema = config.schema.without_permissions();
    let db = block_on(Db::open(config)).unwrap();
    db.set_private_owner_client().unwrap();
    db
}
fn connect(client: &Db, owner: &Db) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    owner.accept_subscriber(b, alice());
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn insert(db: &Db, text: &str, allowed: bool) -> (RowUuid, u64) {
    let row = block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("text".into(), Value::String(text.into())),
            ("allowed".into(), Value::Bool(allowed)),
        ]),
        Default::default(),
    ))
    .unwrap();
    (row.row_uuid(), row.mergeable_tx_id())
}
fn values(db: &Db) -> Vec<String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let mut rows = block_on(db.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
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
                Value::String(v) => v.clone(),
                v => panic!("{v:?}"),
            }
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}
fn subscribe(db: &Db, query: Query, opts: ReadOpts) -> SubscriptionStream {
    let query = db.prepare_query(&query).unwrap();
    block_on(db.subscribe(&query, opts)).unwrap()
}
fn opening(stream: &mut SubscriptionStream, dbs: &[&Db]) -> (Vec<String>, DurabilityTier) {
    for _ in 0..100 {
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    reset: true,
                    added,
                    settled: true,
                    tier,
                    ..
                } => {
                    let mut values = added
                        .iter()
                        .map(|row| {
                            match row.cells().find(|(n, _)| *n == "text").unwrap().1.unwrap() {
                                Value::String(v) => v.clone(),
                                v => panic!("{v:?}"),
                            }
                        })
                        .collect::<Vec<_>>();
                    values.sort();
                    return (values, tier);
                }
                SubscriptionEvent::Rejected { reason } => panic!("query rejected: {reason}"),
                _ => {}
            }
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("foreground never received a settled opening")
}

/// Alice starts offline with the public policy-free schema. Her sibling and
/// reopened foreground recover the matching complete unit, preserving original
/// pending identities and excluding unrelated writes without an authority.
#[test]
fn cold_private_owner_previews_survive_sibling_and_durable_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cold.sqlite");
    {
        let owner = cold_owner(Some(&path));
        let writer = cold_foreground(1);
        connect(&writer, &owner);
        turns(&[&writer, &owner]);
        let batch = block_on(writer.mergeable_tx()).unwrap();
        for text in ["wanted", "companion"] {
            block_on(batch.insert(
                "notes",
                BTreeMap::from([
                    ("text".into(), Value::String(text.into())),
                    ("allowed".into(), Value::Bool(true)),
                ]),
                Default::default(),
            ))
            .unwrap();
        }
        block_on(batch.commit()).unwrap();
        insert(&writer, "unrelated", true);
        turns(&[&writer, &owner]);
        let sibling = cold_foreground(2);
        connect(&sibling, &owner);
        let mut stream = subscribe(
            &sibling,
            Query::from("notes").filter(eq(col("text"), lit("wanted"))),
            ReadOpts::default(),
        );
        assert_eq!(
            opening(&mut stream, &[&sibling, &owner]),
            (vec!["wanted".into()], DurabilityTier::Local),
        );
        assert_eq!(values(&sibling), vec!["companion", "wanted"]);
        sibling.set_remote_link_hint(RemoteLinkHint::Failed);
        let snapshot = block_on(sibling.capture_snapshot()).unwrap();
        assert_eq!(snapshot.global_base.0, 0);
        assert_eq!(snapshot.dots.len(), 1);
        assert_eq!(snapshot.dots[0].node, NodeUuid::from_bytes([1; 16]));
        block_on(writer.close()).unwrap();
        block_on(sibling.close()).unwrap();
        block_on(owner.close()).unwrap();
    }
    let owner = cold_owner(Some(&path));
    let reopened = cold_foreground(3);
    connect(&reopened, &owner);
    let mut stream = subscribe(
        &reopened,
        Query::from("notes").filter(eq(col("text"), lit("wanted"))),
        ReadOpts::default(),
    );
    assert_eq!(
        opening(&mut stream, &[&reopened, &owner]),
        (vec!["wanted".into()], DurabilityTier::Local),
    );
    assert_eq!(values(&reopened), vec!["companion", "wanted"]);
}

/// Alice's cold owner exposes pending previews only to Alice. Mallory's direct
/// query and authenticated peer request both fail before any row or unit leaves.
#[test]
fn cold_private_previews_reject_a_different_admitted_identity() {
    use jazz::db::SyncMessage;
    let owner = cold_owner(None);
    insert(&owner, "private", true);
    let mallory = AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap();
    let query = owner.prepare_query(&Query::from("notes")).unwrap();
    assert!(
        block_on(owner.all_for_identity(
            &query,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            mallory,
        ))
        .is_err()
    );
    let (mut peer, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, mallory);
    peer.send(SyncMessage::Fetch {
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
    let replies = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    assert!(
        replies
            .iter()
            .any(|r| matches!(r, SyncMessage::Refused { id: 7, .. }))
    );
    assert!(!replies.iter().any(|r| matches!(r,
        SyncMessage::Rows { images } if !images.is_empty()
    ) || matches!(r, SyncMessage::OwnerInputsV1 { .. })));
}

/// Alice sees an offline preview, then Bob's authority accepts the insert while
/// denying SELECT. Acceptance must not turn that preview into readable cache
/// content for a new sibling, including after the owner reopens offline.
#[test]
fn cold_preview_does_not_grant_read_access_to_its_accepted_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("accepted.sqlite");
    {
        let owner = cold_owner(Some(&path));
        let writer = cold_foreground(1);
        connect(&writer, &owner);
        turns(&[&writer, &owner]);
        let (_, tx) = insert(&writer, "private preview", true);
        turns(&[&writer, &owner]);
        let sibling = cold_foreground(2);
        connect(&sibling, &owner);
        let mut stream = subscribe(&sibling, Query::from("notes"), ReadOpts::default());
        assert_eq!(
            opening(&mut stream, &[&sibling, &owner]).0,
            vec!["private preview"]
        );
        let mut server_config = config(90);
        server_config.schema = common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("text", ColumnType::Text)
                        .column("allowed", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::False)
                                .with_insert(PolicyExpr::True),
                        ),
                )
                .build(),
        );
        let authority = block_on(Db::open(server_config)).unwrap();
        authority.set_authority();
        authority.initialize_authority_catalogue().unwrap();
        connect(&owner, &authority);
        turns(&[&writer, &sibling, &owner, &authority]);
        assert_eq!(writer.write_state(tx).unwrap().fate, Fate::Accepted);
        let reader = cold_foreground(3);
        connect(&reader, &owner);
        let mut read = subscribe(&reader, Query::from("notes"), ReadOpts::default());
        assert!(
            opening(&mut read, &[&reader, &owner, &authority])
                .0
                .is_empty()
        );
        assert!(values(&reader).is_empty());
        block_on(writer.close()).unwrap();
        block_on(sibling.close()).unwrap();
        block_on(reader.close()).unwrap();
        block_on(owner.close()).unwrap();
    }
    let owner = cold_owner(Some(&path));
    let reopened = cold_foreground(4);
    connect(&reopened, &owner);
    let mut read = subscribe(&reopened, Query::from("notes"), ReadOpts::default());
    assert!(opening(&mut read, &[&reopened, &owner]).0.is_empty());
    assert!(values(&reopened).is_empty());
}

/// Alice reloads after her owner reopens. LocalOnly sees the owner cache; a
/// separate fresh tab recovers only the pending inputs named by its first query.
#[test]
fn reloaded_foreground_recovers_relevant_pending_inputs_through_its_query() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner.sqlite");
    let writer = open(1, None);
    let wanted = {
        let owner = owner(Some(&path));
        connect(&writer, &owner);
        let (_, tx) = insert(&writer, "wanted", true);
        insert(&writer, "unrelated", true);
        turns(&[&writer, &owner]);
        tx
    };
    let owner = owner(Some(&path));
    let local_tab = foreground(3);
    connect(&local_tab, &owner);
    let mut local = subscribe(
        &local_tab,
        Query::from("notes"),
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
    );
    assert_eq!(
        opening(&mut local, &[&local_tab, &owner]),
        (
            vec!["unrelated".into(), "wanted".into()],
            DurabilityTier::Local
        )
    );
    drop(local);
    block_on(local_tab.close()).unwrap();
    let tab = foreground(2);
    connect(&tab, &owner);
    assert!(values(&tab).is_empty());
    let mut remote = subscribe(
        &tab,
        Query::from("notes").filter(eq(col("text"), lit("wanted"))),
        ReadOpts::default(),
    );
    assert_eq!(
        opening(&mut remote, &[&tab, &owner]),
        (vec!["wanted".into()], DurabilityTier::Local)
    );
    assert_eq!(values(&tab), vec!["wanted"]);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let snapshot = block_on(tab.capture_snapshot()).unwrap();
    assert_eq!(snapshot.dots.len(), 1);
    assert_eq!(snapshot.dots[0].node, NodeUuid::from_bytes([1; 16]));
    assert_eq!(snapshot.dots[0].time.0, wanted);
    assert_eq!(snapshot.global_base.0, 0);
}

/// Alice's reloaded tab observes two writes with equal numeric times at old
/// writer nodes. The authority's receipt removes only the rejected original.
#[test]
fn recovered_inputs_follow_independent_authority_fates() {
    let owner = owner(None);
    let a = open(1, None);
    let b = open(2, None);
    connect(&a, &owner);
    connect(&b, &owner);
    let (_, accepted) = insert(&a, "accepted", true);
    let (_, rejected) = insert(&b, "rejected", false);
    assert_eq!(accepted, rejected);
    turns(&[&a, &b, &owner]);
    let tab = foreground(3);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(
        opening(&mut query, &[&tab, &owner]).0,
        vec!["accepted", "rejected"]
    );
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&owner, &server, &owner, &tab, &a, &b]);
    assert_eq!(values(&tab), vec!["accepted"]);
    assert_eq!(a.write_state(accepted).unwrap().fate, Fate::Accepted);
    assert!(matches!(
        b.write_state(rejected).unwrap().fate,
        Fate::Rejected(_)
    ));
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    assert!(block_on(tab.capture_snapshot()).unwrap().dots.is_empty());
}

/// Alice's tab disconnects before Bob's authority rejects its recovered input.
/// Its known-identity declaration obtains the real missed fate after reconnect.
#[test]
fn reconnect_reconciles_a_missed_rejection_without_replaying_old_payloads() {
    let owner = owner(None);
    let writer = open(1, None);
    connect(&writer, &owner);
    let (_, tx) = insert(&writer, "rejected", false);
    turns(&[&writer, &owner]);
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(opening(&mut query, &[&tab, &owner]).0, vec!["rejected"]);
    // A fresh link replaces the old transport before it can receive the fate.
    let (empty, _) = duplex_transport::duplex();
    block_on(tab.connect_upstream(empty));
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&owner, &server, &writer]);
    assert!(matches!(
        writer.write_state(tx).unwrap().fate,
        Fate::Rejected(_)
    ));
    connect(&tab, &owner);
    turns(&[&tab, &owner, &server]);
    assert!(values(&tab).is_empty());
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    assert!(block_on(tab.capture_snapshot()).unwrap().dots.is_empty());
}

/// Alice's query matches one row of a complete transaction. Its companion row
/// travels atomically with it; unrelated pending transactions stay at the owner.
#[test]
fn a_query_recovers_the_complete_matching_unit_but_no_unrelated_unit() {
    let owner = owner(None);
    let writer = open(1, None);
    connect(&writer, &owner);
    let batch = block_on(writer.mergeable_tx()).unwrap();
    for (text, allowed) in [("wanted", true), ("companion", false)] {
        block_on(batch.insert(
            "notes",
            BTreeMap::from([
                ("text".into(), Value::String(text.into())),
                ("allowed".into(), Value::Bool(allowed)),
            ]),
            Default::default(),
        ))
        .unwrap();
    }
    block_on(batch.commit()).unwrap();
    insert(&writer, "unrelated", true);
    turns(&[&writer, &owner]);
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(
        &tab,
        Query::from("notes").filter(eq(col("text"), lit("wanted"))),
        ReadOpts::default(),
    );
    assert_eq!(opening(&mut query, &[&tab, &owner]).0, vec!["wanted"]);
    assert_eq!(values(&tab), vec!["companion", "wanted"]);
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&owner, &server, &tab]);
    assert!(values(&tab).is_empty());
}

/// Alice reloads while an update is pending over an accepted row. Bob's
/// authority rejects it, restoring the accepted base without losing provenance.
#[test]
fn recovered_update_replays_over_accepted_base_and_rejection_restores_it() {
    let server = open(90, None);
    server.set_authority();
    let (row, _) = insert(&server, "accepted base", true);
    let owner = owner(None);
    owner.set_remote_link_hint(RemoteLinkHint::Live);
    connect(&owner, &server);
    let writer = foreground(1);
    connect(&writer, &owner);
    let warm = subscribe(
        &writer,
        Query::from("notes"),
        ReadOpts::for_read_tier(ReadTier::Remote),
    );
    turns(&[&writer, &owner, &server]);
    assert_eq!(values(&writer), vec!["accepted base"]);
    drop(warm);
    writer.seed_seq_floor((1u64 << 52) + (1u64 << 51));
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tx = block_on(writer.update(
        "notes",
        row,
        BTreeMap::from([
            ("text".into(), Value::String("pending edit".into())),
            ("allowed".into(), Value::Bool(false)),
        ]),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id();
    connect(&writer, &owner);
    turns(&[&writer, &owner]);
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(opening(&mut query, &[&tab, &owner]).0, vec!["pending edit"]);
    turns(&[&owner, &server, &tab, &writer]);
    assert!(matches!(
        writer.write_state(tx).unwrap().fate,
        Fate::Rejected(_)
    ));
    assert_eq!(values(&tab), vec!["accepted base"]);
}

/// Alice's owner echoes her original counter writes with owner publication
/// positions. Promotion reuses the units, so reconnect cannot add the delta twice.
/// Wire capture proves that the echo arrived; values are checked through reads.
#[test]
fn an_own_counter_echo_is_promoted_once_and_keeps_original_snapshot_dots() {
    use jazz::db::{SyncMessage, Transport, TransportError};
    use jazz::tools::{
        ColumnDescriptor, ColumnMergeStrategy, RowDescriptor, Schema, TableName, TableSchema,
    };
    use std::{cell::RefCell, rc::Rc};
    let schema = common::compile_schema(&Schema::from([(
        TableName::new("notes"),
        TableSchema {
            synthetic_references_v1: Default::default(),
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("text", ColumnType::Text),
                ColumnDescriptor::new("value", ColumnType::Integer)
                    .merge_strategy(ColumnMergeStrategy::Counter),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: common::allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]));
    let opened = |node| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: alice(),
            },
        )))
        .unwrap()
    };
    let owner = opened(50);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = opened(1);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    tab.set_private_owner_client().unwrap();
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    // Stage without driving the owner, so these original tab positions have
    // not yet been promoted when the snapshot is captured.
    tab.seed_seq_floor((1u64 << 52) + (1u64 << 51));
    let row = block_on(tab.insert(
        "notes",
        BTreeMap::from([
            ("text".into(), Value::String("counter".into())),
            ("value".into(), Value::I32(0)),
        ]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    block_on(tab.update(
        "notes",
        row,
        BTreeMap::from([("value".into(), Value::I32(2))]),
        Default::default(),
    ))
    .unwrap();
    let before = block_on(tab.capture_snapshot()).unwrap();
    struct Capture {
        transport: Box<dyn Transport>,
        sent: Rc<RefCell<Vec<SyncMessage>>>,
    }
    impl Transport for Capture {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            self.sent.borrow_mut().push(message.clone());
            self.transport.send(message)
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.transport.try_recv()
        }
    }
    let sent = Rc::new(RefCell::new(Vec::new()));
    let (a, b) = duplex_transport::duplex();
    block_on(tab.connect_upstream(a));
    owner.accept_subscriber(
        Box::new(Capture {
            transport: b,
            sent: sent.clone(),
        }),
        alice(),
    );
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    opening(&mut query, &[&tab, &owner]);
    turns(&[&tab, &owner]);
    assert!(
        sent.borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::OwnerInputsV1 { .. }))
    );
    let value = || {
        let q = tab.prepare_query(&Query::from("notes")).unwrap();
        let rows = block_on(tab.all(&q, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap();
        rows[0]
            .cells()
            .find(|(n, _)| *n == "value")
            .unwrap()
            .1
            .unwrap()
            .clone()
    };
    assert_eq!(value(), Value::I32(2));
    let after = block_on(tab.capture_snapshot()).unwrap();
    assert_eq!(before.dots, after.dots);
    let q = tab.prepare_query(&Query::from("notes")).unwrap();
    let mut frozen = ReadOpts::for_read_tier(ReadTier::LocalOnly);
    frozen.read_view.source = jazz::protocol::ReadViewSourceSpec::Snapshot {
        snapshot: before.clone(),
    };
    let frozen_rows = block_on(tab.all(&q, frozen)).unwrap();
    assert_eq!(
        frozen_rows[0]
            .cells()
            .find(|(name, _)| *name == "value")
            .unwrap()
            .1
            .unwrap(),
        &Value::I32(2)
    );
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    assert_eq!(value(), Value::I32(2));
    assert_eq!(block_on(tab.capture_snapshot()).unwrap().dots, before.dots);
}

/// Mallory cannot read Alice's recovered private input through a second identity
/// on the same foreground, including after the authority settles that input.
#[test]
fn recovered_foreground_remains_bound_to_its_subject_after_settlement() {
    let owner = owner(None);
    let writer = open(1, None);
    connect(&writer, &owner);
    insert(&writer, "private", true);
    turns(&[&writer, &owner]);
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut stream = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    opening(&mut stream, &[&tab, &owner]);
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let mallory = AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap();
    assert!(block_on(tab.all_for_identity(&query, ReadOpts::default(), mallory)).is_err());
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&owner, &server, &tab]);
    assert_eq!(values(&tab), vec!["private"]);
    assert!(block_on(tab.all_for_identity(&query, ReadOpts::default(), mallory)).is_err());
}

/// Alice's legacy peer cannot interpret owner aliases. The owner refuses that
/// query explicitly and never ships a pending image as an ordinary row.
/// Direct wire inspection verifies the qualification boundary.
#[test]
fn a_peer_without_input_support_never_receives_unqualified_pending_rows() {
    use jazz::db::SyncMessage;
    let owner = owner(None);
    let writer = open(1, None);
    connect(&writer, &owner);
    insert(&writer, "private", true);
    turns(&[&writer, &owner]);
    let (mut peer, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, alice());
    peer.send(SyncMessage::Fetch {
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
    let messages = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, SyncMessage::Refused { id: 7, .. }))
    );
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m,SyncMessage::Rows{images} if !images.is_empty()))
    );
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, SyncMessage::Answer { id: 7, .. }))
    );
    assert_eq!(values(&owner), vec!["private"]);
}

/// Mallory changes an otherwise valid private input's author, schema, order or
/// framing. Alice's foreground refuses it and cannot expose a partial replay.
/// Wire mutation is necessary to exercise the decoder and admission boundary.
#[test]
fn malformed_or_misattributed_owner_inputs_fail_closed() {
    use jazz::db::SyncMessage;
    let owner = owner(None);
    let writer = open(1, None);
    connect(&writer, &owner);
    insert(&writer, "first", true);
    insert(&writer, "second", true);
    turns(&[&writer, &owner]);
    let (mut peer, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, alice());
    peer.send(SyncMessage::Schema {
        version: *config(2).schema.version_id().as_bytes(),
    })
    .unwrap();
    peer.send(SyncMessage::OwnerProjectionV1 { known: Vec::new() })
        .unwrap();
    peer.send(SyncMessage::Fetch {
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
    let frame = std::iter::from_fn(|| peer.try_recv())
        .find(|m| matches!(m, SyncMessage::OwnerInputsV1 { .. }))
        .unwrap();
    for mode in ["author", "schema", "order", "trailing"] {
        let mut message = frame.clone();
        let SyncMessage::OwnerInputsV1 { schema, units, .. } = &mut message else {
            unreachable!()
        };
        assert_eq!(units.len(), 2);
        match mode {
            "author" => {
                let mut commit: SyncMessage = postcard::from_bytes(&units[1].1).unwrap();
                let SyncMessage::Commit { author, .. } = &mut commit else {
                    unreachable!()
                };
                *author = Some(
                    AuthorSubject::authenticated("https://issuer.example", "mallory")
                        .unwrap()
                        .canonical()
                        .into(),
                );
                units[1].1 = postcard::to_allocvec(&commit).unwrap();
            }
            "schema" => *schema = [0x99; 16],
            "order" => units[1].0 = units[0].0,
            "trailing" => units[1].1.push(0),
            _ => unreachable!(),
        }
        let tab = foreground(2);
        let (link, mut malicious) = duplex_transport::duplex();
        block_on(tab.connect_upstream(link));
        malicious.send(message).unwrap();
        let error = block_on(tab.tick()).unwrap_err();
        assert_eq!(
            error.code,
            jazz::db::ErrorCode::Protocol,
            "{mode}: {error:?}"
        );
        let query = tab.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(tab.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).is_err(),
            "{mode}"
        );
    }
}

/// Alice and Bob's original identities retain exact byte layouts on the new
/// handshake and complete-input envelopes. Independently authored bytes pin
/// enum tags, varint boundaries, field order and the nested Commit encoding.
#[test]
fn owner_input_wire_v2_matches_independent_byte_fixtures() {
    use jazz::db::SyncMessage;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v2/owner-inputs.json")).unwrap();
    assert_eq!(fixture["version"], 2);
    assert_eq!(
        fixture["projectionRole"],
        "jazz.sqlite.sync.owner-projection.v1"
    );
    assert_eq!(fixture["inputRole"], "jazz.sqlite.sync.owner-inputs.v1");
    let hello = SyncMessage::OwnerProjectionV1 {
        known: vec![([0x11; 16], 127), ([0x22; 16], 128)],
    };
    assert_eq!(
        hex::encode(jazz::db::encode_message(&hello).unwrap()),
        fixture["helloHex"].as_str().unwrap()
    );
    let commit = hex::decode(fixture["commitHex"].as_str().unwrap()).unwrap();
    let decoded: SyncMessage = postcard::from_bytes(&commit).unwrap();
    let SyncMessage::Commit {
        node,
        tx,
        kind,
        ops,
        author,
        session,
        expect,
    } = &decoded
    else {
        panic!("wrong nested variant")
    };
    assert_eq!(*node, [0x11; 16]);
    assert_eq!(*tx, 300);
    assert_eq!(
        serde_json::to_value(ops).unwrap(),
        serde_json::json!([
            {"Delete": {"table": "notes", "row": vec![0x66_u8;16], "target": "Root", "at": 42}}
        ])
    );
    assert_eq!(
        serde_json::to_value(kind).unwrap(),
        serde_json::json!("Mergeable")
    );
    assert_eq!(
        author.as_deref(),
        Some(fixture["subject"].as_str().unwrap())
    );
    assert_eq!(session, &Some(BTreeMap::new()));
    assert!(expect.is_empty());
    assert_eq!(postcard::to_allocvec(&decoded).unwrap(), commit);
    let message = SyncMessage::OwnerInputsV1 {
        schema: [0x22; 16],
        through: 128,
        frozen: vec![jazz::db::OwnerFrozenBaseV1 {
            base: jazz::db::OwnerInputBaseV1 {
                table: "notes".into(),
                id: [0x55; 16],
                key: vec![0xcc],
                seq: Some(3),
            },
            at: 7,
        }],
        bases: vec![
            jazz::db::OwnerInputBaseV1 {
                table: "notes".into(),
                id: [0x33; 16],
                key: vec![],
                seq: None,
            },
            jazz::db::OwnerInputBaseV1 {
                table: "notes".into(),
                id: [0x44; 16],
                key: vec![0xaa, 0xbb],
                seq: Some(128),
            },
        ],
        units: vec![(fixture["position"].as_u64().unwrap(), commit)],
    };
    let encoded = jazz::db::encode_message(&message).unwrap();
    assert_eq!(
        hex::encode(&encoded),
        fixture["inputsHex"].as_str().unwrap()
    );
    let decoded = jazz::sync_codec::decode_sync_message(&encoded).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(message).unwrap()
    );
}

/// Alice retains an accepted row from an earlier policy. A pending update that
/// grants SELECT under the new policy cannot authorise shipping that old base.
/// The wire assertion verifies refusal happens before any base or unit escapes.
#[test]
fn pending_read_grant_cannot_authorise_an_accepted_recovery_base() {
    use jazz::db::SyncMessage;
    let schema = |restricted| {
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("text", ColumnType::Text)
                        .column("allowed", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(if restricted {
                                    PolicyExpr::eq_literal(
                                        "allowed",
                                        jazz::tools::Value::Boolean(true),
                                    )
                                } else {
                                    PolicyExpr::True
                                })
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build(),
        )
    };
    let opened = |node| {
        block_on(Db::open(DbConfig::new(
            schema(false),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: alice(),
            },
        )))
        .unwrap()
    };
    let server = opened(90);
    server.set_authority();
    let (row, _) = insert(&server, "accepted before policy change", false);
    let owner = opened(50);
    owner.set_scope_isolated_storage_owner().unwrap();
    connect(&owner, &server);
    let warm = subscribe(
        &owner,
        Query::from("notes"),
        ReadOpts::for_read_tier(ReadTier::Remote),
    );
    turns(&[&owner, &server]);
    assert_eq!(values(&owner), vec!["accepted before policy change"]);
    drop(warm);
    let new_schema = schema(true);
    block_on(owner.activate_schema(new_schema.clone())).unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(owner.update(
        "notes",
        row,
        BTreeMap::from([("allowed".into(), Value::Bool(true))]),
        Default::default(),
    ))
    .unwrap();
    let (mut peer, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, alice());
    peer.send(SyncMessage::Schema {
        version: *new_schema.version_id().as_bytes(),
    })
    .unwrap();
    peer.send(SyncMessage::OwnerProjectionV1 { known: Vec::new() })
        .unwrap();
    peer.send(SyncMessage::Fetch {
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
    // Keep the old upstream asleep while inspecting the owner's local answer.
    // Changing the owner's declared upstream schema is intentionally blocked
    // while its old connection has a different version, so detach it first.
    let (unused, _) = duplex_transport::duplex();
    let upstream = block_on(owner.connect_upstream(unused));
    owner.detach_connection(&upstream);
    block_on(owner.tick()).unwrap();
    let replies = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    assert!(replies.iter().any(|m|matches!(m,SyncMessage::Refused{id:7,message,..} if message.contains("accepted base"))),"{replies:?}");
    assert!(
        !replies
            .iter()
            .any(|m| matches!(m,SyncMessage::Rows{images} if !images.is_empty()))
    );
    assert!(
        !replies
            .iter()
            .any(|m| matches!(m, SyncMessage::OwnerInputsV1 { .. }))
    );
}

/// Alice queries only her pending draft. Its inherited base was never queried
/// by the new tab, so the owner's complete input closure must supply it too.
#[test]
fn a_pending_draft_recovers_its_unqueried_inherited_base() {
    use jazz::db::{ExactWriteTarget, InsertOptions, UpdateOptions, WriteTarget};
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
    let opened = |node| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: alice(),
            },
        )))
        .unwrap()
    };
    let owner = opened(50);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let writer = opened(1);
    connect(&writer, &owner);
    let base = BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([3; 16])))]);
    let head = BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([4; 16])))]);
    let row = block_on(writer.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("base".into()))]),
        InsertOptions {
            target: ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    ))
    .unwrap()
    .row_uuid();
    block_on(writer.update(
        "notes",
        row,
        BTreeMap::from([("text".into(), Value::String("draft".into()))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head,
                base: Some(BranchViewBase::Current(base)),
            },
            ..Default::default()
        },
    ))
    .unwrap();
    turns(&[&writer, &owner]);
    let tab = opened(2);
    tab.set_private_owner_client().unwrap();
    connect(&tab, &owner);
    let mut stream = subscribe(
        &tab,
        Query::from("notes").filter(eq(
            col("branch"),
            lit(Value::Uuid(uuid::Uuid::from_bytes([4; 16]))),
        )),
        ReadOpts::default(),
    );
    assert_eq!(opening(&mut stream, &[&tab, &owner]).0, vec!["draft"]);
    assert_eq!(values(&tab), vec!["base", "draft"]);
    // The dependency is directional: reading the base does not fetch every
    // unrelated draft that happens to inherit from it.
    let base_tab = opened(3);
    base_tab.set_private_owner_client().unwrap();
    connect(&base_tab, &owner);
    let mut base_stream = subscribe(
        &base_tab,
        Query::from("notes").filter(eq(
            col("branch"),
            lit(Value::Uuid(uuid::Uuid::from_bytes([3; 16]))),
        )),
        ReadOpts::default(),
    );
    assert_eq!(
        opening(&mut base_stream, &[&base_tab, &owner]).0,
        vec!["base"]
    );
    assert_eq!(values(&base_tab), vec!["base"]);
}

fn assert_wait_pending(future: impl std::future::Future) {
    let mut future = std::pin::pin!(future);
    assert!(
        future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(
                std::task::Waker::noop(),
            ))
            .is_pending()
    );
}
fn assert_wait_ready(future: impl std::future::Future<Output = Result<(), jazz::db::Error>>) {
    let mut future = std::pin::pin!(future);
    assert!(matches!(
        future.as_mut().poll(&mut std::task::Context::from_waker(
            std::task::Waker::noop(),
        )),
        std::task::Poll::Ready(Ok(()))
    ));
}

/// Alice's new tab recovers an old tab's pending write before the owner's real
/// flush. Recovery alone cannot satisfy Local; the captured storage receipt can.
#[test]
fn recovered_relay_input_waits_for_the_actual_storage_barrier() {
    let owner = owner(None);
    owner.require_owner_durability_flush();
    let writer = open(1, None);
    writer.set_remote_link_hint(RemoteLinkHint::Failed);
    connect(&writer, &owner);
    insert(&writer, "recovered", true);
    turns(&[&writer, &owner]);
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(opening(&mut query, &[&tab, &owner]).0, vec!["recovered"]);
    turns(&[&tab, &owner]);
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Local));
    let barrier = owner.prepare_owner_durability().unwrap().unwrap();
    turns(&[&tab, &owner]);
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Local));
    owner.confirm_owner_durability(barrier).unwrap();
    owner.flush_owner_durability().unwrap();
    turns(&[&tab, &owner]);
    assert_wait_ready(tab.wait_for_pending_writes(DurabilityTier::Local));
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Global));
}

/// Alice's owner write and old tab write share a numeric transaction time.
/// A barrier captured before the tab write can acknowledge only the owner write.
#[test]
fn recovered_own_and_relay_inputs_keep_distinct_storage_barriers() {
    use std::future::Future;
    let owner = owner(None);
    owner.require_owner_durability_flush();
    let (_, own_tx) = insert(&owner, "owner", true);
    let first = owner.prepare_owner_durability().unwrap().unwrap();
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(opening(&mut query, &[&tab, &owner]).0, vec!["owner"]);
    let mut first_wait = std::pin::pin!(tab.wait_for_pending_writes(DurabilityTier::Local));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(first_wait.as_mut().poll(&mut context).is_pending());
    let writer = open(1, None);
    writer.set_remote_link_hint(RemoteLinkHint::Failed);
    let (_, relay_tx) = insert(&writer, "relay", true);
    assert_eq!(own_tx, relay_tx);
    connect(&writer, &owner);
    turns(&[&writer, &owner, &tab]);
    assert_eq!(values(&tab), vec!["owner", "relay"]);
    owner.confirm_owner_durability(first).unwrap();
    owner.flush_owner_durability().unwrap();
    turns(&[&tab, &owner]);
    assert!(matches!(
        first_wait.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Local));
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Global));
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    turns(&[&tab, &owner]);
    assert_wait_ready(tab.wait_for_pending_writes(DurabilityTier::Local));
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Global));
    assert!(owner.prepare_owner_durability().unwrap().is_none());
}

/// Alice opens a tab after her own and relayed writes were flushed. Losing the
/// first storage receipt does not strand the known inputs after reconnect.
#[test]
fn recovered_inputs_receive_prior_storage_receipts_again_after_reconnect() {
    use jazz::db::{SyncMessage, Transport, TransportError};
    struct DropStored(Box<dyn Transport>);
    impl Transport for DropStored {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            self.0.send(message)
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            loop {
                let message = self.0.try_recv()?;
                if !matches!(message, SyncMessage::OwnerStoredV1 { .. }) {
                    return Some(message);
                }
            }
        }
    }
    let owner = owner(None);
    insert(&owner, "owner", true);
    let writer = open(1, None);
    writer.set_remote_link_hint(RemoteLinkHint::Failed);
    insert(&writer, "relay", true);
    connect(&writer, &owner);
    turns(&[&writer, &owner]);
    assert!(owner.prepare_owner_durability().unwrap().is_none());
    let tab = foreground(2);
    let (a, b) = duplex_transport::duplex();
    block_on(tab.connect_upstream(Box::new(DropStored(a))));
    owner.accept_subscriber(b, alice());
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(
        opening(&mut query, &[&tab, &owner]).0,
        vec!["owner", "relay"]
    );
    turns(&[&tab, &owner]);
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Local));
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    assert_wait_ready(tab.wait_for_pending_writes(DurabilityTier::Local));
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Global));
    assert_eq!(values(&tab), vec!["owner", "relay"]);
    assert!(owner.prepare_owner_durability().unwrap().is_none());
}

/// Alice reopens her private owner with both kinds of pending input. A fresh
/// storage barrier covers their recovered durable records without new uploads.
#[test]
fn reopened_owner_confirms_own_and_relay_inputs_to_a_new_tab() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner.sqlite");
    {
        let owner = owner(Some(&path));
        insert(&owner, "owner", true);
        let writer = open(1, None);
        writer.set_remote_link_hint(RemoteLinkHint::Failed);
        insert(&writer, "relay", true);
        connect(&writer, &owner);
        turns(&[&writer, &owner]);
        assert!(owner.prepare_owner_durability().unwrap().is_none());
    }
    let owner = owner(Some(&path));
    owner.require_owner_durability_flush();
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(
        opening(&mut query, &[&tab, &owner]).0,
        vec!["owner", "relay"]
    );
    turns(&[&tab, &owner]);
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Local));
    owner
        .confirm_owner_durability(owner.prepare_owner_durability().unwrap().unwrap())
        .unwrap();
    owner.flush_owner_durability().unwrap();
    turns(&[&tab, &owner]);
    assert_wait_ready(tab.wait_for_pending_writes(DurabilityTier::Local));
    assert_wait_pending(tab.wait_for_pending_writes(DurabilityTier::Global));
}

/// Bob's authority rejects Alice's own and relayed inputs during an owner
/// flush. Confirming its old barrier cannot resurrect either rejected write.
#[test]
fn late_storage_confirmation_cannot_override_recovered_input_rejection() {
    use std::future::Future;
    let owner = owner(None);
    owner.require_owner_durability_flush();
    insert(&owner, "owner", false);
    let writer = open(1, None);
    writer.set_remote_link_hint(RemoteLinkHint::Failed);
    insert(&writer, "relay", false);
    connect(&writer, &owner);
    turns(&[&writer, &owner]);
    let barrier = owner.prepare_owner_durability().unwrap().unwrap();
    let tab = foreground(2);
    connect(&tab, &owner);
    let mut query = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(
        opening(&mut query, &[&tab, &owner]).0,
        vec!["owner", "relay"]
    );
    let mut wait = std::pin::pin!(tab.wait_for_pending_writes(DurabilityTier::Local));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(wait.as_mut().poll(&mut context).is_pending());
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&owner, &server, &tab]);
    owner.confirm_owner_durability(barrier).unwrap();
    owner.flush_owner_durability().unwrap();
    turns(&[&owner, &tab]);
    assert!(matches!(
        wait.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(values(&tab).is_empty());
    assert!(owner.prepare_owner_durability().unwrap().is_none());
    assert_wait_ready(tab.wait_for_pending_writes(DurabilityTier::Local));
}

/// Alice's pending edit depends on an accepted row. Mallory omits or alters
/// the preceding base frame; the foreground must refuse a partial replay.
/// Wire mutation exercises missing data and provenance that public writes cannot create.
#[test]
fn owner_input_requires_its_exact_accepted_base_and_evidence() {
    use jazz::db::SyncMessage;
    let server = open(90, None);
    server.set_authority();
    let (row, _) = insert(&server, "accepted", true);
    let owner = owner(None);
    owner.set_remote_link_hint(RemoteLinkHint::Live);
    connect(&owner, &server);
    let warm = subscribe(
        &owner,
        Query::from("notes"),
        ReadOpts::for_read_tier(ReadTier::Remote),
    );
    turns(&[&owner, &server]);
    assert_eq!(values(&owner), vec!["accepted"]);
    drop(warm);
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(owner.update(
        "notes",
        row,
        BTreeMap::from([("text".into(), Value::String("pending".into()))]),
        Default::default(),
    ))
    .unwrap();
    let (mut peer, link) = duplex_transport::duplex();
    owner.accept_subscriber(link, alice());
    peer.send(SyncMessage::Schema {
        version: *config(2).schema.version_id().as_bytes(),
    })
    .unwrap();
    peer.send(SyncMessage::OwnerProjectionV1 { known: Vec::new() })
        .unwrap();
    peer.send(SyncMessage::Fetch {
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
    let frames = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    assert!(
        frames
            .iter()
            .any(|message| matches!(message, SyncMessage::OwnerInputsV1 { .. }))
    );
    for mode in [
        "missing row",
        "wrong version",
        "missing evidence",
        "missing base",
        "extra base",
        "duplicate base",
        "optimistic base",
        "unknown table",
        "false absence",
    ] {
        let tab = foreground(2);
        let (link, mut malicious) = duplex_transport::duplex();
        block_on(tab.connect_upstream(link));
        for mut message in frames.clone() {
            match (&mut message, mode) {
                (SyncMessage::Rows { .. }, "missing row")
                | (SyncMessage::AcceptedRowsV1 { .. }, "missing evidence") => continue,
                (SyncMessage::Rows { images }, "wrong version") => {
                    for image in images {
                        image.seq += 1;
                    }
                }
                (SyncMessage::OwnerInputsV1 { bases, .. }, mode) => match mode {
                    "missing base" => bases.clear(),
                    "extra base" => bases.push(jazz::db::OwnerInputBaseV1 {
                        table: "notes".into(),
                        id: [0x55; 16],
                        key: Vec::new(),
                        seq: None,
                    }),
                    "duplicate base" => bases.push(bases[0].clone()),
                    "optimistic base" => bases[0].seq = Some(1 << 52),
                    "unknown table" => bases[0].table = "absent_table".into(),
                    "false absence" => bases[0].seq = None,
                    _ => {}
                },
                _ => {}
            }
            malicious.send(message).unwrap();
        }
        let result = block_on(tab.tick());
        assert!(result.is_err(), "{mode}: partial recovery was accepted");
        assert_eq!(
            result.unwrap_err().code,
            jazz::db::ErrorCode::Protocol,
            "{mode}"
        );
        let query = tab.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(tab.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).is_err(),
            "{mode}"
        );
    }
}

/// Alice receives authority provenance and row images one frame per turn while
/// the write's actual receipt is held. Her private foreground must not replay
/// the accepted insert/update/delete twice or claim Global durability early.
#[test]
fn fragmented_accepted_images_do_not_replay_a_pending_owner_unit_twice() {
    use jazz::db::{SyncMessage, Transport, TransportError};
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        rc::Rc,
    };
    // Transport control is necessary to separate accepted row evidence from
    // its later real receipt; application writes cannot choose that delivery.
    struct Fragmented {
        wire: Box<dyn Transport>,
        allow: Rc<Cell<bool>>,
        hold: Rc<Cell<bool>>,
        receipts: Rc<RefCell<VecDeque<SyncMessage>>>,
    }
    impl Transport for Fragmented {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            self.wire.send(message)
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            if !self.allow.get() {
                return None;
            }
            if !self.hold.get()
                && let Some(receipt) = self.receipts.borrow_mut().pop_front()
            {
                self.allow.set(false);
                return Some(receipt);
            }
            while let Some(message) = self.wire.try_recv() {
                if self.hold.get()
                    && matches!(
                        &message,
                        SyncMessage::OriginFateV2 { .. }
                            | SyncMessage::FateV1 { .. }
                            | SyncMessage::Fate { .. }
                    )
                {
                    self.receipts.borrow_mut().push_back(message);
                    continue;
                }
                self.allow.set(false);
                return Some(message);
            }
            None
        }
    }
    let authority = open(90, None);
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    let owner = owner(None);
    let writer = foreground(1);
    let (up, down) = duplex_transport::duplex();
    let allow = Rc::new(Cell::new(false));
    let hold = Rc::new(Cell::new(false));
    let receipts = Rc::new(RefCell::new(VecDeque::new()));
    block_on(owner.connect_upstream(Box::new(Fragmented {
        wire: up,
        allow: allow.clone(),
        hold: hold.clone(),
        receipts: receipts.clone(),
    })));
    owner.set_remote_link_hint(RemoteLinkHint::Live);
    authority.accept_subscriber(down, alice());
    connect(&writer, &owner);
    let stream = RefCell::new(subscribe(
        &writer,
        Query::from("notes"),
        ReadOpts::default(),
    ));
    let pump = || {
        for _ in 0..160 {
            allow.set(true);
            for db in [&writer, &owner, &authority] {
                block_on(db.tick()).unwrap();
            }
            while let Some(event) = stream.borrow_mut().try_next_event() {
                assert!(
                    !matches!(
                        event,
                        SubscriptionEvent::Rejected { .. } | SubscriptionEvent::Closed
                    ),
                    "{event:?}"
                );
            }
        }
    };
    pump();
    let mut row = None;
    for action in ["insert", "update", "delete"] {
        hold.set(true);
        let tx = match action {
            "insert" => {
                let (id, tx) = insert(&writer, "original", true);
                row = Some(id);
                tx
            }
            "update" => block_on(writer.update(
                "notes",
                row.unwrap(),
                BTreeMap::from([("text".into(), Value::String("updated".into()))]),
                Default::default(),
            ))
            .unwrap()
            .mergeable_tx_id(),
            "delete" => block_on(writer.delete("notes", row.unwrap(), Default::default()))
                .unwrap()
                .mergeable_tx_id(),
            _ => unreachable!(),
        };
        pump();
        assert!(
            !receipts.borrow().is_empty(),
            "authority receipt was held for {action}"
        );
        assert_wait_pending(writer.wait_for_transaction(tx, DurabilityTier::Global));
        match action {
            "insert" => assert_eq!(values(&writer), ["original"]),
            "update" => assert_eq!(values(&writer), ["updated"]),
            "delete" => assert!(values(&writer).is_empty()),
            _ => unreachable!(),
        }
        hold.set(false);
        pump();
        assert_eq!(writer.write_state(tx).unwrap().fate, Fate::Accepted);
        assert_wait_ready(async {
            writer
                .wait_for_transaction(tx, DurabilityTier::Global)
                .await
                .map(|_| ())
        });
    }
}

/// Alice's reopened tab recovers accepted history from its offline owner even
/// when no pending input units remain. The cached opening stays Local, and a
/// snapshot begun before hydration cannot acquire the newly received evidence.
///
/// authority -> durable owner -> close/reopen offline -> Alice's fresh tab
#[test]
fn reopened_foreground_recovers_accepted_cut_without_pending_inputs() {
    use jazz::tools::OpenTransactionId;
    let authority = open(90, None);
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    let (_, accepted_tx) = insert(&authority, "retained", true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("accepted-owner.sqlite");
    {
        let owner = owner(Some(&path));
        connect(&owner, &authority);
        let mut read = subscribe(
            &owner,
            Query::from("notes"),
            ReadOpts::for_read_tier(ReadTier::Remote),
        );
        assert_eq!(
            opening(&mut read, &[&owner, &authority]),
            (vec!["retained".into()], DurabilityTier::Global),
        );
        drop(read);
        block_on(owner.close()).unwrap();
    }
    let owner = owner(Some(&path));
    let tab = foreground(2);
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    block_on(tab.wait_for_storage_owner()).unwrap();
    let before = OpenTransactionId::new();
    block_on(tab.begin_serializable(before)).unwrap();
    let mut read = subscribe(&tab, Query::from("notes"), ReadOpts::default());
    assert_eq!(
        opening(&mut read, &[&tab, &owner]),
        (vec!["retained".into()], DurabilityTier::Local),
    );
    let observe = |id| {
        block_on(
            tab.all_settled(
                tab.prepare_query(&Query::from("notes")).unwrap(),
                ReadOpts::for_read_tier(ReadTier::LocalOnly),
                id,
                true,
                None,
            )
            .unwrap(),
        )
        .unwrap()
    };
    assert!(observe(before).rows.is_empty());
    let after = OpenTransactionId::new();
    block_on(tab.begin_serializable(after)).unwrap();
    let accepted = observe(after);
    assert_eq!(accepted.rows.len(), 1);
    assert_eq!(accepted.settlements.len(), 1);
    assert_eq!(
        accepted.settlements[0].0.node,
        NodeUuid::from_bytes([90; 16])
    );
    assert_eq!(accepted.settlements[0].0.time.0, accepted_tx);
}

/// Alice's transaction in a fresh tab reads Bob's cached row through the
/// admitted owner while the authority is offline. Mergeable preparation must
/// not mistake empty foreground memory for a missing row.
#[test]
fn foreground_transaction_preparation_reads_its_offline_owner() {
    use jazz::tools::OpenTransactionId;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    let authority = open(90, None);
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    insert(&authority, "retained", true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("transaction-owner.sqlite");
    {
        let owner = owner(Some(&path));
        connect(&owner, &authority);
        let mut read = subscribe(
            &owner,
            Query::from("notes"),
            ReadOpts::for_read_tier(ReadTier::Remote),
        );
        assert_eq!(
            opening(&mut read, &[&owner, &authority]).0,
            vec!["retained"]
        );
        drop(read);
        block_on(owner.close()).unwrap();
    }
    let owner = owner(Some(&path));
    {
        let tab = foreground(2);
        tab.set_remote_link_hint(RemoteLinkHint::Failed);
        connect(&tab, &owner);
        turns(&[&tab, &owner]);
        block_on(tab.wait_for_storage_owner()).unwrap();
        let tx = OpenTransactionId::new();
        block_on(tab.begin_mergeable(tx)).unwrap();
        let query = tab.prepare_query(&Query::from("notes")).unwrap();
        let handle = tab.mergeable_tx_ref(tx);
        let mut read = pin!(handle.all_prepared(&query));
        let mut result = None;
        for _ in 0..100 {
            if let Poll::Ready(rows) = read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                result = Some(rows.unwrap());
                break;
            }
            for db in [&tab, &owner] {
                block_on(db.tick()).unwrap();
            }
        }
        let rows = result.expect("the offline owner answers the transaction read");
        assert_eq!(rows.len(), 1);
    }
}

/// Alice seeds a new offline owner with Bob's structural catalogue capture.
/// The capture contains no read policies; Alice's pending inputs must remain
/// readable to her sibling and after restart without claiming acceptance.
#[test]
fn cached_catalogue_keeps_cold_owner_pending_previews() {
    let authority = open(90, None);
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    let capture = authority
        .take_authenticated_catalogue_state()
        .unwrap()
        .capture
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cached-cold-owner.sqlite");
    let open_owner = || {
        let db = cold_owner(Some(&path));
        // SAFETY: this capture came from this fixture's own authority and
        // application scope, before the owner admits any foreground.
        unsafe { db.install_cached_catalogue(&capture) }.unwrap();
        db
    };
    {
        let owner = open_owner();
        let writer = cold_foreground(1);
        connect(&writer, &owner);
        turns(&[&writer, &owner]);
        let (_, tx) = insert(&writer, "offline with catalogue", true);
        turns(&[&writer, &owner]);
        let sibling = cold_foreground(2);
        connect(&sibling, &owner);
        let mut stream = subscribe(&sibling, Query::from("notes"), ReadOpts::default());
        assert_eq!(
            opening(&mut stream, &[&sibling, &owner]),
            (vec!["offline with catalogue".into()], DurabilityTier::Local)
        );
        assert_wait_pending(writer.wait_for_transaction(tx, DurabilityTier::Global));
        block_on(writer.close()).unwrap();
        block_on(sibling.close()).unwrap();
        block_on(owner.close()).unwrap();
    }
    let owner = open_owner();
    let reopened = cold_foreground(3);
    connect(&reopened, &owner);
    let mut stream = subscribe(&reopened, Query::from("notes"), ReadOpts::default());
    assert_eq!(
        opening(&mut stream, &[&reopened, &owner]),
        (vec!["offline with catalogue".into()], DurabilityTier::Local)
    );
    assert_eq!(values(&reopened), vec!["offline with catalogue"]);
}
