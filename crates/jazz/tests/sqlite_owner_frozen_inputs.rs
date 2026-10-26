//! Recovery of private drafts whose inherited branch base is a snapshot.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{
    Db, DbConfig, DbIdentity, ExactWriteTarget, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint,
    SubscriptionEvent, UpdateOptions, WriteTarget, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
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
                        .column("note", ColumnType::Text)
                        .column("branch", ColumnType::Uuid)
                        .branch_by("branch")
                        .policies(common::allow_all_policies()),
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
fn branch(n: u8) -> BranchSelector {
    BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([n; 16])))])
}
fn query(n: u8) -> Query {
    Query::from("notes").filter(eq(
        col("branch"),
        lit(Value::Uuid(uuid::Uuid::from_bytes([n; 16]))),
    ))
}
fn connect(client: &Db, server: &Db) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, alice());
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn text(db: &Db, n: u8) -> Vec<(String, String)> {
    let q = db.prepare_query(&query(n)).unwrap();
    block_on(db.all(&q, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .iter()
        .map(|r| {
            let cell = |name: &str| {
                r.cells()
                    .find(|(n, _)| *n == name)
                    .unwrap()
                    .1
                    .unwrap()
                    .clone()
            };
            match (cell("text"), cell("note")) {
                (Value::String(a), Value::String(b)) => (a, b),
                _ => panic!("not text"),
            }
        })
        .collect()
}
fn insert(db: &Db) -> RowUuid {
    block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("text".into(), Value::String("old".into())),
            ("note".into(), Value::String("base".into())),
        ]),
        InsertOptions {
            target: ExactWriteTarget::Branch(branch(3)),
            ..Default::default()
        },
    ))
    .unwrap()
    .row_uuid()
}

/// Alice freezes the old base, then Bob updates the live base. After Alice's
/// owner reopens, a new tab querying only the draft must inherit the old text,
/// retain its own patch and later agree with the authority's accepted result.
#[test]
fn a_reloaded_frozen_draft_uses_the_historical_base_not_the_live_base() {
    let server = open(90, None);
    server.set_authority();
    let row = insert(&server);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner.sqlite");
    {
        let owner = open(50, Some(&path));
        owner.set_scope_isolated_storage_owner().unwrap();
        owner.set_remote_link_hint(RemoteLinkHint::Live);
        connect(&owner, &server);
        let q = owner.prepare_query(&query(3)).unwrap();
        let _warm =
            block_on(owner.subscribe(&q, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
        turns(&[&owner, &server]);
        owner.set_remote_link_hint(RemoteLinkHint::Failed);
        let frozen = block_on(owner.capture_snapshot()).unwrap();
        assert!(frozen.dots.is_empty());
        block_on(server.update(
            "notes",
            row,
            BTreeMap::from([("text".into(), Value::String("new".into()))]),
            UpdateOptions {
                target: WriteTarget::BranchView {
                    head: branch(3),
                    base: None,
                },
                ..Default::default()
            },
        ))
        .unwrap();
        turns(&[&server, &owner]);
        assert_eq!(text(&owner, 3), vec![("new".into(), "base".into())]);
        block_on(owner.update(
            "notes",
            row,
            BTreeMap::from([("note".into(), Value::String("draft".into()))]),
            UpdateOptions {
                target: WriteTarget::BranchView {
                    head: branch(4),
                    base: Some(BranchViewBase::Snapshot {
                        branch: branch(3),
                        snapshot: frozen,
                    }),
                },
                ..Default::default()
            },
        ))
        .unwrap();
        assert_eq!(text(&owner, 4), vec![("old".into(), "draft".into())]);
    }
    let owner = open(50, Some(&path));
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = open(2, None);
    tab.set_private_owner_client().unwrap();
    connect(&tab, &owner);
    let q = tab.prepare_query(&query(4)).unwrap();
    let mut stream = block_on(tab.subscribe(&q, ReadOpts::default())).unwrap();
    turns(&[&tab, &owner]);
    let mut opened = false;
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Rejected { reason } => {
                panic!("frozen draft recovery refused: {reason}")
            }
            SubscriptionEvent::Delta {
                reset: true,
                settled: true,
                ..
            } => opened = true,
            _ => {}
        }
    }
    assert!(opened);
    assert_eq!(text(&tab, 4), vec![("old".into(), "draft".into())]);
    connect(&owner, &server);
    turns(&[&owner, &server, &tab]);
    assert_eq!(text(&server, 4), vec![("old".into(), "draft".into())]);
    assert_eq!(text(&tab, 4), text(&server, 4));
}

fn seeded_owner() -> (Db, Db, RowUuid, jazz::protocol::SnapshotRef) {
    let server = open(90, None);
    server.set_authority();
    let row = insert(&server);
    let owner = open(50, None);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Live);
    connect(&owner, &server);
    let q = owner.prepare_query(&query(3)).unwrap();
    let _warm = block_on(owner.subscribe(&q, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&[&owner, &server]);
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let first = block_on(owner.capture_snapshot()).unwrap();
    block_on(server.update(
        "notes",
        row,
        BTreeMap::from([("text".into(), Value::String("new".into()))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: branch(3),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
    turns(&[&server, &owner]);
    (server, owner, row, first)
}
fn draft(owner: &Db, row: RowUuid, head: u8, snapshot: jazz::protocol::SnapshotRef) {
    block_on(owner.update(
        "notes",
        row,
        BTreeMap::from([("note".into(), Value::String("draft".into()))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: branch(head),
                base: Some(BranchViewBase::Snapshot {
                    branch: branch(3),
                    snapshot,
                }),
            },
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Alice's two drafts freeze different versions of the same base coordinate.
/// A single recovery envelope must retain both cuts while the current base is
/// newer, and both drafts must later agree with Bob's authority.
#[test]
fn multiple_frozen_cuts_recover_without_replacing_the_current_base() {
    let (server, owner, row, first) = seeded_owner();
    let second = block_on(owner.capture_snapshot()).unwrap();
    assert!(second.global_base > first.global_base);
    draft(&owner, row, 4, first);
    draft(&owner, row, 5, second);
    let tab = open(2, None);
    tab.set_private_owner_client().unwrap();
    connect(&tab, &owner);
    let q = tab.prepare_query(&Query::from("notes")).unwrap();
    let _stream = block_on(tab.subscribe(&q, ReadOpts::default())).unwrap();
    turns(&[&tab, &owner]);
    assert_eq!(text(&tab, 3), vec![("new".into(), "base".into())]);
    assert_eq!(text(&tab, 4), vec![("old".into(), "draft".into())]);
    assert_eq!(text(&tab, 5), vec![("new".into(), "draft".into())]);
    block_on(server.update(
        "notes",
        row,
        BTreeMap::from([("text".into(), Value::String("latest".into()))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: branch(3),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
    turns(&[&owner, &server, &tab]);
    for head in [4, 5] {
        assert_eq!(text(&tab, head), text(&server, head));
    }
    assert_eq!(text(&tab, 4), vec![("old".into(), "draft".into())]);
    assert_eq!(text(&tab, 5), vec![("new".into(), "draft".into())]);
}

/// Mallory omits or changes the frozen base or its accepted boundary. Alice's
/// tab must reject the frame before it exposes a draft copied from the wrong
/// version. Wire mutation exercises data and provenance admission directly.
#[test]
fn frozen_input_requires_exact_history_manifest_and_accepted_prefix() {
    use jazz::db::SyncMessage;
    let (_server, owner, row, frozen) = seeded_owner();
    draft(&owner, row, 4, frozen);
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
        query: query(4),
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
            .any(|message| matches!(message, SyncMessage::OwnerInputsV1 { .. }))
    );
    for mode in [
        "missing image",
        "missing evidence",
        "missing manifest",
        "duplicate",
        "wrong cut",
        "wrong version",
        "false absence",
        "low prefix",
        "optimistic prefix",
    ] {
        let tab = open(2, None);
        tab.set_private_owner_client().unwrap();
        let (link, mut source) = duplex_transport::duplex();
        block_on(tab.connect_upstream(link));
        for mut message in messages.clone() {
            match (&mut message, mode) {
                (SyncMessage::Rows { .. }, "missing image")
                | (SyncMessage::AcceptedRowsV1 { .. }, "missing evidence") => continue,
                (
                    SyncMessage::OwnerInputsV1 {
                        through, frozen, ..
                    },
                    mode,
                ) => {
                    assert_eq!(frozen.len(), 1);
                    match mode {
                        "missing manifest" => frozen.clear(),
                        "duplicate" => frozen.push(frozen[0].clone()),
                        "wrong cut" => frozen[0].at += 1,
                        "wrong version" => frozen[0].base.seq = Some(frozen[0].at + 1),
                        "false absence" => frozen[0].base.seq = None,
                        "low prefix" => *through = 0,
                        "optimistic prefix" => *through = 1 << 52,
                        _ => {}
                    }
                }
                _ => {}
            }
            source.send(message).unwrap();
        }
        let error = block_on(tab.tick()).unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::Protocol, "{mode}: {error}");
        let q = tab.prepare_query(&query(4)).unwrap();
        assert!(
            block_on(tab.all(&q, ReadOpts::for_read_tier(ReadTier::LocalOnly))).is_err(),
            "{mode}"
        );
    }
}
