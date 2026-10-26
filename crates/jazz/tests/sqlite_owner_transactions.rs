//! Serializable read validation while private owner inputs are replayed.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, OpenTransactionId, RowDescriptor, Schema,
    TableName, TableSchema,
};
use jazz::tx::{DurabilityTier, Fate};
use std::collections::BTreeMap;
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn config(node: u8) -> DbConfig {
    let schema = common::compile_schema(&Schema::from([(
        TableName::new("counters"),
        TableSchema {
            synthetic_references_v1: Default::default(),
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("value", ColumnType::Integer)
                    .merge_strategy(ColumnMergeStrategy::Counter),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: common::allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]));
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn open(node: u8) -> Db {
    block_on(Db::open(config(node))).unwrap()
}
fn owner() -> Db {
    let db = open(50);
    db.set_scope_isolated_storage_owner().unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    db
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
fn row() -> RowUuid {
    RowUuid::from_bytes([7; 16])
}
fn insert(db: &Db) {
    block_on(db.insert(
        "counters",
        BTreeMap::from([("value".into(), Value::I32(0))]),
        InsertOptions {
            row_id: Some(row()),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn add(db: &Db, delta: i32) {
    block_on(db.update(
        "counters",
        row(),
        BTreeMap::from([("value".into(), Value::I32(delta))]),
        Default::default(),
    ))
    .unwrap();
}
fn query() -> Query {
    Query::from("counters").select(["value"])
}
fn value(db: &Db) -> i32 {
    let query = db.prepare_query(&query()).unwrap();
    let rows = block_on(db.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap();
    let Some(Value::I32(value)) = rows[0].cell_at(0) else {
        panic!("not a counter")
    };
    value
}
fn begin_read(db: &Db, expected: i32) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let query = db.prepare_query(&query()).unwrap();
    let rows = block_on(db.serializable_tx_ref(tx).all_prepared(&query)).unwrap();
    assert_eq!(rows[0].cell_at(0), Some(Value::I32(expected)));
    tx
}
fn subscribe(db: &Db) -> jazz::db::SubscriptionStream {
    let query = db.prepare_query(&query()).unwrap();
    block_on(db.subscribe(&query, ReadOpts::default())).unwrap()
}
fn hydrate(db: &Db, server: &Db) {
    db.set_remote_link_hint(RemoteLinkHint::Live);
    connect(db, server);
    let query = db.prepare_query(&query()).unwrap();
    let _warm = block_on(db.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&[db, server]);
    assert_eq!(value(db), 0);
    db.set_remote_link_hint(RemoteLinkHint::Failed);
}

/// Alice reads her own counter before the owner echoes it. Promotion changes
/// its publication number, not its content. The qualified owner admits the
/// commit and Bob's authority validates the unchanged read through normal sync.
#[test]
fn an_own_echo_does_not_create_a_serializable_read_conflict() {
    let owner = owner();
    let tab = open(1);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    tab.set_private_owner_client().unwrap();
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    tab.seed_seq_floor((1u64 << 52) + (1u64 << 51));
    insert(&tab);
    add(&tab, 2);
    let tx = begin_read(&tab, 2);
    connect(&tab, &owner);
    let _query = subscribe(&tab);
    turns(&[&tab, &owner]);
    assert_eq!(value(&tab), 2);
    let receipt = block_on(tab.commit_serializable_handle(tx)).unwrap().0;
    let server = open(90);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&tab, &owner, &server]);
    assert_eq!(tab.write_state(receipt).unwrap().fate, Fate::Accepted);
    assert_eq!(value(&server), 2);
}

/// Bob's accepted counter change arrives beneath Alice's pending delta. The
/// pending row keeps its original numeric version but its changed content must
/// invalidate Alice's earlier serializable read.
#[test]
fn a_rebased_counter_change_conflicts_even_at_the_same_pending_position() {
    let server = open(90);
    server.set_authority();
    insert(&server);
    let owner = owner();
    hydrate(&owner, &server);
    add(&owner, 2);
    let tx = begin_read(&owner, 2);
    let before = block_on(owner.all(
        &owner.prepare_query(&query()).unwrap(),
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
    ))
    .unwrap()[0]
        .seq();
    add(&server, 3);
    owner.set_remote_link_hint(RemoteLinkHint::Live);
    // A read asks for the new authority base; the owner's unsent upload remains
    // queued until its next tick, so receive the authority answer first.
    let _query = subscribe(&owner);
    block_on(server.tick()).unwrap();
    block_on(owner.tick()).unwrap();
    assert_eq!(value(&owner), 5);
    let after = block_on(owner.all(
        &owner.prepare_query(&query()).unwrap(),
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
    ))
    .unwrap()[0]
        .seq();
    assert_eq!(before, after);
    assert_eq!(
        block_on(owner.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}

/// Bob changes the accepted base while Alice's private owner is offline. Her
/// pending delta still becomes the same original row version at the authority,
/// but its changed result must reject the subsequent read-only transaction.
#[test]
fn authority_checks_private_owner_read_content_after_pending_replay() {
    for (private, changed) in [(false, false), (false, true), (true, false), (true, true)] {
        let server = open(90);
        server.set_authority();
        insert(&server);
        let owner = if private { owner() } else { open(50) };
        hydrate(&owner, &server);
        add(&owner, 2);
        let tx = begin_read(&owner, 2);
        if changed {
            add(&server, 3);
        }
        let receipt = block_on(owner.commit_serializable_handle(tx)).unwrap().0;
        turns(&[&owner, &server]);
        let state = owner.write_state(receipt).unwrap();
        if changed {
            assert!(
                matches!(state.fate, Fate::Rejected(_)),
                "changed read was accepted: {state:?}"
            );
        } else {
            assert_eq!(state.fate, Fate::Accepted);
            assert_eq!(state.durability, DurabilityTier::Global);
        }
    }
}

/// Alice's read evidence survives owner reopen with its original pending
/// uploads. Bob's authority still distinguishes unchanged and changed results.
#[test]
fn persisted_read_content_is_validated_after_owner_reopen() {
    for changed in [false, true] {
        let server = open(90);
        server.set_authority();
        insert(&server);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("owner.sqlite");
        let receipt = {
            let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
            owner.set_scope_isolated_storage_owner().unwrap();
            hydrate(&owner, &server);
            add(&owner, 2);
            let tx = begin_read(&owner, 2);
            block_on(owner.commit_serializable_handle(tx)).unwrap().0
        };
        if changed {
            add(&server, 3);
        }
        let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
        connect(&owner, &server);
        turns(&[&owner, &server]);
        let state = owner.write_state(receipt).unwrap();
        if changed {
            assert!(matches!(state.fate, Fate::Rejected(_)));
        } else {
            assert_eq!(state.fate, Fate::Accepted);
        }
    }
}

/// Mallory alters Alice's read evidence or dependency map on the wire. Bob's
/// authority refuses each malformed claim instead of validating only versions.
/// The transport mutation is necessary to exercise untrusted wire admission.
#[test]
fn malformed_read_content_or_origins_cannot_bypass_authority_validation() {
    use jazz::db::{SyncMessage, Transport, TransportError};
    use std::{cell::Cell, rc::Rc};
    struct Tamper {
        link: Box<dyn Transport>,
        mode: &'static str,
        changed: Rc<Cell<usize>>,
    }
    impl Transport for Tamper {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            let mut json = serde_json::to_value(message).unwrap();
            if let Some(kind) = json
                .get_mut("Commit")
                .and_then(|commit| commit.get_mut("kind"))
                .and_then(|kind| kind.get_mut("SerializableObservedV1"))
            {
                self.changed.set(self.changed.get() + 1);
                match self.mode {
                    "missing read" => {
                        kind["content"].as_array_mut().unwrap().clear();
                    }
                    "missing row" => {
                        kind["content"][0].as_array_mut().unwrap().clear();
                    }
                    "wrong content" => {
                        let prior = kind["content"][0][0][0].as_u64().unwrap();
                        kind["content"][0][0][0] = (prior ^ 1).into();
                    }
                    "duplicate origin" => {
                        let own = kind["own"].as_array_mut().unwrap();
                        own.push(own[0].clone());
                    }
                    _ => unreachable!(),
                }
            }
            self.link.send(serde_json::from_value(json).unwrap())
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.link.try_recv()
        }
    }
    for mode in [
        "missing read",
        "missing row",
        "wrong content",
        "duplicate origin",
    ] {
        let server = open(90);
        server.set_authority();
        insert(&server);
        let owner = owner();
        hydrate(&owner, &server);
        add(&owner, 2);
        let tx = begin_read(&owner, 2);
        let receipt = block_on(owner.commit_serializable_handle(tx)).unwrap().0;
        let changed = Rc::new(Cell::new(0));
        let (a, b) = duplex_transport::duplex();
        block_on(owner.connect_upstream(Box::new(Tamper {
            link: a,
            mode,
            changed: changed.clone(),
        })));
        server.accept_subscriber(b, alice());
        turns(&[&owner, &server]);
        assert!(
            changed.get() > 0,
            "{mode}: no actual evidence frame was tested"
        );
        assert!(
            matches!(owner.write_state(receipt).unwrap().fate, Fate::Rejected(_)),
            "{mode}"
        );
        assert_eq!(value(&server), 2);
    }
}

/// Alice's emitted read digest matches an independently authored postcard
/// preimage. The new commit kind also has fixed append-only wire bytes. Direct
/// encoding inspection is needed to pin the durable upload and hashing contract.
#[test]
fn read_content_and_commit_kind_match_independent_versioned_fixtures() {
    use jazz::db::SyncMessage;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/owner-read-content.json"
    ))
    .unwrap();
    assert_eq!(fixture["version"], 2);
    assert_eq!(fixture["wireRole"], "jazz.sqlite.serializable-observed.v1");
    assert_eq!(
        fixture["contentRole"],
        "jazz.sqlite.serializable-read-content.v1"
    );
    let owner = owner();
    insert(&owner);
    add(&owner, 2);
    let tx = begin_read(&owner, 2);
    block_on(owner.commit_serializable_handle(tx)).unwrap();
    let (a, mut peer) = duplex_transport::duplex();
    block_on(owner.connect_upstream(a));
    block_on(owner.tick()).unwrap();
    let commit = std::iter::from_fn(|| peer.try_recv())
        .map(|message| serde_json::to_value(message).unwrap())
        .find(|message| {
            message["Commit"]["kind"]
                .get("SerializableObservedV1")
                .is_some()
        })
        .unwrap();
    let preimage = hex::decode(fixture["preimageHex"].as_str().unwrap()).unwrap();
    let expected = blake3::derive_key(fixture["contentRole"].as_str().unwrap(), &preimage);
    assert_eq!(
        commit["Commit"]["kind"]["SerializableObservedV1"]["content"],
        serde_json::json!([[expected.to_vec()]])
    );
    let message: SyncMessage = serde_json::from_value(serde_json::json!({"Commit":{
        "node":vec![0x22u8;16],"tx":128,
        "kind":{"SerializableObservedV1":{"snapshot":127,"reads":[],"own":[[(1u64<<52)+1,vec![0x11u8;16],300]],"content":[]}},
        "ops":[],"author":null,"session":null,"expect":[]
    }})).unwrap();
    let bytes = hex::decode(fixture["commitHex"].as_str().unwrap()).unwrap();
    assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(message).unwrap()
    );
}

/// Alice's pending counter is promoted after her other tab's earlier delta.
/// Its original identity survives, but the changed value must invalidate a
/// serializable read made before that owner prefix was received.
#[test]
fn promotion_cannot_hide_a_foreign_counter_change() {
    let server = open(90);
    server.set_authority();
    insert(&server);
    let owner = owner();
    hydrate(&owner, &server);
    let tab = open(1);
    tab.set_private_owner_client().unwrap();
    hydrate(&tab, &owner);
    tab.seed_seq_floor((1u64 << 52) + (1u64 << 51));
    add(&tab, 2);
    let tx = begin_read(&tab, 2);
    add(&owner, 3);
    connect(&tab, &owner);
    let _query = subscribe(&tab);
    turns(&[&tab, &owner]);
    assert_eq!(value(&tab), 5);
    assert_eq!(
        block_on(tab.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}

/// Alice's owner sends an insert followed by a counter update. Mallory drops
/// the predecessor unit but keeps its dependent update; the foreground must
/// refuse that incomplete replay instead of returning an empty successful read.
/// Wire omission is necessary to exercise the complete-input boundary.
#[test]
fn a_missing_pending_predecessor_cannot_produce_a_successful_partial_replay() {
    use jazz::db::SyncMessage;
    let owner = owner();
    insert(&owner);
    add(&owner, 2);
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
        query: query(),
        binding: BTreeMap::new(),
        at: None,
        include_deleted: false,
    })
    .unwrap();
    block_on(owner.tick()).unwrap();
    let messages = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    for omit in [false, true] {
        let tab = open(2);
        tab.set_private_owner_client().unwrap();
        let (link, mut source) = duplex_transport::duplex();
        block_on(tab.connect_upstream(link));
        for mut message in messages.clone() {
            if let SyncMessage::OwnerInputsV1 { units, .. } = &mut message {
                assert_eq!(units.len(), 2);
                if omit {
                    units.remove(0);
                }
            }
            source.send(message).unwrap();
        }
        let result = block_on(tab.tick());
        if omit {
            assert_eq!(result.unwrap_err().code, ErrorCode::Protocol);
            let query = tab.prepare_query(&query()).unwrap();
            assert!(
                block_on(tab.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).is_err()
            );
        } else {
            result.unwrap();
            assert_eq!(value(&tab), 2);
        }
    }
}
