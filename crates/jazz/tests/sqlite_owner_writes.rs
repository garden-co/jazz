//! Foreground write admission through a qualified, private storage owner.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, ReadTier, RemoteLinkHint, SyncMessage, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::{collections::BTreeMap, path::Path};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn open(node: u8, path: Option<&Path>) -> Db {
    let schema = common::compile_schema(
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
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    );
    let db = match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    if node != 90 {
        db.set_remote_link_hint(RemoteLinkHint::Failed);
    }
    db
}
fn foreground(node: u8) -> Db {
    let db = open(node, None);
    db.set_private_owner_client().unwrap();
    db
}
fn owner(path: Option<&Path>) -> Db {
    let db = open(50, path);
    db.set_scope_isolated_storage_owner().unwrap();
    db
}
fn connect(client: &Db, owner: &Db) -> jazz::db::Upstream {
    let (a, b) = duplex_transport::duplex();
    let upstream = block_on(client.connect_upstream(a));
    owner.accept_subscriber(b, alice());
    upstream
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn cells(text: &str, allowed: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("text".into(), Value::String(text.into())),
        ("allowed".into(), Value::Bool(allowed)),
    ])
}
fn insert(db: &Db, text: &str, allowed: bool) -> jazz::db::WriteHandle {
    block_on(db.insert("notes", cells(text, allowed), Default::default())).unwrap()
}
fn values(db: &Db) -> Vec<String> {
    let query = db
        .prepare_query(&Query::from("notes").select(["text"]))
        .unwrap();
    let rows = block_on(db.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap();
    let mut values = rows
        .iter()
        .map(|row| match row.cell_at(0) {
            Some(Value::String(value)) => value.clone(),
            other => panic!("{other:?}"),
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}
fn denied(db: &Db) {
    let error =
        block_on(db.insert("notes", cells("unadmitted", true), Default::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("non-durable foreground"),
        "{error:?}"
    );
    assert!(!values(db).contains(&"unadmitted".into()));
}

/// Alice's tab can write only after its current private owner answers. A generic
/// relay, a newly connected transport and a detached owner confer no admission.
#[test]
fn writes_require_current_private_owner_admission() {
    let tab = foreground(1);
    denied(&tab);
    let relay = open(51, None);
    connect(&tab, &relay);
    turns(&[&tab, &relay]);
    denied(&tab);
    let owner = owner(None);
    let upstream = connect(&tab, &owner);
    denied(&tab);
    turns(&[&tab, &owner]);
    insert(&tab, "admitted", true);
    assert_eq!(values(&tab), ["admitted"]);
    assert!(tab.detach_connection(&upstream));
    denied(&tab);
    connect(&tab, &owner);
    denied(&tab);
    turns(&[&tab, &owner]);
    insert(&tab, "reconnected", true);
    assert_eq!(values(&tab), ["admitted", "reconnected"]);
}

/// Alice's ordinary tab can stage an offline write before its worker connects;
/// only Bob's authority can accept that write and earn Global durability.
#[test]
fn ordinary_worker_mode_allows_writes_before_connecting() {
    let tab = open(1, None);
    tab.set_non_durable_client();
    let tx = insert(&tab, "offline ordinary", true).mergeable_tx_id();
    assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Pending);
    assert_eq!(values(&tab), ["offline ordinary"]);
    let worker = open(50, None);
    connect(&tab, &worker);
    turns(&[&tab, &worker]);
    assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Pending);
    assert_ne!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::Global
    );
    let denied = insert(&tab, "denied by authority", false).mergeable_tx_id();
    turns(&[&tab, &worker]);
    assert_eq!(tab.write_state(denied).unwrap().fate, Fate::Pending);
    let server = open(90, None);
    server.set_authority();
    connect(&worker, &server);
    turns(&[&tab, &worker, &server]);
    assert_eq!(tab.write_state(tx).unwrap().fate, Fate::Accepted);
    assert_eq!(
        tab.write_state(tx).unwrap().durability,
        DurabilityTier::Global
    );
    assert!(matches!(
        tab.write_state(denied).unwrap().fate,
        Fate::Rejected(_)
    ));
    assert_eq!(values(&server), ["offline ordinary"]);
}

/// Alice's E2EE tab cannot remove its admission fence by selecting ordinary
/// mode, reconnecting to a generic worker, or promoting itself to authority.
#[test]
fn private_foreground_mode_cannot_be_downgraded() {
    let tab = foreground(1);
    tab.set_non_durable_client();
    denied(&tab);
    let owner = owner(None);
    let connection = connect(&tab, &owner);
    turns(&[&tab, &owner]);
    insert(&tab, "admitted private", true);
    assert!(tab.detach_connection(&connection));
    tab.set_non_durable_client();
    let ordinary = open(51, None);
    connect(&tab, &ordinary);
    turns(&[&tab, &ordinary]);
    denied(&tab);
    assert!(
        values(&ordinary).is_empty(),
        "private retries must not escape before admission"
    );
    tab.set_authority();
    assert!(!tab.is_authority());
    assert!(
        block_on(tab.insert("notes", cells("bad promotion", true), Default::default())).is_err()
    );
}

/// Alice selects her host role before peer admission or writes. Reusing an
/// ordinary connection or an already written store cannot adopt private inputs.
#[test]
fn private_foreground_mode_requires_an_unused_host() {
    let standalone = open(90, None);
    insert(&standalone, "already written", true);
    assert!(standalone.set_private_owner_client().is_err());
    let ordinary = open(1, None);
    ordinary.set_non_durable_client();
    insert(&ordinary, "pending ordinary", true);
    assert!(ordinary.set_private_owner_client().is_err());
    let tab = open(2, None);
    let worker = open(50, None);
    let connection = connect(&tab, &worker);
    assert!(tab.detach_connection(&connection));
    assert!(tab.set_private_owner_client().is_err());
    let authority = open(90, None);
    authority.set_authority();
    assert!(authority.set_private_owner_client().is_err());
    let leased = open(3, None);
    leased.seed_seq_floor(10_000);
    leased.set_private_owner_client().unwrap();
    denied(&leased);
}

/// Mallory's worker cannot turn Alice's ordinary tab into a private foreground.
/// Wire injection is needed here because a correct worker never sends an
/// unsolicited owner acknowledgement; all subject/schema/node fields are valid.
#[test]
fn peer_messages_cannot_select_private_foreground_mode() {
    let tab = open(1, None);
    tab.set_non_durable_client();
    let (up, mut peer) = duplex_transport::duplex();
    let (mut bridge, down) = duplex_transport::duplex();
    let owner = owner(None);
    block_on(tab.connect_upstream(up));
    owner.accept_subscriber(down, alice());
    block_on(tab.tick()).unwrap();
    let mut schema = None;
    while let Some(message) = peer.try_recv() {
        if let SyncMessage::Schema { version } = &message {
            schema = Some(*version);
        }
        bridge.send(message).unwrap();
    }
    block_on(owner.tick()).unwrap();
    while let Some(message) = bridge.try_recv() {
        peer.send(message).unwrap();
    }
    peer.send(SyncMessage::OwnerReadyV1 {
        schema: schema.unwrap(),
        subject: alice().canonical().into(),
        node: [50; 16],
    })
    .unwrap();
    assert_eq!(block_on(tab.tick()).unwrap_err().code, ErrorCode::Protocol);
    assert!(block_on(tab.wait_for_storage_owner()).is_err());
}

/// Alice writes while her owner is offline from Bob's authority. Owner storage
/// earns Local only; Bob later accepts one write and rejects the other.
#[test]
fn admitted_writes_keep_real_storage_and_authority_receipts() {
    let owner = owner(None);
    owner.require_owner_durability_flush();
    let tab = foreground(1);
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    let yes = insert(&tab, "yes", true).mergeable_tx_id();
    let no = insert(&tab, "no", false).mergeable_tx_id();
    turns(&[&tab, &owner]);
    for id in [yes, no] {
        let state = tab.write_state(id).unwrap();
        assert_eq!(state.fate, Fate::Pending);
        assert_eq!(state.durability, DurabilityTier::None);
    }
    let barrier = owner.prepare_owner_durability().unwrap().unwrap();
    owner.confirm_owner_durability(barrier).unwrap();
    owner.flush_owner_durability().unwrap();
    turns(&[&tab, &owner]);
    for id in [yes, no] {
        let state = tab.write_state(id).unwrap();
        assert_eq!(state.fate, Fate::Pending);
        assert_eq!(state.durability, DurabilityTier::Local);
    }
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&tab, &owner, &server]);
    assert_eq!(tab.write_state(yes).unwrap().fate, Fate::Accepted);
    assert_eq!(
        tab.write_state(yes).unwrap().durability,
        DurabilityTier::Global
    );
    assert!(matches!(
        tab.write_state(no).unwrap().fate,
        Fate::Rejected(_)
    ));
    assert_eq!(values(&tab), ["yes"]);
}

/// Alice's Global receipt through her private owner must leave the accepted
/// content locally readable at the start of her next E2EE transaction.
#[test]
fn global_receipt_preserves_local_accepted_content_for_the_next_transaction() {
    let owner = owner(None);
    let tab = foreground(1);
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    connect(&tab, &owner);
    turns(&[&tab, &owner, &server]);
    let id = insert(&tab, "accepted account", true).mergeable_tx_id();
    turns(&[&tab, &owner, &server]);
    assert_eq!(tab.write_state(id).unwrap().fate, Fate::Accepted);
    assert_eq!(values(&tab), ["accepted account"]);
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(tab.begin_serializable(tx)).unwrap();
    let read = block_on(
        tab.all_settled(
            tab.prepare_query(&Query::from("notes")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            tx,
            true,
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(read.rows.len(), 1);
    assert_eq!(read.settlements.len(), 1);
}

/// Alice closes her original tab and reopens the durable owner. A new tab's
/// propagated query recovers both writes of an atomic unit before editing them.
#[test]
fn foreground_transaction_recovers_after_both_tab_and_owner_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner.sqlite");
    {
        let owner = owner(Some(&path));
        let tab = foreground(1);
        connect(&tab, &owner);
        turns(&[&tab, &owner]);
        block_on(tab.transaction(async |tx| {
            tx.insert("notes", cells("first", true), Default::default())
                .await?;
            tx.insert("notes", cells("second", true), Default::default())
                .await?;
            Ok(())
        }))
        .unwrap();
        turns(&[&tab, &owner]);
    }
    let owner = owner(Some(&path));
    let tab = foreground(2);
    connect(&tab, &owner);
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let _subscription = block_on(tab.subscribe(&query, ReadOpts::default())).unwrap();
    turns(&[&tab, &owner]);
    assert_eq!(values(&tab), ["first", "second"]);
    let server = open(90, None);
    server.set_authority();
    connect(&owner, &server);
    turns(&[&tab, &owner, &server]);
    assert_eq!(values(&server), ["first", "second"]);
}

/// Mallory alters a real private owner's acknowledgement. Wrong subject,
/// schema or own-node aliases fail closed before Alice can mint a write.
#[test]
fn misbound_owner_acknowledgements_fail_closed() {
    for mode in 0..3 {
        let owner = owner(None);
        let tab = foreground(1);
        let (up, mut bridge_in) = duplex_transport::duplex();
        let (mut bridge_out, down) = duplex_transport::duplex();
        block_on(tab.connect_upstream(up));
        owner.accept_subscriber(down, alice());
        block_on(tab.tick()).unwrap();
        while let Some(message) = bridge_in.try_recv() {
            bridge_out.send(message).unwrap();
        }
        block_on(owner.tick()).unwrap();
        let mut changed = false;
        while let Some(mut message) = bridge_out.try_recv() {
            if let SyncMessage::OwnerReadyV1 {
                schema,
                subject,
                node,
            } = &mut message
            {
                match mode {
                    0 => {
                        *subject = AuthorSubject::authenticated("https://issuer.example", "mallory")
                            .unwrap()
                            .canonical()
                            .into()
                    }
                    1 => schema[0] ^= 1,
                    _ => *node = [1; 16],
                }
                changed = true;
            }
            bridge_in.send(message).unwrap();
        }
        assert!(changed);
        assert_eq!(block_on(tab.tick()).unwrap_err().code, ErrorCode::Protocol);
        assert!(block_on(tab.insert("notes", cells("bad", true), Default::default())).is_err());
    }
}

/// Alice's already admitted tab refuses a different owner's acknowledgement
/// after reconnect, even when Bob's replacement has the same subject and schema.
#[test]
fn reconnect_cannot_silently_replace_the_private_owner() {
    let tab = foreground(1);
    let owner = owner(None);
    connect(&tab, &owner);
    turns(&[&tab, &owner]);
    let replacement = open(51, None);
    replacement.set_scope_isolated_storage_owner().unwrap();
    connect(&tab, &replacement);
    block_on(tab.tick()).unwrap();
    block_on(replacement.tick()).unwrap();
    assert_eq!(block_on(tab.tick()).unwrap_err().code, ErrorCode::Protocol);
}

/// Alice and Bob use independently authored bytes to pin tag 31, field order,
/// subject UTF-8 length and the owner's raw UUID; round trips alone are insufficient.
#[test]
fn owner_ready_v1_matches_independent_bytes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v2/owner-ready.json")).unwrap();
    let bytes: Vec<u8> = fixture["readyHex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let message = SyncMessage::OwnerReadyV1 {
        schema: [0x22; 16],
        subject: alice().canonical().into(),
        node: [0x33; 16],
    };
    assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    assert!(matches!(
        jazz::db::decode_message(&bytes).unwrap(),
        SyncMessage::OwnerReadyV1 { .. }
    ));
}
