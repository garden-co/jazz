//! Readable permission dependencies through a cold private storage owner.
mod common;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SubscriptionEvent, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([2; 16])
}
fn schema() -> JazzSchema {
    schema_with_member_read(PolicyExpr::True)
}
fn schema_with_member_read(read: PolicyExpr) -> JazzSchema {
    let member = common::exists(
        "members",
        vec![
            common::outer_eq("scope", "id"),
            PolicyExpr::eq_session("account", vec!["user".into(), "account".into()]),
        ],
    );
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("scopes").policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_update(Some(member), PolicyExpr::False),
                ),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("scope", "scopes")
                    .column("account", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(read)),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("scope", "scopes")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Update,
                        via_column: "scope".into(),
                        max_depth: None,
                    })),
            )
            .build(),
    )
}
fn open(node: u8, subject: AuthorSubject, schema: JazzSchema) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: subject,
        },
    )))
    .unwrap()
}
fn connect(client: &Db, server: &Db, identity: AuthorSubject) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, identity);
}
fn turns(dbs: &[&Db]) {
    for _ in 0..12 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        turns(dbs);
    }
    panic!("operation did not finish")
}
fn insert(
    db: &Db,
    table: &str,
    fields: impl IntoIterator<Item = (&'static str, Value)>,
) -> RowUuid {
    block_on(db.insert(
        table,
        fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        Default::default(),
    ))
    .unwrap()
    .row_uuid()
}

/// Alice shares a note with Bob. Bob's fresh foreground reads through his cold
/// private owner without separately querying the readable membership tables.
/// Revoking Bob's membership removes the note from the same subscription.
#[test]
fn cold_owner_follows_readable_membership_dependencies_and_revocation() {
    let schema = schema();
    let authority = open(90, AuthorSubject::SYSTEM, schema.clone());
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    let scope = insert(&authority, "scopes", []);
    let membership = insert(
        &authority,
        "members",
        [
            ("scope", Value::Uuid(scope.0)),
            ("account", Value::Uuid(bob().account_id().unwrap().0)),
        ],
    );
    let note = insert(
        &authority,
        "notes",
        [
            ("scope", Value::Uuid(scope.0)),
            ("text", Value::String("shared".into())),
        ],
    );
    let owner = open(50, bob(), schema.without_permissions());
    owner.set_scope_isolated_storage_owner().unwrap();
    let tab = open(1, bob(), schema.without_permissions());
    tab.set_private_owner_client().unwrap();
    connect(&owner, &authority, bob());
    connect(&tab, &owner, bob());
    let dbs = [&tab, &owner, &authority];
    drive(&dbs, tab.wait_for_storage_owner()).unwrap();
    drive(&dbs, tab.cover_catalogue("notes", None)).unwrap();
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let mut stream =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&dbs);
    let mut added = Vec::new();
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta { added: rows, .. } => {
                added.extend(rows.iter().map(|row| row.row_uuid()))
            }
            SubscriptionEvent::Rejected { reason } => panic!("query rejected: {reason}"),
            _ => {}
        }
    }
    assert_eq!(added, vec![note]);
    block_on(authority.delete("members", membership, Default::default())).unwrap();
    turns(&dbs);
    assert!(
        std::iter::from_fn(|| stream.try_next_event()).any(|event| matches!(event,
        SubscriptionEvent::Delta {removed,..} if removed.iter().any(|row|row.row_uuid==note)))
    );
    let query = owner.prepare_query(&Query::from("members")).unwrap();
    let rows = block_on(owner.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap();
    assert!(rows.is_empty());
}

fn populated(schema: JazzSchema) -> (Db, RowUuid, RowUuid, RowUuid) {
    let authority = open(90, AuthorSubject::SYSTEM, schema);
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    let scope = insert(&authority, "scopes", []);
    let membership = insert(
        &authority,
        "members",
        [
            ("scope", Value::Uuid(scope.0)),
            ("account", Value::Uuid(bob().account_id().unwrap().0)),
        ],
    );
    let note = insert(
        &authority,
        "notes",
        [
            ("scope", Value::Uuid(scope.0)),
            ("text", Value::String("shared".into())),
        ],
    );
    (authority, scope, membership, note)
}
fn private_reader(authority: &Db, schema: &JazzSchema) -> (Db, Db) {
    let owner = open(50, bob(), schema.without_permissions());
    owner.set_scope_isolated_storage_owner().unwrap();
    let tab = open(1, bob(), schema.without_permissions());
    tab.set_private_owner_client().unwrap();
    connect(&owner, authority, bob());
    connect(&tab, &owner, bob());
    drive(&[&tab, &owner, authority], tab.wait_for_storage_owner()).unwrap();
    drive(
        &[&tab, &owner, authority],
        tab.cover_catalogue("notes", None),
    )
    .unwrap();
    (owner, tab)
}
fn local_ids(db: &Db, table: &str) -> Vec<RowUuid> {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

/// Bob's authority permits the note using an unreadable membership. A private
/// owner must refuse this unsupported projection without receiving that input,
/// and the refusal must not strand a later supported read on the connection.
#[test]
fn unreadable_inputs_refuse_only_the_query_and_never_reach_the_owner() {
    let schema = schema_with_member_read(PolicyExpr::False);
    let (authority, scope, _, _) = populated(schema.clone());
    let (owner, tab) = private_reader(&authority, &schema);
    let dbs = [&tab, &owner, &authority];
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let mut stream =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&dbs);
    assert!(
        matches!(stream.try_next_event(), Some(SubscriptionEvent::Rejected {reason}) if reason.contains("dependency is not readable"))
    );
    assert!(local_ids(&owner, "members").is_empty());
    assert!(local_ids(&owner, "notes").is_empty());
    let query = tab.prepare_query(&Query::from("scopes")).unwrap();
    let mut supported =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&dbs);
    assert!(
        matches!(supported.try_next_event(), Some(SubscriptionEvent::Delta {added,settled:true,..}) if added.len()==1 && added[0].row_uuid()==scope)
    );
}

/// Alice replaces Bob's grant in one accepted transaction. Although the note
/// itself does not change, Bob receives the new input without losing the note;
/// deleting that replacement later revokes the live subscription.
#[test]
fn changed_permission_evidence_refreshes_an_unchanged_result() {
    let schema = schema();
    let (authority, scope, membership, note) = populated(schema.clone());
    let (owner, tab) = private_reader(&authority, &schema);
    let dbs = [&tab, &owner, &authority];
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let mut stream =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&dbs);
    assert!(
        matches!(stream.try_next_event(),Some(SubscriptionEvent::Delta {added,..}) if added.len()==1 && added[0].row_uuid()==note)
    );
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(authority.begin_serializable(tx)).unwrap();
    let replacement = block_on(authority.tx_ref(tx).insert(
        "members",
        std::collections::BTreeMap::from([
            ("scope".into(), Value::Uuid(scope.0)),
            ("account".into(), Value::Uuid(bob().account_id().unwrap().0)),
        ]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    block_on(
        authority
            .tx_ref(tx)
            .delete("members", membership, Default::default()),
    )
    .unwrap();
    block_on(authority.commit_serializable_handle(tx)).unwrap();
    turns(&dbs);
    assert_eq!(local_ids(&owner, "members"), vec![replacement]);
    assert!(!std::iter::from_fn(|| stream.try_next_event()).any(
        |event| matches!(event, SubscriptionEvent::Delta {removed,..} if !removed.is_empty())
    ));
    block_on(authority.delete("members", replacement, Default::default())).unwrap();
    turns(&dbs);
    assert!(std::iter::from_fn(||stream.try_next_event()).any(|event|matches!(event, SubscriptionEvent::Delta {removed,..} if removed.iter().any(|row|row.row_uuid==note))));
}

/// Bob has two simultaneous connections with different claims. Wire inspection
/// is necessary here: a denied input must never leave the authority, even if a
/// downstream policy would hide it. The session with permission receives it.
#[test]
fn readable_inputs_use_each_connections_claims_without_borrowing_another_token() {
    use jazz::db::{SyncMessage, Transport};
    use jazz::tools::{CmpOp, Value as PublicValue};
    use std::collections::BTreeMap;
    let schema = schema_with_member_read(PolicyExpr::SessionCmp {
        path: vec!["claims".into(), "readMembership".into()],
        op: CmpOp::Eq,
        value: PublicValue::Boolean(true),
    });
    let (authority, _, _, note) = populated(schema);
    authority.set_identity_claims(
        bob(),
        BTreeMap::from([(
            jazz::query::provider_claim_key("readMembership"),
            Value::Bool(true),
        )]),
    );
    let peer = |allowed| {
        let (mut peer, server) = duplex();
        authority.accept_subscriber_with_claims(
            server,
            bob(),
            Some(BTreeMap::from([(
                jazz::query::provider_claim_key("readMembership"),
                Value::Bool(allowed),
            )])),
        );
        peer.send(SyncMessage::SubscribeReadablePolicyV1 {
            id: 1,
            query: Query::from("notes"),
            binding: BTreeMap::new(),
            since: None,
            include_deleted: false,
        })
        .unwrap();
        peer
    };
    let mut denied = peer(false);
    let mut allowed = peer(true);
    turns(&[&authority]);
    let frames =
        |peer: &mut Box<dyn Transport>| std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    let refused = frames(&mut denied);
    assert!(refused.iter().any(|message|matches!(message,SyncMessage::Refused {id:1,message,..} if message.contains("dependency is not readable"))));
    assert!(
        !refused
            .iter()
            .any(|message| matches!(message,SyncMessage::Rows {images} if !images.is_empty()))
    );
    let accepted = frames(&mut allowed);
    assert!(accepted.iter().any(|message|matches!(message,SyncMessage::Rows {images} if images.iter().any(|row| row.table=="members"))));
    // An ordinary authority read still follows its note policy, which does not
    // require disclosing the supporting membership to this connection.
    denied
        .send(SyncMessage::Subscribe {
            tier: jazz::db::ReadTier::Remote,
            wait: None,
            id: 2,
            query: Query::from("notes"),
            binding: BTreeMap::new(),
            since: None,
            include_deleted: false,
        })
        .unwrap();
    turns(&[&authority]);
    assert!(frames(&mut denied).iter().any(|message|matches!(message,SyncMessage::Rows {images} if images.iter().any(|row|row.id==*note.as_bytes()))));
}

/// Bob's readable-input subscription has an appended, versioned wire tag.
/// Its request bytes and the unchanged ordinary subscription are pinned so a
/// serializer change cannot silently reinterpret resumed IDs or query options.
#[test]
fn readable_policy_subscription_v1_has_frozen_wire_bytes() {
    use jazz::db::SyncMessage;
    use std::collections::BTreeMap;
    let query = Query::from("notes").limit(2).offset(1);
    let binding = BTreeMap::from([("partition".into(), Value::String("blue".into()))]);
    let request = SyncMessage::SubscribeReadablePolicyV1 {
        id: 300,
        query: query.clone(),
        binding: binding.clone(),
        since: Some(128),
        include_deleted: true,
    };
    let ordinary = SyncMessage::Subscribe {
        tier: jazz::db::ReadTier::Remote,
        wait: None,
        id: 300,
        query,
        binding,
        since: Some(128),
        include_deleted: true,
    };
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/readable-policy.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["role"],
        "jazz.sqlite.sync.readable-policy-subscribe.v1"
    );
    assert_eq!(fixture["version"], 2);
    for (name, message) in [("readable", request), ("ordinary", ordinary)] {
        let bytes = hex::decode(fixture[name].as_str().unwrap()).unwrap();
        assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
        let decoded = jazz::db::decode_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
}

/// Alice revokes Bob while his private owner is disconnected. Reconnecting
/// resubscribes with readable inputs and removes the stale grant and note.
#[test]
fn owner_reconnect_preserves_the_input_request_and_reconciles_revocation() {
    let schema = schema();
    let (authority, _, membership, note) = populated(schema.clone());
    let owner = open(50, bob(), schema.without_permissions());
    owner.set_scope_isolated_storage_owner().unwrap();
    let tab = open(1, bob(), schema.without_permissions());
    tab.set_private_owner_client().unwrap();
    let (up, down) = duplex();
    let upstream = block_on(owner.connect_upstream(up));
    let subscriber = authority.accept_subscriber(down, bob());
    connect(&tab, &owner, bob());
    let dbs = [&tab, &owner, &authority];
    drive(&dbs, tab.wait_for_storage_owner()).unwrap();
    drive(&dbs, tab.cover_catalogue("notes", None)).unwrap();
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let mut stream =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&dbs);
    assert!(
        matches!(stream.try_next_event(),Some(SubscriptionEvent::Delta {added,..}) if added.len()==1 && added[0].row_uuid()==note)
    );
    assert!(owner.detach_connection(&upstream));
    authority.disconnect_subscriber(subscriber);
    block_on(authority.delete("members", membership, Default::default())).unwrap();
    connect(&owner, &authority, bob());
    turns(&dbs);
    assert!(std::iter::from_fn(||stream.try_next_event()).any(|event|matches!(event,SubscriptionEvent::Delta {removed,..} if removed.iter().any(|row|row.row_uuid==note))));
    assert!(local_ids(&owner, "members").is_empty());
    let mut fresh =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&dbs);
    assert!(
        matches!(fresh.try_next_event(),Some(SubscriptionEvent::Delta {added,settled:true,..}) if added.is_empty())
    );
}

/// Bob asks for one of Alice's shared notes. The returned policy closure must
/// contain that scope's membership only, even when other readable scopes exist.
#[test]
fn policy_inputs_follow_the_selected_scope_without_loading_other_memberships() {
    let schema = schema();
    let (authority, _, membership, note) = populated(schema.clone());
    let unrelated = insert(&authority, "scopes", []);
    insert(
        &authority,
        "members",
        [
            ("scope", Value::Uuid(unrelated.0)),
            ("account", Value::Uuid(bob().account_id().unwrap().0)),
        ],
    );
    insert(
        &authority,
        "notes",
        [
            ("scope", Value::Uuid(unrelated.0)),
            ("text", Value::String("another shared note".into())),
        ],
    );
    let (owner, tab) = private_reader(&authority, &schema);
    let query = tab
        .prepare_query(&Query::from("notes").filter(jazz::query::eq(
            jazz::query::col("id"),
            jazz::query::lit(note.0),
        )))
        .unwrap();
    let mut stream =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&[&tab, &owner, &authority]);
    assert!(
        matches!(stream.try_next_event(),Some(SubscriptionEvent::Delta {added,..}) if added.len()==1 && added[0].row_uuid()==note)
    );
    assert_eq!(local_ids(&owner, "notes"), vec![note]);
    assert_eq!(local_ids(&owner, "members"), vec![membership]);
}

/// Even an empty result cannot make an uncorrelated permission scan acceptable.
/// Bob gets a typed refusal before the readable-input subscription is opened.
#[test]
fn empty_result_rejects_unindexed_permission_dependencies() {
    use jazz::db::SyncMessage;
    use std::collections::BTreeMap;
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("members")
                    .column("account", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("notes").policies(TablePolicies::new().with_select(
                    common::exists(
                        "members",
                        vec![PolicyExpr::eq_session(
                            "account",
                            vec!["user".into(), "account".into()],
                        )],
                    ),
                )),
            )
            .build(),
    );
    let authority = open(90, AuthorSubject::SYSTEM, schema);
    authority.set_authority();
    authority.initialize_authority_catalogue().unwrap();
    let (mut peer, server) = duplex();
    authority.accept_subscriber(server, bob());
    peer.send(SyncMessage::SubscribeReadablePolicyV1 {
        id: 1,
        query: Query::from("notes"),
        binding: BTreeMap::new(),
        since: None,
        include_deleted: false,
    })
    .unwrap();
    turns(&[&authority]);
    let frames = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    assert!(frames.iter().any(|frame|matches!(frame,SyncMessage::Refused{id:1,code:jazz::db::ErrorCode::Query,message} if message.contains("not supported"))));
    assert!(
        !frames
            .iter()
            .any(|frame| matches!(frame, SyncMessage::Settled { id: 1, .. }))
    );
}

/// Alice's unusually large membership set exceeds the bounded closure budget.
/// Bob's owner receives a typed refusal and no partial membership projection.
#[test]
fn over_budget_dependency_closure_is_refused_without_partial_rows() {
    let schema = schema();
    let (authority, scope, _, _) = populated(schema.clone());
    for _ in 0..2000 {
        insert(
            &authority,
            "members",
            [
                ("scope", Value::Uuid(scope.0)),
                ("account", Value::Uuid(bob().account_id().unwrap().0)),
            ],
        );
    }
    let (owner, tab) = private_reader(&authority, &schema);
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    let mut stream =
        block_on(tab.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&[&tab, &owner, &authority]);
    assert!(
        matches!(stream.try_next_event(),Some(SubscriptionEvent::Rejected{reason}) if reason.contains("limit") || reason.contains("budget"))
    );
    assert!(local_ids(&owner, "notes").is_empty());
    assert!(local_ids(&owner, "members").is_empty());
}
