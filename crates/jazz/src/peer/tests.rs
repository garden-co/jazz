use super::*;
use crate::legacy_test_future::{
    FutureResolveExt as _, OptionFutureExt as _, ResultFutureExt as _, SettledNodeTestExt as _,
};

use std::collections::BTreeMap;

use crate::ids::{NodeUuid, RowUuid};
use crate::node::MergeableCommit;
use crate::protocol::{ProgramFactEntry, RealRowMemberEntry, SyncMessage, VersionRecord};
use crate::query::{
    Aggregate, ArraySubquery, OrderDirection, Query, col, eq, gt, is_null, lit, ne, not, param,
};
use crate::schema::{JazzSchema, TableSchema};
use crate::time::{GlobalTime, TxTime};
use crate::tools::OpenTransactionId;
use crate::tools::{
    ColumnType as PublicColumnType, PolicyExpr as PublicPolicyExpr,
    SchemaBuilder as PublicSchemaBuilder, TablePolicies as PublicTablePolicies,
    TableSchemaBuilder as PublicTableSchemaBuilder,
};
use crate::tx::DeletionEvent;
use crate::tx::{DurabilityTier, Fate, TxKind};
use groove::records::{BorrowedRecord, RecordDescriptor, Value, ValueType};
use jazz_storage_rocksdb::RocksDbStorage;

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn row_from_u64(value: u64) -> RowUuid {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn settled_member(row_uuid: RowUuid, position: u64) -> ResultMemberEntry {
    ResultMemberEntry::Row(
        crate::protocol::RealRowMemberEntry::current_content((
            String::from("docs").into(),
            row_uuid,
            TxId::new(TxTime(position), node(0x44)),
        ))
        .with_settle_position(Some(GlobalTime(position))),
    )
}

#[test]
fn fast_cursor_membership_mismatch_detects_pre_cursor_changes() {
    let direct = settled_member(row(1), 5);
    let revoked = settled_member(row(2), 6);
    let newly_granted_old_row = settled_member(row(3), 7);
    let newly_granted_at_cursor = settled_member(row(5), 10);
    let new_post_cursor_row = settled_member(row(4), 12);
    let cursor = GlobalTime(10);

    let previous = BTreeSet::from([direct.clone(), revoked.clone()]);
    assert!(fast_cursor_membership_mismatch(
        cursor,
        &previous,
        &BTreeSet::from([direct.clone()]),
    ));
    assert!(fast_cursor_membership_mismatch(
        cursor,
        &previous,
        &BTreeSet::from([direct.clone(), revoked.clone(), newly_granted_old_row]),
    ));
    // A row settled exactly at the fast cursor is not reconstructible from
    // a cursor that already claims that position. Keep the equality case
    // authoritative; changing `<=` to `<` here would silently lose it.
    assert!(fast_cursor_membership_mismatch(
        cursor,
        &previous,
        &BTreeSet::from([direct.clone(), revoked.clone(), newly_granted_at_cursor]),
    ));
    assert!(!fast_cursor_membership_mismatch(
        cursor,
        &previous,
        &BTreeSet::from([direct, revoked, new_post_cursor_row]),
    ));
}

#[test]
fn fast_cursor_membership_bounds_authoritative_resets() {
    // This is intentionally an internal test: the four-way decision is a
    // peer-only protocol control-plane predicate, with no public API
    // surface. End-to-end rehydrate tests cover application of its output.
    let old = settled_member(row(1), 7);
    let new = settled_member(row(2), 12);
    let cursor = GlobalTime(10);
    let previous = BTreeSet::from([old.clone()]);

    // (1) same authorization, sufficient cursor: no reset.
    assert!(!fast_cursor_requires_authoritative_reset(
        cursor, &previous, &previous,
    ));
    // (2) same authorization, post-cursor add: incremental repair remains
    // sufficient and this predicate must not force a reset.
    assert!(!fast_cursor_requires_authoritative_reset(
        cursor,
        &previous,
        &BTreeSet::from([old.clone(), new.clone()]),
    ));
    // Membership-affecting policy facts need the #1266 authoritative reset
    // even when the link-local authorization generation did not advance.
    assert!(fast_cursor_requires_authoritative_reset(
        cursor,
        &previous,
        &BTreeSet::from([old.clone(), settled_member(row(3), 8)]),
    ));
    assert!(fast_cursor_requires_authoritative_reset(
        cursor,
        &previous,
        &BTreeSet::new(),
    ));
    // (3) changed authorization with a reconstructible post-cursor add.
    assert!(!fast_cursor_requires_authoritative_reset(
        cursor,
        &previous,
        &BTreeSet::from([old.clone(), new]),
    ));
    // (4) changed authorization with either a pre-cursor grant or revoke.
    assert!(fast_cursor_requires_authoritative_reset(
        cursor,
        &previous,
        &BTreeSet::from([old.clone(), settled_member(row(3), 8)]),
    ));
    assert!(fast_cursor_requires_authoritative_reset(
        cursor,
        &previous,
        &BTreeSet::new(),
    ));
}

#[test]
fn client_fast_cursor_requires_retained_matching_authorization_progress() {
    let subscription = SubscriptionKey {
        shape_id: crate::query::ShapeId(uuid::Uuid::from_u128(1)),
        binding_id: crate::query::BindingId(uuid::Uuid::from_u128(2)),
        read_view: Default::default(),
    };
    let cursor = GlobalTime(10);
    let known_state = Some(KnownStateDeclaration::FastWithAuthorizationProgress {
        completeness: KnownStateCompleteness::FastCurrentMembership,
        position: cursor,
        authorization_progress: 0,
    });
    let previous = BTreeSet::from([settled_member(row(1), 7)]);
    let revoked = BTreeSet::new();

    let mut fresh_client = PeerState::client_link(AuthorSubject::for_test_bytes([0x11; 16]));
    fresh_client.declare_known_state(subscription, known_state.clone());
    assert!(!fresh_client.fast_cursor_authorization_matches(subscription, &known_state));
    assert!(fast_cursor_requires_authoritative_reset(
        cursor, &previous, &revoked,
    ));

    let state = fresh_client
        .publication_states
        .entry(subscription)
        .or_default();
    state.authorization_progress = 0;
    state.has_served_authorization_progress = true;
    assert!(fresh_client.fast_cursor_authorization_matches(subscription, &known_state));

    let legacy = Some(KnownStateDeclaration::Fast {
        completeness: KnownStateCompleteness::FastCurrentMembership,
        position: cursor,
    });
    assert!(!fresh_client.fast_cursor_authorization_matches(subscription, &legacy));
    assert!(!fresh_client.fast_cursor_authorization_matches(subscription, &None));

    let relay = PeerState::relay();
    assert!(relay.fast_cursor_authorization_matches(subscription, &legacy));
}

#[test]
fn client_fast_cursor_authorization_proof_controls_rehydrate_reset() {
    let (_dir, mut core) = open_node_with_uuid(node(0x91));
    let live = row(0x31);
    let live_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", live, 1_000).cells(title_cells("live")))
        .unwrap();
    accept_global(&mut core, live_tx, 1);
    let shape = Query::from("todos").validate(&schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let known = |position, authorization_progress| {
        Some(KnownStateDeclaration::FastWithAuthorizationProgress {
            completeness: KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime(position),
            authorization_progress,
        })
    };

    let identity = AuthorSubject::for_test_bytes([0x11; 16]);
    let mut fresh = PeerState::client_link(identity);
    fresh.declare_known_state(subscription, known(1, 0));
    let fresh_update = fresh.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        ..
    }) = fresh_update
    else {
        panic!("expected view update");
    };
    assert!(
        reset_result_set,
        "fresh client token must not suppress reset"
    );
    assert_eq!(result_member_adds.len(), 1);

    let retained_member = fresh.publication_states[&subscription]
        .result_member_set
        .iter()
        .next()
        .unwrap()
        .clone();
    apply_contribution_add(
        fresh.publication_states.get_mut(&subscription).unwrap(),
        std::iter::once(&retained_member),
        &mut Vec::new(),
        &mut Vec::new(),
    );
    assert_eq!(
        fresh.publication_states[&subscription]
            .member_index
            .values()
            .next()
            .unwrap()
            .refcount,
        2
    );

    fresh.declare_known_state(subscription, known(1, 0));
    let retained_update = fresh.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        ..
    }) = retained_update
    else {
        panic!("expected view update");
    };
    assert!(!reset_result_set, "retained matching token may resume");
    assert!(result_member_adds.is_empty());
    assert_eq!(
        fresh.publication_states[&subscription]
            .member_index
            .values()
            .next()
            .unwrap()
            .refcount,
        2,
        "retained resume must preserve contribution refcounts"
    );

    let deleted_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", live, 2_000).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, deleted_tx, 2);
    fresh.declare_known_state(subscription, known(2, 1));
    let revoke_update = fresh.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set, ..
    }) = revoke_update
    else {
        panic!("expected view update");
    };
    assert!(
        reset_result_set,
        "mismatched authorization token must reset a retained revoke"
    );
}

#[test]
fn duplicate_structured_query_authorization_mismatch_forces_reset() {
    let (_dir, mut core) = open_node_with_uuid(node(0x92));
    for (index, title) in ["one", "two"].into_iter().enumerate() {
        let tx = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(0x40 + index as u8), 1_000 + index as u64)
                    .cells(title_cells(title)),
            )
            .unwrap();
        accept_global(&mut core, tx, index as u64 + 1);
    }
    let shape = Query::from("todos")
        .aggregate([Aggregate::count()])
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let canonical = subscription_key(&shape, &binding);
    let target = SubscriptionKey {
        binding_id: crate::query::BindingId(uuid::Uuid::from_u128(0x47)),
        ..canonical
    };
    let mut peer = PeerState::client_link(AuthorSubject::for_test_bytes([0x11; 16]));
    peer.rehydrate_query_for_subscription_with_opts(
        &mut core,
        canonical,
        &shape,
        &binding,
        RegisterShapeOptions::default(),
    )
    .unwrap();
    peer.advance_authorization_progress();
    peer.declare_known_state(
        target,
        Some(KnownStateDeclaration::FastWithAuthorizationProgress {
            completeness: KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime(2),
            authorization_progress: 0,
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_from_maintained_subscription(
            &mut core, canonical, target, &shape,
        )
        .unwrap()
        .expect("expected view update");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set, ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert!(
        reset_result_set,
        "structured duplicate usage must not resume across authorization generations"
    );
}

fn output_member(root: RowUuid, joined: RowUuid, time: u64) -> ResultMemberEntry {
    RealRowMemberEntry::current_content((
        "todos".to_owned().into(),
        root,
        TxId::new(crate::time::TxTime(time), node(0xee)),
    ))
    .with_occurrence_id(crate::tools::OutputOccurrenceId::new(
        crate::tools::ObjectId::from_uuid(root.0),
        [crate::tools::ObjectId::from_uuid(joined.0)],
    ))
    .into()
}

fn indexed_members(members: &[ResultMemberEntry]) -> PeerSubscriptionState {
    let mut state = PeerSubscriptionState::default();
    apply_contribution_add(&mut state, members.iter(), &mut Vec::new(), &mut Vec::new());
    state
}

#[test]
fn incremental_delivery_removes_a_superseded_output_occurrence() {
    let root = row(0x31);
    let previous = output_member(root, row(0x32), 10);
    let replacement = output_member(root, row(0x32), 11);

    assert_eq!(
        replacement_removals(&indexed_members(std::slice::from_ref(&previous)), &[replacement]),
        vec![previous],
        "a newer current-row version must replace the version already sent for the same output occurrence"
    );
}

#[test]
fn incremental_delivery_finds_replacements_by_physical_member_key() {
    let root = row(0x34);
    let previous = output_member(root, row(0x35), 10);
    let replacement = output_member(root, row(0x35), 11);
    let mut members = Vec::with_capacity(4_097);
    members.push(previous.clone());
    for value in 0..4_096u64 {
        members.push(output_member(
            row_from_u64(value + 0x1_000),
            row_from_u64(value + 0x2_000),
            10,
        ));
    }

    assert_eq!(
        replacement_removals(&indexed_members(&members), &[replacement]),
        vec![previous],
        "unrelated live members must not participate in replacement lookup"
    );
}

#[test]
fn incremental_delivery_keeps_terminal_children_with_their_root() {
    let root = row(0x41);
    let occurrence =
        crate::tools::OutputOccurrenceId::new(crate::tools::ObjectId::from_uuid(root.0), []);
    let root_member: ResultMemberEntry = RealRowMemberEntry::current_content((
        "users".to_owned().into(),
        root,
        TxId::new(crate::time::TxTime(10), node(0xee)),
    ))
    .with_occurrence_id(occurrence.clone())
    .into();
    let child_member: ResultMemberEntry = RealRowMemberEntry::current_content((
        "todos".to_owned().into(),
        row(0x42),
        TxId::new(crate::time::TxTime(11), node(0xee)),
    ))
    .with_occurrence_id(occurrence)
    .into();

    assert!(
        replacement_removals(
            &indexed_members(std::slice::from_ref(&root_member)),
            std::slice::from_ref(&child_member)
        )
        .is_empty()
    );

    let subscription = SubscriptionKey {
        shape_id: crate::query::ShapeId(uuid::Uuid::from_u128(41)),
        binding_id: crate::query::BindingId(uuid::Uuid::from_u128(42)),
        read_view: Default::default(),
    };
    let update = |adds, removes| SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(0),
        reset_result_set: false,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: Default::default(),
        result_member_adds: adds,
        result_member_removes: removes,
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    let mut peer = PeerState::default();
    peer.apply_outgoing_view_update_result_set(&update(
        vec![root_member.clone(), child_member.clone()],
        Vec::new(),
    ));
    peer.apply_outgoing_view_update_result_set(&update(Vec::new(), vec![child_member]));

    let state = &peer.publication_states[&subscription];
    assert_eq!(state.result_member_set, BTreeSet::from([root_member]));
    assert_eq!(state.member_index.len(), 1);
}

#[test]
fn incremental_delivery_replaces_equal_tx_with_a_new_rendered_revision() {
    let root = row(0x51);
    let previous: ResultMemberEntry = RealRowMemberEntry::current_content((
        "todos".to_owned().into(),
        root,
        TxId::new(crate::time::TxTime(10), node(0xee)),
    ))
    .with_row_digest(vec![1])
    .into();
    let replacement: ResultMemberEntry = RealRowMemberEntry::current_content((
        "todos".to_owned().into(),
        root,
        TxId::new(crate::time::TxTime(10), node(0xee)),
    ))
    .with_row_digest(vec![2])
    .into();

    assert_eq!(
        replacement_removals(&indexed_members(std::slice::from_ref(&previous)), &[replacement]),
        vec![previous],
    );
}

#[test]
fn maintained_delivery_does_not_leak_intermediate_replacement_refcounts() {
    let root = row(0x61);
    let tx10 = output_member(root, row(0x62), 10);
    let tx11 = output_member(root, row(0x62), 11);
    let tx12 = output_member(root, row(0x62), 12);
    let subscription = SubscriptionKey {
        shape_id: crate::query::ShapeId(uuid::Uuid::from_u128(11)),
        binding_id: crate::query::BindingId(uuid::Uuid::from_u128(12)),
        read_view: Default::default(),
    };
    let update = |adds, removes| SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(0),
        reset_result_set: false,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: Default::default(),
        result_member_adds: adds,
        result_member_removes: removes,
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    let mut peer = PeerState::default();
    peer.apply_outgoing_view_update_result_set(&update(vec![tx10.clone()], Vec::new()));
    peer.apply_outgoing_view_update_result_set(&update(
        vec![tx11.clone(), tx12.clone()],
        vec![tx10, tx11],
    ));
    peer.apply_outgoing_view_update_result_set(&update(Vec::new(), vec![tx12]));

    let state = &peer.publication_states[&subscription];
    assert!(state.result_member_set.is_empty());
    assert!(state.member_index.is_empty());
}

// This exercises the maintained protocol boundary directly: public joined
// subscriptions remain deliberately rejected until the next PR, so no
// black-box public query can produce two output occurrences yet.
#[test]
fn maintained_delivery_rekeys_delta_and_reset_by_output_occurrence() {
    let root = row(0x41);
    let first = output_member(root, row(0x42), 10);
    let second = output_member(root, row(0x43), 11);
    let replacement = output_member(root, row(0x42), 12);
    let subscription = SubscriptionKey {
        shape_id: crate::query::ShapeId(uuid::Uuid::from_u128(1)),
        binding_id: crate::query::BindingId(uuid::Uuid::from_u128(2)),
        read_view: Default::default(),
    };
    let update = |reset_result_set, adds, removes| SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(0),
        reset_result_set,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: Default::default(),
        result_member_adds: adds,
        result_member_removes: removes,
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    let mut peer = PeerState::default();

    peer.apply_outgoing_view_update_result_set(&update(
        true,
        vec![first.clone(), second.clone()],
        Vec::new(),
    ));
    peer.apply_outgoing_view_update_result_set(&update(
        false,
        vec![replacement.clone()],
        vec![first],
    ));
    peer.apply_outgoing_view_update_result_set(&update(
        true,
        vec![replacement.clone(), second.clone()],
        Vec::new(),
    ));

    assert_eq!(
        peer.publication_states[&subscription].result_member_set,
        BTreeSet::from([replacement, second])
    );
    assert_eq!(peer.publication_states[&subscription].member_index.len(), 2);
}

fn current_row_pair(row: crate::node::CurrentRow) -> (RowUuid, BTreeMap<String, Value>) {
    (row.row_uuid(), row.test_cells_by_descriptor())
}

fn wire_version_cells(record: &VersionRecord, table: &TableSchema) -> BTreeMap<String, Value> {
    table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            record
                .cell_at(idx)
                .map(|value| (column.name.clone(), value))
        })
        .collect()
}

fn title_cells(title: impl Into<String>) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(title.into()))])
}

fn maybe_title_cells(title: Option<&str>) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "anchor".to_owned(),
            Value::String(format!("anchor-{}", title.unwrap_or("null"))),
        ),
        (
            "maybe_title".to_owned(),
            Value::Nullable(title.map(|title| Box::new(Value::String(title.to_owned())))),
        ),
    ])
}

fn priority_cells(title: impl Into<String>, priority: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("priority".to_owned(), Value::U64(priority)),
    ])
}

fn public_session_eq(column: &str, path: &[&str]) -> PublicPolicyExpr {
    let path = path.iter().map(|segment| (*segment).to_owned()).collect();
    PublicPolicyExpr::eq_session(
        column,
        path,
    )
}

fn public_exists(table: &str, conditions: Vec<PublicPolicyExpr>) -> PublicPolicyExpr {
    PublicPolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PublicPolicyExpr::and(conditions)),
    }
}

fn access_policy_schema() -> JazzSchema {
    let read = public_exists(
        "docAccess",
        vec![
            public_session_eq("doc", &["__jazz_outer_row", "id"]),
            public_session_eq("userID", &["claims", "sub"]),
        ],
    );
    public_peer_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .column("project", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_select(read)),
            )
            .table(
                PublicTableSchemaBuilder::new("docAccess")
                    .fk_column("doc", "docs")
                    .column("userID", PublicColumnType::Uuid),
            ),
    )
}

fn doc_cells(title: impl Into<String>, project: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("project".to_owned(), Value::Uuid(project.0)),
    ])
}

fn access_cells(doc: RowUuid, user: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("doc".to_owned(), Value::Uuid(doc.0)),
        ("userID".to_owned(), Value::Uuid(user.test_uuid())),
    ])
}

fn aggregate_access_policy_schema() -> JazzSchema {
    let read = public_exists(
        "docAccess",
        vec![
            public_session_eq("doc", &["__jazz_outer_row", "id"]),
            public_session_eq("userID", &["claims", "sub"]),
        ],
    );
    public_peer_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .column("score", PublicColumnType::Timestamp)
                    .policies(PublicTablePolicies::new().with_select(read)),
            )
            .table(
                PublicTableSchemaBuilder::new("docAccess")
                    .fk_column("doc", "docs")
                    .column("userID", PublicColumnType::Uuid),
            ),
    )
}

fn session_claim_read_policy_schema() -> JazzSchema {
    let policy = public_session_eq("owner", &["claims", "session_id"]);
    public_peer_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("resources")
            .column("owner", PublicColumnType::Uuid)
            .policies(
                PublicTablePolicies::new()
                    .with_select(policy.clone())
                    .with_insert(policy),
            ),
    ))
}

fn session_seed_write_policy_schema() -> JazzSchema {
    let policy = crate::test_public_schema::seeded_recursive_access_policy(
        "resourceAccess",
        "resource",
        "team",
        &[],
        &[],
        "teams",
        "teamMemberships",
        "member",
        "parent",
        &[],
        "teamSeeds",
        "user",
        &["claims", "session_id"],
        "team",
    );
    public_peer_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("owner", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_insert(policy)),
            )
            .table(PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("teamSeeds")
                    .fk_column("team", "teams")
                    .column("user", PublicColumnType::Uuid),
            )
            .table(
                PublicTableSchemaBuilder::new("resourceAccess")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("teamMemberships")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams"),
            ),
    )
}

fn resource_commit_unit(
    writer: &mut NodeState<RocksDbStorage>,
    author: AuthorSubject,
    row_uuid: RowUuid,
) -> (Transaction, Vec<VersionRecord>) {
    let (_, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("resources", row_uuid, 10)
                .made_by(author)
                .cells(BTreeMap::from([(
                    "owner".to_owned(),
                    Value::Uuid(author.test_uuid()),
                )])),
        )
        .expect("writer creates policy-protected resource commit");
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("mergeable unit must carry a CommitUnit");
    };
    (tx, versions)
}

#[test]
fn edge_support_hydration_uses_writer_claims_and_fails_closed_when_missing() {
    let schema = session_claim_read_policy_schema();
    let writer = AuthorSubject::for_test_bytes([0xa1; 16]);
    let transport_identity = AuthorSubject::for_test_bytes([0xa2; 16]);
    let resource = row(0xa3);
    let (_writer_dir, mut writer_node) = open_node_with_schema(node(0xa4), schema.clone());
    let (tx, versions) = resource_commit_unit(&mut writer_node, writer, resource);

    // The public edge/deferred entrypoint must turn a missing custom
    // session claim into a settled empty support view, not a binding
    // error. The edge parks the first hydration turn by design.
    let (_missing_dir, mut missing_edge) = open_node_with_schema(node(0xa5), schema.clone());
    let mut missing_peer = PeerState::edge_client(writer);
    let missing_outcome = missing_peer
        .ingest_edge_mergeable_commit_unit(&mut missing_edge, tx.clone(), versions.clone(), 10)
        .expect("missing policy claim must fail closed, not abort edge ingest");
    let missing_updates = missing_edge
        .persist_and_settle_outcome(missing_outcome)
        .unwrap();
    assert!(missing_updates.is_empty(), "{missing_updates:?}");
    assert_eq!(missing_peer.deferred_edge_fate_count(), 1);

    // A backend/transport identity is not the commit permission subject.
    // The support rehydrate must temporarily evaluate the writer's bound
    // session claim even while the peer normally serves as another user.
    let (_bound_dir, mut bound_edge) = open_node_with_schema(node(0xa6), schema.clone());
    bound_edge.set_test_provider_claims(
        writer,
        BTreeMap::from([("session_id".to_owned(), Value::Uuid(writer.test_uuid()))]),
    );
    let prior = bound_edge
        .commit_mergeable_settled(
            MergeableCommit::new("resources", row(0xa8), 1)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([(
                    "owner".to_owned(),
                    Value::Uuid(writer.test_uuid()),
                )])),
        )
        .expect("seed readable resource at the edge");
    accept_global(&mut bound_edge, prior, 1);
    let mut system_serving_peer =
        PeerState::edge_client_with_permission_identity(transport_identity, AuthorSubject::SYSTEM);
    let bound_subscriptions = system_serving_peer
        .unsettled_authority_scope_subscriptions(&mut bound_edge, writer, &versions, None, true)
        .expect("edge support must bind the writer rather than the transport identity");
    let bound_subscription = bound_subscriptions
        .and_then(|subscriptions| subscriptions.into_iter().next())
        .expect("write support must register one policy subscription");
    assert!(
        !system_serving_peer
            .publication_states
            .get(&bound_subscription)
            .expect("bound support subscription state")
            .result_member_set
            .is_empty(),
        "the writer's bound claim must authorize the seeded resource"
    );
    assert_eq!(system_serving_peer.link_identity(), transport_identity);
    assert_eq!(system_serving_peer.identity(), AuthorSubject::SYSTEM);

    // A present but ill-typed claim remains a real binding error; only an
    // absent claim receives the fail-closed empty-proof treatment.
    let (_wrong_type_dir, mut wrong_type_edge) = open_node_with_schema(node(0xa7), schema);
    wrong_type_edge.set_test_provider_claims(
        writer,
        BTreeMap::from([(
            "session_id".to_owned(),
            Value::String("not-a-uuid".to_owned()),
        )]),
    );
    let mut wrong_type_peer =
        PeerState::edge_client_with_permission_identity(transport_identity, transport_identity);
    wrong_type_peer
        .unsettled_authority_scope_subscriptions(
            &mut wrong_type_edge,
            writer,
            &versions,
            None,
            true,
        )
        .expect_err("present ill-typed claim must remain an error");
    assert_eq!(wrong_type_peer.link_identity(), transport_identity);
    assert_eq!(wrong_type_peer.identity(), transport_identity);
}

#[test]
fn edge_ingest_turns_missing_prepared_seed_claim_into_deferred_empty_support() {
    let schema = session_seed_write_policy_schema();
    let writer = AuthorSubject::for_test_bytes([0xb1; 16]);
    let resource = row(0xb2);
    let (_writer_dir, mut writer_node) = open_node_with_schema(node(0xb3), schema.clone());
    let (tx, versions) = resource_commit_unit(&mut writer_node, writer, resource);
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xb4), schema);
    let mut peer = PeerState::edge_client(writer);

    let outcome = peer
        .ingest_edge_mergeable_commit_unit(&mut edge, tx.clone(), versions, 10)
        .expect("missing prepared seed claim must be a deferred empty support proof");
    let updates = edge.persist_and_settle_outcome(outcome).unwrap();
    assert!(updates.is_empty());
    assert_eq!(peer.deferred_edge_fate_count(), 1);
    assert!(
        edge.transaction_state(tx.tx_id).is_none(),
        "a pending support proof must not admit a client commit into edge history"
    );
}

#[test]
fn deferred_edge_ingest_rejects_a_conflicting_retransmit() {
    let schema = session_seed_write_policy_schema();
    let writer = AuthorSubject::for_test_bytes([0xc1; 16]);
    let resource = row(0xc2);
    let (_writer_dir, mut writer_node) = open_node_with_schema(node(0xc3), schema.clone());
    let (tx, versions) = resource_commit_unit(&mut writer_node, writer, resource);
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xc4), schema);
    let mut peer = PeerState::edge_client(writer);

    let _ = peer
        .ingest_edge_mergeable_commit_unit(&mut edge, tx.clone(), versions.clone(), 10)
        .expect("missing support claim parks the first commit unit");
    assert_eq!(peer.deferred_edge_fate_count(), 1);

    let _ = peer
        .ingest_edge_mergeable_commit_unit(&mut edge, tx.clone(), versions.clone(), 10)
        .expect("an identical deferred retransmit remains idempotent");
    assert_eq!(peer.deferred_edge_fate_count(), 1);

    let mut conflicting = tx.clone();
    conflicting.n_total_writes = conflicting.n_total_writes.saturating_add(1);
    assert!(matches!(
        peer.ingest_edge_mergeable_commit_unit(&mut edge, conflicting, versions, 10)
            .resolve(),
        Err(Error::ConflictingCommitUnit(tx_id)) if tx_id == tx.tx_id
    ));
    assert_eq!(peer.deferred_edge_fate_count(), 1);
    assert!(
        edge.transaction_state(tx.tx_id).is_none(),
        "a conflicting retry must not enter history while the original remains deferred"
    );
}

fn scored_doc_cells(title: impl Into<String>, score: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("score".to_owned(), Value::U64(score)),
    ])
}

fn schema() -> JazzSchema {
    public_peer_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text),
        ),
    )
}

fn nullable_title_schema() -> JazzSchema {
    public_peer_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("anchor", PublicColumnType::Text)
            .nullable_column("maybe_title", PublicColumnType::Text),
    ))
}

fn priority_schema() -> JazzSchema {
    public_peer_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("priority", PublicColumnType::Timestamp),
    ))
}

fn public_peer_schema(builder: PublicSchemaBuilder) -> JazzSchema {
    crate::schema::JazzSchema::new(&builder.build())
        .expect("peer-test public schema compiles")
}

fn open_node_with_schema(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (temp_dir, node)
}

fn open_node_with_uuid(node_uuid: NodeUuid) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let schema = schema();
    open_node_with_schema(node_uuid, schema)
}

fn accept_global(core: &mut NodeState<RocksDbStorage>, tx_id: TxId, seq: u64) {
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(seq)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn accept_edge(core: &mut NodeState<RocksDbStorage>, tx_id: TxId) {
    core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
}

fn title_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("title")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String(title.to_owned()),
        )]))
        .unwrap();
    (shape, binding)
}

fn title_param_eq_column_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(eq(param("title"), col("title")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String(title.to_owned()),
        )]))
        .unwrap();
    (shape, binding)
}

fn title_contains_shape_binding(needle: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(crate::query::contains(
            col("title"),
            crate::query::lit(needle),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn title_contains_param_shape_binding(needle: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(crate::query::contains(col("title"), param("needle")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "needle".to_owned(),
            Value::String(needle.to_owned()),
        )]))
        .unwrap();
    (shape, binding)
}

fn title_not_param_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(ne(col("title"), param("title")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String(title.to_owned()),
        )]))
        .unwrap();
    (shape, binding)
}

fn title_after_literal_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(gt(col("title"), lit(title)))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn title_before_reversed_literal_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(gt(lit(title), col("title")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn title_any_literal_shape_binding(left: &str, right: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(crate::query::any_of([
            eq(col("title"), lit(left)),
            eq(col("title"), lit(right)),
        ]))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn title_in_literal_shape_binding(
    values: impl IntoIterator<Item = &'static str>,
) -> (ValidatedQuery, Binding) {
    let shape = Query::from("todos")
        .filter(crate::query::in_list(
            col("title"),
            values.into_iter().map(lit),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn nullable_title_shape_binding(non_null: bool) -> (ValidatedQuery, Binding) {
    let predicate = is_null(col("maybe_title"));
    let predicate = if non_null { not(predicate) } else { predicate };
    let shape = Query::from("todos")
        .filter(predicate)
        .validate(&nullable_title_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn subscription_key(shape: &ValidatedQuery, binding: &Binding) -> SubscriptionKey {
    subscription_key_with_opts(shape, binding, &RegisterShapeOptions::default())
}

fn subscription_key_with_opts(
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: &RegisterShapeOptions,
) -> SubscriptionKey {
    SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    }
}

fn register_shape_binding_for_receiver(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    register_shape_binding_for_receiver_with_opts(
        node,
        shape,
        binding,
        RegisterShapeOptions::default(),
    );
}

fn register_shape_binding_for_receiver_with_opts(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
) {
    node.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(shape),
        opts: opts.clone(),
    })
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: subscription_key_with_opts(shape, binding, &opts),
        values,
        known_state: None,
    }))
    .unwrap();
}

fn version_bundles_for_update(update: &SyncMessage) -> Vec<VersionBundle> {
    match update {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            version_carriers,
            version_bundles,
            ..
        }) => {
            let mut bundles = version_bundles.clone();
            bundles.extend(
                crate::protocol::expand_version_carriers(version_carriers)
                    .expect("test update carriers should expand"),
            );
            bundles
        }
        _ => Vec::new(),
    }
}

#[test]
fn non_global_peer_query_subscriptions_use_maintained_path() {
    let (_dir, mut core) = open_node_with_uuid(node(0x44));
    let (shape, binding) = title_shape_binding("match");
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    let mut peer = PeerState::new();

    peer.rehydrate_query_with_opts(&mut core, &shape, &binding, opts)
        .unwrap();
    assert!(
        peer.publication_states
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some()
    );
    peer.query_update_for_subscription(&mut core, subscription, &shape, &binding)
        .unwrap();
}

fn row_result_set(
    peer: &PeerState,
    subscription: SubscriptionKey,
) -> Option<BTreeSet<ResultRowEntry>> {
    peer.publication_states.get(&subscription).map(|state| {
        state
            .result_member_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .collect()
    })
}

fn maintained_subscription_id(
    peer: &PeerState,
    subscription: SubscriptionKey,
) -> Option<groove::ivm::SubscriptionId> {
    peer.publication_states
        .get(&subscription)
        .and_then(|state| state.maintained_subscription_view.as_ref())
        .map(|maintained| maintained.subscription.id())
}

fn aggregate_payload_count(fact: &ProgramFactEntry) -> Value {
    let ProgramFactEntry::ResultPayload(payload) = fact else {
        panic!("expected result payload fact");
    };
    let fields: Vec<(Option<String>, ValueType)> =
        postcard::from_bytes(&payload.descriptor).unwrap();
    let descriptor = RecordDescriptor::new(
        fields
            .into_iter()
            .map(|(name, value_type)| (name.unwrap(), value_type)),
    );
    let record = BorrowedRecord::new(&payload.record, &descriptor);
    // Aggregate aliases are logical app names, but maintained-program
    // payload descriptors use the dedicated physical aggregate namespace
    // so they cannot collide with grouped source columns. Decode the
    // protocol boundary instead of treating `count` as a record field.
    let count_field = crate::node::query_engine::aggregate_output_field("count");
    record.get(&count_field).unwrap().clone()
}

fn aggregate_cells(row: &crate::node::CurrentRow) -> BTreeMap<String, Value> {
    row.test_cells_by_descriptor()
}

fn view_update_added_rows(update: SyncMessage) -> BTreeSet<RowUuid> {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert!(!reset_result_set);
    assert!(result_member_removes.is_empty());
    result_member_adds
        .into_iter()
        .filter_map(ResultMemberEntry::into_row)
        .map(|(_, row_uuid, _)| row_uuid)
        .collect()
}

fn assert_view_update_rows(
    update: SyncMessage,
    expected_adds: Vec<(&str, RowUuid, TxId)>,
    expected_removes: Vec<(&str, RowUuid, TxId)>,
) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    let mut result_member_adds = result_member_adds;
    let mut result_member_removes = result_member_removes;
    result_member_adds.sort();
    result_member_removes.sort();
    let mut expected_adds = expected_adds
        .into_iter()
        .map(|(table, row, tx)| (table.to_owned().into(), row, tx))
        .collect::<Vec<_>>();
    let mut expected_removes = expected_removes
        .into_iter()
        .map(|(table, row, tx)| (table.to_owned().into(), row, tx))
        .collect::<Vec<_>>();
    expected_adds.sort();
    expected_removes.sort();
    assert_eq!(result_member_adds, expected_adds);
    assert_eq!(result_member_removes, expected_removes);
}

fn assert_view_update_row_order(
    update: SyncMessage,
    expected_adds: Vec<(&str, RowUuid, TxId)>,
    expected_removes: Vec<(&str, RowUuid, TxId)>,
) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        expected_adds
            .into_iter()
            .map(|(table, row, tx)| (table.to_owned().into(), row, tx))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result_member_removes,
        expected_removes
            .into_iter()
            .map(|(table, row, tx)| (table.to_owned().into(), row, tx))
            .collect::<Vec<_>>()
    );
}

#[test]
fn maintained_subscription_view_default_rehydrate_installs_subscription() {
    let (_dir, mut core) = open_node_with_uuid(node(0x90));
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x10), 1_000).cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let (shape, binding) = title_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert!(maintained_subscription_id(&peer, subscription).is_some());
}

#[test]
fn maintained_structured_terminal_only_change_is_not_dropped_by_empty_guard() {
    let schema = public_peer_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("owner_id", PublicColumnType::Uuid),
            ),
    );
    let (_dir, mut core) = open_node_with_schema(node(0x93), schema.clone());
    let user = row(0xa1);
    let user_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("users", user, 1_000).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String("owner".to_owned()),
            )])),
        )
        .unwrap();
    accept_global(&mut core, user_tx, 1);
    let shape = Query::from("users")
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();
    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let child_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xb1), 1_001).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("child".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(user.0)),
            ])),
        )
        .unwrap();
    accept_global(&mut core, child_tx, 2);
    peer.query_update(&mut core, &shape, &binding).unwrap();
    let child_update_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xb1), 1_002).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String("updated child".to_owned()),
            )])),
        )
        .unwrap();
    accept_global(&mut core, child_update_tx, 3);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        terminal_operations,
        ..
    }) = update
    else {
        panic!("expected view update")
    };
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert!(!terminal_operations.is_empty());
    assert!(!maintained_view_update_is_empty(
        &[],
        &[],
        &terminal_operations,
        &[],
        &[],
    ));
    let _ = (program_fact_adds, program_fact_removes);
}

#[test]
fn maintained_rehydrate_run_emission_matches_forced_singleton_receiver_results() {
    struct ForceSingletonGuard;
    impl Drop for ForceSingletonGuard {
        fn drop(&mut self) {
            crate::protocol::set_force_singleton_version_carriers_for_tests(false);
        }
    }

    let (_core_dir, mut core) = open_node_with_uuid(node(0x91));
    for idx in 0..4 {
        let tx_id = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row_from_u64(idx), 1_000 + idx)
                    .cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, tx_id, idx + 1);
    }
    let ignored = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(100), 2_000).cells(title_cells("other")),
        )
        .unwrap();
    accept_global(&mut core, ignored, 10);
    let (shape, binding) = title_shape_binding("match");
    let mut singleton_peer = PeerState::new();
    let mut run_peer = PeerState::new();

    crate::protocol::set_force_singleton_version_carriers_for_tests(true);
    let _guard = ForceSingletonGuard;
    let singleton_update = singleton_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    crate::protocol::set_force_singleton_version_carriers_for_tests(false);
    let run_update = run_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();

    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        version_carriers, ..
    }) = &run_update
    else {
        panic!("expected view update");
    };
    assert!(
        version_carriers
            .iter()
            .any(|carrier| matches!(carrier, VersionCarrier::Run(run) if run.bodies.len() > 1)),
        "multi-carrier maintained rehydrate should emit a run"
    );

    let (_singleton_dir, mut singleton_reader) = open_node_with_uuid(node(0x92));
    let (_run_dir, mut run_reader) = open_node_with_uuid(node(0x93));
    register_shape_binding_for_receiver(&mut singleton_reader, &shape, &binding);
    register_shape_binding_for_receiver(&mut run_reader, &shape, &binding);
    singleton_reader
        .apply_sync_message_settled(singleton_update)
        .unwrap();
    run_reader.apply_sync_message_settled(run_update).unwrap();

    let singleton_rows = singleton_reader
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let run_rows = run_reader
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(run_rows, singleton_rows);
    assert_eq!(run_rows.len(), 4);
    for idx in 0..4 {
        let row = row_from_u64(idx);
        assert_eq!(
            run_reader.row_history("todos", row).unwrap(),
            singleton_reader.row_history("todos", row).unwrap(),
            "run receiver apply should store the same row history as singleton apply"
        );
        let run_tx = run_reader
            .local_content_winner_tx_id("todos", row)
            .unwrap()
            .expect("run reader should have content winner");
        let singleton_tx = singleton_reader
            .local_content_winner_tx_id("todos", row)
            .unwrap()
            .expect("singleton reader should have content winner");
        assert_eq!(run_tx, singleton_tx);
        assert_eq!(
            run_reader.transaction_state_settled(run_tx),
            singleton_reader.transaction_state_settled(singleton_tx),
            "run receiver apply should preserve transaction state"
        );
    }
}

#[test]
fn maintained_subscription_view_limit_one_installs_subscription() {
    let (_dir, mut core) = open_node_with_uuid(node(0x90));
    let higher_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_000).cells(title_cells("higher")),
        )
        .unwrap();
    let lower_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_001).cells(title_cells("lower")),
        )
        .unwrap();
    accept_global(&mut core, higher_tx, 1);
    accept_global(&mut core, lower_tx, 2);
    let shape = Query::from("todos").limit(1).validate(&schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_eq!(
        peer.maintained_subscription_view_metrics()
            .unsupported_skips_out,
        0
    );
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row_from_u64(10), lower_tx)]
    );
    assert!(result_member_removes.is_empty());
}

#[test]
fn maintained_subscription_view_cold_rehydrate_after_restore_ships_restored_content() {
    let (_core_dir, mut core) = open_node_with_uuid(node(0x92));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(0x93));
    let row_uuid = row_from_u64(10);
    let original_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("old")))
        .unwrap();
    accept_global(&mut core, original_tx, 1);
    let delete_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_001)
                .parents(vec![original_tx])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, delete_tx, 2);
    let restored_content_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_002)
                .parents(vec![delete_tx])
                .cells(title_cells("restored")),
        )
        .unwrap();
    accept_global(&mut core, restored_content_tx, 3);
    let restore_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_003)
                .parents(vec![restored_content_tx])
                .deletion(DeletionEvent::Restored),
        )
        .unwrap();
    accept_global(&mut core, restore_tx, 4);
    let (shape, binding) = title_shape_binding("restored");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = &update
    else {
        panic!("expected view update");
    };
    let version_bundles = version_bundles_for_update(&update);
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row_uuid, restored_content_tx)]
    );
    assert!(
        version_bundles.iter().any(|bundle| {
            bundle.tx.tx_id == restored_content_tx
                && bundle.versions.iter().any(|version| {
                    version.table() == "todos"
                        && version.row_uuid() == row_uuid
                        && version.deletion().is_none()
                        && wire_version_cells(version, core.table("todos").unwrap())
                            == title_cells("restored")
                })
        }),
        "rehydrate must ship the restored content version, not the pre-delete content"
    );
    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("restored"))])
    );
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            restored_content_tx
        )]))
    );
}

#[test]
fn local_rehydrate_after_edge_restore_ships_restored_row() {
    let (_core_dir, mut core) = open_node_with_uuid(node(0x94));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(0x95));
    let row_uuid = row_from_u64(10);
    let original_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("old")))
        .unwrap();
    accept_edge(&mut core, original_tx);
    let delete_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_001)
                .parents(vec![original_tx])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_edge(&mut core, delete_tx);
    let restored_content_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_002)
                .parents(vec![delete_tx])
                .cells(title_cells("restored")),
        )
        .unwrap();
    accept_edge(&mut core, restored_content_tx);
    let restore_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_003)
                .parents(vec![restored_content_tx])
                .deletion(DeletionEvent::Restored),
        )
        .unwrap();
    accept_edge(&mut core, restore_tx);
    let (shape, binding) = title_shape_binding("restored");
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Local,
        ..RegisterShapeOptions::default()
    };
    let subscription = subscription_key_with_opts(&shape, &binding, &opts);
    register_shape_binding_for_receiver_with_opts(&mut reader, &shape, &binding, opts.clone());
    let mut peer = PeerState::new();

    let update = peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts.clone())
        .unwrap();

    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = &update
    else {
        panic!("expected view update");
    };
    let version_bundles = version_bundles_for_update(&update);
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row_uuid, restored_content_tx)]
    );
    assert!(version_bundles.iter().any(|bundle| {
        bundle.tx.tx_id == restore_tx
            && bundle
                .versions
                .iter()
                .any(|version| version.deletion() == Some(DeletionEvent::Restored))
    }));
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("restored"))])
    );
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            restored_content_tx
        )]))
    );
}

#[test]
fn local_rehydrate_after_edge_restore_transaction_ships_restored_row() {
    let (_core_dir, mut core) = open_node_with_uuid(node(0x96));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(0x97));
    let row_uuid = row_from_u64(10);
    let original_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("old")))
        .unwrap();
    accept_edge(&mut core, original_tx);
    let delete_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_001)
                .parents(vec![original_tx])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_edge(&mut core, delete_tx);
    let restore_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", row_uuid, 1_002).cells(title_cells("restored")),
            MergeableCommit::new("todos", row_uuid, 1_003).deletion(DeletionEvent::Restored),
        ])
        .unwrap();
    accept_edge(&mut core, restore_tx);
    let (shape, binding) = title_shape_binding("restored");
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Local,
        ..RegisterShapeOptions::default()
    };
    let subscription = subscription_key_with_opts(&shape, &binding, &opts);
    register_shape_binding_for_receiver_with_opts(&mut reader, &shape, &binding, opts.clone());
    let mut peer = PeerState::new();

    let update = peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts.clone())
        .unwrap();

    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = &update
    else {
        panic!("expected view update");
    };
    let version_bundles = version_bundles_for_update(&update);
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row_uuid, restore_tx)]
    );
    assert!(version_bundles.iter().any(|bundle| {
        bundle.tx.tx_id == restore_tx
            && bundle
                .versions
                .iter()
                .any(|version| version.deletion() == Some(DeletionEvent::Restored))
            && bundle
                .versions
                .iter()
                .any(|version| version.deletion().is_none())
    }));
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("restored"))])
    );
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            restore_tx
        )]))
    );
}

#[test]
fn maintained_subscription_view_limit_one_switches_after_winner_delete_and_lower_insert() {
    let (_dir, mut core) = open_node_with_uuid(node(0x91));
    let first_row = row_from_u64(10);
    let second_row = row_from_u64(20);
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", first_row, 1_000).cells(title_cells("first")),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", second_row, 1_001).cells(title_cells("second")),
        )
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    accept_global(&mut core, second_tx, 2);
    let shape = Query::from("todos").limit(1).validate(&schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let delete_first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", first_row, 1_002).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, delete_first_tx, 3);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_removes,
        vec![("todos".to_owned().into(), first_row, first_tx)]
    );
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), second_row, second_tx)]
    );

    let new_first_row = row_from_u64(5);
    let new_first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", new_first_row, 1_003).cells(title_cells("new first")),
        )
        .unwrap();
    accept_global(&mut core, new_first_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_removes,
        vec![("todos".to_owned().into(), second_row, second_tx)]
    );
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), new_first_row, new_first_tx)]
    );
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 3);
}

#[test]
fn maintained_subscription_view_order_by_asc_limit_two_initial_hydration() {
    let (_dir, mut core) = open_node_with_schema(node(0x92), priority_schema());
    let charlie_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_000)
                .cells(priority_cells("charlie", 30)),
        )
        .unwrap();
    let alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_001)
                .cells(priority_cells("alpha", 10)),
        )
        .unwrap();
    let bravo_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_002)
                .cells(priority_cells("bravo", 20)),
        )
        .unwrap();
    accept_global(&mut core, charlie_tx, 1);
    accept_global(&mut core, alpha_tx, 2);
    accept_global(&mut core, bravo_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .limit(2)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_view_update_rows(
        update,
        vec![
            ("todos", row_from_u64(10), alpha_tx),
            ("todos", row_from_u64(20), bravo_tx),
        ],
        vec![],
    );
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.unsupported_skips_out, 0);
}

#[test]
fn maintained_subscription_view_order_by_asc_limit_two_boundary_insert_delete_updates() {
    let (_dir, mut core) = open_node_with_schema(node(0x93), priority_schema());
    let alpha = row_from_u64(10);
    let bravo = row_from_u64(20);
    let charlie = row_from_u64(30);
    let alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", alpha, 1_000).cells(priority_cells("alpha", 10)),
        )
        .unwrap();
    let bravo_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", bravo, 1_001).cells(priority_cells("bravo", 20)),
        )
        .unwrap();
    let charlie_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", charlie, 1_002).cells(priority_cells("charlie", 30)),
        )
        .unwrap();
    accept_global(&mut core, alpha_tx, 1);
    accept_global(&mut core, bravo_tx, 2);
    accept_global(&mut core, charlie_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .limit(2)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let aardvark = row_from_u64(5);
    let aardvark_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", aardvark, 1_003).cells(priority_cells("aardvark", 5)),
        )
        .unwrap();
    accept_global(&mut core, aardvark_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        update,
        vec![("todos", aardvark, aardvark_tx)],
        vec![("todos", bravo, bravo_tx)],
    );

    let delete_alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", alpha, 1_004).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, delete_alpha_tx, 5);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        update,
        vec![("todos", bravo, bravo_tx)],
        vec![("todos", alpha, alpha_tx)],
    );
}

#[test]
fn maintained_subscription_view_order_by_limit_updates_move_rows_across_boundary() {
    let (_dir, mut core) = open_node_with_schema(node(0x93), priority_schema());
    let alpha = row_from_u64(10);
    let bravo = row_from_u64(20);
    let charlie = row_from_u64(30);
    let alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", alpha, 1_000).cells(priority_cells("alpha", 10)),
        )
        .unwrap();
    let bravo_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", bravo, 1_001).cells(priority_cells("bravo", 20)),
        )
        .unwrap();
    let charlie_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", charlie, 1_002).cells(priority_cells("charlie", 30)),
        )
        .unwrap();
    accept_global(&mut core, alpha_tx, 1);
    accept_global(&mut core, bravo_tx, 2);
    accept_global(&mut core, charlie_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .limit(2)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let charlie_promoted_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", charlie, 1_003).cells(priority_cells("charlie", 5)),
        )
        .unwrap();
    accept_global(&mut core, charlie_promoted_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        update,
        vec![("todos", charlie, charlie_promoted_tx)],
        vec![("todos", bravo, bravo_tx)],
    );

    let charlie_demoted_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", charlie, 1_004).cells(priority_cells("charlie", 35)),
        )
        .unwrap();
    accept_global(&mut core, charlie_demoted_tx, 5);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        update,
        vec![("todos", bravo, bravo_tx)],
        vec![("todos", charlie, charlie_promoted_tx)],
    );
}

#[test]
fn maintained_subscription_view_order_by_desc_limit_two_initial_hydration() {
    let (_dir, mut core) = open_node_with_schema(node(0x94), priority_schema());
    let alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_000)
                .cells(priority_cells("alpha", 10)),
        )
        .unwrap();
    let delta_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(40), 1_001)
                .cells(priority_cells("delta", 40)),
        )
        .unwrap();
    let charlie_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_002)
                .cells(priority_cells("charlie", 30)),
        )
        .unwrap();
    accept_global(&mut core, alpha_tx, 1);
    accept_global(&mut core, delta_tx, 2);
    accept_global(&mut core, charlie_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Desc)
        .limit(2)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_view_update_rows(
        update,
        vec![
            ("todos", row_from_u64(40), delta_tx),
            ("todos", row_from_u64(30), charlie_tx),
        ],
        vec![],
    );
    assert_eq!(
        peer.maintained_subscription_view_metrics()
            .unsupported_skips_out,
        0
    );
}

#[test]
fn maintained_subscription_view_order_by_limit_two_ties_are_stable_by_row_uuid() {
    let (_dir, mut core) = open_node_with_schema(node(0x95), priority_schema());
    let third_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_000)
                .cells(priority_cells("third", 7)),
        )
        .unwrap();
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_001)
                .cells(priority_cells("first", 7)),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_002)
                .cells(priority_cells("second", 7)),
        )
        .unwrap();
    accept_global(&mut core, third_tx, 1);
    accept_global(&mut core, first_tx, 2);
    accept_global(&mut core, second_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .limit(2)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_view_update_rows(
        update,
        vec![
            ("todos", row_from_u64(10), first_tx),
            ("todos", row_from_u64(20), second_tx),
        ],
        vec![],
    );

    let replacement = row_from_u64(5);
    let replacement_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", replacement, 1_003).cells(priority_cells("zeroth", 7)),
        )
        .unwrap();
    accept_global(&mut core, replacement_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        update,
        vec![("todos", replacement, replacement_tx)],
        vec![("todos", row_from_u64(20), second_tx)],
    );
}

#[test]
fn maintained_subscription_view_order_by_offset_limit_uses_top_by_window() {
    let (_dir, mut core) = open_node_with_schema(node(0x96), priority_schema());
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_000)
                .cells(priority_cells("first", 10)),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_001)
                .cells(priority_cells("second", 20)),
        )
        .unwrap();
    let third_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_002)
                .cells(priority_cells("third", 30)),
        )
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    accept_global(&mut core, second_tx, 2);
    accept_global(&mut core, third_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .offset(1)
        .limit(1)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_view_update_rows(update, vec![("todos", row_from_u64(20), second_tx)], vec![]);

    let zeroth = row_from_u64(5);
    let zeroth_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", zeroth, 1_003).cells(priority_cells("zeroth", 5)),
        )
        .unwrap();
    accept_global(&mut core, zeroth_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        update,
        vec![("todos", row_from_u64(10), first_tx)],
        vec![("todos", row_from_u64(20), second_tx)],
    );
}

#[test]
fn maintained_subscription_view_order_by_without_limit_matches_one_shot_order() {
    let (_dir, mut core) = open_node_with_schema(node(0x97), priority_schema());
    let charlie_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_000)
                .cells(priority_cells("charlie", 30)),
        )
        .unwrap();
    let alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_001)
                .cells(priority_cells("alpha", 10)),
        )
        .unwrap();
    let bravo_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_002)
                .cells(priority_cells("bravo", 20)),
        )
        .unwrap();
    accept_global(&mut core, charlie_tx, 1);
    accept_global(&mut core, alpha_tx, 2);
    accept_global(&mut core, bravo_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let one_shot = core
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_eq!(
        one_shot,
        vec![row_from_u64(10), row_from_u64(20), row_from_u64(30)]
    );
    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_view_update_row_order(
        update,
        vec![
            ("todos", row_from_u64(10), alpha_tx),
            ("todos", row_from_u64(20), bravo_tx),
            ("todos", row_from_u64(30), charlie_tx),
        ],
        vec![],
    );
}

#[test]
fn maintained_subscription_view_order_by_offset_without_limit_matches_one_shot_window() {
    let (_dir, mut core) = open_node_with_schema(node(0x98), priority_schema());
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_000)
                .cells(priority_cells("first", 10)),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_001)
                .cells(priority_cells("second", 20)),
        )
        .unwrap();
    let third_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_002)
                .cells(priority_cells("third", 30)),
        )
        .unwrap();
    accept_global(&mut core, third_tx, 1);
    accept_global(&mut core, first_tx, 2);
    accept_global(&mut core, second_tx, 3);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .offset(1)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let one_shot = core
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_eq!(one_shot, vec![row_from_u64(20), row_from_u64(30)]);
    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_view_update_row_order(
        update,
        vec![
            ("todos", row_from_u64(20), second_tx),
            ("todos", row_from_u64(30), third_tx),
        ],
        vec![],
    );

    let zeroth = row_from_u64(5);
    let zeroth_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", zeroth, 1_003).cells(priority_cells("zeroth", 5)),
        )
        .unwrap();
    accept_global(&mut core, zeroth_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_row_order(update, vec![("todos", row_from_u64(10), first_tx)], vec![]);

    let delete_first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_004).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, delete_first_tx, 5);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_row_order(update, vec![], vec![("todos", row_from_u64(10), first_tx)]);
}

#[test]
fn maintained_subscription_view_order_by_limit_handles_emptying_below_limit_and_repopulate() {
    let (_dir, mut core) = open_node_with_schema(node(0x98), priority_schema());
    let alpha = row_from_u64(10);
    let bravo = row_from_u64(20);
    let alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", alpha, 1_000).cells(priority_cells("alpha", 10)),
        )
        .unwrap();
    let bravo_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", bravo, 1_001).cells(priority_cells("bravo", 20)),
        )
        .unwrap();
    accept_global(&mut core, alpha_tx, 1);
    accept_global(&mut core, bravo_tx, 2);
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .limit(3)
        .validate(&priority_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let delete_alpha_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", alpha, 1_002).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, delete_alpha_tx, 3);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(update, vec![], vec![("todos", alpha, alpha_tx)]);

    let delete_bravo_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", bravo, 1_003).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, delete_bravo_tx, 4);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(update, vec![], vec![("todos", bravo, bravo_tx)]);

    let charlie = row_from_u64(30);
    let charlie_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", charlie, 1_004).cells(priority_cells("charlie", 30)),
        )
        .unwrap();
    accept_global(&mut core, charlie_tx, 5);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(update, vec![("todos", charlie, charlie_tx)], vec![]);
}

#[test]
fn maintained_subscription_view_without_order_by_matches_one_shot_row_id_order() {
    let (_dir, mut core) = open_node_with_uuid(node(0x99));
    let third_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(30), 1_000).cells(title_cells("third")),
        )
        .unwrap();
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(10), 1_001).cells(title_cells("first")),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_from_u64(20), 1_002).cells(title_cells("second")),
        )
        .unwrap();
    accept_global(&mut core, third_tx, 1);
    accept_global(&mut core, first_tx, 2);
    accept_global(&mut core, second_tx, 3);
    let shape = Query::from("todos").validate(&schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let one_shot = core
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_eq!(
        one_shot,
        vec![row_from_u64(10), row_from_u64(20), row_from_u64(30)]
    );
    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_view_update_row_order(
        update,
        vec![
            ("todos", row_from_u64(10), first_tx),
            ("todos", row_from_u64(20), second_tx),
            ("todos", row_from_u64(30), third_tx),
        ],
        vec![],
    );
}

#[test]
fn maintained_subscription_view_default_order_limited_variants_are_supported() {
    let (_dir, mut core) = open_node_with_uuid(node(0x90));
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x10), 1_000).cells(title_cells("alpha")),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x11), 1_001).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    accept_global(&mut core, second_tx, 2);
    let no_order_limit = Query::from("todos").limit(2).validate(&schema()).unwrap();
    let offset_limit_one = Query::from("todos")
        .limit(1)
        .offset(1)
        .validate(&schema())
        .unwrap();
    let shapes = [
        (
            no_order_limit,
            vec![
                ("todos", row(0x10), first_tx),
                ("todos", row(0x11), second_tx),
            ],
        ),
        (offset_limit_one, vec![("todos", row(0x11), second_tx)]),
    ];
    let mut peer = PeerState::new();

    for (shape, expected_adds) in shapes {
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);

        let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_view_update_row_order(update, expected_adds, vec![]);
    }

    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.unsupported_skips_out, 0);
}

#[test]
fn maintained_subscription_view_aggregate_rehydrate_ships_payload_fact() {
    let (_dir, mut core) = open_node_with_uuid(node(0x90));
    let first_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x10), 1_000).cells(title_cells("alpha")),
        )
        .unwrap();
    let second_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x11), 1_001).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    accept_global(&mut core, second_tx, 2);
    let aggregate_shape = Query::from("todos").count().validate(&schema()).unwrap();
    let aggregate_binding = aggregate_shape.bind(BTreeMap::new()).unwrap();
    let aggregate_subscription = subscription_key(&aggregate_shape, &aggregate_binding);
    let mut peer = PeerState::new();

    let update = peer
        .rehydrate_query(&mut core, &aggregate_shape, &aggregate_binding)
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };

    assert!(reset_result_set);
    assert_eq!(result_member_adds.len(), 1);
    assert!(result_member_removes.is_empty());
    assert_eq!(program_fact_adds.len(), 1);
    assert!(program_fact_removes.is_empty());
    assert_eq!(
        aggregate_payload_count(&program_fact_adds[0]),
        Value::U64(2)
    );
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.unsupported_skips_out, 0);
    assert!(maintained_subscription_id(&peer, aggregate_subscription).is_some());
}

#[test]
fn maintained_subscription_view_aggregate_updates_incrementally() {
    let (_dir, mut core) = open_node_with_uuid(node(0x90));
    for (idx, title) in [(0x10, "alpha"), (0x11, "beta")] {
        let tx = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(idx), 1_000 + idx as u64)
                    .cells(title_cells(title)),
            )
            .unwrap();
        accept_global(&mut core, tx, idx as u64);
    }
    let shape = Query::from("todos").count().validate(&schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        program_fact_adds, ..
    }) = initial
    else {
        panic!("expected view update");
    };
    assert_eq!(
        aggregate_payload_count(&program_fact_adds[0]),
        Value::U64(2)
    );

    let tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x12), 2_000).cells(title_cells("gamma")),
        )
        .unwrap();
    accept_global(&mut core, tx, 100);
    let update = peer
        .query_update_for_subscription(&mut core, subscription, &shape, &binding)
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };

    assert!(!reset_result_set);
    assert_eq!(result_member_adds.len(), 1);
    assert_eq!(result_member_removes.len(), 1);
    assert_eq!(program_fact_adds.len(), 1);
    assert_eq!(program_fact_removes.len(), 1);
    assert_eq!(
        aggregate_payload_count(&program_fact_adds[0]),
        Value::U64(3)
    );
}

#[test]
fn aggregate_policy_oracle_matches_visible_rows_per_identity() {
    let admin = AuthorSubject::for_test_bytes([0xa1; 16]);
    let member = AuthorSubject::for_test_bytes([0xb2; 16]);
    let spy = AuthorSubject::for_test_bytes([0xc3; 16]);
    let (_dir, mut core) = open_node_with_schema(node(0x90), aggregate_access_policy_schema());
    let docs = [
        (row(0x10), "alpha", 10, vec![admin, member]),
        (row(0x11), "beta", 20, vec![admin]),
        (row(0x12), "gamma", 30, vec![member]),
    ];
    let mut seq = 1;
    for (doc, title, score, readers) in docs {
        let tx = core
            .commit_mergeable_settled(
                MergeableCommit::new("docs", doc, 1_000 + seq)
                    .cells(scored_doc_cells(title, score)),
            )
            .unwrap();
        accept_global(&mut core, tx, seq);
        seq += 1;
        for reader in readers {
            let tx = core
                .commit_mergeable_settled(
                    MergeableCommit::new("docAccess", row(seq as u8), 2_000 + seq)
                        .cells(access_cells(doc, reader)),
                )
                .unwrap();
            accept_global(&mut core, tx, seq);
            seq += 1;
        }
    }
    let shape = Query::from("docs")
        .aggregate([Aggregate::count(), Aggregate::sum("score")])
        .validate(&aggregate_access_policy_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    for (identity, expected_count, expected_sum) in
        [(admin, 2, Some(30)), (member, 2, Some(40)), (spy, 0, None)]
    {
        core.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                crate::query::provider_claim_key("sub"),
                Value::Uuid(identity.test_uuid()),
            )]),
        );
        let rows = core
            .query_rows_with_prepared_plan_for_identity(
                &shape,
                &binding,
                DurabilityTier::Global,
                None,
                identity,
            )
            .unwrap();
        let cells = aggregate_cells(&rows[0]);
        assert_eq!(cells["count"], Value::U64(expected_count));
        assert_eq!(
            cells.get("sum_score").cloned(),
            expected_sum.map(Value::U64)
        );
    }
}

#[test]
fn peer_runtime_handles_do_not_cross_node_runtime_instances() {
    let user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let (_first_dir, mut first_core) = open_node_with_schema(node(0x90), access_policy_schema());
    let mut peer = PeerState::edge_client(user);

    peer.current_rows_update(&mut first_core, "docs").unwrap();

    let (_second_dir, mut second_core) = open_node_with_schema(node(0x90), access_policy_schema());

    peer.current_rows_update(&mut second_core, "docs").unwrap();
}

#[test]
fn maintained_subscription_view_forget_with_node_unsubscribes_and_drops_state() {
    let (_dir, mut core) = open_node_with_uuid(node(0x91));
    let row_uuid = row(0x11);
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let (shape, binding) = title_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let maintained_id = maintained_subscription_id(&peer, subscription)
        .expect("supported maintained-view rehydrate must install maintained subscription");

    assert!(peer.forget_subscription_with_node(&mut core, subscription));
    assert!(maintained_subscription_id(&peer, subscription).is_none());
    assert!(row_result_set(&peer, subscription).is_none());
    assert!(!core.unsubscribe_groove_subscription(maintained_id));

    let stale_tick = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        stale_tick,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: crate::time::GlobalTime(0),
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
    );
}

#[test]
fn maintained_subscription_view_forget_query_binding_with_node_unsubscribes() {
    let (_dir, mut core) = open_node_with_uuid(node(0x94));
    let row_uuid = row(0x41);
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let (shape, binding) = title_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let maintained_id = maintained_subscription_id(&peer, subscription).unwrap();

    assert!(peer.forget_query_binding_with_node(&mut core, &shape, &binding));
    assert!(maintained_subscription_id(&peer, subscription).is_none());
    assert!(!core.unsubscribe_groove_subscription(maintained_id));
}

#[test]
fn maintained_subscription_view_hit_metrics_and_footprint_update() {
    let (_dir, mut core) = open_node_with_uuid(node(0x95));
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x51), 1_000).cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let (shape, binding) = title_shape_binding("match");
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.hits_out, 1);
    assert_eq!(metrics.footprint.result_rows, 1);
    assert!(metrics.footprint.version_identities >= 1);
    assert!(metrics.footprint.version_tx_entries >= 1);
}

#[test]
fn maintained_subscription_view_contains_literal_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0x9a));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x5a), 1_000).cells(title_cells("api docs")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x5b), 1_001).cells(title_cells("notes")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);
    let (shape, binding) = title_contains_shape_binding("api");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x5c), 1_002).cells(title_cells("api reference")),
        )
        .unwrap();
    accept_global(&mut core, added, 3);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row(0x5c), added)]
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_contains_param_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0x9b));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x6a), 1_000).cells(title_cells("api docs")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x6b), 1_001).cells(title_cells("notes")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);
    let (shape, binding) = title_contains_param_shape_binding("api");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x6c), 1_002).cells(title_cells("api reference")),
        )
        .unwrap();
    accept_global(&mut core, added, 3);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row(0x6c), added)]
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_eq_param_left_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0x9f));
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x6f), 1_000).cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x70), 1_001).cells(title_cells("other")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);
    let (shape, binding) = title_param_eq_column_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert!(maintained_subscription_id(&peer, subscription).is_some());
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row(0x6f),
            tx_id,
        )]))
    );
}

#[test]
fn maintained_subscription_view_ne_param_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0x9c));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x7a), 1_000).cells(title_cells("ship it")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x7b), 1_001).cells(title_cells("skip")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);
    let (shape, binding) = title_not_param_shape_binding("skip");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x7c), 1_002).cells(title_cells("done")),
        )
        .unwrap();
    accept_global(&mut core, added, 3);
    let still_excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x7d), 1_003).cells(title_cells("skip")),
        )
        .unwrap();
    accept_global(&mut core, still_excluded, 4);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row(0x7c), added)]
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_range_literal_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0xa1));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x81), 1_000).cells(title_cells("omega")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x82), 1_001).cells(title_cells("alpha")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);
    let (shape, binding) = title_after_literal_shape_binding("m");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x83), 1_002).cells(title_cells("zeta")),
        )
        .unwrap();
    accept_global(&mut core, added, 3);
    let still_excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x84), 1_003).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, still_excluded, 4);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x83)]));
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_reversed_range_literal_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0xa2));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x85), 1_000).cells(title_cells("alpha")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x86), 1_001).cells(title_cells("omega")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);
    let (shape, binding) = title_before_reversed_literal_shape_binding("m");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x87), 1_002).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, added, 3);
    let still_excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x88), 1_003).cells(title_cells("zeta")),
        )
        .unwrap();
    accept_global(&mut core, still_excluded, 4);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x87)]));
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_any_literal_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0xa4));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x89), 1_000).cells(title_cells("alpha")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let (shape, binding) = title_any_literal_shape_binding("alpha", "beta");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x8a), 1_001).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, added, 2);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x8a)]));
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_in_literal_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0xa5));
    let initial = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x8b), 1_000).cells(title_cells("alpha")),
        )
        .unwrap();
    accept_global(&mut core, initial, 1);
    let (shape, binding) = title_in_literal_shape_binding(["alpha", "beta"]);
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let added = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x8c), 1_001).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, added, 2);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x8c)]));
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_empty_in_and_any_are_false() {
    let (_dir, mut core) = open_node_with_uuid(node(0xa6));
    let existing = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x8d), 1_000).cells(title_cells("alpha")),
        )
        .unwrap();
    accept_global(&mut core, existing, 1);
    let empty_in = title_in_literal_shape_binding([]).0;
    let empty_any = Query::from("todos")
        .filter(crate::query::any_of([]))
        .validate(&schema())
        .unwrap();
    let mut peer = PeerState::new();

    for shape in [empty_in, empty_any] {
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);
        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert!(row_result_set(&peer, subscription).unwrap().is_empty());
    }
}

#[test]
fn maintained_subscription_view_any_with_bound_param_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0xa7));
    let shape = Query::from("todos")
        .filter(crate::query::any_of([
            eq(col("title"), lit("alpha")),
            eq(col("title"), param("title")),
        ]))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String("beta".to_owned()),
        )]))
        .unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(maintained_subscription_id(&peer, subscription).is_some());

    let matched_row = row(0xa8);
    let matched = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", matched_row, 1_000).cells(title_cells("beta")),
        )
        .unwrap();
    accept_global(&mut core, matched, 1);
    let excluded = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xa9), 1_001).cells(title_cells("gamma")),
        )
        .unwrap();
    accept_global(&mut core, excluded, 2);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        view_update_added_rows(update),
        BTreeSet::from([matched_row])
    );
}

#[test]
fn maintained_subscription_view_null_predicates_stay_maintained() {
    for (case, non_null) in [(0xa3, false), (0xa4, true)] {
        let (_dir, mut core) = open_node_with_schema(node(case), nullable_title_schema());
        let initial = core
            .commit_mergeable_settled(MergeableCommit::new("todos", row(case), 1_000).cells(
                maybe_title_cells(if non_null { Some("present") } else { None }),
            ))
            .unwrap();
        accept_global(&mut core, initial, 1);
        let excluded = core
            .commit_mergeable_settled(MergeableCommit::new("todos", row(case + 1), 1_001).cells(
                maybe_title_cells(if non_null { None } else { Some("present") }),
            ))
            .unwrap();
        accept_global(&mut core, excluded, 2);
        let (shape, binding) = nullable_title_shape_binding(non_null);
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());

        let added_row = row(case + 2);
        let added = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", added_row, 1_002)
                    .cells(maybe_title_cells(if non_null { Some("new") } else { None })),
            )
            .unwrap();
        accept_global(&mut core, added, 3);
        if !non_null {
            let still_excluded = core
                .commit_mergeable_settled(
                    MergeableCommit::new("todos", row(case + 3), 1_003)
                        .cells(maybe_title_cells(Some("new"))),
                )
                .unwrap();
            accept_global(&mut core, still_excluded, 4);
        }

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_eq!(view_update_added_rows(update), BTreeSet::from([added_row]));
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }
}

#[test]
fn maintained_subscription_view_exclusive_delta_stays_maintained() {
    let (_dir, mut core) = open_node_with_uuid(node(0x96));
    let (shape, binding) = title_shape_binding("match");
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(tx, "todos", row(0x61), title_cells("match"), None)
        .unwrap();
    let (tx_id, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 1_000).unwrap();
    accept_global(&mut core, tx_id, 1);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row(0x61), tx_id)]
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
}

#[test]
fn maintained_subscription_view_exclusive_delta_ships_view_scoped_partial_bundle() {
    let (_dir, mut core) = open_node_with_uuid(node(0x97));
    let (shape, binding) = title_shape_binding("match");
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(tx, "todos", row(0x71), title_cells("match"), None)
        .unwrap();
    core.tx_write(tx, "todos", row(0x72), title_cells("other"), None)
        .unwrap();
    let (tx_id, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 1_000).unwrap();
    accept_global(&mut core, tx_id, 1);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row(0x71), tx_id)]
    );
    assert!(result_member_removes.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(
        version_bundles[0].scope,
        crate::protocol::VersionBundleScope::ViewScoped
    );
    assert_eq!(version_bundles[0].tx.n_total_writes, 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), row(0x71));
    assert!(peer.shipped_complete_tx_payloads().is_empty());
}

#[test]
fn maintained_subscription_view_can_ship_complete_exclusive_payload_for_writer_peer() {
    let (_core_dir, mut core) = open_node_with_uuid(node(0x98));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(0x99));
    let (shape, binding) = title_shape_binding("match");
    let mut peer = PeerState::new();
    peer.set_ship_complete_exclusive_payloads(true);

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(tx, "todos", row(0x71), title_cells("match"), None)
        .unwrap();
    core.tx_write(tx, "todos", row(0x72), title_cells("other"), None)
        .unwrap();
    let (tx_id, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 1_000).unwrap();
    accept_global(&mut core, tx_id, 1);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row(0x71), tx_id)]
    );
    assert!(result_member_removes.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(version_bundles[0].versions.len(), 2);
    assert_eq!(
        version_bundles[0]
            .versions
            .iter()
            .map(VersionRecord::row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0x71), row(0x72)])
    );

    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![
            (row(0x71), title_cells("match")),
            (row(0x72), title_cells("other")),
        ]
    );
    let open = OpenTransactionId::new();
    reader.open_exclusive(open).unwrap();
    assert_eq!(
        reader.tx_read(open, "todos", row(0x72)).unwrap(),
        Some(title_cells("other"))
    );
}

#[test]
fn maintained_subscription_view_tags_terminal_columns_by_table() {
    let schema = public_peer_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("warehouses")
                    .column("ytd", PublicColumnType::Double),
            )
            .table(
                PublicTableSchemaBuilder::new("stock")
                    .column("ytd", PublicColumnType::Timestamp),
            )
            .table(
                PublicTableSchemaBuilder::new("orderLines")
                    .fk_column("warehouse", "warehouses")
                    .fk_column("stock", "stock"),
            ),
    );
    let (_dir, mut core) = open_node_with_schema(node(0x9a), schema);
    let warehouse = row(0x80);
    let stock = row(0x81);
    let line = row(0x82);
    let warehouse_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("warehouses", warehouse, 10)
                .cells(BTreeMap::from([("ytd".to_owned(), Value::F64(1.5))])),
        )
        .unwrap();
    accept_global(&mut core, warehouse_tx, 1);
    let stock_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("stock", stock, 11)
                .cells(BTreeMap::from([("ytd".to_owned(), Value::U64(2))])),
        )
        .unwrap();
    accept_global(&mut core, stock_tx, 2);
    let line_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("orderLines", line, 12).cells(BTreeMap::from([
                ("warehouse".to_owned(), Value::Uuid(warehouse.0)),
                ("stock".to_owned(), Value::Uuid(stock.0)),
            ])),
        )
        .unwrap();
    accept_global(&mut core, line_tx, 3);

    let mut peer = PeerState::new();
    let update = peer.current_rows_update(&mut core, "orderLines").unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("orderLines".to_owned().into(), line, line_tx)]
    );
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, line_tx);
}

#[test]
fn maintained_subscription_view_policy_view_exclusive_delta_ships_identity_scoped_partial_bundle() {
    let schema = access_policy_schema();
    let (_dir, mut core) = open_node_with_schema(node(0x98), schema);
    let user_a = AuthorSubject::for_test_bytes([0xa1; 16]);
    let user_b = AuthorSubject::for_test_bytes([0xb2; 16]);
    let doc_a = row(0x81);
    let doc_b = row(0x82);
    let project = row(0x83);

    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(tx, "docs", doc_a, doc_cells("a", project), None)
        .unwrap();
    core.tx_write(tx, "docs", doc_b, doc_cells("b", project), None)
        .unwrap();
    let (docs_tx, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10).unwrap();
    accept_global(&mut core, docs_tx, 1);
    let grant_a = core
        .commit_mergeable_settled(
            MergeableCommit::new("docAccess", row(0x84), 11).cells(access_cells(doc_a, user_a)),
        )
        .unwrap();
    accept_global(&mut core, grant_a, 2);
    let grant_b = core
        .commit_mergeable_settled(
            MergeableCommit::new("docAccess", row(0x85), 12).cells(access_cells(doc_b, user_b)),
        )
        .unwrap();
    accept_global(&mut core, grant_b, 3);

    let mut peer = PeerState::client_link(user_a);
    core.set_test_provider_claims(
        user_a,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(user_a.test_uuid()),
        )]),
    );
    peer.set_ship_complete_exclusive_payloads(true);
    core.reset_query_engine_read_metrics();
    let update = peer.current_rows_update(&mut core, "docs").unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![("docs".to_owned().into(), doc_a, docs_tx)]
    );
    assert!(result_member_removes.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, docs_tx);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(
        version_bundles[0].scope,
        crate::protocol::VersionBundleScope::ViewScoped
    );
    assert_eq!(version_bundles[0].tx.n_total_writes, 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), doc_a);
    assert!(peer.shipped_complete_tx_payloads().is_empty());
    let read_metrics = core.query_engine_read_metrics();
    assert!(read_metrics.policy_authorization_graphs > 0);
    assert!(read_metrics.policy_authorized_source_joins > 0);
}

#[test]
fn maintained_subscription_view_rehydrate_replaces_subscription_and_fresh_indexes() {
    let (_dir, mut core) = open_node_with_uuid(node(0x92));
    let first = row(0x21);
    let second = row(0x22);
    let first_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", first, 1_000).cells(title_cells("match")))
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    let (shape, binding) = title_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(initial, vec![("todos", first, first_tx)], vec![]);
    let old_id = maintained_subscription_id(&peer, subscription)
        .expect("initial maintained subscription missing");

    let second_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", second, 2_000).cells(title_cells("match")))
        .unwrap();
    accept_global(&mut core, second_tx, 2);
    let tick = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(tick, vec![("todos", second, second_tx)], vec![]);

    let rehydrate = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let new_id = maintained_subscription_id(&peer, subscription)
        .expect("replacement maintained subscription missing");
    assert_ne!(old_id, new_id);
    assert!(!core.unsubscribe_groove_subscription(old_id));
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set, ..
    }) = &rehydrate
    else {
        panic!("expected view update");
    };
    assert!(*reset_result_set);
    assert_view_update_rows(
        rehydrate,
        vec![("todos", first, first_tx), ("todos", second, second_tx)],
        vec![],
    );
}

#[test]
fn maintained_subscription_view_new_binding_after_forget_has_no_stale_state() {
    let (_dir, mut core) = open_node_with_uuid(node(0x93));
    let match_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x31), 1_000).cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, match_tx, 1);
    let other_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x32), 1_001).cells(title_cells("other")),
        )
        .unwrap();
    accept_global(&mut core, other_tx, 2);

    let (shape, match_binding) = title_shape_binding("match");
    let (_, other_binding) = title_shape_binding("other");
    let match_subscription = subscription_key(&shape, &match_binding);
    let other_subscription = subscription_key(&shape, &other_binding);
    assert_ne!(match_subscription, other_subscription);

    let mut peer = PeerState::new();
    peer.rehydrate_query(&mut core, &shape, &match_binding)
        .unwrap();
    assert!(peer.forget_subscription_with_node(&mut core, match_subscription));

    let update = peer
        .rehydrate_query(&mut core, &shape, &other_binding)
        .unwrap();
    assert!(maintained_subscription_id(&peer, match_subscription).is_none());
    assert!(maintained_subscription_id(&peer, other_subscription).is_some());
    assert_eq!(
        row_result_set(&peer, other_subscription),
        Some(BTreeSet::from([(
            groove::Intern::new("todos".to_owned()),
            row(0x32),
            other_tx,
        )]))
    );
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        reset_result_set,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert!(reset_result_set);
    assert_eq!(
        result_member_adds,
        vec![(groove::Intern::new("todos".to_owned()), row(0x32), other_tx,)]
    );
    assert!(result_member_removes.is_empty());
}

#[test]
fn peer_state_dedups_version_payloads_across_subscription_views() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(1);
    let tx_id = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("shared")))
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let mut peer = PeerState::new();

    let first = peer.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&first);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = first
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row, tx_id)]
    );
    assert!(result_member_removes.is_empty());

    let second = peer.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&second);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = second
    else {
        panic!("expected view update");
    };
    assert!(version_bundles.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert_eq!(peer.metrics.version_bundles_out, 1);
    assert_eq!(peer.metrics.complete_tx_payload_refs_out, 0);
    assert_eq!(peer.metrics.result_adds_out, 1);
    assert_eq!(peer.metrics.result_removes_out, 0);
    assert!(peer.shipped_complete_tx_payloads().is_empty());
}

#[test]
fn current_rows_update_installs_maintained_subscription_for_relay_and_edge_client() {
    let schema = access_policy_schema();
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb2; 16]);
    let project = row(0x40);
    let doc = row(0x41);
    let grant = row(0x42);
    let doc_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("docs", doc, 10).cells(doc_cells("visible", project)),
        )
        .unwrap();
    accept_global(&mut core, doc_tx, 1);
    let grant_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("docAccess", grant, 11).cells(access_cells(doc, owner)),
        )
        .unwrap();
    accept_global(&mut core, grant_tx, 2);
    let subscription = core.whole_table_subscription_key("docs").unwrap();

    let mut relay = PeerState::relay();
    let relay_update = relay.current_rows_update(&mut core, "docs").unwrap();
    assert!(maintained_subscription_id(&relay, subscription).is_some());
    assert_eq!(relay.maintained_subscription_view_metrics().hits_out, 1);
    assert!(view_update_added_rows(relay_update).contains(&doc));

    let mut edge_owner = PeerState::edge_client(owner);
    core.set_test_provider_claims(
        owner,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(owner.test_uuid()),
        )]),
    );
    let edge_update = edge_owner.current_rows_update(&mut core, "docs").unwrap();
    assert!(maintained_subscription_id(&edge_owner, subscription).is_some());
    assert_eq!(
        edge_owner.maintained_subscription_view_metrics().hits_out,
        1
    );
    assert!(view_update_added_rows(edge_update).contains(&doc));

    let mut edge_other = PeerState::edge_client(other);
    core.set_test_provider_claims(
        other,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(other.test_uuid()),
        )]),
    );
    let other_update = edge_other.current_rows_update(&mut core, "docs").unwrap();
    assert!(maintained_subscription_id(&edge_other, subscription).is_some());
    assert_eq!(
        edge_other.maintained_subscription_view_metrics().hits_out,
        1
    );
    assert!(!view_update_added_rows(other_update).contains(&doc));
}

#[test]
fn grant_later_exclusive_tx_extends_view_scoped_partial_bundle_after_policy_grant() {
    let schema = access_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema);
    let user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let doc_one = row(1);
    let doc_two = row(2);
    let project = row(9);

    let tx = OpenTransactionId::new();
    writer.open_exclusive(tx).unwrap();
    writer
        .tx_write(tx, "docs", doc_one, doc_cells("one", project), None)
        .unwrap();
    writer
        .tx_write(tx, "docs", doc_two, doc_cells("two", project), None)
        .unwrap();
    let (docs_tx, unit) = writer.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10).unwrap();
    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let first_grant = core
        .commit_mergeable_settled(
            MergeableCommit::new("docAccess", row(11), 11).cells(access_cells(doc_one, user)),
        )
        .unwrap();
    accept_global(&mut core, first_grant, 2);

    let mut peer = PeerState::client_link(user);
    core.set_test_provider_claims(
        user,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(user.test_uuid()),
        )]),
    );
    let first_update = peer.current_rows_update(&mut core, "docs").unwrap();
    let version_bundles = version_bundles_for_update(&first_update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        ..
    }) = &first_update
    else {
        panic!("expected view update");
    };
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(
        result_member_adds,
        &vec![("docs".to_owned().into(), doc_one, docs_tx)]
    );
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, docs_tx);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), doc_one);
    assert!(peer.shipped_complete_tx_payloads().is_empty());
    reader.apply_sync_message_settled(first_update).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("docs", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(doc_one, doc_cells("one", project))])
    );

    let second_grant = core
        .commit_mergeable_settled(
            MergeableCommit::new("docAccess", row(12), 12).cells(access_cells(doc_two, user)),
        )
        .unwrap();
    accept_global(&mut core, second_grant, 3);

    let grant_update = peer.current_rows_update(&mut core, "docs").unwrap();
    let version_bundles = version_bundles_for_update(&grant_update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = &grant_update
    else {
        panic!("expected view update");
    };
    assert!(complete_tx_payload_refs.is_empty());
    assert!(result_member_removes.is_empty());
    assert_eq!(
        result_member_adds,
        &vec![("docs".to_owned().into(), doc_two, docs_tx),]
    );
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, docs_tx);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), doc_two);

    reader.apply_sync_message_settled(grant_update).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("docs", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (doc_one, doc_cells("one", project)),
            (doc_two, doc_cells("two", project)),
        ])
    );
}

#[test]
fn all_exclusive_never_gated_stays_incremental() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row_one = row(1);
    let row_two = row(2);
    let mut peer = PeerState::new();

    let empty = peer.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&empty);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = empty
    else {
        panic!("expected view update");
    };
    assert!(result_member_adds.is_empty());
    assert!(version_bundles.is_empty());

    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(tx, "todos", row_one, title_cells("one"), None)
        .unwrap();
    core.tx_write(tx, "todos", row_two, title_cells("two"), None)
        .unwrap();
    let (tx_id, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10).unwrap();
    accept_global(&mut core, tx_id, 1);

    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![
            ("todos".to_owned().into(), row_one, tx_id),
            ("todos".to_owned().into(), row_two, tx_id),
        ]
    );
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(result_member_removes.is_empty());
}

#[test]
fn peer_state_records_current_result_set_and_can_rehydrate() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(1);
    let tx_id = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("task")))
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let mut peer = PeerState::new();
    let subscription = core.whole_table_subscription_key("todos").unwrap();

    peer.current_rows_update(&mut core, "todos").unwrap();
    assert_eq!(
        peer.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_id]))
    );

    peer.forget_subscription(subscription);
    assert!(peer.subscription_result_sets(subscription).is_none());
    let rehydrated = peer.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&rehydrated);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = rehydrated
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row, tx_id)]
    );
    assert!(result_member_removes.is_empty());

    let rows = core.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn rehydrate_keeps_peer_payload_dedup_but_resends_result_set() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let deleted_row = row(1);
    let live_row = row(2);
    let deleted_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", deleted_row, 10).cells(title_cells("deleted")),
        )
        .unwrap();
    accept_global(&mut core, deleted_tx, 1);
    let live_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", live_row, 11).cells(title_cells("live")))
        .unwrap();
    accept_global(&mut core, live_tx, 2);
    let mut peer = PeerState::new();

    let initial = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message_settled(initial).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .len(),
        2
    );

    let deletion_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", deleted_row, 12).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    accept_global(&mut core, deletion_tx, 3);
    let missed_remove = peer.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_removes,
        ..
    }) = &missed_remove
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_removes,
        &vec![("todos".to_owned().into(), deleted_row, deleted_tx)]
    );

    let rehydrated = peer.reset_current_rows(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&rehydrated);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = &rehydrated
    else {
        panic!("expected view update");
    };
    assert!(*reset_result_set);
    assert!(complete_tx_payload_refs.is_empty());
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), live_row, live_tx)]
    );
    assert!(result_member_removes.is_empty());
    assert!(
        version_bundles
            .iter()
            .any(|bundle| bundle.tx.tx_id == live_tx)
            && version_bundles
                .iter()
                .all(|bundle| bundle.tx.tx_id != deleted_tx),
        "rehydrate should resend the live view-scoped payload without reviving deleted rows"
    );
    reader.apply_sync_message_settled(rehydrated).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(live_row, title_cells("live"))])
    );
}

#[test]
fn peer_state_sends_result_removes_after_deletes() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(1);
    let tx_id = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("task")))
        .unwrap();
    accept_global(&mut core, tx_id, 1);
    let mut peer = PeerState::new();

    let initial = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message_settled(initial).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .len(),
        1
    );

    let deletion_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    accept_global(&mut core, deletion_tx, 2);
    let removed = peer.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = &removed
    else {
        panic!("expected view update");
    };
    assert!(result_member_adds.is_empty());
    assert_eq!(
        result_member_removes,
        &vec![("todos".to_owned().into(), row, tx_id)]
    );
    reader.apply_sync_message_settled(removed).unwrap();
    assert!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
    assert_eq!(peer.metrics.result_removes_out, 1);
}

#[test]
fn whole_table_incremental_delta_ships_restore_register_witness() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(1);
    let content_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("task")))
        .unwrap();
    accept_global(&mut core, content_tx, 1);
    let mut peer = PeerState::new();

    reader
        .apply_sync_message_settled(peer.current_rows_update(&mut core, "todos").unwrap())
        .unwrap();
    let deletion_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    accept_global(&mut core, deletion_tx, 2);
    reader
        .apply_sync_message_settled(peer.current_rows_update(&mut core, "todos").unwrap())
        .unwrap();
    assert!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let restore_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Restored))
        .unwrap();
    accept_global(&mut core, restore_tx, 3);
    let restored = peer.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&restored);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = &restored
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row, content_tx)]
    );
    assert!(result_member_removes.is_empty());
    assert!(
        version_bundles
            .iter()
            .any(|bundle| bundle.tx.tx_id == restore_tx)
            || complete_tx_payload_refs.contains(&restore_tx),
        "restore register must ship as negative knowledge with the result add"
    );
    reader.apply_sync_message_settled(restored).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("task"))])
    );
}

#[test]
fn incremental_query_result_set_tracks_identical_cell_rewrite_tx_id() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row_uuid = row(1);
    let first_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("same")))
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("title")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String("same".to_owned()),
        )]))
        .unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };
    let mut peer = PeerState::new();
    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            first_tx
        )]))
    );

    let second_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 11).cells(title_cells("same")))
        .unwrap();
    accept_global(&mut core, second_tx, 2);
    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected query view update");
    };
    assert_eq!(
        result_member_removes,
        vec![("todos".to_owned().into(), row_uuid, first_tx)]
    );
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row_uuid, second_tx)]
    );
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            second_tx
        )]))
    );
}

#[test]
fn incremental_query_result_set_drops_enter_then_leave_same_drain_cycle() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(1);
    let (shape, binding) = title_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    reader
        .apply_sync_message_settled(peer.rehydrate_query(&mut core, &shape, &binding).unwrap())
        .unwrap();

    let match_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("match")))
        .unwrap();
    accept_global(&mut core, match_tx, 1);
    let unmatch_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 11)
                .parents(vec![match_tx])
                .cells(title_cells("other")),
        )
        .unwrap();
    accept_global(&mut core, unmatch_tx, 2);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = &update
    else {
        panic!("expected query view update");
    };
    assert!(
        result_member_adds.is_empty(),
        "enter-then-leave in one drain must not ship a stale add"
    );
    assert!(result_member_removes.is_empty());
    assert!(row_result_set(&peer, subscription).is_none_or(|set| set.is_empty()));
    reader.apply_sync_message_settled(update).unwrap();
    assert!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn incremental_query_result_set_keeps_leave_then_reenter_same_drain_cycle() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(1);
    let first_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("match")))
        .unwrap();
    accept_global(&mut core, first_tx, 1);
    let (shape, binding) = title_shape_binding("match");
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    reader
        .apply_sync_message_settled(peer.rehydrate_query(&mut core, &shape, &binding).unwrap())
        .unwrap();
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            first_tx
        )]))
    );

    let unmatch_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 11)
                .parents(vec![first_tx])
                .cells(title_cells("other")),
        )
        .unwrap();
    accept_global(&mut core, unmatch_tx, 2);
    let second_match_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 12)
                .parents(vec![unmatch_tx])
                .cells(title_cells("match")),
        )
        .unwrap();
    accept_global(&mut core, second_match_tx, 3);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = &update
    else {
        panic!("expected query view update");
    };
    assert_eq!(
        result_member_removes,
        &vec![("todos".to_owned().into(), row_uuid, first_tx)]
    );
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row_uuid, second_match_tx)]
    );
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([(
            "todos".to_owned().into(),
            row_uuid,
            second_match_tx
        )]))
    );
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("match"))])
    );
}

#[test]
fn incremental_query_result_set_rebuilds_stale_closure_rows() {
    let schema = public_peer_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("stock")
                    .column("quantity", PublicColumnType::Timestamp),
            )
            .table(PublicTableSchemaBuilder::new("orderLines").fk_column("stock", "stock")),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let stock_row = row(1);
    let first_line_row = row(2);
    let second_line_row = row(3);
    let stock_v1 = core
        .commit_mergeable_settled(
            MergeableCommit::new("stock", stock_row, 10)
                .cells(BTreeMap::from([("quantity".to_owned(), Value::U64(10))])),
        )
        .unwrap();
    accept_global(&mut core, stock_v1, 1);
    let first_line_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("orderLines", first_line_row, 11).cells(BTreeMap::from([(
                "stock".to_owned(),
                Value::Uuid(stock_row.0),
            )])),
        )
        .unwrap();
    accept_global(&mut core, first_line_tx, 2);
    let shape = Query::from("orderLines").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = subscription_key(&shape, &binding);
    let mut peer = PeerState::new();

    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([
            (
                "orderLines".to_owned().into(),
                first_line_row,
                first_line_tx
            ),
            ("stock".to_owned().into(), stock_row, stock_v1),
        ]))
    );

    let stock_v2 = core
        .commit_mergeable_settled(
            MergeableCommit::new("stock", stock_row, 12)
                .parents(vec![stock_v1])
                .cells(BTreeMap::from([("quantity".to_owned(), Value::U64(9))])),
        )
        .unwrap();
    accept_global(&mut core, stock_v2, 3);
    let second_line_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("orderLines", second_line_row, 13).cells(BTreeMap::from([(
                "stock".to_owned(),
                Value::Uuid(stock_row.0),
            )])),
        )
        .unwrap();
    accept_global(&mut core, second_line_tx, 4);

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected query view update");
    };
    assert_eq!(
        result_member_removes,
        vec![("stock".to_owned().into(), stock_row, stock_v1)]
    );
    assert_eq!(
        result_member_adds,
        vec![
            (
                "orderLines".to_owned().into(),
                second_line_row,
                second_line_tx
            ),
            ("stock".to_owned().into(), stock_row, stock_v2),
        ]
    );
    assert_eq!(
        row_result_set(&peer, subscription),
        Some(BTreeSet::from([
            (
                "orderLines".to_owned().into(),
                first_line_row,
                first_line_tx
            ),
            (
                "orderLines".to_owned().into(),
                second_line_row,
                second_line_tx
            ),
            ("stock".to_owned().into(), stock_row, stock_v2),
        ]))
    );
}

#[test]
fn incremental_query_result_sets_match_full_rehydrate_after_seeded_commits() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let initial = [("a", row(1)), ("b", row(2)), ("a", row(3)), ("c", row(4))];
    let mut seq = 1;
    for (title, row_uuid) in initial {
        let tx_id = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row_uuid, 10 + seq).cells(title_cells(title)),
            )
            .unwrap();
        accept_global(&mut core, tx_id, seq);
        seq += 1;
    }
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("title")))
        .validate(&schema())
        .unwrap();
    let bindings = ["a", "b", "c"]
        .into_iter()
        .map(|title| {
            shape
                .bind(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String(title.to_owned()),
                )]))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut peers = bindings
        .iter()
        .map(|binding| {
            let mut peer = PeerState::new();
            peer.rehydrate_query(&mut core, &shape, binding).unwrap();
            peer
        })
        .collect::<Vec<_>>();
    let whole_subscription = core.whole_table_subscription_key("todos").unwrap();
    let mut whole_table_link = PeerState::new();
    whole_table_link
        .current_rows_update(&mut core, "todos")
        .unwrap();

    let title_cycle = ["b", "c", "a", "b", "a", "c"];
    let mut current_titles = ["a", "b", "a", "c"];
    for step in 0..18 {
        let row_idx = step % 4;
        let row_uuid = row(row_idx as u8 + 1);
        let mut title = title_cycle[step % title_cycle.len()];
        if title == current_titles[row_idx] {
            title = title_cycle[(step + 1) % title_cycle.len()];
        }
        current_titles[row_idx] = title;
        let tx_id = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row_uuid, 100 + step as u64)
                    .cells(title_cells(title)),
            )
            .unwrap();
        accept_global(&mut core, tx_id, seq);
        seq += 1;

        for (peer, binding) in peers.iter_mut().zip(bindings.iter()) {
            peer.query_update(&mut core, &shape, binding).unwrap();
        }
        whole_table_link
            .current_rows_update(&mut core, "todos")
            .unwrap();
        for (peer, binding) in peers.iter().zip(bindings.iter()) {
            let mut fresh = PeerState::new();
            fresh.rehydrate_query(&mut core, &shape, binding).unwrap();
            let subscription = SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: Default::default(),
            };
            assert_eq!(
                row_result_set(peer, subscription),
                row_result_set(&fresh, subscription),
                "incremental result set diverged from full rehydrate at step {step}"
            );
        }
        let mut fresh_whole = PeerState::new();
        fresh_whole.current_rows_update(&mut core, "todos").unwrap();
        assert_eq!(
            row_result_set(&whole_table_link, whole_subscription),
            row_result_set(&fresh_whole, whole_subscription),
            "incremental whole-table result set diverged from full rehydrate at step {step}"
        );
    }
}
