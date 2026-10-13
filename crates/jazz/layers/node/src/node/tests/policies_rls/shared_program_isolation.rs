// Subscribers that share one prepared program must still each see only their
// own binding's rows: parameters, policy claims, aggregates, windows and
// reference closures must all stay separated per binding.

fn shared_program_opts() -> RegisterShapeOptions {
    RegisterShapeOptions {
        tier: DurabilityTier::Global,
        ..RegisterShapeOptions::default()
    }
}

fn shared_program_params(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn shared_program_added(update: &SyncMessage, table: &str) -> BTreeSet<RowUuid> {
    canonical_view_update_rows(update)
        .into_iter()
        .filter(|(t, _, _)| t.as_str() == table)
        .map(|(_, r, _)| r)
        .collect()
}

fn shared_program_removed(update: &SyncMessage, table: &str) -> BTreeSet<RowUuid> {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        supporting_rows, ..
    }) = update
    else {
        panic!("expected view update");
    };
    supporting_rows
        .removed_rows()
        .iter()
        .filter(|input| {
            input.version.layer == crate::protocol::ResultRowLayer::Content
                && input.version_table.as_str() == table
        })
        .map(|input| input.row)
        .collect()
}

/// Applies a view update (snapshot replaces, delta adds/removes) to a
/// per-subscription model of the delivered supporting rows, by table.
type SharedProgramState = BTreeMap<String, BTreeSet<RowUuid>>;

fn shared_program_apply(state: &mut SharedProgramState, update: &SyncMessage) {
    let SyncMessage::ViewUpdate(payload) = update else {
        panic!("expected view update");
    };
    if matches!(payload.supporting_rows, crate::protocol::SupportingRowsUpdate::Snapshot { .. }) {
        state.clear();
    }
    for input in payload.supporting_rows.removed_rows() {
        if input.version.layer == crate::protocol::ResultRowLayer::Content {
            state.entry(input.version_table.as_str().to_owned()).or_default().remove(&input.row);
        }
    }
    for (t, r, _) in canonical_view_update_rows(update) {
        state.entry(t.as_str().to_owned()).or_default().insert(r);
    }
}

fn shared_program_t(state: &SharedProgramState, table: &str) -> BTreeSet<RowUuid> {
    state.get(table).cloned().unwrap_or_default()
}

fn shared_program_sub(
    core: &mut NodeState,
    peer: &mut PeerState,
    shape: &ValidatedQuery,
    params: &BTreeMap<String, Value>,
) -> SyncMessage {
    let binding = shape.bind(params.clone()).unwrap();
    let up = peer.rehydrate_query_with_opts(core, shape, &binding, shared_program_opts())
        .unwrap();
    up
}

fn shared_program_upd(
    core: &mut NodeState,
    peer: &mut PeerState,
    shape: &ValidatedQuery,
    params: &BTreeMap<String, Value>,
) -> SyncMessage {
    let binding = shape.bind(params.clone()).unwrap();
    let up = peer.query_update(core, shape, &binding).unwrap();
    if let SyncMessage::ViewUpdate(payload) = &up {
        let kind = match &payload.supporting_rows {
            crate::protocol::SupportingRowsUpdate::Snapshot { .. } => "snapshot",
            _ => "delta",
        };
    }
    up
}

fn shared_program_forget(
    core: &mut NodeState,
    peer: &mut PeerState,
    shape: &ValidatedQuery,
    params: &BTreeMap<String, Value>,
) {
    let key = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: shape.bind(params.clone()).unwrap().binding_id(),
        read_view: shared_program_opts().read_view_key(),
    };
    assert!(peer.forget_subscription_with_node(core, key));
}

fn shared_program_public_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("author", PublicColumnType::Text)
                    .nullable_column("tag", PublicColumnType::Text)
                    .column("n", PublicColumnType::Integer)
                    .column("pinned", PublicColumnType::Boolean)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("notes")
                    .column("ownerId", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(
                        public_all_policies().with_select(public_claim_eq("ownerId", "user_id")),
                    ),
            ),
    )
}

struct SharedProgramMsg<'a> {
    chat: RowUuid,
    author: &'a str,
    tag: Option<&'a str>,
    n: i32,
    pinned: bool,
    t: u64,
}

fn shared_program_put_msg(core: &mut NodeState, row_id: RowUuid, tx: u64, m: SharedProgramMsg<'_>) {
    accept_global(
        core,
        MergeableCommit::new("messages", row_id, tx).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(m.chat.0)),
            ("author".to_owned(), Value::String(m.author.to_owned())),
            (
                "tag".to_owned(),
                Value::Nullable(m.tag.map(|t| Box::new(Value::String(t.to_owned())))),
            ),
            ("n".to_owned(), Value::I32(m.n)),
            ("pinned".to_owned(), Value::Bool(m.pinned)),
            ("createdAt".to_owned(), Value::U64(m.t)),
        ])),
    );
}

fn shared_program_msg(chat: RowUuid, author: &str, t: u64) -> SharedProgramMsg<'_> {
    SharedProgramMsg {
        chat,
        author,
        tag: None,
        n: 0,
        pinned: false,
        t,
    }
}

fn shared_program_seed_chats(core: &mut NodeState, chats: &[RowUuid]) {
    for (i, c) in chats.iter().enumerate() {
        accept_global(
            core,
            MergeableCommit::new("chats", *c, 5 + i as u64).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String(format!("chat {i}")),
            )])),
        );
    }
}

fn shared_program_shapes(core: &NodeState) -> usize {
    core.runtime_stats_for_test().active_prepared_shapes
}

/// Public table (policy admits everything), so only the param routing can
/// keep subscribers apart. Same user with two params on one peer, two users
/// with the same param, unsubscribe of one of two same-key subscribers, key
/// re-entry after every subscriber left.
#[test]
fn shared_program_param_window_is_per_binding() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2) = (row(0x11), row(0x12));
    shared_program_seed_chats(&mut core, &[c1, c2]);
    let (a100, a300, b200, b400) = (row(0x41), row(0x42), row(0x43), row(0x44));
    shared_program_put_msg(&mut core, a100, 20, shared_program_msg(c1, "a", 100));
    shared_program_put_msg(&mut core, a300, 21, shared_program_msg(c1, "a", 300));
    shared_program_put_msg(&mut core, b200, 22, shared_program_msg(c2, "a", 200));
    shared_program_put_msg(&mut core, b400, 23, shared_program_msg(c2, "a", 400));
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    for (offset, want1, want2) in [(0usize, a100, b200), (1, a300, b400)] {
        let shape = Query::from("messages")
            .filter(eq(col("chatId"), param("chat")))
            .order_by("createdAt", OrderDirection::Asc)
            .offset(offset)
            .limit(1)
            .validate(&core.catalogue.schema)
            .unwrap();
        core.drive_query_runtime().unwrap();
        let base = shared_program_shapes(&core);
        let mut u0 = PeerState::client_link(user(0xa0));
        let mut u1 = PeerState::client_link(user(0xa1));
        let up0 = shared_program_sub(&mut core, &mut u0, &shape, &p(c1));
        let first = shared_program_shapes(&core) - base;
        let up1 = shared_program_sub(&mut core, &mut u1, &shape, &p(c2));
        let total = shared_program_shapes(&core) - base;
        assert_eq!(shared_program_added(&up0, "messages"), BTreeSet::from([want1]));
        assert_eq!(shared_program_added(&up1, "messages"), BTreeSet::from([want2]));
        shared_program_forget(&mut core, &mut u0, &shape, &p(c1));
        shared_program_forget(&mut core, &mut u1, &shape, &p(c2));
    }

    // Live: LIMIT 1 page; an earlier c2 message must replace c2's page only.
    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .order_by("createdAt", OrderDirection::Asc)
        .limit(1)
        .validate(&core.catalogue.schema)
        .unwrap();
    let mut u0 = PeerState::client_link(user(0xa0));
    let mut u1 = PeerState::client_link(user(0xa1));
    shared_program_sub(&mut core, &mut u0, &shape, &p(c1));
    shared_program_sub(&mut core, &mut u1, &shape, &p(c2));
    let b050 = row(0x45);
    shared_program_put_msg(&mut core, b050, 30, shared_program_msg(c2, "a", 50));
    let up0 = shared_program_upd(&mut core, &mut u0, &shape, &p(c1));
    let up1 = shared_program_upd(&mut core, &mut u1, &shape, &p(c2));
    assert!(shared_program_added(&up0, "messages").is_empty() && shared_program_removed(&up0, "messages").is_empty(),
        "c1 page must not move: {:?}/{:?}", shared_program_added(&up0, "messages"), shared_program_removed(&up0, "messages"));
    assert_eq!(shared_program_added(&up1, "messages"), BTreeSet::from([b050]));
    assert_eq!(shared_program_removed(&up1, "messages"), BTreeSet::from([b200]));
}

/// count() over a param equality: the supporting rows shipped to each
/// subscriber must be only its own chat's rows.
#[test]
fn shared_program_param_count_supporting_rows_per_binding() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2) = (row(0x11), row(0x12));
    shared_program_seed_chats(&mut core, &[c1, c2]);
    let (a, b, b2) = (row(0x41), row(0x42), row(0x43));
    shared_program_put_msg(&mut core, a, 20, shared_program_msg(c1, "a", 1));
    shared_program_put_msg(&mut core, b, 21, shared_program_msg(c2, "a", 2));
    shared_program_put_msg(&mut core, b2, 22, shared_program_msg(c2, "a", 3));
    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .count()
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut u0 = PeerState::client_link(user(0xa0));
    let mut u1 = PeerState::client_link(user(0xa1));
    let up0 = shared_program_sub(&mut core, &mut u0, &shape, &p(c1));
    let first = shared_program_shapes(&core) - base;
    let up1 = shared_program_sub(&mut core, &mut u1, &shape, &p(c2));
    let total = shared_program_shapes(&core) - base;
    assert_eq!(shared_program_added(&up0, "messages"), BTreeSet::from([a]));
    assert_eq!(shared_program_added(&up1, "messages"), BTreeSet::from([b, b2]));
    let (mut s0, mut s1) = (SharedProgramState::new(), SharedProgramState::new());
    shared_program_apply(&mut s0, &up0);
    shared_program_apply(&mut s1, &up1);
    let c = row(0x44);
    shared_program_put_msg(&mut core, c, 30, shared_program_msg(c2, "a", 4));
    shared_program_apply(&mut s0, &shared_program_upd(&mut core, &mut u0, &shape, &p(c1)));
    shared_program_apply(&mut s1, &shared_program_upd(&mut core, &mut u1, &shape, &p(c2)));
    assert_eq!(shared_program_t(&s0, "messages"), BTreeSet::from([a]), "c1 count subscriber");
    assert_eq!(shared_program_t(&s1, "messages"), BTreeSet::from([b, b2, c]), "c2 count subscriber");
}

/// Two params in one conjunction (chat and author, both indexed) and an id
/// param with a forward include.
#[test]
fn shared_program_two_params_and_id_include() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2) = (row(0x11), row(0x12));
    shared_program_seed_chats(&mut core, &[c1, c2]);
    let (c1x, c1y, c2x, c2y) = (row(0x41), row(0x42), row(0x43), row(0x44));
    shared_program_put_msg(&mut core, c1x, 20, shared_program_msg(c1, "x", 1));
    shared_program_put_msg(&mut core, c1y, 21, shared_program_msg(c1, "y", 2));
    shared_program_put_msg(&mut core, c2x, 22, shared_program_msg(c2, "x", 3));
    shared_program_put_msg(&mut core, c2y, 23, shared_program_msg(c2, "y", 4));
    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .filter(eq(col("author"), param("author")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |c: RowUuid, a: &str| {
        shared_program_params(&[("chat", Value::Uuid(c.0)), ("author", Value::String(a.to_owned()))])
    };
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut peers: Vec<_> = (0..4).map(|i| PeerState::client_link(user(0xa0 + i))).collect();
    let cases = [(c1, "x", c1x), (c1, "y", c1y), (c2, "x", c2x), (c2, "y", c2y)];
    let mut states: Vec<SharedProgramState> = Vec::new();
    for (peer, (c, a, want)) in peers.iter_mut().zip(cases) {
        let mut st = SharedProgramState::new();
        shared_program_apply(&mut st, &shared_program_sub(&mut core, peer, &shape, &p(c, a)));
        assert_eq!(shared_program_t(&st, "messages"), BTreeSet::from([want]), "{a}");
        states.push(st);
    }
    // Move c1x to author y: leaves (c1,x), enters (c1,y).
    shared_program_put_msg(&mut core, c1x, 30, shared_program_msg(c1, "y", 1));
    let wants = [BTreeSet::new(), BTreeSet::from([c1x, c1y]), BTreeSet::from([c2x]), BTreeSet::from([c2y])];
    for (i, (peer, (c, a, _))) in peers.iter_mut().zip(cases).enumerate() {
        shared_program_apply(&mut states[i], &shared_program_upd(&mut core, peer, &shape, &p(c, a)));
        assert_eq!(shared_program_t(&states[i], "messages"), wants[i], "sub {i}");
    }

    let id_shape = Query::from("messages")
        .filter(eq(col("id"), param("id")))
        .include("chatId")
        .validate(&core.catalogue.schema)
        .unwrap();
    let pid = |r: RowUuid| shared_program_params(&[("id", Value::Uuid(r.0))]);
    let base = shared_program_shapes(&core);
    let mut a = PeerState::client_link(user(0xb0));
    let mut b = PeerState::client_link(user(0xb1));
    let upa = shared_program_sub(&mut core, &mut a, &id_shape, &pid(c1y));
    let first = shared_program_shapes(&core) - base;
    let upb = shared_program_sub(&mut core, &mut b, &id_shape, &pid(c2x));
    let (mut sa, mut sb) = (SharedProgramState::new(), SharedProgramState::new());
    shared_program_apply(&mut sa, &upa);
    shared_program_apply(&mut sb, &upb);
    assert_eq!(shared_program_added(&upa, "messages"), BTreeSet::from([c1y]));
    assert_eq!(shared_program_added(&upa, "chats"), BTreeSet::from([c1]));
    assert_eq!(shared_program_added(&upb, "messages"), BTreeSet::from([c2x]));
    assert_eq!(shared_program_added(&upb, "chats"), BTreeSet::from([c2]));
    // Re-parent c1y into c2: a's include must switch to c2, b unchanged.
    shared_program_put_msg(&mut core, c1y, 31, shared_program_msg(c2, "y", 2));
    shared_program_apply(&mut sa, &shared_program_upd(&mut core, &mut a, &id_shape, &pid(c1y)));
    shared_program_apply(&mut sb, &shared_program_upd(&mut core, &mut b, &id_shape, &pid(c2x)));
    assert_eq!((shared_program_t(&sa, "messages"), shared_program_t(&sa, "chats")), (BTreeSet::from([c1y]), BTreeSet::from([c2])), "a after re-parent");
    assert_eq!((shared_program_t(&sb, "messages"), shared_program_t(&sb, "chats")), (BTreeSet::from([c2x]), BTreeSet::from([c2])), "b after re-parent");
}

/// A nullable indexed column probed by a non-null text key: null and
/// empty-string rows never match; a row moving to null leaves.
#[test]
fn shared_program_param_naming_other_users_claim_reads_nothing() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let users = [user(0xa0), user(0xa1), user(0xa2)];
    let ids: Vec<String> = users.iter().map(|u| u.test_uuid().to_string()).collect();
    for u in &users {
        core.set_test_provider_claims(
            *u,
            BTreeMap::from([("user_id".to_owned(), Value::String(u.test_uuid().to_string()))]),
        );
    }
    let notes = [row(0x41), row(0x42), row(0x43)];
    for (i, n) in notes.iter().enumerate() {
        accept_global(
            &mut core,
            MergeableCommit::new("notes", *n, 20 + i as u64).cells(BTreeMap::from([
                ("ownerId".to_owned(), Value::String(ids[i].clone())),
                ("createdAt".to_owned(), Value::U64(i as u64)),
            ])),
        );
    }
    let shape = Query::from("notes")
        .filter(eq(col("ownerId"), param("owner")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |s: &str| shared_program_params(&[("owner", Value::String(s.to_owned()))]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut own0 = PeerState::client_link(users[0]);
    let mut own1 = PeerState::client_link(users[1]);
    let mut spy = PeerState::client_link(users[2]);
    assert_eq!(shared_program_added(&shared_program_sub(&mut core, &mut own0, &shape, &p(&ids[0])), "notes"), BTreeSet::from([notes[0]]));
    let first = shared_program_shapes(&core) - base;
    assert_eq!(shared_program_added(&shared_program_sub(&mut core, &mut own1, &shape, &p(&ids[1])), "notes"), BTreeSet::from([notes[1]]));
    // users[2] asks for users[0]'s notes: params match a live key, claim does not.
    assert!(shared_program_added(&shared_program_sub(&mut core, &mut spy, &shape, &p(&ids[0])), "notes").is_empty());
    let fresh = row(0x44);
    accept_global(
        &mut core,
        MergeableCommit::new("notes", fresh, 30).cells(BTreeMap::from([
            ("ownerId".to_owned(), Value::String(ids[0].clone())),
            ("createdAt".to_owned(), Value::U64(9)),
        ])),
    );
    assert_eq!(shared_program_added(&shared_program_upd(&mut core, &mut own0, &shape, &p(&ids[0])), "notes"), BTreeSet::from([fresh]));
    assert!(shared_program_added(&shared_program_upd(&mut core, &mut own1, &shape, &p(&ids[1])), "notes").is_empty());
    assert!(shared_program_added(&shared_program_upd(&mut core, &mut spy, &shape, &p(&ids[0])), "notes").is_empty());
}

/// Two different queries with the same param name and type share a binding
/// source; a subscriber of one must never receive the other's rows.
#[test]
fn shared_program_cross_shape_same_param_name() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2) = (row(0x11), row(0x12));
    shared_program_seed_chats(&mut core, &[c1, c2]);
    let m1 = row(0x41);
    shared_program_put_msg(&mut core, m1, 20, shared_program_msg(c1, "a", 1));
    let mem = row(0x51);
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", mem, 21).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(c1.0)),
            ("userId".to_owned(), Value::String("x".to_owned())),
        ])),
    );
    let q_msgs = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let q_mem = Query::from("chatMembers")
        .filter(eq(col("chatId"), param("chat")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    let mut x = PeerState::client_link(user(0xa0));
    let mut y = PeerState::client_link(user(0xa1));
    let upx = shared_program_sub(&mut core, &mut x, &q_mem, &p(c1));
    let upy = shared_program_sub(&mut core, &mut y, &q_msgs, &p(c2));
    assert_eq!(shared_program_added(&upx, "chatMembers"), BTreeSet::from([mem]));
    assert!(shared_program_added(&upx, "messages").is_empty());
    assert!(shared_program_added(&upy, "messages").is_empty() && shared_program_added(&upy, "chatMembers").is_empty());
    let m2 = row(0x42);
    shared_program_put_msg(&mut core, m2, 30, shared_program_msg(c1, "a", 2));
    let upx = shared_program_upd(&mut core, &mut x, &q_mem, &p(c1));
    let upy = shared_program_upd(&mut core, &mut y, &q_msgs, &p(c2));
    assert!(shared_program_added(&upx, "messages").is_empty(), "{:?}", canonical_view_update_rows(&upx));
    assert!(shared_program_added(&upy, "messages").is_empty(), "{:?}", canonical_view_update_rows(&upy));
}

/// Membership policy schema; one user is a member of every chat and holds
/// two subscriptions with different chat params: the policy admits all, so
/// only the param keeps them apart. Then membership of c1 is revoked.
#[test]
fn shared_program_member_of_all_chats_two_param_subs() {
    let (_dir, mut core) = open_node_with_schema(node(9), private_message_membership_schema());
    let reader = user(0xa0);
    let rid = reader.test_uuid().to_string();
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("user_id".to_owned(), Value::String(rid.clone()))]),
    );
    let chats = [row(0x11), row(0x12), row(0x13)];
    let mut msgs = Vec::new();
    for (i, c) in chats.iter().enumerate() {
        let i8 = i as u8;
        accept_global(
            &mut core,
            MergeableCommit::new("chats", *c, 10).cells(BTreeMap::from([
                ("isPublic".to_owned(), Value::Bool(false)),
                ("createdBy".to_owned(), Value::String(rid.clone())),
            ])),
        );
        accept_global(
            &mut core,
            MergeableCommit::new("chatMembers", row(0x20 + i8), 11).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(c.0)),
                ("userId".to_owned(), Value::String(rid.clone())),
            ])),
        );
        let m = row(0x30 + i8);
        accept_global(
            &mut core,
            MergeableCommit::new("messages", m, 12).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(c.0)),
                ("text".to_owned(), Value::String(format!("m{i}"))),
                ("createdAt".to_owned(), Value::U64(i as u64)),
            ])),
        );
        msgs.push(m);
    }
    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut peer = PeerState::client_link(reader);
    let up0 = shared_program_sub(&mut core, &mut peer, &shape, &p(chats[0]));
    let first = shared_program_shapes(&core) - base;
    let up1 = shared_program_sub(&mut core, &mut peer, &shape, &p(chats[1]));
    let mut other = PeerState::client_link(reader);
    let up2 = shared_program_sub(&mut core, &mut other, &shape, &p(chats[2]));
    assert_eq!(shared_program_added(&up0, "messages"), BTreeSet::from([msgs[0]]));
    assert_eq!(shared_program_added(&up1, "messages"), BTreeSet::from([msgs[1]]));
    assert_eq!(shared_program_added(&up2, "messages"), BTreeSet::from([msgs[2]]));
    // Revoke membership of chat 1.
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", row(0x21), 20).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chats[1].0)),
            ("userId".to_owned(), Value::String("someone-else".to_owned())),
        ])),
    );
    let up0 = shared_program_upd(&mut core, &mut peer, &shape, &p(chats[0]));
    let up1 = shared_program_upd(&mut core, &mut peer, &shape, &p(chats[1]));
    let up2 = shared_program_upd(&mut core, &mut other, &shape, &p(chats[2]));
    assert!(shared_program_removed(&up0, "messages").is_empty());
    assert_eq!(shared_program_removed(&up1, "messages"), BTreeSet::from([msgs[1]]));
    assert!(shared_program_removed(&up2, "messages").is_empty());
}

/// OR with a param branch, IN list of params, and a non-equality param
/// (n > $min): no lookup for these, but isolation must hold.
fn shared_program_private_chats_schema() -> JazzSchema {
    let member_exists = |outer_column: &str| {
        public_outer_exists(
            "chatMembers",
            "chatId",
            outer_column,
            [public_claim_eq("userId", "user_id")],
        )
    };
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_claim_eq("userId", "user_id"))),
            )
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(member_exists("id"))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(public_all_policies().with_select(member_exists("chatId"))),
            ),
    )
}

fn shared_program_seed_private(core: &mut NodeState, n: u8) -> (Vec<AuthorSubject>, Vec<RowUuid>, Vec<RowUuid>) {
    let readers: Vec<_> = (0..n).map(|i| user(0xa0 + i)).collect();
    let mut chats = Vec::new();
    let mut msgs = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        let i = i as u8;
        let rid = reader.test_uuid().to_string();
        core.set_test_provider_claims(
            *reader,
            BTreeMap::from([("user_id".to_owned(), Value::String(rid.clone()))]),
        );
        let chat = row(0x10 + i);
        accept_global(
            core,
            MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String(format!("secret title {i}")),
            )])),
        );
        accept_global(
            core,
            MergeableCommit::new("chatMembers", row(0x20 + i), 11).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("userId".to_owned(), Value::String(rid)),
            ])),
        );
        let m = row(0x30 + i);
        accept_global(
            core,
            MergeableCommit::new("messages", m, 12).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String(format!("hello {i}"))),
                ("createdAt".to_owned(), Value::U64(u64::from(i))),
            ])),
        );
        chats.push(chat);
        msgs.push(m);
    }
    (readers, chats, msgs)
}

/// Each reader is the only member of its own private chat and subscribes to
/// `messages where chatId = $chat` for that chat. The implicit reference
/// closure ships the message's chat row; no reader may receive another
/// reader's (unreadable) chat row.
#[test]
fn shared_program_reference_closure_chat_rows_stay_per_binding() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_private_chats_schema());
    let (readers, chats, msgs) = shared_program_seed_private(&mut core, 4);
    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut peers = Vec::new();
    let mut leaks = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        let mut peer = PeerState::client_link(*reader);
        let up = shared_program_sub(&mut core, &mut peer, &shape, &p(chats[i]));
        assert_eq!(shared_program_added(&up, "messages"), BTreeSet::from([msgs[i]]));
        let got = shared_program_added(&up, "chats");
        if !got.is_subset(&BTreeSet::from([chats[i]])) {
            leaks.push((i, "initial", got.clone()));
        }
        peers.push(peer);
    }
    // A new message in chat 0 and a chat 1 title edit.
    accept_global(
        &mut core,
        MergeableCommit::new("messages", row(0x50), 20).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chats[0].0)),
            ("text".to_owned(), Value::String("fresh".to_owned())),
            ("createdAt".to_owned(), Value::U64(99)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", chats[1], 21).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("renamed secret".to_owned()),
        )])),
    );
    for (i, peer) in peers.iter_mut().enumerate() {
        let up = shared_program_upd(&mut core, peer, &shape, &p(chats[i]));
        let got = shared_program_added(&up, "chats");
        if !got.is_subset(&BTreeSet::from([chats[i]])) {
            leaks.push((i, "update", got));
        }
        let m = shared_program_added(&up, "messages");
        if !m.is_subset(&BTreeSet::from([msgs[i], row(0x50)])) || (i != 0 && m.contains(&row(0x50))) {
            leaks.push((i, "update-msgs", m));
        }
    }
    assert!(leaks.is_empty(), "foreign chat rows delivered: {leaks:?}");
}

/// Same fixture, no query params: users share one graph by claim routing
/// only.
#[test]
fn shared_program_reference_closure_chat_rows_no_params() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_private_chats_schema());
    let (readers, chats, msgs) = shared_program_seed_private(&mut core, 4);
    let shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = shared_program_params(&[]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut peers = Vec::new();
    let mut leaks = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        let mut peer = PeerState::client_link(*reader);
        let up = shared_program_sub(&mut core, &mut peer, &shape, &p);
        assert_eq!(shared_program_added(&up, "messages"), BTreeSet::from([msgs[i]]));
        let got = shared_program_added(&up, "chats");
        if !got.is_subset(&BTreeSet::from([chats[i]])) {
            leaks.push((i, "initial", got.clone()));
        }
        peers.push(peer);
    }
    accept_global(
        &mut core,
        MergeableCommit::new("chats", chats[1], 21).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("renamed secret".to_owned()),
        )])),
    );
    for (i, peer) in peers.iter_mut().enumerate() {
        let up = shared_program_upd(&mut core, peer, &shape, &p);
        let got = shared_program_added(&up, "chats");
        if !got.is_subset(&BTreeSet::from([chats[i]])) {
            leaks.push((i, "update", got));
        }
    }
    assert!(leaks.is_empty(), "foreign chat rows delivered: {leaks:?}");
}

/// Messages are public, chats are member-only. The root carries no claim
/// route, so the implicit chat closure takes the route-less semi-join path.
fn shared_program_public_msgs_private_chats_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_claim_eq("userId", "user_id"))),
            )
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_outer_exists(
                        "chatMembers",
                        "chatId",
                        "id",
                        [public_claim_eq("userId", "user_id")],
                    ))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(public_all_policies()),
            ),
    )
}

fn shared_program_closure_leak_check(with_param: bool) {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_msgs_private_chats_schema());
    let (readers, chats, msgs) = shared_program_seed_private(&mut core, 3);
    let shape = if with_param {
        Query::from("messages").filter(eq(col("chatId"), param("chat")))
    } else {
        Query::from("messages")
    }
    .validate(&core.catalogue.schema)
    .unwrap();
    let p = |c: RowUuid| {
        if with_param {
            shared_program_params(&[("chat", Value::Uuid(c.0))])
        } else {
            shared_program_params(&[])
        }
    };
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut leaks = Vec::new();
    let mut peers = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        let mut peer = PeerState::client_link(*reader);
        let up = shared_program_sub(&mut core, &mut peer, &shape, &p(chats[i]));
        if with_param {
            assert_eq!(shared_program_added(&up, "messages"), BTreeSet::from([msgs[i]]));
        }
        let got = shared_program_added(&up, "chats");
        if !got.is_subset(&BTreeSet::from([chats[i]])) {
            leaks.push((i, "initial", got));
        }
        peers.push(peer);
    }
    accept_global(
        &mut core,
        MergeableCommit::new("chats", chats[1], 21).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("renamed secret".to_owned()),
        )])),
    );
    for (i, peer) in peers.iter_mut().enumerate() {
        let up = shared_program_upd(&mut core, peer, &shape, &p(chats[i]));
        let got = shared_program_added(&up, "chats");
        if !got.is_subset(&BTreeSet::from([chats[i]])) {
            leaks.push((i, "update", got));
        }
    }
    assert!(leaks.is_empty(), "unreadable chat rows delivered: {leaks:?}");
}

#[test]
fn shared_program_public_msgs_private_chats_closure_with_param() {
    shared_program_closure_leak_check(true);
}

#[test]
fn shared_program_public_msgs_private_chats_closure_no_param() {
    shared_program_closure_leak_check(false);
}

/// A count over member-only rows must count only the reader's own chats,
/// whether or not the query carries a parameter.
fn shared_program_private_count(with_param: bool) {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_private_chats_schema());
    let (readers, chats, msgs) = shared_program_seed_private(&mut core, 3);
    let shape = if with_param {
        Query::from("messages").filter(eq(col("chatId"), param("chat"))).count()
    } else {
        Query::from("messages").count()
    }
    .validate(&core.catalogue.schema)
    .unwrap();
    let p = |c: RowUuid| if with_param { shared_program_params(&[("chat", Value::Uuid(c.0))]) } else { shared_program_params(&[]) };
    let base = shared_program_shapes(&core);
    let mut peers: Vec<_> = readers.iter().map(|r| PeerState::client_link(*r)).collect();
    let mut states = Vec::new();
    for (i, peer) in peers.iter_mut().enumerate() {
        let mut st = SharedProgramState::new();
        shared_program_apply(&mut st, &shared_program_sub(&mut core, peer, &shape, &p(chats[i])));
        assert_eq!(shared_program_t(&st, "messages"), BTreeSet::from([msgs[i]]), "initial {i}");
        states.push(st);
    }
    let fresh = row(0x60);
    accept_global(
        &mut core,
        MergeableCommit::new("messages", fresh, 40).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chats[1].0)),
            ("text".to_owned(), Value::String("secret".to_owned())),
            ("createdAt".to_owned(), Value::U64(50)),
        ])),
    );
    let mut leaks = Vec::new();
    for (i, peer) in peers.iter_mut().enumerate() {
        shared_program_apply(&mut states[i], &shared_program_upd(&mut core, peer, &shape, &p(chats[i])));
        let mut allowed = BTreeSet::from([msgs[i]]);
        if i == 1 { allowed.insert(fresh); }
        let got = shared_program_t(&states[i], "messages");
        if !got.is_subset(&allowed) { leaks.push((i, got)); }
    }
    assert!(leaks.is_empty(), "foreign member-only messages delivered: {leaks:?}");
}

#[test]
fn shared_program_private_count_param() {
    shared_program_private_count(true);
}

/// Same without params, where readers share one program by claim routing.
#[test]
fn shared_program_private_count_without_params() {
    shared_program_private_count(false);
}

// Parameter sharing: subscribers whose parameters differ share one program.

/// Public table (policy admits everything), so only the param routing can
/// keep subscribers apart. Same user with two params on one peer, two users
/// with the same param, unsubscribe of one of two same-key subscribers, key
/// re-entry after every subscriber left.
#[test]
fn shared_program_public_param_eq_routes_each_binding() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2, c3) = (row(0x11), row(0x12), row(0x13));
    shared_program_seed_chats(&mut core, &[c1, c2, c3]);
    // Model: message -> chat.
    let mut model = BTreeMap::<RowUuid, RowUuid>::new();
    let mut put = |core: &mut NodeState, model: &mut BTreeMap<RowUuid, RowUuid>, m: RowUuid, c: RowUuid, tx: u64| {
        shared_program_put_msg(core, m, tx, shared_program_msg(c, "a", tx));
        model.insert(m, c);
    };
    put(&mut core, &mut model, row(0x31), c1, 20);
    put(&mut core, &mut model, row(0x32), c1, 21);
    put(&mut core, &mut model, row(0x33), c2, 22);
    put(&mut core, &mut model, row(0x34), c3, 23);
    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut peers: Vec<PeerState> = (0..4).map(|i| PeerState::client_link(user(0xa0 + i))).collect();
    // Live subscriptions: (peer index, chat, delivered state).
    let mut subs: Vec<(usize, RowUuid, SharedProgramState)> = Vec::new();
    let mut chat_overdelivery = Vec::new();
    let want = |model: &BTreeMap<RowUuid, RowUuid>, c: RowUuid| {
        model.iter().filter(|(_, mc)| **mc == c).map(|(m, _)| *m).collect::<BTreeSet<_>>()
    };
    let mut first = None;
    for (pi, c) in [(0usize, c1), (0, c2), (1, c1), (2, c3)] {
        let mut st = SharedProgramState::new();
        shared_program_apply(&mut st, &shared_program_sub(&mut core, &mut peers[pi], &shape, &p(c)));
        assert_eq!(shared_program_t(&st, "messages"), want(&model, c), "initial peer {pi} chat {c:?}");
        if !shared_program_t(&st, "chats").is_subset(&BTreeSet::from([c])) {
            chat_overdelivery.push(("initial", pi, c, shared_program_t(&st, "chats")));
        }
        subs.push((pi, c, st));
        first.get_or_insert(shared_program_shapes(&core) - base);
    }
    let total = shared_program_shapes(&core) - base;
    if std::env::var_os("SharedProgram_OFF").is_none() { assert_eq!(first, Some(total), "the param shapes must share one prepared program"); }

    macro_rules! check_all {
        ($step:expr) => {
            for (pi, c, st) in subs.iter_mut() {
                shared_program_apply(st, &shared_program_upd(&mut core, &mut peers[*pi], &shape, &p(*c)));
                assert_eq!(shared_program_t(st, "messages"), want(&model, *c), "{} peer {} chat {:?}", $step, pi, c);
                if !shared_program_t(st, "chats").is_subset(&BTreeSet::from([*c])) {
                    chat_overdelivery.push(($step, *pi, *c, shared_program_t(st, "chats")));
                }
            }
        };
    }
    put(&mut core, &mut model, row(0x35), c2, 30);
    check_all!("insert c2");
    put(&mut core, &mut model, row(0x31), c3, 31);
    check_all!("move c1->c3");
    // peer 1 (c1) leaves; peer 0 (c1) remains.
    let (pi, c, _) = subs.remove(2);
    shared_program_forget(&mut core, &mut peers[pi], &shape, &p(c));
    put(&mut core, &mut model, row(0x36), c1, 32);
    check_all!("insert c1 after one c1 sub left");
    // the last c1 sub leaves; c1 gets a row; a new c1 subscriber re-probes.
    let (pi, c, _) = subs.remove(0);
    shared_program_forget(&mut core, &mut peers[pi], &shape, &p(c));
    put(&mut core, &mut model, row(0x37), c1, 33);
    check_all!("insert c1 with no c1 sub");
    let mut st = SharedProgramState::new();
    shared_program_apply(&mut st, &shared_program_sub(&mut core, &mut peers[3], &shape, &p(c1)));
    assert_eq!(shared_program_t(&st, "messages"), want(&model, c1), "re-entered c1 key");
    subs.push((3, c1, st));
    put(&mut core, &mut model, row(0x32), c2, 34);
    check_all!("move c1->c2 after re-entry");

    for (pi, c, _) in subs.drain(..) {
        shared_program_forget(&mut core, &mut peers[pi], &shape, &p(c));
    }
    core.drive_query_runtime().unwrap();
    assert_eq!(shared_program_shapes(&core), base);
    assert!(chat_overdelivery.is_empty(), "foreign chats rows delivered (closure): {chat_overdelivery:?}");
}

/// A nullable indexed column probed by a non-null text key: null and
/// empty-string rows never match; a row moving to null leaves.
#[test]
fn shared_program_nullable_indexed_param_column() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let c1 = row(0x11);
    shared_program_seed_chats(&mut core, &[c1]);
    let (tx_, ty, tnull, tempty) = (row(0x41), row(0x42), row(0x43), row(0x44));
    let put = |core: &mut NodeState, r: RowUuid, tx: u64, tag: Option<&str>| {
        shared_program_put_msg(core, r, tx, SharedProgramMsg { tag, ..shared_program_msg(c1, "a", tx) });
    };
    put(&mut core, tx_, 20, Some("x"));
    put(&mut core, ty, 21, Some("y"));
    put(&mut core, tnull, 22, None);
    put(&mut core, tempty, 23, Some(""));
    let shape = Query::from("messages")
        .filter(eq(col("tag"), param("tag")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let p = |t: &str| shared_program_params(&[("tag", Value::Nullable(Some(Box::new(Value::String(t.to_owned())))))]);
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut ux = PeerState::client_link(user(0xa0));
    let mut uy = PeerState::client_link(user(0xa1));
    let mut ue = PeerState::client_link(user(0xa2));
    assert_eq!(shared_program_added(&shared_program_sub(&mut core, &mut ux, &shape, &p("x")), "messages"), BTreeSet::from([tx_]));
    let first = shared_program_shapes(&core) - base;
    assert_eq!(shared_program_added(&shared_program_sub(&mut core, &mut uy, &shape, &p("y")), "messages"), BTreeSet::from([ty]));
    assert_eq!(shared_program_added(&shared_program_sub(&mut core, &mut ue, &shape, &p("")), "messages"), BTreeSet::from([tempty]));
    // Null param, if the API accepts one: must not read null rows of other subscribers' keys.
    let null_params = shared_program_params(&[("tag", Value::Nullable(None))]);
    match shape.bind(null_params.clone()) {
        Ok(_) => {
            let mut un = PeerState::client_link(user(0xa3));
            let up = shared_program_sub(&mut core, &mut un, &shape, &null_params);
            assert!(!shared_program_added(&up, "messages").contains(&tx_) && !shared_program_added(&up, "messages").contains(&ty));
        }
        Err(_) => {}
    }
    // x -> null, y -> x.
    put(&mut core, tx_, 30, None);
    put(&mut core, ty, 31, Some("x"));
    let upx = shared_program_upd(&mut core, &mut ux, &shape, &p("x"));
    let upy = shared_program_upd(&mut core, &mut uy, &shape, &p("y"));
    let upe = shared_program_upd(&mut core, &mut ue, &shape, &p(""));
    assert_eq!((shared_program_added(&upx, "messages"), shared_program_removed(&upx, "messages")), (BTreeSet::from([ty]), BTreeSet::from([tx_])));
    assert_eq!((shared_program_added(&upy, "messages"), shared_program_removed(&upy, "messages")), (BTreeSet::new(), BTreeSet::from([ty])));
    assert!(shared_program_added(&upe, "messages").is_empty() && shared_program_removed(&upe, "messages").is_empty());
}

/// OR with a param branch, IN list of params, and a non-equality param
/// (n > $min): no lookup for these, but isolation must hold.
#[test]
fn shared_program_or_in_and_range_params() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2, c3) = (row(0x11), row(0x12), row(0x13));
    shared_program_seed_chats(&mut core, &[c1, c2, c3]);
    let (m1, m2, m3p) = (row(0x41), row(0x42), row(0x43));
    shared_program_put_msg(&mut core, m1, 20, SharedProgramMsg { n: 1, ..shared_program_msg(c1, "a", 1) });
    shared_program_put_msg(&mut core, m2, 21, SharedProgramMsg { n: 5, ..shared_program_msg(c2, "a", 2) });
    shared_program_put_msg(&mut core, m3p, 22, SharedProgramMsg { n: 9, pinned: true, ..shared_program_msg(c3, "a", 3) });
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);

    let int_shape = Query::from("messages")
        .filter(eq(col("n"), param("n")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let pn = |n: i32| shared_program_params(&[("n", Value::I32(n))]);
    let base = shared_program_shapes(&core);
    let mut a = PeerState::client_link(user(0xd0));
    let mut b = PeerState::client_link(user(0xd1));
    let ua = shared_program_sub(&mut core, &mut a, &int_shape, &pn(1));
    let first = shared_program_shapes(&core) - base;
    let ub = shared_program_sub(&mut core, &mut b, &int_shape, &pn(9));
    assert_eq!(shared_program_added(&ua, "messages"), BTreeSet::from([m1]));
    assert_eq!(shared_program_added(&ub, "messages"), BTreeSet::from([m3p]));
    let range_shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat")))
        .filter(crate::query::gt(col("n"), param("min")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let pr = |c: RowUuid, min: i32| shared_program_params(&[("chat", Value::Uuid(c.0)), ("min", Value::I32(min))]);
    let base = shared_program_shapes(&core);
    let mut a = PeerState::client_link(user(0xc0));
    let mut b = PeerState::client_link(user(0xc1));
    let mut c = PeerState::client_link(user(0xc2));
    let ua = shared_program_sub(&mut core, &mut a, &range_shape, &pr(c1, 0));
    let ub = shared_program_sub(&mut core, &mut b, &range_shape, &pr(c2, 7));
    let uc = shared_program_sub(&mut core, &mut c, &range_shape, &pr(c2, 0));
    assert_eq!(shared_program_added(&ua, "messages"), BTreeSet::from([m1]));
    assert!(shared_program_added(&ub, "messages").is_empty());
    assert_eq!(shared_program_added(&uc, "messages"), BTreeSet::from([m2]));

    let in_shape = Query::from("messages")
        .filter(crate::query::in_list(col("chatId"), [param("x"), param("y")]))
        .validate(&core.catalogue.schema)
        .unwrap();
    let pin = |x: RowUuid, y: RowUuid| shared_program_params(&[("x", Value::Uuid(x.0)), ("y", Value::Uuid(y.0))]);
    let base = shared_program_shapes(&core);
    let mut a = PeerState::client_link(user(0xb0));
    let mut b = PeerState::client_link(user(0xb1));
    let ua = shared_program_sub(&mut core, &mut a, &in_shape, &pin(c1, c2));
    let ub = shared_program_sub(&mut core, &mut b, &in_shape, &pin(c3, c3));
    assert_eq!(shared_program_added(&ua, "messages"), BTreeSet::from([m1, m2]));
    assert_eq!(shared_program_added(&ub, "messages"), BTreeSet::from([m3p]));
}

/// OR with a param branch: the literal branch reaches every subscriber, the
/// param branch only its own.
#[test]
fn shared_program_or_param_branch() {
    let (_dir, mut core) = open_node_with_schema(node(9), shared_program_public_schema());
    let (c1, c2, c3) = (row(0x11), row(0x12), row(0x13));
    shared_program_seed_chats(&mut core, &[c1, c2, c3]);
    let (m1, m2, m3p) = (row(0x41), row(0x42), row(0x43));
    shared_program_put_msg(&mut core, m1, 20, SharedProgramMsg { n: 1, ..shared_program_msg(c1, "a", 1) });
    shared_program_put_msg(&mut core, m2, 21, SharedProgramMsg { n: 5, ..shared_program_msg(c2, "a", 2) });
    shared_program_put_msg(&mut core, m3p, 22, SharedProgramMsg { n: 9, pinned: true, ..shared_program_msg(c3, "a", 3) });
    let p = |c: RowUuid| shared_program_params(&[("chat", Value::Uuid(c.0))]);
    let or_shape = Query::from("messages")
        .filter(crate::query::any_of([eq(col("chatId"), param("chat")), eq(col("pinned"), lit(true))]))
        .validate(&core.catalogue.schema)
        .unwrap();
    let base = shared_program_shapes(&core);
    let mut a = PeerState::client_link(user(0xa0));
    let mut b = PeerState::client_link(user(0xa1));
    let ua = shared_program_sub(&mut core, &mut a, &or_shape, &p(c1));
    let ub = shared_program_sub(&mut core, &mut b, &or_shape, &p(c2));
    assert_eq!(shared_program_added(&ua, "messages"), BTreeSet::from([m1, m3p]));
    assert_eq!(shared_program_added(&ub, "messages"), BTreeSet::from([m2, m3p]));

}

// Claim-only sharing: one prepared program per policy shape, each reader
// still confined to the rows its own claims authorize.

fn shared_claims_set_claim(core: &mut NodeState, reader: AuthorSubject) {
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(reader.test_uuid().to_string()),
        )]),
    );
}

/// Subscribes every reader to `shape` and returns, per reader, the rows by
/// table and the number of prepared shapes added after the first reader.
fn shared_claims_subscribe_all(
    core: &mut NodeState,
    readers: &[AuthorSubject],
    shape: &ValidatedQuery,
) -> (Vec<BTreeMap<String, BTreeSet<RowUuid>>>, usize, usize, Vec<PeerState>) {
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        ..RegisterShapeOptions::default()
    };
    core.drive_query_runtime().unwrap();
    let baseline = core.runtime_stats_for_test();
    let mut out = Vec::new();
    let mut peers = Vec::new();
    let mut after_first = None;
    for reader in readers {
        let mut peer = PeerState::client_link(*reader);
        let update = peer
            .rehydrate_query_with_opts(core, shape, &binding, opts.clone())
            .unwrap();
        let mut by_table = BTreeMap::<String, BTreeSet<RowUuid>>::new();
        for (table, row_uuid, _) in canonical_view_update_rows(&update) {
            by_table.entry(table.as_str().to_owned()).or_default().insert(row_uuid);
        }
        out.push(by_table);
        peers.push(peer);
        after_first.get_or_insert_with(|| core.runtime_stats_for_test());
    }
    let after_first = after_first.unwrap();
    let after_all = core.runtime_stats_for_test();
    (
        out,
        after_first.active_prepared_shapes - baseline.active_prepared_shapes,
        after_all.active_prepared_shapes - after_first.active_prepared_shapes,
        peers,
    )
}

/// Direct claim equality under an ordered LIMIT 1 window: the window must be
/// per binding, not computed over the union of every user's rows.
#[test]
fn shared_claims_claim_eq_policy_under_limit_window_is_per_binding() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("notes")
                .column("ownerId", PublicColumnType::Text)
                .column("createdAt", PublicColumnType::Timestamp)
                .policies(public_all_policies().with_select(public_claim_eq("ownerId", "user_id"))),
        ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1), user(0xa2)];
    let mut own = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        shared_claims_set_claim(&mut core, *reader);
        let i = i as u8;
        // Reader i owns rows at t = 100 - 10*i (earliest for the last reader) and 500.
        let early = row(0x40 + i);
        let late = row(0x50 + i);
        for (r, t) in [(early, 100 - 10 * u64::from(i)), (late, 500)] {
            accept_global(
                &mut core,
                MergeableCommit::new("notes", r, 10 + u64::from(i)).cells(BTreeMap::from([
                    ("ownerId".to_owned(), Value::String(reader.test_uuid().to_string())),
                    ("createdAt".to_owned(), Value::U64(t)),
                ])),
            );
        }
        own.push(early);
    }
    let shape = Query::from("notes")
        .order_by("createdAt", OrderDirection::Asc)
        .limit(1)
        .validate(&core.catalogue.schema)
        .unwrap();
    let (rows, first, later, _peers) = shared_claims_subscribe_all(&mut core, &readers, &shape);
    for (i, by_table) in rows.iter().enumerate() {
        assert_eq!(
            by_table.get("notes").cloned().unwrap_or_default(),
            BTreeSet::from([own[i]]),
            "reader {i} must see exactly its own earliest note"
        );
    }
}

/// OR policy where only one branch reads a claim, under LIMIT 2.
#[test]
fn shared_claims_or_policy_with_one_claim_branch_under_window() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("notes")
                .column("ownerId", PublicColumnType::Text)
                .column("isPublic", PublicColumnType::Boolean)
                .column("createdAt", PublicColumnType::Timestamp)
                .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                    public_claim_eq("ownerId", "user_id"),
                    public_literal_eq("isPublic", PublicValue::Boolean(true)),
                ]))),
        ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1)];
    for r in &readers {
        shared_claims_set_claim(&mut core, *r);
    }
    let put = |core: &mut NodeState, r: RowUuid, owner: &str, public: bool, t: u64, tx: u64| {
        accept_global(
            core,
            MergeableCommit::new("notes", r, tx).cells(BTreeMap::from([
                ("ownerId".to_owned(), Value::String(owner.to_owned())),
                ("isPublic".to_owned(), Value::Bool(public)),
                ("createdAt".to_owned(), Value::U64(t)),
            ])),
        );
    };
    let a = readers[0].test_uuid().to_string();
    let b = readers[1].test_uuid().to_string();
    put(&mut core, row(0x41), &a, false, 20, 10);
    put(&mut core, row(0x42), &b, false, 10, 11);
    put(&mut core, row(0x43), "nobody", true, 30, 12);
    let shape = Query::from("notes")
        .order_by("createdAt", OrderDirection::Asc)
        .limit(2)
        .validate(&core.catalogue.schema)
        .unwrap();
    let (rows, first, later, _peers) = shared_claims_subscribe_all(&mut core, &readers, &shape);
    assert_eq!(rows[0].get("notes").cloned().unwrap_or_default(), BTreeSet::from([row(0x41), row(0x43)]));
    assert_eq!(rows[1].get("notes").cloned().unwrap_or_default(), BTreeSet::from([row(0x42), row(0x43)]));
}

/// Default (inner) include of a reference whose target table is claim-routed.
#[test]
fn shared_claims_inner_include_of_claim_routed_target_stays_per_binding() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("projects")
                    .column("ownerId", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_claim_eq("ownerId", "user_id"))),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .fk_column("project", "projects")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1)];
    let mut todos = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        shared_claims_set_claim(&mut core, *reader);
        let i = i as u8;
        let project = row(0x10 + i);
        let todo = row(0x30 + i);
        accept_global(
            &mut core,
            MergeableCommit::new("projects", project, 10 + u64::from(i)).cells(BTreeMap::from([(
                "ownerId".to_owned(),
                Value::String(reader.test_uuid().to_string()),
            )])),
        );
        accept_global(
            &mut core,
            MergeableCommit::new("todos", todo, 20 + u64::from(i)).cells(BTreeMap::from([
                ("project".to_owned(), Value::Uuid(project.0)),
                ("title".to_owned(), Value::String(format!("t{i}"))),
            ])),
        );
        todos.push((project, todo));
    }
    let shape = Query::from("todos")
        .include("project")
        .validate(&core.catalogue.schema)
        .unwrap();
    let (rows, first, later, _peers) = shared_claims_subscribe_all(&mut core, &readers, &shape);
    for (i, by_table) in rows.iter().enumerate() {
        let (project, todo) = todos[i];
        assert_eq!(by_table.get("todos").cloned().unwrap_or_default(), BTreeSet::from([todo]), "reader {i} todos");
        assert_eq!(by_table.get("projects").cloned().unwrap_or_default(), BTreeSet::from([project]), "reader {i} projects");
    }
}

/// Nested EXISTS: messages visible if a chat member row matches the claim,
/// and the member row's chat is created by the claim holder (two claim
/// equalities at different nesting depths).
#[test]
fn shared_claims_two_level_exists_with_claims_at_each_level() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("createdBy", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_outer_exists(
                        "chatMembers",
                        "chatId",
                        "chatId",
                        [
                            public_claim_eq("userId", "user_id"),
                            public_outer_exists(
                                "chats",
                                "id",
                                "chatId",
                                [public_claim_eq("createdBy", "user_id")],
                            ),
                        ],
                    ))),
            ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1)];
    for r in &readers {
        shared_claims_set_claim(&mut core, *r);
    }
    let a = readers[0].test_uuid().to_string();
    let b = readers[1].test_uuid().to_string();
    // chat X created by A, both A and B are members. Only A satisfies both levels.
    let chat = row(0x10);
    accept_global(&mut core, MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([(
        "createdBy".to_owned(), Value::String(a.clone()))])));
    for (i, who) in [a.clone(), b.clone()].into_iter().enumerate() {
        accept_global(&mut core, MergeableCommit::new("chatMembers", row(0x20 + i as u8), 11 + i as u64).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(who)),
        ])));
    }
    let msg = row(0x30);
    accept_global(&mut core, MergeableCommit::new("messages", msg, 20).cells(BTreeMap::from([
        ("chatId".to_owned(), Value::Uuid(chat.0)),
        ("text".to_owned(), Value::String("hi".to_owned())),
    ])));
    // Subscribe B first so B's binding exists when A's graph is shared.
    let shape = Query::from("messages").validate(&core.catalogue.schema).unwrap();
    let (rows, first, later, _peers) =
        shared_claims_subscribe_all(&mut core, &[readers[1], readers[0]], &shape);
    assert!(rows[0].get("messages").is_none_or(|m| m.is_empty()), "B is a member but did not create the chat");
    assert_eq!(rows[1].get("messages").cloned().unwrap_or_default(), BTreeSet::from([msg]));
}

fn shared_claims_rows(update: &SyncMessage, table: &str) -> BTreeSet<RowUuid> {
    canonical_view_update_rows(update)
        .into_iter()
        .filter(|(t, _, _)| t.as_str() == table)
        .map(|(_, r, _)| r)
        .collect()
}

/// Two-hop INHERITS (parts -> files -> folders) with a claim-equality leaf,
/// then a live re-parent of Bob's file into Alice's folder.
#[test]
fn shared_claims_two_hop_inherits_shared_and_reparent() {
    let inherits = |col: &str| PublicPolicyExpr::Inherits {
        operation: PublicOperation::Select,
        via_column: col.to_owned(),
        max_depth: None,
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("folders")
                    .column("ownerId", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_claim_eq("ownerId", "user_id"))),
            )
            .table(
                PublicTableSchemaBuilder::new("files")
                    .fk_column("folder", "folders")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(inherits("folder"))),
            )
            .table(
                PublicTableSchemaBuilder::new("parts")
                    .fk_column("file", "files")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(inherits("file"))),
            ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1), user(0xa2)];
    let mut fixture = Vec::new();
    for (i, reader) in readers.iter().enumerate() {
        shared_claims_set_claim(&mut core, *reader);
        let i = i as u8;
        let (folder, file, part) = (row(0x10 + i), row(0x20 + i), row(0x30 + i));
        accept_global(&mut core, MergeableCommit::new("folders", folder, 10 + u64::from(i)).cells(BTreeMap::from([
            ("ownerId".to_owned(), Value::String(reader.test_uuid().to_string())),
        ])));
        accept_global(&mut core, MergeableCommit::new("files", file, 20 + u64::from(i)).cells(BTreeMap::from([
            ("folder".to_owned(), Value::Uuid(folder.0)),
            ("title".to_owned(), Value::String(format!("f{i}"))),
        ])));
        accept_global(&mut core, MergeableCommit::new("parts", part, 30 + u64::from(i)).cells(BTreeMap::from([
            ("file".to_owned(), Value::Uuid(file.0)),
            ("title".to_owned(), Value::String(format!("p{i}"))),
        ])));
        fixture.push((folder, file, part));
    }
    let shape = Query::from("parts").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (rows, first, later, mut peers) = shared_claims_subscribe_all(&mut core, &readers, &shape);
    for (i, by_table) in rows.iter().enumerate() {
        assert_eq!(by_table.get("parts").cloned().unwrap_or_default(), BTreeSet::from([fixture[i].2]), "reader {i}");
    }
    // Re-parent reader 1's file into reader 0's folder.
    accept_global(&mut core, MergeableCommit::new("files", fixture[1].1, 50).cells(BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(fixture[0].0.0)),
        ("title".to_owned(), Value::String("f1".to_owned())),
    ])));
    let mut updates = Vec::new();
    for peer in peers.iter_mut() {
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        updates.push(format!("{update:?}"));
    }
    let p1 = format!("{:?}", fixture[1].2);
    assert!(updates[0].contains(&p1), "reader 0 must gain the moved part");
    assert!(!updates[2].contains(&p1), "reader 2 must not see the moved part");
}

/// Live window update: a new earliest row for reader 2 must not shift
/// reader 0's LIMIT 1 page in the shared graph.
#[test]
fn shared_claims_claim_eq_window_live_insert_stays_per_binding() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("notes")
                .column("ownerId", PublicColumnType::Text)
                .column("createdAt", PublicColumnType::Timestamp)
                .policies(public_all_policies().with_select(public_claim_eq("ownerId", "user_id"))),
        ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1)];
    for (i, reader) in readers.iter().enumerate() {
        shared_claims_set_claim(&mut core, *reader);
        accept_global(&mut core, MergeableCommit::new("notes", row(0x40 + i as u8), 10 + i as u64).cells(BTreeMap::from([
            ("ownerId".to_owned(), Value::String(reader.test_uuid().to_string())),
            ("createdAt".to_owned(), Value::U64(100)),
        ])));
    }
    let shape = Query::from("notes").order_by("createdAt", OrderDirection::Asc).limit(1)
        .validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (_rows, first, later, mut peers) = shared_claims_subscribe_all(&mut core, &readers, &shape);
    accept_global(&mut core, MergeableCommit::new("notes", row(0x4f), 30).cells(BTreeMap::from([
        ("ownerId".to_owned(), Value::String(readers[1].test_uuid().to_string())),
        ("createdAt".to_owned(), Value::U64(1)),
    ])));
    let u0 = peers[0].query_update(&mut core, &shape, &binding).unwrap();
    let u1 = peers[1].query_update(&mut core, &shape, &binding).unwrap();
    assert!(shared_claims_rows(&u0, "notes").is_empty(), "reader 0's page must not change");
    assert!(!format!("{u0:?}").contains(&format!("{:?}", row(0x4f))));
}

/// Same nested-EXISTS policy, but the chat creator (A) is NOT a member and
/// the member (B) did not create it. Neither satisfies both levels. With a
/// flattened inner-join chain that drops the middle input's claim route, A's
/// creator proof pairs with B's membership and the result routes to A.
#[test]
fn shared_claims_two_level_exists_creator_not_member() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("createdBy", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_outer_exists(
                        "chatMembers",
                        "chatId",
                        "chatId",
                        [
                            public_claim_eq("userId", "user_id"),
                            public_outer_exists(
                                "chats",
                                "id",
                                "chatId",
                                [public_claim_eq("createdBy", "user_id")],
                            ),
                        ],
                    ))),
            ),
    );
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let readers = [user(0xa0), user(0xa1)];
    for r in &readers {
        shared_claims_set_claim(&mut core, *r);
    }
    let a = readers[0].test_uuid().to_string();
    let b = readers[1].test_uuid().to_string();
    let chat = row(0x10);
    accept_global(&mut core, MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([(
        "createdBy".to_owned(), Value::String(a.clone()))])));
    accept_global(&mut core, MergeableCommit::new("chatMembers", row(0x21), 11).cells(BTreeMap::from([
        ("chatId".to_owned(), Value::Uuid(chat.0)),
        ("userId".to_owned(), Value::String(b.clone())),
    ])));
    let msg = row(0x30);
    accept_global(&mut core, MergeableCommit::new("messages", msg, 20).cells(BTreeMap::from([
        ("chatId".to_owned(), Value::Uuid(chat.0)),
        ("text".to_owned(), Value::String("hi".to_owned())),
    ])));
    let shape = Query::from("messages").validate(&core.catalogue.schema).unwrap();
    for order in [[readers[1], readers[0]], [readers[0], readers[1]]] {
        let (rows, first, later, _peers) = shared_claims_subscribe_all(&mut core, &order, &shape);
        for (i, r) in rows.iter().enumerate() {
            assert!(r.get("messages").is_none_or(|m| m.is_empty()), "subscriber {i} saw {r:?}");
        }
    }
    for r in &readers {
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let one_shot = core
            .query_rows_for_link(&shape, &binding, DurabilityTier::Global, *r)
            .unwrap();
        let ids: Vec<_> = one_shot.iter().map(|row| row.row_uuid()).collect(); assert!(one_shot.is_empty(), "one-shot leaked to {} (a={a}, b={b}): {ids:?}", r.test_uuid());
    }
}
