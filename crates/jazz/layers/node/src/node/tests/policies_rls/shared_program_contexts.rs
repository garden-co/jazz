// Shared programs across authorization contexts: an admin beside users,
// one query at several tiers, isolated programs under program-named binding
// sources, and a recursive policy whose program has no source name (#4024).

/// Subscribes `peer` with explicit options and records its view.
fn shared_ctx_sub(
    core: &mut NodeState,
    peer: &mut PeerState,
    shape: &ValidatedQuery,
    opts: RegisterShapeOptions,
) -> (SharedProgramState, BTreeMap<String, BTreeSet<RowUuid>>) {
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let up = peer
        .rehydrate_query_with_opts(core, shape, &binding, opts)
        .unwrap();
    let mut st = SharedProgramState::new();
    shared_program_apply(&mut st, &up);
    (st, rv_mentioned(&up))
}

fn shared_ctx_upd(
    core: &mut NodeState,
    peer: &mut PeerState,
    shape: &ValidatedQuery,
    st: &mut SharedProgramState,
    mentioned: &mut BTreeMap<String, BTreeSet<RowUuid>>,
) {
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let up = peer.query_update(core, shape, &binding).unwrap();
    shared_program_apply(st, &up);
    for (t, rows) in rv_mentioned(&up) {
        mentioned.entry(t).or_default().extend(rows);
    }
}

/// Admin (SYSTEM permission identity) and ordinary users subscribe to the
/// same query, plain and count. The admin graph has no policy filter and
/// must never share a binding source or a shape with the users' graphs.
#[test]
fn shared_ctx_admin_and_users_same_query_stay_apart() {
    let mut failures = Vec::new();
    for table in ["edocs", "sdocs", "adocs"] {
        for query in [RvQuery::Plain, RvQuery::Count] {
            let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
            let mut world = RvWorld::new(&mut core);
            let shape = rv_shape(&core, table, query);
            core.drive_query_runtime().unwrap();
            let opts = shared_program_opts();
            // Admin first, so a later user would land on the admin's graph
            // if the names collided.
            let mut admin = PeerState::client_link_with_permission_identity(
                world.readers[2],
                AuthorSubject::SYSTEM,
            );
            let (mut admin_st, mut admin_m) =
                shared_ctx_sub(&mut core, &mut admin, &shape, opts.clone());
            let mut users = Vec::new();
            for reader in 0..2 {
                let mut peer = PeerState::client_link(world.readers[reader]);
                let (st, m) = shared_ctx_sub(&mut core, &mut peer, &shape, opts.clone());
                users.push((reader, peer, st, m));
            }
            let before: Vec<BTreeSet<RowUuid>> =
                (0..3).map(|r| world.allowed_rows(table, r)).collect();
            rv_mutate(&mut core, &mut world);
            shared_ctx_upd(&mut core, &mut admin, &shape, &mut admin_st, &mut admin_m);
            for (_, peer, st, m) in users.iter_mut() {
                shared_ctx_upd(&mut core, peer, &shape, st, m);
            }
            for (reader, _, st, m) in &users {
                let allowed = world.ever_allowed(table, *reader, &before[*reader]);
                let mentioned = m.get(table).cloned().unwrap_or_default();
                let foreign: BTreeSet<_> = mentioned.difference(&allowed).copied().collect();
                if !foreign.is_empty() {
                    failures.push(format!("{table} {query:?}: user {reader} sent {foreign:?}"));
                }
                let held = shared_program_t(st, table);
                if held != world.allowed_rows(table, *reader) {
                    failures.push(format!("{table} {query:?}: user {reader} holds {held:?}"));
                }
            }
            let all: BTreeSet<RowUuid> = world.docs[table].iter().map(|d| d.row).collect();
            if shared_program_t(&admin_st, table) != all {
                failures.push(format!("{table} {query:?}: admin holds wrong rows"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The same query from three users at different durability tiers. A
/// Global and a Local-tier program read different sources; whatever they
/// share, each user must see only its own rows, live too.
#[test]
fn shared_ctx_same_query_different_tiers() {
    let mut failures = Vec::new();
    for table in ["edocs", "sdocs"] {
        for query in [RvQuery::Plain, RvQuery::Count, RvQuery::Window] {
            let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
            let mut world = RvWorld::new(&mut core);
            let shape = rv_shape(&core, table, query);
            core.drive_query_runtime().unwrap();
            let tiers = [
                DurabilityTier::Global,
                DurabilityTier::Local,
                DurabilityTier::Global,
            ];
            let mut users = Vec::new();
            for (reader, tier) in tiers.iter().enumerate() {
                let mut peer = PeerState::client_link(world.readers[reader]);
                let opts = RegisterShapeOptions {
                    tier: *tier,
                    ..RegisterShapeOptions::default()
                };
                let (st, m) = shared_ctx_sub(&mut core, &mut peer, &shape, opts);
                users.push((reader, peer, st, m));
            }
            let before: Vec<BTreeSet<RowUuid>> =
                (0..3).map(|r| world.allowed_rows(table, r)).collect();
            rv_mutate(&mut core, &mut world);
            for (reader, peer, st, m) in users.iter_mut() {
                let opts = RegisterShapeOptions {
                    tier: tiers[*reader],
                    ..RegisterShapeOptions::default()
                };
                let binding = shape.bind(BTreeMap::new()).unwrap();
                let key = SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: binding.binding_id(),
                    read_view: opts.read_view_key(),
                };
                let up = peer
                    .query_update_for_subscription_with_opts(&mut core, key, &shape, &binding, opts)
                    .unwrap();
                if let Some(up) = up {
                    shared_program_apply(st, &up);
                    for (t, rows) in rv_mentioned(&up) {
                        m.entry(t).or_default().extend(rows);
                    }
                }
            }
            for (reader, _, st, m) in &users {
                let allowed = world.ever_allowed(table, *reader, &before[*reader]);
                let mentioned = m.get(table).cloned().unwrap_or_default();
                let foreign: BTreeSet<_> = mentioned.difference(&allowed).copied().collect();
                if !foreign.is_empty() {
                    failures.push(format!(
                        "{table} {query:?}: reader {reader} sent {foreign:?}"
                    ));
                }
                let held = shared_program_t(st, table);
                let expected: BTreeSet<RowUuid> = match query {
                    RvQuery::Window => world
                        .allowed(table, *reader)
                        .into_iter()
                        .skip(1)
                        .take(2)
                        .map(|d| d.row)
                        .collect(),
                    _ => world.allowed_rows(table, *reader),
                };
                if held != expected {
                    failures.push(format!(
                        "{table} {query:?}: reader {reader} holds {held:?}, expected {expected:?}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Two programs with equal lowered graphs but run for different users must
/// read one binding source and still deliver per user; the hash name must
/// not make a route-unsafe (isolated) program share across sessions.
#[test]
fn shared_ctx_isolated_programs_stay_per_session_under_program_names() {
    let mut failures = Vec::new();
    for (table, query) in [
        ("mdocs", RvQuery::Plain),
        ("odocs", RvQuery::Window),
        ("edocs", RvQuery::ParamOrg),
        ("sdocs", RvQuery::GroupCount),
    ] {
        let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
        let world = RvWorld::new(&mut core);
        let shape = rv_shape(&core, table, query);
        let run = rv_subscribe_q(&mut core, &world, &shape, query);
        if run.shapes_added != 3 {
            failures.push(format!(
                "{table} {query:?}: {} shapes for 3 sessions",
                run.shapes_added
            ));
        }
        let mut f = Vec::new();
        rv_check(
            &world,
            table,
            query,
            &run,
            &(0..3)
                .map(|r| world.allowed_rows(table, r))
                .collect::<Vec<_>>(),
            "initial",
            &mut f,
        );
        failures.extend(f);
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A recursive claim-seeded policy compiles with no source name, yet its
/// graph reads a binding source of claim slots. It is route-checked like any
/// other program, and since a recursive operator is not route-aware, each
/// reader gets a program of its own. Two readers on disjoint team trees
/// stay apart, live too.
#[test]
fn shared_ctx_recursive_policy_without_source_name_is_checked() {
    let schema = recursive_doc_policy_schema(recursive_doc_access_policy());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (a, b) = (user(0xb2), user(0xb3));
    for r in [a, b] {
        core.set_test_provider_claims(
            r,
            BTreeMap::from([(
                crate::query::provider_claim_key("sub"),
                Value::Uuid(r.test_uuid()),
            )]),
        );
    }
    let doc_a = RowUuid(uuid::uuid!("10000000-0000-0000-0000-0000000000a1"));
    let doc_pa = RowUuid(uuid::uuid!("10000000-0000-0000-0000-0000000000a2"));
    let doc_b = RowUuid(uuid::uuid!("10000000-0000-0000-0000-0000000000b1"));
    let doc_pb = RowUuid(uuid::uuid!("10000000-0000-0000-0000-0000000000b2"));
    let parent_a = RowUuid(uuid::uuid!("20000000-0000-0000-0000-0000000000a0"));
    let parent_b = RowUuid(uuid::uuid!("20000000-0000-0000-0000-0000000000b0"));
    let (ta, tb) = (RowUuid(a.test_uuid()), RowUuid(b.test_uuid()));
    for (team, name) in [(ta, "a"), (tb, "b"), (parent_a, "pa"), (parent_b, "pb")] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(name.to_owned()),
            )])),
        );
    }
    for (doc, t) in [(doc_a, 20), (doc_pa, 21), (doc_b, 22), (doc_pb, 23)] {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, t).cells(recursive_doc_cells("d", "visible")),
        );
    }
    for (idx, doc, team) in [
        (0xa1, doc_a, ta),
        (0xa2, doc_pa, parent_a),
        (0xa3, doc_b, tb),
        (0xa4, doc_pb, parent_b),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(idx), 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    for (idx, member, parent) in [(0xe1, ta, parent_a), (0xe2, tb, parent_b)] {
        accept_global(
            &mut core,
            MergeableCommit::new("team_edges", row(idx), 40).cells(BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
            ])),
        );
    }
    let shape = Query::from("docs")
        .validate(&core.catalogue.schema)
        .unwrap();
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(&core);
    let mut failures = Vec::new();
    let mut runs = Vec::new();
    for (who, opts) in [
        (a, RegisterShapeOptions::default()),
        (b, RegisterShapeOptions::default()),
    ] {
        let mut peer = PeerState::client_link(who);
        let (st, m) = shared_ctx_sub(&mut core, &mut peer, &shape, opts);
        runs.push((peer, st, m));
    }
    assert_eq!(
        shared_program_shapes(&core) - base,
        2,
        "a recursive policy program is not shared between readers"
    );
    // Live: a new doc for A's parent team, and B gains A's parent too.
    let doc_pa2 = RowUuid(uuid::uuid!("10000000-0000-0000-0000-0000000000a3"));
    accept_global(
        &mut core,
        MergeableCommit::new("docs", doc_pa2, 50).cells(recursive_doc_cells("d2", "visible")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("doc_access", row(0xa5), 51).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc_pa2.0)),
            ("team".to_owned(), Value::Uuid(parent_a.0)),
        ])),
    );
    for (peer, st, m) in runs.iter_mut() {
        shared_ctx_upd(&mut core, peer, &shape, st, m);
    }
    let expected_a = BTreeSet::from([doc_a, doc_pa, doc_pa2]);
    let expected_b = BTreeSet::from([doc_b, doc_pb]);
    for (i, expected) in [(0, &expected_a), (1, &expected_b)] {
        let held = shared_program_t(&runs[i].1, "docs");
        if &held != expected {
            failures.push(format!("reader {i} holds {held:?}, expected {expected:?}"));
        }
        let sent = runs[i].2.get("docs").cloned().unwrap_or_default();
        let foreign: BTreeSet<_> = sent.difference(expected).copied().collect();
        if !foreign.is_empty() {
            failures.push(format!("reader {i} was sent {foreign:?}"));
        }
    }
    // B joins A's parent: now B reads A's parent docs, A unchanged.
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0xe3), 60).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(tb.0)),
            ("parent".to_owned(), Value::Uuid(parent_a.0)),
        ])),
    );
    for (peer, st, m) in runs.iter_mut() {
        shared_ctx_upd(&mut core, peer, &shape, st, m);
    }
    let held_a = shared_program_t(&runs[0].1, "docs");
    let held_b = shared_program_t(&runs[1].1, "docs");
    if held_a != expected_a {
        failures.push(format!("after B joins: A holds {held_a:?}"));
    }
    let expected_b2 = BTreeSet::from([doc_b, doc_pb, doc_pa, doc_pa2]);
    if held_b != expected_b2 {
        failures.push(format!(
            "after B joins: B holds {held_b:?}, expected {expected_b2:?}"
        ));
    }
    let sent_a = runs[0].2.get("docs").cloned().unwrap_or_default();
    if sent_a.contains(&doc_b) || sent_a.contains(&doc_pb) {
        failures.push(format!("A was sent B's docs: {sent_a:?}"));
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
