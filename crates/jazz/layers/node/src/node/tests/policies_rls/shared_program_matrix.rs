// Adversarial shapes that might pass the structural route check yet leak
// across subscribers that share one prepared program. Every test asserts the
// per-session truth computed from a hand model of the policy, so it holds
// whether or not a program shares; the plain listing also pins which
// policies share one program across readers.

fn shared_program_shapes(core: &NodeState) -> usize {
    core.runtime_stats_for_test().active_prepared_shapes
}

const RV_ORGS: [&str; 3] = ["orgA", "orgA", "orgB"];
const RV_ROLES: [&str; 3] = ["admin", "member", "member"];
const RV_TABLES: [&str; 7] = ["adocs", "odocs", "sdocs", "ndocs", "rdocs", "edocs", "mdocs"];

fn rv_doc_table(name: &str, select: PublicPolicyExpr) -> PublicTableSchemaBuilder {
    PublicTableSchemaBuilder::new(name)
        .column("ownerId", PublicColumnType::Text)
        .column("orgId", PublicColumnType::Text)
        .column("createdAt", PublicColumnType::Timestamp)
        .policies(public_all_policies().with_select(select))
}

fn rv_claim(name: &str) -> Vec<String> {
    vec!["claims".to_owned(), name.to_owned()]
}

/// NOT EXISTS is rejected by the core schema ("do not support Exists ...
/// yet"), so anti-join policies are unreachable; use a negated claim
/// equality instead (everyone's docs except the reader's own).
fn rv_not_blocked() -> PublicPolicyExpr {
    PublicPolicyExpr::Not(Box::new(public_claim_eq("ownerId", "user_id")))
}

fn rv_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("blocks")
                    .column("blocker", PublicColumnType::Text)
                    .column("blockedOwner", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("shares")
                    .column("docId", PublicColumnType::Text)
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            // NOT EXISTS with a claim: an anti-join keyed on the claim.
            .table(rv_doc_table("adocs", rv_not_blocked()))
            // OR over two different claims.
            .table(rv_doc_table(
                "odocs",
                PublicPolicyExpr::Or(vec![
                    public_claim_eq("ownerId", "user_id"),
                    public_claim_eq("orgId", "org_id"),
                ]),
            ))
            // OR of a claim equality and an EXISTS over a claim-keyed table.
            .table(rv_doc_table(
                "sdocs",
                PublicPolicyExpr::Or(vec![
                    public_claim_eq("ownerId", "user_id"),
                    public_outer_exists(
                        "shares",
                        "docId",
                        "ownerId",
                        [public_claim_eq("userId", "user_id")],
                    ),
                ]),
            ))
            // AND of a claim equality and NOT EXISTS.
            .table(rv_doc_table(
                "ndocs",
                PublicPolicyExpr::And(vec![public_claim_eq("orgId", "org_id"), rv_not_blocked()]),
            ))
            // A row-independent session predicate OR a claim equality.
            .table(rv_doc_table(
                "rdocs",
                PublicPolicyExpr::Or(vec![
                    PublicPolicyExpr::SessionCmp {
                        path: rv_claim("role"),
                        op: PublicCmpOp::Eq,
                        value: PublicValue::Text("admin".to_owned()),
                    },
                    public_claim_eq("ownerId", "user_id"),
                ]),
            ))
            // Plain owner equality (baseline that shares).
            .table(rv_doc_table("edocs", public_claim_eq("ownerId", "user_id")))
            // Two claims, both equality-routed: EXISTS over user_id AND org_id.
            .table(rv_doc_table(
                "mdocs",
                PublicPolicyExpr::And(vec![
                    public_outer_exists(
                        "shares",
                        "docId",
                        "ownerId",
                        [public_claim_eq("userId", "user_id")],
                    ),
                    public_claim_eq("orgId", "org_id"),
                ]),
            ))
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .fk_column("doc", "edocs")
                    .column("body", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("replies")
                    .fk_column("comment", "comments")
                    .column("body", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    )
}

#[derive(Clone, Debug)]
struct RvDoc {
    row: RowUuid,
    owner: usize,
    org: String,
    created: u64,
}

struct RvWorld {
    uids: Vec<String>,
    readers: Vec<AuthorSubject>,
    docs: BTreeMap<&'static str, Vec<RvDoc>>,
    blocks: BTreeSet<(usize, usize)>,
    shares: BTreeSet<(usize, usize)>, // (doc owner, user): sdocs policy keys on ownerId
    deleted: BTreeSet<RowUuid>,
    tx: u64,
}

fn rv_table_index(table: &str) -> u8 {
    RV_TABLES.iter().position(|t| *t == table).unwrap() as u8
}

impl RvWorld {
    fn new(core: &mut NodeState) -> Self {
        let readers = vec![user(0xa0), user(0xa1), user(0xa2)];
        let uids: Vec<String> = readers.iter().map(|r| r.test_uuid().to_string()).collect();
        for (i, r) in readers.iter().enumerate() {
            core.set_test_provider_claims(
                *r,
                BTreeMap::from([
                    ("user_id".to_owned(), Value::String(uids[i].clone())),
                    ("org_id".to_owned(), Value::String(RV_ORGS[i].to_owned())),
                    ("role".to_owned(), Value::String(RV_ROLES[i].to_owned())),
                ]),
            );
        }
        let mut world = Self {
            uids,
            readers,
            docs: BTreeMap::new(),
            blocks: BTreeSet::new(),
            shares: BTreeSet::new(),
            deleted: BTreeSet::new(),
            tx: 10,
        };
        for table in RV_TABLES {
            for owner in 0..3 {
                for j in 0..2u64 {
                    let row = RowUuid(uuid::Uuid::from_bytes([
                        0x40 + rv_table_index(table),
                        owner as u8,
                        j as u8,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        1,
                    ]));
                    world.put_doc(
                        core,
                        table,
                        RvDoc {
                            row,
                            owner,
                            org: RV_ORGS[owner].to_owned(),
                            created: 10 * (j + 1) + owner as u64,
                        },
                    );
                }
            }
        }
        // U0 blocks U1's docs; U2 blocks U0's docs.
        world.add_block(core, 0, 1);
        world.add_block(core, 2, 0);
        // U2 may read U0's sdocs.
        world.add_share(core, 0, 2);
        world.add_share(core, 0, 1);
        for i in 0..3 {
            world.add_share(core, i, i);
        }
        world
    }

    fn next_tx(&mut self) -> u64 {
        self.tx += 1;
        self.tx
    }

    fn put_doc(&mut self, core: &mut NodeState, table: &'static str, doc: RvDoc) {
        let tx = self.next_tx();
        accept_global(
            core,
            MergeableCommit::new(table, doc.row, tx).cells(BTreeMap::from([
                ("ownerId".to_owned(), Value::String(self.uids[doc.owner].clone())),
                ("orgId".to_owned(), Value::String(doc.org.clone())),
                ("createdAt".to_owned(), Value::U64(doc.created)),
            ])),
        );
        let docs = self.docs.entry(table).or_default();
        docs.retain(|d| d.row != doc.row);
        docs.push(doc);
    }

    fn delete_doc(&mut self, core: &mut NodeState, table: &'static str, row: RowUuid) {
        let tx = self.next_tx();
        accept_global(
            core,
            MergeableCommit::new(table, row, tx).deletion(DeletionEvent::Deleted),
        );
        self.docs.entry(table).or_default().retain(|d| d.row != row);
        self.deleted.insert(row);
    }

    fn add_block(&mut self, core: &mut NodeState, blocker: usize, owner: usize) {
        let tx = self.next_tx();
        let row = RowUuid(uuid::Uuid::from_bytes([0x30, blocker as u8, owner as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        accept_global(
            core,
            MergeableCommit::new("blocks", row, tx).cells(BTreeMap::from([
                ("blocker".to_owned(), Value::String(self.uids[blocker].clone())),
                ("blockedOwner".to_owned(), Value::String(self.uids[owner].clone())),
            ])),
        );
        self.blocks.insert((blocker, owner));
    }

    fn add_share(&mut self, core: &mut NodeState, owner: usize, reader: usize) {
        let tx = self.next_tx();
        let row = RowUuid(uuid::Uuid::from_bytes([0x31, owner as u8, reader as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        accept_global(
            core,
            MergeableCommit::new("shares", row, tx).cells(BTreeMap::from([
                ("docId".to_owned(), Value::String(self.uids[owner].clone())),
                ("userId".to_owned(), Value::String(self.uids[reader].clone())),
            ])),
        );
        self.shares.insert((owner, reader));
    }

    fn visible(&self, table: &str, reader: usize, doc: &RvDoc) -> bool {
        let own = doc.owner == reader;
        let blocked = doc.owner == reader;
        match table {
            "adocs" => !blocked,
            "odocs" => own || doc.org == RV_ORGS[reader],
            "sdocs" => own || self.shares.contains(&(doc.owner, reader)),
            "ndocs" => doc.org == RV_ORGS[reader] && !blocked,
            "rdocs" => RV_ROLES[reader] == "admin" || own,
            "edocs" => own,
            "mdocs" => self.shares.contains(&(doc.owner, reader)) && doc.org == RV_ORGS[reader],
            _ => unreachable!(),
        }
    }

    fn allowed(&self, table: &str, reader: usize) -> Vec<RvDoc> {
        let mut docs: Vec<_> = self.docs[table]
            .iter()
            .filter(|d| self.visible(table, reader, d))
            .cloned()
            .collect();
        docs.sort_by_key(|d| (d.created, d.row));
        docs
    }

    fn allowed_rows(&self, table: &str, reader: usize) -> BTreeSet<RowUuid> {
        self.allowed(table, reader).into_iter().map(|d| d.row).collect()
    }

    /// Rows `reader` could read at any time in the test (a deleted row the
    /// reader could see may still appear as a deletion witness).
    fn ever_allowed(&self, table: &str, reader: usize, before: &BTreeSet<RowUuid>) -> BTreeSet<RowUuid> {
        let mut rows = self.allowed_rows(table, reader);
        rows.extend(before.iter().copied());
        rows
    }
}

/// Every row any layer of the update mentions, by table.
fn rv_mentioned(update: &SyncMessage) -> BTreeMap<String, BTreeSet<RowUuid>> {
    let SyncMessage::ViewUpdate(payload) = update else {
        panic!("expected view update, got {update:?}");
    };
    let mut out = BTreeMap::<String, BTreeSet<RowUuid>>::new();
    for input in payload
        .supporting_rows
        .added_rows()
        .iter()
        .chain(payload.supporting_rows.removed_rows())
    {
        out.entry(input.version_table.as_str().to_owned())
            .or_default()
            .insert(input.row);
    }
    out
}

struct RvRun {
    peers: Vec<PeerState>,
    states: Vec<SharedProgramState>,
    mentioned: Vec<BTreeMap<String, BTreeSet<RowUuid>>>,
    shapes_added: usize,
}

fn rv_params(query: RvQuery, reader: usize) -> BTreeMap<String, Value> {
    match query {
        // Readers 0 and 2 ask for orgA, reader 1 for orgB.
        RvQuery::ParamOrg => shared_program_params(&[(
            "org",
            Value::String(rv_param_org(reader).to_owned()),
        )]),
        _ => BTreeMap::new(),
    }
}

fn rv_param_org(reader: usize) -> &'static str {
    ["orgA", "orgB", "orgA"][reader]
}

fn rv_subscribe(core: &mut NodeState, world: &RvWorld, shape: &ValidatedQuery) -> RvRun {
    rv_subscribe_q(core, world, shape, RvQuery::Plain)
}

fn rv_subscribe_q(core: &mut NodeState, world: &RvWorld, shape: &ValidatedQuery, query: RvQuery) -> RvRun {
    core.drive_query_runtime().unwrap();
    let base = shared_program_shapes(core);
    let mut run = RvRun {
        peers: Vec::new(),
        states: Vec::new(),
        mentioned: Vec::new(),
        shapes_added: 0,
    };
    // Subscribe in reverse so a later reader's graph already holds others.
    for (i, reader) in world.readers.iter().enumerate() {
        let mut peer = PeerState::client_link(*reader);
        let up = shared_program_sub(core, &mut peer, shape, &rv_params(query, i));
        let mut st = SharedProgramState::new();
        shared_program_apply(&mut st, &up);
        run.mentioned.push(rv_mentioned(&up));
        run.states.push(st);
        run.peers.push(peer);
    }
    run.shapes_added = shared_program_shapes(core) - base;
    run
}

fn rv_update(core: &mut NodeState, shape: &ValidatedQuery, run: &mut RvRun) {
    rv_update_q(core, shape, run, RvQuery::Plain, &[]);
}

fn rv_update_q(core: &mut NodeState, shape: &ValidatedQuery, run: &mut RvRun, query: RvQuery, skip: &[usize]) {
    for (i, peer) in run.peers.iter_mut().enumerate() {
        if skip.contains(&i) {
            continue;
        }
        let up = shared_program_upd(core, peer, shape, &rv_params(query, i));
        shared_program_apply(&mut run.states[i], &up);
        for (t, rows) in rv_mentioned(&up) {
            run.mentioned[i].entry(t).or_default().extend(rows);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RvQuery {
    Plain,
    Window,
    SelectCreated,
    Count,
    GroupCount,
    ArrayChildrenLimited,
    ParamOrg,
}

fn rv_shape(core: &NodeState, table: &str, query: RvQuery) -> ValidatedQuery {
    let q = Query::from(table);
    let q = match query {
        RvQuery::Plain => q,
        RvQuery::Window => q.order_by("createdAt", OrderDirection::Asc).limit(2).offset(1),
        RvQuery::SelectCreated => q.select(["createdAt"]),
        RvQuery::Count => q.count(),
        RvQuery::GroupCount => q.group_by("orgId").count(),
        RvQuery::ArrayChildrenLimited => q.array_subquery(
            ArraySubquery::new("kids", "comments", "doc", "id")
                .order_by("createdAt", OrderDirection::Asc)
                .limit(1),
        ),
        RvQuery::ParamOrg => q.filter(eq(col("orgId"), param("org"))),
    };
    q.validate(&core.catalogue.schema).unwrap()
}

/// Mutations: a new earliest doc for U1, U1 blocks U2, U2's doc moves to
/// orgA, U2 gains a share of U1's sdocs, and U0's late doc is deleted.
fn rv_mutate(core: &mut NodeState, world: &mut RvWorld) {
    for table in RV_TABLES {
        let row = RowUuid(uuid::Uuid::from_bytes([0x60 + rv_table_index(table), 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        world.put_doc(core, table, RvDoc { row, owner: 1, org: RV_ORGS[1].to_owned(), created: 1 });
        let moved = world.docs[table].iter().find(|d| d.owner == 2 && d.created == 12).unwrap().clone();
        world.put_doc(core, table, RvDoc { org: "orgA".to_owned(), ..moved });
    }
    world.add_block(core, 1, 2);
    world.add_share(core, 1, 0);
    for table in RV_TABLES {
        let gone = world.docs[table].iter().find(|d| d.owner == 0 && d.created == 20).unwrap().row;
        world.delete_doc(core, table, gone);
    }
}

fn rv_check(
    world: &RvWorld,
    table: &str,
    query: RvQuery,
    run: &RvRun,
    before: &[BTreeSet<RowUuid>],
    stage: &str,
    failures: &mut Vec<String>,
) {
    for reader in 0..3 {
        let allowed = world.ever_allowed(table, reader, &before[reader]);
        let mentioned = run.mentioned[reader].get(table).cloned().unwrap_or_default();
        let foreign: BTreeSet<_> = mentioned.difference(&allowed).copied().collect();
        if !foreign.is_empty() {
            failures.push(format!(
                "{table} {query:?} {stage}: reader {reader} was sent unreadable rows {foreign:?}"
            ));
        }
        let held = shared_program_t(&run.states[reader], table);
        let expected: BTreeSet<RowUuid> = match query {
            RvQuery::Plain | RvQuery::SelectCreated | RvQuery::Count | RvQuery::GroupCount | RvQuery::ArrayChildrenLimited => {
                world.allowed_rows(table, reader)
            }
            RvQuery::ParamOrg => world
                .allowed(table, reader)
                .into_iter()
                .filter(|d| d.org == rv_param_org(reader))
                .map(|d| d.row)
                .collect(),
            RvQuery::Window => world
                .allowed(table, reader)
                .into_iter()
                .skip(1)
                .take(2)
                .map(|d| d.row)
                .collect(),
        };
        if held != expected {
            failures.push(format!(
                "{table} {query:?} {stage}: reader {reader} holds {held:?}, expected {expected:?}"
            ));
        }
    }
}

fn rv_case(table: &'static str, query: RvQuery) -> (usize, Vec<String>) {
    let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
    let mut world = RvWorld::new(&mut core);
    let shape = rv_shape(&core, table, query);
    let mut run = rv_subscribe_q(&mut core, &world, &shape, query);
    let mut failures = Vec::new();
    let before: Vec<_> = (0..3).map(|r| world.allowed_rows(table, r)).collect();
    rv_check(&world, table, query, &run, &before, "initial", &mut failures);
    rv_mutate(&mut core, &mut world);
    rv_update_q(&mut core, &shape, &mut run, query, &[]);
    rv_check(&world, table, query, &run, &before, "after-mutations", &mut failures);
    (run.shapes_added, failures)
}

/// Runs every query shape against `table`'s policy for three readers. A
/// plain listing of a policy that `shares` installs one program for all
/// three; otherwise each reader gets its own.
fn rv_matrix(table: &'static str, shares: bool) {
    let mut all = Vec::new();
    for query in [
        RvQuery::Plain,
        RvQuery::Window,
        RvQuery::SelectCreated,
        RvQuery::Count,
        RvQuery::GroupCount,
        RvQuery::ParamOrg,
        RvQuery::ArrayChildrenLimited,
    ] {
        let (shapes, failures) = rv_case(table, query);
        if query == RvQuery::Plain {
            assert_eq!(shapes, if shares { 1 } else { 3 }, "{table} plain listing programs");
        }
        all.extend(failures);
    }
    assert!(all.is_empty(), "leaks or wrong results:\n{}", all.join("\n"));
}

#[test]
fn shared_matrix_not_exists_claim_policy_matrix() {
    rv_matrix("adocs", false);
}

#[test]
fn shared_matrix_or_of_two_claims_policy_matrix() {
    rv_matrix("odocs", false);
}

#[test]
fn shared_matrix_or_claim_or_exists_policy_matrix() {
    rv_matrix("sdocs", true);
}

#[test]
fn shared_matrix_and_claim_not_exists_policy_matrix() {
    rv_matrix("ndocs", false);
}

#[test]
fn shared_matrix_session_role_or_claim_policy_matrix() {
    rv_matrix("rdocs", false);
}

#[test]
fn shared_matrix_owner_claim_policy_matrix() {
    rv_matrix("edocs", true);
}

#[test]
fn shared_matrix_two_equality_claims_policy_matrix() {
    rv_matrix("mdocs", false);
}

/// The proof-holding binding unsubscribes while others keep sharing the
/// graph; later proof changes must still route only to their holders, and a
/// re-subscribe must get exactly its own rows.
#[test]
fn shared_matrix_unsubscribe_proof_holder_then_mutate() {
    let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
    let mut world = RvWorld::new(&mut core);
    let table = "sdocs";
    let shape = rv_shape(&core, table, RvQuery::Plain);
    let mut run = rv_subscribe(&mut core, &world, &shape);
    assert_eq!(run.shapes_added, 1, "sdocs plain should share one program");
    let before: Vec<_> = (0..3).map(|r| world.allowed_rows(table, r)).collect();
    // Reader 2 holds the extra share proof (U0 -> U2); it leaves.
    shared_program_forget(&mut core, &mut run.peers[2], &shape, &BTreeMap::new());
    rv_mutate(&mut core, &mut world);
    world.add_share(&mut core, 2, 1);
    rv_update_q(&mut core, &shape, &mut run, RvQuery::Plain, &[2]);
    let mut failures = Vec::new();
    for reader in 0..2 {
        let held = shared_program_t(&run.states[reader], table);
        let expected = world.allowed_rows(table, reader);
        if held != expected {
            failures.push(format!("reader {reader} holds {held:?}, expected {expected:?}"));
        }
        let mentioned = run.mentioned[reader].get(table).cloned().unwrap_or_default();
        let allowed = world.ever_allowed(table, reader, &before[reader]);
        let foreign: BTreeSet<_> = mentioned.difference(&allowed).copied().collect();
        if !foreign.is_empty() {
            failures.push(format!("reader {reader} sent {foreign:?}"));
        }
    }
    // Reader 2 comes back on a fresh link.
    let mut peer = PeerState::client_link(world.readers[2]);
    let up = shared_program_sub(&mut core, &mut peer, &shape, &BTreeMap::new());
    let mut st = SharedProgramState::new();
    shared_program_apply(&mut st, &up);
    let held = shared_program_t(&st, table);
    if held != world.allowed_rows(table, 2) {
        failures.push(format!("re-subscribed reader 2 holds {held:?}, expected {:?}", world.allowed_rows(table, 2)));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn rv_seed_comments(core: &mut NodeState, world: &mut RvWorld) -> Vec<(RowUuid, RowUuid, usize)> {
    // One comment and one reply per edocs doc; comments and replies are public.
    let mut out = Vec::new();
    let docs = world.docs["edocs"].clone();
    for (k, doc) in docs.iter().enumerate() {
        let comment = RowUuid(uuid::Uuid::from_bytes([0x70, k as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        let reply = RowUuid(uuid::Uuid::from_bytes([0x71, k as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        let tx = world.next_tx();
        accept_global(core, MergeableCommit::new("comments", comment, tx).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.row.0)),
            ("body".to_owned(), Value::String(format!("c{k}"))),
            ("createdAt".to_owned(), Value::U64(k as u64)),
        ])));
        let tx = world.next_tx();
        accept_global(core, MergeableCommit::new("replies", reply, tx).cells(BTreeMap::from([
            ("comment".to_owned(), Value::Uuid(comment.0)),
            ("body".to_owned(), Value::String(format!("r{k}"))),
        ])));
        out.push((comment, doc.row, doc.owner));
    }
    out
}

/// Public comments / replies whose include chain ends at owner-private
/// edocs. Each reader may receive every comment and reply, but only its own
/// docs. Includes (inner and nested), plus a live doc edit and a re-point of
/// a comment from one owner's doc to another's.
#[test]
fn shared_matrix_public_include_chain_to_private_docs() {
    let mut failures = Vec::new();
    for (label, query) in [
        ("comments.include(doc)", Query::from("comments").include("doc")),
        ("comments implicit closure", Query::from("comments")),
        ("replies.include(comment.doc)", Query::from("replies").include("comment.doc")),
        ("replies implicit closure", Query::from("replies")),
        (
            "edocs with limited comments array",
            Query::from("edocs").array_subquery(
                ArraySubquery::new("kids", "comments", "doc", "id")
                    .order_by("createdAt", OrderDirection::Desc)
                    .limit(1),
            ),
        ),
    ] {
        let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
        let mut world = RvWorld::new(&mut core);
        let comments = rv_seed_comments(&mut core, &mut world);
        let shape = query.validate(&core.catalogue.schema).unwrap();
        let mut run = rv_subscribe(&mut core, &world, &shape);
        let before: Vec<_> = (0..3).map(|r| world.allowed_rows("edocs", r)).collect();
        // Live: edit U1's doc, re-point U0's first comment at U2's doc.
        let d1 = world.docs["edocs"].iter().find(|d| d.owner == 1).unwrap().clone();
        world.put_doc(&mut core, "edocs", RvDoc { created: 77, ..d1 });
        let d2 = world.docs["edocs"].iter().find(|d| d.owner == 2).unwrap().row;
        let (c0, _, _) = comments[0];
        let tx = world.next_tx();
        accept_global(&mut core, MergeableCommit::new("comments", c0, tx).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(d2.0)),
            ("body".to_owned(), Value::String("moved".to_owned())),
            ("createdAt".to_owned(), Value::U64(99)),
        ])));
        rv_update(&mut core, &shape, &mut run);
        for reader in 0..3 {
            let allowed = world.ever_allowed("edocs", reader, &before[reader]);
            let mentioned = run.mentioned[reader].get("edocs").cloned().unwrap_or_default();
            let foreign: BTreeSet<_> = mentioned.difference(&allowed).copied().collect();
            if !foreign.is_empty() {
                failures.push(format!("{label}: reader {reader} sent private docs {foreign:?}"));
            }
            let held = shared_program_t(&run.states[reader], "edocs");
            if !held.is_subset(&world.allowed_rows("edocs", reader)) {
                failures.push(format!("{label}: reader {reader} holds {held:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Per-subscriber cost stays flat as subscribers join, for programs that
/// share (edocs, sdocs) and for programs each reader gets alone (adocs: the
/// negated claim is inlined; odocs: an OR over two claims). A program of its
/// own must not read other readers' bindings, or every subscriber pays for
/// every earlier one. Leaving returns the runtime to its baseline.
#[test]
fn shared_matrix_per_subscriber_state_stays_flat() {
    for table in ["edocs", "adocs", "odocs", "sdocs"] {
        let (_dir, mut core) = open_node_with_schema(node(9), rv_schema());
        let mut readers = Vec::new();
        for i in 0..8u8 {
            let r = user(0xb0 + i);
            core.set_test_provider_claims(r, BTreeMap::from([
                ("user_id".to_owned(), Value::String(r.test_uuid().to_string())),
                ("org_id".to_owned(), Value::String(format!("org{}", i % 2))),
                ("role".to_owned(), Value::String("member".to_owned())),
            ]));
            readers.push(r);
        }
        for k in 0..64u8 {
            accept_global(&mut core, MergeableCommit::new(table, RowUuid(uuid::Uuid::from_bytes([0x90, k, 0,0,0,0,0,0,0,0,0,0,0,0,0,1])), 10 + u64::from(k)).cells(BTreeMap::from([
                ("ownerId".to_owned(), Value::String(readers[(k % 8) as usize].test_uuid().to_string())),
                ("orgId".to_owned(), Value::String(format!("org{}", k % 2))),
                ("createdAt".to_owned(), Value::U64(u64::from(k))),
            ])));
        }
        let shape = Query::from(table).validate(&core.catalogue.schema).unwrap();
        core.drive_query_runtime().unwrap();
        let baseline = core.runtime_stats_for_test();
        let mut last = baseline.clone();
        let mut steps = Vec::new();
        let mut peers = Vec::new();
        for r in &readers {
            let mut peer = PeerState::client_link(*r);
            let _ = shared_program_sub(&mut core, &mut peer, &shape, &BTreeMap::new());
            peers.push(peer);
            let now = core.runtime_stats_for_test();
            steps.push((now.arrangement_rows as i64 - last.arrangement_rows as i64, now.active_prepared_shapes, now.graph_nodes));
            last = now;
        }
        let deltas = steps.iter().map(|(rows, _, _)| *rows).collect::<Vec<_>>();
        assert!(
            deltas[1..].iter().all(|rows| *rows <= deltas[1]),
            "{table}: later subscribers must not cost more than the second: {deltas:?}"
        );
        let shapes = steps.last().unwrap().1 - baseline.active_prepared_shapes;
        let shares = matches!(table, "edocs" | "sdocs");
        assert_eq!(shapes, if shares { 1 } else { readers.len() }, "{table} programs");
        for peer in peers.iter_mut() {
            shared_program_forget(&mut core, peer, &shape, &BTreeMap::new());
        }
        core.drive_query_runtime().unwrap();
        let after = core.runtime_stats_for_test();
        assert_eq!(after.arrangement_rows, baseline.arrangement_rows, "{table} rows after leaving");
        assert_eq!(after.graph_nodes, baseline.graph_nodes, "{table} nodes after leaving");
        assert_eq!(
            after.active_prepared_shapes, baseline.active_prepared_shapes,
            "{table} shapes after leaving"
        );
    }
}
