//! Correctness contracts the Groove-era `jazz` benches asserted, restated
//! on the SQLite engine: cold subscription over deep history, four-tier
//! sync through relays, serializable validation against a model, relation
//! include delivery, and same-shape route subscriptions. Timing is not
//! measured here; the scenario benches and CodSpeed own that.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, lit, param};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_sim::distributions::Lcg;
use jazz_sim::oracle::{Live, Rows, cell};
use jazz_sim::support::{
    allow_all, cells, compile, insert, int, is_me, open, read_gated, row, settle, subject, text,
    user, uuid,
};
use jazz_sim::{PeerProfile, Sim, SimulatorTransportCodec};

fn link() -> PeerProfile {
    PeerProfile::new("smoke", 2, 1, 0)
}

fn items() -> JazzSchema {
    compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("title", ColumnType::Text)
                    .policies(allow_all()),
            )
            .build(),
    )
}

// ---- cold subscription -----------------------------------------------------------------

/// A current-state subscriber receives only the current winner of a row,
/// however many superseded versions the authority retains, and the bytes
/// it receives do not grow with history depth. The authority is reopened
/// from its file before the subscriber arrives, so nothing is served from
/// the writing session's caches.
#[test]
fn cold_subscription_correctness_smoke() {
    let target = row(0x10, 1);
    let mut downstream_bytes = None;
    for depth in [1_usize, 10, 1_000, 1_025] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("core.sqlite");
        let config = || {
            DbConfig::new(
                items(),
                DbIdentity {
                    node: NodeUuid::from_bytes([0; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            )
        };
        {
            let core =
                block_on(Db::open_at(config(), &path, FileOptions::default())).expect("open core");
            insert(
                &core,
                "items",
                target,
                cells([("title", text(format!("v{:08}", 0)))]),
            );
            for version in 1..depth {
                block_on(core.update(
                    "items",
                    target,
                    cells([("title", text(format!("v{version:08}")))]),
                    Default::default(),
                ))
                .expect("update");
            }
        }
        let core =
            block_on(Db::open_at(config(), &path, FileOptions::default())).expect("reopen core");
        let winner = cells([("title", text(format!("v{:08}", depth - 1)))]);
        let local = jazz_sim::oracle::read(&core, &Query::from("items"), ReadOpts::default());
        assert_eq!(
            local,
            Rows::from([(target, winner.clone())]),
            "depth {depth}: one current row"
        );
        let versions = block_on(
            core.all(
                &core
                    .prepare_query(
                        &Query::from("history.items").filter(eq(col("id"), lit(uuid(target)))),
                    )
                    .expect("prepare history"),
                ReadOpts::default(),
            ),
        )
        .expect("history read")
        .len();
        assert_eq!(
            versions, depth,
            "depth {depth}: the authority keeps every version"
        );

        let mut sim = Sim::new(0xc01d, SimulatorTransportCodec::WireFrames);
        sim.add("core", core);
        sim.add("client", open(items(), 1, user(1)));
        sim.connect("client", "core", user(1), link());
        sim.reset_stats();
        let mut live =
            Live::subscribe(sim.db("client"), &Query::from("items"), ReadOpts::default());
        sim.settle();
        live.poll();
        assert_eq!(
            live.rows,
            Rows::from([(target, winner)]),
            "depth {depth}: the client sees the winner"
        );
        let (_, down) = sim.link_stats("client");
        assert_eq!(
            down.row_images, 1,
            "depth {depth}: one row image crosses the wire"
        );
        // Only varint-encoded sequence numbers may widen with depth.
        let baseline = *downstream_bytes.get_or_insert(down.bytes);
        assert!(
            down.bytes <= baseline + 8,
            "depth {depth}: {} bytes against {baseline} at depth 1; bytes must not grow with history",
            down.bytes
        );
    }
}

// ---- four-tier sync --------------------------------------------------------------------

fn owned_todos() -> JazzSchema {
    compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(read_gated(is_me("owner"))),
            )
            .build(),
    )
}

/// core <- relay <- worker <- ui. The UI writes mergeable updates,
/// serializable transactions, deletes and restores, and one row owned by
/// someone else. Relay and worker links run as the system and do not
/// narrow; the UI's link converges to the rows its read policy admits.
#[test]
fn sync_correctness_smoke() {
    let mut sim = Sim::new(0x510c_4eed, SimulatorTransportCodec::WireFrames);
    sim.add("core", open(owned_todos(), 0, AuthorSubject::SYSTEM));
    sim.add("relay", open(owned_todos(), 1, AuthorSubject::SYSTEM));
    sim.add("worker", open(owned_todos(), 2, AuthorSubject::SYSTEM));
    let me = user(3);
    let other = user(4);
    sim.add("ui", open(owned_todos(), 3, me));
    sim.connect("relay", "core", AuthorSubject::SYSTEM, link());
    sim.connect("worker", "relay", AuthorSubject::SYSTEM, link());
    sim.connect("ui", "worker", me, link());
    let all = Query::from("todos");
    let mut relay = Live::subscribe(sim.db("relay"), &all, ReadOpts::default());
    let mut worker = Live::subscribe(sim.db("worker"), &all, ReadOpts::default());
    let mut ui = Live::subscribe(sim.db("ui"), &all, ReadOpts::default());
    sim.settle();

    let todo = |title: String, owner: AuthorSubject| {
        cells([("title", text(title)), ("owner", subject(owner))])
    };
    let hidden = row(0x20, 250);
    let handle = insert(sim.db("ui"), "todos", hidden, todo("hidden".into(), other));
    settle(&sim, sim.db("ui"), &handle);

    let mut rng = Lcg::new(0x510c_4eed);
    let mut expected_ui: BTreeMap<RowUuid, String> = BTreeMap::new();
    let deleted = row(0x20, 200);
    for step in 0..30_usize {
        let ui_db = sim.db("ui");
        match step {
            10 => {
                let handle = insert(
                    ui_db,
                    "todos",
                    deleted,
                    todo(format!("delete-base-{step}"), me),
                );
                settle(&sim, ui_db, &handle);
                expected_ui.insert(deleted, format!("delete-base-{step}"));
            }
            11 => {
                let handle = sim
                    .drive(ui_db.delete("todos", deleted, Default::default()))
                    .expect("delete");
                settle(&sim, ui_db, &handle);
                expected_ui.remove(&deleted);
            }
            12 => {
                let handle = sim
                    .drive(ui_db.restore("todos", deleted, None, Default::default()))
                    .expect("restore");
                settle(&sim, ui_db, &handle);
                expected_ui.insert(deleted, format!("delete-base-{}", step - 2));
            }
            3 => {
                // A serializable read-modify-write of one of the UI's rows,
                // validated at the authority through both relays.
                let target = *expected_ui.keys().next().expect("a row");
                let tx = sim.drive(ui_db.serializable_tx()).expect("begin");
                let prepared = ui_db
                    .prepare_query(&Query::from("todos").filter(eq(col("id"), lit(uuid(target)))))
                    .expect("prepare");
                let read = sim.drive(tx.all_prepared(&prepared)).expect("read");
                assert_eq!(read.len(), 1, "the UI reads its own row in a transaction");
                sim.drive(tx.update(
                    "todos",
                    target,
                    cells([("title", text(format!("serializable-{step}")))]),
                    Default::default(),
                ))
                .expect("stage");
                let receipt = sim.drive(tx.commit()).expect("commit");
                sim.drive(ui_db.wait_for_transaction(receipt.0, DurabilityTier::Global))
                    .expect("a serializable commit through relays is accepted");
                expected_ui.insert(target, format!("serializable-{step}"));
            }
            _ => {
                let target = row(0x20, 1 + rng.usize(12) as u64);
                let title = format!("merge-{step}");
                let handle = if expected_ui.contains_key(&target) {
                    sim.drive(ui_db.update(
                        "todos",
                        target,
                        cells([("title", text(title.clone()))]),
                        Default::default(),
                    ))
                    .expect("update")
                } else {
                    insert(ui_db, "todos", target, todo(title.clone(), me))
                };
                settle(&sim, ui_db, &handle);
                expected_ui.insert(target, title);
            }
        }
        if (step + 1) % 5 == 0 {
            sim.settle();
        }
    }
    sim.settle();
    relay.poll();
    worker.poll();
    ui.poll();

    let core = jazz_sim::oracle::read(sim.db("core"), &all, ReadOpts::default());
    assert!(
        core.contains_key(&hidden),
        "the authority holds the row the UI cannot read"
    );
    assert_eq!(relay.rows, core, "a system relay link does not narrow");
    assert_eq!(worker.rows, core, "a system worker link does not narrow");
    let expected: Rows = expected_ui
        .iter()
        .map(|(id, title)| {
            (
                *id,
                cells([("title", text(title.clone())), ("owner", subject(me))]),
            )
        })
        .collect();
    assert_eq!(
        ui.rows, expected,
        "the UI converges to the rows its read policy admits"
    );
    assert!(
        !ui.rows.contains_key(&hidden),
        "the UI never sees the other owner's row"
    );
}

// ---- serializable validation -----------------------------------------------------------

/// Clients run serializable transactions over a few hot rows, several open
/// at once. A read-set model predicts each fate: a transaction commits
/// exactly when no row it read was written by a transaction that committed
/// after it began. The authority must agree on every transaction and end
/// in the model's state.
#[test]
fn validation_correctness_smoke() {
    const CLIENTS: usize = 3;
    const ROWS: u64 = 6;
    const ROUNDS: usize = 12;
    let mut sim = Sim::new(0x5eed_1234, SimulatorTransportCodec::WireFrames);
    let core = sim.add("core", open(items(), 0, AuthorSubject::SYSTEM));
    let mut model: BTreeMap<RowUuid, String> = BTreeMap::new();
    for n in 0..ROWS {
        insert(
            core,
            "items",
            row(0x30, n),
            cells([("title", text(format!("seed-{n}")))]),
        );
        model.insert(row(0x30, n), format!("seed-{n}"));
    }
    let clients: Vec<String> = (0..CLIENTS).map(|c| format!("client-{c}")).collect();
    for (c, name) in clients.iter().enumerate() {
        sim.add(
            name.clone(),
            open(items(), 1 + c as u32, user(1 + c as u32)),
        );
        sim.connect(name, "core", user(1 + c as u32), link());
    }
    sim.settle();

    let mut rng = Lcg::new(0x5eed_1234);
    let (mut accepted, mut rejected) = (0, 0);
    for round in 0..ROUNDS {
        let contenders = 2 + rng.usize(CLIENTS - 1);
        // Stage every contender before any commits: they share a snapshot.
        let staged: Vec<_> = clients[..contenders]
            .iter()
            .enumerate()
            .map(|(c, name)| {
                let db = sim.db(name);
                let tx = sim.drive(db.serializable_tx()).expect("begin");
                let mut reads = BTreeSet::new();
                // One hot row in a fifth of the transactions, else a spread.
                if rng.usize(5) == 0 {
                    reads.insert(row(0x30, 0));
                }
                for _ in 0..1 + rng.usize(2) {
                    reads.insert(row(0x30, rng.usize(ROWS as usize) as u64));
                }
                for id in &reads {
                    let prepared = db
                        .prepare_query(&Query::from("items").filter(eq(col("id"), lit(uuid(*id)))))
                        .expect("prepare");
                    let rows = sim.drive(tx.all_prepared(&prepared)).expect("read");
                    assert_eq!(
                        cell(&rows[0], "title"),
                        Some(text(model[id].clone())),
                        "round {round}: a transaction reads the committed state"
                    );
                }
                // Write a subset of what was read: never a blind write.
                let writes: Vec<RowUuid> = reads
                    .iter()
                    .copied()
                    .filter(|_| rng.usize(3) != 0)
                    .collect();
                let writes = if writes.is_empty() {
                    vec![*reads.iter().next().expect("a read")]
                } else {
                    writes
                };
                for id in &writes {
                    sim.drive(tx.update(
                        "items",
                        *id,
                        cells([("title", text(format!("r{round}-c{c}")))]),
                        Default::default(),
                    ))
                    .expect("stage write");
                }
                (db, tx, reads, writes, c)
            })
            .collect();
        let mut written_this_round: BTreeSet<RowUuid> = BTreeSet::new();
        for (db, tx, reads, writes, c) in staged {
            let predicted = reads.is_disjoint(&written_this_round);
            let outcome = match sim.drive(tx.commit()) {
                Ok(receipt) => sim
                    .drive(db.wait_for_transaction(receipt.0, DurabilityTier::Global))
                    .map(|_| ()),
                Err(error) => Err(error),
            };
            let committed = match outcome {
                Ok(()) => true,
                Err(error) if error.code == ErrorCode::TransactionConflict => false,
                Err(error) => panic!("unexpected commit failure: {error}"),
            };
            assert_eq!(
                committed, predicted,
                "round {round} client {c}: the authority agrees with the model"
            );
            if committed {
                accepted += 1;
                for id in writes {
                    written_this_round.insert(id);
                    model.insert(id, format!("r{round}-c{c}"));
                }
            } else {
                rejected += 1;
            }
        }
        sim.settle();
    }
    assert!(
        accepted > 0 && rejected > 0,
        "the run both commits and rejects ({accepted}/{rejected})"
    );
    let expected: Rows = model
        .iter()
        .map(|(id, title)| (*id, cells([("title", text(title.clone()))])))
        .collect();
    assert_eq!(
        jazz_sim::oracle::read(sim.db("core"), &Query::from("items"), ReadOpts::default()),
        expected,
        "the authority ends in the model's state"
    );
}

// ---- relation include delivery ---------------------------------------------------------

fn relation_schema() -> JazzSchema {
    compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("label", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .column("label", ColumnType::Text)
                    .column("ordinal", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .build(),
    )
}

fn children_of(row: &jazz::db::CurrentRow) -> usize {
    match cell(row, "children") {
        Some(Value::Array(children)) => children.len(),
        other => panic!("children: {other:?}"),
    }
}

/// Inserting one child under a parent with an included child array updates
/// that one root in place: no reset, nothing added or removed, and the
/// root's array grows by exactly the new child.
#[test]
fn relation_include_delivery_correctness_smoke() {
    let db = open(relation_schema(), 0, AuthorSubject::SYSTEM);
    let parent = row(0x40, 1);
    let other_parent = row(0x40, 2);
    for id in [parent, other_parent] {
        insert(&db, "parents", id, cells([("label", text("parent"))]));
    }
    for n in 0..3 {
        insert(
            &db,
            "children",
            row(0x41, n),
            cells([
                ("parent_id", uuid(parent)),
                ("label", text(format!("child-{n}"))),
                ("ordinal", int(n as i64)),
            ]),
        );
    }
    let query = Query::from("parents").array_subquery(
        ArraySubquery::new("children", "children", "parent_id", "id").select(["label", "ordinal"]),
    );
    let prepared = db.prepare_query(&query).expect("prepare");
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("subscribe");
    match block_on(stream.next_event()).expect("initial hydration") {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => {
            let root = added
                .iter()
                .find(|row| row.row_uuid() == parent)
                .expect("the parent is hydrated");
            assert_eq!(children_of(root), 3);
        }
        other => panic!("expected the initial reset, got {other:?}"),
    }

    insert(
        &db,
        "children",
        row(0x41, 100),
        cells([
            ("parent_id", uuid(parent)),
            ("label", text("measured-child")),
            ("ordinal", int(3)),
        ]),
    );
    match block_on(stream.next_event()).expect("child delta") {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(!reset, "a child change stays incremental");
            assert!(added.is_empty(), "an existing root is not added");
            assert!(removed.is_empty(), "an existing root is not removed");
            assert_eq!(updated.len(), 1, "exactly one root changes");
            assert_eq!(
                updated[0].row_uuid(),
                parent,
                "the child's own parent changes"
            );
            assert_eq!(
                children_of(&updated[0]),
                4,
                "the root's array holds the new child"
            );
        }
        other => panic!("expected a child delta, got {other:?}"),
    }
    assert!(stream.try_next_event().is_none(), "one insert is one event");
}

// ---- route subscriptions ---------------------------------------------------------------

const ROUTES: usize = 8;
const PAGE: usize = 4;
const DOCUMENTS_PER_TEAM: u64 = 6;

fn team(n: usize) -> RowUuid {
    row(0x50, n as u64)
}

fn document(team_index: usize, rank: u64) -> RowUuid {
    row(0x51, ((team_index as u64) << 16) | rank)
}

fn take_reset(stream: &mut SubscriptionStream) -> Vec<RowUuid> {
    match stream.try_next_event().expect("an initial reset") {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            removed,
            ..
        } => {
            assert!(removed.is_empty());
            added.iter().map(|row| row.row_uuid()).collect()
        }
        other => panic!("expected an initial reset, got {other:?}"),
    }
}

/// Many routes subscribe one query shape with distinct bindings. Each
/// binding hydrates exactly its own page; one matching write reaches only
/// its route, as one delta that adds the new row and drops the page's last.
#[test]
fn route_subscription_curve_correctness_smoke() {
    let schema = compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("team", ColumnType::Uuid)
                    .column("rank", ColumnType::BigInt)
                    .policies(allow_all()),
            )
            .build(),
    );
    let db = open(schema, 0, AuthorSubject::SYSTEM);
    let insert_document = |team_index: usize, rank: u64| {
        insert(
            &db,
            "documents",
            document(team_index, rank),
            cells([("team", uuid(team(team_index))), ("rank", int(rank as i64))]),
        );
    };
    for team_index in 0..ROUTES {
        for rank in 0..DOCUMENTS_PER_TEAM {
            insert_document(team_index, rank);
        }
    }
    let query = Query::from("documents")
        .filter(eq(col("team"), param("team")))
        .order_by("rank", OrderDirection::Desc)
        .limit(PAGE);
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut shape = None;
    let mut streams: Vec<SubscriptionStream> = (0..ROUTES)
        .map(|team_index| {
            let prepared = db
                .prepare_query_bound(
                    &query,
                    BTreeMap::from([("team".to_owned(), uuid(team(team_index)))]),
                )
                .expect("prepare route");
            match shape {
                None => shape = Some(prepared.shape().shape_id()),
                Some(id) => assert_eq!(
                    prepared.shape().shape_id(),
                    id,
                    "every binding shares one shape"
                ),
            }
            let mut stream =
                block_on(db.subscribe(&prepared, local.clone())).expect("subscribe route");
            let expected: Vec<RowUuid> = (0..DOCUMENTS_PER_TEAM)
                .rev()
                .take(PAGE)
                .map(|rank| document(team_index, rank))
                .collect();
            assert_eq!(
                take_reset(&mut stream),
                expected,
                "route {team_index} hydrates its own page"
            );
            stream
        })
        .collect();

    insert_document(0, DOCUMENTS_PER_TEAM);
    match streams[0]
        .try_next_event()
        .expect("the matching route changes")
    {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(!reset);
            assert_eq!(
                added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                vec![document(0, DOCUMENTS_PER_TEAM)]
            );
            assert_eq!(
                removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
                vec![document(0, DOCUMENTS_PER_TEAM - PAGE as u64)]
            );
            // Rows that stay on the page may only move down by one.
            assert!(
                updated
                    .iter()
                    .all(|row| row.index == row.previous_index.map_or(row.index, |i| i + 1))
            );
        }
        other => panic!("expected a route delta, got {other:?}"),
    }
    assert!(
        streams[0].try_next_event().is_none(),
        "one write is one event"
    );
    for (team_index, stream) in streams.iter_mut().enumerate().skip(1) {
        assert!(
            stream.try_next_event().is_none(),
            "route {team_index} stays quiet"
        );
    }
}
