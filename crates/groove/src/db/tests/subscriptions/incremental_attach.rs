//! Attaching a binding to a live prepared shape.
//!
//! A shape's bindings share one graph. Attaching the Nth binding must give
//! that binding exactly the rows a fresh, independent hydration would give,
//! and must leave every already-live sibling exact. The oracle for each
//! binding is a literal (binding-free) subscription per sink over the same
//! data. `Database::live_attaches` shows which bind path ran, since result
//! equality alone cannot.

use std::collections::BTreeMap;

use super::*;

const ARTIST_SHAPE: &str = "attach_artist";
const SEED_SHAPE: &str = "attach_seed";
const ARTISTS: u64 = 6;
const NODES: u64 = 8;

// `Value` is not `Ord`; its debug form is a faithful, ordered row key.
type Rows = BTreeMap<String, i64>;

fn artist_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("artist", ColumnType::U64)])
}

fn seed_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("seed", ColumnType::U64)])
}

/// `albums` restricted to one artist, carrying the artist as a column.
fn bound_albums() -> GraphBuilder {
    let binding = GraphBuilder::binding_source(ARTIST_SHAPE, artist_descriptor())
        .project_fields([ProjectField::named("artist")]);
    GraphBuilder::join(
        binding,
        GraphBuilder::table("albums"),
        ["artist"],
        ["artist_id"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.artist", "artist"),
    ])
}

fn literal_albums(artist: u64) -> GraphBuilder {
    GraphBuilder::table("albums")
        .filter(PredicateExpr::eq("artist_id", Value::U64(artist)))
        .project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("artist", Value::U64(artist)),
        ])
}

/// The newest two titles per artist.
fn top_two(input: GraphBuilder) -> GraphBuilder {
    GraphBuilder::top_by(
        input,
        ["artist"],
        [TopByOrder::desc("title")],
        ["id"],
        0,
        TopByLimit::Finite(2),
    )
}

/// Each album with its artist's name.
fn with_artist_name(input: GraphBuilder) -> GraphBuilder {
    GraphBuilder::join(input, GraphBuilder::table("artists"), ["artist"], ["id"]).project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("right.name", "name"),
        ProjectField::renamed("left.artist", "artist"),
    ])
}

/// Docs whose team is reachable from `seed`, with `seed` as the route.
fn bound_reach_docs() -> GraphBuilder {
    let seed = GraphBuilder::binding_source(SEED_SHAPE, seed_descriptor()).project_fields([
        ProjectField::renamed("seed", "seed"),
        ProjectField::renamed("seed", "dst"),
    ]);
    reach_docs(seed)
}

fn literal_reach_docs(seed: u64) -> GraphBuilder {
    let seed = GraphBuilder::values(
        RecordDescriptor::new([("seed", ColumnType::U64), ("dst", ColumnType::U64)]),
        [[Value::U64(seed), Value::U64(seed)]],
    )
    .unwrap();
    reach_docs(seed)
}

fn reach_docs(seed: GraphBuilder) -> GraphBuilder {
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        RecordDescriptor::new([("seed", ColumnType::U64), ("dst", ColumnType::U64)]),
    );
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("edges").project(["src", "dst"]),
        ["dst"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    let reach = GraphBuilder::recursive(seed, step, "frontier", 16);
    GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"]).project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.team", "team"),
        ProjectField::renamed("right.seed", "seed"),
    ])
}

#[derive(Clone, Copy, Debug)]
enum ShapeKind {
    TopBy,
    Join,
    /// Two sinks over one binding source: albums and their artist names.
    MultiSink,
    /// A recursive reachability closure joined to docs.
    Recursive,
}

impl ShapeKind {
    fn keys(self) -> u64 {
        match self {
            Self::Recursive => NODES,
            _ => ARTISTS,
        }
    }

    fn source(self) -> (&'static str, RecordDescriptor) {
        match self {
            Self::Recursive => (SEED_SHAPE, seed_descriptor()),
            _ => (ARTIST_SHAPE, artist_descriptor()),
        }
    }

    /// `(sink, prepared graph, route field, public fields)`.
    fn terminals(self) -> Vec<(&'static str, GraphBuilder, &'static str, Vec<&'static str>)> {
        match self {
            Self::TopBy => vec![(
                "rows",
                top_two(bound_albums()),
                "artist",
                vec!["id", "title", "artist"],
            )],
            Self::Join => vec![(
                "rows",
                with_artist_name(bound_albums()),
                "artist",
                vec!["id", "name", "artist"],
            )],
            Self::MultiSink => vec![
                ("albums", bound_albums(), "artist", vec!["id", "title"]),
                (
                    "names",
                    with_artist_name(bound_albums()),
                    "artist",
                    vec!["id", "name"],
                ),
            ],
            Self::Recursive => vec![("docs", bound_reach_docs(), "seed", vec!["id", "team"])],
        }
    }

    fn literal(self, sink: &str, key: u64) -> GraphBuilder {
        match (self, sink) {
            (Self::TopBy, _) => top_two(literal_albums(key)),
            (Self::Join, _) => with_artist_name(literal_albums(key)),
            (Self::MultiSink, "albums") => literal_albums(key).project(["id", "title"]),
            (Self::MultiSink, _) => with_artist_name(literal_albums(key)).project(["id", "name"]),
            (Self::Recursive, _) => literal_reach_docs(key).project(["id", "team"]),
        }
    }

    fn schema(self) -> (DatabaseSchema, &'static [&'static str]) {
        match self {
            Self::Recursive => (edges_docs_schema(), &["edges", "docs"]),
            _ => (albums_artists_schema(), &["albums", "artists"]),
        }
    }

    async fn prepare(self, database: &mut Database) -> PreparedShapeId {
        let (source, descriptor) = self.source();
        let terminals = self
            .terminals()
            .into_iter()
            .map(|(sink, graph, route, public)| {
                RoutedMultisinkTerminal::new(sink, graph, [route], public)
            })
            .collect::<Vec<_>>();
        database
            .prepare(terminals, source, descriptor)
            .await
            .unwrap()
            .id()
    }

    async fn seed(self, database: &mut Database) {
        let mut batch = database.open_batch();
        match self {
            Self::Recursive => {
                for node in 1..=NODES {
                    batch.insert("docs", vec![Value::U64(100 + node), Value::U64(node)]);
                }
                for id in 1..=6 {
                    insert_edge(&mut batch, id, id, id % NODES + 1);
                }
            }
            _ => {
                for artist in 1..=ARTISTS {
                    batch.insert(
                        "artists",
                        vec![
                            Value::U64(artist),
                            Value::String(format!("artist-{artist}")),
                        ],
                    );
                }
                for id in 1..=24_u64 {
                    batch.insert(
                        "albums",
                        vec![
                            Value::U64(id),
                            Value::U64(id % ARTISTS + 1),
                            Value::String(format!("title-{:02}", id * 7 % 24)),
                        ],
                    );
                }
            }
        }
        database.commit_batch(batch).await.unwrap();
    }

    /// One write, so bindings land on state that has already been
    /// maintained incrementally, not just freshly hydrated.
    async fn churn(self, database: &mut Database, step: u64) {
        let mut batch = database.open_batch();
        match self {
            Self::Recursive => {
                // Re-point one of the seeded edges, so reachability both
                // grows and shrinks.
                batch.update(
                    "edges",
                    vec![
                        Value::U64(step % 6 + 1),
                        Value::U64(step * 5 % NODES + 1),
                        Value::U64(step * 3 % NODES + 1),
                    ],
                );
            }
            _ => {
                let id = step * 5 % 24 + 1;
                match step % 3 {
                    0 => batch.update(
                        "albums",
                        vec![
                            Value::U64(id),
                            Value::U64(step % ARTISTS + 1),
                            Value::String(format!("moved-{step:02}")),
                        ],
                    ),
                    1 => batch.insert(
                        "albums",
                        vec![
                            Value::U64(100 + step),
                            Value::U64(step * 3 % ARTISTS + 1),
                            Value::String(format!("title-{:02}", 30 + step)),
                        ],
                    ),
                    _ => batch.delete("albums", PrimaryKeyValue::U64(id)),
                }
            }
        }
        database.commit_batch(batch).await.unwrap();
    }
}

fn drain_one(subscription: &Subscription, rows: &mut Rows) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *rows.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
}

struct Live {
    key: u64,
    prepared: MultisinkSubscription,
    literals: Vec<(String, Subscription)>,
    prepared_rows: BTreeMap<String, Rows>,
    literal_rows: BTreeMap<String, Rows>,
}

impl Live {
    fn check(&mut self, kind: ShapeKind, when: &str) {
        while let Ok(deltas) = self.prepared.try_recv() {
            for (sink, deltas) in deltas.sinks {
                let rows = self.prepared_rows.entry(sink).or_default();
                for (values, weight) in deltas.to_values().unwrap() {
                    *rows.entry(format!("{values:?}")).or_default() += weight;
                }
                rows.retain(|_, weight| *weight != 0);
            }
        }
        for (sink, literal) in &self.literals {
            drain_one(literal, self.literal_rows.entry(sink.clone()).or_default());
        }
        for (sink, _) in &self.literals {
            assert_eq!(
                self.prepared_rows.get(sink).cloned().unwrap_or_default(),
                self.literal_rows.get(sink).cloned().unwrap_or_default(),
                "{kind:?} key {} sink {sink} diverged from its fresh hydration {when}",
                self.key
            );
        }
    }
}

struct Harness {
    kind: ShapeKind,
    database: Database,
    /// One prepared shape bound repeatedly, or a fresh `prepare` per bind
    /// (which Jazz does, relying on graph dedup).
    shared: Option<PreparedShapeId>,
    live: Vec<Live>,
}

impl Harness {
    async fn new(kind: ShapeKind, share_prepared_shape: bool) -> Self {
        let (schema, tables) = kind.schema();
        let storage = MemoryStorage::new(tables).expect("valid memory storage families");
        let mut database = Database::new(schema, storage).await.unwrap();
        kind.seed(&mut database).await;
        let shared = if share_prepared_shape {
            Some(kind.prepare(&mut database).await)
        } else {
            None
        };
        Self {
            kind,
            database,
            shared,
            live: Vec::new(),
        }
    }

    /// Bind `key` without first driving whatever earlier steps left pending,
    /// then drive and check every live binding. Returns whether this bind
    /// took the live-attach path.
    async fn bind(&mut self, key: u64, when: &str) -> bool {
        let shape = match self.shared {
            Some(shape) => shape,
            None => self.kind.prepare(&mut self.database).await,
        };
        let attaches = self.database.live_attaches();
        let prepared = self
            .database
            .bind_shape(shape, &[Value::U64(key)])
            .await
            .unwrap();
        let attached = self.database.live_attaches() > attaches;
        let mut literals = Vec::new();
        for (sink, ..) in self.kind.terminals() {
            let literal = self
                .database
                .subscribe_one_sink(self.kind.literal(sink, key))
                .await
                .unwrap();
            literals.push((sink.to_owned(), literal));
        }
        self.live.push(Live {
            key,
            prepared,
            literals,
            prepared_rows: BTreeMap::new(),
            literal_rows: BTreeMap::new(),
        });
        self.drive_and_check(when).await;
        attached
    }

    /// Drop a binding without driving, so its retraction may stay queued.
    fn drop_key(&mut self, key: u64) {
        self.live.retain(|live| live.key != key);
    }

    async fn churn(&mut self, step: u64) {
        self.kind.churn(&mut self.database, step).await;
    }

    async fn drive_and_check(&mut self, when: &str) {
        self.database.drive_progress().await.unwrap();
        for live in &mut self.live {
            live.check(self.kind, when);
        }
    }
}

async fn attach_in_turn(kind: ShapeKind, share_prepared_shape: bool) {
    let mut harness = Harness::new(kind, share_prepared_shape).await;
    for key in 1..=kind.keys() {
        let attached = harness.bind(key, &format!("after binding {key}")).await;
        assert_eq!(
            attached,
            key > 1,
            "{kind:?}: binding {key} onto a settled shape must attach live"
        );
        harness.churn(key).await;
        harness.drive_and_check(&format!("after write {key}")).await;
    }

    // Retire one binding and re-attach it while its siblings stay live.
    harness.drop_key(3);
    harness.drive_and_check("after retiring 3").await;
    harness.churn(40).await;
    harness.drive_and_check("after write 40").await;
    assert!(
        harness.bind(3, "on re-attach").await,
        "{kind:?}: re-attaching a retired binding onto a settled shape must attach live"
    );
    for step in 41..60 {
        harness.churn(step).await;
        harness
            .drive_and_check(&format!("after write {step}"))
            .await;
    }
}

#[futures_test::test]
async fn nth_top_by_binding_matches_fresh_hydration() {
    attach_in_turn(ShapeKind::TopBy, false).await;
}

#[futures_test::test]
async fn nth_top_by_binding_of_one_prepared_shape_matches_fresh_hydration() {
    attach_in_turn(ShapeKind::TopBy, true).await;
}

#[futures_test::test]
async fn nth_join_binding_matches_fresh_hydration() {
    attach_in_turn(ShapeKind::Join, false).await;
}

#[futures_test::test]
async fn nth_join_binding_of_one_prepared_shape_matches_fresh_hydration() {
    attach_in_turn(ShapeKind::Join, true).await;
}

#[futures_test::test]
async fn nth_multi_sink_binding_matches_fresh_hydration() {
    attach_in_turn(ShapeKind::MultiSink, true).await;
}

#[futures_test::test]
async fn nth_recursive_binding_matches_fresh_hydration() {
    attach_in_turn(ShapeKind::Recursive, true).await;
}

/// A binding dropped and written past without driving leaves its retraction
/// queued. The next bind must not fully hydrate without it and then have the
/// retraction applied on top, or a later live attach builds on that state and
/// never sees its own later edits.
async fn rebind_after_queued_retraction(kind: ShapeKind) {
    let mut harness = Harness::new(kind, true).await;
    harness.bind(2, "after binding 2").await;
    harness.drop_key(2);
    // A write touching key 2, committed but not driven.
    let mut batch = harness.database.open_batch();
    match kind {
        ShapeKind::Recursive => insert_edge(&mut batch, 30, 2, 6),
        _ => batch.insert(
            "albums",
            vec![
                Value::U64(90),
                Value::U64(2),
                Value::String("title-90".to_owned()),
            ],
        ),
    }
    harness.database.commit_batch(batch).await.unwrap();
    harness.bind(6, "after binding 6").await;
    assert!(
        harness.bind(2, "after re-binding 2").await,
        "{kind:?}: re-binding 2 beside a settled 6 must attach live"
    );
    for step in 0..24 {
        harness.churn(step).await;
        harness
            .drive_and_check(&format!("after write {step}"))
            .await;
    }
}

#[futures_test::test]
async fn top_by_rebind_after_queued_retraction_keeps_receiving_edits() {
    rebind_after_queued_retraction(ShapeKind::TopBy).await;
}

#[futures_test::test]
async fn join_rebind_after_queued_retraction_keeps_receiving_edits() {
    rebind_after_queued_retraction(ShapeKind::Join).await;
}

#[futures_test::test]
async fn multi_sink_rebind_after_queued_retraction_keeps_receiving_edits() {
    rebind_after_queued_retraction(ShapeKind::MultiSink).await;
}

#[futures_test::test]
async fn recursive_rebind_after_queued_retraction_keeps_receiving_edits() {
    rebind_after_queued_retraction(ShapeKind::Recursive).await;
}

/// Seeded interleavings of binds, undriven drops and writes that are driven
/// only some of the time, checked against literal subscriptions after every
/// drive.
async fn bind_drop_churn(kind: ShapeKind, share_prepared_shape: bool) {
    let mut attaches = 0;
    for seed in 0..12_u64 {
        let mut harness = Harness::new(kind, share_prepared_shape).await;
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for step in 0..40 {
            let key = next() % kind.keys() + 1;
            let when = format!("seed {seed} step {step}");
            match next() % 5 {
                0 | 1 if harness.live.iter().all(|live| live.key != key) => {
                    if harness.bind(key, &when).await {
                        attaches += 1;
                    }
                }
                2 => harness.drop_key(key),
                _ => {
                    harness.churn(step).await;
                    if next() % 2 == 0 {
                        harness.drive_and_check(&when).await;
                    }
                }
            }
        }
        harness.drive_and_check(&format!("seed {seed} end")).await;
    }
    assert!(
        attaches > 0,
        "{kind:?}: churn never exercised a live attach"
    );
}

#[futures_test::test]
async fn top_by_bind_drop_churn_matches_fresh_hydration() {
    bind_drop_churn(ShapeKind::TopBy, true).await;
}

#[futures_test::test]
async fn join_bind_drop_churn_matches_fresh_hydration() {
    bind_drop_churn(ShapeKind::Join, false).await;
}

#[futures_test::test]
async fn multi_sink_bind_drop_churn_matches_fresh_hydration() {
    bind_drop_churn(ShapeKind::MultiSink, true).await;
}

#[futures_test::test]
async fn recursive_bind_drop_churn_matches_fresh_hydration() {
    bind_drop_churn(ShapeKind::Recursive, true).await;
}
