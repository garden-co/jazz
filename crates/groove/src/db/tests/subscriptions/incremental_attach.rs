//! Attaching a binding to a live prepared shape.
//!
//! A shape's bindings share one graph. Attaching the Nth binding must give
//! that binding exactly the rows a fresh, independent hydration would give,
//! and must leave every already-live sibling exact. The oracle for each
//! binding is a literal (binding-free) subscription over the same data.

use std::collections::BTreeMap;

use super::*;

const SHAPE: &str = "attach_artist";
const ARTISTS: u64 = 6;

// `Value` is not `Ord`; its debug form is a faithful, ordered row key.
type Rows = BTreeMap<String, i64>;

fn artist_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("artist", ColumnType::U64)])
}

/// `albums` restricted to one artist, carrying the artist as a column.
fn bound_albums() -> GraphBuilder {
    let binding = GraphBuilder::binding_source(SHAPE, artist_descriptor())
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

#[derive(Clone, Copy, Debug)]
enum ShapeKind {
    TopBy,
    Join,
}

impl ShapeKind {
    fn prepared(self) -> GraphBuilder {
        match self {
            Self::TopBy => top_two(bound_albums()),
            Self::Join => with_artist_name(bound_albums()),
        }
    }

    fn literal(self, artist: u64) -> GraphBuilder {
        match self {
            Self::TopBy => top_two(literal_albums(artist)),
            Self::Join => with_artist_name(literal_albums(artist)),
        }
    }
}

fn drain(subscription: &Subscription, rows: &mut Rows) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            *rows.entry(format!("{values:?}")).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
}

struct Live {
    artist: u64,
    prepared: Subscription,
    literal: Subscription,
    prepared_rows: Rows,
    literal_rows: Rows,
}

impl Live {
    fn check(&mut self, kind: ShapeKind, when: &str) {
        drain(&self.prepared, &mut self.prepared_rows);
        drain(&self.literal, &mut self.literal_rows);
        assert_eq!(
            self.prepared_rows, self.literal_rows,
            "{kind:?} artist {} diverged from its fresh hydration {when}",
            self.artist
        );
    }
}

async fn seed(database: &mut Database) {
    let mut batch = database.open_batch();
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
    database.commit_batch(batch).await.unwrap();
}

/// One write between attaches, so each new binding lands on state that has
/// already been maintained incrementally, not just freshly hydrated.
async fn churn(database: &mut Database, step: u64) {
    let mut batch = database.open_batch();
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
    database.commit_batch(batch).await.unwrap();
}

async fn attach(
    database: &mut Database,
    kind: ShapeKind,
    artist: u64,
    shared: Option<PreparedShapeId>,
) -> Live {
    // Jazz prepares one shape per subscription and relies on graph dedup, so
    // exercise both that and several bindings of one prepared shape.
    let shape = match shared {
        Some(shape) => shape,
        None => database
            .prepare_one_sink(kind.prepared(), SHAPE, artist_descriptor(), ["artist"])
            .await
            .unwrap()
            .id(),
    };
    let prepared = database
        .bind_shape_one_sink(shape, &[Value::U64(artist)])
        .await
        .unwrap();
    let literal = database
        .subscribe_one_sink(kind.literal(artist))
        .await
        .unwrap();
    database.drive_progress().await.unwrap();
    Live {
        artist,
        prepared,
        literal,
        prepared_rows: Rows::new(),
        literal_rows: Rows::new(),
    }
}

async fn run(kind: ShapeKind, share_prepared_shape: bool) {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    seed(&mut database).await;
    let shared = if share_prepared_shape {
        Some(
            database
                .prepare_one_sink(kind.prepared(), SHAPE, artist_descriptor(), ["artist"])
                .await
                .unwrap()
                .id(),
        )
    } else {
        None
    };

    let mut live = Vec::<Live>::new();
    for artist in 1..=ARTISTS {
        let mut attached = attach(&mut database, kind, artist, shared).await;
        attached.check(kind, &format!("on attach as binding {artist}"));
        for sibling in &mut live {
            sibling.check(kind, &format!("after binding {artist} attached"));
        }
        live.push(attached);
        churn(&mut database, artist).await;
        database.drive_progress().await.unwrap();
        for subscription in &mut live {
            subscription.check(kind, &format!("after write {artist}"));
        }
    }

    // Retire one binding and re-attach it while its siblings stay live.
    let retired = live.remove(2);
    let artist = retired.artist;
    drop(retired);
    database.drive_progress().await.unwrap();
    churn(&mut database, 40).await;
    let mut reattached = attach(&mut database, kind, artist, shared).await;
    reattached.check(kind, "on re-attach");
    live.push(reattached);
    for step in 41..60 {
        churn(&mut database, step).await;
        database.drive_progress().await.unwrap();
        for subscription in &mut live {
            subscription.check(kind, &format!("after write {step}"));
        }
    }
}

#[futures_test::test]
async fn nth_top_by_binding_matches_fresh_hydration() {
    run(ShapeKind::TopBy, false).await;
}

#[futures_test::test]
async fn nth_top_by_binding_of_one_prepared_shape_matches_fresh_hydration() {
    run(ShapeKind::TopBy, true).await;
}

#[futures_test::test]
async fn nth_join_binding_matches_fresh_hydration() {
    run(ShapeKind::Join, false).await;
}

#[futures_test::test]
async fn nth_join_binding_of_one_prepared_shape_matches_fresh_hydration() {
    run(ShapeKind::Join, true).await;
}
