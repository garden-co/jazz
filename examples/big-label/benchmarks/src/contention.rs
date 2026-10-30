//! Two record labels on one server: one label's staffer searches its whole
//! back catalogue while a staffer of another label works on theirs.
//!
//! BigLabel is multi-tenant. Every organization's releases live in the same
//! table, and a member reads and edits only their organization's releases.
//! Search is `contains` over a lower-cased `search_key`, newest first, as the
//! app's release list does while someone types. A search that matches nothing
//! reads every release of the searching label, which is what makes it slow.
//!
//! The measured requests belong to the other label: opening its newest
//! releases (a remote read) and renaming one release (a write acknowledged by
//! the server). Each runs once with the server otherwise idle, and once while
//! the searching staffer issues searches back to back. The difference is how
//! long one tenant's slow query holds up another tenant's cheap requests.

use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, contains, eq, lit};
use jazz::records::Value as CoreValue;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, DurabilityTier, ObjectId, PolicyExpr, ReadTier, Schema, SchemaBuilder, TableSchema,
    Value, permissions, policy_expr as pe,
};
use jazz_testkit::contention::{Background, ClientThread, ContentionServer};

/// Releases of the label whose staffer searches, in the benchmark.
pub const SEARCHED_RELEASES: usize = 300_000;
/// Releases of the label whose staffer is measured.
pub const OWN_RELEASES: usize = 500;
/// The size of a release page.
pub const PAGE: usize = 50;

const SEARCHER: &str = "6b1a5e2c-0000-4000-8000-000000000001";
const EDITOR: &str = "6b1a5e2c-0000-4000-8000-000000000002";
const SEARCHED_LABEL: usize = 0;
const OWN_LABEL: usize = 1;
const WORDS: [&str; 16] = [
    "midnight",
    "harbour",
    "static",
    "orchard",
    "velvet",
    "signal",
    "canyon",
    "ember",
    "lantern",
    "tide",
    "paper",
    "satellite",
    "meadow",
    "neon",
    "river",
    "glass",
];

/// The server, both staffers, and the release the editor renames.
pub struct ContentionFixture {
    editor: ClientThread,
    searcher: ClientThread,
    renamed: ObjectId,
    renames: u64,
    _server: ContentionServer,
}

impl ContentionFixture {
    /// `searched_releases` sets how long a search takes.
    pub fn new(searched_releases: usize) -> Self {
        let schema = schema();
        let server = ContentionServer::start(schema.clone(), |path| {
            seed(path, &schema, searched_releases)
        });
        let editor = server.connect_user(EDITOR);
        let searcher = server.connect_user(SEARCHER);
        let mut fixture = Self {
            editor,
            searcher,
            renamed: ObjectId::from_uuid(release_row(OWN_LABEL, OWN_RELEASES - 1).0),
            renames: 0,
            _server: server,
        };
        // Warm both request paths, and load the renamed release into the
        // editor's store, before anything is measured.
        fixture.open_own_releases();
        fixture.rename_release();
        fixture
    }

    /// The editor opens their label's newest releases from the server.
    pub fn open_own_releases(&self) -> usize {
        self.editor.run(|client| {
            Box::pin(async move {
                let rows = client
                    .query(own_releases(), ReadTier::Remote)
                    .await
                    .expect("open own releases");
                assert_eq!(rows.len(), PAGE, "a full page of own releases");
                for row in &rows {
                    assert_eq!(
                        row.get("organization_id"),
                        Some(&label_id(OWN_LABEL)),
                        "only the editor's own releases"
                    );
                }
                rows.len()
            })
        })
    }

    /// The editor renames one of their releases and waits until the server
    /// has accepted it.
    pub fn rename_release(&mut self) {
        self.renames += 1;
        let title = format!("Midnight Harbour (take {})", self.renames);
        let renamed = self.renamed;
        self.editor.run(move |client| {
            Box::pin(async move {
                let search_key = title.to_lowercase();
                let tx = client
                    .update(
                        "releases",
                        renamed,
                        vec![
                            ("title".to_owned(), Value::Text(title)),
                            ("search_key".to_owned(), Value::Text(search_key)),
                        ],
                    )
                    .expect("rename release")
                    .expect("the rename is a write");
                client
                    .wait_for_transaction(tx, DurabilityTier::GlobalServer)
                    .await
                    .expect("server accepts the rename");
            })
        });
    }

    /// The other label's staffer searches back to back until stopped. Each
    /// search has a new term, so no earlier result can answer it.
    pub fn start_searching(&self) -> Background {
        self.searcher.repeat(|client, n| {
            Box::pin(async move {
                let rows = client
                    .query(missed_search(n), ReadTier::Remote)
                    .await
                    .expect("search the back catalogue");
                assert!(rows.is_empty(), "the search term matches nothing");
            })
        })
    }

    /// One search that matches, so the searcher provably reads its catalogue.
    pub fn matched_search_count(&self) -> usize {
        self.searcher.run(|client| {
            Box::pin(async move {
                let rows = client
                    .query(search(WORDS[3]), ReadTier::Remote)
                    .await
                    .expect("search the back catalogue");
                for row in &rows {
                    assert_eq!(
                        row.get("organization_id"),
                        Some(&label_id(SEARCHED_LABEL)),
                        "a search sees only the searcher's label"
                    );
                }
                rows.len()
            })
        })
    }
}

fn own_releases() -> Query {
    Query::from("releases")
        .filter(eq(col("organization_id"), lit(label_row(OWN_LABEL).0)))
        .order_by("released_at", OrderDirection::Desc)
        .limit(PAGE)
}

fn search(term: &str) -> Query {
    Query::from("releases")
        .filter(contains(col("search_key"), lit(term.to_owned())))
        .order_by("released_at", OrderDirection::Desc)
        .limit(PAGE)
}

/// A term a staffer might type that no release contains.
fn missed_search(n: u64) -> Query {
    search(&format!("{} remaster {n}", WORDS[n as usize % WORDS.len()]))
}

fn member_of_organization() -> PolicyExpr {
    pe::exists(pe::table("memberships").where_(pe::rel::all_of([
        pe::rel::eq_session("user_id", vec!["claims", "sub"]),
        pe::rel::eq_outer("organization_id", "id"),
    ])))
}

pub fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("organizations")
                .column("name", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read().where_(member_of_organization());
                })),
        )
        .table(
            TableSchema::builder("memberships")
                .fk_column("organization_id", "organizations")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
                })),
        )
        .table(
            TableSchema::builder("releases")
                .fk_column("organization_id", "organizations")
                .column("title", ColumnType::Text)
                .column("search_key", ColumnType::Text)
                .column("released_at", ColumnType::Timestamp)
                .composite_index(["organization_id", "released_at"])
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::allowed_to_read("organization_id"));
                    p.allow_update()
                        .where_(pe::allowed_to_read("organization_id"));
                })),
        )
        .build()
}

/// Both labels and their releases, written straight into the server's store
/// file before the server opens it.
fn seed(path: &Path, schema: &Schema, searched_releases: usize) {
    let db = block_on(Db::open_at(
        DbConfig::new(
            JazzSchema::new(schema).expect("BigLabel contention schema compiles"),
            DbIdentity {
                node: NodeUuid::from_bytes([0xb2; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        FileOptions::default(),
    ))
    .expect("open the server store for seeding");
    for (label, member) in [(SEARCHED_LABEL, SEARCHER), (OWN_LABEL, EDITOR)] {
        insert(
            &db,
            "organizations",
            label_row(label),
            [("name", CoreValue::String(format!("Label {label}")))],
        );
        insert(
            &db,
            "memberships",
            row(0x6b1a_0000 + label as u128),
            [
                ("organization_id", CoreValue::Uuid(label_row(label).0)),
                ("user_id", CoreValue::String(member.to_owned())),
            ],
        );
    }
    // Interleave the labels' releases, as tenants sign up and release over
    // time, so neither label's rows are one contiguous run.
    let total = searched_releases + OWN_RELEASES;
    let stride = total / OWN_RELEASES;
    let mut own = 0;
    let mut searched = 0;
    for start in (0..total).step_by(500) {
        block_on(db.transaction(async |tx| {
            for n in start..(start + 500).min(total) {
                let (label, ordinal) = if n % stride == 0 && own < OWN_RELEASES {
                    own += 1;
                    (OWN_LABEL, own - 1)
                } else {
                    searched += 1;
                    (SEARCHED_LABEL, searched - 1)
                };
                let title = release_title(n);
                tx.insert(
                    "releases",
                    BTreeMap::from([
                        (
                            "organization_id".to_owned(),
                            CoreValue::Uuid(label_row(label).0),
                        ),
                        (
                            "search_key".to_owned(),
                            CoreValue::String(title.to_lowercase()),
                        ),
                        ("title".to_owned(), CoreValue::String(title)),
                        ("released_at".to_owned(), CoreValue::U64(n as u64)),
                    ]),
                    InsertOptions {
                        row_id: Some(release_row(label, ordinal)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .expect("seed releases");
    }
    assert_eq!((own, searched), (OWN_RELEASES, searched_releases));
    block_on(db.close()).expect("close the seeded store");
}

/// Two or three words and a catalogue number, e.g. "Velvet Tide LP 01234".
fn release_title(n: usize) -> String {
    let word = |k: usize| {
        let w = WORDS[(n / k + n * k) % WORDS.len()];
        let mut chars = w.chars();
        chars
            .next()
            .unwrap()
            .to_uppercase()
            .chain(chars)
            .collect::<String>()
    };
    let mut title = format!("{} {}", word(1), word(7));
    if n.is_multiple_of(3) {
        title = format!("{title} {}", word(13));
    }
    format!("{title} LP {n:05}")
}

fn insert<const N: usize>(db: &Db, table: &str, row: RowUuid, cells: [(&str, CoreValue); N]) {
    block_on(
        db.insert(
            table,
            cells
                .into_iter()
                .map(|(column, value)| (column.to_owned(), value))
                .collect(),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ),
    )
    .expect("seed row");
}

fn row(n: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(n))
}

fn label_row(label: usize) -> RowUuid {
    row(0x6b1a_1000 + label as u128)
}

fn release_row(label: usize, ordinal: usize) -> RowUuid {
    row(((0x6b1a_2000 + label as u128) << 64) | ordinal as u128)
}

fn label_id(label: usize) -> Value {
    Value::Uuid(ObjectId::from_uuid(label_row(label).0))
}
