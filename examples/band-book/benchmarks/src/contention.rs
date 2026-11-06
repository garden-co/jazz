//! Two bands on one server: a member of a big band opens its workspace while
//! a member of another band works on their own pages.
//!
//! BandBook's sidebar loads every page of the open workspace, oldest first,
//! with no page bound. A page is readable by its owner or by any member of
//! its workspace. In a workspace with tens of thousands of pages, opening it
//! is a large read: every page is checked against the policy and sent.
//!
//! The measured requests belong to the other band's member: opening their
//! newest pages (a remote read) and checking off one page (a write
//! acknowledged by the server). Each runs once with the server otherwise
//! idle, and once while the big band's member reopens the workspace back to
//! back on two devices. Each open is a remote read, so the server checks and
//! sends every page each time.

use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value as CoreValue;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, DurabilityTier, ObjectId, PolicyExpr, ReadTier, Schema, SchemaBuilder, TableSchema,
    Value, permissions, policy_expr as pe,
};
use jazz_testkit::contention::{Background, ClientThread, ContentionServer};

/// Pages in the big band's workspace, in the benchmark.
pub const WORKSPACE_PAGES: usize = 5_000;
/// Pages in the measured member's workspace, all theirs.
pub const OWN_PAGES: usize = 200;
/// The size of a page list.
pub const LIST: usize = 50;

const BROWSER: &str = "b00cb00c-0000-4000-8000-000000000001";
/// Devices of the big band's member that open the workspace at once.
pub const BROWSERS: usize = 2;
const WRITER: &str = "b00cb00c-0000-4000-8000-000000000002";
const BIG_BAND: usize = 0;
const OWN_BAND: usize = 1;

/// The server, both members, and the page the writer checks off.
pub struct ContentionFixture {
    writer: ClientThread,
    browsers: Vec<ClientThread>,
    checked: ObjectId,
    checks: u64,
    workspace_pages: usize,
    _server: ContentionServer,
}

impl ContentionFixture {
    /// `workspace_pages` sets how long opening the big workspace takes.
    pub fn new(workspace_pages: usize) -> Self {
        let schema = schema();
        let server =
            ContentionServer::start(schema.clone(), |path| seed(path, &schema, workspace_pages));
        let writer = server.connect_user(WRITER);
        let browsers = (0..BROWSERS)
            .map(|_| server.connect_user(BROWSER))
            .collect();
        let mut fixture = Self {
            writer,
            browsers,
            checked: ObjectId::from_uuid(page_row(OWN_BAND, OWN_PAGES - 1).0),
            checks: 0,
            workspace_pages,
            _server: server,
        };
        // Warm both request paths, and load the checked page into the
        // writer's store, before anything is measured.
        fixture.open_own_pages();
        fixture.check_off_page();
        fixture
    }

    /// The writer opens their newest pages from the server.
    pub fn open_own_pages(&self) -> usize {
        self.writer.run(|client| {
            Box::pin(async move {
                let rows = client
                    .query(own_pages(), ReadTier::Remote)
                    .await
                    .expect("open own pages");
                assert_eq!(rows.len(), LIST, "a full list of own pages");
                for row in &rows {
                    assert_eq!(row.get("owner_id"), Some(&Value::Text(WRITER.to_owned())));
                }
                rows.len()
            })
        })
    }

    /// The writer toggles one page's checkbox and waits until the server has
    /// accepted it.
    pub fn check_off_page(&mut self) {
        self.checks += 1;
        let done = self.checks % 2 == 1;
        let updated_at = 1_000_000 + self.checks;
        let checked = self.checked;
        self.writer.run(move |client| {
            Box::pin(async move {
                let tx = client
                    .update(
                        "pages",
                        checked,
                        vec![
                            ("done".to_owned(), Value::Boolean(done)),
                            ("updated_at".to_owned(), Value::Timestamp(updated_at)),
                        ],
                    )
                    .expect("check off page")
                    .expect("the check is a write");
                client
                    .wait_for_transaction(tx, DurabilityTier::GlobalServer)
                    .await
                    .expect("server accepts the check");
            })
        });
    }

    /// The big band's member opens the workspace back to back on every
    /// device until stopped.
    pub fn start_browsing(&self) -> Browsing {
        let pages = self.workspace_pages;
        Browsing(
            self.browsers
                .iter()
                .map(|browser| {
                    browser.repeat(move |client, _| {
                        Box::pin(async move {
                            let rows = client
                                .query(workspace_sidebar(), ReadTier::Remote)
                                .await
                                .expect("open the workspace");
                            assert_eq!(rows.len(), pages, "the sidebar lists every page");
                        })
                    })
                })
                .collect(),
        )
    }
}

/// Workspace opens running on every browsing device.
pub struct Browsing(Vec<Background>);

impl Browsing {
    /// Stop every device and return the duration of each open.
    pub fn stop(self) -> Vec<std::time::Duration> {
        self.0.into_iter().flat_map(Background::stop).collect()
    }
}

fn own_pages() -> Query {
    Query::from("pages")
        .filter(eq(col("owner_id"), lit(WRITER.to_owned())))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(LIST)
}

fn workspace_sidebar() -> Query {
    Query::from("pages")
        .filter(eq(col("workspace_id"), lit(workspace_row(BIG_BAND).0)))
        .order_by("updated_at", OrderDirection::Asc)
}

fn member_of_workspace() -> PolicyExpr {
    pe::exists(pe::table("members").where_(pe::rel::all_of([
        pe::rel::eq_session("user_id", vec!["claims", "sub"]),
        pe::rel::eq_outer("workspace_id", "id"),
    ])))
}

fn owner_or_workspace_member() -> PolicyExpr {
    pe::any_of([
        pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
        pe::allowed_to_read("workspace_id"),
    ])
}

pub fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("workspaces")
                .column("name", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read().where_(member_of_workspace());
                })),
        )
        .table(
            TableSchema::builder("members")
                .fk_column("workspace_id", "workspaces")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
                })),
        )
        .table(
            TableSchema::builder("pages")
                .fk_column("workspace_id", "workspaces")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .column("updated_at", ColumnType::Timestamp)
                .composite_index(["workspace_id", "updated_at"])
                .composite_index(["owner_id", "updated_at"])
                .policies(permissions(|p| {
                    p.allow_read().where_(owner_or_workspace_member());
                    p.allow_update().where_(owner_or_workspace_member());
                })),
        )
        .build()
}

/// Both workspaces and their pages, written straight into the server's store
/// file before the server opens it.
fn seed(path: &Path, schema: &Schema, workspace_pages: usize) {
    let db = block_on(Db::open_at(
        DbConfig::new(
            JazzSchema::new(schema).expect("BandBook contention schema compiles"),
            DbIdentity {
                node: NodeUuid::from_bytes([0xbb; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        FileOptions::default(),
    ))
    .expect("open the server store for seeding");
    for (band, member) in [(BIG_BAND, BROWSER), (OWN_BAND, WRITER)] {
        insert(
            &db,
            "workspaces",
            workspace_row(band),
            [("name", CoreValue::String(format!("Band {band}")))],
        );
        insert(
            &db,
            "members",
            row(0xb00c_0000 + band as u128),
            [
                ("workspace_id", CoreValue::Uuid(workspace_row(band).0)),
                ("user_id", CoreValue::String(member.to_owned())),
            ],
        );
    }
    // The big band's pages belong to its members, sixteen of whom never
    // signed in here; every page is visible to the browsing member through
    // the workspace.
    let total = workspace_pages + OWN_PAGES;
    let stride = total / OWN_PAGES;
    let mut own = 0;
    let mut big = 0;
    for start in (0..total).step_by(500) {
        block_on(db.transaction(async |tx| {
            for n in start..(start + 500).min(total) {
                let (band, ordinal, owner) = if n % stride == 0 && own < OWN_PAGES {
                    own += 1;
                    (OWN_BAND, own - 1, WRITER.to_owned())
                } else {
                    big += 1;
                    (
                        BIG_BAND,
                        big - 1,
                        format!("b00cb00c-0000-4000-8000-1000000000{:02}", n % 16),
                    )
                };
                tx.insert(
                    "pages",
                    BTreeMap::from([
                        (
                            "workspace_id".to_owned(),
                            CoreValue::Uuid(workspace_row(band).0),
                        ),
                        ("owner_id".to_owned(), CoreValue::String(owner)),
                        (
                            "title".to_owned(),
                            CoreValue::String(format!("Setlist notes {n:05}")),
                        ),
                        ("done".to_owned(), CoreValue::Bool(n % 3 == 0)),
                        ("updated_at".to_owned(), CoreValue::U64(n as u64)),
                    ]),
                    InsertOptions {
                        row_id: Some(page_row(band, ordinal)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .expect("seed pages");
    }
    assert_eq!((own, big), (OWN_PAGES, workspace_pages));
    block_on(db.close()).expect("close the seeded store");
}

fn insert<const N: usize>(db: &Db, table: &str, id: RowUuid, cells: [(&str, CoreValue); N]) {
    block_on(
        db.insert(
            table,
            cells
                .into_iter()
                .map(|(column, value)| (column.to_owned(), value))
                .collect(),
            InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .expect("seed row");
}

fn row(n: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(n))
}

fn workspace_row(band: usize) -> RowUuid {
    row(0xb00c_1000 + band as u128)
}

fn page_row(band: usize, ordinal: usize) -> RowUuid {
    row(((0xb00c_2000 + band as u128) << 64) | ordinal as u128)
}
