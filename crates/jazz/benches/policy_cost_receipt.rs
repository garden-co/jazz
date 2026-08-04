//! Current-core policy cost and write-to-reader visibility receipt.
//!
//! The five lanes use identical deterministic rows and identities. Only the
//! cumulative document read policy changes, so the receipt attributes policy
//! complexity separately from row and subscription fan-out.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::{OrderDirection, PolicyBranch, Query, claim, col, eq, in_list, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;
use serde::Serialize;

const ORGANIZATIONS: &str = "policy_organizations";
const TEAMS: &str = "policy_teams";
const TEAM_MEMBERSHIPS: &str = "policy_team_memberships";
const ORGANIZATION_MEMBERSHIPS: &str = "policy_organization_memberships";
const DOCUMENTS: &str = "policy_documents";
const DOCUMENT_ACLS: &str = "policy_document_acls";
const DOCUMENT_COUNT: usize = 1_200;
const PAGE_SIZE: usize = 100;

const WRITER: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000001"));
const MEMBER: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000002"));
const ORG_ADMIN: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000003"));
const ACL_READER: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000004"));
const PUBLIC_READER: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000005"));
const TRUSTED_ADMIN: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000006"));
const LIFECYCLE_READER: AuthorId = AuthorId(uuid::uuid!("71000000-0000-0000-0000-000000000007"));

type BenchDb = Db<MemoryStorage>;

#[derive(Clone, Copy)]
struct Document {
    row: RowUuid,
    organization: RowUuid,
    team: RowUuid,
    updated_at: u64,
    public: bool,
}

struct Fixture {
    organizations: [RowUuid; 2],
    teams: [RowUuid; 2],
    documents: Vec<Document>,
    acl_rows: BTreeSet<RowUuid>,
}

#[derive(Serialize)]
struct Receipt {
    scenario: &'static str,
    rows: usize,
    page_size: usize,
    tiers: Vec<TierReceipt>,
    lifecycle: LifecycleReceipt,
    ok: bool,
}

#[derive(Serialize)]
struct TierReceipt {
    branches: usize,
    policy: &'static str,
    seed_us: u64,
    reads_us: u64,
    identities: Vec<IdentityReceipt>,
    exact: bool,
}

#[derive(Serialize)]
struct IdentityReceipt {
    access_path: &'static str,
    elapsed_us: u64,
    rows: usize,
    digest: String,
    exact: bool,
}

#[derive(Serialize)]
struct LifecycleReceipt {
    grant_us: u64,
    grant_exact: bool,
    revoke_us: u64,
    revoke_exact: bool,
    restore_us: u64,
    restore_exact: bool,
    move_out_us: u64,
    move_out_exact: bool,
    move_back_us: u64,
    move_back_exact: bool,
    exact: bool,
}

fn main() {
    let mut tiers = Vec::new();
    let mut all_exact = true;
    for branches in 1..=5 {
        let (tier, _, _) = run_tier(branches);
        all_exact &= tier.exact;
        tiers.push(tier);
    }
    let (_, db, fixture) = run_tier(5);
    let lifecycle = run_lifecycle(&db, &fixture);
    all_exact &= lifecycle.exact;

    println!(
        "{}",
        serde_json::to_string(&Receipt {
            scenario: "policy_cost_receipt",
            rows: DOCUMENT_COUNT,
            page_size: PAGE_SIZE,
            tiers,
            lifecycle,
            ok: all_exact,
        })
        .expect("serialize policy receipt")
    );
    assert!(all_exact, "policy receipt correctness gate failed");
}

fn run_tier(branches: usize) -> (TierReceipt, BenchDb, Fixture) {
    let db = open_db(branches);
    let fixture = build_fixture();
    let seed_started = Instant::now();
    seed_fixture(&db, &fixture);
    let seed_us = micros(seed_started.elapsed());
    db.set_identity_claims(
        TRUSTED_ADMIN,
        BTreeMap::from([("isAdmin".to_owned(), Value::Bool(true))]),
    );

    let prepared = db
        .prepare_query(
            &Query::from(DOCUMENTS)
                .order_by("updated_at", OrderDirection::Desc)
                .limit(PAGE_SIZE),
        )
        .expect("prepare policy receipt query");
    let reads_started = Instant::now();
    let identities = [
        ("team_membership", MEMBER),
        ("organization_admin", ORG_ADMIN),
        ("direct_acl", ACL_READER),
        ("public_published", PUBLIC_READER),
        ("trusted_admin", TRUSTED_ADMIN),
    ]
    .into_iter()
    .map(|(access_path, identity)| {
        let expected = expected_page(&fixture, branches, identity);
        let started = Instant::now();
        let actual = block_on(db.all_for_identity(&prepared, local_opts(), identity))
            .expect("read policy tier")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        IdentityReceipt {
            access_path,
            elapsed_us: micros(started.elapsed()),
            rows: actual.len(),
            digest: digest(&actual),
            exact: actual == expected,
        }
    })
    .collect::<Vec<_>>();
    let reads_us = micros(reads_started.elapsed());
    let exact = identities.iter().all(|identity| identity.exact);

    (
        TierReceipt {
            branches,
            policy: match branches {
                1 => "membership",
                2 => "membership+organization_admin",
                3 => "membership+organization_admin+direct_acl",
                4 => "membership+organization_admin+direct_acl+public_published",
                5 => "membership+organization_admin+direct_acl+public_published+trusted_admin",
                _ => unreachable!(),
            },
            seed_us,
            reads_us,
            identities,
            exact,
        },
        db,
        fixture,
    )
}

fn run_lifecycle(db: &BenchDb, fixture: &Fixture) -> LifecycleReceipt {
    let prepared = db
        .prepare_query(
            &Query::from(DOCUMENTS)
                .order_by("updated_at", OrderDirection::Desc)
                .limit(PAGE_SIZE),
        )
        .expect("prepare lifecycle query");
    let mut stream = block_on(db.subscribe_for_identity(&prepared, local_opts(), LIFECYCLE_READER))
        .expect("subscribe lifecycle reader");
    let mut observed = take_reset(&mut stream);
    let mut exact = observed_page(&observed) == expected_page(fixture, 5, LIFECYCLE_READER);

    let membership = tagged_row(0x74, 9_000);
    let grant_started = Instant::now();
    db.insert_with_id(
        TEAM_MEMBERSHIPS,
        membership,
        membership_cells(fixture.teams[0], LIFECYCLE_READER),
    )
    .expect("grant membership");
    let grant_us = micros(grant_started.elapsed());
    apply_events(&mut stream, &mut observed);
    let grant_exact = observed_page(&observed) == expected_member_page(fixture, fixture.teams[0]);
    exact &= grant_exact;

    let revoke_started = Instant::now();
    db.delete(TEAM_MEMBERSHIPS, membership)
        .expect("revoke membership");
    let revoke_us = micros(revoke_started.elapsed());
    apply_events(&mut stream, &mut observed);
    let revoke_exact = observed_page(&observed) == expected_page(fixture, 5, LIFECYCLE_READER);
    exact &= revoke_exact;

    let restore_started = Instant::now();
    db.insert_with_id(
        TEAM_MEMBERSHIPS,
        tagged_row(0x74, 9_001),
        membership_cells(fixture.teams[0], LIFECYCLE_READER),
    )
    .expect("restore membership");
    let restore_us = micros(restore_started.elapsed());
    apply_events(&mut stream, &mut observed);
    let restore_exact = observed_page(&observed) == expected_member_page(fixture, fixture.teams[0]);
    exact &= restore_exact;

    let moved = fixture
        .documents
        .iter()
        .filter(|document| document.team == fixture.teams[0])
        .max_by_key(|document| (document.updated_at, document.row))
        .copied()
        .expect("team has document");
    let move_out_started = Instant::now();
    db.update(
        DOCUMENTS,
        moved.row,
        BTreeMap::from([
            (
                "organization".to_owned(),
                Value::Uuid(fixture.organizations[1].0),
            ),
            ("team".to_owned(), Value::Uuid(fixture.teams[1].0)),
        ]),
    )
    .expect("move document out of scope");
    let move_out_us = micros(move_out_started.elapsed());
    apply_events(&mut stream, &mut observed);
    let mut moved_fixture = fixture.documents.clone();
    let changed = moved_fixture
        .iter_mut()
        .find(|document| document.row == moved.row)
        .expect("moved oracle document");
    changed.team = fixture.teams[1];
    changed.organization = fixture.organizations[1];
    let move_out_exact =
        observed_page(&observed) == expected_member_page_from(&moved_fixture, fixture.teams[0]);
    exact &= move_out_exact;

    let move_back_started = Instant::now();
    db.update(
        DOCUMENTS,
        moved.row,
        BTreeMap::from([
            (
                "organization".to_owned(),
                Value::Uuid(fixture.organizations[0].0),
            ),
            ("team".to_owned(), Value::Uuid(fixture.teams[0].0)),
        ]),
    )
    .expect("move document back into scope");
    let move_back_us = micros(move_back_started.elapsed());
    apply_events(&mut stream, &mut observed);
    let move_back_exact =
        observed_page(&observed) == expected_member_page(fixture, fixture.teams[0]);
    exact &= move_back_exact;

    LifecycleReceipt {
        grant_us,
        grant_exact,
        revoke_us,
        revoke_exact,
        restore_us,
        restore_exact,
        move_out_us,
        move_out_exact,
        move_back_us,
        move_back_exact,
        exact,
    }
}

fn open_db(branches: usize) -> BenchDb {
    let schema = schema(branches);
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x7100 + branches as u64)),
    ))
    .expect("open policy receipt db")
}

fn schema(branches: usize) -> JazzSchema {
    let mut policy = Query::from(DOCUMENTS).join_via_column(
        TEAM_MEMBERSHIPS,
        "team",
        "team",
        [
            eq(col("user"), claim("sub")),
            eq(col("active"), lit(true)),
            in_list(col("role"), [lit("viewer"), lit("admin")]),
        ],
    );
    if branches >= 2 {
        policy = policy.policy_branch(alternative(Query::from(DOCUMENTS).join_via_column(
            ORGANIZATION_MEMBERSHIPS,
            "organization",
            "organization",
            [
                eq(col("user"), claim("sub")),
                eq(col("active"), lit(true)),
                eq(col("all_teams"), lit(true)),
                eq(col("role"), lit("admin")),
            ],
        )));
    }
    if branches >= 3 {
        policy = policy.policy_branch(alternative(Query::from(DOCUMENTS).join_via(
            DOCUMENT_ACLS,
            "document",
            [eq(col("user"), claim("sub")), eq(col("active"), lit(true))],
        )));
    }
    if branches >= 4 {
        policy = policy.policy_branch(alternative(
            Query::from(DOCUMENTS)
                .filter(eq(col("visibility"), lit("public")))
                .filter(eq(col("published"), lit(true))),
        ));
    }
    if branches >= 5 {
        policy = policy.policy_branch(alternative(
            Query::from(DOCUMENTS).filter(eq(claim("isAdmin"), lit(true))),
        ));
    }

    JazzSchema::new([
        public_table(
            ORGANIZATIONS,
            [ColumnSchema::new("name", ColumnType::String)],
        ),
        public_table(
            TEAMS,
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_reference("organization", ORGANIZATIONS),
        TableSchema::new(
            TEAM_MEMBERSHIPS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
                ColumnSchema::new("active", ColumnType::Bool),
            ],
        )
        .with_reference("team", TEAMS)
        .with_read_policy(Policy::owner_only(TEAM_MEMBERSHIPS, "user"))
        .with_write_policy(Policy::public()),
        public_table(
            ORGANIZATION_MEMBERSHIPS,
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
                ColumnSchema::new("active", ColumnType::Bool),
                ColumnSchema::new("all_teams", ColumnType::Bool),
            ],
        )
        .with_reference("organization", ORGANIZATIONS),
        TableSchema::new(
            DOCUMENTS,
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("updated_at", ColumnType::U64),
                ColumnSchema::new("visibility", ColumnType::String),
                ColumnSchema::new("published", ColumnType::Bool),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_reference("organization", ORGANIZATIONS)
        .with_reference("team", TEAMS)
        .with_indexed_columns([
            "organization",
            "team",
            "updated_at",
            "visibility",
            "published",
        ])
        .with_read_policy(Policy::shape(policy))
        .with_write_policy(Policy::public()),
        public_table(
            DOCUMENT_ACLS,
            [
                ColumnSchema::new("document", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("active", ColumnType::Bool),
            ],
        )
        .with_reference("document", DOCUMENTS),
    ])
}

fn public_table<const N: usize>(name: &str, columns: [ColumnSchema; N]) -> TableSchema {
    TableSchema::new(name, columns)
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())
}

fn alternative(query: Query) -> PolicyBranch {
    PolicyBranch::single_alternative_from_query(query)
}

fn build_fixture() -> Fixture {
    let organizations = [tagged_row(0x71, 1), tagged_row(0x71, 2)];
    let teams = [tagged_row(0x72, 1), tagged_row(0x72, 2)];
    let documents = (0..DOCUMENT_COUNT)
        .map(|index| Document {
            row: tagged_row(0x73, index as u64),
            organization: organizations[index % 2],
            team: teams[index % 2],
            updated_at: index as u64,
            // Public rows stay inside the member/admin scope so adding the
            // public branch changes policy complexity without changing those
            // identities' expected result page. Five of the ACL reader's
            // highest-ranked 100 rows deliberately overlap this branch.
            public: index % 20 == 6,
        })
        .collect::<Vec<_>>();
    let acl_rows = documents
        .iter()
        .rev()
        .take(PAGE_SIZE)
        .map(|document| document.row)
        .collect();
    Fixture {
        organizations,
        teams,
        documents,
        acl_rows,
    }
}

fn seed_fixture(db: &BenchDb, fixture: &Fixture) {
    for (index, organization) in fixture.organizations.iter().enumerate() {
        db.insert_with_id(
            ORGANIZATIONS,
            *organization,
            BTreeMap::from([(
                "name".to_owned(),
                Value::String(format!("Organization {index}")),
            )]),
        )
        .expect("seed organization");
    }
    for (index, team) in fixture.teams.iter().enumerate() {
        db.insert_with_id(
            TEAMS,
            *team,
            BTreeMap::from([
                (
                    "organization".to_owned(),
                    Value::Uuid(fixture.organizations[index].0),
                ),
                ("name".to_owned(), Value::String(format!("Team {index}"))),
            ]),
        )
        .expect("seed team");
    }
    db.insert_with_id(
        TEAM_MEMBERSHIPS,
        tagged_row(0x74, 1),
        membership_cells(fixture.teams[0], MEMBER),
    )
    .expect("seed team member");
    db.insert_with_id(
        ORGANIZATION_MEMBERSHIPS,
        tagged_row(0x75, 1),
        BTreeMap::from([
            (
                "organization".to_owned(),
                Value::Uuid(fixture.organizations[0].0),
            ),
            ("user".to_owned(), Value::Uuid(ORG_ADMIN.0)),
            ("role".to_owned(), Value::String("admin".to_owned())),
            ("active".to_owned(), Value::Bool(true)),
            ("all_teams".to_owned(), Value::Bool(true)),
        ]),
    )
    .expect("seed organization admin");
    for document in &fixture.documents {
        db.insert_with_id(DOCUMENTS, document.row, document_cells(*document))
            .expect("seed document");
    }
    for (index, document) in fixture.acl_rows.iter().enumerate() {
        db.insert_with_id(
            DOCUMENT_ACLS,
            tagged_row(0x76, index as u64),
            BTreeMap::from([
                ("document".to_owned(), Value::Uuid(document.0)),
                ("user".to_owned(), Value::Uuid(ACL_READER.0)),
                ("active".to_owned(), Value::Bool(true)),
            ]),
        )
        .expect("seed document ACL");
    }
}

fn membership_cells(team: RowUuid, user: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("team".to_owned(), Value::Uuid(team.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
        ("role".to_owned(), Value::String("viewer".to_owned())),
        ("active".to_owned(), Value::Bool(true)),
    ])
}

fn document_cells(document: Document) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "organization".to_owned(),
            Value::Uuid(document.organization.0),
        ),
        ("team".to_owned(), Value::Uuid(document.team.0)),
        ("updated_at".to_owned(), Value::U64(document.updated_at)),
        (
            "visibility".to_owned(),
            Value::String(if document.public { "public" } else { "private" }.to_owned()),
        ),
        ("published".to_owned(), Value::Bool(document.public)),
        (
            "title".to_owned(),
            Value::String(format!("Document {}", document.updated_at)),
        ),
    ])
}

fn expected_page(fixture: &Fixture, branches: usize, identity: AuthorId) -> Vec<RowUuid> {
    let visible = |document: &&Document| {
        (identity == MEMBER && document.team == fixture.teams[0])
            || (branches >= 2
                && identity == ORG_ADMIN
                && document.organization == fixture.organizations[0])
            || (branches >= 3 && identity == ACL_READER && fixture.acl_rows.contains(&document.row))
            || (branches >= 4 && document.public)
            || (branches >= 5 && identity == TRUSTED_ADMIN)
    };
    fixture
        .documents
        .iter()
        .rev()
        .filter(visible)
        .take(PAGE_SIZE)
        .map(|document| document.row)
        .collect()
}

fn expected_member_page(fixture: &Fixture, team: RowUuid) -> Vec<RowUuid> {
    expected_member_page_from(&fixture.documents, team)
}

fn expected_member_page_from(documents: &[Document], team: RowUuid) -> Vec<RowUuid> {
    documents
        .iter()
        .rev()
        .filter(|document| document.team == team || document.public)
        .take(PAGE_SIZE)
        .map(|document| document.row)
        .collect()
}

fn take_reset(stream: &mut SubscriptionStream) -> BTreeMap<RowUuid, u64> {
    match stream.try_next_event().expect("initial subscription reset") {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            ..
        } => added.into_iter().chain(updated).map(observed_row).collect(),
        other => panic!("expected initial reset, got {other:?}"),
    }
}

fn apply_events(stream: &mut SubscriptionStream, observed: &mut BTreeMap<RowUuid, u64>) {
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } => {
                if reset {
                    observed.clear();
                }
                for row in removed {
                    observed.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    let (row, updated_at) = observed_row(row);
                    observed.insert(row, updated_at);
                }
            }
            SubscriptionEvent::Rejected { reason } => panic!("subscription rejected: {reason:?}"),
            SubscriptionEvent::Closed => panic!("subscription closed"),
        }
    }
}

fn observed_row(row: CurrentRow) -> (RowUuid, u64) {
    let updated_at = match row.cell_at(2) {
        Some(Value::U64(updated_at)) => updated_at,
        other => panic!("expected projected updated_at, got {other:?}"),
    };
    (row.row_uuid(), updated_at)
}

fn observed_page(observed: &BTreeMap<RowUuid, u64>) -> Vec<RowUuid> {
    let mut rows = observed
        .iter()
        .map(|(row, updated_at)| (*row, *updated_at))
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(row, updated_at)| (std::cmp::Reverse(*updated_at), *row));
    rows.into_iter().map(|(row, _)| row).collect()
}

fn local_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn tagged_row(tag: u8, index: u64) -> RowUuid {
    let mut bytes = [tag; 16];
    bytes[8..].copy_from_slice(&index.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn digest(rows: &[RowUuid]) -> String {
    let mut hasher = blake3::Hasher::new();
    for row in rows {
        hasher.update(row.0.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}
