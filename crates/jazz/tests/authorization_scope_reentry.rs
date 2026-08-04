use std::collections::{BTreeMap, BTreeSet};

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, SubscriptionStream,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, claim, col, eq};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

const TEAMS: &str = "teams";
const MEMBERSHIPS: &str = "team_memberships";
const DOCUMENTS: &str = "documents";
const WRITER: AuthorId = AuthorId(uuid::uuid!("81000000-0000-0000-0000-000000000001"));
const READER: AuthorId = AuthorId(uuid::uuid!("81000000-0000-0000-0000-000000000002"));

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn schema() -> JazzSchema {
    let document_policy = Query::from(DOCUMENTS).join_via_column(
        MEMBERSHIPS,
        "team",
        "team",
        [eq(col("user"), claim("sub"))],
    );
    JazzSchema::new([
        TableSchema::new(TEAMS, [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            MEMBERSHIPS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        )
        .with_reference("team", TEAMS)
        .with_read_policy(Policy::owner_only(MEMBERSHIPS, "user"))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            DOCUMENTS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("rank", ColumnType::U64),
            ],
        )
        .with_reference("team", TEAMS)
        .with_indexed_columns(["team", "rank"])
        .with_read_policy(Policy::shape(document_policy))
        .with_write_policy(Policy::public()),
    ])
}

fn open_db() -> Db<MemoryStorage> {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x81; 16]),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x8100)),
    ))
    .expect("open authorization scope re-entry db")
}

fn opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn insert_document(db: &Db<MemoryStorage>, id: RowUuid, team: RowUuid, rank: u64) {
    db.insert_with_id(
        DOCUMENTS,
        id,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("rank".to_owned(), Value::U64(rank)),
        ]),
    )
    .expect("insert document");
}

fn initial_rows(stream: &mut SubscriptionStream) -> BTreeSet<RowUuid> {
    match stream.try_next_event().expect("initial subscription reset") {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            added.into_iter().map(|row| row.row_uuid()).collect()
        }
        event => panic!("expected initial reset, got {event:?}"),
    }
}

fn exact_delta(stream: &mut SubscriptionStream) -> (BTreeSet<RowUuid>, BTreeSet<RowUuid>) {
    let event = stream.try_next_event().expect("incremental scope delta");
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("expected subscription delta, got {event:?}");
    };
    assert!(!reset);
    assert!(updated.is_empty());
    assert!(stream.try_next_event().is_none());
    (
        added.into_iter().map(|row| row.row_uuid()).collect(),
        removed.into_iter().map(|row| row.row_uuid).collect(),
    )
}

#[test]
fn maintained_authorization_restores_an_ordered_page_after_scope_reentry() {
    let db = open_db();
    let authorized_team = row(0x11);
    let unauthorized_team = row(0x12);
    let winner = row(0x21);
    let second = row(0x22);
    let refill = row(0x23);

    for (team, name) in [
        (authorized_team, "authorized"),
        (unauthorized_team, "unauthorized"),
    ] {
        db.insert_with_id(
            TEAMS,
            team,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .expect("insert team");
    }
    db.insert_with_id(
        MEMBERSHIPS,
        row(0x31),
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(authorized_team.0)),
            ("user".to_owned(), Value::Uuid(READER.0)),
        ]),
    )
    .expect("insert reader membership");
    insert_document(&db, winner, authorized_team, 30);
    insert_document(&db, second, authorized_team, 20);
    insert_document(&db, refill, authorized_team, 10);

    let prepared = db
        .prepare_query(
            &Query::from(DOCUMENTS)
                .order_by("rank", OrderDirection::Desc)
                .limit(2),
        )
        .expect("prepare exact ordered page");
    let one_shot = || {
        block_on(db.all_for_identity(&prepared, opts(), READER))
            .expect("one-shot reader page")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>()
    };
    let writer_page = || {
        block_on(db.all_for_identity(&prepared, opts(), WRITER))
            .expect("one-shot writer page")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>()
    };
    let mut stream = block_on(db.subscribe_for_identity(&prepared, opts(), READER))
        .expect("subscribe reader page");

    let mut maintained = initial_rows(&mut stream);
    assert_eq!(maintained, BTreeSet::from([winner, second]));
    assert_eq!(one_shot(), vec![winner, second]);

    // The whole scenario rests on WRITER being able to write these documents
    // without being able to read them. Assert that premise directly: without
    // this, a read policy that accidentally admitted WRITER would leave every
    // other assertion in this test passing while testing nothing.
    assert!(
        writer_page().is_empty(),
        "WRITER must not be able to read documents; the partial-update scenario \
         is only meaningful for a write-only principal"
    );

    db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([("team".to_owned(), Value::Uuid(unauthorized_team.0))]),
    )
    .expect("move winning document out of scope");
    let move_out_delta = exact_delta(&mut stream);
    assert_eq!(
        move_out_delta,
        (BTreeSet::from([refill]), BTreeSet::from([winner]))
    );
    maintained.remove(&winner);
    maintained.insert(refill);
    assert_eq!(maintained, BTreeSet::from([second, refill]));
    assert_eq!(one_shot(), vec![second, refill]);
    assert!(
        writer_page().is_empty(),
        "WRITER must still not read documents after the move out of scope"
    );

    db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([("team".to_owned(), Value::Uuid(authorized_team.0))]),
    )
    .expect("move winning document back into scope");
    assert_eq!(one_shot(), vec![winner, second]);
    let move_back_delta = exact_delta(&mut stream);
    assert_eq!(
        move_back_delta,
        (BTreeSet::from([winner]), BTreeSet::from([refill]))
    );
    maintained.remove(&refill);
    maintained.insert(winner);
    assert_eq!(maintained, BTreeSet::from([winner, second]));
}
