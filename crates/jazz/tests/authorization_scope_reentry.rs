use std::collections::{BTreeMap, BTreeSet};

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, LocalUpdates, MergeableTxOps, Propagation,
    ReadOpts, SeededRowIdSource, SubscriptionEvent, SubscriptionStream, UpdateOptions,
    UpsertOptions, WriteIdentity,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType as PublicColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const TEAMS: &str = "teams";
const MEMBERSHIPS: &str = "team_memberships";
const DOCUMENTS: &str = "documents";
const WRITER_UUID: uuid::Uuid = uuid::uuid!("81000000-0000-0000-0000-000000000001");
const READER_UUID: uuid::Uuid = uuid::uuid!("81000000-0000-0000-0000-000000000002");
const MAINTAINER_UUID: uuid::Uuid = uuid::uuid!("81000000-0000-0000-0000-000000000003");

fn writer() -> AuthorSubject {
    AuthorSubject::for_test_uuid(WRITER_UUID)
}

fn reader() -> AuthorSubject {
    AuthorSubject::for_test_uuid(READER_UUID)
}

fn maintainer() -> AuthorSubject {
    AuthorSubject::for_test_uuid(MAINTAINER_UUID)
}

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn schema() -> JazzSchema {
    let document_policy = exists(
        MEMBERSHIPS,
        vec![
            outer_eq("team", "team"),
            session_eq("user", &["claims", "sub"]),
        ],
    );
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(TEAMS)
                    .column("name", PublicColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(MEMBERSHIPS)
                    .fk_column("team", TEAMS)
                    .column("user", PublicColumnType::Uuid)
                    .policies(read_and_allow_all_writes(session_eq(
                        "user",
                        &["claims", "sub"],
                    ))),
            )
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .fk_column("team", TEAMS)
                    .column("rank", PublicColumnType::Timestamp)
                    .index_only(["team", "rank"])
                    .policies(read_and_allow_all_writes(document_policy)),
            )
            .build(),
    )
}

fn open_db() -> Db<TestStorage> {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x81; 16]),
                author: writer(),
            },
        )
        .with_id_source(SeededRowIdSource::new(0x8100)),
    ))
    .expect("open authorization scope re-entry db");
    for identity in [writer(), reader(), maintainer()] {
        db.set_identity_claims(
            identity,
            BTreeMap::from([("sub".to_owned(), Value::Uuid(identity.test_uuid()))]),
        );
    }
    db
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

fn insert_document(db: &Db<TestStorage>, id: RowUuid, team: RowUuid, rank: u64) {
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("rank".to_owned(), Value::U64(rank)),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert document");
}

fn insert_membership(db: &Db<TestStorage>, id: RowUuid, team: RowUuid, user: AuthorSubject) {
    block_on(db.insert(
        MEMBERSHIPS,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("user".to_owned(), Value::Uuid(user.test_uuid())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert membership");
}

/// An explicit-id upsert by Bob follows INSERT policy when the id is genuinely
/// absent, but the same call cannot merge a now-existing row hidden by read
/// policy. This distinguishes storage existence from policy-filtered absence
/// without exposing hidden cells.
///
/// bob ──upsert(absent)──► insert ✓ ──upsert(hidden existing)──► denied
#[test]
fn upsert_applies_insert_policy_only_to_a_genuinely_absent_target() {
    let db = open_db();
    let document = row(0xd0);
    let hidden_team = row(0xd1);
    let cells = BTreeMap::from([
        ("team".to_owned(), Value::Uuid(hidden_team.0)),
        ("rank".to_owned(), Value::U64(1)),
    ]);

    block_on(db.upsert(
        DOCUMENTS,
        document,
        cells.clone(),
        UpsertOptions {
            identity: WriteIdentity::Session(reader()),
            ..Default::default()
        },
    ))
    .expect("absent target follows public insert policy");
    let error = match block_on(db.upsert(
        DOCUMENTS,
        document,
        cells,
        UpsertOptions {
            identity: WriteIdentity::Session(reader()),
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("hidden existing target requires read permission"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(error.message.contains("read policy denied UPSERT"));
}

fn ordered_page(
    db: &Db<TestStorage>,
    identity: AuthorSubject,
    prepared: &jazz::db::PreparedQuery,
) -> Vec<RowUuid> {
    block_on(db.all_for_identity(prepared, opts(), identity))
        .expect("one-shot ordered page")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect()
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

fn exact_mixed_reentry_delta(
    stream: &mut SubscriptionStream,
) -> (
    BTreeSet<RowUuid>,
    BTreeSet<RowUuid>,
    BTreeMap<RowUuid, Value>,
) {
    let event = stream.try_next_event().expect("mixed subscription delta");
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
    assert!(stream.try_next_event().is_none());
    (
        added.into_iter().map(|row| row.row_uuid()).collect(),
        removed.into_iter().map(|row| row.row_uuid).collect(),
        updated
            .into_iter()
            .map(|row| {
                (
                    row.row_uuid(),
                    row.cell_at(1).expect("updated document rank payload"),
                )
            })
            .collect(),
    )
}

/// A write-only session cannot mutate a read-hidden row, even with a complete
/// replacement, through an ordinary facade or a mergeable transaction.
///
/// writer ──full/partial update or upsert──► hidden document ──► denied
#[test]
fn write_only_updates_and_upserts_are_denied_without_disclosing_the_target() {
    let db = open_db();
    let authorized_team = row(0x11);
    let winner = row(0x21);
    let second = row(0x22);
    let refill = row(0x23);

    block_on(db.insert(
        TEAMS,
        BTreeMap::from([("name".to_owned(), Value::String("authorized".to_owned()))]),
        InsertOptions {
            row_id: Some(authorized_team),
            ..Default::default()
        },
    ))
    .expect("insert team");
    insert_membership(&db, row(0x31), authorized_team, reader());
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

    let full_error = match block_on(db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(authorized_team.0)),
            ("rank".to_owned(), Value::U64(5)),
        ]),
        UpdateOptions {
            identity: WriteIdentity::Session(writer()),
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("write-only principal's full-row update must be denied"),
        Err(error) => error,
    };
    assert_eq!(full_error.code, ErrorCode::WriteRejected);
    assert!(
        full_error.message.contains("UPDATE")
            && full_error.message.contains("requires read permission"),
        "full-update denial must not distinguish the hidden target: {full_error:?}"
    );
    assert_eq!(ordered_page(&db, reader(), &prepared), vec![winner, second]);

    let partial_error = match block_on(db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([("rank".to_owned(), Value::U64(40))]),
        UpdateOptions {
            identity: WriteIdentity::Session(writer()),
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("write-only principal's partial update must be denied"),
        Err(error) => error,
    };
    assert_eq!(partial_error.code, ErrorCode::WriteRejected);
    assert!(
        partial_error.message.contains("UPDATE")
            && partial_error.message.contains("requires read permission"),
        "partial-update denial must explain its read authorization requirement: {partial_error:?}"
    );
    assert_eq!(ordered_page(&db, reader(), &prepared), vec![winner, second]);

    let transaction_error = match block_on(db.transaction_for_identity(writer(), async |tx| {
        tx.update(
            DOCUMENTS,
            winner,
            BTreeMap::from([("rank".to_owned(), Value::U64(5))]),
            Default::default(),
        )
        .await
    })) {
        Ok(_) => panic!("write-only transaction update must be denied"),
        Err(error) => error,
    };
    assert_eq!(transaction_error.code, ErrorCode::WriteRejected);
    assert!(transaction_error.message.contains("UPDATE"));

    let transaction_upsert_error =
        match block_on(db.transaction_for_identity(writer(), async |tx| {
            tx.upsert(
                DOCUMENTS,
                winner,
                BTreeMap::from([("rank".to_owned(), Value::U64(5))]),
                Default::default(),
            )
            .await
        })) {
            Ok(_) => panic!("write-only transaction upsert must be denied"),
            Err(error) => error,
        };
    assert_eq!(transaction_upsert_error.code, ErrorCode::WriteRejected);
    assert!(transaction_upsert_error.message.contains("UPSERT"));

    let upsert_error = match block_on(db.upsert(
        DOCUMENTS,
        winner,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(authorized_team.0)),
            ("rank".to_owned(), Value::U64(40)),
        ]),
        UpsertOptions {
            identity: WriteIdentity::Session(writer()),
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("write-only principal's upsert must be denied"),
        Err(error) => error,
    };
    assert_eq!(upsert_error.code, ErrorCode::WriteRejected);
    assert!(
        upsert_error.message.contains("UPSERT")
            && upsert_error.message.contains("requires read permission"),
        "upsert denial must explain its read authorization requirement: {upsert_error:?}"
    );

    // An ordinary client staging path is deliberately permission-elided: it
    // may carry an optimistic partial merge and let the serving host enforce
    // policy when the write arrives. The explicit identity APIs above remain
    // the trusted read-for-write boundary.
    block_on(db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([("rank".to_owned(), Value::U64(41))]),
        Default::default(),
    ))
    .expect("client-local partial update stages optimistically");
    block_on(db.upsert(
        DOCUMENTS,
        winner,
        BTreeMap::from([("rank".to_owned(), Value::U64(42))]),
        Default::default(),
    ))
    .expect("client-local upsert stages optimistically");
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
        block_on(db.insert(
            TEAMS,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            InsertOptions {
                row_id: Some(team),
                ..Default::default()
            },
        ))
        .expect("insert team");
    }
    insert_membership(&db, row(0x31), authorized_team, reader());
    insert_membership(&db, row(0x32), authorized_team, maintainer());
    insert_membership(&db, row(0x33), unauthorized_team, maintainer());
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
    let writer_page = || ordered_page(&db, writer(), &prepared);
    let reader_page = || ordered_page(&db, reader(), &prepared);
    let mut stream = block_on(db.subscribe_for_identity(&prepared, opts(), reader()))
        .expect("subscribe reader page");

    let mut maintained = initial_rows(&mut stream);
    assert_eq!(maintained, BTreeSet::from([winner, second]));
    assert_eq!(reader_page(), vec![winner, second]);

    // The whole scenario rests on WRITER being able to write these documents
    // without being able to read them. Assert that premise directly: without
    // this, a read policy that accidentally admitted WRITER would leave every
    // other assertion in this test passing while testing nothing.
    assert!(
        writer_page().is_empty(),
        "WRITER must not be able to read documents; the partial-update scenario \
         is only meaningful for a write-only principal"
    );

    let denied = match block_on(db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([("team".to_owned(), Value::Uuid(unauthorized_team.0))]),
        UpdateOptions {
            identity: WriteIdentity::Session(writer()),
            ..Default::default()
        },
    )) {
        Ok(_) => {
            panic!("write-only partial update must be denied before it can corrupt omitted cells")
        }
        Err(error) => error,
    };
    assert_eq!(denied.code, ErrorCode::WriteRejected);
    assert!(denied.message.contains("requires read permission"));
    assert_eq!(reader_page(), vec![winner, second]);
    assert!(stream.try_next_event().is_none());

    block_on(db.update(
        DOCUMENTS,
        winner,
        BTreeMap::from([("team".to_owned(), Value::Uuid(unauthorized_team.0))]),
        UpdateOptions {
            identity: WriteIdentity::Session(maintainer()),
            ..Default::default()
        },
    ))
    .expect("reader-authorized principal moves winning document out of scope");
    let move_out_delta = exact_delta(&mut stream);
    assert_eq!(
        move_out_delta,
        (BTreeSet::from([refill]), BTreeSet::from([winner]))
    );
    maintained.remove(&winner);
    maintained.insert(refill);
    assert_eq!(maintained, BTreeSet::from([second, refill]));
    assert_eq!(reader_page(), vec![second, refill]);
    assert!(
        writer_page().is_empty(),
        "WRITER must still not read documents after the move out of scope"
    );

    block_on(db.transaction_for_identity(maintainer(), async |tx| {
        tx.update(
            DOCUMENTS,
            winner,
            BTreeMap::from([("team".to_owned(), Value::Uuid(authorized_team.0))]),
            Default::default(),
        )
        .await?;
        // The authority re-entry and this ordinary content update share one
        // committed transition batch. Only `winner` owns replacement
        // provenance; `second` must remain an ordinary payload update.
        tx.update(
            DOCUMENTS,
            second,
            BTreeMap::from([("rank".to_owned(), Value::U64(21))]),
            Default::default(),
        )
        .await?;
        Ok(())
    }))
    .expect("commit mixed scope re-entry and retained-row update");
    assert_eq!(reader_page(), vec![winner, second]);
    let move_back_delta = exact_mixed_reentry_delta(&mut stream);
    assert_eq!(
        move_back_delta,
        (
            BTreeSet::from([winner]),
            BTreeSet::from([refill]),
            BTreeMap::from([(second, Value::U64(21))]),
        )
    );
    maintained.remove(&refill);
    maintained.insert(winner);
    assert_eq!(maintained, BTreeSet::from([winner, second]));
}

#[test]
fn client_subscription_skips_policy_only_compile_validation_but_identity_subscription_guards_it() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("profiles")
                    .column("name", PublicColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("items")
                    .column("name", PublicColumnType::Text)
                    .fk_column("profile", "profiles")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x82; 16]),
                author: writer(),
            },
        )
        .with_id_source(SeededRowIdSource::new(0x8200)),
    ))
    .expect("open client subscription validation fixture");
    db.mutate_current_compiled_schema_for_test(|compiled| {
        compiled
            .tables
            .iter_mut()
            .find(|table| table.name == "items")
            .expect("items table")
            .read_policy = Some(Query::from("items").include("profile"));
    });
    let prepared = db
        .prepare_query(&Query::from("items"))
        .expect("prepare items query");

    let client = block_on(db.subscribe(&prepared, opts()));
    if let Err(error) = client {
        panic!("client-local subscription must skip serving-only policy compilation: {error:?}");
    }
    let trusted = block_on(db.subscribe_for_identity(&prepared, opts(), writer()));
    assert!(
        trusted.is_err(),
        "trusted-serving subscription must continue to validate policy dependencies"
    );
}
