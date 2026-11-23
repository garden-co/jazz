//! The encrypted BandChat example orders its room list by each room's newest
//! message, read through plain columns only: "the messages of one room,
//! newest first, limit 1", once per listed room. This reads the example's
//! generated app file and checks that SQLite serves that query from the
//! `messages` index on (`roomId`, `$createdAt`) without sorting the room's
//! history. A change to the example's schema can fail this test: the file
//! it reads is `examples/band-chat/apps/react-hono-betterauth-e2ee/jazz.app.json`.

use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::deployment::artifact::load;

const APP_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/band-chat/apps/react-hono-betterauth-e2ee"
);

fn read(file: &str) -> Vec<u8> {
    let path = format!("{APP_DIR}/{file}");
    std::fs::read(&path).unwrap_or_else(|error| panic!("{path}: {error}"))
}

#[test]
fn the_newest_message_of_a_room_is_read_from_the_index_without_sorting() {
    let loaded = load(&read("jazz.app.json"), &read("jazz.permissions.json"))
        .unwrap_or_else(|error| panic!("{error}"));
    let schema = JazzSchema::new(loaded.current_schema()).unwrap();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let room = uuid::Uuid::from_bytes([7; 16]);
    // The room list's query: plain columns only, so it needs no key.
    let newest = db
        .prepare_query(
            &Query::from("messages")
                .filter(eq(col("roomId"), lit(Value::Uuid(room))))
                .select(["id", "senderId", "$createdAt"])
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(1),
        )
        .unwrap();
    let (_, plan) = jazz::db::block_on(db.explain_query_plan(&newest, None)).unwrap();
    assert!(plan.contains("messages__ix"), "{plan}");
    assert!(!plan.contains("TEMP B-TREE FOR ORDER BY"), "{plan}");
}
