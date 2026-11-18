//! A room page whose messages nest their people: opening it, and a presence
//! stamp while it is open.
//!
//! The page is the room's newest 100 messages, each with its sender and its
//! system rows ("Ana added Ben"), and each system row with the three people
//! it names: forward relations under a reverse one, as an app's nested
//! includes compile. Every member writes `lastSeen` on their own profile
//! every few seconds; the page shows whole profiles, so a stamp by someone
//! on the page re-reads the page and its relations. Messages and system rows
//! are readable to room members only.
//!
//! The stamp's own commit is about 0.5% of the measured stamp; the rest is
//! re-reading the open page.

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, ReadOpts, SubscriptionEvent,
    SubscriptionStream, UpdateOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

const MEMBERS: usize = 8;
const ROOMS: usize = 4;
const MESSAGES_PER_ROOM: usize = 100;
const PAGE: usize = 100;
/// The member whose stamps are measured; they appear on the open page.
const STAMPING: usize = 1;

fn row(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn member(n: usize) -> AuthorSubject {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x55;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn profile(n: usize) -> Value {
    uuid(row(1, n % MEMBERS))
}

fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

fn writes_allowed(read: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(read)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn session(column: &str, path: &[&str]) -> PolicyExpr {
    PolicyExpr::eq_session(column, path.iter().map(|s| (*s).to_owned()).collect())
}

/// The row's room has a membership for the reader.
fn room_member() -> PolicyExpr {
    PolicyExpr::Exists {
        table: "presence_memberships".to_owned(),
        condition: Box::new(PolicyExpr::and(vec![
            session("room", &["__jazz_outer_row", "room"]),
            session("member", &["identity", "subject"]),
        ])),
    }
}

/// A seeded room whose page nests its people; nothing open yet.
pub struct NestedPageFixture {
    db: Db,
    page: PreparedQuery,
}

/// The page open and hydrated for member 0.
pub struct PresenceFixture {
    fixture: NestedPageFixture,
    page: SubscriptionStream,
    stamps: i64,
}

impl NestedPageFixture {
    pub fn new() -> Self {
        let schema = jazz::schema::JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("presence_profiles")
                        .column("name", ColumnType::Text)
                        .column("lastSeen", ColumnType::BigInt)
                        .policies(writes_allowed(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("presence_rooms")
                        .column("name", ColumnType::Text)
                        .policies(writes_allowed(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("presence_memberships")
                        .fk_column("room", "presence_rooms")
                        .column("member", ColumnType::Text)
                        .index_only(["member"])
                        .policies(writes_allowed(session("member", &["identity", "subject"]))),
                )
                .table(
                    TableSchemaBuilder::new("presence_messages")
                        .fk_column("room", "presence_rooms")
                        .fk_column("sender", "presence_profiles")
                        .column("body", ColumnType::Text)
                        .column("sentAt", ColumnType::BigInt)
                        .composite_index(["room", "sentAt"])
                        .policies(writes_allowed(room_member())),
                )
                .table(
                    TableSchemaBuilder::new("presence_system")
                        .fk_column("room", "presence_rooms")
                        .fk_column("message", "presence_messages")
                        .column("kind", ColumnType::Text)
                        .fk_column("actor", "presence_profiles")
                        .fk_column("target", "presence_profiles")
                        .fk_column("subject", "presence_profiles")
                        .policies(writes_allowed(room_member())),
                )
                .build(),
        )
        .expect("presence schema");
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x11; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open presence db");
        let insert = |table: &str, id: RowUuid, values: BTreeMap<String, Value>| {
            block_on(db.insert(
                table,
                values,
                InsertOptions {
                    row_id: Some(id),
                    ..Default::default()
                },
            ))
            .expect("seed presence fixture");
        };
        for m in 0..MEMBERS {
            insert(
                "presence_profiles",
                row(1, m),
                cells(vec![
                    ("name", Value::String(format!("member {m}"))),
                    ("lastSeen", Value::I64(0)),
                ]),
            );
        }
        for r in 0..ROOMS {
            insert(
                "presence_rooms",
                row(2, r),
                cells(vec![("name", Value::String(format!("room {r}")))]),
            );
            for m in 0..MEMBERS {
                insert(
                    "presence_memberships",
                    row(3, r * MEMBERS + m),
                    cells(vec![
                        ("room", uuid(row(2, r))),
                        ("member", Value::String(member(m).principal_parts().1)),
                    ]),
                );
            }
        }
        // Half the messages carry one or two system rows.
        let mut system = 0;
        for n in 0..ROOMS * MESSAGES_PER_ROOM {
            let (room, ordinal) = (n % ROOMS, n / ROOMS);
            insert(
                "presence_messages",
                row(4, n),
                cells(vec![
                    ("room", uuid(row(2, room))),
                    ("sender", profile(ordinal)),
                    ("body", Value::String(format!("message {n}"))),
                    ("sentAt", Value::I64(n as i64)),
                ]),
            );
            for k in 0..[0, 1, 0, 2][ordinal % 4] {
                insert(
                    "presence_system",
                    row(5, system),
                    cells(vec![
                        ("room", uuid(row(2, room))),
                        ("message", uuid(row(4, n))),
                        ("kind", Value::String("added".to_owned())),
                        ("actor", profile(ordinal + k)),
                        ("target", profile(ordinal + k + 1)),
                        ("subject", profile(ordinal + k + 2)),
                    ]),
                );
                system += 1;
            }
        }
        block_on(db.tick()).expect("settle presence fixture");

        let person = |name: &str| ArraySubquery::new(name, "presence_profiles", "id", name);
        let system = ArraySubquery::new("system", "presence_system", "message", "id")
            .nested(person("actor"))
            .nested(person("target"))
            .nested(person("subject"));
        let query = Query::from("presence_messages")
            .filter(eq(col("room"), lit(uuid(row(2, 0)))))
            .order_by("sentAt", OrderDirection::Desc)
            .limit(PAGE)
            .array_subquery(person("sender"))
            .array_subquery(system);
        let page = db.prepare_query(&query).expect("prepare room page");
        Self { db, page }
    }

    /// Member 0 opens the page: one full read of it and its nested
    /// relations, through the read policies, until the first result.
    pub fn open(&self) -> SubscriptionStream {
        let mut page = block_on(self.db.subscribe_for_identity(
            &self.page,
            ReadOpts::default(),
            member(0),
        ))
        .expect("open room page");
        block_on(self.db.tick()).expect("open room page");
        let mut shown = 0;
        while let Some(event) = page.try_next_event() {
            if let SubscriptionEvent::Delta { added, .. } = event {
                shown += added.len();
            }
        }
        assert_eq!(shown, PAGE, "the open page shows a full page");
        page
    }

    /// Keep the page open, for stamps.
    pub fn keep_open(self) -> PresenceFixture {
        let page = self.open();
        PresenceFixture {
            fixture: self,
            page,
            stamps: 0,
        }
    }
}

impl Default for NestedPageFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl PresenceFixture {
    pub fn new() -> Self {
        NestedPageFixture::new().keep_open()
    }

    /// One presence stamp by a member on the page, until the open page has
    /// re-read it. Returns the page rows the stamp updated.
    pub fn stamp(&mut self) -> usize {
        self.stamps += 1;
        let db = &self.fixture.db;
        block_on(db.update(
            "presence_profiles",
            row(1, STAMPING),
            cells(vec![("lastSeen", Value::I64(self.stamps))]),
            UpdateOptions::default(),
        ))
        .expect("presence stamp");
        block_on(db.tick()).expect("deliver presence stamp");
        let mut updated = 0;
        while let Some(event) = self.page.try_next_event() {
            if let SubscriptionEvent::Delta { updated: rows, .. } = event {
                updated += rows.len();
            }
        }
        assert!(updated > 0, "the stamp reached the open page");
        updated
    }
}

impl Default for PresenceFixture {
    fn default() -> Self {
        Self::new()
    }
}
