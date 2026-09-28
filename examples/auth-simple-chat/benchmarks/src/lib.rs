//! Self-contained model of the auth chat examples' message room.
//!
//! `auth-simple-chat` and `auth-workos-chat` share this schema and these
//! claim-gated message policies; `auth-betterauth-chat` uses the same message
//! table with the role check removed from the general room. The benchmark
//! duplicates only that surface and does not import application runtime code.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::account_registry::AccountId;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, MergeableTxOps, PreparedQuery,
    Propagation, ReadOpts, SubscriptionEvent, SubscriptionStream, WriteIdentity, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::policy_claims::canonical_policy_binding_claims;
use jazz::tools::policy_expr::{SessionWhere, always, session_where};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

/// The examples' `CHAT_ID` and `ANNOUNCEMENTS_CHAT_ID` constants.
pub const CHAT_ID: &str = "chat-01";
pub const ANNOUNCEMENTS_CHAT_ID: &str = "announcements";
/// Signed-in authors who wrote the seeded history.
pub const AUTHORS: usize = 16;
/// Seeding transaction size; not part of any measured closure.
const SEED_BATCH: usize = 1_000;

type BenchDb = Db<MemoryStorage>;

/// A signed-in user whose token carries no role claim.
pub fn guest() -> AuthorSubject {
    user(1)
}

/// A signed-in member: the identity that `ChatPanel` reads and writes as.
pub fn member() -> AuthorSubject {
    user(0)
}

fn user(index: usize) -> AuthorSubject {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xa7;
    bytes[6] = 0x40;
    bytes[8] = 0x80;
    bytes[9..].copy_from_slice(&(index as u64).to_be_bytes()[1..]);
    let id = uuid::Uuid::from_bytes(bytes);
    AuthorSubject::for_test_uuid(id).with_account(AccountId(id))
}

fn role_claims(author: AuthorSubject, role: &str) -> BTreeMap<String, Value> {
    canonical_policy_binding_claims(
        &author,
        BTreeMap::from([("role".to_owned(), Value::String(role.to_owned()))]),
    )
}

/// Message SELECT/INSERT policies from `auth-simple-chat/permissions.ts`.
fn schema() -> JazzSchema {
    let room = |chat: &str| jazz::tools::policy_expr::eq("chat_id", chat);
    let is_admin = || session_where("claims.role", "admin");
    let is_member_or_admin =
        || session_where("claims.role", SessionWhere::in_list(["admin", "member"]));
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::or(vec![
            room(ANNOUNCEMENTS_CHAT_ID),
            PolicyExpr::and(vec![room(CHAT_ID), is_member_or_admin()]),
        ]))
        .with_insert(PolicyExpr::or(vec![
            PolicyExpr::and(vec![room(ANNOUNCEMENTS_CHAT_ID), is_admin()]),
            PolicyExpr::and(vec![room(CHAT_ID), is_member_or_admin()]),
        ]))
        .with_update(Some(always()), PolicyExpr::False)
        .with_delete(PolicyExpr::False);
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("messages")
                .column("author_name", ColumnType::Text)
                .column("chat_id", ColumnType::Text)
                .column("text", ColumnType::Text)
                .column("sent_at", ColumnType::Timestamp)
                .index_only(["chat_id", "sent_at"])
                .policies(policies),
        )
        .build();
    JazzSchema::new(&source).expect("auth chat benchmark schema compiles")
}

fn open_db() -> BenchDb {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa7; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open auth chat benchmark database");
    db.set_identity_claims(member(), role_claims(member(), "member"));
    db
}

fn message_row(index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xa8;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn message_cells(chat: &str, index: usize, sent_at: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "author_name".to_owned(),
            Value::String(format!("Author {:02}", index % AUTHORS)),
        ),
        ("chat_id".to_owned(), Value::String(chat.to_owned())),
        (
            "text".to_owned(),
            Value::String(format!("Message {index:06} in {chat}")),
        ),
        ("sent_at".to_owned(), Value::U64(sent_at)),
    ])
}

/// Seed `room_messages` messages into each of the two rooms. Writes run as the
/// database identity so seeding is not itself a policy benchmark.
fn seed(db: &BenchDb, room_messages: usize) {
    for (room_index, chat) in [CHAT_ID, ANNOUNCEMENTS_CHAT_ID].into_iter().enumerate() {
        for start in (0..room_messages).step_by(SEED_BATCH) {
            let end = (start + SEED_BATCH).min(room_messages);
            let ((), tx_id) = block_on(db.transaction(async |tx| {
                for message in start..end {
                    tx.insert(
                        "messages",
                        message_cells(chat, message, message as u64),
                        InsertOptions {
                            row_id: Some(message_row(room_index * room_messages + message)),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                Ok(())
            }))
            .unwrap_or_else(|error| panic!("seed {chat} messages {start}..{end}: {error}"));
            db.finalize_local_mergeable_commit_for_test(tx_id)
                .expect("settle seeded messages");
        }
    }
}

fn room_query(db: &BenchDb) -> PreparedQuery {
    // `ChatPanel`: `messages.where({ chat_id }).orderBy("sent_at", "asc")`.
    db.prepare_query(
        &Query::from("messages")
            .filter(eq(col("chat_id"), lit(Value::String(CHAT_ID.to_owned()))))
            .order_by("sent_at", OrderDirection::Asc),
    )
    .expect("prepare auth chat room query")
}

fn subscription_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

/// Drive the in-process runtime until the stream publishes. The native harness
/// owns the loop: there is no JS host or server ticking in the background.
fn next_event(db: &BenchDb, stream: &mut SubscriptionStream) -> SubscriptionEvent {
    let start = Instant::now();
    loop {
        if let Some(event) = stream.try_next_event() {
            return event;
        }
        block_on(db.tick()).expect("drive auth chat subscription");
        assert!(
            start.elapsed().as_secs() < 600,
            "auth chat subscription did not publish"
        );
    }
}

fn added_rows(event: &SubscriptionEvent) -> usize {
    match event {
        SubscriptionEvent::Delta { added, .. } => added.len(),
        other => panic!("auth chat subscription ended: {other:?}"),
    }
}

/// A seeded room whose member has not opened it yet.
pub struct OpenFixture {
    db: BenchDb,
    query: PreparedQuery,
}

impl OpenFixture {
    pub fn new(room_messages: usize) -> Self {
        let db = open_db();
        seed(&db, room_messages);
        let query = room_query(&db);
        Self { db, query }
    }

    /// A signed-in member opens the general room: claim-checked subscription
    /// through its first published result. The stream is returned so Divan
    /// excludes its teardown.
    pub fn open_room(&self) -> (SubscriptionStream, usize) {
        self.open_room_as(member())
    }

    /// Untimed oracle: the general room as `author`.
    pub fn open_room_as(&self, author: AuthorSubject) -> (SubscriptionStream, usize) {
        let mut stream = block_on(self.db.subscribe_for_identity(
            &self.query,
            subscription_opts(),
            author,
        ))
        .expect("member subscribes to the general room");
        let first = next_event(&self.db, &mut stream);
        let rows = added_rows(&first);
        (stream, rows)
    }
}

/// A seeded room that the member already has open.
pub struct SendFixture {
    db: BenchDb,
    stream: SubscriptionStream,
    next_message: usize,
    pub visible: usize,
}

impl SendFixture {
    pub fn new(room_messages: usize) -> Self {
        let fixture = OpenFixture::new(room_messages);
        let (stream, visible) = fixture.open_room();
        Self {
            db: fixture.db,
            stream,
            next_message: 2 * room_messages,
            visible,
        }
    }

    /// Send `count` messages as the member, one standalone write per message
    /// as `ChatPanel` does, each through its insert-policy check and until the
    /// open room subscription shows it.
    pub fn send_messages(&mut self, count: usize) -> usize {
        for _ in 0..count {
            let index = self.next_message;
            self.next_message += 1;
            let write = block_on(self.db.insert(
                "messages",
                message_cells(CHAT_ID, index, index as u64),
                InsertOptions {
                    row_id: Some(message_row(index)),
                    identity: WriteIdentity::Session(member()),
                    ..Default::default()
                },
            ))
            .expect("member sends a message");
            // The in-process authority plays the server's part: it runs the
            // claim-gated insert policy and accepts the message.
            self.db
                .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
                .expect("authority accepts the member's message");
            let mut shown = 0;
            while shown == 0 {
                shown = added_rows(&next_event(&self.db, &mut self.stream));
            }
            self.visible += shown;
        }
        count
    }

    /// Untimed oracle: a member may not post to announcements.
    pub fn member_announcement_is_denied(&self) -> bool {
        let write = block_on(self.db.insert(
            "messages",
            message_cells(ANNOUNCEMENTS_CHAT_ID, usize::MAX, 0),
            InsertOptions {
                identity: WriteIdentity::Session(member()),
                ..Default::default()
            },
        ))
        .expect("a denied write is still recorded locally");
        self.db
            .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .expect("authority decides the announcement");
        block_on(write.wait(DurabilityTier::Global)).is_err()
    }
}
