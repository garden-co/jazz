//! Self-contained model of the Chat example's (`chat-react`) message path.
//!
//! This duplicates only the schema and membership policies that opening a chat
//! and sending a message touch. It does not import application runtime code.

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
use jazz::tools::policy_expr::{all_of, any_of, exists, session, table};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

pub const USERS: usize = 32;
pub const CHATS: usize = 64;
/// Chat 0 is private; the benchmark's reader is one of its members.
pub const OPEN_CHAT: usize = 0;
/// `INITIAL_MESSAGES_TO_SHOW + 1` from `ChatView`.
pub const PAGE: usize = 21;
const MEMBERS_PER_CHAT: usize = 4;
const SEED_BATCH: usize = 1_000;

type BenchDb = Db<MemoryStorage>;

pub fn user(index: usize) -> AuthorSubject {
    let id = uuid::Uuid::from_bytes(tagged(0xc0, index, true));
    AuthorSubject::for_test_uuid(id).with_account(AccountId(id))
}

/// The member who opens the chat and sends messages.
pub fn reader() -> AuthorSubject {
    user(0)
}

fn tagged(tag: u8, index: usize, uuid_v4: bool) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[0] = tag;
    if uuid_v4 {
        bytes[6] = 0x40;
        bytes[8] = 0x80;
    }
    bytes[9..].copy_from_slice(&(index as u64).to_be_bytes()[1..]);
    bytes
}

fn row(tag: u8, index: usize) -> RowUuid {
    RowUuid::from_bytes(tagged(tag, index, false))
}

fn profile_row(user: usize) -> RowUuid {
    row(0xc1, user)
}

fn chat_row(chat: usize) -> RowUuid {
    row(0xc2, chat)
}

fn membership_row(index: usize) -> RowUuid {
    row(0xc3, index)
}

fn message_row(index: usize) -> RowUuid {
    row(0xc4, index)
}

fn account(index: usize) -> Value {
    Value::Uuid(user(index).test_uuid())
}

fn outer(column: &str) -> jazz::tools::policy_expr::PolicyValueInput {
    session(format!("__jazz_outer_row.{column}"))
}

/// Message and membership policies from `chat-react/permissions.ts`.
fn schema() -> JazzSchema {
    use jazz::tools::policy_expr::eq as is;
    let me = || session("user.account");
    let is_member_of_message_chat = || {
        exists(
            table("chatMembers")
                .where_(all_of([is("chatId", outer("chatId")), is("userId", me())])),
        )
    };
    let readable = || TablePolicies::new().with_select(PolicyExpr::True);
    let source =
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("profiles")
                    .column("userId", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .index_only(["userId"])
                    .policies(readable()),
            )
            .table(
                TableSchemaBuilder::new("chats")
                    .nullable_column("name", ColumnType::Text)
                    .column("isPublic", ColumnType::Boolean)
                    .policies(readable()),
            )
            .table(
                TableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", ColumnType::Uuid)
                    .index_only(["chatId", "userId"])
                    .policies(readable()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", ColumnType::Text)
                    .fk_column("senderId", "profiles")
                    .index_only(["chatId", "senderId"])
                    .policies(
                        TablePolicies::new()
                            .with_select(any_of([
                                exists(table("chats").where_(all_of([
                                    is("id", outer("chatId")),
                                    is("isPublic", true),
                                ]))),
                                is_member_of_message_chat(),
                            ]))
                            .with_insert(all_of([
                                is_member_of_message_chat(),
                                exists(table("profiles").where_(all_of([
                                    is("id", outer("senderId")),
                                    is("userId", me()),
                                ]))),
                            ])),
                    ),
            )
            .build();
    JazzSchema::new(&source).expect("Chat benchmark schema compiles")
}

fn open_db() -> BenchDb {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0xc0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open Chat benchmark database")
}

fn cells<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
    entries
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn with_id(row_id: RowUuid, created_at_ms: Option<u64>) -> InsertOptions {
    InsertOptions {
        row_id: Some(row_id),
        updated_at_ms: created_at_ms,
        ..Default::default()
    }
}

/// Chat that seeded message `index` belongs to: a quarter of all history is in
/// the opened chat, the rest round-robins over the other chats.
pub fn message_chat(index: usize) -> usize {
    if index.is_multiple_of(4) {
        OPEN_CHAT
    } else {
        1 + index % (CHATS - 1)
    }
}

fn message_cells(chat: usize, sender: usize, index: usize) -> BTreeMap<String, Value> {
    cells([
        ("chatId", Value::Uuid(chat_row(chat).0)),
        ("text", Value::String(format!("Message {index:06}"))),
        ("senderId", Value::Uuid(profile_row(sender).0)),
    ])
}

/// Seed profiles, chats (odd chats public), four members per chat and
/// `messages` chat messages. Writes run as the database identity.
fn seed(db: &BenchDb, messages: usize) {
    let ((), tx_id) = block_on(db.transaction(async |tx| {
        for user in 0..USERS {
            tx.insert(
                "profiles",
                cells([
                    ("userId", account(user)),
                    ("name", Value::String(format!("User {user:02}"))),
                ]),
                with_id(profile_row(user), None),
            )
            .await?;
        }
        for chat in 0..CHATS {
            tx.insert(
                "chats",
                cells([
                    (
                        "name",
                        Value::Nullable(Some(Box::new(Value::String(format!("Chat {chat:02}"))))),
                    ),
                    ("isPublic", Value::Bool(chat % 2 == 1)),
                ]),
                with_id(chat_row(chat), None),
            )
            .await?;
            for slot in 0..MEMBERS_PER_CHAT {
                tx.insert(
                    "chatMembers",
                    cells([
                        ("chatId", Value::Uuid(chat_row(chat).0)),
                        ("userId", account(chat_member(chat, slot))),
                    ]),
                    with_id(membership_row(chat * MEMBERS_PER_CHAT + slot), None),
                )
                .await?;
            }
        }
        Ok(())
    }))
    .expect("seed Chat profiles, chats and memberships");
    db.finalize_local_mergeable_commit_for_test(tx_id)
        .expect("settle Chat profiles, chats and memberships");

    for start in (0..messages).step_by(SEED_BATCH) {
        let end = (start + SEED_BATCH).min(messages);
        let ((), tx_id) = block_on(db.transaction(async |tx| {
            for index in start..end {
                let chat = message_chat(index);
                tx.insert(
                    "messages",
                    message_cells(chat, chat_member(chat, index % MEMBERS_PER_CHAT), index),
                    // Seeded history is older than anything sent during the run.
                    with_id(message_row(index), Some(1 + index as u64)),
                )
                .await?;
            }
            Ok(())
        }))
        .unwrap_or_else(|error| panic!("seed Chat messages {start}..{end}: {error}"));
        db.finalize_local_mergeable_commit_for_test(tx_id)
            .expect("settle seeded messages");
    }
}

/// A user who is not a member of the opened private chat.
pub fn outsider() -> AuthorSubject {
    user(outsider_index())
}

fn outsider_index() -> usize {
    (0..USERS)
        .find(|user| (0..MEMBERS_PER_CHAT).all(|slot| chat_member(OPEN_CHAT, slot) != *user))
        .expect("some user is not a member")
}

fn chat_member(chat: usize, slot: usize) -> usize {
    (chat + slot * 8) % USERS
}

fn open_chat_query(db: &BenchDb) -> PreparedQuery {
    // `ChatView`: messages.where({ chatId }).include({ sender: true })
    //   .orderBy("$createdAt", "desc").limit(INITIAL_MESSAGES_TO_SHOW + 1)
    db.prepare_query(
        &Query::from("messages")
            .filter(eq(col("chatId"), lit(Value::Uuid(chat_row(OPEN_CHAT).0))))
            .include("senderId")
            .order_by("$createdAt", OrderDirection::Desc)
            .limit(PAGE),
    )
    .expect("prepare Chat open-chat query")
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
        block_on(db.tick()).expect("drive Chat subscription");
        assert!(
            start.elapsed().as_secs() < 600,
            "Chat subscription did not publish"
        );
    }
}

fn added_rows(event: &SubscriptionEvent) -> usize {
    match event {
        SubscriptionEvent::Delta { added, .. } => added.len(),
        other => panic!("Chat subscription ended: {other:?}"),
    }
}

/// Seeded chats that the member has not opened yet.
pub struct OpenFixture {
    db: BenchDb,
    query: PreparedQuery,
}

impl OpenFixture {
    pub fn new(messages: usize) -> Self {
        let db = open_db();
        seed(&db, messages);
        let query = open_chat_query(&db);
        Self { db, query }
    }

    /// A member opens a private chat: the newest page with senders, through
    /// the membership read policy, until the first published result.
    pub fn open_chat(&self) -> (SubscriptionStream, usize) {
        self.open_chat_as(reader())
    }

    /// Untimed oracle: the same page as `author`.
    pub fn open_chat_as(&self, author: AuthorSubject) -> (SubscriptionStream, usize) {
        let mut stream = block_on(self.db.subscribe_for_identity(
            &self.query,
            subscription_opts(),
            author,
        ))
        .expect("member opens the chat");
        let rows = added_rows(&next_event(&self.db, &mut stream));
        (stream, rows)
    }
}

/// A chat the member already has open.
pub struct SendFixture {
    db: BenchDb,
    stream: SubscriptionStream,
    next_message: usize,
    pub shown: usize,
}

impl SendFixture {
    pub fn new(messages: usize) -> Self {
        let fixture = OpenFixture::new(messages);
        let (stream, shown) = fixture.open_chat();
        Self {
            db: fixture.db,
            stream,
            next_message: messages,
            shown,
        }
    }

    /// Send `count` messages, each a standalone write through the membership
    /// and own-profile insert policy, until the open chat shows it.
    pub fn send_messages(&mut self, count: usize) -> usize {
        for _ in 0..count {
            let index = self.next_message;
            self.next_message += 1;
            let write = block_on(self.db.insert(
                "messages",
                message_cells(OPEN_CHAT, 0, index),
                InsertOptions {
                    row_id: Some(message_row(index)),
                    identity: WriteIdentity::Session(reader()),
                    // Newer than all seeded history, so it tops the open page.
                    updated_at_ms: Some(1 + index as u64),
                    ..Default::default()
                },
            ))
            .expect("member sends a message");
            // The in-process authority plays the server's part: it runs the
            // membership and own-profile insert policy and accepts the message.
            self.db
                .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
                .expect("authority accepts the member's message");
            let mut added = 0;
            while added == 0 {
                added = added_rows(&next_event(&self.db, &mut self.stream));
            }
            self.shown += added;
        }
        count
    }

    /// Untimed oracle: a non-member cannot post into the private chat.
    pub fn non_member_send_is_denied(&self) -> bool {
        let outsider = outsider_index();
        let write = block_on(self.db.insert(
            "messages",
            message_cells(OPEN_CHAT, outsider, usize::MAX),
            InsertOptions {
                identity: WriteIdentity::Session(user(outsider)),
                ..Default::default()
            },
        ))
        .expect("a denied write is still recorded locally");
        self.db
            .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .expect("authority decides the outsider's message");
        block_on(write.wait(DurabilityTier::Global)).is_err()
    }
}
