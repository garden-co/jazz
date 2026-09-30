//! BandChat's announcements room: session-claim-gated policies over a room
//! whose whole history stays live.
//!
//! Moved from the former auth chat example benchmark (`auth-simple-chat`,
//! whose `permissions.ts` the policies mirror). Two rooms share one messages
//! table: reading either needs a `member` or `admin` role claim, posting to
//! the general room needs the same, and posting to announcements needs
//! `admin`. The open announcements view is the room's full ascending history
//! with no LIMIT, so every post updates a view of the whole room (the shape of
//! #2086). The workload lives in the benchmark only; the BandChat app has no
//! claim-gated room.

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

pub const GENERAL_ROOM: &str = "general";
pub const ANNOUNCEMENTS_ROOM: &str = "announcements";
/// Authors who wrote the seeded history.
pub const AUTHORS: usize = 16;
/// Seeding transaction size; not part of any measured closure.
const SEED_BATCH: usize = 1_000;

/// The band's admin: role claim `admin`, the only identity that may post
/// announcements.
pub fn admin() -> AuthorSubject {
    user(0)
}

/// A band member: role claim `member`; reads both rooms, posts only to general.
pub fn member() -> AuthorSubject {
    user(1)
}

/// A signed-in user whose token carries no role claim.
pub fn guest() -> AuthorSubject {
    user(2)
}

fn user(index: usize) -> AuthorSubject {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xa9;
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

fn schema() -> JazzSchema {
    let room = |name: &str| jazz::tools::policy_expr::eq("chat_id", name);
    let is_admin = || session_where("claims.role", "admin");
    let is_member_or_admin =
        || session_where("claims.role", SessionWhere::in_list(["admin", "member"]));
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::or(vec![
            PolicyExpr::and(vec![room(ANNOUNCEMENTS_ROOM), is_member_or_admin()]),
            PolicyExpr::and(vec![room(GENERAL_ROOM), is_member_or_admin()]),
        ]))
        .with_insert(PolicyExpr::or(vec![
            PolicyExpr::and(vec![room(ANNOUNCEMENTS_ROOM), is_admin()]),
            PolicyExpr::and(vec![room(GENERAL_ROOM), is_member_or_admin()]),
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
    JazzSchema::new(&source).expect("announcements benchmark schema compiles")
}

fn open_db() -> Db {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open announcements benchmark database");
    db.set_identity_claims(admin(), role_claims(admin(), "admin"));
    db.set_identity_claims(member(), role_claims(member(), "member"));
    db
}

fn message_row(index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xaa;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn message_cells(room: &str, index: usize, sent_at: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "author_name".to_owned(),
            Value::String(format!("Author {:02}", index % AUTHORS)),
        ),
        ("chat_id".to_owned(), Value::String(room.to_owned())),
        (
            "text".to_owned(),
            Value::String(format!("Message {index:06} in {room}")),
        ),
        ("sent_at".to_owned(), Value::U64(sent_at)),
    ])
}

/// Seed `room_messages` messages into each room. Writes run as the database
/// identity, so seeding is not itself a policy benchmark.
fn seed(db: &Db, room_messages: usize) {
    for (room_index, room) in [ANNOUNCEMENTS_ROOM, GENERAL_ROOM].into_iter().enumerate() {
        for start in (0..room_messages).step_by(SEED_BATCH) {
            let end = (start + SEED_BATCH).min(room_messages);
            let ((), tx_id) = block_on(db.transaction(async |tx| {
                for message in start..end {
                    tx.insert(
                        "messages",
                        message_cells(room, message, message as u64),
                        InsertOptions {
                            row_id: Some(message_row(room_index * room_messages + message)),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                Ok(())
            }))
            .unwrap_or_else(|error| panic!("seed {room} messages {start}..{end}: {error}"));
            db.finalize_local_mergeable_commit_for_test(tx_id)
                .expect("settle seeded messages");
        }
    }
}

fn room_query(db: &Db, room: &str) -> PreparedQuery {
    // The whole room in send order: `messages.where({ chat_id }).orderBy("sent_at", "asc")`.
    db.prepare_query(
        &Query::from("messages")
            .filter(eq(col("chat_id"), lit(Value::String(room.to_owned()))))
            .order_by("sent_at", OrderDirection::Asc),
    )
    .expect("prepare room query")
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
fn next_event(db: &Db, stream: &mut SubscriptionStream) -> SubscriptionEvent {
    let start = Instant::now();
    loop {
        if let Some(event) = stream.try_next_event() {
            return event;
        }
        block_on(db.tick()).expect("drive announcements subscription");
        assert!(
            start.elapsed().as_secs() < 600,
            "announcements subscription did not publish"
        );
    }
}

fn added_rows(event: &SubscriptionEvent) -> usize {
    match event {
        SubscriptionEvent::Delta { added, .. } => added.len(),
        other => panic!("announcements subscription ended: {other:?}"),
    }
}

/// Both rooms seeded, with the admin's announcements view open and hydrated.
pub struct AnnouncementsFixture {
    db: Db,
    query: PreparedQuery,
    stream: SubscriptionStream,
    next_message: usize,
    /// Messages the admin's open announcements view has shown.
    pub visible: usize,
}

impl AnnouncementsFixture {
    pub fn new(room_messages: usize) -> Self {
        let db = open_db();
        seed(&db, room_messages);
        let query = room_query(&db, ANNOUNCEMENTS_ROOM);
        let (stream, visible) = open_room_as(&db, &query, admin());
        Self {
            db,
            query,
            stream,
            next_message: 2 * room_messages,
            visible,
        }
    }

    /// The admin posts `count` announcements, one standalone write each: every
    /// post passes the admin-only insert policy, and the admin's open view of
    /// the whole room shows it before the next.
    pub fn post_announcements(&mut self, count: usize) -> usize {
        for _ in 0..count {
            let index = self.next_message;
            self.next_message += 1;
            let write = block_on(self.db.insert(
                "messages",
                message_cells(ANNOUNCEMENTS_ROOM, index, index as u64),
                InsertOptions {
                    row_id: Some(message_row(index)),
                    identity: WriteIdentity::Session(admin()),
                    ..Default::default()
                },
            ))
            .expect("admin posts an announcement");
            // The in-process authority plays the server's part: it runs the
            // claim-gated insert policy and accepts the post.
            self.db
                .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
                .expect("authority accepts the admin's announcement");
            let mut shown = 0;
            while shown == 0 {
                shown = added_rows(&next_event(&self.db, &mut self.stream));
            }
            self.visible += shown;
        }
        count
    }

    /// Untimed oracle: the announcements `author` can read.
    pub fn announcements_as(&self, author: AuthorSubject) -> usize {
        open_room_as(&self.db, &self.query, author).1
    }

    /// Untimed oracle: a member's announcement is recorded locally but the
    /// authority's insert policy rejects it.
    pub fn member_announcement_is_denied(&self) -> bool {
        let write = block_on(self.db.insert(
            "messages",
            message_cells(ANNOUNCEMENTS_ROOM, usize::MAX, 0),
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

fn open_room_as(
    db: &Db,
    query: &PreparedQuery,
    author: AuthorSubject,
) -> (SubscriptionStream, usize) {
    let mut stream = block_on(db.subscribe_for_identity(query, subscription_opts(), author))
        .expect("subscribe to the announcements room");
    let first = next_event(db, &mut stream);
    let rows = added_rows(&first);
    (stream, rows)
}
