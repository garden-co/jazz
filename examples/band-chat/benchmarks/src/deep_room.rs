//! BandChat after years of use: one room with a long history, a member who is
//! in a hundred rooms, unread state derived from each room's newest message,
//! and read receipts with read dates.
//!
//! The schema and policies mirror `apps/nextjs-betterauth/schema.ts` and
//! `permissions.ts` with the read-state model those files describe:
//!
//! - A room is unread when its newest message is newer than the reader's
//!   marker and was not sent by the reader. The room list reads each room's
//!   newest message (`messagesViaRoom`, newest first, limit 1); no member has
//!   to update the room when they post.
//! - The unread count is a capped read of the messages after the marker.
//! - Markers are readable by the room's members (check marks), and every
//!   marker move appends a `readProgress` row (read dates for "Read by").
//!
//! Only the tables these workloads touch are duplicated, with their
//! membership policies; no application runtime code is imported. Canvases,
//! strokes, join requests and attachments are left out.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::account_registry::AccountId;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, MergeableTxOps, PreparedQuery,
    Propagation, ReadOpts, SubscriptionEvent, SubscriptionStream, UpdateOptions, WriteIdentity,
    block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    ArraySubquery, ArraySubqueryRequirement, OrderDirection, Query, col, contains, eq, gt, gte,
    lit, lt, lte, ne,
};
use jazz::schema::JazzSchema;
use jazz::tools::policy_expr::{
    all_of, allowed_to_read, allowed_to_read_referencing, any_of, exists, session, table,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

/// Profiles in the band. The reader is user 0.
pub const USERS: usize = 200;
/// The deep room is room 0; its members are users 0..DEEP_MEMBERS.
pub const DEEP_ROOM: usize = 0;
pub const DEEP_MEMBERS: usize = 50;
/// Members of each busy room, the reader included.
const BUSY_MEMBERS: usize = 20;
/// Rooms the reader belongs to, when the fixture has that many.
pub const INBOX_ROOMS: usize = 100;
/// Messages shown when a room opens and per page when scrolling back.
pub const PAGE: usize = 50;
/// Messages loaded on each side of a jump target.
pub const JUMP_HALF: usize = 25;
/// Search results shown, newest first.
pub const SEARCH_LIMIT: usize = 50;
/// The room list stops counting at this many (`RoomNav`'s `UNREAD_CAP`).
pub const UNREAD_CAP: usize = 100;
/// Messages from others after the reader's marker in the deep room.
pub const DEEP_UNREAD: usize = 5;
/// One in this many deep-room messages contains the search word.
pub const SEARCH_EVERY: usize = 2_000;
pub const SEARCH_WORD: &str = "cadenza";
/// One in this many messages in the deep and busy rooms has reactions.
const REACTED_EVERY: usize = 10;
/// A member's marker moves (and is journaled) about every this many messages.
const JOURNAL_STRIDE: usize = 500;
/// Seeded history spans this many milliseconds, so every room's newest
/// message is recent and live sends are newer than all of it.
const BASE_MS: u64 = 1_700_000_000_000;
const SPAN_MS: u64 = 1_000_000_000;
const SEED_BATCH: usize = 2_000;

/// How much history the band has.
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    /// Messages in the deep room. A multiple of `DEEP_MEMBERS`.
    pub deep: usize,
    /// Rooms `1..=busy_rooms`, each holding `busy` messages.
    pub busy_rooms: usize,
    pub busy: usize,
    /// All rooms; the ones after the busy rooms hold 10–200 messages each.
    pub rooms: usize,
}

impl Shape {
    /// The walltime shape: one deep room, ten busy rooms of 10,000 messages
    /// and 989 small rooms.
    pub const fn band(deep: usize) -> Self {
        Self {
            deep,
            busy_rooms: 10,
            busy: 10_000,
            rooms: 1_000,
        }
    }

    /// A miniature with the same structure, for tests.
    pub const fn miniature(deep: usize) -> Self {
        Self {
            deep,
            busy_rooms: 2,
            busy: 300,
            rooms: 40,
        }
    }

    pub fn messages_in(&self, room: usize) -> usize {
        if room == DEEP_ROOM {
            self.deep
        } else if room <= self.busy_rooms {
            self.busy
        } else {
            10 + (room * 37) % 191
        }
    }

    pub fn total_messages(&self) -> usize {
        (0..self.rooms).map(|room| self.messages_in(room)).sum()
    }

    /// Rooms the reader is a member of: the deep room, every busy room and
    /// every 11th small room, up to `INBOX_ROOMS`.
    pub fn reader_rooms(&self) -> Vec<usize> {
        let first_small = self.busy_rooms + 1;
        (0..self.rooms)
            .filter(|&room| room < first_small || (room - first_small).is_multiple_of(11))
            .take(INBOX_ROOMS)
            .collect()
    }

    /// User indexes of a room's members, without duplicates. The reader is
    /// listed first when they are a member.
    pub fn members(&self, room: usize) -> Vec<usize> {
        let reader_in = self.reader_rooms().contains(&room);
        let count = if room == DEEP_ROOM {
            DEEP_MEMBERS
        } else if room <= self.busy_rooms {
            BUSY_MEMBERS
        } else {
            2 + room % 7
        };
        let mut members = Vec::with_capacity(count);
        if reader_in {
            members.push(0);
        }
        if room == DEEP_ROOM {
            members.extend(1..DEEP_MEMBERS);
            return members;
        }
        let mut step = 0;
        while members.len() < count {
            let user = 1 + (room * 13 + step * 17) % (USERS - 1);
            if !members.contains(&user) {
                members.push(user);
            }
            step += 1;
        }
        members
    }

    /// Send time of message `k` in `room`. Every room's history spans the
    /// same period, so the newest message of every room is recent.
    pub fn sent_at(&self, room: usize, k: usize) -> u64 {
        let n = self.messages_in(room) as u64;
        BASE_MS + (k as u64 * SPAN_MS) / n + (room as u64 % 7)
    }

    /// Member slot that sent message `k`. The deep room's last `DEEP_UNREAD`
    /// messages come from other members.
    fn sender_slot(&self, room: usize, k: usize, members: usize) -> usize {
        let slot = k % members;
        if room == DEEP_ROOM && k + DEEP_UNREAD >= self.deep && slot == 0 {
            1
        } else {
            slot
        }
    }

    /// The message index each member has read up to in `room`.
    ///
    /// In the deep room the reader is `DEEP_UNREAD` behind and member `s` is
    /// `2s` behind, so the further down the member list, the older their
    /// marker. In every third other room the reader has two unread messages.
    pub fn read_up_to(&self, room: usize, slot: usize) -> usize {
        let newest = self.messages_in(room) - 1;
        if room == DEEP_ROOM {
            if slot == 0 {
                newest - DEEP_UNREAD
            } else {
                newest.saturating_sub(2 * slot)
            }
        } else if slot == 0 && room.is_multiple_of(3) {
            newest.saturating_sub(2)
        } else {
            newest
        }
    }

    /// Journaled marker positions for a member: every `JOURNAL_STRIDE`
    /// messages, staggered by slot, then the current marker. Only rooms with
    /// a long history are journaled; small rooms have only markers.
    fn journal(&self, room: usize, slot: usize) -> Vec<usize> {
        if room > self.busy_rooms {
            return Vec::new();
        }
        let up_to = self.read_up_to(room, slot);
        let mut entries = (slot * 10..up_to)
            .step_by(JOURNAL_STRIDE)
            .collect::<Vec<_>>();
        entries.push(up_to);
        entries
    }
}

pub fn user(index: usize) -> AuthorSubject {
    let id = uuid::Uuid::from_bytes(tagged(0xd0, index as u64, true));
    AuthorSubject::for_test_uuid(id).with_account(AccountId(id))
}

/// The member who is in a hundred rooms and reads the deep room.
pub fn reader() -> AuthorSubject {
    user(0)
}

/// A user who is in none of the reader's rooms' member lists for the deep room.
pub fn outsider() -> AuthorSubject {
    user(USERS - 1)
}

fn tagged(tag: u8, index: u64, uuid_v4: bool) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[0] = tag;
    if uuid_v4 {
        bytes[6] = 0x40;
        bytes[8] = 0x80;
    }
    bytes[9..].copy_from_slice(&index.to_be_bytes()[1..]);
    bytes
}

fn row(tag: u8, index: u64) -> RowUuid {
    RowUuid::from_bytes(tagged(tag, index, false))
}

pub fn profile_row(user: usize) -> RowUuid {
    row(0xd1, user as u64)
}

pub fn room_row(room: usize) -> RowUuid {
    row(0xd2, room as u64)
}

fn member_row(room: usize, slot: usize) -> RowUuid {
    row(0xd3, (room as u64) << 8 | slot as u64)
}

pub fn message_row(room: usize, k: usize) -> RowUuid {
    row(0xd4, (room as u64) << 32 | k as u64)
}

fn reaction_row(room: usize, k: usize, j: usize) -> RowUuid {
    row(0xd5, (room as u64) << 40 | (k as u64) << 4 | j as u64)
}

fn marker_row(room: usize, slot: usize) -> RowUuid {
    row(0xd6, (room as u64) << 8 | slot as u64)
}

fn progress_row(room: usize, slot: usize, j: usize) -> RowUuid {
    row(0xd7, (room as u64) << 40 | (slot as u64) << 24 | j as u64)
}

fn account(user: usize) -> Value {
    Value::Uuid(self::user(user).test_uuid())
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn some(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

fn outer(column: &str) -> jazz::tools::policy_expr::PolicyValueInput {
    session(format!("__jazz_outer_row.{column}"))
}

/// `permissions.ts`, for the tables these workloads touch.
fn schema() -> JazzSchema {
    use jazz::tools::policy_expr::eq as is;
    let me = || session("user.account");
    // isMemberOf(row.roomId)
    let is_member_of = |room_column: &str| {
        exists(table("roomMembers").where_(all_of([
            is("roomId", outer(room_column)),
            is("memberAuthor", me()),
        ])))
    };
    let owns_profile = |profile_column: &str| {
        exists(table("profiles").where_(all_of([
            is("id", outer(profile_column)),
            is("author", me()),
        ])))
    };
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("profiles")
                .column("author", ColumnType::Uuid)
                .column("displayName", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        // Owner, anyone who can read a message they sent, and
                        // co-members through a membership naming them.
                        .with_select(any_of([
                            is("author", me()),
                            allowed_to_read_referencing("messages", "senderId"),
                            allowed_to_read_referencing("roomMembers", "memberProfileId"),
                        ]))
                        .with_insert(is("author", me())),
                ),
        )
        .table(
            // No `lastActivityAt`: nobody bumps the room when they post, and
            // only the creator may update it.
            TableSchemaBuilder::new("rooms")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(is_member_of("id"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchemaBuilder::new("roomMembers")
                .fk_column("roomId", "rooms")
                .column("memberAuthor", ColumnType::Uuid)
                .nullable_fk_column("memberProfileId", "profiles")
                .policies(TablePolicies::new().with_select(allowed_to_read("roomId"))),
        )
        .table(
            // Readable by the room's members: check marks.
            TableSchemaBuilder::new("readMarkers")
                .fk_column("roomId", "rooms")
                .column("reader", ColumnType::Uuid)
                .column("lastReadAt", ColumnType::Timestamp)
                .policies(
                    TablePolicies::new()
                        .with_select(is_member_of("roomId"))
                        .with_insert(all_of([is("reader", me()), is_member_of("roomId")]))
                        .with_update(
                            Some(is("reader", me())),
                            all_of([is("reader", me()), is_member_of("roomId")]),
                        ),
                ),
        )
        .table(
            // Append-only journal of marker moves: read dates.
            TableSchemaBuilder::new("readProgress")
                .fk_column("roomId", "rooms")
                .fk_column("memberId", "roomMembers")
                .column("reader", ColumnType::Uuid)
                .column("upToAt", ColumnType::Timestamp)
                .policies(
                    TablePolicies::new()
                        .with_select(is_member_of("roomId"))
                        .with_insert(all_of([
                            is("reader", me()),
                            is_member_of("roomId"),
                            exists(table("roomMembers").where_(all_of([
                                is("id", outer("memberId")),
                                is("memberAuthor", me()),
                            ]))),
                        ])),
                ),
        )
        .table(
            TableSchemaBuilder::new("messages")
                .fk_column("roomId", "rooms")
                .fk_column("senderId", "profiles")
                .column("text", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(is_member_of("roomId"))
                        .with_insert(all_of([is_member_of("roomId"), owns_profile("senderId")])),
                ),
        )
        .table(
            TableSchemaBuilder::new("reactions")
                .fk_column("roomId", "rooms")
                .fk_column("messageId", "messages")
                .column("author", ColumnType::Uuid)
                .column("emoji", ColumnType::Text)
                .policies(TablePolicies::new().with_select(allowed_to_read("messageId"))),
        )
        .build();
    JazzSchema::new(&source).expect("BandChat deep-room schema compiles")
}

fn open_db() -> Db {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0xd0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open BandChat deep-room database")
}

fn cells<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
    entries
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn with_id(row_id: RowUuid, at_ms: Option<u64>) -> InsertOptions {
    InsertOptions {
        row_id: Some(row_id),
        updated_at_ms: at_ms,
        ..Default::default()
    }
}

const WORDS: [&str; 8] = [
    "soundcheck",
    "setlist",
    "tempo",
    "bridge",
    "chorus",
    "tuning",
    "rehearsal",
    "encore",
];

fn message_text(room: usize, k: usize) -> String {
    let word = if room == DEEP_ROOM && k % SEARCH_EVERY == SEARCH_EVERY / 2 {
        SEARCH_WORD
    } else {
        WORDS[(k * 7 + room) % WORDS.len()]
    };
    format!("Message {k:07} about the {word} in room {room:04}")
}

/// Rows written in one seeding transaction.
enum Seed {
    Message {
        room: usize,
        k: usize,
        sender: usize,
    },
    Reaction {
        room: usize,
        k: usize,
        j: usize,
        author: usize,
    },
}

fn commit_batch(db: &Db, batch: &[Seed], shape: &Shape) {
    let ((), tx_id) = block_on(db.transaction(async |tx| {
        for seed in batch {
            match *seed {
                Seed::Message { room, k, sender } => {
                    tx.insert(
                        "messages",
                        cells([
                            ("roomId", uuid(room_row(room))),
                            ("senderId", uuid(profile_row(sender))),
                            ("text", Value::String(message_text(room, k))),
                        ]),
                        with_id(message_row(room, k), Some(shape.sent_at(room, k))),
                    )
                    .await?;
                }
                Seed::Reaction { room, k, j, author } => {
                    tx.insert(
                        "reactions",
                        cells([
                            ("roomId", uuid(room_row(room))),
                            ("messageId", uuid(message_row(room, k))),
                            ("author", account(author)),
                            ("emoji", Value::String(["👍", "🔥", "🎸"][j].to_owned())),
                        ]),
                        with_id(
                            reaction_row(room, k, j),
                            Some(shape.sent_at(room, k) + 1 + j as u64),
                        ),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }))
    .expect("seed BandChat history");
    db.finalize_local_mergeable_commit_for_test(tx_id)
        .expect("settle seeded history");
}

/// Seed profiles, rooms, memberships, history, reactions, markers and the
/// read journal. Writes run as the database identity and are settled.
fn seed(db: &Db, shape: &Shape) {
    let ((), tx_id) = block_on(db.transaction(async |tx| {
        for user in 0..USERS {
            tx.insert(
                "profiles",
                cells([
                    ("author", account(user)),
                    ("displayName", Value::String(format!("Player {user:03}"))),
                ]),
                with_id(profile_row(user), Some(BASE_MS - 1)),
            )
            .await?;
        }
        for room in 0..shape.rooms {
            tx.insert(
                "rooms",
                cells([("name", Value::String(format!("Room {room:04}")))]),
                with_id(room_row(room), Some(BASE_MS - 1)),
            )
            .await?;
            for (slot, &member) in shape.members(room).iter().enumerate() {
                tx.insert(
                    "roomMembers",
                    cells([
                        ("roomId", uuid(room_row(room))),
                        ("memberAuthor", account(member)),
                        ("memberProfileId", some(uuid(profile_row(member)))),
                    ]),
                    with_id(member_row(room, slot), Some(BASE_MS - 1)),
                )
                .await?;
            }
        }
        Ok(())
    }))
    .expect("seed BandChat profiles, rooms and memberships");
    db.finalize_local_mergeable_commit_for_test(tx_id)
        .expect("settle profiles, rooms and memberships");

    let mut batch = Vec::with_capacity(SEED_BATCH);
    for room in 0..shape.rooms {
        let members = shape.members(room);
        let reacted = room <= shape.busy_rooms;
        for k in 0..shape.messages_in(room) {
            let sender = members[shape.sender_slot(room, k, members.len())];
            batch.push(Seed::Message { room, k, sender });
            if reacted && k.is_multiple_of(REACTED_EVERY) {
                for j in 0..=(k / REACTED_EVERY) % 3 {
                    let author = members[(k + j + 1) % members.len()];
                    batch.push(Seed::Reaction { room, k, j, author });
                }
            }
            if batch.len() >= SEED_BATCH {
                commit_batch(db, &batch, shape);
                batch.clear();
            }
        }
    }
    if !batch.is_empty() {
        commit_batch(db, &batch, shape);
    }

    let ((), tx_id) = block_on(db.transaction(async |tx| {
        for room in 0..shape.rooms {
            for (slot, &member) in shape.members(room).iter().enumerate() {
                let up_to = shape.read_up_to(room, slot);
                tx.insert(
                    "readMarkers",
                    cells([
                        ("roomId", uuid(room_row(room))),
                        ("reader", account(member)),
                        ("lastReadAt", Value::U64(shape.sent_at(room, up_to))),
                    ]),
                    with_id(
                        marker_row(room, slot),
                        Some(shape.sent_at(room, up_to) + 10),
                    ),
                )
                .await?;
                for (j, k) in shape.journal(room, slot).into_iter().enumerate() {
                    tx.insert(
                        "readProgress",
                        cells([
                            ("roomId", uuid(room_row(room))),
                            ("memberId", uuid(member_row(room, slot))),
                            ("reader", account(member)),
                            ("upToAt", Value::U64(shape.sent_at(room, k))),
                        ]),
                        // Read a little after the message arrived.
                        with_id(
                            progress_row(room, slot, j),
                            Some(shape.sent_at(room, k) + 10),
                        ),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }))
    .expect("seed BandChat markers and read journal");
    db.finalize_local_mergeable_commit_for_test(tx_id)
        .expect("settle markers and read journal");
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

/// Drive the in-process runtime until the stream publishes.
fn next_event(db: &Db, stream: &mut SubscriptionStream) -> SubscriptionEvent {
    let start = Instant::now();
    loop {
        if let Some(event) = stream.try_next_event() {
            return event;
        }
        block_on(db.tick()).expect("drive BandChat subscription");
        assert!(
            start.elapsed().as_secs() < 600,
            "BandChat subscription did not publish"
        );
    }
}

/// Rows a delta adds, and rows it changes: updated rows plus structural
/// edits inside rows already shown (an included array gaining a child).
fn changed_rows(event: &SubscriptionEvent) -> (usize, usize) {
    match event {
        SubscriptionEvent::Delta {
            added,
            updated,
            terminal_operations,
            ..
        } => (added.len(), updated.len() + terminal_operations.len()),
        other => panic!("BandChat subscription ended: {other:?}"),
    }
}

/// The shown page of a room: the message, its sender and its reactions.
fn with_sender_and_reactions(query: Query) -> Query {
    query.include("senderId").array_subquery(ArraySubquery::new(
        "reactionsViaMessage",
        "reactions",
        "messageId",
        "id",
    ))
}

fn in_room(room: usize) -> Query {
    Query::from("messages").filter(eq(col("roomId"), lit(uuid(room_row(room)))))
}

/// A seeded band with no views open.
pub struct DeepRoom {
    pub shape: Shape,
    db: Db,
    newest_page: PreparedQuery,
    older_page: PreparedQuery,
    jump_older: PreparedQuery,
    jump_newer: PreparedQuery,
    search: PreparedQuery,
    unread: PreparedQuery,
    inbox: PreparedQuery,
    receipts: PreparedQuery,
    read_by: PreparedQuery,
    next_send: usize,
}

impl DeepRoom {
    pub fn seeded(shape: Shape) -> Self {
        assert!(shape.deep.is_multiple_of(DEEP_MEMBERS) && shape.deep >= 4 * SEARCH_EVERY);
        let db = open_db();
        seed(&db, &shape);
        let prepare = |query: Query| db.prepare_query(&query).expect("prepare BandChat query");
        let deep = shape.deep;

        // `RoomView`: the newest page, each message with its sender and its
        // reactions.
        let newest_page = prepare(with_sender_and_reactions(
            in_room(DEEP_ROOM)
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(PAGE),
        ));
        // Scrolling back: the page before a cursor halfway down the history.
        let older_page = prepare(with_sender_and_reactions(
            in_room(DEEP_ROOM)
                .filter(lt(
                    col("$createdAt"),
                    lit(Value::U64(shape.scroll_cursor())),
                ))
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(PAGE),
        ));
        // Jumping to a reply or a search hit a quarter of the way in: the
        // target and what is around it, as two bounded reads.
        let anchor = shape.sent_at(DEEP_ROOM, shape.jump_target());
        let jump_older = prepare(with_sender_and_reactions(
            in_room(DEEP_ROOM)
                .filter(lte(col("$createdAt"), lit(Value::U64(anchor))))
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(JUMP_HALF),
        ));
        let jump_newer = prepare(with_sender_and_reactions(
            in_room(DEEP_ROOM)
                .filter(gt(col("$createdAt"), lit(Value::U64(anchor))))
                .order_by("$createdAt", OrderDirection::Asc)
                .limit(JUMP_HALF),
        ));
        // Search within the room, newest hits first.
        let search = prepare(
            in_room(DEEP_ROOM)
                .filter(contains(
                    col("text"),
                    lit(Value::String(SEARCH_WORD.to_owned())),
                ))
                .include("senderId")
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(SEARCH_LIMIT),
        );
        // `RoomNav`'s count: messages from others after the reader's
        // marker, capped.
        let marker = shape.sent_at(DEEP_ROOM, shape.read_up_to(DEEP_ROOM, 0));
        let unread = prepare(
            in_room(DEEP_ROOM)
                .filter(ne(col("senderId"), lit(uuid(profile_row(0)))))
                .filter(gt(col("$createdAt"), lit(Value::U64(marker))))
                .select(["roomId"])
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(UNREAD_CAP),
        );
        // The room list: every room the reader can see, its newest message
        // and the reader's own marker.
        let inbox = prepare(
            Query::from("rooms")
                .array_subquery(
                    ArraySubquery::new("messagesViaRoom", "messages", "roomId", "id")
                        .order_by("$createdAt", OrderDirection::Desc)
                        .limit(1),
                )
                .array_subquery(
                    ArraySubquery::new("readMarkersViaRoom", "readMarkers", "roomId", "id")
                        .filter(eq(col("reader"), lit(account(0)))),
                ),
        );
        // Check marks: every member's marker in the open room.
        let receipts = prepare(
            Query::from("readMarkers").filter(eq(col("roomId"), lit(uuid(room_row(DEEP_ROOM))))),
        );
        // "Read by": members whose journal reaches the message, with the
        // first time it did.
        let read_by_at = shape.sent_at(DEEP_ROOM, shape.read_by_target());
        let read_by = prepare(
            Query::from("roomMembers")
                .filter(eq(col("roomId"), lit(uuid(room_row(DEEP_ROOM)))))
                .include("memberProfileId")
                .array_subquery(
                    ArraySubquery::new("progressViaMember", "readProgress", "memberId", "id")
                        .filter(gte(col("upToAt"), lit(Value::U64(read_by_at))))
                        .order_by("upToAt", OrderDirection::Asc)
                        .limit(1)
                        .requirement(ArraySubqueryRequirement::AtLeastOne),
                ),
        );
        Self {
            shape,
            db,
            newest_page,
            older_page,
            jump_older,
            jump_newer,
            search,
            unread,
            inbox,
            receipts,
            read_by,
            next_send: deep,
        }
    }

    fn subscribe(
        &self,
        query: &PreparedQuery,
        author: AuthorSubject,
    ) -> (SubscriptionStream, usize) {
        let mut stream = block_on(self.db.subscribe_for_identity(query, local_opts(), author))
            .expect("open BandChat view");
        let (added, _) = changed_rows(&next_event(&self.db, &mut stream));
        (stream, added)
    }

    fn read_once(&self, query: &PreparedQuery, author: AuthorSubject) -> usize {
        block_on(self.db.all_for_identity(query, local_opts(), author))
            .expect("BandChat one-shot read")
            .len()
    }

    /// A member opens the deep room: the newest page with senders and
    /// reactions, through the membership policy, until it is published.
    pub fn open_newest_page(&self) -> (SubscriptionStream, usize) {
        self.subscribe(&self.newest_page, reader())
    }

    /// Untimed oracle: the same page as `author`.
    pub fn open_newest_page_as(&self, author: AuthorSubject) -> usize {
        self.subscribe(&self.newest_page, author).1
    }

    /// The page before a cursor halfway down the deep room.
    pub fn open_older_page(&self) -> (SubscriptionStream, usize) {
        self.subscribe(&self.older_page, reader())
    }

    /// The target message and its neighbours, as two windows.
    pub fn jump_to_message(&self) -> [(SubscriptionStream, usize); 2] {
        [
            self.subscribe(&self.jump_older, reader()),
            self.subscribe(&self.jump_newer, reader()),
        ]
    }

    /// One-shot search of the deep room.
    pub fn search_room(&self) -> usize {
        self.read_once(&self.search, reader())
    }

    /// The reader's capped unread count for the deep room.
    pub fn open_unread_count(&self) -> (SubscriptionStream, usize) {
        self.subscribe(&self.unread, reader())
    }

    /// The reader's room list.
    pub fn open_inbox(&self) -> (SubscriptionStream, usize) {
        self.subscribe(&self.inbox, reader())
    }

    /// One-shot "Read by" sheet for a message near the end of the deep room.
    pub fn read_by_sheet(&self) -> usize {
        self.read_once(&self.read_by, reader())
    }

    /// Runs the engine's pending work, such as finalizing dropped
    /// subscriptions, so a memory receipt sees what a closed view leaves.
    pub fn settle(&self) {
        block_on(self.db.tick()).expect("settle BandChat");
    }

    /// Untimed oracle: the room list's rows as the reader sees them, with
    /// the number of newest messages and markers each room carries.
    pub fn inbox_rows(&self) -> Vec<(usize, usize)> {
        block_on(
            self.db
                .all_for_identity(&self.inbox, local_opts(), reader()),
        )
        .expect("read the room list")
        .iter()
        .map(|room| {
            let (descriptor, raw) = room.encoded_record();
            let record = descriptor.bind(raw);
            let len = |field: &str| match record.get(field) {
                Ok(Value::Array(rows)) => rows.len(),
                other => panic!("expected {field} array, got {other:?}"),
            };
            (len("messagesViaRoom"), len("readMarkersViaRoom"))
        })
        .collect()
    }

    /// A member of the deep room sends a message as themselves; the
    /// in-process authority checks the insert policy and accepts it.
    fn member_sends(&mut self, room: usize, member: usize) -> RowUuid {
        let k = self.next_send;
        self.next_send += 1;
        let id = message_row(room, k);
        let write = block_on(self.db.insert(
            "messages",
            cells([
                ("roomId", uuid(room_row(room))),
                ("senderId", uuid(profile_row(member))),
                ("text", Value::String(message_text(room, k))),
            ]),
            InsertOptions {
                row_id: Some(id),
                identity: WriteIdentity::Session(user(member)),
                // Newer than all seeded history.
                updated_at_ms: Some(BASE_MS + SPAN_MS + k as u64),
                ..Default::default()
            },
        ))
        .expect("member sends a message");
        self.db
            .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .expect("authority accepts the member's message");
        id
    }

    /// A member moves their marker to the newest message: the marker update
    /// and its journal row commit as one transaction.
    fn member_reads_to(&self, slot: usize, member: usize, at: u64, journal_entry: usize) {
        let ((), tx_id) = block_on(self.db.transaction_for_identity(user(member), async |tx| {
            tx.update(
                "readMarkers",
                marker_row(DEEP_ROOM, slot),
                cells([("lastReadAt", Value::U64(at))]),
                UpdateOptions {
                    updated_at_ms: Some(at + 10),
                    ..Default::default()
                },
            )
            .await?;
            tx.insert(
                "readProgress",
                cells([
                    ("roomId", uuid(room_row(DEEP_ROOM))),
                    ("memberId", uuid(member_row(DEEP_ROOM, slot))),
                    ("reader", account(member)),
                    ("upToAt", Value::U64(at)),
                ]),
                with_id(
                    progress_row(DEEP_ROOM, slot, 1 << 16 | journal_entry),
                    Some(at + 10),
                ),
            )
            .await?;
            Ok(())
        }))
        .expect("member moves their marker");
        self.db
            .finalize_local_mergeable_commit_for_test(tx_id)
            .expect("authority accepts the marker move");
    }
}

impl Shape {
    /// Scrolling back reads the page before the middle of the deep room.
    pub fn scroll_cursor(&self) -> u64 {
        self.sent_at(DEEP_ROOM, self.deep / 2)
    }

    pub fn jump_target(&self) -> usize {
        self.deep / 4
    }

    /// "Read by" is opened on the 40th newest message.
    pub fn read_by_target(&self) -> usize {
        self.deep - 40
    }

    /// Members whose marker reaches the "Read by" target.
    pub fn expected_read_by(&self) -> usize {
        (0..DEEP_MEMBERS)
            .filter(|&slot| self.read_up_to(DEEP_ROOM, slot) >= self.read_by_target())
            .count()
    }

    pub fn expected_search_hits(&self) -> usize {
        (0..self.deep)
            .filter(|k| k % SEARCH_EVERY == SEARCH_EVERY / 2)
            .count()
            .min(SEARCH_LIMIT)
    }
}

/// The deep room open on the reader's screen.
pub struct LiveWindow {
    room: DeepRoom,
    stream: SubscriptionStream,
    pub shown: usize,
}

impl LiveWindow {
    pub fn new(room: DeepRoom) -> Self {
        let (stream, shown) = room.open_newest_page();
        Self {
            room,
            stream,
            shown,
        }
    }

    /// Another member sends `count` messages; each is accepted and reaches
    /// the reader's open page before the next.
    pub fn member_sends(&mut self, count: usize) -> usize {
        for _ in 0..count {
            self.room.member_sends(DEEP_ROOM, 1);
            let mut added = 0;
            while added == 0 {
                added = changed_rows(&next_event(&self.room.db, &mut self.stream)).0;
            }
            self.shown += added;
        }
        count
    }
}

/// The reader's room list, open.
pub struct LiveInbox {
    room: DeepRoom,
    stream: SubscriptionStream,
    pub rooms: usize,
    pub updates: usize,
}

impl LiveInbox {
    pub fn new(room: DeepRoom) -> Self {
        let (stream, rooms) = room.open_inbox();
        Self {
            room,
            stream,
            rooms,
            updates: 0,
        }
    }

    /// A message lands in one of the reader's busy rooms; the room list
    /// shows it as that room's newest message.
    pub fn message_lands(&mut self) -> usize {
        let room = 1;
        let sender = self.room.shape.members(room)[1];
        self.room.member_sends(room, sender);
        let mut updated = 0;
        while updated == 0 {
            let (added, changed) = changed_rows(&next_event(&self.room.db, &mut self.stream));
            updated = added + changed;
        }
        self.updates += updated;
        updated
    }
}

/// The reader's unread count for the deep room, live.
pub struct LiveUnreadCount {
    room: DeepRoom,
    stream: SubscriptionStream,
    pub count: usize,
}

impl LiveUnreadCount {
    pub fn new(room: DeepRoom) -> Self {
        let (stream, count) = room.open_unread_count();
        Self {
            room,
            stream,
            count,
        }
    }

    /// Another member sends a message; the count goes up.
    pub fn message_arrives(&mut self) -> usize {
        self.room.member_sends(DEEP_ROOM, 2);
        let mut added = 0;
        while added == 0 {
            added = changed_rows(&next_event(&self.room.db, &mut self.stream)).0;
        }
        self.count += added;
        self.count
    }
}

/// Every member of the deep room has it open, with everyone's markers.
pub struct ReceiptsFanout {
    room: DeepRoom,
    streams: Vec<SubscriptionStream>,
    moves: usize,
    pub markers_seen: usize,
}

impl ReceiptsFanout {
    pub fn new(room: DeepRoom, open_by: usize) -> Self {
        let members = room.shape.members(DEEP_ROOM);
        let mut markers_seen = 0;
        let streams = members[..open_by]
            .iter()
            .map(|&member| {
                let (stream, markers) = room.subscribe(&room.receipts, user(member));
                markers_seen = markers;
                stream
            })
            .collect();
        Self {
            room,
            streams,
            moves: 0,
            markers_seen,
        }
    }

    /// One member reads to the newest message; every open room view shows
    /// the moved marker.
    pub fn member_reads(&mut self) -> usize {
        self.moves += 1;
        let slot = 1 + self.moves % (DEEP_MEMBERS - 1);
        let member = self.room.shape.members(DEEP_ROOM)[slot];
        let at = BASE_MS + SPAN_MS + (self.room.next_send + self.moves) as u64;
        self.room.member_reads_to(slot, member, at, self.moves);
        let start = Instant::now();
        let mut pending = (0..self.streams.len()).collect::<Vec<_>>();
        while !pending.is_empty() {
            block_on(self.room.db.tick()).expect("drive receipts");
            pending.retain(|&view| match self.streams[view].try_next_event() {
                Some(event) => {
                    let (added, updated) = changed_rows(&event);
                    added + updated == 0
                }
                None => true,
            });
            assert!(start.elapsed().as_secs() < 600, "receipts did not publish");
        }
        self.streams.len()
    }
}
