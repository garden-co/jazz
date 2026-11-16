//! Fan-out with a hundred members online: one write while every member keeps
//! a live chat list, or a room, open. Shaped like an adopter's chat app,
//! load-tested with 100 users.
//!
//! ```text
//! chats ◄── chat_members(chatId, userId, isBanned, deletedAtMs, pinnedAtMs) ──► users
//!   ▲                                                                            ▲
//!   ├── calls(chatId, kind, ended)                      unique_names(userId) ────┤
//!   ├── message_read_marks(chatId, userId, lastReadAtMs) ────────────────────────┤
//!   ├── chat_agents(chatId, name)                                                │
//!   ├── messages(chatId, senderId, body, sentAtMs, replyToId ──► messages) ──────┤
//!   └── drafts(chatId, userId, body, updatedAtMs) ───────────────────────────────┘
//! ```
//!
//! "Me" is the users row named by the `userId` claim. Chats, calls, read
//! marks, agents and messages are readable to the chat's members; a
//! membership row when it is mine or its chat is one of mine; a user when
//! they are me or discoverable; a unique name when its user is (`INHERITS`);
//! a draft by its writer only. The membership check is written one of two
//! ways ([`Access`]): as the plain `chat_members.exists.where({ chatId,
//! userId: session })` or as a relation-style `exists(...)` that joins
//! `users`. The two grant the same rows.
//!
//! 100 members and 100 chats; each member is in five chats, which overlap as
//! group chats do (five members each). Every membership has its read mark,
//! every chat an agent and a call (every other one ended) and 30 messages,
//! every third a reply to the one before it.
//!
//! Each case's live results are open and hydrated before timing, and every
//! write is dispatched (`tick`) and every open result drained inside it.

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, PreparedQuery, ReadOpts,
    SubscriptionEvent, SubscriptionStream, UpdateOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, is_null, lit, ne};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinCondition, RelationJoinKind,
    RelationPredicate, RelationValueRef,
};
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

/// Members online in every case, each with one live result open.
pub const MEMBERS: usize = 100;
/// Chats each member is in: the length of their chat list.
pub const CHATS_PER_MEMBER: usize = 5;
const MESSAGES_PER_CHAT: usize = 30;
/// Messages an open room shows, newest first.
pub const ROOM_PAGE: usize = 25;
/// Draft writes before a message: one per keystroke of a short message.
pub const KEYSTROKES: usize = 13;

const USER: u8 = 1;
const UNIQUE_NAME: u8 = 2;
const CHAT: u8 = 3;
const MEMBER: u8 = 4;
const CALL: u8 = 5;
const MARK: u8 = 6;
const AGENT: u8 = 7;
const MESSAGE: u8 = 8;
const DRAFT: u8 = 9;

const CLAIM: [&str; 2] = ["claims", "userId"];

/// How the read policies check that the reader is a member of the row's
/// chat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// `chat_members.exists.where({ chatId: row.chatId, userId: session })`:
    /// a legacy `EXISTS` with a session pin, which fan-out watches by
    /// evidence (#5341).
    Exists,
    /// The same check as a relation-style `exists(...)`: the chat's
    /// memberships joined to their `users` row, which must be me. Fan-out
    /// watches the tables it reads whole.
    Relation,
}

fn id(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn member(n: usize) -> AuthorSubject {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x57;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn text(s: impl Into<String>) -> Value {
    Value::String(s.into())
}

fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

/// The chat of member `u`'s `k`-th membership: windows of five chats that
/// overlap, so each chat has five members.
fn chat_of(u: usize, k: usize, members: usize) -> usize {
    (u + k * 7) % members
}

fn membership(u: usize, k: usize) -> usize {
    u * CHATS_PER_MEMBER + k
}

fn session(column: &str, path: &[&str]) -> PolicyExpr {
    PolicyExpr::eq_session(column, path.iter().map(|s| (*s).to_owned()).collect())
}

fn read(select: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(select)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

/// The row's `column` names a chat I am a member of.
fn member_of(access: Access, column: &str) -> PolicyExpr {
    match access {
        Access::Exists => PolicyExpr::Exists {
            table: "chat_members".to_owned(),
            condition: Box::new(PolicyExpr::and(vec![
                session("chatId", &["__jazz_outer_row", column]),
                session("userId", &CLAIM),
            ])),
        },
        Access::Relation => {
            // As the TS DSL compiles `policy.exists(policy.chat_members
            // .where({ chatId: row.chatId }).join(policy.users, ...)
            // .where({ "users.id": session.claims.userId }))`.
            let scoped = |scope: &str, column: &str| RelationColumnRef {
                scope: Some(scope.to_owned()),
                column: column.to_owned(),
            };
            let cmp = |left, right| RelationPredicate::Cmp {
                left,
                op: RelationCmpOp::Eq,
                right,
            };
            PolicyExpr::ExistsRel {
                rel: RelationExpr::Filter {
                    input: Box::new(RelationExpr::Join {
                        left: Box::new(RelationExpr::TableScan {
                            table: "chat_members".into(),
                            alias: None,
                        }),
                        right: Box::new(RelationExpr::TableScan {
                            table: "users".into(),
                            alias: Some("__join_0".to_owned()),
                        }),
                        on: vec![RelationJoinCondition {
                            left: scoped("chat_members", "userId"),
                            right: scoped("__join_0", "id"),
                        }],
                        join_kind: RelationJoinKind::Inner,
                    }),
                    predicate: RelationPredicate::And(vec![
                        cmp(
                            scoped("chat_members", "chatId"),
                            RelationValueRef::OuterColumn(RelationColumnRef::unscoped(column)),
                        ),
                        cmp(
                            scoped("__join_0", "id"),
                            RelationValueRef::SessionRef(
                                CLAIM.iter().map(|s| (*s).to_owned()).collect(),
                            ),
                        ),
                    ]),
                },
            }
        }
    }
}

fn schema(access: Access) -> JazzSchema {
    let member_of = |column: &str| member_of(access, column);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .column("discoverable", ColumnType::Boolean)
                    .policies(read(PolicyExpr::or(vec![
                        session("id", &CLAIM),
                        PolicyExpr::eq_literal("discoverable", jazz::tools::Value::Boolean(true)),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("unique_names")
                    .fk_column("userId", "users")
                    .column("name", ColumnType::Text)
                    .policies(read(PolicyExpr::inherits(Operation::Select, "userId"))),
            )
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(read(member_of("id"))),
            )
            .table(
                TableSchemaBuilder::new("chat_members")
                    .fk_column("chatId", "chats")
                    .fk_column("userId", "users")
                    .column("isBanned", ColumnType::Boolean)
                    .nullable_column("deletedAtMs", ColumnType::BigInt)
                    .nullable_column("pinnedAtMs", ColumnType::BigInt)
                    .index_only(["userId"])
                    .index_only(["chatId"])
                    .policies(read(PolicyExpr::or(vec![
                        session("userId", &CLAIM),
                        member_of("chatId"),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("calls")
                    .fk_column("chatId", "chats")
                    .column("kind", ColumnType::Text)
                    .column("ended", ColumnType::Boolean)
                    .policies(read(member_of("chatId"))),
            )
            .table(
                TableSchemaBuilder::new("message_read_marks")
                    .fk_column("chatId", "chats")
                    .fk_column("userId", "users")
                    .column("lastReadAtMs", ColumnType::BigInt)
                    .policies(read(member_of("chatId"))),
            )
            .table(
                TableSchemaBuilder::new("chat_agents")
                    .fk_column("chatId", "chats")
                    .column("name", ColumnType::Text)
                    .policies(read(member_of("chatId"))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .fk_column("senderId", "users")
                    .column("body", ColumnType::Text)
                    .column("sentAtMs", ColumnType::BigInt)
                    .nullable_fk_column("replyToId", "messages")
                    .composite_index(["chatId", "sentAtMs"])
                    .policies(read(member_of("chatId"))),
            )
            .table(
                TableSchemaBuilder::new("drafts")
                    .fk_column("chatId", "chats")
                    .fk_column("userId", "users")
                    .column("body", ColumnType::Text)
                    .column("updatedAtMs", ColumnType::BigInt)
                    .composite_index(["chatId", "userId"])
                    .policies(read(session("userId", &CLAIM))),
            )
            .build(),
    )
    .expect("chat fan-out schema compiles")
}

/// Member `u`'s chat list, as the adopter's app reads it:
///
/// ```text
/// chat_members.where({ userId: me, isBanned: false, deletedAtMs: null })
///   .orderBy(pinnedAtMs desc, $createdAt desc)
///   .include({ chat: { chat_membersViaChat: { user: { unique_namesViaUser } },
///                      callsViaChat: calls.where(group, not ended).limit(1),
///                      message_read_marksViaChat:
///                        marks.where(userId != me).orderBy(lastReadAtMs desc).limit(1),
///                      chat_agentsViaChat } })
/// ```
fn chat_list(u: usize) -> Query {
    let me = uuid(id(USER, u));
    Query::from("chat_members")
        .filter(eq(col("userId"), lit(me.clone())))
        .filter(eq(col("isBanned"), lit(Value::Bool(false))))
        .filter(is_null(col("deletedAtMs")))
        .order_by("pinnedAtMs", OrderDirection::Desc)
        .order_by("$createdAt", OrderDirection::Desc)
        .array_subquery(
            ArraySubquery::new("chat", "chats", "id", "chatId")
                .nested(
                    ArraySubquery::new("chat_membersViaChat", "chat_members", "chatId", "id")
                        .nested(ArraySubquery::new("user", "users", "id", "userId").nested(
                            ArraySubquery::new(
                                "unique_namesViaUser",
                                "unique_names",
                                "userId",
                                "id",
                            ),
                        )),
                )
                .nested(
                    ArraySubquery::new("callsViaChat", "calls", "chatId", "id")
                        .filter(eq(col("kind"), lit(text("group"))))
                        .filter(eq(col("ended"), lit(Value::Bool(false))))
                        .limit(1),
                )
                .nested(
                    ArraySubquery::new(
                        "message_read_marksViaChat",
                        "message_read_marks",
                        "chatId",
                        "id",
                    )
                    .filter(ne(col("userId"), lit(me)))
                    .order_by("lastReadAtMs", OrderDirection::Desc)
                    .limit(1),
                )
                .nested(ArraySubquery::new(
                    "chat_agentsViaChat",
                    "chat_agents",
                    "chatId",
                    "id",
                )),
        )
}

/// An open room: chat `c`'s newest messages, each with the message it
/// replies to (a self-relation).
fn room(c: usize) -> Query {
    Query::from("messages")
        .filter(eq(col("chatId"), lit(uuid(id(CHAT, c)))))
        .order_by("sentAtMs", OrderDirection::Desc)
        .limit(ROOM_PAGE)
        .array_subquery(ArraySubquery::new("replyTo", "messages", "id", "replyToId"))
}

/// Member `u`'s composer in chat `c`: their own draft there.
fn composer(u: usize, c: usize) -> Query {
    Query::from("drafts")
        .filter(eq(col("chatId"), lit(uuid(id(CHAT, c)))))
        .filter(eq(col("userId"), lit(uuid(id(USER, u)))))
        .limit(1)
}

/// The seeded chats; nothing open yet.
pub struct ChatFixture {
    db: Db,
    members: usize,
    /// The newest message of each chat, and its `sentAtMs`.
    newest: Vec<usize>,
    next_message: usize,
    writes: i64,
    joins: usize,
}

impl ChatFixture {
    /// Seed `members` members (and as many chats), with the membership
    /// check written as `access`.
    pub fn seeded(members: usize, access: Access) -> Self {
        assert!(members > 7 * (CHATS_PER_MEMBER + 1));
        let db = block_on(Db::open(DbConfig::new(
            schema(access),
            DbIdentity {
                node: NodeUuid::from_bytes([0x57; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open chat fan-out db");
        let insert = |table: &str, row: RowUuid, values: Vec<(&str, Value)>| {
            block_on(db.insert(
                table,
                cells(values),
                InsertOptions {
                    row_id: Some(row),
                    ..Default::default()
                },
            ))
            .expect("seed chat fan-out fixture");
        };
        for u in 0..members {
            insert(
                "users",
                id(USER, u),
                vec![
                    ("name", text(format!("member {u}"))),
                    ("discoverable", Value::Bool(u % 2 == 0)),
                ],
            );
            insert(
                "unique_names",
                id(UNIQUE_NAME, u),
                vec![
                    ("userId", uuid(id(USER, u))),
                    ("name", text(format!("m{u}"))),
                ],
            );
            // A uuid claim: a relation-style policy compares it with
            // `users.id` as stored, and a uuid in text form would not match
            // there (the legacy `EXISTS` accepts either).
            db.set_identity_claims(
                member(u),
                BTreeMap::from([("\0claims:userId".to_owned(), Value::Uuid(id(USER, u).0))]),
            );
        }
        for c in 0..members {
            let chat = uuid(id(CHAT, c));
            insert(
                "chats",
                id(CHAT, c),
                vec![("title", text(format!("chat {c}")))],
            );
            insert(
                "calls",
                id(CALL, c),
                vec![
                    ("chatId", chat.clone()),
                    ("kind", text("group")),
                    ("ended", Value::Bool(c % 2 == 0)),
                ],
            );
            insert(
                "chat_agents",
                id(AGENT, c),
                vec![("chatId", chat), ("name", text("agent"))],
            );
        }
        for u in 0..members {
            for k in 0..CHATS_PER_MEMBER {
                let chat = uuid(id(CHAT, chat_of(u, k, members)));
                let n = membership(u, k);
                insert(
                    "chat_members",
                    id(MEMBER, n),
                    vec![
                        ("chatId", chat.clone()),
                        ("userId", uuid(id(USER, u))),
                        ("isBanned", Value::Bool(false)),
                        ("deletedAtMs", Value::Nullable(None)),
                        ("pinnedAtMs", Value::Nullable(None)),
                    ],
                );
                insert(
                    "message_read_marks",
                    id(MARK, n),
                    vec![
                        ("chatId", chat),
                        ("userId", uuid(id(USER, u))),
                        ("lastReadAtMs", Value::I64(n as i64)),
                    ],
                );
            }
            insert(
                "drafts",
                id(DRAFT, u),
                vec![
                    ("chatId", uuid(id(CHAT, u))),
                    ("userId", uuid(id(USER, u))),
                    ("body", text("")),
                    ("updatedAtMs", Value::I64(0)),
                ],
            );
        }
        // Member u is in chats u, u-7, u-14, ... (k = 0..5), so the
        // senders of chat c are c, c-7, ... mod members.
        let mut next_message = 0;
        let mut newest = Vec::with_capacity(members);
        for c in 0..members {
            for n in 0..MESSAGES_PER_CHAT {
                let reply = if n > 0 && n % 3 == 0 {
                    uuid(id(MESSAGE, next_message - 1))
                } else {
                    Value::Nullable(None)
                };
                let sender = (c + members - 7 * (n % CHATS_PER_MEMBER)) % members;
                insert(
                    "messages",
                    id(MESSAGE, next_message),
                    vec![
                        ("chatId", uuid(id(CHAT, c))),
                        ("senderId", uuid(id(USER, sender))),
                        ("body", text(format!("message {n}"))),
                        ("sentAtMs", Value::I64(n as i64)),
                        ("replyToId", reply),
                    ],
                );
                next_message += 1;
            }
            newest.push(next_message - 1);
        }
        block_on(db.tick()).expect("settle chat fan-out fixture");
        Self {
            db,
            members,
            newest,
            next_message,
            writes: 0,
            joins: 0,
        }
    }

    fn subscribe(&self, query: &Query, u: usize) -> SubscriptionStream {
        let prepared: PreparedQuery = self.db.prepare_query(query).expect("prepare live result");
        block_on(
            self.db
                .subscribe_for_identity(&prepared, ReadOpts::default(), member(u)),
        )
        .expect("open live result")
    }

    fn open(&self, streams: &mut [SubscriptionStream], rows: usize) {
        block_on(self.db.tick()).expect("hydrate live results");
        for stream in streams {
            let delta = drain(stream);
            assert!(delta.reset, "a live result opened");
            assert_eq!(delta.added, rows, "a live result shows its rows");
        }
    }

    /// Every member opens their chat list.
    pub fn open_chat_lists(self) -> ChatLists {
        let mut lists: Vec<_> = (0..self.members)
            .map(|u| self.subscribe(&chat_list(u), u))
            .collect();
        self.open(&mut lists, CHATS_PER_MEMBER);
        ChatLists {
            fixture: self,
            lists,
            joined: Vec::new(),
        }
    }

    /// Every member `u` opens room `u` (one of theirs) and, with
    /// `composers`, their composer's draft in it.
    pub fn open_rooms(self, composers: bool) -> OpenRooms {
        let mut rooms: Vec<_> = (0..self.members)
            .map(|u| self.subscribe(&room(u), u))
            .collect();
        self.open(&mut rooms, ROOM_PAGE);
        let mut drafts: Vec<_> = (0..if composers { self.members } else { 0 })
            .map(|u| self.subscribe(&composer(u, u), u))
            .collect();
        self.open(&mut drafts, 1);
        OpenRooms {
            fixture: self,
            rooms,
            drafts,
        }
    }

    /// Member `u` updates their read mark in chat `chat_of(u, 0)`: they read
    /// to the newest message.
    fn read_mark(&mut self, u: usize) {
        self.writes += 1;
        block_on(self.db.update(
            "message_read_marks",
            id(MARK, membership(u, 0)),
            cells(vec![("lastReadAtMs", Value::I64(1_000_000 + self.writes))]),
            UpdateOptions::default(),
        ))
        .expect("read mark update");
        block_on(self.db.tick()).expect("deliver read mark");
    }

    /// Member `u` posts in chat `u`, replying to its newest message.
    fn post(&mut self, u: usize) {
        let reply = self.newest[u];
        let row = self.next_message;
        self.next_message += 1;
        self.newest[u] = row;
        self.writes += 1;
        block_on(self.db.insert(
            "messages",
            cells(vec![
                ("chatId", uuid(id(CHAT, u))),
                ("senderId", uuid(id(USER, u))),
                ("body", text("ok, see you there")),
                ("sentAtMs", Value::I64(1_000_000 + self.writes)),
                ("replyToId", uuid(id(MESSAGE, reply))),
            ]),
            InsertOptions {
                row_id: Some(id(MESSAGE, row)),
                ..Default::default()
            },
        ))
        .expect("post message");
        block_on(self.db.tick()).expect("deliver message");
    }

    /// The fan-out counters (`woken`, `reran`, ...) since the fixture opened.
    pub fn fanout_stats(&self) -> jazz::db::FanoutStats {
        self.db.fanout_stats()
    }

    /// Member `u` joins a chat outside their list. Returns the membership.
    fn join(&mut self, u: usize) -> RowUuid {
        self.joins += 1;
        let row = id(MEMBER, 1_000_000 + self.joins);
        let chat = chat_of(u, CHATS_PER_MEMBER + 1, self.members);
        block_on(self.db.insert(
            "chat_members",
            cells(vec![
                ("chatId", uuid(id(CHAT, chat))),
                ("userId", uuid(id(USER, u))),
                ("isBanned", Value::Bool(false)),
                ("deletedAtMs", Value::Nullable(None)),
                ("pinnedAtMs", Value::Nullable(None)),
            ]),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .expect("join chat");
        block_on(self.db.tick()).expect("deliver join");
        row
    }

    /// Member `u` types one more character into their draft in chat `u`.
    fn draft(&mut self, u: usize) {
        self.writes += 1;
        let body: String = "ok, see you t"
            .chars()
            .take(1 + (self.writes as usize) % KEYSTROKES)
            .collect();
        block_on(self.db.update(
            "drafts",
            id(DRAFT, u),
            cells(vec![
                ("body", text(body)),
                ("updatedAtMs", Value::I64(self.writes)),
            ]),
            UpdateOptions::default(),
        ))
        .expect("draft update");
        block_on(self.db.tick()).expect("deliver draft");
    }
}

/// Every member's chat list open and hydrated.
pub struct ChatLists {
    fixture: ChatFixture,
    lists: Vec<SubscriptionStream>,
    joined: Vec<RowUuid>,
}

impl ChatLists {
    /// The member whose write the `n`-th iteration times: a stride through
    /// all members, so every chat takes its turn.
    fn writer(&self, n: i64) -> usize {
        (n as usize * 13) % self.fixture.members
    }

    /// One read-mark upsert: a member reads to the newest message of one of
    /// their chats. The four other members' lists show that mark as the
    /// chat's latest; nobody else's list changes. Returns the lists updated.
    pub fn read_mark(&mut self) -> usize {
        let u = self.writer(self.fixture.writes);
        self.fixture.read_mark(u);
        let updated = drain_all(&mut self.lists);
        assert_eq!(
            updated,
            CHATS_PER_MEMBER - 1,
            "the mark reached the chat's other members"
        );
        updated
    }

    /// One member joins a chat they were not in: their list gains it, and
    /// the chat's five members see them in its roster. Returns the lists
    /// changed. Undo it with [`ChatLists::leave`] (untimed).
    pub fn join(&mut self) -> usize {
        let u = self.writer(self.fixture.joins as i64);
        let row = self.fixture.join(u);
        self.joined.push(row);
        let changed = drain_all(&mut self.lists);
        assert_eq!(
            changed,
            CHATS_PER_MEMBER + 1,
            "the joiner's list and the chat's members changed"
        );
        changed
    }

    /// The last joiner leaves again (the membership is deleted), so every
    /// join starts from the seeded lists.
    pub fn leave(&mut self) {
        if let Some(row) = self.joined.pop() {
            block_on(
                self.fixture
                    .db
                    .delete("chat_members", row, DeleteOptions::default()),
            )
            .expect("leave chat");
            block_on(self.fixture.db.tick()).expect("deliver leave");
            drain_all(&mut self.lists);
        }
    }

    pub fn fanout_stats(&self) -> jazz::db::FanoutStats {
        self.fixture.fanout_stats()
    }
}

/// Every member's room (and composer) open and hydrated.
pub struct OpenRooms {
    fixture: ChatFixture,
    rooms: Vec<SubscriptionStream>,
    drafts: Vec<SubscriptionStream>,
}

impl OpenRooms {
    fn writer(&self) -> usize {
        (self.fixture.writes as usize * 13) % self.fixture.members
    }

    /// One new message, a reply, in one open room: that room shows it and
    /// drops its oldest shown message; the other rooms stay quiet.
    pub fn new_message(&mut self) -> usize {
        let u = self.writer();
        self.fixture.post(u);
        let changed = drain_all(&mut self.rooms);
        assert_eq!(changed, 1, "only the room the message landed in changed");
        changed
    }

    /// A member types a short message: 13 draft upserts, one per keystroke,
    /// each reaching their composer, then the message, reaching their room.
    /// Returns the results updated (14).
    pub fn type_and_send(&mut self) -> usize {
        let u = self.writer();
        let mut updated = 0;
        for _ in 0..KEYSTROKES {
            self.fixture.draft(u);
            updated += drain_all(&mut self.drafts);
        }
        self.fixture.post(u);
        updated += drain_all(&mut self.rooms);
        assert_eq!(updated, KEYSTROKES + 1, "every draft and the message shown");
        updated
    }

    pub fn fanout_stats(&self) -> jazz::db::FanoutStats {
        self.fixture.fanout_stats()
    }
}

#[derive(Default)]
struct Drained {
    events: usize,
    reset: bool,
    added: usize,
}

fn drain(stream: &mut SubscriptionStream) -> Drained {
    let mut out = Drained::default();
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta { reset, added, .. } => {
                out.events += 1;
                out.reset |= reset;
                out.added += added.len();
            }
            SubscriptionEvent::Rejected { reason, .. } => {
                panic!("live result rejected: {reason:?}")
            }
            SubscriptionEvent::Closed => panic!("live result closed"),
        }
    }
    out
}

/// Drain every stream; the number that had an event.
fn drain_all(streams: &mut [SubscriptionStream]) -> usize {
    streams
        .iter_mut()
        .map(|stream| drain(stream).events)
        .filter(|&events| events > 0)
        .count()
}
