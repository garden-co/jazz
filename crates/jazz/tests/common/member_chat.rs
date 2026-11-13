//! A group-chat schema whose read policies read the membership and
//! people tables, and a person's room list over it: the shape that keeps
//! policy-read tables on whole-table watches. Shared by
//! `fanout_policy_pins` and the `fanout_policy_tables` bench.
//!
//! ```text
//! rooms   ◄── members(room, person, banned, left_at, pinned_at) ──► people
//!   ▲                                                                  ▲
//!   ├── calls(room, kind, ended)                    handles(person) ───┘
//!   ├── seen(room, person, at)
//!   └── bots(room, name)
//! ```
//!
//! "Me" is the person row named by the `person` claim. Rooms, calls, seen
//! marks and bots are readable to the room's members (`EXISTS members
//! WHERE room = row.room AND person = claims.person`). A member row is
//! readable when it is mine or its room is one of mine. People are
//! readable when they are me or discoverable; a handle when its person is
//! (`INHERITS SELECT VIA person`).

use jazz::ids::RowUuid;
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, is_null, lit, ne};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use super::{compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq};

pub const PERSON_CLAIM: [&str; 2] = ["claims", "person"];

/// The claims naming `person` as me, as `Db::set_identity_claims` takes
/// them.
pub fn claims(person: RowUuid) -> std::collections::BTreeMap<String, Value> {
    std::collections::BTreeMap::from([(
        "\0claims:person".to_owned(),
        Value::String(person.0.to_string()),
    )])
}

/// The current row's `column` names a room I am a member of.
pub fn member_of(column: &str) -> PolicyExpr {
    exists(
        "members",
        vec![
            outer_eq("room", column),
            session_eq("person", &PERSON_CLAIM),
        ],
    )
}

pub fn schema() -> JazzSchema {
    let read = read_and_allow_all_writes;
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("people")
                    .column("name", ColumnType::Text)
                    .column("discoverable", ColumnType::Boolean)
                    .policies(read(PolicyExpr::or(vec![
                        session_eq("id", &PERSON_CLAIM),
                        PolicyExpr::eq_literal("discoverable", jazz::tools::Value::Boolean(true)),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("handles")
                    .fk_column("person", "people")
                    .column("handle", ColumnType::Text)
                    .policies(read(PolicyExpr::inherits(Operation::Select, "person"))),
            )
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("title", ColumnType::Text)
                    .policies(read(member_of("id"))),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("room", "rooms")
                    .fk_column("person", "people")
                    .column("banned", ColumnType::Boolean)
                    .nullable_column("left_at", ColumnType::BigInt)
                    .nullable_column("pinned_at", ColumnType::BigInt)
                    .index_only(["person"])
                    .index_only(["room"])
                    .policies(read(PolicyExpr::or(vec![
                        session_eq("person", &PERSON_CLAIM),
                        member_of("room"),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("calls")
                    .fk_column("room", "rooms")
                    .column("kind", ColumnType::Text)
                    .column("ended", ColumnType::Boolean)
                    .policies(read(member_of("room"))),
            )
            .table(
                TableSchemaBuilder::new("seen")
                    .fk_column("room", "rooms")
                    .fk_column("person", "people")
                    .column("at", ColumnType::BigInt)
                    .policies(read(member_of("room"))),
            )
            .table(
                TableSchemaBuilder::new("bots")
                    .fk_column("room", "rooms")
                    .column("name", ColumnType::Text)
                    .policies(read(member_of("room"))),
            )
            .build(),
    )
}

/// `person`'s room list (relations named apart from columns, as the
/// adopter's `chat` is from `chatId`): their memberships, each with its room, the
/// room's members (each with their person and handles), its open group
/// call, the latest seen mark of someone else, and its bots.
pub fn room_list(person: RowUuid) -> Query {
    let me = Value::Uuid(person.0);
    Query::from("members")
        .filter(eq(col("person"), lit(me.clone())))
        .filter(eq(col("banned"), lit(Value::Bool(false))))
        .filter(is_null(col("left_at")))
        .order_by("pinned_at", OrderDirection::Desc)
        .order_by("id", OrderDirection::Desc)
        .array_subquery(
            ArraySubquery::new("chat", "rooms", "id", "room")
                .nested(
                    ArraySubquery::new("roster", "members", "room", "id").nested(
                        ArraySubquery::new("profile", "people", "id", "person")
                            .nested(ArraySubquery::new("handles", "handles", "person", "id")),
                    ),
                )
                .nested(
                    ArraySubquery::new("call", "calls", "room", "id")
                        .filter(eq(col("kind"), lit(Value::String("group".into()))))
                        .filter(eq(col("ended"), lit(Value::Bool(false))))
                        .limit(1),
                )
                .nested(
                    ArraySubquery::new("last_seen", "seen", "room", "id")
                        .filter(ne(col("person"), lit(me)))
                        .order_by("at", OrderDirection::Desc)
                        .limit(1),
                )
                .nested(ArraySubquery::new("bots", "bots", "room", "id")),
        )
}
