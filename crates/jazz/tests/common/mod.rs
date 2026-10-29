#![allow(dead_code)]

use jazz::schema::JazzSchema;
use jazz::tools::{PolicyExpr, Schema, TablePolicies};

pub fn compile_schema(source: &Schema) -> JazzSchema {
    jazz::schema::JazzSchema::new(source).expect("integration-test public schema compiles")
}

pub fn allow_all_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

pub fn allow_all_writes() -> TablePolicies {
    TablePolicies::new()
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

pub fn read_and_allow_all_writes(read: PolicyExpr) -> TablePolicies {
    allow_all_writes().with_select(read)
}

pub fn session_eq(column: &str, path: &[&str]) -> PolicyExpr {
    PolicyExpr::eq_session(
        column,
        path.iter().map(|segment| (*segment).to_owned()).collect(),
    )
}

pub fn outer_eq(column: &str, outer_column: &str) -> PolicyExpr {
    session_eq(column, &["__jazz_outer_row", outer_column])
}

pub fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::and(conditions)),
    }
}

/// Give each admitted fixture scope a distinct ordinary usage handle.
pub fn direct_subscription(
    schema: &JazzSchema,
    table: &str,
    identity: jazz::ids::AuthorSubject,
) -> jazz::protocol::SubscriptionKey {
    let shape = jazz::query::Query::from(table).validate(schema).unwrap();
    jazz::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        // Fixture handles are distinct for the upstream SYSTEM subscription
        // and the downstream user's subscription, even with identical queries.
        // These fixtures have one immutable claims snapshot per identity.
        binding_id: jazz::query::BindingId(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            identity.canonical().as_bytes(),
        )),
        read_view: Default::default(),
    }
}

/// The authority rejected a serializable commit because a read it made
/// no longer holds.
#[track_caller]
pub fn assert_serialization_conflict(error: &jazz::db::Error) {
    assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected, "{error}");
    // The reason crosses the sync wire with the rejection; the message
    // keeps the text earlier bindings matched.
    assert_eq!(
        error.reason,
        Some(jazz::db::Reason::SerializationConflict),
        "{error}"
    );
    assert!(error.is_serialization_conflict(), "{error}");
    assert!(error.message.contains("SerializationConflict"), "{error}");
}

/// `text` with each stored column's hash suffix (`~` and six hex digits,
/// SPEC 10.7) removed: physical names, SQL and plans as the logical names
/// spell them.
pub fn unhashed(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        // Only a suffix of a name: `~` after a name character, then
        // exactly six lowercase hex digits and no further name character.
        if bytes[i] == b'~'
            && i > 0
            && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_')
            && bytes.len() >= i + 7
            && bytes[i + 1..i + 7]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
            && bytes
                .get(i + 7)
                .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_')
        {
            i += 7;
            continue;
        }
        let c = text[i..].chars().next().unwrap();
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// The physical column logical `column` of stored table `table` is kept
/// in (`column~` and its hash, SPEC 10.7), quoted for SQL.
pub fn physical_column(conn: &rusqlite::Connection, table: &str, column: &str) -> String {
    let mut statement = conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .unwrap();
    let names: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let name = names
        .into_iter()
        .find(|name| unhashed(name) == column)
        .unwrap_or_else(|| panic!("{table} stores no column {column}"));
    format!("\"{name}\"")
}
