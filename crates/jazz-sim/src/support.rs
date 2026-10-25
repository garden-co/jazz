//! Small helpers every scenario uses: schemas, identities, ids, writes.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, Error, InsertOptions, WriteHandle, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{PolicyExpr, Schema, TablePolicies};
use jazz::tx::DurabilityTier;

use crate::net::Sim;

pub fn compile(schema: &Schema) -> JazzSchema {
    JazzSchema::new(schema).expect("scenario schema compiles")
}

pub fn allow_all() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

/// Read gated by `read`; every write allowed.
pub fn read_gated(read: PolicyExpr) -> TablePolicies {
    allow_all().with_select(read)
}

/// `column` equals the session value at `path`.
pub fn session_eq(column: &str, path: &[&str]) -> PolicyExpr {
    PolicyExpr::eq_session(
        column,
        path.iter().map(|segment| (*segment).to_owned()).collect(),
    )
}

/// `column` equals the reading user's subject.
pub fn is_me(column: &str) -> PolicyExpr {
    session_eq(column, &["user", "identity", "subject"])
}

/// `column` equals `outer_column` of the row the policy is evaluated for.
pub fn outer_eq(column: &str, outer_column: &str) -> PolicyExpr {
    session_eq(column, &["__jazz_outer_row", outer_column])
}

pub fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::and(conditions)),
    }
}

/// A test user.
pub fn user(n: u32) -> AuthorSubject {
    let mut bytes = [0x5a; 16];
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

/// A user's subject as it appears in rows and policies.
pub fn subject(user: AuthorSubject) -> Value {
    Value::String(user.principal_parts().1)
}

/// A deterministic row id: `kind` names the table, `n` the row.
pub fn row(kind: u8, n: u64) -> RowUuid {
    let mut bytes = [kind; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

pub fn uuid(id: RowUuid) -> Value {
    Value::Uuid(id.0)
}

pub fn text(value: impl Into<String>) -> Value {
    Value::String(value.into())
}

pub fn int(value: i64) -> Value {
    Value::I64(value)
}

pub fn cells<const N: usize>(pairs: [(&str, Value); N]) -> BTreeMap<String, Value> {
    pairs
        .into_iter()
        .map(|(column, value)| (column.to_owned(), value))
        .collect()
}

/// Open an in-memory `Db`.
pub fn open(schema: JazzSchema, node: u32, author: AuthorSubject) -> Db {
    block_on(Db::open(config(schema, node, author))).expect("open scenario db")
}

pub fn config(schema: JazzSchema, node: u32, author: AuthorSubject) -> DbConfig {
    let mut bytes = [0x4e; 16];
    bytes[12..].copy_from_slice(&node.to_be_bytes());
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes(bytes),
            author,
        },
    )
}

/// Insert a row with a known id.
pub fn insert(db: &Db, table: &str, id: RowUuid, values: BTreeMap<String, Value>) -> WriteHandle {
    block_on(db.insert(
        table,
        values,
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap_or_else(|error| panic!("insert into {table}: {error}"))
}

/// Wait, stepping `sim`, until the write's transaction reaches `tier` on `db`.
pub fn wait(sim: &Sim, db: &Db, handle: &WriteHandle, tier: DurabilityTier) -> Result<u64, Error> {
    sim.drive(db.wait_for_transaction(handle.mergeable_tx_id(), tier))
}

/// Wait until the authority accepted the write.
pub fn settle(sim: &Sim, db: &Db, handle: &WriteHandle) {
    wait(sim, db, handle, DurabilityTier::Global).expect("write settles at the authority");
}
