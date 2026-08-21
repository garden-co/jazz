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
