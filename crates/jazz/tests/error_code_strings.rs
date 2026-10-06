//! The stable string spelling of every public `ErrorCode`.
//!
//! Bindings hand these strings to JavaScript as `error.code` and mutation-error
//! events report them as their `code`, so they are a durable contract: a
//! renamed variant must not change what a TypeScript caller matches on.

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode};
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

/// Every variant, spelled out. The exhaustive `match` in `pinned` stops this
/// file compiling when a variant is added, so a new code must be pinned here.
const ALL: [ErrorCode; 10] = [
    ErrorCode::Schema,
    ErrorCode::Query,
    ErrorCode::WriteRejected,
    ErrorCode::TransactionConflict,
    ErrorCode::Storage,
    ErrorCode::Protocol,
    ErrorCode::Backpressure,
    ErrorCode::NotObserved,
    ErrorCode::HistoricalReadRequiresServer,
    ErrorCode::Busy,
];

fn pinned(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Schema => "schema",
        ErrorCode::Query => "query",
        ErrorCode::WriteRejected => "write_rejected",
        ErrorCode::TransactionConflict => "transaction_conflict",
        ErrorCode::Storage => "storage",
        ErrorCode::Protocol => "protocol",
        ErrorCode::Backpressure => "backpressure",
        ErrorCode::NotObserved => "not_observed",
        ErrorCode::HistoricalReadRequiresServer => "historical_read_requires_server",
        ErrorCode::Busy => "busy",
    }
}

/// Contract: each `ErrorCode` has exactly the pinned snake_case string, and no
/// two codes share one. Pure table check: there is no actor, only the
/// published vocabulary a TypeScript caller classifies errors by.
#[test]
fn every_error_code_has_its_pinned_stable_string() {
    let mut seen = std::collections::BTreeSet::new();
    for code in ALL {
        assert_eq!(code.as_str(), pinned(code), "{code:?}");
        assert!(
            seen.insert(code.as_str()),
            "duplicate code string for {code:?}"
        );
    }
}

/// Contract: a real error from the public API carries a code whose stable
/// string is independent of the unchanged human-readable `Display` text.
///
/// ```text
/// alice ──prepare_query(missing table)──► Db ──✗── Error { code, message }
///                                                  ├── code.as_str() == "query"
///                                                  └── to_string() == "Query: unknown table missing"
/// ```
#[test]
fn a_public_api_error_keeps_its_display_text_and_exposes_the_stable_code() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
            .build(),
    )
    .expect("public schema compiles");
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let alice = jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage: TestStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
        id_source: None,
    }))
    .expect("alice opens a database");

    let error = alice
        .prepare_query(&Query::from("missing"))
        .expect_err("querying an undeclared table fails");

    assert_eq!(error.code, ErrorCode::Query);
    assert_eq!(error.code.as_str(), "query");
    assert_eq!(
        error.to_string(),
        "Query: unknown table missing",
        "the binding-visible message is unchanged by the stable code"
    );
}
