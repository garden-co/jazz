//! The stable string spelling of every public `ErrorCode` and `Reason`.
//!
//! Bindings hand these strings to JavaScript as `error.code` and mutation-error
//! events report them as their `code`, so they are a durable contract: a
//! renamed variant must not change what a TypeScript caller matches on. The
//! sync wire carries a rejection's `Reason` by its string, so those are a
//! wire contract too.

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, Reason};
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
    ErrorCode::UnsupportedStorageCodecs,
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
        ErrorCode::UnsupportedStorageCodecs => "unsupported_storage_codecs",
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

fn pinned_reason(reason: Reason) -> &'static str {
    match reason {
        Reason::AuthorizationDenied => "authorization_denied",
        Reason::ClientClockTooFarAhead => "client_clock_too_far_ahead",
        Reason::SerializationConflict => "serialization_conflict",
        Reason::RowAlreadyDeleted => "row_already_deleted",
        Reason::RowAlreadyExists => "row_already_exists",
        Reason::RowNotDeleted => "row_not_deleted",
        Reason::MissingRequired => "missing_required",
        Reason::BranchMove => "branch_move",
        Reason::MissingChunks => "missing_chunks",
        Reason::PermissionsHeadMissing => "permissions_head_missing",
        Reason::PredecessorRejected => "predecessor_rejected",
        Reason::SchemaVersionNotDeployed => "schema_version_not_deployed",
        Reason::IdentityMismatch => "identity_mismatch",
    }
}

/// Contract: each `Reason` has exactly the pinned snake_case string, no two
/// share one, and the name reads back as the reason. A name this build does
/// not know (a newer peer's reason) reads as no reason at all.
#[test]
fn every_reason_has_its_pinned_stable_string() {
    let mut seen = std::collections::BTreeSet::new();
    for reason in Reason::ALL {
        assert_eq!(reason.as_str(), pinned_reason(reason), "{reason:?}");
        assert_eq!(Reason::from_name(reason.as_str()), Some(reason));
        assert_eq!(
            serde_json::to_value(reason).unwrap(),
            reason.as_str(),
            "serde spells {reason:?} by its stable name"
        );
        assert!(
            seen.insert(reason.as_str()),
            "duplicate reason string for {reason:?}"
        );
    }
    assert_eq!(Reason::from_name("a_reason_from_a_newer_peer"), None);
}

/// Contract: the message prefixes of policy and clock-skew rejections are
/// their reasons' names, so the text earlier bindings matched is unchanged.
#[test]
fn rejection_message_prefixes_are_reason_names() {
    assert_eq!(
        jazz::db::AUTHORIZATION_DENIED,
        Reason::AuthorizationDenied.as_str()
    );
    assert_eq!(
        jazz::db::CLIENT_CLOCK_TOO_FAR_AHEAD,
        Reason::ClientClockTooFarAhead.as_str()
    );
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
    let alice = jazz::db::block_on(Db::open(DbConfig {
        schema,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
        id_source: None,
        chunk_store: None,
        migrations: Default::default(),
        system_node: None,
    }))
    .expect("alice opens a database");

    let error = alice
        .prepare_query(&Query::from("missing"))
        .expect_err("querying an undeclared table fails");

    assert_eq!(error.code, ErrorCode::Query);
    assert_eq!(error.code.as_str(), "query");
    assert_eq!(error.reason, None, "a query error has no finer reason");
    assert_eq!(
        error.to_string(),
        "Query: unknown table missing",
        "the binding-visible message is unchanged by the stable code"
    );
}
