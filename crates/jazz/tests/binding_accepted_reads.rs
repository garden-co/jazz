//! The shared host admission must retain the core identity and read lifecycle.
use crate::common::{allow_all_policies, compile_schema};
use jazz::binding_host::{Binding, HostError};
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::tools::{ColumnType, OpenTransactionId, Schema, SchemaBuilder, TableSchemaBuilder};
use serde_json::json;

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .build()
}
fn open(node: u8, system: bool, backend: bool) -> Binding {
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: if system {
            AuthorSubject::SYSTEM
        } else {
            AuthorSubject::for_test_bytes([node; 16])
        },
    };
    let db = block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(&schema()),
        identity,
    )))
    .unwrap();
    db.set_authority();
    Binding::new(db, identity, backend)
}
fn query() -> Vec<u8> {
    postcard::to_allocvec(&Query::from("notes")).unwrap()
}
fn header(bytes: &[u8]) -> serde_json::Value {
    let length = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    serde_json::from_slice(&bytes[4..4 + length]).unwrap()
}
fn read(binding: &Binding, id: &str) -> Vec<u8> {
    block_on(
        binding
            .accepted_read(query(), id, false, 1000.0, None, None)
            .unwrap(),
    )
    .unwrap()
}

/// Alice's ordinary omitted/false admissions stay unbound; an accepted read
/// refuses without preventing an ordinary read or commit on the same handle.
#[test]
fn omitted_and_false_capability_preserve_ordinary_unbound_batches() {
    let alice = open(1, false, false);
    for omitted in [false, true] {
        let id = OpenTransactionId::new().to_string();
        if omitted {
            alice
                .begin_transaction(&id, "serializable", None, None)
                .unwrap();
        } else {
            alice
                .begin_transaction_with_capability(&id, "serializable", None, None, false)
                .unwrap();
        }
        assert!(
            alice
                .accepted_read(query(), &id, false, 1000.0, None, None)
                .is_err()
        );
        let rows = block_on(
            alice
                .all(
                    query(),
                    &json!({"tier":"local-only"}),
                    Some(&id),
                    None,
                    None,
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rows, vec![0]);
        alice.commit_transaction(&id, Some("serializable")).unwrap();
    }
}

/// Alice's actual opened identity and registered claims survive opt-in from a
/// retained schema view; changing those claims later invalidates the batch.
#[test]
fn implicit_peer_and_claims_are_shared_by_schema_views() {
    let alice = open(1, false, false);
    alice.set_session_claims(json!({"team":"a"})).unwrap();
    let view = alice
        .register_schema(&serde_json::to_vec(&schema()).unwrap())
        .unwrap();
    assert_eq!(view.author(), alice.author());
    let id = OpenTransactionId::new().to_string();
    view.begin_transaction_with_capability(&id, "serializable", None, None, true)
        .unwrap();
    let result = header(&read(&view, &id));
    assert_eq!(result["format"], "jazz.accepted-read.host.v1");
    assert_eq!(result["evidence"], json!([]));
    alice.commit_transaction(&id, Some("serializable")).unwrap();

    let id = OpenTransactionId::new().to_string();
    view.begin_transaction_with_capability(&id, "serializable", None, None, true)
        .unwrap();
    read(&view, &id);
    alice.set_session_claims(json!({"team":"b"})).unwrap();
    let error = alice
        .commit_transaction(&id, Some("serializable"))
        .err()
        .unwrap();
    assert!(
        matches!(error, HostError::Engine(ref error) if error.code == ErrorCode::WriteRejected && error.message.contains("claims changed"))
    );
}

/// Bob's backend explicitly binds Alice before its read-only transaction. The
/// raw existing author path supplies B's identity requirement without SDK state.
#[test]
fn explicit_session_binds_the_core_for_read_only_accepted_work() {
    let bob = open(2, true, true);
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    bob.set_identity_claims(alice.canonical().as_bytes(), json!({"team":"a"}))
        .unwrap();
    let id = OpenTransactionId::new().to_string();
    bob.begin_transaction(
        &id,
        "serializable",
        Some(alice.canonical().as_bytes()),
        None,
    )
    .unwrap();
    read(&bob, &id);
    bob.commit_transaction(&id, Some("serializable")).unwrap();
}

/// Alice and Bob's invalid capability admissions fail before creating a batch;
/// each rejected opaque id remains available for a valid explicit session.
#[test]
fn refused_capabilities_leave_no_handle_or_identity_side_effects() {
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    for (system, backend, kind, attribution) in [
        (false, false, "mergeable", false),
        (false, false, "serializable", true),
        (true, false, "serializable", false),
        (true, true, "serializable", false),
        (false, true, "serializable", false),
    ] {
        let db = open(2, system, backend);
        let id = OpenTransactionId::new().to_string();
        assert!(
            db.begin_transaction_with_capability(
                &id,
                kind,
                None,
                attribution.then_some(alice.canonical().as_bytes()),
                true
            )
            .is_err()
        );
        db.begin_transaction_with_capability(
            &id,
            "serializable",
            Some(alice.canonical().as_bytes()),
            None,
            true,
        )
        .unwrap();
        read(&db, &id);
        db.rollback_transaction(&id).unwrap();
    }
}

/// Bob's SYSTEM backend keeps ordinary attributed transactions; Alice's peer
/// and an accepted capability both refuse attribution before admitting a batch.
#[test]
fn ordinary_attribution_keeps_its_backend_guard() {
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    let backend = open(2, true, true);
    let id = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(
            &id,
            "serializable",
            None,
            Some(alice.canonical().as_bytes()),
        )
        .unwrap();
    assert!(
        backend
            .accepted_read(query(), &id, false, 1000.0, None, None)
            .is_err()
    );
    backend
        .commit_transaction(&id, Some("serializable"))
        .unwrap();
    let peer = open(1, false, false);
    assert!(
        peer.begin_transaction(
            &id,
            "serializable",
            None,
            Some(alice.canonical().as_bytes())
        )
        .is_err()
    );
    peer.begin_transaction(&id, "serializable", None, None)
        .unwrap();
    peer.rollback_transaction(&id).unwrap();
}

/// Alice cannot replace an admitted scope or pass an unsafe JS timeout. A
/// rejected argument leaves her valid accepted transaction usable.
#[test]
fn timeout_and_identity_validation_precede_read_admission() {
    let alice = open(1, false, false);
    let id = OpenTransactionId::new().to_string();
    alice
        .begin_transaction_with_capability(&id, "serializable", None, None, true)
        .unwrap();
    for timeout in [-1.0, 0.5, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0] {
        let error = alice
            .accepted_read(query(), &id, false, timeout, None, None)
            .err()
            .unwrap();
        assert!(
            matches!(error, HostError::Argument(ref message) if message.contains("safe integer"))
        );
    }
    let bob = AuthorSubject::for_test_bytes([2; 16]);
    assert!(
        alice
            .accepted_read(
                query(),
                &id,
                false,
                1000.0,
                Some(bob.canonical().as_bytes()),
                None
            )
            .is_err()
    );
    read(&alice, &id);
    alice.commit_transaction(&id, Some("serializable")).unwrap();
}

/// Alice's bound ordinary reads inherit registered claims when omitted, through
/// owner and retained handles. Explicit empty overrides and real drift refuse.
#[test]
fn mixed_reads_inherit_live_claims_without_erasing_explicit_overrides() {
    for retained in [false, true] {
        for explicit in [false, true] {
            for mode in ["stable", "drift", "stale-explicit", "empty-override"] {
                let alice = open(1, false, false);
                alice.set_session_claims(json!({"team":"a"})).unwrap();
                let view = alice
                    .register_schema(&serde_json::to_vec(&schema()).unwrap())
                    .unwrap();
                let binding = if retained { &view } else { &alice };
                let author = binding.author().canonical().to_string();
                let id = OpenTransactionId::new().to_string();
                binding
                    .begin_transaction_with_capability(
                        &id,
                        "serializable",
                        explicit.then_some(author.as_bytes()),
                        None,
                        true,
                    )
                    .unwrap();
                read(binding, &id);
                if mode == "drift" || mode == "stale-explicit" {
                    alice.set_session_claims(json!({"team":"b"})).unwrap();
                }
                let rows = block_on(
                    binding
                        .all(
                            query(),
                            &json!({"tier":"local-only"}),
                            Some(&id),
                            None,
                            match mode {
                                "empty-override" => Some(json!({})),
                                "stale-explicit" => Some(json!({"team":"a"})),
                                _ => None,
                            },
                        )
                        .unwrap(),
                );
                if mode == "stable" {
                    assert_eq!(rows.unwrap(), vec![0]);
                    binding
                        .commit_transaction(&id, Some("serializable"))
                        .unwrap();
                } else {
                    assert!(rows.is_err(), "scope mismatch was ignored: {mode}");
                    assert!(
                        binding
                            .commit_transaction(&id, Some("serializable"))
                            .is_err()
                    );
                }
            }
        }
    }
}

/// Alice’s backend or authority-side binding cannot request own-client reads.
#[test]
fn own_client_evidence_binding_refuses_authority_and_backend_modes() {
    for (system, backend) in [(false, false), (true, false), (true, true)] {
        let host = open(1, system, backend);
        let id = OpenTransactionId::new().to_string();
        assert!(host.begin_client_evidence_transaction(&id).is_err());
        assert!(host.commit_transaction(&id, Some("serializable")).is_err());
    }
}
