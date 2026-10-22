//! E12 host bindings preserve the distinction between authenticated admission
//! and backend provenance, including staged uploads and transaction rollback.
mod common;

use std::collections::BTreeMap;
use std::path::Path;

use jazz::binding_host::{Binding, HostError, StagedStreamingMutation};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use serde_json::json;

fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}

fn identity(node: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    }
}

fn open(node: u8, backend: bool, path: Option<&Path>) -> Binding {
    let policy = PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    );
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("files")
                    .column("owner", ColumnType::Text)
                    .nullable_column("payload", ColumnType::Bytea)
                    .policies(
                        TablePolicies::new()
                            .with_select(policy.clone())
                            .with_insert(policy.clone())
                            .with_update(Some(policy.clone()), policy)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let id = identity(
        node,
        if backend {
            AuthorSubject::SYSTEM
        } else {
            author("alice")
        },
    );
    let config = DbConfig::new(schema, id);
    let db = match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    Binding::new(db, id, backend)
}

fn cells(owner: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("owner", ValueType::String)]);
    let row = descriptor
        .create(&[Value::String(owner.to_owned())])
        .unwrap();
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(row, descriptor)).unwrap()
}

fn bytes() -> Vec<u8> {
    (0..200_000).map(|i| (i % 251) as u8).collect()
}

fn stage(
    db: &Binding,
    id: u8,
    session: Option<&[u8]>,
    attribution: Option<&[u8]>,
) -> StagedStreamingMutation {
    let mut upload = db
        .begin_streaming_mutation(
            "files".into(),
            &[id; 16],
            &cells("alice"),
            "payload".into(),
            None,
            session,
            attribution,
            None,
            None,
            None,
        )
        .unwrap();
    for chunk in bytes().chunks(4096) {
        upload.push(chunk).unwrap();
    }
    upload.stage().unwrap()
}

fn rows(db: &Binding) -> Vec<jazz::db::CurrentRow> {
    let query = db
        .db
        .prepare_query(&Query::from("files").select([
            "owner",
            "$createdBy.identity.subject",
            "$updatedBy.identity.subject",
        ]))
        .unwrap();
    block_on(db.db.all(&query, ReadOpts::default())).unwrap()
}

fn subject(row: &jazz::db::CurrentRow, column: &str) -> String {
    let value = row
        .cells()
        .find(|(name, _)| *name == column)
        .unwrap()
        .1
        .unwrap();
    match value {
        Value::String(value) => value.clone(),
        Value::Nullable(Some(value)) => match &**value {
            Value::String(value) => value.clone(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

fn core_error(error: HostError) -> jazz::db::Error {
    match error {
        HostError::Engine(error) => error,
        other => panic!("{other:?}"),
    }
}

/// Alice attaches ciphertext and a companion row through the host API; Mallory
/// cannot attach Alice-owned data. Every row inherits its session for policy
/// admission and provenance; uncommitted uploads remain invisible.
#[test]
fn authenticated_binding_batches_apply_session_policies_to_uploads_and_companions() {
    let backend = open(1, true, None);
    let alice = author("alice").canonical().to_owned();
    let mallory = author("mallory").canonical().to_owned();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", Some(alice.as_bytes()), None)
        .unwrap();
    stage(&backend, 1, Some(alice.as_bytes()), None)
        .attach(&tx)
        .unwrap();
    backend
        .insert_in_transaction(
            &tx,
            "files",
            &cells("alice"),
            &json!({"rowId": vec![2; 16]}),
        )
        .unwrap();
    assert!(rows(&backend).is_empty());
    backend
        .commit_transaction(&tx, Some("serializable"))
        .unwrap();
    let committed = rows(&backend);
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].seq(), committed[1].seq());
    for row in committed {
        assert_eq!(subject(&row, "$createdBy.identity.subject"), "alice");
    }

    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", Some(mallory.as_bytes()), None)
        .unwrap();
    let mut forbidden = stage(&backend, 3, Some(mallory.as_bytes()), None);
    assert_eq!(
        core_error(forbidden.attach(&tx).unwrap_err()).code,
        ErrorCode::WriteRejected
    );
    assert!(forbidden.attach(&tx).is_err());
    backend.rollback_transaction(&tx).unwrap();
    assert_eq!(rows(&backend).len(), 2);
}

/// A backend records Bob as the writer while SYSTEM authorizes its transaction.
/// Alice-only policy would reject Bob's session, but attribution grants Bob no
/// session access. Provenance and ciphertext survive reopening the SQLite file.
#[test]
fn backend_attribution_is_atomic_inherited_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let backend = open(2, true, Some(&path));
    let alice = author("alice").canonical().to_owned();
    let bob = author("bob").canonical().to_owned();
    backend
        .insert(
            "files",
            &cells("alice"),
            &json!({"rowId": vec![9; 16], "attribution": alice.as_bytes()}),
        )
        .unwrap();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", None, Some(bob.as_bytes()))
        .unwrap();
    stage(&backend, 1, None, Some(bob.as_bytes()))
        .attach(&tx)
        .unwrap();
    backend
        .insert_in_transaction(
            &tx,
            "files",
            &cells("alice"),
            &json!({"rowId": vec![2; 16]}),
        )
        .unwrap();
    backend
        .update_in_transaction(&tx, "files", &[9; 16], &cells("alice"), &json!({}))
        .unwrap();
    assert_eq!(rows(&backend).len(), 1);
    backend
        .commit_transaction(&tx, Some("serializable"))
        .unwrap();
    drop(backend);

    let reopened = open(2, true, Some(&path));
    let committed = rows(&reopened);
    assert_eq!(committed.len(), 3);
    for row in &committed {
        assert_eq!(row.seq(), committed[0].seq());
        assert_eq!(
            subject(row, "$createdBy.identity.subject"),
            if row.row_uuid() == RowUuid::from_bytes([9; 16]) {
                "alice"
            } else {
                "bob"
            }
        );
        assert_eq!(subject(row, "$updatedBy.identity.subject"), "bob");
    }
    assert_eq!(
        block_on(reopened.db.read_value_range(
            "files",
            RowUuid::from_bytes([1; 16]),
            "payload",
            0..200_000
        ))
        .unwrap(),
        bytes()
    );
    let query = reopened.db.prepare_query(&Query::from("files")).unwrap();
    assert!(
        block_on(
            reopened
                .db
                .all_for_identity(&query, ReadOpts::default(), author("bob"))
        )
        .unwrap()
        .is_empty()
    );
}

/// Mallory cannot enter backend attribution through the untrusted host or the
/// raw core. Bob's capability cannot move to Alice's attributed batch, and a
/// successful attachment still disappears when its transaction is rolled back.
#[test]
fn attribution_admission_and_capabilities_cannot_change_identity() {
    let client = open(3, false, None);
    let bob = author("bob").canonical().to_owned();
    let alice = author("alice").canonical().to_owned();
    let tx = OpenTransactionId::new().to_string();
    assert!(
        client
            .begin_transaction(&tx, "serializable", None, Some(bob.as_bytes()))
            .is_err()
    );
    client
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    assert!(
        client
            .insert_in_transaction(
                &tx,
                "files",
                &cells("alice"),
                &json!({"attribution": bob.as_bytes()})
            )
            .is_err()
    );
    client.rollback_transaction(&tx).unwrap();
    assert!(
        block_on(client.db.insert(
            "files",
            BTreeMap::from([("owner".into(), Value::String("alice".into()))]),
            InsertOptions {
                identity: WriteIdentity::Attribution(author("bob")),
                ..Default::default()
            }
        ))
        .is_err()
    );
    assert!(
        block_on(
            client
                .db
                .begin_serializable_for_attribution(OpenTransactionId::new(), author("bob"))
        )
        .is_err()
    );
    assert!(rows(&client).is_empty());

    let backend = open(4, true, None);
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", None, Some(alice.as_bytes()))
        .unwrap();
    let mut wrong = stage(&backend, 1, None, Some(bob.as_bytes()));
    assert!(wrong.attach(&tx).is_err());
    assert!(!wrong.abort().unwrap());
    assert!(
        backend
            .insert_in_transaction(
                &tx,
                "files",
                &cells("alice"),
                &json!({"attribution": bob.as_bytes()})
            )
            .is_err()
    );
    stage(&backend, 2, None, Some(alice.as_bytes()))
        .attach(&tx)
        .unwrap();
    backend.rollback_transaction(&tx).unwrap();
    assert!(rows(&backend).is_empty());
}

/// A backend that becomes a replica cannot upload an attributed batch using a
/// wire protocol that has no separate provenance field. It publishes nothing.
#[test]
fn attributed_batch_cannot_escape_through_replica_sync() {
    let backend = open(5, true, None);
    let bob = author("bob").canonical().to_owned();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", None, Some(bob.as_bytes()))
        .unwrap();
    stage(&backend, 1, None, Some(bob.as_bytes()))
        .attach(&tx)
        .unwrap();
    let _upstream = backend.connect_upstream();
    assert!(
        backend
            .commit_transaction(&tx, Some("serializable"))
            .is_err()
    );
    assert!(rows(&backend).is_empty());
    let tx = OpenTransactionId::new().to_string();
    assert!(
        backend
            .begin_transaction(&tx, "serializable", None, Some(bob.as_bytes()))
            .is_err()
    );
    assert!(
        backend
            .begin_transaction(&tx, "serializable", Some(bob.as_bytes()), None)
            .is_err()
    );
}

/// Alice receives the backend's Bob-attributed row through ordinary sync;
/// database-local author codes never become another peer's identities.
#[test]
fn attributed_provenance_survives_authority_to_replica_sync() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    let backend = open(6, true, None);
    let alice = open(7, false, None);
    let bob = author("bob").canonical().to_owned();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", None, Some(bob.as_bytes()))
        .unwrap();
    stage(&backend, 1, None, Some(bob.as_bytes()))
        .attach(&tx)
        .unwrap();
    backend
        .commit_transaction(&tx, Some("serializable"))
        .unwrap();
    let up = alice.connect_upstream();
    let down = backend.accept_subscriber(alice.author(), BTreeMap::new());
    let query =
        Query::from("files").select(["$createdBy.identity.subject", "$updatedBy.identity.subject"]);
    let read = alice
        .all(
            postcard::to_allocvec(&query).unwrap(),
            &json!({"tier": "remote"}),
            None,
            None,
            None,
        )
        .unwrap();
    let mut read = pin!(read);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..50 {
        if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
            let result = jazz::binding_codec::decode_rows(&result.unwrap()).unwrap();
            let rows: Vec<_> = result.iter().flat_map(|b| &b.rows).collect();
            assert_eq!(rows.len(), 1);
            for column in ["$createdBy.identity.subject", "$updatedBy.identity.subject"] {
                assert_eq!(
                    rows[0].cells[column],
                    Value::Nullable(Some(Box::new(Value::String("bob".into()))))
                );
            }
            return;
        }
        down.receive(up.take_outbound());
        up.receive(down.take_outbound());
        block_on(backend.tick()).unwrap();
        block_on(alice.tick()).unwrap();
    }
    panic!("Alice's remote read did not complete");
}
