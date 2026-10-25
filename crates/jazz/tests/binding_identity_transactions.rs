//! E12 host bindings preserve the distinction between authenticated admission
//! and backend provenance, including staged uploads and transaction rollback.
mod common;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::binding_host::{Binding, HostError, Link, StagedStreamingMutation};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query, col, eq, lit};
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
    open_with_policy(node, backend, path, policy)
}

fn file_table(policy: PolicyExpr) -> TableSchemaBuilder {
    TableSchemaBuilder::new("files")
        .column("owner", ColumnType::Text)
        .nullable_column("payload", ColumnType::Bytea)
        .nullable_column("parent", ColumnType::Uuid)
        .policies(
            TablePolicies::new()
                .with_select(policy.clone())
                .with_insert(policy.clone())
                .with_update(Some(policy.clone()), policy)
                .with_delete(PolicyExpr::True),
        )
}

fn open_with_policy(node: u8, backend: bool, path: Option<&Path>, policy: PolicyExpr) -> Binding {
    let schema = common::compile_schema(&SchemaBuilder::new().table(file_table(policy)).build());
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
    block_on(
        db.db
            .all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)),
    )
    .unwrap()
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

fn drive<T>(up: &Link, down: &Link, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        down.receive(up.take_outbound());
        up.receive(down.take_outbound());
        block_on(down.binding().tick()).unwrap();
        block_on(up.binding().tick()).unwrap();
    }
    panic!("replica operation did not settle");
}

fn companion(backend: &Binding, tx: &str, id: u8) {
    backend
        .insert_in_transaction(
            tx,
            "files",
            &cells("alice"),
            &json!({"rowId": vec![id; 16]}),
        )
        .unwrap();
}

/// A backend may stage Bob's attributed or session batch, but an Alice session
/// connection cannot vouch for Bob. The authority rejects the whole upload and
/// companion transaction, and the replica retracts both optimistic rows.
/// backend --Bob batch--> Alice link --reject--> authority
#[test]
fn attributed_batch_cannot_escape_through_replica_sync() {
    for session in [false, true] {
        let backend = open(5, true, None);
        let authority = open(8, true, None);
        let bob = author("bob").canonical().to_owned();
        let tx = OpenTransactionId::new().to_string();
        let session_identity = session.then_some(bob.as_bytes());
        let attribution = (!session).then_some(bob.as_bytes());
        // Preserve the role-change case: open before becoming a replica.
        backend
            .begin_transaction(&tx, "serializable", session_identity, attribution)
            .unwrap();
        let up = backend.connect_upstream();
        let down = authority.accept_subscriber(author("alice"), BTreeMap::new());
        stage(&backend, 1, session_identity, attribution)
            .attach(&tx)
            .unwrap();
        companion(&backend, &tx, 2);
        let write = backend
            .commit_transaction(&tx, Some("serializable"))
            .unwrap();
        assert_eq!(rows(&backend).len(), 2);
        let result = drive(
            &up,
            &down,
            backend.wait_for_write(&write, "global").unwrap(),
        );
        let error = core_error(result.unwrap_err());
        assert_eq!(error.code, ErrorCode::WriteRejected);
        assert!(error.message.contains("trusted serving node"), "{error:?}");
        assert!(rows(&authority).is_empty());
        assert!(rows(&backend).is_empty());
    }
}

/// Bob's attribution and Alice's ciphertext survive a pending replica restart,
/// then settle atomically through a trusted backend link. Provenance does not
/// grant Bob read access to Alice-owned rows.
/// backend --persist pending--> reopen --SYSTEM link--> authority
#[test]
fn attributed_replica_upload_survives_restart_and_settles_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let backend = open(9, true, Some(&path));
    let up = backend.connect_upstream();
    let bob = author("bob").canonical().to_owned();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "serializable", None, Some(bob.as_bytes()))
        .unwrap();
    stage(&backend, 1, None, Some(bob.as_bytes()))
        .attach(&tx)
        .unwrap();
    companion(&backend, &tx, 2);
    let write = backend
        .commit_transaction(&tx, Some("serializable"))
        .unwrap();
    assert_eq!(rows(&backend).len(), 2);
    drop(up);
    drop(backend);

    let backend = open(9, true, Some(&path));
    let authority = open(10, true, None);
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    drive(
        &up,
        &down,
        backend.wait_for_write(&write, "global").unwrap(),
    )
    .unwrap();
    for db in [&backend, &authority] {
        let committed = rows(db);
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].seq(), committed[1].seq());
        for row in committed {
            assert_eq!(subject(&row, "$createdBy.identity.subject"), "bob");
            assert_eq!(subject(&row, "$updatedBy.identity.subject"), "bob");
        }
        assert_eq!(
            block_on(db.db.read_value_range(
                "files",
                RowUuid::from_bytes([1; 16]),
                "payload",
                0..200_000
            ))
            .unwrap(),
            bytes()
        );
    }
    let query = authority.db.prepare_query(&Query::from("files")).unwrap();
    assert!(
        block_on(
            authority
                .db
                .all_for_identity(&query, ReadOpts::default(), author("bob"))
        )
        .unwrap()
        .is_empty()
    );
}

/// A trusted backend replica may act for Alice, but still obeys her session's
/// policies at the authority. Mallory's upload and companion are both refused;
/// a trusted connection does not turn session writes into SYSTEM writes.
#[test]
fn trusted_replica_session_batches_enforce_authority_policy_atomically() {
    for name in ["alice", "mallory"] {
        let backend = open(11, true, None);
        let authority = open(12, true, None);
        let up = backend.connect_upstream();
        let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
        let session = author(name).canonical().to_owned();
        let tx = OpenTransactionId::new().to_string();
        backend
            .begin_transaction(&tx, "serializable", Some(session.as_bytes()), None)
            .unwrap();
        stage(&backend, 1, Some(session.as_bytes()), None)
            .attach(&tx)
            .unwrap();
        companion(&backend, &tx, 2);
        let write = backend
            .commit_transaction(&tx, Some("serializable"))
            .unwrap();
        assert_eq!(rows(&backend).len(), 2);
        let result = drive(
            &up,
            &down,
            backend.wait_for_write(&write, "global").unwrap(),
        );
        if name == "alice" {
            result.unwrap();
            let committed = rows(&authority);
            assert_eq!(committed.len(), 2);
            assert_eq!(committed[0].seq(), committed[1].seq());
            for row in committed {
                assert_eq!(subject(&row, "$createdBy.identity.subject"), "alice");
            }
            assert_eq!(
                block_on(authority.db.read_value_range(
                    "files",
                    RowUuid::from_bytes([1; 16]),
                    "payload",
                    0..200_000
                ))
                .unwrap(),
                bytes()
            );
        } else {
            assert_eq!(
                core_error(result.unwrap_err()).code,
                ErrorCode::WriteRejected
            );
            assert!(rows(&authority).is_empty());
            assert!(rows(&backend).is_empty());
        }
    }
}

/// Alice receives the backend's Bob-attributed row through ordinary sync;
/// database-local author codes never become another peer's identities.
#[test]
fn attributed_provenance_survives_authority_to_replica_sync() {
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

/// Alice's session reads and validates the same permitted rows on both the
/// backend replica and authority, even when Bob's rows are already cached.
#[test]
fn replica_session_read_sets_validate_as_the_bound_session() {
    let authority = open(13, true, None);
    let backend = open(14, true, None);
    for (id, owner) in [(1, "alice"), (2, "bob")] {
        authority
            .insert("files", &cells(owner), &json!({"rowId": vec![id; 16]}))
            .unwrap();
    }
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let session = author("alice").canonical().to_owned();
    let tx = OpenTransactionId::new();
    backend
        .begin_transaction(
            &tx.to_string(),
            "serializable",
            Some(session.as_bytes()),
            None,
        )
        .unwrap();
    let query = backend.db.prepare_query(&Query::from("files")).unwrap();
    let transaction = backend.db.serializable_tx_ref(tx);
    let visible = drive(&up, &down, transaction.all_prepared(&query)).unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(subject(&visible[0], "owner"), "alice");
    companion(&backend, &tx.to_string(), 3);
    let write = backend
        .commit_transaction(&tx.to_string(), Some("serializable"))
        .unwrap();
    drive(
        &up,
        &down,
        backend.wait_for_write(&write, "global").unwrap(),
    )
    .unwrap();
    assert_eq!(rows(&authority).len(), 3);
}

/// Alice's session cannot read Bob's nested child through an Alice-owned root.
/// The same restriction applies on the authority and a trusted backend replica.
#[test]
fn session_transaction_relations_keep_the_bound_reader() {
    for replica in [false, true] {
        let authority = open(15, true, None);
        authority.db.set_authority();
        authority
            .insert("files", &cells("alice"), &json!({"rowId": vec![1; 16]}))
            .unwrap();
        for (id, owner) in [(2, "alice"), (3, "bob")] {
            let descriptor =
                RecordDescriptor::new([("owner", ValueType::String), ("parent", ValueType::Uuid)]);
            let row = descriptor
                .create(&[
                    Value::String(owner.into()),
                    Value::Uuid(RowUuid::from_bytes([1; 16]).0),
                ])
                .unwrap();
            let cells = jazz::binding_codec::encode_named_cells(&OwnedRecord::new(row, descriptor))
                .unwrap();
            authority
                .insert("files", &cells, &json!({"rowId": vec![id; 16]}))
                .unwrap();
        }
        let backend = open(16, true, None);
        let links = replica.then(|| {
            (
                backend.connect_upstream(),
                authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new()),
            )
        });
        let db = if replica { &backend } else { &authority };
        let session = author("alice").canonical().to_owned();
        let tx = OpenTransactionId::new();
        db.begin_transaction(
            &tx.to_string(),
            "serializable",
            Some(session.as_bytes()),
            None,
        )
        .unwrap();
        let query = db
            .db
            .prepare_query(
                &Query::from("files")
                    .filter(eq(
                        col("id"),
                        lit(Value::Uuid(RowUuid::from_bytes([1; 16]).0)),
                    ))
                    .array_subquery(ArraySubquery::new("children", "files", "parent", "id")),
            )
            .unwrap();
        let transaction = db.db.serializable_tx_ref(tx);
        let future = transaction.all_prepared(&query);
        let visible = match &links {
            Some((up, down)) => drive(up, down, future),
            None => block_on(future),
        }
        .unwrap();
        assert_eq!(visible.len(), 1);
        let Value::Array(children) = visible[0]
            .cells()
            .find_map(|(name, value)| (name == "children").then_some(value).flatten())
            .unwrap()
        else {
            panic!("children must be an array")
        };
        assert_eq!(children.len(), 1, "Bob's child is private");
        companion(db, &tx.to_string(), 4);
        let write = db
            .commit_transaction(&tx.to_string(), Some("serializable"))
            .unwrap();
        let future = db.wait_for_write(&write, "global").unwrap();
        match &links {
            Some((up, down)) => drive(up, down, future),
            None => block_on(future),
        }
        .unwrap();
    }
}

fn decode_read(bytes: &[u8], relation: bool) -> Vec<jazz::binding_codec::DecodedRow> {
    let batches = if relation {
        jazz::binding_codec::decode_relation_snapshot(bytes)
            .unwrap()
            .1
    } else {
        jazz::binding_codec::decode_rows(bytes).unwrap()
    };
    batches.into_iter().flat_map(|batch| batch.rows).collect()
}

/// A backend serving Alice must filter one-shot root and nested reads as Alice,
/// including rows already cached by a backend replica. A trusted remote request
/// returns only Alice's authority-approved roots.
#[test]
fn one_shot_binding_reads_keep_the_requested_session() {
    for replica in [false, true] {
        let authority = open(20, true, None);
        for (id, owner) in [(1, "alice"), (2, "bob")] {
            authority
                .insert("files", &cells(owner), &json!({"rowId": vec![id; 16]}))
                .unwrap();
        }
        for (id, owner) in [(3, "alice"), (4, "bob")] {
            let descriptor =
                RecordDescriptor::new([("owner", ValueType::String), ("parent", ValueType::Uuid)]);
            let row = descriptor
                .create(&[
                    Value::String(owner.into()),
                    Value::Uuid(RowUuid::from_bytes([1; 16]).0),
                ])
                .unwrap();
            let cells = jazz::binding_codec::encode_named_cells(&OwnedRecord::new(row, descriptor))
                .unwrap();
            authority
                .insert("files", &cells, &json!({"rowId": vec![id; 16]}))
                .unwrap();
        }
        let backend = open(21, true, None);
        let links = replica.then(|| {
            (
                backend.connect_upstream(),
                authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new()),
            )
        });
        let db = if replica { &backend } else { &authority };
        let alice = author("alice").canonical().to_owned();
        let bob = author("bob").canonical().to_owned();
        if let Some((up, down)) = &links {
            let read = db
                .all(
                    postcard::to_allocvec(&Query::from("files")).unwrap(),
                    &json!({"tier": "remote"}),
                    None,
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(decode_read(&drive(up, down, read).unwrap(), false).len(), 4);
            let read = db
                .all(
                    postcard::to_allocvec(&Query::from("files")).unwrap(),
                    &json!({"tier": "remote"}),
                    None,
                    Some(alice.as_bytes()),
                    None,
                )
                .unwrap();
            let visible = decode_read(&drive(up, down, read).unwrap(), false);
            assert_eq!(visible.len(), 2);
            assert!(
                visible
                    .iter()
                    .all(|row| row.cells["owner"] == Value::String("alice".into()))
            );
        }

        for (identity, expected) in [
            (None, 4),
            (Some(alice.as_bytes()), 2),
            (Some(bob.as_bytes()), 2),
            (Some(alice.as_bytes()), 2),
        ] {
            let read = db
                .all(
                    postcard::to_allocvec(&Query::from("files")).unwrap(),
                    &json!({"tier": "local-only"}),
                    None,
                    identity,
                    None,
                )
                .unwrap();
            let bytes = match &links {
                Some((up, down)) => drive(up, down, read),
                None => block_on(read),
            }
            .unwrap();
            assert_eq!(decode_read(&bytes, false).len(), expected);
        }
        let query = Query::from("files")
            .filter(eq(
                col("id"),
                lit(Value::Uuid(RowUuid::from_bytes([1; 16]).0)),
            ))
            .array_subquery(ArraySubquery::new("children", "files", "parent", "id"));
        let read = db
            .all(
                postcard::to_allocvec(&query).unwrap(),
                &json!({"tier": "local-only"}),
                None,
                Some(alice.as_bytes()),
                None,
            )
            .unwrap();
        let bytes = match &links {
            Some((up, down)) => drive(up, down, read),
            None => block_on(read),
        }
        .unwrap();
        let rows = decode_read(&bytes, true);
        assert_eq!(rows.len(), 1);
        let Value::Array(children) = &rows[0].cells["children"] else {
            panic!("children must be an array")
        };
        assert_eq!(children.len(), 1, "Alice must not see Bob's nested child");
    }
}

/// Alice's mergeable batch inherits her session for reads and all writes. A
/// denied companion leaves storage untouched, rollback discards earlier staged
/// writes, and successful rows share one commit.
#[test]
fn mergeable_session_batches_are_atomic_and_policy_checked() {
    let backend = open(22, true, None);
    backend.db.set_authority();
    backend
        .insert("files", &cells("bob"), &json!({"rowId": vec![9; 16]}))
        .unwrap();
    let alice = author("alice").canonical().to_owned();
    for forbidden in [false, true] {
        let tx = OpenTransactionId::new().to_string();
        backend
            .begin_transaction(&tx, "mergeable", Some(alice.as_bytes()), None)
            .unwrap();
        companion(&backend, &tx, if forbidden { 3 } else { 1 });
        let read = block_on(
            backend
                .all(
                    postcard::to_allocvec(&Query::from("files")).unwrap(),
                    &json!({}),
                    Some(&tx),
                    None,
                    None,
                )
                .unwrap(),
        )
        .unwrap();
        let visible = decode_read(&read, false);
        assert!(
            visible
                .iter()
                .all(|row| row.cells["owner"] == Value::String("alice".into()))
        );
        let staged = backend.insert_in_transaction(
            &tx,
            "files",
            &cells(if forbidden { "bob" } else { "alice" }),
            &json!({"rowId": vec![if forbidden { 4 } else { 2 }; 16]}),
        );
        assert_eq!(rows(&backend).len(), if forbidden { 3 } else { 1 });
        if forbidden {
            assert_eq!(
                core_error(staged.unwrap_err()).code,
                ErrorCode::WriteRejected
            );
            backend.rollback_transaction(&tx).unwrap();
        } else {
            staged.unwrap();
            let write = backend.commit_transaction(&tx, Some("mergeable")).unwrap();
            block_on(backend.wait_for_write(&write, "global").unwrap()).unwrap();
        }
        let committed = rows(&backend);
        assert_eq!(committed.len(), 3);
        let written: Vec<_> = committed
            .iter()
            .filter(|row| row.row_uuid() != RowUuid::from_bytes([9; 16]))
            .collect();
        assert_eq!(written[0].seq(), written[1].seq());
        for row in written {
            assert_eq!(subject(row, "$createdBy.identity.subject"), "alice");
        }
    }
}

/// Bob's mergeable backend provenance is inherited and durable, while reads
/// retain SYSTEM authority. A different author or upload cannot enter the batch;
/// upload capabilities still require a serializable transaction.
#[test]
fn attributed_mergeable_batches_keep_backend_authority_and_capability_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mergeable.sqlite");
    let backend = open(23, true, Some(&path));
    backend
        .insert("files", &cells("alice"), &json!({"rowId": vec![9; 16]}))
        .unwrap();
    let bob = author("bob").canonical().to_owned();
    let alice = author("alice").canonical().to_owned();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "mergeable", None, Some(bob.as_bytes()))
        .unwrap();
    let read = block_on(
        backend
            .all(
                postcard::to_allocvec(&Query::from("files")).unwrap(),
                &json!({}),
                Some(&tx),
                None,
                None,
            )
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        decode_read(&read, false).len(),
        1,
        "attribution does not narrow backend reads"
    );
    assert!(
        backend
            .insert_in_transaction(
                &tx,
                "files",
                &cells("alice"),
                &json!({"attribution": alice.as_bytes()})
            )
            .is_err()
    );
    companion(&backend, &tx, 1);
    companion(&backend, &tx, 2);
    backend.commit_transaction(&tx, Some("mergeable")).unwrap();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "mergeable", None, Some(bob.as_bytes()))
        .unwrap();
    let mut upload = stage(&backend, 3, None, Some(bob.as_bytes()));
    assert!(upload.attach(&tx).is_err());
    assert!(
        !upload.abort().unwrap(),
        "failed attachment consumes its capability"
    );
    companion(&backend, &tx, 4);
    backend.rollback_transaction(&tx).unwrap();
    drop(backend);
    let reopened = open(23, true, Some(&path));
    let committed = rows(&reopened);
    assert_eq!(committed.len(), 3);
    let written: Vec<_> = committed
        .iter()
        .filter(|row| row.row_uuid() != RowUuid::from_bytes([9; 16]))
        .collect();
    assert_eq!(written[0].seq(), written[1].seq());
    for row in written {
        assert_eq!(subject(row, "$createdBy.identity.subject"), "bob");
    }
    let ordinary = open(24, false, None);
    assert!(
        ordinary
            .begin_transaction(&tx, "mergeable", None, Some(bob.as_bytes()))
            .is_err()
    );
    assert!(
        block_on(
            ordinary
                .db
                .begin_mergeable_for_attribution(OpenTransactionId::new(), author("bob"))
        )
        .is_err()
    );
    // An incorrectly flagged host still cannot turn a non-SYSTEM core into a backend.
    let id = identity(30, author("alice"));
    let core = block_on(Db::open(DbConfig::new(
        common::compile_schema(&SchemaBuilder::new().build()),
        id,
    )))
    .unwrap();
    let misconfigured = Binding::new(core, id, true);
    assert!(
        misconfigured
            .begin_transaction(&tx, "mergeable", None, Some(bob.as_bytes()))
            .is_err()
    );
}

/// Trusted replica links preserve Bob's mergeable attribution or enforce Alice's
/// session policy. Ordinary Alice links reject Bob's forged attribution/session.
/// backend -> atomic two-row commit -> trusted or Alice link -> authority verdict
#[test]
fn mergeable_replica_identity_batches_enforce_link_and_session_boundaries() {
    for (trusted, session, name, accepted) in [
        (true, false, "bob", true),
        (true, true, "alice", true),
        (true, true, "mallory", false),
        (false, false, "bob", false),
        (false, true, "bob", false),
    ] {
        let backend = open(25, true, None);
        let authority = open(26, true, None);
        let up = backend.connect_upstream();
        let down = authority.accept_subscriber(
            if trusted {
                AuthorSubject::SYSTEM
            } else {
                author("alice")
            },
            BTreeMap::new(),
        );
        let identity = author(name).canonical().to_owned();
        let tx = OpenTransactionId::new().to_string();
        backend
            .begin_transaction(
                &tx,
                "mergeable",
                session.then_some(identity.as_bytes()),
                (!session).then_some(identity.as_bytes()),
            )
            .unwrap();
        companion(&backend, &tx, 1);
        companion(&backend, &tx, 2);
        let write = backend.commit_transaction(&tx, Some("mergeable")).unwrap();
        assert_eq!(rows(&backend).len(), 2);
        let result = drive(
            &up,
            &down,
            backend.wait_for_write(&write, "global").unwrap(),
        );
        if accepted {
            result.unwrap();
            let committed = rows(&authority);
            assert_eq!(committed.len(), 2);
            assert_eq!(committed[0].seq(), committed[1].seq());
            for row in committed {
                assert_eq!(subject(&row, "$createdBy.identity.subject"), name);
            }
        } else {
            let error = core_error(result.unwrap_err());
            assert_eq!(error.code, ErrorCode::WriteRejected);
            if !trusted {
                assert!(error.message.contains("trusted serving node"), "{error:?}");
            }
            assert!(rows(&authority).is_empty());
            assert!(rows(&backend).is_empty());
        }
    }
}

/// Concurrent served reads for Alice use their own claims without overwriting
/// cached identity claims. Omitting an owner claim cannot reuse a previous one.
#[test]
fn one_shot_request_claims_are_isolated_and_do_not_change_cached_claims() {
    let backend = open_with_policy(
        27,
        true,
        None,
        PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]),
    );
    for (id, owner) in [(1, "alice"), (2, "bob")] {
        backend
            .insert("files", &cells(owner), &json!({"rowId": vec![id; 16]}))
            .unwrap();
    }
    let alice = author("alice").canonical().to_owned();
    backend
        .set_identity_claims(alice.as_bytes(), json!({"owner": "bob"}))
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("files")).unwrap();
    let alice_read = backend
        .all(
            query.clone(),
            &json!({}),
            None,
            Some(alice.as_bytes()),
            Some(json!({"owner": "alice"})),
        )
        .unwrap();
    let bob_read = backend
        .all(
            query.clone(),
            &json!({}),
            None,
            Some(alice.as_bytes()),
            Some(json!({"owner": "bob"})),
        )
        .unwrap();
    for (read, owner) in [(bob_read, "bob"), (alice_read, "alice")] {
        let rows = decode_read(&block_on(read).unwrap(), false);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cells["owner"], Value::String(owner.into()));
    }
    let no_claim = block_on(
        backend
            .all(query, &json!({}), None, Some(alice.as_bytes()), None)
            .unwrap(),
    )
    .unwrap();
    assert!(decode_read(&no_claim, false).is_empty());
    let prepared = backend.db.prepare_query(&Query::from("files")).unwrap();
    let cached = block_on(backend.db.all_for_identity(
        &prepared,
        ReadOpts::default(),
        author("alice"),
    ))
    .unwrap();
    assert_eq!(cached.len(), 1);
    assert_eq!(subject(&cached[0], "owner"), "bob");
}

/// Alice's mergeable replica commit persists its admitted claims across restart.
/// A trusted authority checks those claims for this commit and restores its own
/// cached claims afterward, so another request cannot inherit Alice's grant.
#[test]
fn pending_mergeable_session_claims_survive_restart_without_leaking() {
    let policy = || PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pending.sqlite");
    let backend = open_with_policy(28, true, Some(&path), policy());
    let up = backend.connect_upstream();
    let alice = author("alice").canonical().to_owned();
    backend
        .set_identity_claims(alice.as_bytes(), json!({"owner": "alice"}))
        .unwrap();
    let tx = OpenTransactionId::new().to_string();
    backend
        .begin_transaction(&tx, "mergeable", Some(alice.as_bytes()), None)
        .unwrap();
    companion(&backend, &tx, 1);
    companion(&backend, &tx, 2);
    let write = backend.commit_transaction(&tx, Some("mergeable")).unwrap();
    drop(up);
    drop(backend);
    let backend = open_with_policy(28, true, Some(&path), policy());
    let authority = open_with_policy(29, true, None, policy());
    authority
        .set_identity_claims(alice.as_bytes(), json!({"owner": "bob"}))
        .unwrap();
    authority
        .insert("files", &cells("bob"), &json!({"rowId": vec![9; 16]}))
        .unwrap();
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    drive(
        &up,
        &down,
        backend.wait_for_write(&write, "global").unwrap(),
    )
    .unwrap();
    let committed = rows(&authority);
    assert_eq!(committed.len(), 3);
    let written: Vec<_> = committed
        .iter()
        .filter(|row| row.row_uuid() != RowUuid::from_bytes([9; 16]))
        .collect();
    assert_eq!(written.len(), 2);
    assert_eq!(written[0].seq(), written[1].seq());
    for row in written {
        assert_eq!(subject(row, "$createdBy.identity.subject"), "alice");
    }
    let prepared = authority.db.prepare_query(&Query::from("files")).unwrap();
    let cached = block_on(authority.db.all_for_identity(
        &prepared,
        ReadOpts::default(),
        author("alice"),
    ))
    .unwrap();
    assert_eq!(cached.len(), 1);
    assert_eq!(subject(&cached[0], "owner"), "bob");
}

fn remote_as(
    db: &Binding,
    query: Query,
    name: &str,
    claims: Option<serde_json::Value>,
) -> jazz::binding_host::HostFuture<Vec<u8>> {
    let author = author(name).canonical().to_owned();
    db.all(
        postcard::to_allocvec(&query).unwrap(),
        &json!({"tier": "remote"}),
        None,
        Some(author.as_bytes()),
        claims,
    )
    .unwrap()
}

/// Alice and Bob's concurrent backend requests carry separate claims all the
/// way to the authority. Only each request's permitted rows are shipped, and
/// neither request changes another connection's cached claims.
#[test]
fn scoped_remote_reads_isolate_concurrent_claims_and_ship_only_permitted_roots() {
    let policy = || PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]);
    let authority = open_with_policy(31, true, None, policy());
    for (id, owner) in [(1, "alice"), (2, "bob"), (3, "mallory")] {
        authority
            .insert("files", &cells(owner), &json!({"rowId": vec![id; 16]}))
            .unwrap();
    }
    let backend = open_with_policy(32, true, None, policy());
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let query = Query::from("files").filter(eq(
        col("owner"),
        jazz::query::claim(jazz::query::provider_claim_key("owner")),
    ));
    let alice = author("alice").canonical().to_owned();
    authority
        .set_identity_claims(alice.as_bytes(), json!({"owner": "mallory"}))
        .unwrap();
    let visible = decode_read(
        &drive(
            &up,
            &down,
            remote_as(
                &backend,
                query.clone(),
                "alice",
                Some(json!({"owner": "alice"})),
            ),
        )
        .unwrap(),
        false,
    );
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].cells["owner"], Value::String("alice".into()));
    assert_eq!(
        rows(&backend).len(),
        1,
        "no unrelated Bob or Mallory rows are shipped"
    );
    let mut a = remote_as(
        &backend,
        query.clone(),
        "alice",
        Some(json!({"owner": "alice"})),
    );
    let mut b = remote_as(&backend, query, "alice", Some(json!({"owner": "bob"})));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(a.as_mut().poll(&mut cx).is_pending());
    assert!(b.as_mut().poll(&mut cx).is_pending());
    for (future, owner) in [(b, "bob"), (a, "alice")] {
        let visible = decode_read(&drive(&up, &down, future).unwrap(), false);
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].cells["owner"], Value::String(owner.into()));
    }
    assert_eq!(rows(&backend).len(), 2);
    let prepared = authority.db.prepare_query(&Query::from("files")).unwrap();
    let cached = block_on(authority.db.all_for_identity(
        &prepared,
        ReadOpts::default(),
        author("alice"),
    ))
    .unwrap();
    assert_eq!(cached.len(), 1);
    assert_eq!(subject(&cached[0], "owner"), "mallory");
}

/// Ordinary Alice connections cannot forge Bob or fresh claims, even
/// if the local host calls itself a backend. A refused request leaks no rows.
#[test]
fn untrusted_connections_cannot_submit_scoped_reads() {
    for name in ["alice", "bob"] {
        let authority = open_with_policy(
            33,
            true,
            None,
            PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]),
        );
        authority
            .insert("files", &cells("bob"), &json!({"rowId": vec![1; 16]}))
            .unwrap();
        let backend = open(34, true, None);
        let up = backend.connect_upstream();
        let down = authority.accept_subscriber(author("alice"), BTreeMap::new());
        let id = author(name);
        let read = backend
            .all(
                postcard::to_allocvec(&Query::from("files")).unwrap(),
                &json!({"tier": "remote"}),
                None,
                Some(id.canonical().as_bytes()),
                Some(json!({"owner": "bob"})),
            )
            .unwrap();
        let error = core_error(drive(&up, &down, read).unwrap_err());
        assert_eq!(error.code, ErrorCode::WriteRejected);
        assert!(error.message.contains("trusted backend connection"));
        assert!(rows(&backend).is_empty());
    }
}

/// A revoked or local-only Alice row cannot leak from the backend cache into a
/// remote answer. Pagination applies once, at the authority, and an empty
/// authorized answer stays empty even while the cache contains Alice rows.
#[test]
fn scoped_remote_answers_exclude_stale_and_pending_rows_and_preserve_windows() {
    let authority = open(35, true, None);
    for id in 1..=4 {
        authority
            .insert("files", &cells("alice"), &json!({"rowId": vec![id; 16]}))
            .unwrap();
    }
    let backend = open(36, true, None);
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let query = Query::from("files").order_by("id", jazz::query::OrderDirection::Asc);
    assert_eq!(
        decode_read(
            &drive(
                &up,
                &down,
                remote_as(&backend, query.clone(), "alice", None)
            )
            .unwrap(),
            false
        )
        .len(),
        4
    );
    authority
        .update("files", &[1; 16], &cells("bob"), &json!({}))
        .unwrap();
    let visible = decode_read(
        &drive(
            &up,
            &down,
            remote_as(&backend, query.clone().offset(1).limit(1), "alice", None),
        )
        .unwrap(),
        false,
    );
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].row_id, RowUuid::from_bytes([3; 16]));
    let staged = backend
        .insert("files", &cells("alice"), &json!({"rowId": vec![9; 16]}))
        .unwrap();
    // Poll registers the read; feed only read frames, keeping the local write
    // unsent. The wire-level filtering models a delayed upload, not a core hook.
    let mut read = remote_as(&backend, query.clone(), "alice", None);
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    block_on(backend.tick()).unwrap();
    let outbound = up.take_outbound();
    let mut deferred = Vec::new();
    for frame in outbound {
        if matches!(
            postcard::from_bytes::<jazz::db::SyncMessage>(&frame).unwrap(),
            jazz::db::SyncMessage::Commit { .. }
        ) {
            deferred.push(frame);
        } else {
            down.receive([frame]);
        }
    }
    block_on(authority.tick()).unwrap();
    up.receive(down.take_outbound());
    block_on(backend.tick()).unwrap();
    let visible = decode_read(&drive(&up, &down, read).unwrap(), false);
    assert_eq!(visible.len(), 3);
    assert!(
        visible
            .iter()
            .all(|row| row.row_id != RowUuid::from_bytes([9; 16]))
    );
    down.receive(deferred);
    drive(
        &up,
        &down,
        backend.wait_for_write(&staged, "global").unwrap(),
    )
    .unwrap();
    for id in [2, 3, 4, 9] {
        authority
            .update("files", &[id; 16], &cells("bob"), &json!({}))
            .unwrap();
    }
    assert!(
        decode_read(
            &drive(&up, &down, remote_as(&backend, query, "alice", None)).unwrap(),
            false
        )
        .is_empty()
    );
}

/// A peer can bypass the SDK's shape checks, so raw wire requests exercise the
/// authority boundary: Alice's nested/cross-row evidence rejects explicitly;
/// malformed authors reject without closing the serving connection.
#[test]
fn scoped_wire_requests_reject_unported_shapes_and_malformed_authors() {
    for kind in ["nested", "evidence", "malformed", "forged-system"] {
        let policy = if kind == "evidence" {
            PolicyExpr::Exists {
                table: "files".into(),
                condition: Box::new(PolicyExpr::True),
            }
        } else {
            PolicyExpr::eq_session(
                "owner",
                vec!["user".into(), "identity".into(), "subject".into()],
            )
        };
        let authority = open_with_policy(37, true, None, policy);
        let down = authority.accept_subscriber(
            if kind == "forged-system" {
                author("alice")
            } else {
                AuthorSubject::SYSTEM
            },
            BTreeMap::new(),
        );
        let query = if kind == "nested" {
            Query::from("files")
                .array_subquery(ArraySubquery::new("children", "files", "parent", "id"))
        } else {
            Query::from("files")
        };
        let request = jazz::db::SyncMessage::ScopedFetchV1 {
            id: 41,
            query,
            binding: BTreeMap::new(),
            author: if kind == "malformed" {
                "not-an-author".into()
            } else if kind == "forged-system" {
                AuthorSubject::SYSTEM.canonical().to_owned()
            } else {
                author("alice").canonical().to_owned()
            },
            claims: BTreeMap::new(),
            include_deleted: false,
        };
        down.receive([postcard::to_allocvec(&request).unwrap()]);
        block_on(authority.tick()).unwrap();
        let messages: Vec<jazz::db::SyncMessage> = down
            .take_outbound()
            .iter()
            .map(|frame| postcard::from_bytes(frame).unwrap())
            .collect();
        let expected = if kind == "forged-system" {
            ErrorCode::WriteRejected
        } else {
            ErrorCode::Query
        };
        assert!(
            messages.iter().any(|message| matches!(
                message,
                jazz::db::SyncMessage::Refused {
                    id: 41,
                    code,
                    ..
                } if *code == expected
            )),
            "{kind}: {messages:?}"
        );
        assert!(!messages.iter().any(|message| matches!(
            message,
            jazz::db::SyncMessage::Rows { .. } | jazz::db::SyncMessage::Answer { .. }
        )));
    }
}

/// Alice's cancelled request is removed before transport dispatch; reconnecting
/// while a read waits yields a typed error rather than reusing old request claims.
#[test]
fn scoped_reads_cancel_cleanly_and_fail_on_reconnect() {
    let authority = open(38, true, None);
    let backend = open(39, true, None);
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    // Drain the handshake before cancellation.
    for _ in 0..4 {
        down.receive(up.take_outbound());
        up.receive(down.take_outbound());
        block_on(authority.tick()).unwrap();
        block_on(backend.tick()).unwrap();
    }
    for _ in 0..20 {
        let mut read = remote_as(&backend, Query::from("files"), "alice", None);
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(read);
    }
    block_on(backend.tick()).unwrap();
    assert!(!up.take_outbound().iter().any(|frame| matches!(
        postcard::from_bytes::<jazz::db::SyncMessage>(frame).unwrap(),
        jazz::db::SyncMessage::ScopedFetchV1 { .. }
    )));
    let mut read = remote_as(&backend, Query::from("files"), "alice", None);
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let _new_up = backend.connect_upstream();
    let error = core_error(block_on(read).unwrap_err());
    assert_eq!(error.code, ErrorCode::NotObserved);
}

/// Alice can explicitly read her tombstones, but Bob's remain hidden. Losing
/// the authority cannot promote the backend's cached Alice rows to a remote read.
#[test]
fn scoped_remote_reads_preserve_deleted_rows_and_fail_without_authority() {
    let authority = open(40, true, None);
    for (id, owner) in [(1, "alice"), (2, "bob")] {
        authority
            .insert("files", &cells(owner), &json!({"rowId": vec![id; 16]}))
            .unwrap();
        authority.delete("files", &[id; 16], &json!({})).unwrap();
    }
    let backend = open(41, true, None);
    let up = backend.connect_upstream();
    let down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let alice = author("alice");
    let query = Query::from("files").select(["owner"]);
    let read = backend
        .all(
            postcard::to_allocvec(&query).unwrap(),
            &json!({"tier": "remote", "include_deleted": true}),
            None,
            Some(alice.canonical().as_bytes()),
            None,
        )
        .unwrap();
    let visible = decode_read(&drive(&up, &down, read).unwrap(), false);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].row_id, RowUuid::from_bytes([1; 16]));
    assert_eq!(visible[0].cells["owner"], Value::String("alice".into()));
    assert!(
        decode_read(
            &drive(
                &up,
                &down,
                remote_as(&backend, query.clone(), "alice", None)
            )
            .unwrap(),
            false,
        )
        .is_empty()
    );
    backend
        .db
        .set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    let error = core_error(block_on(remote_as(&backend, query, "alice", None)).unwrap_err());
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(error.message.contains("no available authority"));
}

fn pump_network(links: &[(&Link, &Link)]) {
    for (up, down) in links {
        down.receive(up.take_outbound());
        up.receive(down.take_outbound());
        block_on(down.binding().tick()).unwrap();
        block_on(up.binding().tick()).unwrap();
    }
}

fn drive_network<T>(links: &[(&Link, &Link)], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
            return result;
        }
        pump_network(links);
    }
    panic!("relayed operation did not settle");
}

fn scoped_request(id: u64, name: &str) -> Vec<u8> {
    postcard::to_allocvec(&jazz::db::SyncMessage::ScopedFetchV1 {
        id,
        query: Query::from("files"),
        binding: BTreeMap::new(),
        author: author(name).canonical().to_owned(),
        claims: BTreeMap::new(),
        include_deleted: false,
    })
    .unwrap()
}

/// Two backend connections can reuse local request IDs while Alice's different
/// claims traverse two trusted relays independently. Pagination applies once.
/// alice/bob requests -> relay -> relay -> authority -> exact scoped rows
#[test]
fn scoped_reads_cross_multiple_relays_with_isolated_claims_and_windows() {
    let policy = || PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]);
    let authority = open_with_policy(50, true, None, policy());
    for (id, owner) in [(1, "alice"), (2, "alice"), (3, "bob"), (4, "mallory")] {
        authority
            .insert("files", &cells(owner), &json!({"rowId": vec![id;16]}))
            .unwrap();
    }
    let outer = open_with_policy(51, true, None, policy());
    let inner = open_with_policy(52, true, None, policy());
    let alice = open_with_policy(53, true, None, policy());
    let bob = open_with_policy(54, true, None, policy());
    let a = alice.connect_upstream();
    let ad = outer.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let b = bob.connect_upstream();
    let bd = outer.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let ou = outer.connect_upstream();
    let od = inner.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let iu = inner.connect_upstream();
    let id = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let links = [(&a, &ad), (&b, &bd), (&ou, &od), (&iu, &id)];
    let query = Query::from("files").order_by("id", jazz::query::OrderDirection::Asc);
    let mut ar = remote_as(
        &alice,
        query.clone().offset(1).limit(1),
        "alice",
        Some(json!({"owner":"alice"})),
    );
    let mut br = remote_as(&bob, query.clone(), "alice", Some(json!({"owner":"bob"})));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(ar.as_mut().poll(&mut cx).is_pending());
    assert!(br.as_mut().poll(&mut cx).is_pending());
    for (future, expected, owner) in [(br, 3, "bob"), (ar, 2, "alice")] {
        let rows = decode_read(&drive_network(&links, future).unwrap(), false);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, RowUuid::from_bytes([expected; 16]));
        assert_eq!(rows[0].cells["owner"], Value::String(owner.into()));
    }
    assert_eq!(rows(&alice).len(), 1);
    assert_eq!(rows(&bob).len(), 1);
    assert_eq!(rows(&outer).len(), 2);
    assert_eq!(rows(&inner).len(), 2);
    authority
        .update("files", &[2; 16], &cells("bob"), &json!({}))
        .unwrap();
    let rows = decode_read(
        &drive_network(
            &links,
            remote_as(&alice, query, "alice", Some(json!({"owner":"alice"}))),
        )
        .unwrap(),
        false,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, RowUuid::from_bytes([1; 16]));
}

/// An ordinary Alice connection at any hop cannot delegate Bob or replacement
/// claims. Every upstream refusal reaches the original backend without rows.
#[test]
fn scoped_relay_reads_require_trusted_connections_at_every_hop() {
    for untrusted in 0..3 {
        let authority = open(55, true, None);
        authority
            .insert("files", &cells("bob"), &json!({"rowId":vec![1;16]}))
            .unwrap();
        let inner = open(56, true, None);
        let outer = open(57, true, None);
        let backend = open(58, true, None);
        let id = |hop| {
            if hop == untrusted {
                author("alice")
            } else {
                AuthorSubject::SYSTEM
            }
        };
        let a = backend.connect_upstream();
        let ad = outer.accept_subscriber(id(0), BTreeMap::new());
        let b = outer.connect_upstream();
        let bd = inner.accept_subscriber(id(1), BTreeMap::new());
        let c = inner.connect_upstream();
        let cd = authority.accept_subscriber(id(2), BTreeMap::new());
        let error = core_error(
            drive_network(
                &[(&a, &ad), (&b, &bd), (&c, &cd)],
                remote_as(
                    &backend,
                    Query::from("files"),
                    "bob",
                    Some(json!({"owner":"bob"})),
                ),
            )
            .unwrap_err(),
        );
        assert_eq!(error.code, ErrorCode::WriteRejected);
        assert!(error.message.contains("trusted backend connection"));
        assert!(rows(&backend).is_empty());
        assert!(rows(&outer).is_empty());
        assert!(rows(&inner).is_empty());
    }
}

/// Alice receives the authority's original snapshot even if Bob's update reaches
/// the relay before it emits that answer. A delayed optimistic relay write is
/// excluded. Raw frame timing is necessary to put the update ahead of delivery.
#[test]
fn scoped_relay_answers_preserve_authority_cuts_and_exclude_pending_rows() {
    let authority = open(59, true, None);
    authority
        .insert("files", &cells("alice"), &json!({"rowId":vec![1;16]}))
        .unwrap();
    let relay = open(60, true, None);
    let backend = open(61, true, None);
    let up = backend.connect_upstream();
    let down = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let ru = relay.connect_upstream();
    let rd = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let links = [(&up, &down), (&ru, &rd)];
    for _ in 0..4 {
        pump_network(&links);
    }
    relay
        .insert("files", &cells("alice"), &json!({"rowId":vec![9;16]}))
        .unwrap();
    let mut read = remote_as(&backend, Query::from("files"), "alice", None);
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    block_on(backend.tick()).unwrap();
    down.receive(up.take_outbound());
    block_on(relay.tick()).unwrap();
    // Delay the relay's upload while letting the read overtake it.
    rd.receive(ru.take_outbound().into_iter().filter(|frame| {
        !matches!(
            postcard::from_bytes::<jazz::db::SyncMessage>(frame).unwrap(),
            jazz::db::SyncMessage::Commit { .. }
        )
    }));
    block_on(authority.tick()).unwrap();
    authority
        .update("files", &[1; 16], &cells("bob"), &json!({}))
        .unwrap();
    block_on(authority.tick()).unwrap();
    ru.receive(rd.take_outbound());
    block_on(relay.tick()).unwrap();
    let visible = decode_read(&drive_network(&links, read).unwrap(), false);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].row_id, RowUuid::from_bytes([1; 16]));
    assert_eq!(visible[0].cells["owner"], Value::String("alice".into()));
    assert!(
        decode_read(
            &drive_network(
                &links,
                remote_as(&backend, Query::from("files"), "alice", None)
            )
            .unwrap(),
            false
        )
        .is_empty()
    );
    authority.delete("files", &[1; 16], &json!({})).unwrap();
    let bob = author("bob");
    let read = backend
        .all(
            postcard::to_allocvec(&Query::from("files")).unwrap(),
            &json!({"tier":"remote","include_deleted":true}),
            None,
            Some(bob.canonical().as_bytes()),
            None,
        )
        .unwrap();
    let deleted = decode_read(&drive_network(&links, read).unwrap(), false);
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].cells["owner"], Value::String("bob".into()));
}

/// A disconnected Alice downstream cannot deliver a late answer into Bob's
/// replacement slot, even when Bob reuses its request ID. Raw requests let the
/// test disconnect precisely after forwarding but before the authority answers.
#[test]
fn scoped_relay_disconnect_drops_late_answers_before_session_reuse() {
    let authority = open(62, true, None);
    for (id, owner) in [(1, "alice"), (2, "bob")] {
        authority
            .insert("files", &cells(owner), &json!({"rowId":vec![id;16]}))
            .unwrap();
    }
    let relay = open(63, true, None);
    let ru = relay.connect_upstream();
    let rd = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let mut old = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    old.receive([scoped_request(41, "alice")]);
    block_on(relay.tick()).unwrap();
    let late = ru.take_outbound();
    assert!(old.close());
    let new = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    rd.receive(late);
    block_on(authority.tick()).unwrap();
    ru.receive(rd.take_outbound());
    block_on(relay.tick()).unwrap();
    block_on(relay.tick()).unwrap();
    assert!(new.take_outbound().is_empty());
    new.receive([scoped_request(41, "bob")]);
    let mut messages = Vec::new();
    for _ in 0..20 {
        pump_network(&[(&ru, &rd)]);
        messages.extend(
            new.take_outbound()
                .into_iter()
                .map(|f| postcard::from_bytes::<jazz::db::SyncMessage>(&f).unwrap()),
        );
        if messages
            .iter()
            .any(|m| matches!(m, jazz::db::SyncMessage::Answer { id: 41, .. }))
        {
            break;
        }
    }
    assert!(messages.iter().any(
        |m| matches!(m,jazz::db::SyncMessage::Answer {id:41,rows,..} if rows == &vec![[2;16]])
    ));
    for message in messages {
        if let jazz::db::SyncMessage::Rows { images } = message {
            assert!(images.iter().all(|image| image.id == [2; 16]));
        }
    }
}

/// Alice's pending read fails when the relay loses or replaces its upstream.
/// The old request is never replayed; after a reconnect, a fresh read succeeds.
#[test]
fn scoped_relay_reads_fail_on_upstream_loss_and_recover_with_fresh_requests() {
    for reconnect in [false, true] {
        let authority = open(64, true, None);
        authority
            .insert("files", &cells("alice"), &json!({"rowId":vec![1;16]}))
            .unwrap();
        let relay = open(65, true, None);
        let backend = open(66, true, None);
        let up = backend.connect_upstream();
        let down = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
        let mut ru = relay.connect_upstream();
        let rd = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
        let mut read = remote_as(&backend, Query::from("files"), "alice", None);
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        block_on(backend.tick()).unwrap();
        down.receive(up.take_outbound());
        block_on(relay.tick()).unwrap();
        // Hold the old outbound request, so no authority has answered it.
        let _old_frames = ru.take_outbound();
        if reconnect {
            ru = relay.connect_upstream();
        } else {
            assert!(ru.close());
        }
        let error = core_error(drive(&up, &down, read).unwrap_err());
        assert_eq!(error.code, ErrorCode::NotObserved);
        block_on(relay.tick()).unwrap();
        let frames = ru.take_outbound();
        assert!(!frames.iter().any(|frame| matches!(
            postcard::from_bytes::<jazz::db::SyncMessage>(frame).unwrap(),
            jazz::db::SyncMessage::ScopedFetchV1 { .. }
        )));
        if !reconnect {
            ru = relay.connect_upstream();
        } else {
            rd.receive(frames);
        }
        let visible = decode_read(
            &drive_network(
                &[(&up, &down), (&ru, &rd)],
                remote_as(&backend, Query::from("files"), "alice", None),
            )
            .unwrap(),
            false,
        );
        assert_eq!(visible.len(), 1);
    }
}

/// Alice's request cannot be reinterpreted if the downstream changes its schema
/// while an authority answer is pending. A raw handshake models that peer race.
#[test]
fn scoped_relay_reads_reject_schema_changes_while_pending() {
    let relay = open(67, true, None);
    let _up = relay.connect_upstream();
    let down = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    down.receive([scoped_request(41, "alice")]);
    block_on(relay.tick()).unwrap();
    down.receive([postcard::to_allocvec(&jazz::db::SyncMessage::Schema {
        version: [0x99; 16],
    })
    .unwrap()]);
    block_on(relay.tick()).unwrap();
    let messages: Vec<jazz::db::SyncMessage> = down
        .take_outbound()
        .into_iter()
        .map(|f| postcard::from_bytes(&f).unwrap())
        .collect();
    assert!(messages.iter().any(|m| matches!(
        m,
        jazz::db::SyncMessage::Refused {
            id: 41,
            code: ErrorCode::NotObserved,
            ..
        }
    )));
    assert!(!messages.iter().any(|m| matches!(
        m,
        jazz::db::SyncMessage::Rows { .. } | jazz::db::SyncMessage::Answer { .. }
    )));
}

/// An admitted old Alice schema cannot be forwarded under the relay's new
/// schema. Both versions remain valid locally; delegation rejects before fetch.
#[test]
fn scoped_relay_reads_reject_cross_version_forwarding() {
    let relay = open(68, true, None);
    let backend = open(69, true, None);
    let old_schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(file_table(PolicyExpr::eq_session(
                "owner",
                vec!["user".into(), "identity".into(), "subject".into()],
            )))
            .build(),
    );
    let new_schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(file_table(PolicyExpr::eq_session(
                "owner",
                vec!["user".into(), "identity".into(), "subject".into()],
            )))
            .table(TableSchemaBuilder::new("extra").policies(common::allow_all_policies()))
            .build(),
    );
    assert_ne!(old_schema.version_id(), new_schema.version_id());
    block_on(relay.db.activate_schema(new_schema)).unwrap();
    let _old_view = block_on(relay.db.register_schema_view(old_schema)).unwrap();
    let ru = relay.connect_upstream();
    let up = backend.connect_upstream();
    let down = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    let error = core_error(
        drive(
            &up,
            &down,
            remote_as(&backend, Query::from("files"), "alice", None),
        )
        .unwrap_err(),
    );
    assert_eq!(error.code, ErrorCode::Query);
    assert!(error.message.contains("different schema versions"));
    assert!(!ru.take_outbound().iter().any(|frame| matches!(
        postcard::from_bytes::<jazz::db::SyncMessage>(frame).unwrap(),
        jazz::db::SyncMessage::ScopedFetchV1 { .. }
    )));
}

/// Alice's new-schema request cannot use a relay connection opened under the old
/// schema. Reconnecting sends the new declaration before forwarding the request.
#[test]
fn scoped_relay_schema_activation_requires_a_new_upstream_handshake() {
    let relay = open(70, true, None);
    let old_up = relay.connect_upstream();
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(file_table(PolicyExpr::True))
            .table(TableSchemaBuilder::new("extra").policies(common::allow_all_policies()))
            .build(),
    );
    block_on(relay.db.activate_schema(schema.clone())).unwrap();
    let down = relay.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
    down.receive([scoped_request(41, "alice")]);
    block_on(relay.tick()).unwrap();
    let messages: Vec<jazz::db::SyncMessage> = down
        .take_outbound()
        .into_iter()
        .map(|f| postcard::from_bytes(&f).unwrap())
        .collect();
    assert!(messages.iter().any(|m|matches!(m,jazz::db::SyncMessage::Refused {id:41,code:ErrorCode::Query,message} if message.contains("without upstream reconnect"))));
    assert!(!old_up.take_outbound().iter().any(|f| matches!(
        postcard::from_bytes::<jazz::db::SyncMessage>(f).unwrap(),
        jazz::db::SyncMessage::ScopedFetchV1 { .. }
    )));
    let new_up = relay.connect_upstream();
    down.receive([scoped_request(42, "alice")]);
    block_on(relay.tick()).unwrap();
    let messages: Vec<jazz::db::SyncMessage> = new_up
        .take_outbound()
        .into_iter()
        .map(|f| postcard::from_bytes(&f).unwrap())
        .collect();
    assert!(
        matches!(&messages[0],jazz::db::SyncMessage::Schema {version} if version == schema.version_id().as_bytes())
    );
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, jazz::db::SyncMessage::ScopedFetchV1 { .. }))
    );
    assert!(
        down.take_outbound().is_empty(),
        "the relay still waits for the authority"
    );
}

/// Alice's native host supplies an already-verified session identity. Bob's
/// provenance alone cannot grant a normal runtime backend admission.
#[test]
fn typed_native_transaction_admission_retains_backend_and_session_checks() {
    use jazz::binding_host::TransactionKind;
    let alice = open(0xa8, false, None);
    let id = OpenTransactionId::new();
    let refused = alice.begin_transaction_with_identity(
        id,
        TransactionKind::Mergeable,
        WriteIdentity::Attribution(author("bob")),
    );
    assert!(matches!(refused, Err(HostError::Argument(message)) if message.contains("backend")));
    assert!(rows(&alice).is_empty());

    let backend = open(0xa9, true, None);
    backend
        .begin_transaction_with_identity(
            id,
            TransactionKind::Serializable,
            WriteIdentity::Session(author("alice")),
        )
        .unwrap();
    let staged = block_on(backend.db.mergeable_tx_ref(id).insert(
        "files",
        BTreeMap::from([("owner".into(), Value::String("alice".into()))]),
        Default::default(),
    ));
    backend.staged(id, staged).unwrap();
    let write = backend
        .commit_transaction(&id.to_string(), Some("serializable"))
        .unwrap();
    assert!(write.rejected.is_none());
    let committed = rows(&backend);
    assert_eq!(committed.len(), 1);
    assert_eq!(
        subject(&committed[0], "$createdBy.identity.subject"),
        "alice"
    );
}

/// Alice stages a valid insert then an incomplete upsert through native typed
/// lowerings. Catching the stage refusal cannot commit the earlier insert.
#[test]
fn typed_native_stage_refusal_rejects_the_entire_batch() {
    use jazz::binding_host::TransactionKind;
    let alice = open(0xaa, true, None);
    let id = OpenTransactionId::new();
    alice
        .begin_transaction_with_identity(
            id,
            TransactionKind::Mergeable,
            WriteIdentity::Session(author("alice")),
        )
        .unwrap();
    alice
        .staged(
            id,
            block_on(alice.db.mergeable_tx_ref(id).insert(
                "files",
                BTreeMap::from([("owner".into(), Value::String("alice".into()))]),
                Default::default(),
            )),
        )
        .unwrap();
    let missing = RowUuid::from_bytes([0xef; 16]);
    let staged = block_on(alice.db.mergeable_tx_ref(id).upsert(
        "files",
        missing,
        BTreeMap::new(),
        Default::default(),
    ));
    alice.staged(id, staged).unwrap();
    let write = alice.commit_transaction(&id.to_string(), None).unwrap();
    assert!(block_on(alice.wait_for_write(&write, "local").unwrap()).is_err());
    assert!(rows(&alice).is_empty());
}

/// Alice catches a refused streamed attachment after staging another insert.
/// The binding rejects the complete unit and rollback retires the refusal.
#[test]
fn streamed_stage_refusal_rejects_companion_writes_and_retires_on_rollback() {
    let alice = open(0xac, true, None);
    alice
        .insert("files", &cells("alice"), &json!({ "rowId": vec![1; 16] }))
        .unwrap();
    for rollback in [false, true] {
        let mut duplicate = stage(&alice, 1, None, None);
        let id = OpenTransactionId::new().to_string();
        alice
            .begin_transaction(&id, "serializable", None, None)
            .unwrap();
        alice
            .insert_in_transaction(
                &id,
                "files",
                &cells("alice"),
                &json!({ "rowId": vec![2; 16] }),
            )
            .unwrap();
        let attachment = duplicate.attach(&id);
        if rollback {
            alice.rollback_transaction(&id).unwrap();
        } else {
            let write = alice.commit_transaction(&id, Some("serializable")).unwrap();
            assert!(
                write.rejected.is_some(),
                "a caught attachment refusal cannot publish its companion"
            );
            assert!(block_on(alice.wait_for_write(&write, "local").unwrap()).is_err());
        }
        assert!(
            attachment.is_ok(),
            "the refusal belongs to the transaction receipt: {attachment:?}"
        );
        assert!(
            !duplicate.abort().unwrap(),
            "attachment consumes the capability"
        );
        assert_eq!(rows(&alice).len(), 1);
    }
    let id = OpenTransactionId::new().to_string();
    alice
        .begin_transaction(&id, "serializable", None, None)
        .unwrap();
    alice
        .insert_in_transaction(
            &id,
            "files",
            &cells("alice"),
            &json!({ "rowId": vec![2; 16] }),
        )
        .unwrap();
    let write = alice.commit_transaction(&id, Some("serializable")).unwrap();
    assert!(write.rejected.is_none());
    assert_eq!(rows(&alice).len(), 2);
}

/// Alice opens a native session batch. Bob's explicit read scope must fail
/// before reading that batch; Alice can still read it after the failed attempt.
#[test]
fn typed_native_read_cannot_replace_the_transaction_session() {
    use jazz::binding_host::TransactionKind;
    let backend = open(0xab, true, None);
    let id = OpenTransactionId::new();
    backend
        .begin_transaction_with_identity(
            id,
            TransactionKind::Mergeable,
            WriteIdentity::Session(author("alice")),
        )
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("files")).unwrap();
    let read = |who| {
        block_on(backend.db.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            Some(id),
            Some((who, BTreeMap::new())),
            Some(who),
            false,
            || false,
            |_| {},
        ))
    };
    let rejected = read(author("bob")).unwrap_err();
    assert_eq!(rejected.code, ErrorCode::WriteRejected);
    assert!(rejected.message.contains("bound identity"));
    assert!(read(author("alice")).is_ok());
    backend.rollback_transaction(&id.to_string()).unwrap();
}

/// Ordinary Alice runtimes cannot turn local claims into authority credentials,
/// including persisted explicit session batches. Backend and admitted private
/// owners may vouch for a session; ordinary links use server-admitted claims.
/// Alice -> [optional restart] -> authenticated link -> authority receipt
#[test]
fn upstream_session_claims_require_host_admission_even_after_restart() {
    for kind in ["mergeable", "serializable"] {
        for restart in [false, true] {
            for route in [
                "ordinary-backend",
                "ordinary-session",
                "backend",
                "private-owner",
            ] {
                let policy =
                    || PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]);
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("claims.sqlite");
                let open_client = || {
                    let binding = open_with_policy(80, route == "backend", Some(&path), policy());
                    if route == "private-owner" {
                        binding.db.set_scope_isolated_storage_owner().unwrap();
                    }
                    binding
                };
                let client = open_client();
                let up = client.connect_upstream();
                let alice = author("alice").canonical().to_owned();
                client
                    .set_identity_claims(alice.as_bytes(), json!({"owner":"alice"}))
                    .unwrap();
                let tx = OpenTransactionId::new().to_string();
                client
                    .begin_transaction(&tx, kind, Some(alice.as_bytes()), None)
                    .unwrap();
                companion(&client, &tx, 1);
                let write = client.commit_transaction(&tx, Some(kind)).unwrap();
                let (client, up) = if restart {
                    drop(up);
                    drop(client);
                    let client = open_client();
                    let up = client.connect_upstream();
                    (client, up)
                } else {
                    (client, up)
                };
                let authority = open_with_policy(81, true, None, policy());
                authority.db.set_authority();
                let (peer, claims) = if route == "ordinary-session" {
                    (
                        author("alice"),
                        jazz::binding_host::claims(author("alice"), json!({"owner": "alice"}))
                            .unwrap(),
                    )
                } else {
                    (AuthorSubject::SYSTEM, BTreeMap::new())
                };
                let down = authority.accept_subscriber(peer, claims);
                let result = drive(&up, &down, client.wait_for_write(&write, "global").unwrap());
                let allowed = route != "ordinary-backend";
                assert_eq!(
                    result.is_ok(),
                    allowed,
                    "{route}, {kind}, restart={restart}: {result:?}"
                );
                assert_eq!(
                    rows(&authority).len(),
                    usize::from(allowed),
                    "{route}, {kind}, restart={restart}"
                );
                if !allowed {
                    assert_eq!(
                        core_error(result.unwrap_err()).code,
                        ErrorCode::WriteRejected
                    );
                    assert!(
                        rows(&client).is_empty(),
                        "authority rejection retracts the optimistic row"
                    );
                }
            }
        }
    }
}

/// Alice's foreground reserves and publishes through her admitted durable
/// owner. The reservation envelope cannot vouch for forged claims; the owner
/// supplies its authenticated session and the authority decides the fate.
/// Frame inspection pins this binding security boundary, then real receipts
/// prove that removing claims neither bypasses checks nor blocks valid writes.
#[test]
fn foreground_initialization_envelopes_keep_claim_admission_and_real_receipts() {
    for requested_owner in ["alice", "mallory"] {
        let policy = || PolicyExpr::eq_session("owner", vec!["claims".into(), "owner".into()]);
        let authority = open_with_policy(85, true, None, policy());
        authority.db.set_authority();
        let owner = open_with_policy(86, false, None, policy());
        owner.db.set_scope_isolated_storage_owner().unwrap();
        let owner_up = owner.connect_upstream();
        let authority_down = authority.accept_subscriber(AuthorSubject::SYSTEM, BTreeMap::new());
        let foreground = open_with_policy(87, false, None, policy());
        foreground.db.set_non_durable_client();
        let foreground_up = foreground.connect_upstream();
        let owner_down = owner.accept_subscriber(
            author("alice"),
            jazz::binding_host::claims(author("alice"), json!({"owner":"alice"})).unwrap(),
        );
        let links = [(&foreground_up, &owner_down), (&owner_up, &authority_down)];
        drive_network(&links, foreground.db.wait_for_storage_owner()).unwrap();
        let alice = author("alice").canonical().to_owned();
        foreground
            .set_identity_claims(alice.as_bytes(), json!({"owner": requested_owner}))
            .unwrap();
        let tx = OpenTransactionId::new().to_string();
        foreground
            .begin_transaction(&tx, "serializable", Some(alice.as_bytes()), None)
            .unwrap();
        foreground
            .record_initialization_insert_absence(&tx, "files", &[1; 16])
            .unwrap();
        foreground
            .insert_in_transaction(
                &tx,
                "files",
                &cells(requested_owner),
                &json!({"rowId":vec![1;16]}),
            )
            .unwrap();
        let seal = drive_network(
            &links,
            foreground.seal_initialization_transaction_with_owner(&tx),
        )
        .unwrap();
        let write = foreground
            .publish_initialization_transaction(&seal.token)
            .unwrap();
        block_on(foreground.tick()).unwrap();
        let frames = foreground_up.take_outbound();
        let mut published = false;
        for frame in &frames {
            if let jazz::db::SyncMessage::OwnerInitializationPublishV1 { commit, .. } =
                postcard::from_bytes(frame).unwrap()
            {
                let jazz::db::SyncMessage::Commit { session, .. } =
                    postcard::from_bytes(&commit).unwrap()
                else {
                    panic!("initialization body must be a Commit")
                };
                assert_eq!(
                    session,
                    Some(BTreeMap::new()),
                    "untrusted foreground cannot vouch for claims in a reserved publication"
                );
                published = true;
            }
        }
        assert!(
            published,
            "the real sealed publication crossed the binding link"
        );
        owner_down.receive(frames);
        let result = drive_network(&links, foreground.wait_for_write(&write, "global").unwrap());
        assert_eq!(
            result.is_ok(),
            requested_owner == "alice",
            "{requested_owner}: {result:?}"
        );
        assert_eq!(
            rows(&authority).len(),
            usize::from(requested_owner == "alice")
        );
        if requested_owner != "alice" {
            assert_eq!(
                core_error(result.unwrap_err()).code,
                ErrorCode::WriteRejected
            );
        }
    }
}
