use crate::*;
use jazz::groove::records::{RecordDescriptor, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchema};

fn author(subject: &str, byte: u8) -> CoreAuthorSubject {
    CoreAuthorSubject::authenticated("https://issuer.example", subject)
        .unwrap()
        .with_account(jazz::account_registry::AccountId(
            CoreRowUuid::from_bytes([byte; 16]).0,
        ))
}

fn owner(author: CoreAuthorSubject) -> String {
    format!(
        r#"{{"version":1,"appId":"owner-test","env":"dev","auth":{{"kind":"account","account":"{}","registry":"https://registry.example/apps/owner-test/accounts"}}}}"#,
        author.account_id().unwrap().0
    )
}

fn schema() -> Uint8Array {
    Uint8Array::new(
        serde_json::to_vec(
            &SchemaBuilder::new()
                .table(
                    TableSchema::builder("items")
                        .column("label", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(PolicyExpr::True),
                        ),
                )
                .build(),
        )
        .unwrap(),
    )
}

fn config(author: CoreAuthorSubject, node: u8) -> Uint8Array {
    // The existing bridge config grammar, with a different node on restart.
    Uint8Array::new(
        postcard::to_allocvec(&(
            (CoreNodeUuid::from_bytes([node; 16]), author),
            None::<u64>,
            false,
            None::<u32>,
            None::<String>,
        ))
        .unwrap(),
    )
}

fn open(
    path: &std::path::Path,
    author: CoreAuthorSubject,
    storage_owner: String,
    node: u8,
) -> js::Result<NapiDb> {
    NapiDb::open_persistent_account_owner(
        path.to_string_lossy().into_owned(),
        schema(),
        config(author, node),
        storage_owner,
        None,
    )
}

fn close(db: NapiDb) {
    if let Either::B(pending) = db.close().unwrap() {
        for _ in 0..1024 {
            if pending.poll().unwrap().is_some() {
                return;
            }
            std::thread::yield_now();
        }
        panic!("persistent account owner close did not complete");
    }
}

fn publish(db: &NapiDb) -> String {
    let open = CoreOpenTransactionId::new().to_string();
    db.begin_transaction(open.clone(), "exclusive".into(), None, None)
        .unwrap();
    super::initialization::finish_absence(
        db,
        db.record_initialization_insert_absence(
            open.clone(),
            "items".into(),
            Uint8Array::new(vec![0xb2; 16]),
        )
        .unwrap(),
    )
    .unwrap();
    let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
    let raw = descriptor
        .create(&[CoreValue::String("legacy pending row".into())])
        .unwrap();
    let cells = jazz::binding_codec::encode_named_cells(&jazz::groove::records::OwnedRecord::new(
        raw, descriptor,
    ))
    .unwrap();
    db.insert_in_transaction(
        open.clone(),
        "items".into(),
        Uint8Array::new(cells),
        Some(InsertOptions {
            row_id: Some(Uint8Array::new(vec![0xb2; 16])),
            author: None,
            attribution: None,
            branch: None,
            updated_at_ms: None,
        }),
    )
    .unwrap();
    let seal =
        super::initialization::finish_seal(db, db.seal_initialization_transaction(open).unwrap())
            .unwrap();
    let _write = db.publish_initialization_transaction(seal.token).unwrap();
    for _ in 0..16 {
        db.tick().unwrap();
    }
    seal.reserved_tx_id
}

fn row(db: &NapiDb) -> Vec<u8> {
    db.local_current_row("items".into(), Uint8Array::new(vec![0xb2; 16]))
        .unwrap()
        .to_vec()
}

struct OutboundCapture(Rc<RefCell<Vec<jazz::protocol::SyncMessage>>>);

impl jazz::db::Transport for OutboundCapture {
    fn send(
        &mut self,
        message: jazz::protocol::SyncMessage,
    ) -> std::result::Result<(), jazz::wire::TransportError> {
        self.0.borrow_mut().push(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<jazz::protocol::SyncMessage> {
        None
    }
}

fn take_original_pending_unit(db: &NapiDb) -> Vec<u8> {
    let sent = Rc::new(RefCell::new(Vec::new()));
    let inner = db.inner.borrow();
    let Some(NapiDbInnerStorage::Persistent(core)) = inner.as_ref() else {
        panic!("persistent owner");
    };
    let upstream =
        core_block_on(core.connect_upstream(Box::new(OutboundCapture(Rc::clone(&sent)))));
    for _ in 0..16 {
        core_block_on(core.tick()).unwrap();
    }
    let unit = sent
        .borrow()
        .iter()
        .find_map(|message| match message {
            jazz::protocol::SyncMessage::CommitUnit { versions, .. }
                if versions
                    .iter()
                    .any(|version| version.row_uuid() == CoreRowUuid::from_bytes([0xb2; 16])) =>
            {
                Some(postcard::to_allocvec(message).unwrap())
            }
            _ => None,
        })
        .expect("original pending unit reaches a newly attached upstream");
    core_block_on(core.detach_connection_async(&upstream)).unwrap();
    unit
}

#[test]
fn legacy_root_claim_preserves_rows_and_restores_exact_pending_before_transport() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unchanged-hashed-account-root");
    let author = author("alice", 0xb1);
    let db = NapiDb::open_persistent(
        path.to_string_lossy().into_owned(),
        schema(),
        config(author, 0xb3),
        None,
    )
    .unwrap();
    let reserved = publish(&db);
    let expected_row = row(&db);
    let expected_status = super::initialization::finish_status(
        &db,
        db.initialization_transaction_status(vec![reserved.clone()])
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&expected_status).unwrap()["statuses"][0]["fate"]
            ["kind"],
        "pending"
    );
    let expected_unit = take_original_pending_unit(&db);
    close(db);
    assert!(
        !path.join(".jazz-account-owner").exists(),
        "raw factory must not claim an account root"
    );
    for node in [0xb4, 0xb5] {
        let db = open(&path, author, owner(author), node).unwrap();
        assert_eq!(row(&db), expected_row);
        assert_eq!(
            super::initialization::finish_status(
                &db,
                db.initialization_transaction_status(vec![reserved.clone()])
                    .unwrap()
            )
            .unwrap(),
            expected_status
        );
        assert_eq!(
            take_original_pending_unit(&db),
            expected_unit,
            "owner startup restores the exact old-node unit before transport attachment"
        );
        close(db);
    }
}

#[test]
fn owner_author_and_marker_mismatches_preserve_existing_marker_and_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("account-root");
    let alice = author("alice", 0xb6);
    let db = open(&path, alice, owner(alice), 0xb7).unwrap();
    publish(&db);
    let expected_row = row(&db);
    close(db);
    let marker = path.join(".jazz-account-owner");
    let original = std::fs::read(&marker).unwrap();
    let linked = author("linked", 0xb6);
    assert!(open(&path, linked, owner(linked), 0xb7).is_err());
    assert!(
        open(
            &path,
            alice,
            owner(alice).replace("\"dev\"", "\"other\""),
            0xb7
        )
        .is_err()
    );
    assert!(open(&path, alice, owner(author("bob", 0xb8)), 0xb7).is_err());
    assert_eq!(std::fs::read(&marker).unwrap(), original);
    let mut unknown_version = original.clone();
    unknown_version[b"JAZZ-NODE-ACCOUNT-OWNER\0".len()] = 2;
    let mut trailing = original.clone();
    trailing.push(0);
    for malformed in [
        Vec::new(),
        original[..original.len() - 1].to_vec(),
        unknown_version,
        trailing,
        vec![0xff; 131_105],
    ] {
        std::fs::write(&marker, &malformed).unwrap();
        assert!(open(&path, alice, owner(alice), 0xb7).is_err());
        assert_eq!(std::fs::read(&marker).unwrap(), malformed);
        // Raw opening is deliberately unchanged; inspect the original rows
        // without granting the rejected account-owner route any fallback.
        let raw = NapiDb::open_persistent(
            path.to_string_lossy().into_owned(),
            schema(),
            config(alice, 0xb7),
            None,
        )
        .unwrap();
        assert_eq!(row(&raw), expected_row);
        close(raw);
    }
    std::fs::write(marker, original).unwrap();
}

#[test]
fn self_signed_owner_checks_native_proof_claim_and_account_before_marker() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("proof-root");
    let token = identity::mint_jazz_self_signed_token(
        &[0xb9; 32],
        identity::LOCAL_FIRST_ISSUER,
        "owner-test",
        60,
    )
    .unwrap();
    let proof = identity::verify_jazz_self_signed_proof(&token, "owner-test").unwrap();
    let app = jazz::tools::AppId::from_name("owner-test");
    let account = jazz::account_registry::local_first_account_id(*app.uuid(), &proof.user_id);
    let claim = serde_json::to_string(&(
        account.0.to_string(),
        identity::LOCAL_FIRST_ISSUER,
        proof.user_id,
    ))
    .unwrap();
    let resolved = identity::verify_client_runtime_author(&token, "owner-test", &claim).unwrap();
    let placeholder = author("placeholder", 0xbb);
    let attempt = |token: String, claim: String, storage_owner: String| {
        NapiDb::open_persistent_account_owner_with_self_signed_proof(
            path.to_string_lossy().into_owned(),
            schema(),
            config(placeholder, 0xbc),
            storage_owner,
            token,
            "owner-test".into(),
            claim,
            None,
        )
    };
    assert!(attempt("invalid".into(), claim.clone(), owner(resolved)).is_err());
    assert!(
        attempt(
            token.clone(),
            placeholder.canonical().into(),
            owner(resolved)
        )
        .is_err()
    );
    assert!(attempt(token.clone(), claim.clone(), owner(placeholder)).is_err());
    assert!(!path.join(".jazz-account-owner").exists());
    close(attempt(token.clone(), claim.clone(), owner(resolved)).unwrap());
    close(attempt(token, claim, owner(resolved)).unwrap());
}

#[test]
fn raw_and_backend_factories_do_not_claim_account_roots() {
    let directory = tempfile::tempdir().unwrap();
    let author = author("external-host-admitted", 0xbd);
    let raw_path = directory.path().join("raw");
    close(
        NapiDb::open_persistent(
            raw_path.to_string_lossy().into_owned(),
            schema(),
            config(author, 0xbe),
            None,
        )
        .unwrap(),
    );
    assert!(!raw_path.join(".jazz-account-owner").exists());
    let backend_path = directory.path().join("backend");
    close(
        NapiDb::open_persistent_as_backend(
            backend_path.to_string_lossy().into_owned(),
            schema(),
            config(author, 0xbf),
        )
        .unwrap(),
    );
    assert!(!backend_path.join(".jazz-account-owner").exists());
}

#[test]
fn cancelled_or_dropped_pending_seal_retires_the_unpublished_transaction() {
    for complete_before_drop in [false, true] {
        for explicit_cancel in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let author = author("cancelled-seal", 0xc1);
            let db = open(directory.path(), author, owner(author), 0xc2).unwrap();
            let transaction = CoreOpenTransactionId::new().to_string();
            db.begin_transaction(transaction.clone(), "exclusive".into(), None, None)
                .unwrap();
            let Either::B(pending) = db
                .seal_initialization_transaction(transaction.clone())
                .unwrap()
            else {
                panic!("persistent preparation must return control before driving storage");
            };
            if complete_before_drop {
                for _ in 0..16 {
                    db.tick().unwrap();
                }
            }
            if explicit_cancel {
                pending.cancel();
            }
            drop(pending);
            for _ in 0..16 {
                db.tick().unwrap();
            }
            let retry = db.seal_initialization_transaction(transaction);
            assert!(
                retry
                    .and_then(|result| super::initialization::finish_seal(&db, result))
                    .is_err(),
                "an undelivered seal must abandon its original open transaction, not leak a frozen unit"
            );
            close(db);
        }
    }
}

#[test]
fn closing_owner_prevents_pending_seal_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let author = author("closing-seal", 0xc3);
    let db = open(directory.path(), author, owner(author), 0xc4).unwrap();
    let transaction = CoreOpenTransactionId::new().to_string();
    db.begin_transaction(transaction.clone(), "exclusive".into(), None, None)
        .unwrap();
    let Either::B(pending) = db.seal_initialization_transaction(transaction).unwrap() else {
        panic!("persistent preparation must return a pending seal");
    };
    close(db);
    assert!(
        pending.poll().is_err(),
        "shutdown must not register or expose an unpublished token"
    );
}
