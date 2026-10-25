use crate::*;
use jazz::records::{RecordDescriptor, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchema};

fn fixture() -> NapiDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    NapiDb::open_memory(
        Uint8Array::new(serde_json::to_vec(&schema).unwrap()),
        Uint8Array::new(super::encode_persistent_open_config(
            CoreAuthorSubject::for_test_bytes([0xe1; 16]),
        )),
        None,
    )
    .unwrap()
}

pub(super) fn finish_seal(
    db: &NapiDb,
    result: Either<
        crate::initialization::NativeInitializationSeal,
        crate::initialization::PendingNativeInitializationSeal,
    >,
) -> js::Result<crate::initialization::NativeInitializationSeal> {
    let pending = match result {
        Either::A(seal) => return Ok(seal),
        Either::B(pending) => pending,
    };
    for _ in 0..1024 {
        if let Some(seal) = pending.poll()? {
            return Ok(seal);
        }
        db.tick()?;
        std::thread::yield_now();
    }
    panic!("native initialization seal did not complete");
}

pub(super) fn finish_absence(
    db: &NapiDb,
    result: Either<Uint8Array, PendingNativeRead>,
) -> js::Result<()> {
    if let Either::B(pending) = result {
        for _ in 0..1024 {
            if pending.poll()?.is_some() {
                return Ok(());
            }
            db.tick()?;
            std::thread::yield_now();
        }
        panic!("native initialization absence did not complete");
    }
    Ok(())
}

pub(super) fn finish_status(
    db: &NapiDb,
    result: Either<String, PendingNativePermissionAdvice>,
) -> js::Result<String> {
    let pending = match result {
        Either::A(status) => return Ok(status),
        Either::B(pending) => pending,
    };
    for _ in 0..1024 {
        if let Some(status) = pending.poll()? {
            return Ok(status);
        }
        db.tick()?;
        std::thread::yield_now();
    }
    panic!("native initialization status did not complete");
}

fn finish_catalogue_readiness(db: &NapiDb) -> bool {
    let pending = match db.has_authenticated_catalogue().unwrap() {
        Either::A(ready) => return ready,
        Either::B(pending) => pending,
    };
    for _ in 0..1024 {
        if let Some(state) = pending.poll().unwrap() {
            assert!(
                state.capture.is_none(),
                "view readiness must never drain captures"
            );
            return state.ready;
        }
        db.tick().unwrap();
        std::thread::yield_now();
    }
    panic!("native catalogue readiness did not complete");
}

fn finish_catalogue(db: &NapiDb) -> crate::initialization::NativeCatalogueState {
    let pending = match db.take_authenticated_catalogue_state().unwrap() {
        Either::A(state) => return state,
        Either::B(pending) => pending,
    };
    for _ in 0..1024 {
        if let Some(state) = pending.poll().unwrap() {
            return state;
        }
        db.tick().unwrap();
        std::thread::yield_now();
    }
    panic!("native catalogue observation did not complete");
}

#[test]
fn napi_seal_is_unpublished_owner_bound_single_use_and_preserves_insert_absence() {
    let db = fixture();
    let foreign = fixture();
    let open = CoreOpenTransactionId::new().to_string();
    let row = [0xe2; 16];
    db.begin_transaction(open.clone(), "exclusive".into(), None, None)
        .unwrap();
    finish_absence(
        &db,
        db.record_initialization_insert_absence(
            open.clone(),
            "items".into(),
            Uint8Array::new(row.to_vec()),
        )
        .unwrap(),
    )
    .unwrap();
    let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
    let raw = descriptor
        .create(&[CoreValue::String("sealed value".into())])
        .unwrap();
    let cells =
        jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(raw, descriptor))
            .unwrap();
    db.insert_in_transaction(
        open.clone(),
        "items".into(),
        Uint8Array::new(cells),
        Some(InsertOptions {
            row_id: Some(Uint8Array::new(row.to_vec())),
            author: None,
            attribution: None,
            branch: None,
            updated_at_ms: None,
        }),
    )
    .unwrap();
    let sealed = finish_seal(
        &db,
        db.seal_initialization_transaction(open.clone()).unwrap(),
    )
    .unwrap();
    assert!(
        foreign
            .publish_initialization_transaction(sealed.token.clone())
            .is_err()
    );
    let before = db
        .local_current_row("items".into(), Uint8Array::new(row.to_vec()))
        .unwrap();
    assert_eq!(before.as_ref(), &[0], "sealed rows are not published");
    let status: serde_json::Value = serde_json::from_str(
        &finish_status(
            &db,
            db.initialization_transaction_status(vec![sealed.reserved_tx_id.clone()])
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(status["statuses"][0]["kind"], "not-observed");
    let _write = db
        .publish_initialization_transaction(sealed.token.clone())
        .unwrap();
    assert!(db.publish_initialization_transaction(sealed.token).is_err());
    assert!(
        db.initialization_transaction_status(vec![sealed.reserved_tx_id.clone(); 65])
            .is_err()
    );
    let published: serde_json::Value = serde_json::from_str(
        &finish_status(
            &db,
            db.initialization_transaction_status(vec![sealed.reserved_tx_id])
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(published["statuses"][0]["kind"], "complete");
    assert_eq!(published["statuses"][0]["fate"]["kind"], "pending");
    let occupied = CoreOpenTransactionId::new().to_string();
    db.begin_transaction(occupied.clone(), "exclusive".into(), None, None)
        .unwrap();
    let absence = db.record_initialization_insert_absence(
        occupied.clone(),
        "items".into(),
        Uint8Array::new(row.to_vec()),
    );
    assert!(
        absence
            .and_then(|result| finish_absence(&db, result))
            .is_err(),
        "exact absence must never turn insert into overwrite"
    );
    db.rollback_transaction(occupied).unwrap();
}

#[test]
fn schema_view_cannot_drain_authenticated_catalogue_capture_from_runtime_owner() {
    let schema = serde_json::to_vec(
        &SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap();
    let author =
        CoreAuthorSubject::authenticated("https://issuer.example", "capture-owner").unwrap();
    let client_node = CoreNodeUuid::from_bytes([0xc1; 16]);
    let authority_node = CoreNodeUuid::from_bytes([0xc2; 16]);
    let config = |node, history_complete| {
        Uint8Array::new(
            postcard::to_allocvec(&(
                (node, author),
                None::<u64>,
                history_complete,
                None::<u32>,
                None::<String>,
            ))
            .unwrap(),
        )
    };
    let owner = NapiDb::open_memory(
        Uint8Array::new(schema.clone()),
        config(client_node, false),
        None,
    )
    .unwrap();
    let authority = NapiDb::open_memory_as_backend(
        Uint8Array::new(schema.clone()),
        config(authority_node, true),
    )
    .unwrap();
    let core = |db: &NapiDb| {
        let inner = db.inner.borrow();
        let Some(inner) = inner.as_ref() else {
            panic!("memory fixture");
        };
        let core = &inner.core;
        Rc::clone(core)
    };
    let owner_core = core(&owner);
    let authority_core = core(&authority);
    // SQLite authorities explicitly install their accepted catalogue before
    // advertising it; a runtime's opened schema alone is not a deployment.
    let compiled = crate::decode_public_schema(&schema).unwrap();
    authority_core
        .install_authority_catalogue(&jazz::protocol::CatalogueSnapshot {
            genesis_physical_identities: jazz::protocol::PhysicalIdentityManifest::allocate(
                &compiled,
            ),
            schemas: vec![jazz::protocol::SchemaVersion::new(compiled.clone())],
            lineages: vec![],
            current_write_schema: jazz::protocol::CurrentWriteSchema {
                revision: 1,
                schema: compiled.version_id(),
            },
        })
        .unwrap();
    let (client_link, authority_link) = jazz::db::duplex();
    let upstream = core_block_on(owner_core.connect_upstream(client_link));
    let downstream = authority_core.accept_subscriber(authority_link, author);
    for _ in 0..32 {
        owner.tick().unwrap();
        authority.tick().unwrap();
    }
    let alias = owner
        .register_schema(Uint8Array::new(schema.clone()))
        .unwrap();
    assert!(alias.take_authenticated_catalogue_state().is_err());
    assert!(finish_catalogue_readiness(&alias));
    assert!(finish_catalogue_readiness(&alias));
    let state = finish_catalogue(&owner);
    assert!(state.ready);
    let capture = state
        .capture
        .expect("the owner retains the real upstream capture");
    owner
        .validate_catalogue_capture_replacement(
            Uint8Array::new(capture.to_vec()),
            Uint8Array::new(capture.to_vec()),
        )
        .unwrap();
    let offline = NapiDb::open_memory(
        Uint8Array::new(schema),
        config(CoreNodeUuid::from_bytes([0xc3; 16]), false),
        Some(capture),
    )
    .unwrap();
    let offline_state = finish_catalogue(&offline);
    assert!(offline_state.ready);
    assert!(
        offline_state.capture.is_none(),
        "cache installation must not manufacture a live capture"
    );
    let drained = finish_catalogue(&owner);
    assert!(drained.ready);
    assert!(drained.capture.is_none(), "only the owner drains once");
    assert!(owner_core.detach_connection(&upstream));
    authority_core.disconnect_subscriber(downstream);
}
