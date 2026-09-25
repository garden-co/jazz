use super::*;
use crate::initialization::InitializationAction;

fn complete(
    fixture: &NativeHostAbiFixture,
    foreground: u64,
    action: InitializationAction,
) -> ForegroundDbCommandResponse {
    let mut result = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::InitializationV1 { version: 1, action },
    );
    for _ in 0..128 {
        let ForegroundDbCommandResponse::Pending { operation } = result else {
            return result;
        };
        fixture.tick(foreground);
        result = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
    }
    panic!("initialization operation failed to complete");
}

fn stage(fixture: &NativeHostAbiFixture, foreground: u64, row_id: [u8; 16]) -> u64 {
    let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
        foreground,
        ForegroundDbCommandRequest::BeginTransaction {
            kind: ForegroundTransactionKind::Exclusive,
        },
    ) else {
        panic!("exclusive transaction");
    };
    assert!(
        matches!(complete(fixture, foreground, InitializationAction::RecordAbsence {
        transaction, table: "todos".into(), row: row_id,
    }), ForegroundDbCommandResponse::Rows { rows } if rows.is_empty())
    );
    assert_eq!(
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".into(),
                cells: encoded_title_cells("sealed only"),
                row_id: Some(row_id),
            }
        ),
        ForegroundDbCommandResponse::Inserted { row_id }
    );
    transaction
}

fn seal(fixture: &NativeHostAbiFixture, foreground: u64, transaction: u64) -> serde_json::Value {
    let ForegroundDbCommandResponse::Rows { rows } = complete(
        fixture,
        foreground,
        InitializationAction::Seal { transaction },
    ) else {
        panic!("seal response");
    };
    serde_json::from_slice(&rows).unwrap()
}

#[test]
fn native_initialization_seal_is_unpublished_owner_bound_and_single_use() {
    // The runtime-owner token and leased identity are binding-only capabilities;
    // exercise their actual native C ABI, not a mocked SDK collaborator.
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("seal.sqlite"),
        "seal",
        &permissive_schema(),
        0xe1,
    );
    let owner = fixture.open_foreground(&capability);
    let sibling = fixture.open_foreground(&capability);
    let row = [0xe2; 16];
    let transaction = stage(&fixture, owner, row);
    let sealed = seal(&fixture, owner, transaction);
    let token = sealed["token"].as_str().unwrap().to_owned();
    let reserved = sealed["reservedTxId"].as_str().unwrap().to_owned();
    let before = fixture.execute(
        owner,
        ForegroundDbCommandRequest::LocalCurrentRow {
            table: "todos".into(),
            row_id: row,
        },
    );
    let ForegroundDbCommandResponse::Rows { rows } = before else {
        panic!("row response");
    };
    assert!(
        postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
            .unwrap()
            .is_empty(),
        "seal cannot publish a row"
    );
    assert!(matches!(
        complete(
            &fixture,
            sibling,
            InitializationAction::Publish {
                token: token.clone()
            }
        ),
        ForegroundDbCommandResponse::OperationError { .. }
    ));
    let status = complete(
        &fixture,
        owner,
        InitializationAction::Status {
            ids: vec![reserved.clone()],
        },
    );
    let ForegroundDbCommandResponse::Rows { rows } = status else {
        panic!("status response");
    };
    let status: serde_json::Value = serde_json::from_slice(&rows).unwrap();
    assert_eq!(status["statuses"][0]["kind"], "not-observed");
    assert!(matches!(
        complete(
            &fixture,
            owner,
            InitializationAction::Publish {
                token: token.clone()
            }
        ),
        ForegroundDbCommandResponse::TransactionCommitted { .. }
    ));
    assert!(matches!(
        complete(&fixture, owner, InitializationAction::Publish { token }),
        ForegroundDbCommandResponse::OperationError { .. }
    ));
    for _ in 0..32 {
        fixture.tick(owner);
    }
    let ForegroundDbCommandResponse::Rows { rows } = fixture.execute(
        owner,
        ForegroundDbCommandRequest::LocalCurrentRow {
            table: "todos".into(),
            row_id: row,
        },
    ) else {
        panic!("published row");
    };
    let batches: Vec<DecodedForegroundRowBatch> = postcard::from_bytes(&rows).unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.table, "todos");
    assert_eq!(batch.rows.len(), 1);
    let published = &batch.rows[0];
    assert_eq!(published.row_id, RowUuid::from_bytes(row));
    assert!(!published.deleted);
    // LocalCurrentRow carries a native row descriptor, not a query projection
    // with an id in its first cell. Identity is in the binding row envelope.
    let record = BorrowedRecord::new(&published.raw, &batch.descriptor);
    assert_eq!(
        record.get("title").unwrap(),
        Value::Nullable(Some(Box::new(Value::String("sealed only".into())))),
    );
    let ForegroundDbCommandResponse::Rows { rows } = complete(
        &fixture,
        owner,
        InitializationAction::Status {
            ids: vec![reserved],
        },
    ) else {
        panic!("durable status");
    };
    let status: serde_json::Value = serde_json::from_slice(&rows).unwrap();
    assert_eq!(status["statuses"][0]["kind"], "complete");
    assert_eq!(status["statuses"][0]["durability"], "local");
}

#[test]
fn native_unpublished_reservation_is_retired_across_clean_foreground_handoff() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("reservation.sqlite"),
        "reservation",
        &permissive_schema(),
        0xe3,
    );
    let first = fixture.open_foreground(&capability);
    let first_seal = seal(&fixture, first, stage(&fixture, first, [0xe4; 16]));
    assert_eq!(
        fixture.execute(first, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true }
    );
    let next = fixture.open_foreground(&capability);
    let next_seal = seal(&fixture, next, stage(&fixture, next, [0xe5; 16]));
    assert_ne!(
        first_seal["reservedTxId"], next_seal["reservedTxId"],
        "a cleanly returned lease includes unpublished reservations"
    );
    assert!(matches!(
        complete(
            &fixture,
            next,
            InitializationAction::Publish {
                token: first_seal["token"].as_str().unwrap().into()
            }
        ),
        ForegroundDbCommandResponse::OperationError { .. }
    ));
    assert!(
        matches!(complete(&fixture, next, InitializationAction::Cancel { token: next_seal["token"].as_str().unwrap().into() }), ForegroundDbCommandResponse::Rows { rows } if rows.is_empty())
    );
    let ForegroundDbCommandResponse::Rows { rows } = fixture.execute(
        next,
        ForegroundDbCommandRequest::LocalCurrentRow {
            table: "todos".into(),
            row_id: [0xe5; 16],
        },
    ) else {
        panic!("cancelled row");
    };
    assert!(
        postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn native_catalogue_readiness_requires_an_admitted_cache_owner() {
    let directory = tempfile::tempdir().unwrap();
    let relay = NativeRelay::spawn(config(
        directory.path().join("unadmitted-catalogue.sqlite"),
        Some("unadmitted-catalogue"),
    ))
    .unwrap();
    let client = relay
        .attach_client(
            fresh_client_identity(AuthorSubject::for_test_bytes([0xe7; 16])).unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
    assert!(matches!(
        client.initialization_command(InitializationAction::HasAuthenticatedCatalogue),
        Err(RelayError::ForegroundCommand(_)),
    ));
}

#[test]
fn native_catalogue_readiness_waits_for_held_owner_and_survives_waiter_cancellation() {
    // Only the native owner hook can suspend the exact observation lock. All
    // readiness, cancellation and completion assertions cross the real C ABI.
    let directory = tempfile::tempdir().unwrap();
    let fixture = NativeHostAbiFixture::new();
    let capability = fixture.admit(
        &directory.path().join("catalogue-readiness.sqlite"),
        "catalogue-readiness",
        &permissive_schema(),
        0xe6,
    );
    let foreground = fixture.open_foreground(&capability);
    for _ in 0..16 {
        fixture.tick(foreground);
    }
    let relay = unsafe { &*fixture.host }
        .inner
        .lock()
        .unwrap()
        .foreground_client(foreground)
        .unwrap()
        .relay
        .clone();
    hold_persistent_owner(&relay);
    let request = ForegroundDbCommandRequest::InitializationV1 {
        version: 1,
        action: InitializationAction::HasAuthenticatedCatalogue,
    };
    let ForegroundDbCommandResponse::Pending {
        operation: cancelled,
    } = fixture.execute(foreground, request.clone())
    else {
        panic!("contended catalogue observation must be pending, not false");
    };
    let ForegroundDbCommandResponse::Pending { operation } = fixture.execute(foreground, request)
    else {
        panic!("a second readiness waiter must join the pending observation");
    };
    assert_eq!(
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::Cancel {
                operation: cancelled
            }
        ),
        ForegroundDbCommandResponse::Cancelled { cancelled: true },
    );
    for _ in 0..4 {
        fixture.tick(foreground);
        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation }),
            ForegroundDbCommandResponse::Pending { operation },
        );
    }
    relay
        .run(|worker| {
            worker.persistent_tick = None;
            Ok(())
        })
        .unwrap();
    let mut result = ForegroundDbCommandResponse::Pending { operation };
    for _ in 0..128 {
        fixture.tick(foreground);
        result = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        if !matches!(result, ForegroundDbCommandResponse::Pending { .. }) {
            break;
        }
    }
    assert_eq!(
        result,
        ForegroundDbCommandResponse::Rows {
            rows: b"false".to_vec()
        },
        "a local schema is not authenticated catalogue evidence",
    );
    assert_eq!(
        fixture.execute(foreground, ForegroundDbCommandRequest::Close),
        ForegroundDbCommandResponse::Closed { closed: true },
    );
}

#[test]
fn initialization_v1_command_corpus_is_canonical() {
    let cases = [
        (
            InitializationAction::Seal { transaction: 129 },
            vec![37, 1, 0, 129, 1],
        ),
        (
            InitializationAction::Publish { token: "x".into() },
            vec![37, 1, 1, 1, 120],
        ),
        (
            InitializationAction::Cancel { token: "x".into() },
            vec![37, 1, 2, 1, 120],
        ),
        (
            InitializationAction::Status {
                ids: vec!["x".into()],
            },
            vec![37, 1, 4, 1, 1, 120],
        ),
        (
            InitializationAction::HasAuthenticatedCatalogue,
            vec![37, 1, 5],
        ),
    ];
    for (action, bytes) in cases {
        let request = ForegroundDbCommandRequest::InitializationV1 { version: 1, action };
        assert_eq!(postcard::to_allocvec(&request).unwrap(), bytes);
        assert_eq!(decode_foreground_command(&bytes).unwrap(), request);
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_foreground_command(&trailing).is_err());
    }
}

#[test]
fn admitted_owner_restart_replays_exact_pending_leased_foreground_unit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-owner-restart.sqlite");
    let row = RowUuid::from_bytes([0xa4; 16]);
    let account = jazz::account_registry::AccountId(RowUuid::from_bytes([0xa5; 16]).0);
    let author = AuthorSubject::authenticated("https://issuer.example", "restart")
        .unwrap()
        .with_account(account);
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xa6; 16]),
        author,
    };
    let mut expected = None;
    for reopened in [false, true] {
        let fixture = NativeHostAbiFixture::new();
        let capability =
            fixture.admit_identity(&path, author.canonical(), &permissive_schema(), identity);
        let foreground = fixture.open_foreground(&capability);
        let client = unsafe { &*fixture.host }
            .inner
            .lock()
            .unwrap()
            .foreground_client(foreground)
            .unwrap()
            .clone();
        if !reopened {
            let sealed = seal(
                &fixture,
                foreground,
                stage(&fixture, foreground, *row.0.as_bytes()),
            );
            assert!(matches!(
                complete(
                    &fixture,
                    foreground,
                    InitializationAction::Publish {
                        token: sealed["token"].as_str().unwrap().into(),
                    }
                ),
                ForegroundDbCommandResponse::TransactionCommitted { .. }
            ));
        }
        let mut uploaded = None;
        for _ in 0..128 {
            fixture.tick(foreground);
            for mut message in client.relay.wire().take_outbound().unwrap() {
                if let SyncMessage::CommitUnit { tx, versions } = &mut message {
                    if versions.iter().any(|version| version.row_uuid() == row) {
                        assert_ne!(
                            tx.tx_id.node, identity.node,
                            "the transaction belongs to the leased foreground, not the durable owner"
                        );
                        // Relay ingestion deliberately drops the foreground's
                        // untrusted policy hint. Terminal authorization uses the
                        // admitted relay session, never this transport field.
                        assert_eq!(
                            tx.permission_subject,
                            if reopened { None } else { Some(author) },
                        );
                        assert_eq!(tx.made_by, author, "durable provenance remains exact");
                        tx.permission_subject = None;
                        uploaded = Some(postcard::to_allocvec(&message).unwrap());
                    }
                }
            }
            if uploaded.is_some() {
                break;
            }
        }
        let uploaded =
            uploaded.expect("original pending unit reaches upstream after owner restart");
        if reopened {
            assert_eq!(
                Some(uploaded),
                expected,
                "replay preserves the complete commit unit byte-for-byte except the explicitly checked transient policy capability"
            );
        } else {
            expected = Some(uploaded);
        }
        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }
}
