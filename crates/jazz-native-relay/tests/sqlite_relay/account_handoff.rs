//! Account-host admission, rather than caller-selected scope text, owns handoff.
use super::*;
use base64::Engine;

const APP: &str = "01010101-0101-0101-0101-010101010101";
const ACCOUNT: &str = "07070707-0707-0707-0707-070707070707";
const ISSUER: &str = "https://handoff.example";

fn author(subject: &str) -> AuthorSubject {
    AuthorSubject::authenticated(ISSUER, subject)
        .unwrap()
        .with_account(jazz::account_registry::AccountId(ACCOUNT.parse().unwrap()))
}

fn open(root: &Path, subject: &str) -> Result<Host, JazzNativeRelayStatus> {
    // External claims are advisory at this trusted host boundary. These tests
    // attach a real in-process authority separately, under the exact subject.
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&serde_json::json!({"iss":ISSUER,"sub":subject})).unwrap());
    let request = serde_json::to_vec(&serde_json::json!({
        "registry":format!("https://registry.example/apps/{APP}/accounts"),
        "app_id":APP, "env":"test", "account_id":ACCOUNT,
        "issuer":ISSUER, "subject":subject, "jwt":format!("e30.{payload}.signature"),
        "claims":{}
    }))
    .unwrap();
    let host = jazz_native_relay_host_new();
    let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
    let mut result = Host {
        host,
        lease,
        relay: 0,
        foreground: 0,
    };
    let root = root.to_str().unwrap().as_bytes();
    let mut out = empty();
    let check = |status| {
        if status == JazzNativeRelayStatus::Ok {
            Ok(())
        } else {
            Err(status)
        }
    };
    check(unsafe {
        jazz_native_relay_host_lease_begin_account_session_json(
            lease,
            request.as_ptr(),
            request.len(),
            root.as_ptr(),
            root.len(),
            &mut out,
        )
    })?;
    let setup = unsafe { take(&mut out) };
    let schema = serde_json::to_vec(schema().public_schema()).unwrap();
    check(unsafe {
        jazz_native_relay_host_lease_attach_account_schema_json(
            lease,
            setup.as_ptr(),
            setup.len(),
            schema.as_ptr(),
            schema.len(),
            &mut out,
        )
    })?;
    let admission = unsafe { take(&mut out) };
    let command = postcard::to_allocvec(&RelayCommandRequest::Open {
        supported_abi_minimum: NATIVE_RELAY_ABI_V1,
        supported_abi_maximum: NATIVE_RELAY_ABI_V1,
        admitted_scope: postcard::from_bytes(&admission).unwrap(),
    })
    .unwrap();
    check(unsafe {
        jazz_native_relay_host_execute(host, command.as_ptr(), command.len(), &mut out)
    })?;
    let response: RelayCommandResponse = postcard::from_bytes(&unsafe { take(&mut out) }).unwrap();
    let RelayCommandResponse::Opened { relay } = response else {
        panic!("{response:?}")
    };
    result.relay = relay;
    check(unsafe {
        jazz_native_relay_host_lease_open_attached_foreground(
            lease,
            admission.as_ptr(),
            admission.len(),
            &mut result.foreground,
        )
    })?;
    for _ in 0..20 {
        result.pump();
    }
    Ok(result)
}

fn path(root: &Path) -> std::path::PathBuf {
    root.join(root_fixture()["account_base_filename"].as_str().unwrap())
}

fn root_fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../fixtures/sqlite_native_account_login_root_v1.json"
    ))
    .unwrap()
}

fn login_path(root: &Path, subject: &str) -> std::path::PathBuf {
    root.join(root_fixture()[subject]["filename"].as_str().unwrap())
}

fn bind(authority: &mut Authority, subject: &str) {
    authority.frames = RefCell::default();
    authority.inbound = Rc::default();
    authority.outbound = Rc::default();
    authority.db.accept_subscriber(
        Box::new(Queues {
            inbound: authority.inbound.clone(),
            outbound: authority.outbound.clone(),
        }),
        author(subject),
    );
}

fn settle(host: &Host, authority: &Authority, tx: [u8; 16]) {
    let wait = host.command(ForegroundDbCommandRequest::WaitForTransaction {
        tx_id: tx,
        tier: "global".into(),
        observe_only: false,
    });
    assert!(
        matches!(authority.complete(host, wait), ForegroundDbCommandResponse::TransactionSettled { tx_id } if tx_id == tx)
    );
}

fn pending_units(host: &Host, authority: &mut Authority) -> Vec<Vec<u8>> {
    // Complete the real schema handshake but withhold every write from the
    // authority. No receipt or acceptance is manufactured by this fixture.
    let ForegroundDbCommandResponse::NativeSessionMetadata { user_id, .. } =
        host.command(ForegroundDbCommandRequest::NativeSessionMetadata)
    else {
        panic!("session metadata");
    };
    bind(authority, &user_id);
    let mut units = Vec::new();
    for _ in 0..20 {
        host.pump();
        let RelayCommandResponse::Frames { frames } =
            host.relay_command(RelayCommandRequest::ReceiveRelayFrames { relay: host.relay })
        else {
            panic!("frames");
        };
        for frame in frames {
            if matches!(
                jazz::db::decode_message(&frame).unwrap(),
                SyncMessage::Commit { .. }
            ) {
                units.push(frame);
            } else if let Some(message) = authority.frames.borrow_mut().push(&frame).unwrap() {
                authority.inbound.borrow_mut().push_back(message);
            }
        }
        block_on(authority.db.tick()).unwrap();
        for message in authority.outbound.borrow_mut().drain(..) {
            for frame in encode_frames(message).unwrap() {
                host.relay_command(RelayCommandRequest::SendRelayFrame {
                    relay: host.relay,
                    frame,
                });
            }
        }
    }
    units
}

fn local_texts(host: &Host) -> Vec<String> {
    let mut response = host.command(ForegroundDbCommandRequest::All {
        query: postcard::to_allocvec(&Query::from("notes")).unwrap(),
        options_json: r#"{"tier":"local-first"}"#.into(),
        transaction: None,
    });
    for _ in 0..40 {
        match response {
            ForegroundDbCommandResponse::Pending { operation } => {
                host.pump();
                response = host.command(ForegroundDbCommandRequest::Poll { operation });
            }
            ForegroundDbCommandResponse::Rows { rows } => {
                let mut texts = jazz::binding_codec::decode_rows(&rows)
                    .unwrap()
                    .into_iter()
                    .flat_map(|batch| batch.rows)
                    .map(|row| match &row.cells["text"] {
                        Value::String(text) => text.clone(),
                        other => panic!("{other:?}"),
                    })
                    .collect::<Vec<_>>();
                texts.sort();
                return texts;
            }
            other => panic!("{other:?}"),
        }
    }
    panic!("local read did not finish: {response:?}")
}

fn expect_local_texts(host: &Host, expected: &[&str]) {
    // The foreground starts empty. LocalFirst requests its durable owner's
    // inputs; wait for those actual rows through the public read/drive path.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let actual = local_texts(host);
        if actual == expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "local rows {actual:?}, expected {expected:?}"
        );
        host.pump();
        std::thread::yield_now();
    }
}

/// Alice closes her account owner, then linked Bob opens his own login store
/// under the same account lock. Both writes settle under their original login;
/// Bob cannot acquire the account before Alice closes it.
#[test]
fn native_private_account_handoff_preserves_history_and_exclusivity() {
    let directory = tempfile::tempdir().unwrap();
    let alice = open(directory.path(), "alice").unwrap();
    let path = path(directory.path());
    assert!(open(directory.path(), "bob").is_err());
    assert!(!login_path(directory.path(), "bob").exists());
    let mut authority = Authority::new();
    bind(&mut authority, "alice");
    authority.drive(&alice);
    let tx = alice.insert(81, "alice");
    settle(&alice, &authority, tx);
    let provenance = authority
        .db
        .row_provenance("notes", jazz::ids::RowUuid::from_bytes([81; 16]))
        .unwrap();
    drop(alice);
    let previous = generic_marker_bytes(&path);
    let bob = open(directory.path(), "bob").unwrap();
    assert!(open(directory.path(), "alice").is_err());
    assert_eq!(generic_marker_bytes(&path), previous);
    let registry = NativeRelayRegistry::default();
    assert!(matches!(
        registry.open(generic_config(&login_path(directory.path(), "bob"))),
        Err(RelayError::StorageAlreadyOwned)
    ));
    bind(&mut authority, "bob");
    authority.drive(&bob);
    let tx = bob.insert(82, "bob");
    settle(&bob, &authority, tx);
    assert_eq!(authority.texts(), ["alice", "bob"]);
    assert_eq!(
        authority
            .db
            .row_provenance("notes", jazz::ids::RowUuid::from_bytes([81; 16]))
            .unwrap(),
        provenance
    );
    drop(bob);
    let next = generic_marker_bytes(&login_path(directory.path(), "bob"));
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../jazz/fixtures/sqlite_private_owner_handoff_v1.json"
    ))
    .unwrap();
    assert_eq!(previous["author_utf8"], fixture["alice"]["author_utf8"]);
    assert_eq!(
        previous["scope_utf8"],
        fixture["alice"]["native_scope_utf8"]
    );
    assert_eq!(next["author_utf8"], fixture["bob"]["author_utf8"]);
    assert_eq!(next["scope_utf8"], fixture["bob"]["native_scope_utf8"]);
    assert_ne!(previous["node"], next["node"]);
    assert_ne!(previous["author_utf8"], next["author_utf8"]);
    assert_eq!(next["scope_utf8"], next["owner_utf8"]);
    drop(open(directory.path(), "alice").unwrap());
    assert_eq!(generic_marker_bytes(&path), previous);
}

/// Alice and linked Bob both publish offline in turn. Each login sees and
/// resends only its own exact pending Commit; neither upload reaches authority
/// under the other login. Alice's existing account file is never rewritten.
#[test]
fn native_private_account_handoff_preserves_pending_foreground_upload() {
    let directory = tempfile::tempdir().unwrap();
    // Reconnect to the same authority catalogue across every login and reopen.
    let mut authority = Authority::new();
    let alice = open(directory.path(), "alice").unwrap();
    alice.insert(83, "pending alice");
    let units = pending_units(&alice, &mut authority);
    assert_eq!(units.len(), 1);
    let path = path(directory.path());
    drop(alice);
    let marker = generic_marker_bytes(&path);
    let bob = open(directory.path(), "bob").unwrap();
    assert!(local_texts(&bob).is_empty());
    assert!(pending_units(&bob, &mut authority).is_empty());
    bob.insert(84, "pending bob");
    let bob_units = pending_units(&bob, &mut authority);
    assert_eq!(bob_units.len(), 1);
    assert_ne!(bob_units, units);
    expect_local_texts(&bob, &["pending bob"]);
    drop(bob);
    assert_eq!(generic_marker_bytes(&path), marker);
    let reopened = open(directory.path(), "alice").unwrap();
    expect_local_texts(&reopened, &["pending alice"]);
    assert_eq!(pending_units(&reopened, &mut authority), units);
    // The capture consumed this connection's initial handshake and upload.
    // Reopen to give the real authority a complete new transport stream.
    drop(reopened);
    let bob = open(directory.path(), "bob").unwrap();
    expect_local_texts(&bob, &["pending bob"]);
    assert_eq!(pending_units(&bob, &mut authority), bob_units);
    drop(bob);
    let reopened = open(directory.path(), "alice").unwrap();
    bind(&mut authority, "alice");
    authority.drive(&reopened);
    assert_eq!(authority.texts(), ["pending alice"]);
    drop(reopened);
    let bob = open(directory.path(), "bob").unwrap();
    bind(&mut authority, "bob");
    authority.drive(&bob);
    assert_eq!(authority.texts(), ["pending alice", "pending bob"]);
}

/// Alice's account host recovers a crash between the core subject commit and
/// the host marker commit. Returning Alice recovers her old file; Bob's new
/// login store stays separate. Raw SQLite inspection stages that otherwise
/// unreachable crash boundary; all recovery uses public ABI.
#[test]
fn native_private_account_handoff_recovers_interrupted_marker_update() {
    let directory = tempfile::tempdir().unwrap();
    drop(open(directory.path(), "alice").unwrap());
    let path = path(directory.path());
    let before = generic_marker_bytes(&path);
    for requested in ["alice", "bob"] {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE _jazz_owner_scope_v1 SET subject=?1 WHERE singleton=1",
            [author("bob").canonical()],
        )
        .unwrap();
        drop(conn);
        drop(open(directory.path(), requested).unwrap());
        let requested_path = if requested == "alice" {
            path.clone()
        } else {
            login_path(directory.path(), requested)
        };
        let conn = rusqlite::Connection::open(&requested_path).unwrap();
        let core: String = conn
            .query_row(
                "SELECT subject FROM _jazz_owner_scope_v1 WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(core, author(requested).canonical());
        drop(conn);
        drop(open(directory.path(), "alice").unwrap());
        assert_eq!(generic_marker_bytes(&path), before);
    }
}

/// Bob is the first login to claim the old account file, so Alice uses the
/// independently pinned login filename. A planted Bob file at that exact path
/// is rejected without falling back to another identity's store or deleting it.
#[test]
fn native_private_account_handoff_pins_reverse_root_and_refuses_substitution() {
    let directory = tempfile::tempdir().unwrap();
    drop(open(directory.path(), "bob").unwrap());
    let base = path(directory.path());
    let before = generic_marker_bytes(&base);
    let alice = open(directory.path(), "alice").unwrap();
    alice.insert(85, "alice private");
    expect_local_texts(&alice, &["alice private"]);
    drop(alice);
    let alice_path = login_path(directory.path(), "alice");
    assert!(alice_path.is_file());
    assert_eq!(generic_marker_bytes(&base), before);
    // This physical corruption cannot be staged through the public API.
    // Copy a closed, checkpointed Bob database, then require exact admission.
    std::fs::copy(&base, &alice_path).unwrap();
    let planted = std::fs::read(&alice_path).unwrap();
    assert!(open(directory.path(), "alice").is_err());
    assert_eq!(std::fs::read(&alice_path).unwrap(), planted);
    assert_eq!(generic_marker_bytes(&base), before);
    drop(open(directory.path(), "bob").unwrap());
}
