//! Metadata coverage uses protocol taps to distinguish one fresh request from
//! cached admission and prove that no row/evidence messages were evaluated.
use crate::accepted_rpc_reads::{alice, bob, claims, drive, open, tick};
use jazz::db::{
    AcceptedReadScope, ErrorCode, ReadSpan, SyncMessage, Transport, TransportError, block_on,
    duplex,
};
use jazz::ids::AuthorSubject;
use std::{
    cell::RefCell,
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

struct Tap {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}
impl Transport for Tap {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push(message.clone());
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}

/// Alice refreshes catalogue admission twice, directly and through a relay.
/// Several stored-name lookups after each refresh need only one request.
#[test]
fn fresh_catalogue_coverage_uses_one_request_and_no_row_evidence() {
    for relayed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        let relay = open(8, AuthorSubject::SYSTEM, false, false);
        let client = open(7, alice(), false, false);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let returned = Rc::new(RefCell::new(Vec::new()));
        let (up, down) = duplex();
        block_on(client.connect_upstream(Box::new(Tap {
            inner: up,
            sent: sent.clone(),
        })));
        let down = Box::new(Tap {
            inner: down,
            sent: returned.clone(),
        });
        if relayed {
            relay.accept_subscriber(down, alice()).unwrap();
            let (up, down) = duplex();
            block_on(relay.connect_upstream(up));
            server
                .accept_subscriber(down, AuthorSubject::SYSTEM)
                .unwrap();
        } else {
            server.accept_subscriber(down, alice()).unwrap();
        }
        for expected in 1..=2 {
            drive(
                &[&client, &relay, &server],
                client.cover_catalogue_with_timeout(None, 5000).unwrap(),
            )
            .unwrap();
            assert_eq!(
                client.table_identity("notes").unwrap(),
                server.table_identity("notes").unwrap()
            );
            for column in ["body", "owner", "token"] {
                assert_eq!(
                    client.column_identity("notes", column).unwrap(),
                    server.column_identity("notes", column).unwrap()
                );
            }
            assert_eq!(
                sent.borrow()
                    .iter()
                    .filter(|m| matches!(
                        m,
                        SyncMessage::Read {
                            span: ReadSpan::CatalogueOnce { .. },
                            ..
                        }
                    ))
                    .count(),
                expected
            );
        }
        let replies = returned.borrow();
        let first_catalogue = replies
            .iter()
            .position(|m| matches!(m, SyncMessage::Welcome { .. } | SyncMessage::Catalogue(_)))
            .unwrap();
        let first_answer = replies
            .iter()
            .position(|m| matches!(m, SyncMessage::CatalogueCovered { .. }))
            .unwrap();
        assert!(first_catalogue < first_answer);
        assert!(!replies.iter().any(|m| matches!(
            m,
            SyncMessage::Accepted { .. } | SyncMessage::Rows { .. } | SyncMessage::Settled { .. }
        )));
    }
}

/// Alice changes claims after admission but before polling. The future must
/// retain her original scope; an async-fn admission would incorrectly pass.
#[test]
fn catalogue_admission_captures_claims_before_first_poll() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    server.set_identity_claims(alice(), claims("first"));
    let read = server
        .cover_catalogue_with_timeout(
            Some(AcceptedReadScope {
                author: alice(),
                claims: claims("first"),
            }),
            5000,
        )
        .unwrap();
    server.set_identity_claims(alice(), claims("second"));
    assert_eq!(block_on(read).unwrap_err().code, ErrorCode::NotObserved);
}

/// Alice cannot delegate Bob's identity from her own opened store.
#[test]
fn catalogue_admission_refuses_a_different_peer_synchronously() {
    let client = open(7, alice(), false, false);
    assert!(
        client
            .cover_catalogue_with_timeout(
                Some(AcceptedReadScope {
                    author: bob(),
                    claims: Default::default()
                }),
                5000
            )
            .is_err()
    );
}

/// Alice's cancelled metadata requests share ordinary pending-fetch cleanup,
/// whether still queued or already sent. No independent catalogue queue stays.
#[test]
fn catalogue_cancellation_cleans_queued_and_issued_requests() {
    for issued in [false, true] {
        let client = open(7, alice(), false, false);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let (up, _down) = duplex();
        block_on(client.connect_upstream(Box::new(Tap {
            inner: up,
            sent: sent.clone(),
        })));
        let mut read = Box::pin(client.cover_catalogue_with_timeout(None, 5000).unwrap());
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if issued {
            tick(&[&client]);
        }
        drop(read);
        tick(&[&client]);
        let messages = sent.borrow();
        let reads: Vec<_> = messages
            .iter()
            .filter_map(|m| match m {
                SyncMessage::Read {
                    id,
                    span: ReadSpan::CatalogueOnce { .. },
                    ..
                } => Some(*id),
                _ => None,
            })
            .collect();
        if issued {
            assert_eq!(reads.len(), 1);
            assert!(
                messages
                    .iter()
                    .any(|m| matches!(m, SyncMessage::Unsubscribe { id } if *id == reads[0]))
            );
        } else {
            assert!(reads.is_empty());
        }
    }
}

/// Alice spends her budget before polling. Even a local authority cannot
/// reinterpret admission as starting when the future is eventually polled.
#[test]
fn catalogue_deadline_starts_before_first_poll() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let mut read = Box::pin(server.cover_catalogue_with_timeout(None, 0).unwrap());
    let Poll::Ready(result) = read.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("expired admission")
    };
    assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
}

/// Alice and Bob agree on the append-only metadata mode independently of the
/// encoder's object layout. Truncated and trailing fields are not accepted.
#[test]
fn catalogue_coverage_v1_has_independent_wire_fixtures() {
    use jazz::db::{decode_message, encode_frames};
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/catalogue_coverage_v1.json")).unwrap();
    for field in ["request_hex", "reply_hex"] {
        let bytes = hex::decode(fixture[field].as_str().unwrap()).unwrap();
        let decoded = decode_message(&bytes).unwrap();
        if field == "request_hex" {
            assert!(
                matches!(&decoded, SyncMessage::Read { query, span: ReadSpan::CatalogueOnce { schema, scope }, .. } if **query == jazz::query::Query::from("").limit(0) && schema == &[0x44;16] && scope.author == AuthorSubject::SYSTEM && scope.claims.is_empty())
            );
        } else {
            assert!(
                matches!(&decoded, SyncMessage::CatalogueCovered { id: 1, coverage } if coverage.schema == [0x44;16] && coverage.namespace == [0x55;16] && coverage.names_digest == [0x66;16] && coverage.scope.author == AuthorSubject::SYSTEM && coverage.scope.claims.is_empty())
            );
        }
        assert_eq!(encode_frames(decoded).unwrap(), [bytes.clone()]);
        for end in 0..bytes.len() {
            assert!(decode_message(&bytes[..end]).is_err());
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(decode_message(&extra).is_err());
    }
}

struct Corrupt {
    inner: Box<dyn Transport>,
    field: u8,
}
impl Transport for Corrupt {
    fn send(&mut self, mut message: SyncMessage) -> Result<(), TransportError> {
        if let SyncMessage::CatalogueCovered { coverage, .. } = &mut message {
            match self.field {
                0 => coverage.schema[0] ^= 1,
                1 => coverage.namespace[0] ^= 1,
                2 => coverage.names_digest[0] ^= 1,
                3 => coverage.scope.author = bob(),
                4 => coverage.scope.claims = claims("unrequested"),
                _ => unreachable!(),
            }
        }
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}

/// A forged acknowledgement cannot cover Alice's handle or scope, even when
/// the real catalogue arrived first. A tap injects values servers never emit.
#[test]
fn catalogue_completion_checks_schema_namespace_names_identity_and_claims() {
    for field in 0..5 {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        let client = open(7, alice(), false, false);
        let (up, down) = duplex();
        block_on(client.connect_upstream(up));
        server
            .accept_subscriber(Box::new(Corrupt { inner: down, field }), alice())
            .unwrap();
        assert!(
            drive(
                &[&client, &server],
                client.cover_catalogue_with_timeout(None, 5000).unwrap()
            )
            .is_err()
        );
    }
}

/// Alice's retained handle keeps its old catalogue identity after Bob renames
/// a table. Coverage names that handle, independent of the owner's new names.
#[test]
fn catalogue_coverage_keeps_the_requested_schema_handle() {
    use crate::common::{allow_all_policies, compile_schema};
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let old = crate::accepted_rpc_reads::schema(false);
    let client = open(7, alice(), false, false);
    let view = block_on(client.register_schema_view(old.clone())).unwrap();
    let expected = server.table_identity("notes").unwrap();
    let renamed = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .column("token", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let lens = MigrationLens::new(
        old.version_id(),
        renamed.version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "tasks".into(),
            ops: vec![LensOp::RenameColumn {
                from: "body".into(),
                to: "title".into(),
            }],
        }],
    )
    .unwrap();
    block_on(server.activate_schema_with_lenses(renamed, &[lens])).unwrap();
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    drive(
        &[&client, &server],
        view.cover_catalogue_with_timeout(None, 5000).unwrap(),
    )
    .unwrap();
    assert_eq!(view.table_identity("notes").unwrap(), expected);
    assert_eq!(
        view.column_identity("notes", "body").unwrap(),
        server.column_identity("tasks", "title").unwrap()
    );
}

/// Alice has cached names, then her link is replaced during a new operation.
/// Cached admission cannot finish it; only a fresh answer on the new link can.
#[test]
fn catalogue_reconnect_requires_a_new_authority_answer() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let client = open(7, alice(), false, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    drive(
        &[&client, &server],
        client.cover_catalogue_with_timeout(None, 5000).unwrap(),
    )
    .unwrap();
    let (up, _silent) = duplex();
    block_on(client.connect_upstream(up));
    let mut read = Box::pin(client.cover_catalogue_with_timeout(None, 5000).unwrap());
    for _ in 0..3 {
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tick(&[&client]);
    }
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    drive(&[&client, &server], read).unwrap();
}

/// Bob may delegate Alice's scope over his trusted server connection; Alice
/// cannot replace her authenticated claims with those of another session.
#[test]
fn catalogue_coverage_preserves_trusted_delegation_and_refuses_claim_forgery() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let relay = open(8, AuthorSubject::SYSTEM, false, false);
    let (up, down) = duplex();
    block_on(relay.connect_upstream(up));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    drive(
        &[&relay, &server],
        relay
            .cover_catalogue_with_timeout(
                Some(AcceptedReadScope {
                    author: alice(),
                    claims: claims("delegated"),
                }),
                5000,
            )
            .unwrap(),
    )
    .unwrap();
    let client = open(7, alice(), false, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert!(
        drive(
            &[&client, &server],
            client
                .cover_catalogue_with_timeout(
                    Some(AcceptedReadScope {
                        author: alice(),
                        claims: claims("forged"),
                    }),
                    5000
                )
                .unwrap()
        )
        .is_err()
    );
}

/// Alice retires her storage owner before asking for new catalogue coverage.
/// Her metadata request is refused at admission; Bob's fresh owner still works.
#[test]
fn retired_owner_refuses_catalogue_admission() {
    let alice = open(7, alice(), true, false);
    alice.retire_storage_owner().unwrap();
    let error = match alice.cover_catalogue_with_timeout(None, 5000) {
        Err(error) => error,
        Ok(_) => panic!("retired Alice admitted a new catalogue operation"),
    };
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(error.to_string().contains("retired"));

    let bob = open(8, bob(), true, false);
    block_on(bob.cover_catalogue_with_timeout(None, 5000).unwrap()).unwrap();
}

/// Alice captures a metadata operation, then retires before its first poll.
/// capture -> retire -> poll must refuse, rather than report SQLite's incidental error.
#[test]
fn captured_catalogue_future_preserves_owner_retirement() {
    let alice = open(7, alice(), true, false);
    let covering = alice.cover_catalogue_with_timeout(None, 5000).unwrap();
    alice.retire_storage_owner().unwrap();
    let error = block_on(covering).unwrap_err();
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(error.to_string().contains("retired"));
}
