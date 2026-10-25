//! Public-API port of the retired sync soak's late-parent rejection contract.
//! This focused regression does not replace the randomized sync oracle.
mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, SyncMessage, Transport, TransportError,
    UpsertOptions, Upstream, WireError, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{Fate, RejectionReason};

type Messages = Rc<RefCell<VecDeque<SyncMessage>>>;
type Origin = ([u8; 16], u64);

fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".to_owned(), Value::String(text.to_owned()))])
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("text", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn authority(path: &Path) -> Db {
    let db = block_on(Db::open_at(
        config(90, AuthorSubject::SYSTEM),
        path,
        Default::default(),
    ))
    .unwrap();
    db.set_authority();
    db.initialize_authority_catalogue().unwrap();
    db
}

struct Endpoint {
    sent: Messages,
    incoming: Messages,
}

impl Transport for Endpoint {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.incoming.borrow_mut().pop_front()
    }
}

#[derive(Default)]
struct Wire {
    from_client: Messages,
    to_client: Messages,
    from_server: Messages,
    to_server: Messages,
    held: Vec<SyncMessage>,
    fates: Vec<SyncMessage>,
}

impl Wire {
    fn attach(&self, writer: &Db, server: &Db) -> Upstream {
        let upstream = block_on(writer.connect_upstream(Box::new(Endpoint {
            sent: self.from_client.clone(),
            incoming: self.to_client.clone(),
        })));
        server.accept_subscriber(
            Box::new(Endpoint {
                sent: self.from_server.clone(),
                incoming: self.to_server.clone(),
            }),
            author("alice"),
        );
        upstream
    }

    fn pump(&mut self, writer: &Db, server: &Db) {
        block_on(writer.tick()).unwrap();
        while let Some(message) = self.from_client.borrow_mut().pop_front() {
            if matches!(message, SyncMessage::Commit { .. }) {
                // Only generated commits are held. Catalogue, subscription and
                // chunk traffic continue through the ordinary transport path.
                self.held.push(message);
            } else {
                self.to_server.borrow_mut().push_back(message);
            }
        }
        block_on(server.tick()).unwrap();
        while let Some(message) = self.from_server.borrow_mut().pop_front() {
            if matches!(message, SyncMessage::FateV1 { .. }) {
                self.fates.push(message.clone());
            }
            self.to_client.borrow_mut().push_back(message);
        }
        block_on(writer.tick()).unwrap();
    }

    fn capture_after(&mut self, writer: &Db, server: &Db, after: u64) -> SyncMessage {
        for _ in 0..100 {
            if let Some(message) = self.held.iter().find(|message| origin(message).1 > after) {
                return message.clone();
            }
            self.pump(writer, server);
        }
        panic!("writer did not emit a commit after {after}");
    }

    fn deliver(&mut self, writer: &Db, server: &Db, message: &SyncMessage) -> SyncMessage {
        let wanted = origin(message);
        let first_new_fate = self.fates.len();
        self.to_server.borrow_mut().push_back(message.clone());
        for _ in 0..100 {
            self.pump(writer, server);
            if let Some(fate) = self.fates[first_new_fate..].iter().find(|message| {
                matches!(message, SyncMessage::FateV1 { node, tx, .. } if (*node, *tx) == wanted)
            }) {
                return fate.clone();
            }
        }
        panic!("authority did not decide commit {wanted:?}");
    }
}

fn origin(message: &SyncMessage) -> Origin {
    let SyncMessage::Commit { node, tx, .. } = message else {
        panic!("expected a generated commit");
    };
    (*node, *tx)
}

fn result(fate: &SyncMessage) -> &Result<(), WireError> {
    let SyncMessage::FateV1 { result, .. } = fate else {
        panic!("expected an authority fate");
    };
    result
}

fn rows(db: &Db) -> BTreeMap<RowUuid, String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|row| {
            let text = row.cells().find_map(|(name, value)| match (name, value) {
                ("text", Some(Value::String(text))) => Some(text.clone()),
                _ => None,
            });
            (row.row_uuid(), text.expect("text column"))
        })
        .collect()
}

fn restart(writer: &Db, server: Db, upstream: Upstream, path: &Path) -> (Db, Wire, Upstream) {
    assert!(writer.detach_connection(&upstream));
    block_on(server.close()).unwrap();
    drop(server);
    let server = authority(path);
    let mut wire = Wire::default();
    let upstream = wire.attach(writer, &server);
    // Finish the real connection/catalogue exchange before injecting a replay.
    // Resent commits remain held by the same transport fault scheduler.
    for _ in 0..16 {
        wire.pump(writer, &server);
    }
    (server, wire, upstream)
}

/// Alice authors a two-row unit, then a newer patch to one of its rows.
/// Mallory delays the first upload until after the newer one is accepted.
/// Bob may write between them; an authority restart must not erase Alice's
/// accepted writer order. The entire late unit is refused, its unrelated row
/// stays absent, and both decisions remain idempotent after another restart.
///
/// alice: older(B, A) ---- held ---------------------> rejected atomically
///        newer(A) --------------> accepted
/// bob:                               optional(A)
/// authority:                            optional restart
#[test]
fn late_same_writer_unit_cannot_overwrite_its_accepted_successor() {
    for restart_before_late in [false, true] {
        for bob_intervenes in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let server_path = dir.path().join("authority.sqlite");
            let writer = block_on(Db::open_at(
                config(1, author("alice")),
                &dir.path().join("writer.sqlite"),
                Default::default(),
            ))
            .unwrap();
            writer.set_remote_link_hint(RemoteLinkHint::Failed);
            let older = block_on(writer.mergeable_tx()).unwrap();
            // The unrelated row comes first to catch a partial-unit apply.
            block_on(older.upsert("notes", row(2), cells("must-not-land"), Default::default()))
                .unwrap();
            block_on(older.upsert("notes", row(1), cells("older"), Default::default())).unwrap();
            let older_receipt = block_on(older.commit()).unwrap().0;
            let newer_receipt =
                block_on(writer.upsert("notes", row(1), cells("newer"), Default::default()))
                    .unwrap()
                    .mergeable_tx_id();

            let mut server = authority(&server_path);
            let mut wire = Wire::default();
            let mut upstream = wire.attach(&writer, &server);
            let older_message = wire.capture_after(&writer, &server, 0);
            let newer_message = wire.capture_after(&writer, &server, origin(&older_message).1);
            assert_eq!(origin(&older_message).0, origin(&newer_message).0);
            assert!(origin(&older_message).1 < origin(&newer_message).1);
            let accepted = wire.deliver(&writer, &server, &newer_message);
            assert!(result(&accepted).is_ok(), "{accepted:?}");
            assert_eq!(
                writer.write_state(newer_receipt).unwrap().fate,
                Fate::Accepted
            );
            assert_eq!(rows(&server), BTreeMap::from([(row(1), "newer".into())]));

            let final_text = if bob_intervenes {
                block_on(server.upsert(
                    "notes",
                    row(1),
                    cells("bob"),
                    UpsertOptions {
                        identity: WriteIdentity::Session(author("bob")),
                        ..Default::default()
                    },
                ))
                .unwrap();
                "bob"
            } else {
                "newer"
            };
            if restart_before_late {
                (server, wire, upstream) = restart(&writer, server, upstream, &server_path);
            }
            let rejected = wire.deliver(&writer, &server, &older_message);
            assert!(
                matches!(result(&rejected), Err(WireError::Other(_))),
                "a late same-writer unit must be malformed, not accepted: {rejected:?}"
            );
            assert!(matches!(
                writer.write_state(older_receipt).unwrap().fate,
                Fate::Rejected(RejectionReason::MalformedCommit(_))
            ));
            let expected = BTreeMap::from([(row(1), final_text.to_owned())]);
            assert_eq!(
                rows(&server),
                expected,
                "late unit must roll back every row"
            );

            (server, wire, upstream) = restart(&writer, server, upstream, &server_path);
            let rejected_again = wire.deliver(&writer, &server, &older_message);
            assert_eq!(
                postcard::to_allocvec(result(&rejected_again)).unwrap(),
                postcard::to_allocvec(result(&rejected)).unwrap()
            );
            let accepted_again = wire.deliver(&writer, &server, &newer_message);
            assert_eq!(
                postcard::to_allocvec(&accepted_again).unwrap(),
                postcard::to_allocvec(&accepted).unwrap()
            );
            assert_eq!(rows(&server), expected, "replays cannot rewrite the row");
            assert!(writer.detach_connection(&upstream));
            block_on(writer.close()).unwrap();
            block_on(server.close()).unwrap();
        }
    }
}
