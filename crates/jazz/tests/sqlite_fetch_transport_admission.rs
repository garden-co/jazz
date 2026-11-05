//! One-shot requests bind to an admitted transport, not an unfinished handshake.
use jazz::db::{
    Db, DbConfig, DbIdentity, FirstLoad, ReadOpts, ReadTier, RemoteLinkHint, SerializedReadResult,
    block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};
fn fixture() -> (Db, Db) {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap();
    let open = |node, author| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let bob = open(2, AuthorSubject::SYSTEM);
    bob.set_authority();
    bob.initialize_authority_catalogue().unwrap();
    insert(&bob, "on the authority");
    let alice = open(1, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    (alice, bob)
}
fn insert(db: &Db, text: &str) {
    block_on(db.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap();
}
fn connect(alice: &Db, bob: &Db) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Live);
}
fn turn(alice: &Db, bob: &Db) {
    block_on(alice.tick()).unwrap();
    block_on(bob.tick()).unwrap();
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
async fn read(db: &Db, tier: ReadTier) -> Result<usize, jazz::db::Error> {
    let mut opts = ReadOpts::for_read_tier(tier);
    opts.first_load = FirstLoad::WaitForRemote { timeout_ms: 30_000 };
    let result = db
        .all_serialized_query_as(
            &postcard::to_allocvec(&Query::from("notes")).unwrap(),
            opts,
            None,
            None,
            None,
            true,
            || false,
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary query")
    };
    Ok(rows.len())
}

/// Alice opens a read while her host is connecting to Bob. First transport
/// admission must let this fresh request run, not invalidate it as a reconnect.
/// Alice: begin read -> host handshake -> attach transport -> Bob answers.
#[test]
fn first_transport_admits_waiting_local_first_and_remote_reads() {
    for tier in [ReadTier::LocalFirst, ReadTier::Remote] {
        let (alice, bob) = fixture();
        let mut pending = pin!(read(&alice, tier));
        for _ in 0..3 {
            assert!(poll(pending.as_mut()).is_pending());
            turn(&alice, &bob);
        }
        connect(&alice, &bob);
        let mut result = None;
        for _ in 0..16 {
            turn(&alice, &bob);
            if let Poll::Ready(value) = poll(pending.as_mut()) {
                result = Some(value.unwrap());
                break;
            }
        }
        assert_eq!(result, Some(1));
    }
}

/// Bob is unreachable; Alice's bounded connection attempt ends and her
/// local-first opening promptly falls back to her own pending row.
#[test]
fn failed_initial_attempt_releases_local_first_read() {
    let (alice, _bob) = fixture();
    insert(&alice, "available offline");
    let mut pending = pin!(read(&alice, ReadTier::LocalFirst));
    assert!(poll(pending.as_mut()).is_pending());
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(poll(pending.as_mut()).map(Result::unwrap), Poll::Ready(1));
}

/// Alice's preceding write starts during the handshake. Her Remote read still
/// waits for Bob's real acceptance after the transport is admitted.
#[test]
fn preconnection_write_still_precedes_remote_answer() {
    let (alice, bob) = fixture();
    insert(&alice, "written before connecting");
    let mut pending = pin!(read(&alice, ReadTier::Remote));
    assert!(poll(pending.as_mut()).is_pending());
    connect(&alice, &bob);
    let mut result = None;
    for _ in 0..24 {
        turn(&alice, &bob);
        if let Poll::Ready(value) = poll(pending.as_mut()) {
            result = Some(value.unwrap());
            break;
        }
    }
    assert_eq!(result, Some(2));
}
