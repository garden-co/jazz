//! Read-your-writes across independently scheduled channels (SPEC 8 §8.2.1,
//! #3863).
//!
//! A writer's commit units and its requests travel on separately credited
//! channels, so a later read can reach the authority before an earlier
//! write. These tests withhold a writer's uploads at the link to force that
//! order.

use super::*;
use crate::model::test_support::AllowAll;

fn music_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("tracks").column("title", PublicColumnType::Text))
            .allow_all(),
    )
}

fn title(value: impl Into<String>) -> RowCells {
    BTreeMap::from([("title".to_owned(), Value::String(value.into()))])
}

fn position(messages: &[SyncMessage], matches: impl Fn(&SyncMessage) -> bool) -> Option<usize> {
    messages.iter().position(matches)
}

/// Alice adds a track and then opens a remote read of all tracks. Credit
/// delays her write, so Core receives the read first. Core holds the read
/// until it has decided her write, and serves it only after sending her the
/// write's fate. Core handles a link's input in order, so the read it then
/// serves already includes the write.
#[test]
fn a_read_that_overtakes_its_write_is_served_after_the_write() {
    let schema = music_schema();
    let author = AuthorSubject::for_test_bytes([0x71; 16]);
    let core = open_core(0x72, AuthorSubject::SYSTEM, &schema);
    let alice = open_db(0x71, author, &schema);
    let (alice_transport, core_transport, alice_sent, core_sent) = duplex_with_taps();
    let _upstream = crate::local_executor::block_on(alice.connect_upstream(alice_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    for _ in 0..8 {
        alice.tick().unwrap();
        core.tick().unwrap();
    }
    alice.tick().unwrap();

    let written =
        crate::local_executor::block_on(alice.insert("tracks", title("mine"), Default::default()))
            .unwrap();
    let attachment = alice
        .attach_query_with_opts(
            &prepared(&alice, &Query::from("tracks")),
            global_subscribe_opts(),
        )
        .unwrap();
    let is_subscribe = |message: &SyncMessage| {
        matches!(message, SyncMessage::Subscribe(subscribe)
            if subscribe.subscription == attachment.subscription())
    };
    for _ in 0..4 {
        if alice_sent.borrow().iter().any(is_subscribe) {
            break;
        }
        alice.tick().unwrap();
    }
    let sent = alice_sent.borrow_mut().drain(..).collect::<Vec<_>>();

    // Alice's link puts the write first, then a barrier naming it, then
    // her requests.
    let is_write = |message: &SyncMessage| matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == written.tx_id);
    let write = position(&sent, is_write).expect("alice uploads her write");
    let barrier = position(&sent, |message| {
        matches!(message, SyncMessage::ReadBarrier { writes_through } if *writes_through >= 1)
    })
    .expect("a barrier precedes alice's requests");
    let subscribe = position(&sent, is_subscribe).expect("alice opens her read");
    assert!(write < barrier && barrier < subscribe, "{sent:#?}");

    // Credit delays the write: Core receives only the requests.
    let (writes, requests): (Vec<_>, Vec<_>) = sent.into_iter().partition(is_write);
    core_sent.borrow_mut().clear();
    alice_sent.borrow_mut().extend(requests);
    for _ in 0..4 {
        core.tick().unwrap();
    }
    let is_view = |message: &SyncMessage| {
        matches!(message, SyncMessage::ViewUpdate(view)
            if view.subscription == attachment.subscription())
    };
    assert!(
        !core_sent.borrow().iter().any(is_view),
        "Core must not serve the read before deciding the write it follows"
    );

    alice_sent.borrow_mut().extend(writes);
    for _ in 0..4 {
        core.tick().unwrap();
    }
    let served = core_sent.borrow().iter().cloned().collect::<Vec<_>>();
    let fate = position(&served, |message| {
        matches!(message, SyncMessage::FateUpdate { tx_id, fate: Fate::Accepted, .. }
            if *tx_id == written.tx_id)
    })
    .expect("Core accepts alice's write");
    let view = position(&served, is_view).expect("Core serves the held read");
    assert!(fate < view, "{served:#?}");
}

/// Bob reads without having written on this link. His requests carry no
/// barrier and Core serves them at once.
#[test]
fn requests_without_preceding_writes_carry_no_barrier() {
    let schema = music_schema();
    let author = AuthorSubject::for_test_bytes([0x73; 16]);
    let core = open_core(0x74, AuthorSubject::SYSTEM, &schema);
    let bob = open_db(0x73, author, &schema);
    let (bob_transport, core_transport, bob_sent, core_sent) = duplex_with_taps();
    let _upstream = crate::local_executor::block_on(bob.connect_upstream(bob_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let attachment = bob
        .attach_query_with_opts(
            &prepared(&bob, &Query::from("tracks")),
            global_subscribe_opts(),
        )
        .unwrap();
    bob.tick().unwrap();
    assert!(
        !bob_sent
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ReadBarrier { .. }))
    );
    for _ in 0..4 {
        core.tick().unwrap();
    }
    assert!(core_sent.borrow().iter().any(|message| {
        matches!(message, SyncMessage::ViewUpdate(view)
            if view.subscription == attachment.subscription())
    }));
}
