//! Pending operations must stop waiting when their storage owner retires.
use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, ReadTier, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn alice_config() -> DbConfig {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([91; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn alice() -> Db {
    block_on(Db::open(alice_config())).unwrap()
}

/// Alice waits for Bob's remote answer, then her host retires the store:
/// read --> pending --> retire --> storage error, without a further server tick.
#[test]
fn pending_remote_read_rejects_after_owner_retirement() {
    let alice = alice();
    let (client, _bob) = duplex();
    let _link = block_on(alice.connect_upstream(client));
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = pin!(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    alice.retire_storage_owner().unwrap();
    assert!(
        matches!(read.as_mut().poll(&mut cx), Poll::Ready(Err(error)) if error.code == ErrorCode::Storage)
    );
}

/// Alice's snapshot waits for Bob to settle her write. Retiring Alice's store
/// must reject the snapshot rather than leaving it waiting on a revoked owner.
#[test]
fn pending_snapshot_rejects_after_owner_retirement() {
    let alice = alice();
    let (client, _bob) = duplex();
    let _link = block_on(alice.connect_upstream(client));
    block_on(alice.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("pending".into()))]),
        Default::default(),
    ))
    .unwrap();
    let mut snapshot = pin!(alice.capture_snapshot());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(snapshot.as_mut().poll(&mut cx).is_pending());
    alice.retire_storage_owner().unwrap();
    assert!(
        matches!(snapshot.as_mut().poll(&mut cx), Poll::Ready(Err(error)) if error.code == ErrorCode::Storage)
    );
}

/// Alice's outstanding permission preflight becomes unknown when its retired
/// owner can no longer receive Bob's answer.
#[test]
fn pending_advice_becomes_unknown_after_owner_retirement() {
    use jazz::protocol::{PermissionAdvice, PermissionAdviceAction};
    let alice = alice();
    let (client, _bob) = duplex();
    let _link = block_on(alice.connect_upstream(client));
    let id = alice.request_permission_advice(
        PermissionAdviceAction::Insert {
            table: "notes".into(),
            cells: BTreeMap::from([("text".into(), Value::String("probe".into()))]),
        },
        None,
    );
    let mut advice = pin!(alice.permission_advice(id));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(advice.as_mut().poll(&mut cx).is_pending());
    alice.retire_storage_owner().unwrap();
    assert!(matches!(
        advice.as_mut().poll(&mut cx),
        Poll::Ready(PermissionAdvice::Unknown)
    ));
}

/// Alice's host stream closes when its owner retires, including while waiting
/// for the next delta. This uses the shared binding API that drives browser
/// streams because the core's try_next_event does not register a waiter.
#[test]
fn waiting_host_subscription_closes_after_owner_retirement() {
    use jazz::binding_host::Binding;
    use jazz::db::SubscriptionEvent;
    let binding = Binding::new(
        alice(),
        DbIdentity {
            node: NodeUuid::from_bytes([91; 16]),
            author: AuthorSubject::SYSTEM,
        },
        true,
    );
    let query = binding.db.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(
        binding
            .db
            .subscribe(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)),
    )
    .unwrap();
    assert!(matches!(
        stream.try_next_event(),
        Some(SubscriptionEvent::Delta { .. })
    ));
    let mut next = pin!(binding.next_event(&mut stream));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(next.as_mut().poll(&mut cx).is_pending());
    binding.db.retire_storage_owner().unwrap();
    assert!(matches!(
        next.as_mut().poll(&mut cx),
        Poll::Ready(SubscriptionEvent::Closed)
    ));
}

/// Alice's second accepted read queues behind her first unpolled read. Closing
/// the store must release that queue even if the first future stays alive.
#[test]
fn queued_paired_read_rejects_after_owner_retirement() {
    use jazz::tools::OpenTransactionId;
    let alice = block_on(Db::open_history_complete(alice_config())).unwrap();
    let transaction = OpenTransactionId::new();
    block_on(alice.begin_serializable(transaction)).unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let first = alice
        .all_settled(
            query.clone(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            transaction,
            false,
            None,
        )
        .unwrap();
    let mut second = pin!(
        alice
            .all_settled(
                query,
                ReadOpts::for_read_tier(ReadTier::LocalOnly),
                transaction,
                false,
                None
            )
            .unwrap()
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    alice.retire_storage_owner().unwrap();
    assert!(matches!(second.as_mut().poll(&mut cx), Poll::Ready(Err(_))));
    drop(first);
}

/// Alice closes her foreground while it is waiting for its private owner's
/// handshake. Owner admission must terminate without receiving that handshake.
#[test]
fn pending_private_owner_admission_rejects_after_retirement() {
    let mut config = alice_config();
    config.identity.author =
        AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let alice = block_on(Db::open(config)).unwrap();
    alice.set_private_owner_client().unwrap();
    let (client, _owner) = duplex();
    let _link = block_on(alice.connect_upstream(client));
    let mut ready = pin!(alice.wait_for_storage_owner());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(ready.as_mut().poll(&mut cx).is_pending());
    alice.retire_storage_owner().unwrap();
    assert!(
        matches!(ready.as_mut().poll(&mut cx), Poll::Ready(Err(error)) if error.code == ErrorCode::Storage)
    );
}
