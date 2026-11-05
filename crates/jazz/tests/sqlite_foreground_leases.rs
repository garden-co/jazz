//! A native host hands a transaction node between separate SQLite runtimes.
mod common;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, block_on, duplex};
use jazz::foreground_node_lease::{ForegroundNodeLease, ForegroundNodeLeasePool};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::time::TxTime;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn config(node: NodeUuid) -> DbConfig {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    DbConfig::new(
        schema,
        DbIdentity {
            node,
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn drive<T>(peers: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        for peer in peers {
            block_on(peer.tick()).unwrap();
        }
    }
    panic!("write did not settle");
}

fn connect(client: &Db, server: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
}

fn insert(client: &Db, server: &Db, text: &str) -> u64 {
    let write = block_on(client.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap();
    let seq = write.mergeable_tx_id();
    drive(
        &[client, server, client],
        client.wait_for_transaction(seq, DurabilityTier::Global),
    )
    .unwrap();
    seq
}

/// Alice closes one runtime cleanly, then Bob's fresh runtime receives its node.
/// Seeding the returned floor preserves both writes at the real authority,
/// including when the handoff includes a cancelled initialization reservation.
/// Alice -> authority -> close -> host lease pool -> Bob -> authority.
#[test]
fn clean_handoff_does_not_reuse_commits_or_cancelled_reservations() {
    for cancel_reservation in [false, true] {
        let server = block_on(Db::open_history_complete(config(NodeUuid::from_bytes(
            [9; 16],
        ))))
        .unwrap();
        let mut pool = ForegroundNodeLeasePool::default();
        let lease = pool.acquire_fresh(NodeUuid::from_bytes([1; 16])).unwrap();
        let alice = block_on(Db::open(config(lease.node))).unwrap();
        alice.seed_seq_floor(lease.confirmed_tx_time.0);
        connect(&alice, &server);
        let first = insert(&alice, &server, "alice");

        if cancel_reservation {
            let open = OpenTransactionId::new();
            block_on(alice.begin_serializable(open)).unwrap();
            let seal = alice.seal_initialization_transaction(open).unwrap();
            assert!(alice.seq_high_water() > first);
            alice.cancel_initialization_transaction(seal).unwrap();
        }
        let floor = alice.seq_high_water();
        block_on(alice.close()).unwrap();
        drop(alice);
        pool.clean_handoff(ForegroundNodeLease {
            confirmed_tx_time: TxTime(floor),
            ..lease
        })
        .unwrap();

        let reused = pool.acquire_reusable().unwrap();
        assert_eq!(reused.node, lease.node);
        assert_eq!(reused.confirmed_tx_time.0, floor);
        let bob = block_on(Db::open(config(reused.node))).unwrap();
        bob.seed_seq_floor(reused.confirmed_tx_time.0);
        connect(&bob, &server);
        assert!(insert(&bob, &server, "bob") > floor);

        let query = server.prepare_query(&Query::from("notes")).unwrap();
        let mut texts: Vec<_> = block_on(server.all(&query, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|row| row.cell_at(0).unwrap())
            .collect();
        texts.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            texts,
            vec![Value::String("alice".into()), Value::String("bob".into())],
            "the authority must apply Bob's write instead of deduplicating its reused identity"
        );
    }
}
