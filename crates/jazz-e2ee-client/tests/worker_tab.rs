//! A browser tab behind its worker: the tab's database (the runtime's hooks,
//! a non-durable replica) syncs with the worker's (keyless hooks, a client of
//! the authority that serves its tabs), which syncs with the authority. The
//! tab verifies its account through that chain.

mod common;

use std::rc::Rc;

use common::*;
use jazz::db::{Db, DbConfig, DbIdentity, RemoteLinkHint, block_on};
use jazz::e2ee::EncryptionSettings;
use jazz::ids::NodeUuid;
use jazz_e2ee_client::{AccountStatus, KeylessHooks, MemoryStore};

/// Contract: a device whose database is a tab behind a worker verifies its
/// account and bootstraps it, as a device linked to the authority directly does.
#[test]
fn a_tab_behind_a_keyless_worker_verifies_its_account() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa1);
    let authority = secure_authority_with(&settings, None);
    for index in 0..5 {
        project(&authority, &format!("earlier {index}"));
    }
    let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);

    let worker = block_on(Db::open(
        DbConfig::new(
            secure_schema_with(&settings),
            DbIdentity {
                node: NodeUuid::from_bytes([7; 16]),
                author: alice,
            },
        )
        .with_e2ee(KeylessHooks::hooks(declarations_with(settings.clone()))),
    ))
    .unwrap();
    worker.defer_durability_acks();
    jazz::db::block_on(worker.set_remote_link_hint(RemoteLinkHint::Live));
    let _worker_link = link_as(&worker, &authority, alice);

    let tab = block_on(Db::open(
        DbConfig::new(
            secure_schema_with(&settings),
            DbIdentity {
                node: NodeUuid::from_bytes([8; 16]),
                author: alice,
            },
        )
        .with_e2ee(client.hooks()),
    ))
    .unwrap();
    jazz::db::block_on(tab.set_non_durable_client());
    jazz::db::block_on(tab.set_remote_link_hint(RemoteLinkHint::Live));
    let _tab_link = link_as(&tab, &worker, alice);

    for _ in 0..20 {
        block_on(tab.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(authority.tick()).unwrap();
    }
    assert!(
        jazz::db::block_on(client.prepare(&tab)).unwrap(),
        "identities through the worker"
    );
    let _watch = drive(&authority, &[&tab, &worker], client.watch_account(&tab)).unwrap();
    let status = drive(&authority, &[&tab, &worker], client.ensure_account(&tab)).unwrap();
    assert_eq!(status, AccountStatus::Active);
}

/// Runs `future` while the three databases tick in an order the seed picks,
/// as a browser's tab, worker and server do independently. `None` when it did
/// not finish.
fn drive_in_any_order<T>(
    seed: u64,
    authority: &Db,
    worker: &Db,
    tab: &Db,
    future: impl std::future::Future<Output = T>,
) -> Option<T> {
    let mut state = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as usize
    };
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..6_000 {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return Some(value);
        }
        let dbs = [tab, worker, authority];
        let mask = 1 + next() % 7;
        let mut order = [0usize, 1, 2];
        order.swap(0, next() % 3);
        order.swap(1, 1 + next() % 2);
        for i in order {
            if mask & (1 << i) != 0 {
                block_on(dbs[i].tick()).unwrap();
            }
        }
    }
    None
}

#[test]
fn a_tab_behind_a_keyless_worker_verifies_its_account_whatever_the_tick_order() {
    let settings = EncryptionSettings::default();
    let mut hung = Vec::new();
    for seed in 0..20u64 {
        let alice = account(0xa1);
        let authority = secure_authority_with(&settings, None);
        for index in 0..5 {
            project(&authority, &format!("earlier {index}"));
        }
        let client = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
        let worker = block_on(Db::open(
            DbConfig::new(
                secure_schema_with(&settings),
                DbIdentity {
                    node: NodeUuid::from_bytes([7; 16]),
                    author: alice,
                },
            )
            .with_e2ee(KeylessHooks::hooks(declarations_with(settings.clone()))),
        ))
        .unwrap();
        jazz::db::block_on(worker.set_remote_link_hint(RemoteLinkHint::Live));
        let _worker_link = link_as(&worker, &authority, alice);
        let tab = block_on(Db::open(
            DbConfig::new(
                secure_schema_with(&settings),
                DbIdentity {
                    node: NodeUuid::from_bytes([8; 16]),
                    author: alice,
                },
            )
            .with_e2ee(client.hooks()),
        ))
        .unwrap();
        jazz::db::block_on(tab.set_non_durable_client());
        jazz::db::block_on(tab.set_remote_link_hint(RemoteLinkHint::Live));
        let _tab_link = link_as(&tab, &worker, alice);
        for _ in 0..5 {
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
            block_on(authority.tick()).unwrap();
        }
        assert!(jazz::db::block_on(client.prepare(&tab)).unwrap());
        let watched =
            drive_in_any_order(seed, &authority, &worker, &tab, client.watch_account(&tab));
        let verified = watched.and_then(|_watch| {
            drive_in_any_order(seed, &authority, &worker, &tab, client.ensure_account(&tab))
        });
        match verified {
            Some(Ok(AccountStatus::Active)) => {}
            other => hung.push((
                seed,
                format!("{:?}", other.map(|r| r.map_err(|e| e.to_string()))),
            )),
        }
    }
    assert!(hung.is_empty(), "did not verify: {hung:?}");
}
