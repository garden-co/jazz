use super::*;

#[test]
fn remove_client_cleans_outbox_entries() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);

    let row = visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice");
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(alice),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: row.clone(),
        },
    });
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(bob),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: row.clone(),
        },
    });
    let server_id = ServerId::new();
    sm.outbox.push(OutboxEntry {
        destination: Destination::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        },
    });

    sm.remove_client(alice);

    assert_eq!(sm.outbox.len(), 2);
    assert!(sm.outbox.iter().all(|entry| match entry.destination {
        Destination::Client(id) => id != alice,
        Destination::Server(_) => true,
    }));
}

#[test]
fn remove_client_skips_when_inbox_entries_exist() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    add_client(&mut sm, &io, alice);

    sm.push_inbox(InboxEntry {
        source: Source::Client(alice),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice"),
        },
    });

    assert!(!sm.remove_client(alice));
    assert!(sm.get_client(alice).is_some());
}
