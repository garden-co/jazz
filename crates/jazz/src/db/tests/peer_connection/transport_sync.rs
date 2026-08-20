//! General subscription transport coverage retained while legacy branch sync is quarantined.

use super::*;

#[test]
fn db_sync_surface_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn large_logical_snapshot_crosses_byte_peer_transport_and_settles() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0x71; 16]);
    let client_author = AuthorId::from_bytes([0x72; 16]);
    let server = open_core(0x73, AuthorId::SYSTEM, &schema);
    let client = open_db(0x74, client_author, &schema);
    let expected = 900;

    for idx in 0..expected {
        seed(
            &server,
            "todos",
            cells(&format!("row-{idx}-{}", "x".repeat(4096)), false, owner),
        );
    }

    let (client_transport, server_transport) = byte_duplex_uncompressed();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    for _ in 0..200 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            let settled = event_settled(&event);
            let snapshot = snapshot_from_event(event);
            if settled {
                assert_eq!(snapshot.rows.len(), expected);
                return;
            }
        }
    }

    let rows = prepared_read(&client, &query);
    panic!(
        "large logical snapshot subscription did not settle; currently visible rows={}",
        rows.len()
    );
}
