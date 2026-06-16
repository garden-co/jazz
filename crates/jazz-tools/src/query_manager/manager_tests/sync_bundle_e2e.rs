use super::*;

/// A composed bundle seeds a cold client's store so a plain local subscription
/// returns the server's rows with no live sync round-trip.
#[test]
fn composed_bundle_seeds_cold_client_rows() {
    use crate::sync_bundle::{apply_query_bundle, compose_query_bundle};

    let schema = test_schema();

    // Server holding three users.
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema.clone());
    for (name, score) in [("Alice", 75), ("Bob", 30), ("Charlie", 90)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    // Compose a bundle for every user.
    let server_query = server.query("users").build();
    let bundle = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    // Cold client: empty store, never synced.
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema);
    apply_query_bundle(&mut client, &mut client_io, &bundle);

    // A plain local subscription now returns the seeded rows.
    let sub_id = client.subscribe(client.query("users").build()).unwrap();
    client.process(&mut client_io);

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        3,
        "cold client should see all three seeded users"
    );

    let names: Vec<_> = results
        .iter()
        .filter_map(|(_, row)| match &row[0] {
            Value::Text(name) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

/// After a bundle seeds a cold client, connecting live sync to the origin server
/// reconciles as a content-addressed no-op: the rows are byte-faithful, so the
/// server recognises the client's replay rather than duplicating or rejecting it.
#[test]
fn live_sync_after_bundle_reconciles_without_churn() {
    use crate::sync_bundle::{apply_query_bundle, compose_query_bundle};
    use crate::sync_manager::{ClientId, Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let schema = test_schema();

    // Server holding three users.
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema.clone());
    for (name, score) in [("Alice", 75), ("Bob", 30), ("Charlie", 90)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    // Compose and apply to a cold client, as an authenticated user would prefetch.
    let session = PolicySession::new("alice");
    let server_query = server.query("users").build();
    let bundle = compose_query_bundle(
        &mut server,
        &mut server_io,
        server_query,
        Some(session.clone()),
    );
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema.clone());
    apply_query_bundle(&mut client, &mut client_io, &bundle);

    // Connect live sync to the same server and subscribe to the same query, under
    // the same session the bundle was composed for.
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    // A read-only client already holds its catalogue (from the bundle) and does
    // not author schema, so it connects without pushing the catalogue upstream.
    client
        .sync_manager_mut()
        .add_server_with_storage(server_id, true, &client_io);
    connect_client(&mut server, &server_io, client_id);
    server
        .sync_manager_mut()
        .set_client_session(client_id, session.clone());

    let sub_id = client
        .subscribe_with_sync(client.query("users").build(), Some(session), None)
        .unwrap();

    // Before any live delivery, the subscription already returns the bundle's
    // rows — this is the seed that must survive sync-connect without a flash.
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        3,
        "the bundle should seed all three rows before live sync delivers anything"
    );

    // Exchange messages, watching for any rejection or error reaching the client.
    let mut rejected_or_errored = false;
    let mut first_problem: Option<String> = None;
    for _ in 0..16 {
        let to_server: Vec<_> = client
            .sync_manager_mut()
            .take_outbox()
            .into_iter()
            .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
            .collect();
        for entry in to_server {
            server.sync_manager_mut().push_inbox(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
        server.process(&mut server_io);

        let to_client: Vec<_> = server
            .sync_manager_mut()
            .take_outbox()
            .into_iter()
            .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
            .collect();
        if to_client.is_empty() {
            break;
        }
        for entry in to_client {
            if matches!(
                entry.payload,
                SyncPayload::BatchFate {
                    fate: crate::batch_fate::BatchFate::Rejected { .. }
                } | SyncPayload::Error(_)
            ) {
                rejected_or_errored = true;
                if first_problem.is_none() {
                    first_problem = Some(format!("{:?}", entry.payload));
                }
            }
            client.sync_manager_mut().push_inbox(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
        client.process(&mut client_io);
    }

    assert!(
        !rejected_or_errored,
        "live reconciliation must not reject or error on the byte-faithful replay; first problem: {first_problem:?}"
    );

    // The client still sees exactly the three rows — no duplication.
    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        3,
        "live sync should reconcile to the same three rows, not duplicate them"
    );

    // The server was not made to duplicate rows either.
    let server_sub = server.subscribe(server.query("users").build()).unwrap();
    server.process(&mut server_io);
    assert_eq!(
        server.get_subscription_results(server_sub).len(),
        3,
        "the server's own row set is unchanged by the replay"
    );
}

/// A bundle composed for one principal carries only the rows that principal may
/// read — the composer drives the server's own policy evaluation, so another
/// owner's rows never reach the cold client's store.
#[test]
fn composed_bundle_is_permission_filtered() {
    use crate::sync_bundle::{apply_query_bundle, compose_query_bundle};

    let columns = || {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("owner_id", ColumnType::Text),
        ])
    };

    let mut structural_schema = Schema::new();
    structural_schema.insert(TableName::new("documents"), TableSchema::new(columns()));

    let mut authorization_schema = Schema::new();
    authorization_schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            columns(),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    );

    // Server holding documents owned by two different users.
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), structural_schema);
    server.set_authorization_schema(authorization_schema.clone());
    for (title, owner) in [("Alice doc", "alice"), ("Bob doc", "bob")] {
        server
            .insert(
                &mut server_io,
                "documents",
                &[Value::Text(title.into()), Value::Text(owner.into())],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    // Compose a bundle for Alice's session only.
    let server_query = server.query("documents").build();
    let bundle = compose_query_bundle(
        &mut server,
        &mut server_io,
        server_query,
        Some(PolicySession::new("alice")),
    );

    // Cold client seeded from Alice's bundle sees Alice's row — and only Alice's.
    let (mut client, mut client_io) =
        create_query_manager(SyncManager::new(), authorization_schema);
    apply_query_bundle(&mut client, &mut client_io, &bundle);

    let sub_id = client.subscribe(client.query("documents").build()).unwrap();
    client.process(&mut client_io);

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "only Alice's document should reach the bundle, not Bob's"
    );
    assert_eq!(results[0].1[0], Value::Text("Alice doc".into()));
}

/// A composed bundle survives serialisation to bytes and back, so it can cross
/// the server→client boundary on the wire and still seed a cold client's store.
#[test]
fn bundle_survives_serialisation_round_trip() {
    use crate::sync_bundle::{SyncBundle, apply_query_bundle, compose_query_bundle};

    let schema = test_schema();
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema.clone());
    for (name, score) in [("Alice", 75), ("Bob", 30), ("Charlie", 90)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let server_query = server.query("users").build();
    let bundle = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    // Round-trip the envelope through bytes, as it would travel to the client.
    let bytes = serde_json::to_vec(&bundle).expect("bundle serialises");
    let restored: SyncBundle = serde_json::from_slice(&bytes).expect("bundle deserialises");
    assert_eq!(restored.version(), bundle.version());

    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema);
    apply_query_bundle(&mut client, &mut client_io, &restored);

    let sub_id = client.subscribe(client.query("users").build()).unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        3,
        "a deserialised bundle seeds the same rows"
    );
}

/// A client whose storage started completely empty — a first visit with empty
/// OPFS, the primary SSR-hydration case — is fully seeded from the bundle alone,
/// with no pre-existing local state to fall back on.
#[test]
fn bundle_seeds_a_client_with_empty_storage() {
    use crate::sync_bundle::{apply_query_bundle, compose_query_bundle};

    let schema = test_schema();
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema.clone());
    for (name, score) in [("Alice", 75), ("Bob", 30), ("Charlie", 90)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let server_query = server.query("users").build();
    let bundle = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    // Cold-storage client: the app schema is known in memory, but nothing has
    // been persisted — exactly a browser's first visit with empty OPFS.
    let mut client = QueryManager::new(SyncManager::new());
    client.set_current_schema(schema, "dev", "main");
    let mut client_io = MemoryStorage::new();

    apply_query_bundle(&mut client, &mut client_io, &bundle);

    let sub_id = client.subscribe(client.query("users").build()).unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        3,
        "the bundle seeds a client whose storage started empty"
    );
}

/// Composing a bundle must leave the server exactly as it found it — the
/// synthetic subscription used to drive delivery is fully reaped, not left to
/// re-settle on every later tick of a long-lived server.
#[test]
fn compose_leaves_no_server_subscription_behind() {
    use crate::sync_bundle::compose_query_bundle;

    let schema = test_schema();
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema);
    for (name, score) in [("Alice", 75), ("Bob", 30)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let server_query = server.query("users").build();
    let _ = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    // A later tick must not resurrect a synthetic subscription.
    server.process(&mut server_io);
    assert!(
        server.server_subscription_telemetry().is_empty(),
        "compose must not leave a synthetic subscription on the server"
    );
}

/// Composing a bundle must not disturb outbound sync already queued for other
/// peers — on a shared server runtime the composer's synthetic harvest has to
/// leave every other destination's entries in place.
#[test]
fn compose_preserves_outbox_entries_for_other_peers() {
    use crate::sync_bundle::compose_query_bundle;
    use crate::sync_manager::types::OutboxEntry;
    use crate::sync_manager::{ClientId, Destination, SyncPayload};
    use uuid::Uuid;

    let schema = test_schema();
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema);
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server.process(&mut server_io);

    // A real peer already has outbound traffic queued.
    let other_client = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    server.sync_manager_mut().prepend_outbox(vec![OutboxEntry {
        destination: Destination::Client(other_client),
        payload: SyncPayload::BatchFateNeeded { batch_ids: vec![] },
    }]);

    let server_query = server.query("users").build();
    let _ = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    let survived = server
        .sync_manager()
        .outbox()
        .iter()
        .any(|e| matches!(e.destination, Destination::Client(id) if id == other_client));
    assert!(
        survived,
        "compose must leave another peer's queued outbox entry intact"
    );
}

/// The wire helpers round-trip a bundle through bytes (postcard) and still seed
/// a cold client — this is the exact form that crosses the WASM/NAPI boundary.
#[test]
fn bundle_wire_bytes_round_trip_seeds_cold_client() {
    use crate::sync_bundle::{SyncBundle, apply_query_bundle, compose_query_bundle};

    let schema = test_schema();
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema.clone());
    for (name, score) in [("Alice", 75), ("Bob", 30)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let server_query = server.query("users").build();
    let bundle = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    let bytes = bundle.to_bytes().expect("bundle serialises to wire bytes");
    let restored = SyncBundle::from_bytes(&bytes).expect("wire bytes decode");

    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema);
    apply_query_bundle(&mut client, &mut client_io, &restored);

    let sub_id = client.subscribe(client.query("users").build()).unwrap();
    client.process(&mut client_io);
    assert_eq!(client.get_subscription_results(sub_id).len(), 2);
}

/// A bundle whose envelope version this build does not recognise is rejected,
/// rather than mis-applied.
#[test]
fn bundle_from_bytes_rejects_an_unknown_version() {
    use crate::sync_bundle::SyncBundle;
    use crate::sync_bundle::{SyncBundleError, compose_query_bundle};

    let schema = test_schema();
    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema);
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server.process(&mut server_io);

    let server_query = server.query("users").build();
    let bundle = compose_query_bundle(&mut server, &mut server_io, server_query, None);

    // The version is the first field of the envelope; bump it past what this
    // build understands.
    let mut bytes = bundle.to_bytes().unwrap();
    bytes[0] = 99;

    assert!(matches!(
        SyncBundle::from_bytes(&bytes),
        Err(SyncBundleError::UnsupportedVersion(99))
    ));
}
