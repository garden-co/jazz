//! Settlement of tier-gated reads across relay nodes and upstream
//! connection states: a mid-chain relay must never present its cold store as
//! an authority answer, while nodes without any upstream settle locally.

/// Deterministic tick-driven topology tests through the public `Db` API.
///
/// These contracts distinguish "the upstream has not answered yet" from
/// "there is no upstream", which requires exact control over when each hop
/// processes its inbox. The embedded server harness cannot withhold a single
/// hop's processing, so these tests drive `Db` nodes over in-memory duplex
/// transports the same way `crates/jazz/tests/browser_relay_durability.rs`
/// does.
mod relay_topology {
    use std::collections::BTreeMap;

    use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
    use jazz::groove::records::Value;
    use jazz::groove::storage::MemoryStorage;
    use jazz::ids::{AuthorSubject, NodeUuid};
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchema};
    use jazz::tx::DurabilityTier;
    use jazz_testkit::duplex_transport::duplex;

    fn schema() -> JazzSchema {
        let source = SchemaBuilder::new()
            .table(TableSchema::builder("documents").column("title", ColumnType::Text))
            .build();
        JazzSchema::new(&source).expect("replica settlement public schema compiles")
    }

    fn open_db(node: u8, author: AuthorSubject) -> Db<MemoryStorage> {
        let schema = schema();
        let column_families = schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        block_on(Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .expect("open database")
    }

    fn open_authority(node: u8) -> Db<MemoryStorage> {
        let schema = schema();
        let column_families = schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        block_on(Db::open_history_complete(DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open authority database")
    }

    fn document_cells(title: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))])
    }

    fn drain(subscription: &mut jazz::db::SubscriptionStream) -> Vec<SubscriptionEvent> {
        std::iter::from_fn(|| subscription.try_next_event()).collect()
    }

    fn tick(db: &Db<MemoryStorage>, context: &str) {
        block_on(db.tick()).expect(context);
    }

    fn has_settled_event(events: &[SubscriptionEvent]) -> bool {
        events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. }))
    }

    /// A relay whose cold store lags its connected upstream must not settle a
    /// downstream authority-tier subscription from that store; the settled
    /// answer arrives only once the upstream serves it, and carries the
    /// upstream's row rather than an authoritative-looking empty result.
    ///
    /// Actors: bob seeds the authority; alice subscribes through the relay.
    ///
    /// ```text
    /// alice ──subscribe(Global)──► relay ──forward──► authority (has bob's row)
    ///   ▲            │ cold store: must not answer settled-empty
    ///   └────settled row────◄──────────────settled──┘
    /// ```
    #[test]
    fn relay_holds_downstream_settlement_until_upstream_frontier_confirms() {
        let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
        let bob = AuthorSubject::for_test_bytes([0xb1; 16]);
        let client = open_db(0x11, alice);
        let relay = open_db(0x21, alice);
        let authority = open_authority(0x31);

        // Seed the authority through an independent client so the relay's
        // store stays cold.
        let seeder = open_db(0x41, bob);
        let (seeder_transport, authority_seed_transport) = duplex();
        let _seeder_connection = block_on(seeder.connect_upstream(seeder_transport));
        let _authority_seed_subscriber = authority.accept_subscriber(authority_seed_transport, bob);
        let seeded = block_on(seeder.insert(
            "documents",
            document_cells("settled at the authority"),
            Default::default(),
        ))
        .expect("seed authority document");
        tick(&seeder, "upload seeded document");
        tick(&authority, "accept seeded document");
        tick(&seeder, "apply seeded document fate");

        let (client_transport, relay_subscriber_transport) = duplex();
        let _client_connection = block_on(client.connect_upstream(client_transport));
        let _relay_subscriber = relay.accept_subscriber(relay_subscriber_transport, alice);
        let (relay_upstream_transport, authority_transport) = duplex();
        let _relay_upstream = block_on(relay.connect_upstream(relay_upstream_transport));
        let _authority_subscriber = authority.accept_subscriber(authority_transport, alice);

        let documents = client
            .prepare_query(&client.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(client.subscribe(
            &documents,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe at Global through the relay");
        assert!(
            subscription.try_next_event().is_none(),
            "an authority-tier subscription must withhold its provisional local snapshot"
        );

        // The authority never processes during this phase, so any settled
        // event could only come from the relay's cold store.
        for _ in 0..3 {
            tick(&client, "send subscription toward the relay");
            tick(&relay, "forward coverage without settling");
            tick(&client, "apply any relay response");
        }
        let premature = drain(&mut subscription);
        assert!(
            !has_settled_event(&premature),
            "the relay must not settle the subscription from its cold store \
             before its upstream confirms the result: {premature:?}"
        );

        let mut events = premature;
        for _ in 0..4 {
            tick(&authority, "serve the authority result");
            tick(&relay, "apply and relay the authority result");
            tick(&client, "apply the relayed authority result");
            events.extend(drain(&mut subscription));
        }
        let first_settled = events
            .iter()
            .find_map(|event| match event {
                SubscriptionEvent::Delta {
                    settled: true,
                    added,
                    ..
                } => Some(added),
                _ => None,
            })
            .expect("the relayed settlement must reach the downstream subscription");
        assert!(
            first_settled
                .iter()
                .any(|row| row.row.row_uuid() == seeded.row_uuid()),
            "the first settled answer must carry the authority row, never a \
             settled-empty cold-store answer: {events:?}"
        );
    }

    /// A relay whose upstream transport is installed but never answers (the
    /// slow-handshake case) still holds downstream settlement: the downstream
    /// subscription reads as still-loading, not definitely-empty. Once the
    /// upstream finally attaches and confirms, the settlement is relayed.
    ///
    /// Actors: alice subscribes through the relay; the authority attaches to
    /// the relay's already-installed transport only later.
    ///
    /// ```text
    /// alice ──subscribe(Global)──► relay ──queued──► (unattached upstream)
    ///                                │ later: authority attaches, confirms
    ///   ◄───────settled-empty───────┘
    /// ```
    #[test]
    fn relay_with_installed_but_unanswered_upstream_holds_downstream_settlement() {
        let alice = AuthorSubject::for_test_bytes([0xa2; 16]);
        let client = open_db(0x12, alice);
        let relay = open_db(0x22, alice);
        let authority = open_authority(0x32);

        let (client_transport, relay_subscriber_transport) = duplex();
        let _client_connection = block_on(client.connect_upstream(client_transport));
        let _relay_subscriber = relay.accept_subscriber(relay_subscriber_transport, alice);

        // Install the relay's upstream transport but leave the far end
        // unattached so the handshake never completes during the hold phase.
        let (relay_upstream_transport, held_authority_transport) = duplex();
        let _relay_upstream = block_on(relay.connect_upstream(relay_upstream_transport));

        let documents = client
            .prepare_query(&client.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(client.subscribe(
            &documents,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe at Global through the relay");

        for _ in 0..3 {
            tick(&client, "send subscription toward the relay");
            tick(&relay, "queue coverage for the silent upstream");
            tick(&client, "apply any relay response");
        }
        let premature = drain(&mut subscription);
        assert!(
            !has_settled_event(&premature),
            "an installed but unanswered upstream must keep downstream \
             settlement on hold: {premature:?}"
        );

        let _authority_subscriber = authority.accept_subscriber(held_authority_transport, alice);
        let mut events = premature;
        for _ in 0..4 {
            tick(&authority, "process the queued handshake");
            tick(&relay, "apply and relay the authority result");
            tick(&client, "apply the relayed authority result");
            events.extend(drain(&mut subscription));
        }
        assert!(
            events.iter().any(|event| matches!(
                event,
                SubscriptionEvent::Delta {
                    settled: true,
                    added,
                    ..
                } if added.is_empty()
            )),
            "once the upstream confirms, the settled (empty) answer must be \
             relayed downstream: {events:?}"
        );
    }

    /// A relay with no upstream at all settles downstream local-tier reads
    /// from its own store: nothing will ever replay, so the local answer is
    /// the best available and must not load forever.
    ///
    /// Actors: alice opens a fresh non-durable node against a relay that
    /// already holds one document.
    ///
    /// ```text
    /// alice ──subscribe(Local)──► relay (no upstream, one stored row)
    ///   ◄──truthful opening + hydrated row──┘
    /// ```
    #[test]
    fn relay_without_any_upstream_settles_downstream_local_reads() {
        let alice = AuthorSubject::for_test_bytes([0xa3; 16]);
        let relay = open_db(0x23, AuthorSubject::SYSTEM);
        block_on(relay.insert(
            "documents",
            document_cells("stored before alice opens"),
            Default::default(),
        ))
        .expect("seed relay document");

        let client = open_db(0x13, alice);
        client.set_non_durable_client();
        let (client_transport, relay_transport) = duplex();
        let _client_connection = block_on(client.connect_upstream(client_transport));
        let _relay_subscriber = relay.accept_subscriber(relay_transport, alice);

        let documents = client
            .prepare_query(&client.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(client.subscribe(&documents, ReadOpts::default()))
            .expect("subscribe locally through the relay");
        let opening = subscription.try_next_event();
        assert!(
            matches!(
                opening,
                Some(SubscriptionEvent::Delta {
                    reset: true,
                    tier: DurabilityTier::Local,
                    ..
                })
            ),
            "a local-tier subscription must publish its truthful opening \
             immediately instead of waiting for an upstream that does not \
             exist: {opening:?}"
        );

        for _ in 0..3 {
            tick(&client, "request the relay's stored view");
            tick(&relay, "serve the stored view");
            tick(&client, "apply the stored view");
        }
        let events = drain(&mut subscription);
        assert!(
            events.iter().any(|event| matches!(
                event,
                SubscriptionEvent::Delta { added, .. } if added.len() == 1
            )),
            "the upstream-less relay must serve its stored row: {events:?}"
        );
    }

    /// One-shot local reads resolve from the node's own store even while an
    /// authority-tier subscription on the same query is held for a silent
    /// upstream: one-shot local reads are never forwarded upstream, so no
    /// relayed settlement would ever resolve them.
    ///
    /// Actors: alice holds a Global subscription while reading locally.
    #[test]
    fn one_shot_local_reads_resolve_while_authority_subscription_is_held() {
        let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
        let node = open_db(0x14, alice);
        let (upstream_transport, _held_far_end) = duplex();
        let _upstream = block_on(node.connect_upstream(upstream_transport));

        let written = block_on(node.insert(
            "documents",
            document_cells("locally visible"),
            Default::default(),
        ))
        .expect("insert local document");

        let documents = node
            .prepare_query(&node.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(node.subscribe(
            &documents,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe at Global with a silent upstream");
        tick(&node, "queue coverage for the silent upstream");
        assert!(
            !has_settled_event(&drain(&mut subscription)),
            "the authority-tier subscription must stay held while the \
             upstream never answers"
        );

        let rows = node.read(&documents).expect("one-shot local read");
        assert_eq!(rows.len(), 1, "the local read must resolve immediately");
        assert_eq!(rows[0].row_uuid(), written.row_uuid());
        let one = node.one(&documents).expect("one-shot local single read");
        assert_eq!(
            one.map(|row| row.row_uuid()),
            Some(written.row_uuid()),
            "single-row local reads must also resolve immediately"
        );
    }

    /// A node's own authority-tier subscription with an installed but
    /// unanswered upstream stays held instead of pre-settling from the cold
    /// local store; attaching the upstream later delivers the settled answer.
    ///
    /// Actors: alice's node; the authority attaches only later.
    #[test]
    fn node_subscription_with_unanswered_upstream_holds_authority_tier() {
        let alice = AuthorSubject::for_test_bytes([0xa5; 16]);
        let node = open_db(0x15, alice);
        let authority = open_authority(0x35);

        let (upstream_transport, held_authority_transport) = duplex();
        let _upstream = block_on(node.connect_upstream(upstream_transport));

        let documents = node
            .prepare_query(&node.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(node.subscribe(
            &documents,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe at Global with a silent upstream");

        for _ in 0..3 {
            tick(&node, "queue coverage for the silent upstream");
        }
        let premature = drain(&mut subscription);
        assert!(
            premature.is_empty(),
            "an unanswered upstream must keep the authority-tier subscription \
             pending: {premature:?}"
        );

        let _authority_subscriber = authority.accept_subscriber(held_authority_transport, alice);
        let mut events = Vec::new();
        for _ in 0..4 {
            tick(&authority, "process the queued handshake");
            tick(&node, "apply the authority result");
            events.extend(drain(&mut subscription));
        }
        assert!(
            events
                .iter()
                .any(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. })),
            "the settled answer must arrive once the upstream confirms: {events:?}"
        );
    }

    /// A node with no upstream at all settles its own local-tier subscription
    /// immediately from its store: nothing will replay, so the local answer
    /// must not load forever.
    ///
    /// Actors: alice's stand-alone node with one stored document.
    #[test]
    fn node_subscription_without_any_upstream_settles_locally() {
        let alice = AuthorSubject::for_test_bytes([0xa6; 16]);
        let node = open_db(0x16, alice);
        let written = block_on(node.insert(
            "documents",
            document_cells("settles locally"),
            Default::default(),
        ))
        .expect("insert local document");

        let documents = node
            .prepare_query(&node.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(node.subscribe(&documents, ReadOpts::default()))
            .expect("subscribe locally without any upstream");
        let opening = subscription.try_next_event();
        let Some(SubscriptionEvent::Delta {
            reset: true,
            added,
            tier: DurabilityTier::Local,
            ..
        }) = opening
        else {
            panic!("the local opening must be delivered immediately: {opening:?}");
        };
        assert_eq!(added.len(), 1, "the opening must carry the stored row");
        assert_eq!(added[0].row.row_uuid(), written.row_uuid());
    }

    /// A client with a connected upstream must not pre-settle an
    /// authority-tier subscription from its own store: nothing is delivered
    /// until the upstream serves the result, and the settled initial answer
    /// may then be empty. Exactly one settled reset arrives.
    ///
    /// Actors: alice's client connected directly to the authority.
    ///
    /// ```text
    /// alice ──subscribe(Global)──► authority (empty)
    ///   ◄──one settled-empty reset─┘ (nothing before the authority serves)
    /// ```
    #[test]
    fn client_subscription_waits_for_connected_upstream_frontier() {
        let alice = AuthorSubject::for_test_bytes([0xa7; 16]);
        let client = open_db(0x17, alice);
        let authority = open_authority(0x37);

        let (client_transport, authority_transport) = duplex();
        let _client_connection = block_on(client.connect_upstream(client_transport));
        let _authority_subscriber = authority.accept_subscriber(authority_transport, alice);

        let documents = client
            .prepare_query(&client.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(client.subscribe(
            &documents,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe at Global against a connected authority");

        tick(&client, "send the subscription upstream");
        assert!(
            subscription.try_next_event().is_none(),
            "nothing may be delivered before the upstream serves the result"
        );

        let mut events = Vec::new();
        for _ in 0..4 {
            tick(&authority, "serve the empty authority result");
            tick(&client, "apply the authority result");
            events.extend(drain(&mut subscription));
        }
        let settled = events
            .iter()
            .filter(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. }))
            .collect::<Vec<_>>();
        assert_eq!(
            settled.len(),
            1,
            "the settled frontier must deliver exactly one initial answer: {events:?}"
        );
        let SubscriptionEvent::Delta { reset, added, .. } = settled[0] else {
            unreachable!("filtered to deltas above");
        };
        assert!(
            reset,
            "the initial settled answer replaces prior membership"
        );
        assert!(
            added.is_empty(),
            "the settled initial answer may be empty, but must be explicit"
        );
    }

    /// Explicitly detaching the upstream is the definitive "no upstream"
    /// signal: a previously held authority-tier subscription settles from the
    /// local store exactly once — not zero times (loading forever), not twice.
    ///
    /// Actors: alice's node with one local row and a silent upstream.
    ///
    /// ```text
    /// alice ──subscribe(Global)──► (silent upstream)   [held]
    ///   │        detach upstream
    ///   ◄──exactly one local answer──┘
    /// ```
    #[test]
    #[ignore = "#1766: detaching the upstream never releases a held authority-tier subscription: no local settlement is delivered and the read stays pending forever instead of settling from the local store exactly once"]
    fn detaching_the_upstream_settles_held_subscription_locally_exactly_once() {
        let alice = AuthorSubject::for_test_bytes([0xa8; 16]);
        let node = open_db(0x18, alice);
        let (upstream_transport, _held_far_end) = duplex();
        let upstream = block_on(node.connect_upstream(upstream_transport));

        let written = block_on(node.insert(
            "documents",
            document_cells("local answer after detach"),
            Default::default(),
        ))
        .expect("insert local document");

        let documents = node
            .prepare_query(&node.table("documents"))
            .expect("prepare documents query");
        let mut subscription = block_on(node.subscribe(
            &documents,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe at Global with a silent upstream");
        for _ in 0..3 {
            tick(&node, "queue coverage for the silent upstream");
        }
        assert!(
            drain(&mut subscription).is_empty(),
            "the subscription must stay held while the upstream is installed"
        );

        assert!(node.detach_connection(&upstream));
        let mut events = Vec::new();
        for _ in 0..3 {
            tick(&node, "settle the held subscription locally");
            events.extend(drain(&mut subscription));
        }
        let deltas = events
            .iter()
            .filter_map(|event| match event {
                SubscriptionEvent::Delta { added, .. } => Some(added),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            deltas.len(),
            1,
            "detaching the upstream must deliver the local answer exactly \
             once: {events:?}"
        );
        assert!(
            deltas[0]
                .iter()
                .any(|row| row.row.row_uuid() == written.row_uuid()),
            "the local answer must carry the locally stored row: {events:?}"
        );

        tick(&node, "no further settlement after the local answer");
        assert!(
            drain(&mut subscription).is_empty(),
            "the local settlement must not be delivered twice"
        );
    }
}

/// Client-facade contracts against the embedded server: transient transport
/// failures (offline, refused connect, rejected auth) must not release held
/// authority-tier subscriptions to settle from the local store.
mod client_transport {
    use std::time::Duration;

    use jazz::row_input;
    use jazz::tools::test_support::{disconnect_client, reconnect_client};
    use jazz::tools::{
        AppId, ColumnType, DurabilityTier, Schema, SchemaBuilder, SubscriptionStreamItem,
        TableSchema,
    };
    use jazz_server::JazzServer;
    use jazz_server::middleware::auth::TestClock;
    use jazz_testkit as support;
    use support::{TestingClient, collect_stream_deltas, has_added_id};
    use tempfile::TempDir;

    // Bounded observation window for must-not-deliver assertions. // ms.
    const HOLD_WINDOW: Duration = Duration::from_millis(1200);

    fn document_schema() -> Schema {
        SchemaBuilder::new()
            .table(TableSchema::builder("documents").column("title", ColumnType::Text))
            .build()
    }

    fn reserve_local_port() -> u16 {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve local port");
        listener.local_addr().expect("reserved local addr").port()
    }

    /// Waits for the initial settled reset of a fresh subscription and
    /// asserts it carries `expected_id`.
    async fn assert_initial_settled_row(
        stream: &mut jazz::tools::SubscriptionStream,
        expected_id: jazz::tools::ObjectId,
        label: &str,
    ) {
        let item = tokio::time::timeout(Duration::from_secs(15), stream.next())
            .await
            .unwrap_or_else(|_| panic!("{label}: initial settled reset timed out"))
            .unwrap_or_else(|| panic!("{label}: subscription stream closed"));
        let SubscriptionStreamItem::Delta(delta) = item else {
            panic!("{label}: subscription was rejected");
        };
        assert!(!delta.pending, "{label}: initial reset must be settled");
        assert!(
            delta.added.iter().any(|added| added.id == expected_id),
            "{label}: initial reset must carry the settled row: {delta:?}"
        );
    }

    /// An installed upstream that is currently unavailable must not release a
    /// held authority-tier subscription: while the client is offline, a fresh
    /// authority-tier subscription never delivers a settled answer — any
    /// delivery from the local store is explicitly marked pending, so the
    /// cold-or-stale store is never surfaced as authority-backed.
    ///
    /// Actors: alice writes and settles a row online, then goes offline.
    ///
    /// ```text
    /// alice ──insert──► server ──settled──► alice          (online control)
    /// alice ─offline─ subscribe(authority) ──► pending only (never settled)
    /// ```
    #[tokio::test]
    async fn offline_client_keeps_authority_subscription_held() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let schema = document_schema();
                let server = JazzServer::start_with_schema(schema.clone()).await;
                let alice = TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema)
                    .with_user_id("alice-held-subscription")
                    .ready_on("documents", Duration::from_secs(30))
                    .connect()
                    .await;

                let (document_id, _, transaction_id) = alice
                    .insert("documents", row_input!("title" => "settled before offline"))
                    .expect("insert document");
                support::wait_for_edge_txs(
                    &alice,
                    &[transaction_id.expect("ordinary mutation commits immediately")],
                )
                .await;

                // Online control: the same query settles with the row, so a
                // later hold cannot be a subscription that never works.
                let mut online = alice
                    .subscribe(jazz::query::Query::from("documents"))
                    .await
                    .expect("subscribe online");
                assert_initial_settled_row(&mut online, document_id, "online control").await;
                drop(online);

                assert!(disconnect_client(&alice), "detach the live transport");

                let mut held = tokio::time::timeout(
                    Duration::from_secs(15),
                    alice.subscribe(jazz::query::Query::from("documents")),
                )
                .await
                .expect("offline subscribe must not hang")
                .expect("offline subscribe must open");
                let mut log = Vec::new();
                collect_stream_deltas(&mut held, &mut log, HOLD_WINDOW).await;
                assert!(
                    log.iter().all(|delta| delta.pending),
                    "an offline client must hold authority-tier settlement: \
                     every delivery must stay marked pending: {log:?}"
                );

                alice.shutdown().await.expect("shutdown alice");
                server.shutdown().await;
            })
            .await;
    }

    /// A failed connect attempt (refused connection) is transient: it must
    /// not release a held authority-tier subscription to settle from the
    /// local store. Once the upstream returns and the client reconnects, the
    /// held subscription settles with the authority's row.
    ///
    /// Actors: alice; the server restarts on the same address.
    ///
    /// ```text
    /// alice ──insert──► server₁ (settled)      server₁ stops
    /// alice ─reconnect─✗ (refused)  subscribe(authority) ──► nothing
    /// server₂ starts  ─reconnect─✓  ──settled row──► alice
    /// ```
    #[tokio::test]
    async fn connect_failure_keeps_authority_subscription_held_until_upstream_returns() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let schema = document_schema();
                let app_id = AppId::random();
                let port = reserve_local_port();
                let data_dir = TempDir::new().expect("server data dir");

                let first_server = JazzServer::builder()
                    .with_app_id(app_id)
                    .with_port(port)
                    .with_schema(schema.clone())
                    .with_data_dir(data_dir.path())
                    .with_storage_factory(jazz_testkit::persistent_storage_factory())
                    .start()
                    .await;
                let alice = TestingClient::builder()
                    .with_server(&first_server)
                    .with_schema(schema.clone())
                    .with_user_id("alice-connect-failure")
                    .ready_on("documents", Duration::from_secs(30))
                    .connect()
                    .await;

                let (document_id, _, transaction_id) = alice
                    .insert("documents", row_input!("title" => "survives the outage"))
                    .expect("insert document");
                alice
                    .wait_for_transaction(
                        transaction_id.expect("ordinary mutation commits immediately"),
                        DurabilityTier::GlobalServer,
                    )
                    .await
                    .expect("document settles before the outage");

                disconnect_client(&alice);
                first_server.shutdown().await;

                let refused = reconnect_client(&alice).await;
                assert!(
                    refused.is_err(),
                    "reconnecting against the stopped server must fail: {refused:?}"
                );

                let mut held = tokio::time::timeout(
                    Duration::from_secs(15),
                    alice.subscribe(jazz::query::Query::from("documents")),
                )
                .await
                .expect("subscribe after failed connect must not hang")
                .expect("subscribe after failed connect must open");
                let mut log = Vec::new();
                collect_stream_deltas(&mut held, &mut log, HOLD_WINDOW).await;
                assert!(
                    log.iter().all(|delta| delta.pending),
                    "a failed connect must not surface the local store as a \
                     settled authority answer: {log:?}"
                );

                let second_server = JazzServer::builder()
                    .with_app_id(app_id)
                    .with_port(port)
                    .with_schema(schema.clone())
                    .with_data_dir(data_dir.path())
                    .with_storage_factory(jazz_testkit::persistent_storage_factory())
                    .start()
                    .await;
                assert!(
                    reconnect_client(&alice)
                        .await
                        .expect("reconnect once the upstream returns"),
                    "the preserved client state must reattach"
                );

                support::wait_for_subscription_update(
                    &mut held,
                    &mut log,
                    Duration::from_secs(30),
                    "held subscription settles once the upstream returns",
                    |deltas| {
                        deltas.iter().any(|delta| {
                            !delta.pending && has_added_id(std::slice::from_ref(delta), document_id)
                        })
                    },
                )
                .await;

                alice.shutdown().await.expect("shutdown alice");
                second_server.shutdown().await;
            })
            .await;
    }

    /// A rejected credential on reconnect (expired token) is recoverable: it
    /// must not release a held authority-tier subscription to settle from the
    /// local store as an authoritative empty-or-stale answer. Any local
    /// delivery stays explicitly marked pending.
    ///
    /// Actors: alice; the server's auth clock advances past her token's
    /// expiry while she is offline.
    ///
    /// ```text
    /// alice ──insert──► server (settled)    alice goes offline
    /// auth clock jumps past expiry
    /// alice ─reconnect─✗ (auth rejected)  subscribe(authority) ──► nothing
    /// ```
    #[tokio::test]
    async fn auth_failure_keeps_authority_subscription_held() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let auth_clock = TestClock::new(1_700_000_000);
                let schema = document_schema();
                let server = JazzServer::builder()
                    .with_schema(schema.clone())
                    .with_auth_clock(auth_clock.clone())
                    .start()
                    .await;
                let alice = TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema)
                    .with_user_id("alice-auth-failure")
                    .ready_on("documents", Duration::from_secs(30))
                    .connect()
                    .await;

                let (document_id, _, transaction_id) = alice
                    .insert("documents", row_input!("title" => "settled before expiry"))
                    .expect("insert document");
                support::wait_for_edge_txs(
                    &alice,
                    &[transaction_id.expect("ordinary mutation commits immediately")],
                )
                .await;
                let mut online = alice
                    .subscribe(jazz::query::Query::from("documents"))
                    .await
                    .expect("subscribe online");
                assert_initial_settled_row(&mut online, document_id, "online control").await;
                drop(online);

                disconnect_client(&alice);
                // The context token is minted with a one-hour expiry against
                // the server's test auth clock.
                auth_clock.advance(Duration::from_secs(4000));

                let rejected = reconnect_client(&alice).await;
                assert!(
                    rejected.is_err(),
                    "reconnecting with an expired token must fail: {rejected:?}"
                );

                let mut held = tokio::time::timeout(
                    Duration::from_secs(15),
                    alice.subscribe(jazz::query::Query::from("documents")),
                )
                .await
                .expect("subscribe after auth failure must not hang")
                .expect("subscribe after auth failure must open");
                let mut log = Vec::new();
                collect_stream_deltas(&mut held, &mut log, HOLD_WINDOW).await;
                assert!(
                    log.iter().all(|delta| delta.pending),
                    "an auth failure must not surface the local store as a \
                     settled authority answer: {log:?}"
                );

                alice.shutdown().await.expect("shutdown alice");
                server.shutdown().await;
            })
            .await;
    }
}
