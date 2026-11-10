//! Correctness mode for the DO harness; no performance measurements.
use super::*;
use futures_util::FutureExt;

const SAFE: i64 = 9_007_199_254_740_991;
const VALUES: [(i64, f64); 5] = [
    (-SAFE, -1e100),
    (-SAFE + 1, -1.0),
    (0, 1.0),
    (SAFE - 1, 1.5),
    (SAFE, 1e100),
];

fn cells(n: i64, r: f64) -> BTreeMap<String, Value> {
    BTreeMap::from([("n".into(), Value::I64(n)), ("r".into(), Value::F64(r))])
}

/// Alice uploads numeric values; Bob receives them through the DO authority.
/// A fresh Bob repeats the reads after workerd restarts with only SQLite retained.
/// Alice's mixed valid/unsupported batch must leave no rows or history behind.
/// alice (native) -> authority (DO SQLite) -> bob (native)
pub(super) fn run(base: &str) {
    let verify = std::env::var_os("VERIFY_ONLY").is_some();
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("numbers")
                .column("n", ColumnType::BigInt)
                .column("r", ColumnType::Double),
        )
        .build()
        .allow_all();
    let mut control_socket = Some(connect(&format!("{base}/control")));
    let config = OpenConfig {
        identity: OpenIdentity {
            node: node(0),
            author: author(usize::MAX),
        },
        row_id_seed: None,
        history_complete: true,
        initial_sync_flush_every: None,
        migrations: None,
        system_node: None,
    };
    control(
        &mut control_socket,
        if std::env::var_os("REOPEN_CATALOGUE").is_some() {
            json!({ "op": "reopen", "config": base64(&postcard::to_allocvec(&config).unwrap()) })
        } else {
            json!({
                "op": "open", "schema": base64(&serde_json::to_vec(&schema).unwrap()),
                "config": base64(&postcard::to_allocvec(&config).unwrap()),
            })
        },
    );
    let replica = |id: u16| {
        let db = block_on(Db::open(DbConfig::new(
            JazzSchema::new(&schema).unwrap(),
            DbIdentity {
                node: node(id + if verify { 1000 } else { 0 }),
                author: author(id as usize),
            },
        )))
        .unwrap();
        let link = block_on(db.connect_upstream(Box::new(WireTransportAdapter::current(
            Wire::session(base, author(id as usize)),
        ))));
        (db, link)
    };
    let (alice, _alice_link) = replica(1);
    if !verify {
        let (_, receipt) = block_on(alice.transaction(async |tx| {
            for (i, (n, r)) in VALUES.iter().enumerate() {
                tx.insert(
                    "numbers",
                    cells(*n, *r),
                    InsertOptions {
                        row_id: Some(task_id(i)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .unwrap();
        pump(&alice, || settled(&alice, receipt.0));
    }
    let (bob, _bob_link) = replica(2);
    let prepared = bob
        .prepare_query(&Query::from("numbers").order_by("n", jazz::query::OrderDirection::Asc))
        .unwrap();
    let mut subscription = block_on(bob.subscribe(
        &prepared,
        ReadOpts {
            tier: ReadTier::Remote,
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&bob, || bob.read(&prepared).unwrap().len() == VALUES.len());
    let check = || {
        let rows = bob.read(&prepared).unwrap();
        assert_eq!(rows.len(), VALUES.len());
        for (row, (n, r)) in rows.iter().zip(VALUES) {
            assert_eq!(row.cell("n"), Some(Value::I64(n)));
            assert_eq!(row.cell("r"), Some(Value::F64(r)));
        }
    };
    check();
    while subscription.try_next_event().is_some() {}
    if !verify {
        for (case, n) in [i64::MIN, -SAFE - 1, SAFE + 1, i64::MAX]
            .into_iter()
            .enumerate()
        {
            let (_, receipt) = block_on(alice.transaction(async |tx| {
                for (id, value) in [(100 + case * 2, 42), (101 + case * 2, n)] {
                    tx.insert(
                        "numbers",
                        cells(value, 1.0),
                        InsertOptions {
                            row_id: Some(task_id(id)),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                Ok(())
            }))
            .expect("native replica accepts full i64 range locally");
            pump(&alice, || {
                match alice
                    .wait_for_transaction(receipt.0, DurabilityTier::Global)
                    .now_or_never()
                {
                    None => false,
                    Some(Ok(_)) => panic!("DO accepted unsupported integer {n}"),
                    Some(Err(error)) => {
                        // Backend failures travel as WireError::Other (Protocol),
                        // unlike policy/conflict rejections (WriteRejected).
                        assert_eq!(error.code, jazz::db::ErrorCode::Protocol);
                        assert!(error.message.contains("safe integer range"), "{error:?}");
                        true
                    }
                }
            });
        }
        for _ in 0..20 {
            block_on(bob.tick()).unwrap();
            assert!(
                subscription.try_next_event().is_none(),
                "rejected batch published an update"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        check();
    }
    // Remote history reads also inspect the authority after reconstruction.
    // The first row of every rejected batch was valid, but must have rolled back.
    for id in 100..108 {
        // Public history hides versions whose current row is absent. Inspect
        // physical row/history residue through the existing test-only hook too.
        control(
            &mut control_socket,
            json!({
                "op": "checkRejected", "table": "numbers", "row": task_id(id).to_bytes(),
            }),
        );
        let query = bob
            .prepare_query(
                &Query::from("history.numbers")
                    .filter(eq(col("id"), lit(Value::Uuid(task_id(id).0)))),
            )
            .unwrap();
        let future = bob.all(
            &query,
            ReadOpts {
                tier: ReadTier::Remote,
                ..Default::default()
            },
        );
        let mut future = std::pin::pin!(future);
        let mut result = None;
        pump(&bob, || {
            result = future.as_mut().now_or_never();
            result.is_some()
        });
        assert!(
            result.unwrap().unwrap().is_empty(),
            "rejected row {id} retained history"
        );
    }
    let stats = control(&mut control_socket, json!({ "op": "stats" }));
    assert_eq!(stats["errors"], json!([]), "authority errors: {stats}");
    println!(
        "Numeric native/DO sync {} passed",
        if verify {
            "after restart"
        } else {
            "and atomic rejection"
        }
    );
}
