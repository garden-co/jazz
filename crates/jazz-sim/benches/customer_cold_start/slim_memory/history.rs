//! A deliberately explicit, row-local ancestry reference. Not Jazz admission.
use super::*;
use jazz::tx::{Fate, Transaction, TxId, TxKind};

impl Store {
    fn ingest_richer(input: &Encoded) -> Self {
        let mut s = Self {
            rows: Vec::new(),
            tables: BTreeMap::new(),
            keys: BTreeMap::new(),
            transactions: Vec::new(),
            history: BTreeMap::new(),
            fates: BTreeMap::new(),
        };
        let mut per_row = BTreeMap::<(String, Uuid), BTreeMap<TxId, usize>>::new();
        let mut parents = Vec::new();
        for (tx_bytes, row_bytes, fate) in input {
            // Explicit fault injection for the independent winner oracle.
            let fate = if std::env::var_os("JAZZ_SLIM_RICH_IGNORE_REJECTIONS").is_some()
                && matches!(fate, Fate::Rejected(_))
            {
                &Fate::Accepted
            } else {
                fate
            };
            let tx: Transaction = postcard::from_bytes(tx_bytes).unwrap();
            let row: jazz::protocol::VersionRecord = postcard::from_bytes(row_bytes).unwrap();
            assert_eq!(tx.kind, TxKind::Mergeable);
            assert_eq!(tx.n_total_writes, 1);
            assert!(row.deletion().is_none());
            assert_eq!(row.branch_key(), &Default::default());
            let key = (row.table().to_owned(), row.row_uuid().0);
            if let Some(old) = s.fates.insert(tx.tx_id, fate.clone()) {
                assert!(
                    old == *fate || old == Fate::Pending,
                    "terminal fates cannot change"
                );
            }
            let versions = per_row.entry(key.clone()).or_default();
            if let Some(&i) = versions.get(&tx.tx_id) {
                assert_eq!(s.rows[i], row, "exact immutable duplicate");
                assert_eq!(s.transactions[i], *tx_bytes);
            } else {
                let i = s.rows.len();
                versions.insert(tx.tx_id, i);
                s.history.insert((key.0.clone(), key.1, tx.tx_id), i);
                parents.push(row.parents());
                s.rows.push(row);
                s.transactions.push(tx_bytes.clone());
            }
            // Deliberately recompute only this row's small history. Unknown
            // ancestors remain explicit references; when they arrive this same
            // row is recomputed. Accepted descendants need no new admission.
            let accepted = versions
                .iter()
                .filter(|(id, _)| s.fates[id] == Fate::Accepted)
                .map(|(id, i)| (*id, *i))
                .collect::<BTreeMap<_, _>>();
            let mut dominated = BTreeSet::new();
            for &i in accepted.values() {
                let mut todo = parents[i].clone();
                let mut seen = BTreeSet::new();
                while let Some(id) = todo.pop() {
                    if !seen.insert(id) {
                        continue;
                    }
                    if let Some(&ancestor) = accepted.get(&id) {
                        dominated.insert(id);
                        todo.extend(parents[ancestor].iter().copied());
                    }
                }
            }
            let winner = accepted
                .iter()
                .filter(|(id, _)| !dominated.contains(id))
                .max_by_key(|(id, _)| id.time.sort_key(id.node))
                .map(|(_, i)| *i);
            if let Some(i) = winner {
                s.keys.insert(key, i);
            } else {
                s.keys.remove(&key);
            }
        }
        for ((table, _), &i) in &s.keys {
            s.tables.entry(table.clone()).or_default().push(i);
        }
        s
    }
}

fn expand(input: &Encoded) -> (Encoded, BTreeMap<(String, Uuid), TxId>) {
    let schema = schema();
    let mut unique = BTreeMap::new();
    for item in input {
        let tx: Transaction = postcard::from_bytes(&item.0).unwrap();
        unique.entry(tx.tx_id).or_insert(item);
    }
    let mut deliveries = Vec::new();
    let mut final_fates = Vec::new();
    let mut expected = BTreeMap::new();
    for (ordinal, item) in unique.values().enumerate() {
        let original: Transaction = postcard::from_bytes(&item.0).unwrap();
        let row: jazz::protocol::VersionRecord = postcard::from_bytes(&item.1).unwrap();
        let table = schema
            .tables
            .iter()
            .find(|t| t.name == row.table())
            .unwrap();
        let cells = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name().to_owned(), row.cell_at(i).unwrap()))
            .collect::<BTreeMap<_, _>>();
        let ids = (0..5)
            .map(|generation| {
                TxId::new(
                    jazz::time::TxTime(original.tx_id.time.0.checked_add(generation).unwrap()),
                    NodeUuid(Uuid::from_u128(0xf000_0000 + ordinal as u128)),
                )
            })
            .collect::<Vec<_>>();
        // Base, competing siblings, merge, then a pending candidate whose fate
        // eventually accepts or rejects. Payloads stay equal so all application
        // and permission oracles remain directly comparable.
        let ancestry = [
            vec![],
            vec![ids[0]],
            vec![ids[0]],
            vec![ids[1], ids[2]],
            vec![ids[3]],
        ];
        for generation in 0..5 {
            let mut tx = original.clone();
            tx.tx_id = ids[generation];
            let version = jazz::protocol::VersionRecord::from_cells(
                table,
                row.schema_version(),
                row.row_uuid(),
                ancestry[generation].clone(),
                row.created_by(),
                row.created_at_ms(),
                row.updated_by(),
                row.updated_at_ms(),
                &cells,
                None,
            )
            .unwrap();
            let bytes = (
                postcard::to_allocvec(&tx).unwrap(),
                postcard::to_allocvec(&version).unwrap(),
                if generation == 4 {
                    Fate::Pending
                } else {
                    Fate::Accepted
                },
            );
            deliveries.push(bytes.clone());
            if generation == 2 {
                deliveries.push(bytes.clone());
            }
            if generation == 4 {
                let accepted = ordinal % 2 == 0;
                let mut finalized = bytes;
                finalized.2 = if accepted {
                    Fate::Accepted
                } else {
                    Fate::Rejected(jazz::tx::RejectionReason::AuthorizationDenied)
                };
                final_fates.push(finalized);
                expected.insert(
                    (row.table().to_owned(), row.row_uuid().0),
                    ids[if accepted { 4 } else { 3 }],
                );
            }
        }
    }
    deliveries.reverse(); // descendants arrive before ancestors, including duplicates
    deliveries.extend(final_fates);
    (deliveries, expected)
}

pub(super) fn run(root: &Path) {
    let fixture: JsonValue =
        serde_json::from_reader(fs::File::open(root.join("fixture.json")).unwrap()).unwrap();
    let plan = Plan::new(&fixture);
    for dataset in ["core-edge", "edge-client"] {
        let flat = read(&root.join(format!("{dataset}.jsonl")));
        let (rich, winners) = expand(&flat);
        for (name, input) in [("shallow", &flat), ("five-versions-out-of-order", &rich)] {
            for round in 0..3 {
                let t = Instant::now();
                let store = Store::ingest_richer(input);
                let ingest_ms = t.elapsed().as_secs_f64() * 1000.;
                let t = Instant::now();
                let result = plan.evaluate(&store);
                let query_ms = t.elapsed().as_secs_f64() * 1000.;
                plan.verify(&store, &result.0, &fixture);
                if name != "shallow" {
                    for (key, &i) in &store.keys {
                        let tx: Transaction = postcard::from_bytes(&store.transactions[i]).unwrap();
                        assert_eq!(
                            tx.tx_id, winners[key],
                            "current winner must match independently generated oracle"
                        );
                    }
                    assert_eq!(store.keys.len(), winners.len());
                    assert_eq!(store.history.len(), winners.len() * 5);
                }
                println!(
                    "{}",
                    json!({"dataset":dataset,"mode":name,"round":round,"deliveries":input.len(),"history_versions":store.history.len(),"current_rows":store.keys.len(),"ingest_ms":ingest_ms,"query_ms":query_ms})
                );
            }
        }
    }
}
