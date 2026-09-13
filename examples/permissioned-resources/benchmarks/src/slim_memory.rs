//! Workload-specific reference, not an alternative Jazz implementation.
use super::*;
use std::io::BufRead;
use uuid::Uuid;

type Encoded = Vec<(Vec<u8>, Vec<u8>, jazz::tx::Fate)>;
struct Store {
    rows: Vec<jazz::protocol::VersionRecord>,
    tables: BTreeMap<String, Vec<usize>>,
    keys: BTreeMap<(String, Uuid), usize>,
    transactions: Vec<Vec<u8>>,
    history: BTreeMap<(String, Uuid, jazz::tx::TxId), usize>,
    fates: BTreeMap<jazz::tx::TxId, jazz::tx::Fate>,
}
fn hex(value: &JsonValue) -> Vec<u8> {
    value
        .as_str()
        .unwrap()
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn read(path: &Path) -> Encoded {
    let mut result = Vec::new();
    for line in std::io::BufReader::new(fs::File::open(path).unwrap()).lines() {
        let frame: JsonValue = serde_json::from_str(&line.unwrap()).unwrap();
        for b in frame["bundles"].as_array().unwrap() {
            assert_eq!(b["scope"], "CompleteTransaction");
            assert_eq!(b["fate"], "Accepted");
            assert_eq!(b["versions"].as_array().unwrap().len(), 1);
            assert_eq!(b["versions"][0]["parents"], json!([]));
            result.push((
                hex(&b["tx_hex"]),
                hex(&b["versions"][0]["wire_hex"]),
                serde_json::from_value(b["fate"].clone()).unwrap(),
            ));
        }
    }
    result
}
impl Store {
    fn ingest(input: &Encoded) -> Self {
        let history_mode = std::env::var_os("JAZZ_SLIM_HISTORY").is_some();
        let mut s = Self {
            rows: Vec::new(),
            tables: BTreeMap::new(),
            keys: BTreeMap::new(),
            transactions: Vec::new(),
            history: BTreeMap::new(),
            fates: BTreeMap::new(),
        };
        let mut tx_ids = Vec::<jazz::tx::TxId>::new();
        for (tx, bytes, fate) in input {
            let decoded: jazz::tx::Transaction = postcard::from_bytes(tx).unwrap();
            let row: jazz::protocol::VersionRecord = postcard::from_bytes(bytes).unwrap();
            let key = (row.table().to_owned(), row.row_uuid().0);
            if history_mode {
                // Explicit shallow-history contract; never silently approximate
                // ancestor/deletion/exclusive semantics with timestamp ordering.
                assert_eq!(decoded.kind, jazz::tx::TxKind::Mergeable);
                assert!(row.parents().is_empty());
                assert!(row.deletion().is_none());
                if let Some(previous) = s.fates.insert(decoded.tx_id, fate.clone()) {
                    assert_eq!(previous, *fate);
                }
                let history_key = (key.0.clone(), key.1, decoded.tx_id);
                if let Some(&i) = s.history.get(&history_key) {
                    assert_eq!(s.rows[i], row);
                    assert_eq!(s.transactions[i], *tx);
                    continue;
                }
                let i = s.rows.len();
                s.history.insert(history_key, i);
                if s.fates[&decoded.tx_id] == jazz::tx::Fate::Accepted {
                    let wins = s.keys.get(&key).is_none_or(|&old| {
                        decoded.tx_id.time.sort_key(decoded.tx_id.node)
                            > tx_ids[old].time.sort_key(tx_ids[old].node)
                    });
                    if wins {
                        s.keys.insert(key, i);
                    }
                }
                tx_ids.push(decoded.tx_id);
                s.rows.push(row);
                s.transactions.push(tx.clone());
            } else if let Some(&i) = s.keys.get(&key) {
                assert_eq!(s.rows[i], row);
                assert_eq!(s.transactions[i], *tx);
            } else {
                let i = s.rows.len();
                s.keys.insert(key, i);
                s.rows.push(row);
                s.transactions.push(tx.clone());
            }
        }
        for ((table, _), &i) in &s.keys {
            s.tables.entry(table.clone()).or_default().push(i);
        }
        s
    }
    fn table(&self, name: &str) -> &[usize] {
        self.tables.get(name).map(Vec::as_slice).unwrap_or(&[])
    }
}
struct Plan {
    columns: BTreeMap<String, BTreeMap<String, usize>>,
    resources: Vec<(String, String, Option<String>)>,
    expected: BTreeMap<String, BTreeSet<Uuid>>,
    account: Uuid,
}
impl Plan {
    fn new(fixture: &JsonValue) -> Self {
        Self {
            columns: schema()
                .tables
                .iter()
                .map(|t| {
                    (
                        t.name.clone(),
                        t.columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (c.name().to_owned(), i))
                            .collect(),
                    )
                })
                .collect(),
            resources: fixture["resources"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["table"].as_str().unwrap().to_owned(),
                        r["access"].as_str().unwrap().to_owned(),
                        r["child"].as_str().map(str::to_owned),
                    )
                })
                .collect(),
            expected: fixture["expected"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(t, ids)| {
                    (
                        t.clone(),
                        ids.as_array()
                            .unwrap()
                            .iter()
                            .map(|id| Uuid::parse_str(id.as_str().unwrap()).unwrap())
                            .collect(),
                    )
                })
                .collect(),
            account: Uuid::parse_str(fixture["account"].as_str().unwrap()).unwrap(),
        }
    }
    fn cell(&self, s: &Store, i: usize, name: &str) -> Value {
        let r = &s.rows[i];
        r.cell_at(self.columns[r.table()][name]).unwrap()
    }
    fn id(&self, s: &Store, i: usize, name: &str) -> Uuid {
        match self.cell(s, i, name) {
            Value::Uuid(id) => id,
            v => panic!("unexpected ID {v:?}"),
        }
    }
    fn flag(&self, s: &Store, i: usize, name: &str) -> bool {
        match self.cell(s, i, name) {
            Value::Bool(v) => v,
            v => panic!("unexpected flag {v:?}"),
        }
    }
    // Deliberately recompute permissions per query, like the SQL reference.
    // The bounded traversal preserves depth; it never assumes a flat group graph.
    fn query(&self, s: &Store, table: &str) -> (Vec<usize>, BTreeSet<usize>, usize) {
        let resource = self
            .resources
            .iter()
            .find(|(t, _, c)| t == table || c.as_deref() == Some(table));
        let Some((parent, access, child)) = resource else {
            return (
                s.table(table).to_vec(),
                s.table(table).iter().copied().collect(),
                s.table(table).len(),
            );
        };
        let mut visits = 0;
        let mut reached = BTreeSet::new();
        let mut frontier = BTreeSet::new();
        let mut support = BTreeSet::new();
        for &i in s.table("group_access_edges") {
            visits += 1;
            if self.id(s, i, "user_id") == self.account {
                frontier.insert(self.id(s, i, "group_id"));
                support.insert(i);
            }
        }
        reached.extend(&frontier);
        for _ in 0..8 {
            let mut next = BTreeSet::new();
            for &i in s.table("group_entry") {
                visits += 1;
                if !self.flag(s, i, "administrator")
                    && frontier.contains(&self.id(s, i, "member_id"))
                {
                    let target = self.id(s, i, "target_id");
                    if s.keys.contains_key(&("group".to_owned(), target)) {
                        next.insert(target);
                        support.insert(i);
                    }
                }
            }
            reached.extend(&next);
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        for id in &reached {
            if let Some(&i) = s.keys.get(&("group".to_owned(), *id)) {
                support.insert(i);
            }
        }
        let mut allowed = BTreeMap::<Uuid, Vec<usize>>::new();
        for &i in s.table(access) {
            visits += 1;
            if !self.flag(s, i, "administrator") && reached.contains(&self.id(s, i, "team")) {
                allowed
                    .entry(self.id(s, i, "resource"))
                    .or_default()
                    .push(i);
            }
        }
        let is_child = child.as_deref() == Some(table);
        let mut output = Vec::new();
        for &i in s.table(table) {
            visits += 1;
            let id = if is_child {
                self.id(s, i, "parent_id")
            } else {
                s.rows[i].row_uuid().0
            };
            if let Some(grants) = allowed.get(&id) {
                output.push(i);
                support.insert(i);
                support.extend(grants);
                if is_child {
                    support.insert(*s.keys.get(&(parent.clone(), id)).expect("parent witness"));
                }
            }
        }
        (output, support, visits)
    }
    fn evaluate(&self, s: &Store) -> (BTreeMap<String, Vec<usize>>, usize, usize, usize) {
        let mut outputs = BTreeMap::new();
        let mut witnesses = 0;
        let mut visits = 0;
        let mut fields = 0;
        for table in self.expected.keys() {
            let (rows, support, n) = self.query(s, table);
            visits += n;
            witnesses += support.len();
            // Materialize all application fields, rather than count-only results.
            let rendered = rows
                .iter()
                .map(|&i| {
                    let r = &s.rows[i];
                    (0..self.columns[table].len())
                        .map(|c| r.cell_at(c).unwrap())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            fields += rendered.iter().map(Vec::len).sum::<usize>();
            std::hint::black_box(rendered);
            outputs.insert(table.clone(), rows);
        }
        (outputs, witnesses, visits, fields)
    }
    fn verify(&self, s: &Store, outputs: &BTreeMap<String, Vec<usize>>, fixture: &JsonValue) {
        for (table, rows) in outputs {
            assert_eq!(
                rows.iter()
                    .map(|&i| s.rows[i].row_uuid().0)
                    .collect::<BTreeSet<_>>(),
                self.expected[table],
                "{table}"
            );
        }
        let writes = fixture["writes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| ((w["table"].as_str().unwrap(), w["id"].as_str().unwrap()), w))
            .collect::<BTreeMap<_, _>>();
        for rows in outputs.values() {
            for &i in rows {
                let r = &s.rows[i];
                let id = r.row_uuid().0.to_string();
                let w = writes[&(r.table(), id.as_str())];
                for (name, &position) in &self.columns[r.table()] {
                    assert_eq!(
                        serde_json::to_value(r.cell_at(position).unwrap()).unwrap(),
                        w["cells"][name]
                    );
                }
            }
        }
    }
}
pub fn run(root: &Path) {
    let fixture: JsonValue =
        serde_json::from_reader(fs::File::open(root.join("fixture.json")).unwrap()).unwrap();
    let plan = Plan::new(&fixture);
    let ce = read(&root.join("core-edge.jsonl"));
    let ec = read(&root.join("edge-client.jsonl"));
    if std::env::var_os("JAZZ_SLIM_HISTORY").is_some() {
        let mut pending = vec![ce[0].clone()];
        pending[0].2 = jazz::tx::Fate::Pending;
        let probe = Store::ingest(&pending);
        assert_eq!(probe.history.len(), 1);
        assert!(
            probe.keys.is_empty(),
            "pending history must not become accepted current state"
        );
    }
    let core = Store::ingest(&ce); // preexisting Core state, outside load timing
    // Sensitivity: a principal with no membership must not receive protected
    // resource or child outputs. This would fail if the permission filter were
    // removed, while ordinary unprotected tables may remain readable.
    let mut denied = Plan::new(&fixture);
    denied.account = Uuid::nil();
    let mut protected_rows = 0;
    for (table, _, child) in &denied.resources {
        for name in std::iter::once(table).chain(child.iter()) {
            protected_rows += plan.expected[name].len();
            assert!(
                denied.query(&core, name).0.is_empty(),
                "unlinked identity sees {name}"
            );
        }
    }
    assert!(protected_rows > 0);
    for round in 0..3 {
        alloc_metrics::reset_and_start();
        let total = Instant::now();
        let t = Instant::now();
        let core_result = plan.evaluate(&core);
        let core_ms = t.elapsed().as_secs_f64() * 1000.;
        let t = Instant::now();
        let edge = Store::ingest(&ce);
        let edge_ingest_ms = t.elapsed().as_secs_f64() * 1000.;
        let t = Instant::now();
        let edge_result = plan.evaluate(&edge);
        let edge_query_ms = t.elapsed().as_secs_f64() * 1000.;
        let t = Instant::now();
        let client = Store::ingest(&ec);
        let client_ingest_ms = t.elapsed().as_secs_f64() * 1000.;
        let t = Instant::now();
        let client_result = plan.evaluate(&client);
        let client_query_ms = t.elapsed().as_secs_f64() * 1000.;
        let total_ms = total.elapsed().as_secs_f64() * 1000.;
        let allocations = alloc_metrics::stop();
        plan.verify(&core, &core_result.0, &fixture);
        plan.verify(&edge, &edge_result.0, &fixture);
        plan.verify(&client, &client_result.0, &fixture);
        println!(
            "{}",
            json!({"history_mode":std::env::var_os("JAZZ_SLIM_HISTORY").is_some(),"history_entries":[core.history.len(),edge.history.len(),client.history.len()],"fate_entries":[core.fates.len(),edge.fates.len(),client.fates.len()],"round":round,"allocation_requests":allocations.allocs,"allocation_bytes":allocations.bytes,"total_ms":total_ms,"core_query_ms":core_ms,"edge_ingest_ms":edge_ingest_ms,"edge_query_ms":edge_query_ms,"client_ingest_ms":client_ingest_ms,"client_query_ms":client_query_ms,"unique_rows":[core.rows.len(),edge.rows.len(),client.rows.len()],"operator_row_visits":[core_result.2,edge_result.2,client_result.2],"support_memberships":[core_result.1,edge_result.1,client_result.1],"output_fields":[core_result.3,edge_result.3,client_result.3]})
        );
    }
}
