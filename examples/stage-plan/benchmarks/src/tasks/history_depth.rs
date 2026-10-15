//! Diagnostic extension of the todo worker/foreground fixture for #2981.
//! Direct nodes are needed to distinguish author, worker and receiver costs;
//! this is not a JS/API latency receipt. Existing timed workloads are unchanged.
use super::*;

#[derive(Default)]
struct Samples {
    micros: Vec<u128>,
    history_reads: usize,
    reads: usize,
}

fn measure<S: OrderedKvStorage, T>(
    samples: &mut Samples,
    node: &mut NodeState<S>,
    f: impl FnOnce(&mut NodeState<S>) -> T,
) -> T {
    node.reset_storage_read_metrics();
    let start = Instant::now();
    let result = f(node);
    samples.micros.push(start.elapsed().as_micros());
    let metrics = node.storage_read_metrics();
    samples.history_reads += metrics.history_rows.reads;
    samples.reads += metrics.total.reads;
    result
}

fn verify(f: &mut Fixture<RocksDbStorage>, revisions: &[usize]) {
    f.read_all();
    let table = f.schema.tables.iter().find(|t| t.name == "tasks").unwrap();
    let actual = f
        .read_result
        .iter()
        .map(|r| (r.row_uuid(), r))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual.len(), revisions.len());
    for (i, revision) in revisions.iter().copied().enumerate() {
        let r = actual.get(&row(i)).expect("exact task ID");
        let title = if revision == 0 {
            format!("Task {i}")
        } else {
            format!("Task {i} edit {revision}")
        };
        assert_eq!(r.cell(table, "title"), Some(Value::String(title)));
        assert_eq!(r.cell(table, "done"), Some(Value::Bool(revision % 2 == 1)));
    }
}

fn run(rows: usize, updates: usize, window: usize, arm: &str) {
    assert!(matches!(arm, "spread" | "hot" | "stale-parent"));
    assert!(window > 0 && updates > 0);
    let mut f = Fixture::loaded(rows);
    let mut parents = vec![f.seed; rows];
    let mut revisions = vec![0; rows];
    let mut phases = BTreeMap::<&str, Samples>::new();
    for step in 0..updates {
        let i = if arm == "spread" { step % rows } else { 0 };
        revisions[i] += 1;
        let mut values = cells(i, revisions[i] % 2 == 1);
        values.insert(
            "title".into(),
            Value::String(format!("Task {i} edit {}", revisions[i])),
        );
        let parent = if arm == "stale-parent" {
            f.seed
        } else {
            parents[i]
        };
        // Linear history carries no parents; the arm now only varies the
        // authored cells.
        let _ = parent;
        let commit = MergeableCommit::new("tasks", row(i), 2000 + step as u64).cells(values);
        let started = Instant::now();
        let foreground = f.foreground.as_mut().unwrap();
        let publication = measure(phases.entry("author").or_default(), foreground, |node| {
            block_on(node.commit_mergeable(commit)).unwrap()
        });
        parents[i] = publication.tx_id();
        measure(phases.entry("persist").or_default(), foreground, |node| {
            support::settle_transaction(node, publication)
        });
        let unit = measure(phases.entry("upload").or_default(), foreground, |node| {
            let unit = block_on(node.commit_unit_for(parents[i])).unwrap();
            let bytes = jazz::wire::encode_sync_message(&unit).unwrap();
            jazz::wire::decode_sync_message(&bytes).unwrap()
        });
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("commit unit")
        };
        let worker = f.worker.as_mut().unwrap();
        measure(phases.entry("worker_ingest").or_default(), worker, |node| {
            block_on(node.ingest_relay_commit_unit(tx, versions)).unwrap()
        });
        let message = measure(phases.entry("publish").or_default(), worker, |node| {
            let message = publish(node, &mut f.peer, &f.schema, false);
            let bytes = jazz::wire::encode_sync_message(&message).unwrap();
            jazz::wire::decode_sync_message_trusted(&bytes).unwrap()
        });
        measure(
            phases.entry("receiver_ingest").or_default(),
            foreground,
            |node| support::apply_and_settle(node, message),
        );
        phases
            .entry("roundtrip")
            .or_default()
            .micros
            .push(started.elapsed().as_micros());
        if (step + 1) % window == 0 || step + 1 == updates {
            // Independent exact-state assertions are outside all phase timers.
            verify(&mut f, &revisions);
            for (name, mut samples) in std::mem::take(&mut phases) {
                samples.micros.sort_unstable();
                let count = samples.micros.len();
                let mut fields = support::phase_fields(name, samples.micros.iter().sum());
                fields.extend(
                    serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(json!({
                        "fixture_revision": 1, "arm": arm, "rows": rows,
                        "updates_completed": step + 1, "samples": count,
                        "max_row_updates": revisions.iter().max().unwrap(),
                        "median_us": samples.micros[count / 2],
                        "history_reads_per_update": samples.history_reads as f64 / count as f64,
                        "reads_per_update": samples.reads as f64 / count as f64,
                        "worker_backend": "rocksdb_wal_no_sync", "author_backend": "memory",
                    }))
                    .unwrap(),
                );
                support::emit_json_line("todo_history_depth", fields);
            }
        }
    }
    f.reopen();
    verify(&mut f, &revisions);
}

pub fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = support::env_usize("JAZZ_HISTORY_ROWS", 1500);
    let updates = support::env_usize("JAZZ_HISTORY_UPDATES", 2000);
    let window = support::env_usize("JAZZ_HISTORY_WINDOW", 500);
    let arms = std::env::var("JAZZ_HISTORY_ARMS").unwrap_or_else(|_| "spread,hot".into());
    for arm in arms.split(',') {
        run(rows, updates, window, arm);
    }
}

#[cfg(test)]
mod tests {
    /// Alice edits the todo fixture, the worker relays each commit, and Bob's
    /// foreground must contain exactly the latest task values, also after reopen.
    /// Direct nodes expose phase/read counters unavailable through JazzClient.
    /// Alice -> worker -> Bob -> close/reopen -> exact task IDs and values.
    #[test]
    fn repeated_edits_preserve_exact_state_and_reopen() {
        for arm in ["spread", "hot", "stale-parent"] {
            super::run(5, 12, 4, arm);
        }
    }
}
