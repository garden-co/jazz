//! Opt-in synthetic-workload capture. Never enabled in shipping builds.
//! Internal replay is necessary to isolate container costs from key encoding,
//! output encoding, graph scheduling and storage; it is not an end-to-end gate.
use super::*;
use serde::{Deserialize, Serialize};
use std::io::{BufWriter, Write};
use std::sync::{Mutex, OnceLock};

#[derive(Serialize, Deserialize)]
struct Row(Vec<u8>, Vec<u8>, i64);
#[derive(Serialize, Deserialize)]
struct Case {
    before: Vec<Row>,
    deltas: Vec<Row>,
    replace: bool,
    probes: Vec<Vec<u8>>,
}
fn rows(index: &JoinLookup<'_>) -> Vec<Row> {
    let keys: HashSet<_> = match index {
        JoinLookup::Index(index) => index.keys().cloned().collect(),
        JoinLookup::Arrangement(a) => a.index.keys().chain(a.overlay.keys()).cloned().collect(),
    };
    keys.into_iter()
        .flat_map(|key| {
            index
                .bucket(&key)
                .into_iter()
                .flat_map(|b| b.iter())
                .map(|(r, w)| Row(key.to_vec(), r.to_vec(), *w))
                .collect::<Vec<_>>()
        })
        .collect()
}
fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    crate::ARRANGEMENT_CAPTURE_ACTIVE.load(std::sync::atomic::Ordering::Relaxed)
        && *ENABLED.get_or_init(|| std::env::var_os("GROOVE_ARRANGEMENT_CAPTURE").is_some())
}
fn write(case: Case) {
    static OUTPUT: OnceLock<Mutex<BufWriter<std::fs::File>>> = OnceLock::new();
    let output = OUTPUT.get_or_init(|| {
        Mutex::new(BufWriter::new(
            std::fs::File::create(std::env::var_os("GROOVE_ARRANGEMENT_CAPTURE").unwrap()).unwrap(),
        ))
    });
    let bytes = postcard::to_allocvec(&case).unwrap();
    let mut output = output.lock().unwrap();
    output
        .write_all(&(bytes.len() as u64).to_le_bytes())
        .unwrap();
    output.write_all(&bytes).unwrap();
    // Explicit flush: globals do not drop at process exit.
    output.flush().unwrap();
}
pub(super) fn capture_update(
    a: &ArrangementState,
    deltas: &[KeyedRecordDelta<'_>],
    mode: ArrangementUpdateMode,
) {
    if !enabled() || deltas.is_empty() {
        return;
    }
    write(Case {
        before: rows(&JoinLookup::Arrangement(a)),
        deltas: deltas
            .iter()
            .map(|d| Row(d.key.to_vec(), d.delta.record.to_vec(), d.delta.weight))
            .collect(),
        replace: mode == ArrangementUpdateMode::Replace,
        probes: Vec::new(),
    });
}
pub(super) fn capture_probe(a: &JoinLookup<'_>, deltas: &[KeyedRecordDelta<'_>]) {
    if !enabled() || deltas.is_empty() {
        return;
    }
    write(Case {
        before: rows(a),
        deltas: Vec::new(),
        replace: false,
        probes: deltas
            .iter()
            .filter(|d| d.delta.weight != 0)
            .map(|d| d.key.to_vec())
            .collect(),
    });
}

mod engine {
    use super::*;
    use std::io::{BufReader, Read};
    use std::time::{Duration, Instant};
    #[derive(Clone)]
    struct Sorted {
        keys: Vec<JoinKey>,
        offsets: Vec<usize>,
        records: Vec<Bytes>,
        weights: Vec<i64>,
    }
    impl Sorted {
        fn build(mut rows: Vec<(JoinKey, Bytes, i64)>) -> Self {
            rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let mut merged: Vec<(JoinKey, Bytes, i64)> = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(last) = merged
                    .last_mut()
                    .filter(|last| last.0 == row.0 && last.1 == row.1)
                {
                    last.2 += row.2;
                } else {
                    merged.push(row);
                }
            }
            let mut s = Self {
                keys: Vec::new(),
                offsets: Vec::new(),
                records: Vec::new(),
                weights: Vec::new(),
            };
            for (key, record, weight) in merged.into_iter().filter(|r| r.2 != 0) {
                if s.keys.last() != Some(&key) {
                    s.keys.push(key);
                    s.offsets.push(s.records.len());
                }
                s.records.push(record);
                s.weights.push(weight);
            }
            s.offsets.push(s.records.len());
            s
        }
        fn probe(&self, key: &[u8]) -> impl Iterator<Item = (&Bytes, &i64)> {
            let range = self
                .keys
                .binary_search_by(|k| k.as_slice().cmp(key))
                .ok()
                .map(|i| self.offsets[i]..self.offsets[i + 1])
                .unwrap_or(0..0);
            self.records[range.clone()]
                .iter()
                .zip(self.weights[range].iter())
        }
        fn all(&self) -> Vec<(JoinKey, Bytes, i64)> {
            self.keys
                .iter()
                .enumerate()
                .flat_map(|(i, k)| {
                    (self.offsets[i]..self.offsets[i + 1])
                        .map(move |j| (k.clone(), self.records[j].clone(), self.weights[j]))
                })
                .collect()
        }
    }
    fn inputs(rows: &[Row]) -> (Vec<RecordDelta>, Vec<JoinKey>) {
        (
            rows.iter()
                .map(|r| RecordDelta {
                    record: Bytes::copy_from_slice(&r.1),
                    weight: r.2,
                })
                .collect(),
            rows.iter().map(|r| JoinKey::from_slice(&r.0)).collect(),
        )
    }
    fn keyed<'a>(records: &'a [RecordDelta], keys: &[JoinKey]) -> Vec<KeyedRecordDelta<'a>> {
        records
            .iter()
            .zip(keys)
            .map(|(delta, key)| KeyedRecordDelta {
                delta,
                key: key.clone(),
            })
            .collect()
    }
    fn tuples(rows: &[Row]) -> Vec<(JoinKey, Bytes, i64)> {
        rows.iter()
            .map(|r| (JoinKey::from_slice(&r.0), Bytes::copy_from_slice(&r.1), r.2))
            .collect()
    }
    fn canonical(a: &ArrangementState) -> Vec<(JoinKey, Bytes, i64)> {
        let mut v = tuples(&rows(&JoinLookup::Arrangement(a)));
        v.sort();
        v
    }
    #[test]
    fn sorted_batch_preserves_signed_weights_and_cancellation() {
        // Container-level test: signed intermediate weights are not a public row API.
        let row = |key: &'static [u8], record: &'static [u8], weight| {
            (JoinKey::from_slice(key), Bytes::from_static(record), weight)
        };
        let a = Rc::new(Sorted::build(vec![
            row(b"k", b"a", 2),
            row(b"k", b"a", -1),
            row(b"k", b"b", -3),
        ]));
        let snapshot = a.clone();
        let mut changes = a.all();
        changes.extend([row(b"k", b"a", -1), row(b"k", b"b", 3), row(b"z", b"c", 1)]);
        let b = Sorted::build(changes);
        assert_eq!(b.all(), vec![row(b"z", b"c", 1)]);
        assert_eq!(
            snapshot.all(),
            vec![row(b"k", b"a", 1), row(b"k", b"b", -3)]
        );
        assert_eq!(b.probe(b"k").count(), 0);
        assert_eq!(b.probe(b"missing").count(), 0);
        assert_eq!(b.probe(b"z").count(), 1);
    }
    // This test deliberately targets the private representation: public queries
    // cannot distinguish equivalent containers or isolate their operation costs.
    pub(super) fn run() {
        let path = std::env::var("GROOVE_ARRANGEMENT_REPLAY").expect("capture path");
        let mut reader = BufReader::new(std::fs::File::open(path).unwrap());
        let mut count = 0usize;
        let mut times = [Duration::ZERO; 6];
        let mut update_times = [Duration::ZERO; 4];
        let mut total_rows = 0usize;
        let (mut updates, mut replacements, mut probes) = (0usize, 0usize, 0usize);
        loop {
            let mut length = [0; 8];
            match reader.read_exact(&mut length) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => panic!("{e}"),
            }
            let mut bytes = vec![0; u64::from_le_bytes(length) as usize];
            reader.read_exact(&mut bytes).unwrap();
            let c: Case = postcard::from_bytes(&bytes).unwrap();
            let (records, keys) = inputs(&c.before);
            let before = keyed(&records, &keys);
            let (records_d, keys_d) = inputs(&c.deltas);
            let delta = keyed(&records_d, &keys_d);
            let sorted_input = tuples(&c.before);
            let start = Instant::now();
            let mut baseline = ArrangementState {
                index: Rc::new(build_join_delta_index(&before)),
                overlay: Rc::default(),
            };
            times[0] += start.elapsed();
            let start = Instant::now();
            let mut sorted = Rc::new(Sorted::build(sorted_input));
            times[1] += start.elapsed();
            let snapshot = baseline.clone();
            let sorted_snapshot = sorted.clone();
            if !c.deltas.is_empty() {
                let start = Instant::now();
                baseline.apply_update(
                    &delta,
                    if c.replace {
                        ArrangementUpdateMode::Replace
                    } else {
                        ArrangementUpdateMode::Accumulate
                    },
                );
                let elapsed = start.elapsed();
                times[2] += elapsed;
                update_times[if c.replace { 0 } else { 2 }] += elapsed;
                let updates = tuples(&c.deltas);
                let start = Instant::now();
                let mut merged = if c.replace { Vec::new() } else { sorted.all() };
                merged.extend(updates);
                sorted = Rc::new(Sorted::build(merged));
                let elapsed = start.elapsed();
                times[3] += elapsed;
                update_times[if c.replace { 1 } else { 3 }] += elapsed;
            }
            let start = Instant::now();
            for key in &c.probes {
                if let Some(b) = baseline.bucket(key) {
                    for pair in b.iter() {
                        std::hint::black_box(pair);
                    }
                }
            }
            times[4] += start.elapsed();
            let start = Instant::now();
            for key in &c.probes {
                for pair in sorted.probe(key) {
                    std::hint::black_box(pair);
                }
            }
            times[5] += start.elapsed();
            for key in &c.probes {
                let mut expected = baseline
                    .bucket(key)
                    .into_iter()
                    .flat_map(|b| b.iter())
                    .map(|(r, w)| (r.clone(), *w))
                    .collect::<Vec<_>>();
                expected.sort();
                let actual = sorted
                    .probe(key)
                    .map(|(r, w)| (r.clone(), *w))
                    .collect::<Vec<_>>();
                assert_eq!(expected, actual, "probe case {count}");
            }
            assert_eq!(canonical(&baseline), sorted.all(), "case {count}");
            assert_eq!(
                canonical(&snapshot),
                sorted_snapshot.all(),
                "snapshot {count}"
            );
            updates += usize::from(!c.deltas.is_empty());
            replacements += usize::from(c.replace);
            probes += c.probes.len();
            total_rows += c.before.len() + c.deltas.len();
            count += 1;
        }
        println!(
            "updates={updates} replacements={replacements} probes={probes} cases={count} rows={total_rows} baseline_build={:?} sorted_build={:?} baseline_update={:?} sorted_update={:?} baseline_probe={:?} sorted_probe={:?}",
            times[0], times[1], times[2], times[3], times[4], times[5]
        );
        println!(
            "replace baseline={:?} sorted={:?}; accumulate baseline={:?} sorted={:?}",
            update_times[0], update_times[1], update_times[2], update_times[3]
        );
        assert!(count > 0);
    }
}

pub(crate) fn run() {
    engine::run();
}
