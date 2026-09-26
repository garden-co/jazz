//! Native CPU receipt for resident selective ranges in the browser B-tree.
//! The backing store has no artificial latency. These phases do not predict
//! application startup latency; cold store work is reported separately.
use futures::executor::block_on;
use idb_tree::{
    BoxFuture, Commit, IdbTree, MemoryPageStore, Metadata, Options, PageStore, WriteOperation,
};
use std::{cell::Cell, hint::black_box, rc::Rc, time::Instant};

#[derive(Clone, Default)]
struct CountingStore {
    memory: MemoryPageStore,
    calls: Rc<Cell<usize>>,
    pages: Rc<Cell<usize>>,
}

impl CountingStore {
    fn reset(&self) {
        self.calls.set(0);
        self.pages.set(0);
    }
}

impl PageStore for CountingStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }

    fn read_page(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.calls.set(self.calls.get() + 1);
        self.pages.set(self.pages.get() + 1);
        self.memory.read_page(id)
    }

    fn read_pages<'a>(
        &'a self,
        ids: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, String>> {
        self.calls.set(self.calls.get() + 1);
        self.pages.set(self.pages.get() + ids.len());
        Box::pin(async move {
            let mut pages = Vec::with_capacity(ids.len());
            for &id in ids {
                pages.push(self.memory.read_page(id).await?);
            }
            Ok(pages)
        })
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        self.memory.commit(commit)
    }
}

fn key(value: u64) -> Vec<u8> {
    let mut key = b"anonymous-index/".to_vec();
    key.extend_from_slice(&value.to_be_bytes());
    key.extend_from_slice(&[0; 8]);
    key
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    block_on(async {
        const ROWS: u64 = 20_000;
        const READS: u64 = 20_000;
        for page_size in [1024, Options::default().page_size] {
            let options = Options { page_size };
            let store = CountingStore::default();
            let seed_started = Instant::now();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            for batch in 0..20 {
                let writes = (batch * 1000..(batch + 1) * 1000)
                    .map(|index| WriteOperation::Set {
                        key: key(index * 4),
                        value: vec![index as u8; 48],
                    })
                    .collect();
                tree.write_many(writes).await.unwrap();
                tree.flush().await.unwrap();
            }
            drop(tree);
            let seed_ms = elapsed_ms(seed_started);
            let reopen_started = Instant::now();
            let tree = IdbTree::open(store.clone(), options).await.unwrap();
            let reopen_ms = elapsed_ms(reopen_started);

            // Prepare identical, distributed bounds outside the measured loop.
            let bounds: Vec<_> = (0..READS)
                .map(|index| {
                    let id = (index * 7919) % ROWS;
                    (id, key(id * 4), key(id * 4 + 1), key(id * 4 + 2))
                })
                .collect();
            store.reset();
            let cold_started = Instant::now();
            let mut cold_rows = 0;
            for (_, start, end, _) in bounds.iter().take(64) {
                cold_rows += black_box(tree.range(start, end).await.unwrap()).len();
            }
            let cold_ms = elapsed_ms(cold_started);
            let cold_calls = store.calls.get();
            let cold_pages = store.pages.get();

            let warmup_started = Instant::now();
            black_box(tree.range(&key(0), &key(ROWS * 4)).await.unwrap());
            let warmup_ms = elapsed_ms(warmup_started);
            for phase in ["hit", "miss", "reverse_hit", "bounded_prefix", "broad"] {
                store.reset();
                let mut returned_rows = 0;
                let mut signature = 0_u64;
                let phase_started = Instant::now();
                let requests = if phase == "broad" { 16 } else { READS };
                for (id, start, end, missing_end) in bounds.iter().take(requests as usize) {
                    let rows = match phase {
                        "hit" => tree.range(start, end).await.unwrap(),
                        "miss" => tree.range(end, missing_end).await.unwrap(),
                        "reverse_hit" => tree.range_reverse(start, end, usize::MAX).await.unwrap(),
                        "bounded_prefix" => {
                            tree.range_limit(start, &key(ROWS * 4), 7).await.unwrap()
                        }
                        "broad" => tree.range(&key(0), &key(ROWS * 4)).await.unwrap(),
                        _ => unreachable!(),
                    };
                    returned_rows += rows.len();
                    // This deliberately small receipt consumes the same output
                    // bytes in both arms. Boundary correctness lives in tests.
                    for (key, value) in &rows {
                        signature = signature.wrapping_add(
                            *id + u64::from(key[key.len() - 9]) + u64::from(value[0]),
                        );
                    }
                    black_box(rows);
                }
                let phase_ms = elapsed_ms(phase_started);
                println!(
                    "{{\"benchmark\":\"selective_ranges\",\"page_size\":{page_size},\"rows\":{ROWS},\"requests\":{requests},\"phase\":\"{phase}\",\"seed_ms\":{seed_ms:.3},\"reopen_ms\":{reopen_ms:.3},\"cold_ms\":{cold_ms:.3},\"cold_rows\":{cold_rows},\"cold_calls\":{cold_calls},\"cold_pages\":{cold_pages},\"warmup_ms\":{warmup_ms:.3},\"phase_ms\":{phase_ms:.3},\"page_calls\":{},\"pages\":{},\"returned_rows\":{returned_rows},\"signature\":{signature}}}",
                    store.calls.get(),
                    store.pages.get()
                );
            }
        }
    });
}
