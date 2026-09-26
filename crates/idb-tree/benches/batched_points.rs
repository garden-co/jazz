//! Native mechanism receipt for browser cold point-read fan-out. Each page
//! future yields once, exposing concurrent misses without invented I/O latency.
//! Wall time is local CPU work, not an IndexedDB/app startup prediction.
use futures::{
    executor::block_on,
    future::{join_all, poll_fn},
    task::Poll,
};
use idb_tree::{BoxFuture, Commit, IdbTree, MemoryPageStore, Metadata, Options, PageStore};
use std::{cell::Cell, rc::Rc, time::Instant};

#[derive(Clone, Default)]
struct YieldingStore {
    memory: MemoryPageStore,
    reads: Rc<Cell<usize>>,
    bytes: Rc<Cell<usize>>,
}
impl PageStore for YieldingStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        self.memory.load_metadata()
    }
    fn read_page(&self, id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        self.reads.set(self.reads.get() + 1);
        Box::pin(async move {
            let bytes = self.memory.read_page(id).await?;
            self.bytes
                .set(self.bytes.get() + bytes.as_ref().map_or(0, Vec::len));
            let mut yielded = false;
            poll_fn(|cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
            Ok(bytes)
        })
    }
    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        self.memory.commit(commit)
    }
}
fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    block_on(async {
        const ROWS: u32 = 5000;
        const READS: u32 = 1000;
        for payload in [80, 1500] {
            let seed_started = Instant::now();
            let store = YieldingStore::default();
            let tree = IdbTree::open(store.clone(), Options::default())
                .await
                .unwrap();
            for key in 0..ROWS {
                tree.put(key.to_be_bytes().to_vec(), vec![key as u8; payload])
                    .await
                    .unwrap();
            }
            tree.flush().await.unwrap();
            drop(tree);
            let seed_ms = seed_started.elapsed().as_secs_f64() * 1000.;
            for (run, bulk) in [false, true, true, false]
                .into_iter()
                .cycle()
                .take(12)
                .enumerate()
            {
                let reopen_started = Instant::now();
                let tree = IdbTree::open(store.clone(), Options::default())
                    .await
                    .unwrap();
                let reopen_ms = reopen_started.elapsed().as_secs_f64() * 1000.;
                // Startup already loaded the root before indexed row hydration.
                tree.get(&0_u32.to_be_bytes()).await.unwrap();
                store.reads.set(0);
                store.bytes.set(0);
                let ids: Vec<_> = (0..READS).map(|index| (index * 73) % ROWS).collect();
                let keys: Vec<_> = ids.iter().map(|id| id.to_be_bytes().to_vec()).collect();
                let read_started = Instant::now();
                let rows = if bulk {
                    tree.get_many_required(&keys).await.unwrap().unwrap()
                } else {
                    join_all(keys.iter().map(|key| tree.get(key)))
                        .await
                        .into_iter()
                        .map(|row| row.unwrap().unwrap())
                        .collect()
                };
                let read_ms = read_started.elapsed().as_secs_f64() * 1000.;
                let correct = rows.iter().zip(ids.iter()).all(|(row, id)| {
                    row.len() == payload && row.iter().all(|value| *value == *id as u8)
                });
                println!(
                    "{{\"benchmark\":\"concurrent_cold_points\",\"run\":{run},\"bulk\":{bulk},\"rows\":{ROWS},\"point_reads\":{READS},\"payload_bytes\":{payload},\"seed_ms\":{seed_ms:.3},\"reopen_ms\":{reopen_ms:.3},\"read_ms\":{read_ms:.3},\"page_reads\":{},\"page_bytes\":{},\"rows_match\":{correct}}}",
                    store.reads.get(),
                    store.bytes.get()
                );
                if !correct {
                    std::process::exit(1);
                }
            }
        }
    });
}
