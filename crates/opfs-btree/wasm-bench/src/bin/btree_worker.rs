use gloo_worker::Registrable;
use opfs_btree_bench_harness::btree_worker::BtreeWorker;

fn main() {
    BtreeWorker::registrar().register();
}
