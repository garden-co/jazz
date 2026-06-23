use gloo_worker::Registrable;
use opfs_btree_bench_harness::sqlite_worker::SqliteWorker;

fn main() {
    SqliteWorker::registrar().register();
}
