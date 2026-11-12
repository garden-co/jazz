//! Jazz hosted by a public RivetKit actor and its actor-owned SQLite database.
pub mod actor;
mod scratch;
mod sql;
mod storage;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
pub use storage::{open_jazz, open_store};

pub fn app_id_for_actor(actor_id: &str) -> jazz::tools::AppId {
    let id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("jazz-rivet-v1:{actor_id}").as_bytes(),
    );
    jazz::tools::AppId::from_string(&id.to_string()).expect("UUIDv5 is a UUID")
}

pub fn worker_store(
    db: rivetkit::SqliteDb,
    runtime: tokio::runtime::Handle,
    on_failure: impl Fn(String) + Send + Sync + 'static,
) -> jazz_server::runtime::WorkerStore {
    let opened = Arc::new(AtomicBool::new(false));
    let on_failure: Arc<dyn Fn(String) + Send + Sync> = Arc::new(on_failure);
    jazz_server::runtime::WorkerStore::new(
        move || {
            if opened.swap(true, Ordering::SeqCst) {
                return Err("Restart the actor before reopening Jazz".into());
            }
            let _runtime = runtime.enter();
            futures::executor::block_on(storage::open_store_with_failure(
                db.clone(),
                on_failure.clone(),
            ))
            .map_err(|e| e.to_string())
        },
        || futures::executor::block_on(scratch::open()).map_err(|e| e.to_string()),
    )
}

#[cfg(feature = "testing")]
pub use scratch::open as test_scratch;
#[cfg(feature = "testing")]
pub use storage::test_connection;

#[cfg(test)]
mod tests {
    /// Persisted actor namespaces retain the original JazzRivetAppIdV1 bytes.
    #[test]
    fn app_namespace_v1_fixture() {
        assert_eq!(
            super::app_id_for_actor("fixture-actor").uuid().as_bytes(),
            &[
                0xef, 0xcf, 0xa5, 0xbf, 0xf2, 0x03, 0x5a, 0xe4, 0x9e, 0x3b, 0x82, 0x62, 0x98, 0x54,
                0x06, 0x97
            ]
        );
    }
}
