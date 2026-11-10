//! Actor storage uses Rivet's unmodified asynchronous SQLite worker.
//! JazzRivetAppIdV1 is unchanged: UUIDv5(URL, UTF-8("jazz-rivet-v1:" + actor id)).
mod actor;
mod scratch;
mod sql;
mod storage;
pub use actor::ActorStore;
pub use sql::{Store, Transaction};
use sqlite_embedded::vfs::SqliteTransportHandle;
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

/// One worker per actor generation. A crashed server must restart the actor;
/// it must never silently reopen stale or ambiguously committed storage.
pub fn worker_store(
    transport: SqliteTransportHandle,
    actor_id: String,
    generation: u64,
    runtime: tokio::runtime::Handle,
) -> jazz_server::runtime::WorkerStore {
    let opened = Arc::new(AtomicBool::new(false));
    let scratch_runtime = runtime.clone();
    jazz_server::runtime::WorkerStore::new(
        move || {
            if opened.swap(true, Ordering::SeqCst) {
                return Err("Restart the actor before reopening its SQLite worker".into());
            }
            let _runtime = runtime.enter();
            futures::executor::block_on(open_store(transport.clone(), actor_id.clone(), generation))
                .map_err(|e| e.to_string())
        },
        move || {
            let _runtime = scratch_runtime.enter();
            futures::executor::block_on(open_store(
                Arc::new(scratch::Scratch::default()),
                format!("jazz-scratch-{}", uuid::Uuid::new_v4()),
                1,
            ))
            .map_err(|error| error.to_string())
        },
    )
}

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
