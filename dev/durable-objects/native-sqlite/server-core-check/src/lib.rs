//! Compile-only consumer of the shared server runtime on managed WASM storage.
//! This is not a Worker host: the host must supply an open Db, admission,
//! transport, wakeups, and persistence-aware scheduling.
use jazz::db::Db;
use jazz_server::runtime::ServerCore;

pub fn attach(db: Db, wake: std::task::Waker) -> Result<ServerCore, String> {
    ServerCore::from_db(db, None, wake)
}

pub async fn tick(core: &mut ServerCore) {
    core.advance().await;
    core.flush();
}

pub fn enqueue(
    core: &mut ServerCore,
    session: jazz_server::runtime::ServerSession,
    request: jazz::thin::ThinRequest,
    ingress: jazz_server::runtime::ThinIngress,
) {
    core.thin_request(session, request, ingress);
}
