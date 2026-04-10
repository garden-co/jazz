use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::ServerState;
use super::builder::BuiltServer;
use crate::schema_manager::AppId;

/// A running Jazz server that owns its task, shutdown channel, and metadata.
///
/// Shared abstraction used by both `TestingServer` and `DevServer`.
pub struct HostedServer {
    pub state: Arc<ServerState>,
    task: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    pub port: u16,
    pub app_id: AppId,
    pub data_dir: PathBuf,
    pub admin_secret: Option<String>,
    pub backend_secret: Option<String>,
}

impl HostedServer {
    /// Bind a listener, spawn the axum server task, wait for health, and return.
    pub async fn start(
        built: BuiltServer,
        port: Option<u16>,
        app_id: AppId,
        data_dir: PathBuf,
        admin_secret: Option<String>,
        backend_secret: Option<String>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port.unwrap_or(0)))
            .await
            .expect("bind server listener");
        let port = listener.local_addr().expect("local addr").port();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, built.app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve jazz server");
        });

        let server = Self {
            state: built.state,
            task: Some(task),
            shutdown_tx: Some(shutdown_tx),
            port,
            app_id,
            data_dir,
            admin_secret,
            backend_secret,
        };
        server.wait_ready().await;
        server
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Gracefully shut down: flush runtime, send shutdown signal, await task,
    /// flush+close storage, close external identity store.
    pub async fn shutdown(&mut self) {
        self.state
            .runtime
            .flush()
            .await
            .expect("flush server runtime");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(Duration::from_millis(500), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
        }

        self.state
            .runtime
            .with_storage(|storage| {
                storage.flush();
                storage.flush_wal();
                let _ = storage.close();
            })
            .expect("flush and close server storage");

        self.state
            .external_identity_store
            .close()
            .await
            .expect("close external identity store");
    }

    async fn wait_ready(&self) {
        let client = reqwest::Client::new();
        let health_url = format!("{}/health", self.base_url());
        for _ in 0..80 {
            if let Ok(response) = client.get(&health_url).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("jazz server did not become ready in time");
    }
}

impl Drop for HostedServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
