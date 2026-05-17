use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::ServerState;
use super::builder::BuiltServer;
use crate::schema_manager::AppId;

/// A running Jazz server that owns its tasks and metadata.
///
/// Shared abstraction used by both `TestingServer` and `DevServer`.
pub struct HostedServer {
    pub state: Arc<ServerState>,
    task: Option<JoinHandle<()>>,
    shutdown_task: Option<JoinHandle<()>>,
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

        let (serve_shutdown_tx, serve_shutdown_rx) = oneshot::channel();
        let shutdown_state = built.state.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown_state.shutdown.wait_requested().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_state.run_shutdown_finalization().await;
            let _ = serve_shutdown_tx.send(());
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, built.app)
                .with_graceful_shutdown(async {
                    let _ = serve_shutdown_rx.await;
                })
                .await
                .expect("serve jazz server");
        });

        let server = Self {
            state: built.state,
            task: Some(task),
            shutdown_task: Some(shutdown_task),
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

    /// Gracefully shut down: request shutdown, finalize runtime/storage, and await task.
    pub async fn shutdown(&mut self) {
        self.state.shutdown.request_shutdown();
        let shutdown_budget = self.state.shutdown.timeout() * 2 + Duration::from_secs(5);

        if let Some(mut shutdown_task) = self.shutdown_task.take()
            && tokio::time::timeout(shutdown_budget, &mut shutdown_task)
                .await
                .is_err()
        {
            shutdown_task.abort();
            let _ = shutdown_task.await;
        }

        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(shutdown_budget, &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
        }
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
        self.state.shutdown.request_shutdown();
        if let Some(task) = self.shutdown_task.take() {
            task.abort();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
