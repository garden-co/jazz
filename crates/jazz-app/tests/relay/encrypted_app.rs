use jazz_app_support::{AppFiles, AppId, Backend, Config, Jazz};

use crate::relay::{Relay, start};

/// An encrypted app with a keyless backend and a test endpoint.
pub struct EncryptedApp {
    pub app_id: AppId,
    pub server: jazz_server::JazzServer,
    pub backend: Jazz,
    pub relay: Relay,
}

impl EncryptedApp {
    pub async fn start_with(files: AppFiles) -> Self {
        let server = jazz_server::JazzServer::builder()
            .with_app_id(AppId::random())
            .start()
            .await
            .expect("start a Jazz server");
        Self::on_server(files, server).await
    }

    pub async fn on_server(files: AppFiles, server: jazz_server::JazzServer) -> Self {
        let app_id = server.app_id();
        let backend = Jazz::open(
            files,
            Config::new(app_id)
                .server(server.base_url())
                .backend(Backend::new(
                    server.backend_secret(),
                    server.admin_secret(),
                    "service",
                )),
        )
        .await
        .expect("an encrypted app opens as a backend, which holds no keys");
        let relay = start(
            &server.base_url(),
            app_id,
            server.backend_secret(),
            server.admin_secret(),
            backend.bundle().unwrap(),
        )
        .await;
        Self {
            app_id,
            server,
            backend,
            relay,
        }
    }

    pub async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}
