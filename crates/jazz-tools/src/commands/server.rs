//! Server command implementation.

use std::net::SocketAddr;

use jazz_tools::middleware::AuthConfig;
use jazz_tools::schema_manager::AppId;
use jazz_tools::server::{CatalogueAuthorityMode, ServerBuilder};
use tracing::info;

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    catalogue_authority: CatalogueAuthorityMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let app_id = AppId::from_string(app_id_str)?;

    info!("Starting Jazz server for app: {}", app_id);
    if in_memory {
        info!("Storage mode: in-memory");
    } else {
        info!("Data directory: {}", data_dir);
    }

    let builder = ServerBuilder::new(app_id)
        .with_auth_config(auth_config)
        .with_catalogue_authority(catalogue_authority);
    let built = if in_memory {
        builder.with_in_memory_storage().build().await
    } else {
        builder.with_persistent_storage(data_dir).build().await
    }
    .map_err(|e| format!("failed to build server: {e}"))?;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, built.app).await?;

    Ok(())
}
