//! Server command implementation.

use std::time::Duration;

use jazz_server::AuthConfig;

/// Run the Jazz server.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    bound_port_file: Option<String>,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    jazz_server::run(
        app_id_str,
        port,
        data_dir,
        in_memory,
        auth_config,
        bound_port_file,
        shutdown_timeout,
    )
    .await
}
