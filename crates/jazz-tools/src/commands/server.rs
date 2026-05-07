//! Server command implementation.

use std::net::SocketAddr;

use jazz_tools::middleware::AuthConfig;
use jazz_tools::schema_manager::AppId;
use jazz_tools::server::{CatalogueAuthorityMode, ServerBuilder, StorageBackend};
use tracing::info;

const STANDALONE_INSPECTOR_URL: &str = "https://jazz2-inspector.vercel.app/";

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    upstream_url: Option<String>,
    catalogue_authority: CatalogueAuthorityMode,
    bound_port_file: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app_id = AppId::from_string(app_id_str)?;
    let app_id_string = app_id.to_string();
    let admin_secret = auth_config.admin_secret.clone();

    info!("Starting Jazz server for app: {}", app_id);
    if in_memory {
        info!("Storage mode: in-memory");
    } else {
        info!("Data directory: {}", data_dir);
    }

    let builder = ServerBuilder::new(app_id)
        .with_auth_config(auth_config)
        .with_catalogue_authority(catalogue_authority);
    let builder = match upstream_url {
        Some(upstream_url) => builder.with_upstream_url(upstream_url),
        None => builder,
    };
    let built = if in_memory {
        builder.with_storage(StorageBackend::InMemory).build().await
    } else {
        builder
            .with_storage(StorageBackend::Persistent {
                path: data_dir.into(),
            })
            .build()
            .await
    }
    .map_err(|e| format!("failed to build server: {e}"))?;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;
    let inspector_link =
        build_inspector_link(bound_addr.port(), &app_id_string, admin_secret.as_deref());

    if let Some(path) = bound_port_file {
        std::fs::write(&path, bound_addr.port().to_string())
            .map_err(|e| format!("failed to write bound port file {path}: {e}"))?;
    }

    info!("Listening on http://{}", bound_addr);
    info!("Open the inspector: {}", inspector_link);
    axum::serve(listener, built.app).await?;

    Ok(())
}

fn build_inspector_link(port: u16, app_id: &str, admin_secret: Option<&str>) -> String {
    let server_url = format!("http://localhost:{port}");
    let admin_secret_value = admin_secret.unwrap_or("");
    format!(
        "{STANDALONE_INSPECTOR_URL}#serverUrl={}&appId={}&adminSecret={}",
        percent_encode_fragment_value(&server_url),
        percent_encode_fragment_value(app_id),
        percent_encode_fragment_value(admin_secret_value),
    )
}

fn percent_encode_fragment_value(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len());
    for byte in input.bytes() {
        let is_unreserved =
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        if is_unreserved {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::build_inspector_link;

    #[test]
    fn inspector_link_percent_encodes_fragment_values() {
        let link = build_inspector_link(4200, "app:one/two", Some("secret with spaces"));

        assert_eq!(
            link,
            "https://jazz2-inspector.vercel.app/#serverUrl=http%3A%2F%2Flocalhost%3A4200&appId=app%3Aone%2Ftwo&adminSecret=secret%20with%20spaces"
        );
    }
}
