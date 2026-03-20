//! Server command implementation.

use std::net::SocketAddr;

use jazz_tools::middleware::AuthConfig;
use jazz_tools::schema_manager::AppId;
use jazz_tools::server::ServerBuilder;
use tracing::info;

const STANDALONE_INSPECTOR_URL: &str = "https://jazz2-inspector.vercel.app/";

/// Run the Jazz server.
pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let app_id = AppId::from_string(app_id_str)?;
    let app_id_string = app_id.to_string();
    let inspector_link =
        build_inspector_link(port, &app_id_string, auth_config.admin_secret.as_deref());

    info!("Starting Jazz server for app: {}", app_id);
    if in_memory {
        info!("Storage mode: in-memory");
    } else {
        info!("Data directory: {}", data_dir);
    }

    let builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
    let built = if in_memory {
        builder.with_in_memory_storage().build().await
    } else {
        builder.with_persistent_storage(data_dir).build().await
    }
    .map_err(|e| format!("failed to build server: {e}"))?;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on http://{}", addr);
    info!("Open the inspector: {}", inspector_link);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, built.app).await?;

    Ok(())
}

fn build_inspector_link(port: u16, app_id: &str, admin_secret: Option<&str>) -> String {
    let server_url = format!("http://localhost:{port}");
    let admin_secret_value = admin_secret.unwrap_or("");
    format!(
        "{STANDALONE_INSPECTOR_URL}#url={}&appId={}&adminSecret={}",
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
            "https://jazz2-inspector.vercel.app/#url=http%3A%2F%2Flocalhost%3A4200&appId=app%3Aone%2Ftwo&adminSecret=secret%20with%20spaces"
        );
    }
}
