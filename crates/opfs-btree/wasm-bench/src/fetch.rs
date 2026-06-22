use gloo_net::http::Request;

/// Fetch a committed fixture (e.g. `objects.kv`) served under `/data/`.
pub async fn fetch_data(base_url: &str, fixture: &str) -> Result<Vec<u8>, String> {
    let base_url = base_url.trim_end_matches('/');
    fetch_bytes(&format!("{base_url}/data/{fixture}")).await
}

async fn fetch_bytes(path: &str) -> Result<Vec<u8>, String> {
    let response = Request::get(path)
        .send()
        .await
        .map_err(|e| format!("fetch {path}: {e}"))?;
    if !response.ok() {
        return Err(format!("fetch {path}: HTTP {}", response.status()));
    }
    response
        .binary()
        .await
        .map_err(|e| format!("read {path}: {e}"))
}
