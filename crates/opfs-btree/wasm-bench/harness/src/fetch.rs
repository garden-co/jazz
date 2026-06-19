use gloo_net::http::Request;

pub async fn fetch_dataset(profile: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let kv = fetch_bytes(&format!("/data/{profile}.kv")).await?;
    let ops = fetch_bytes(&format!("/data/{profile}.ops")).await?;
    Ok((kv, ops))
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
