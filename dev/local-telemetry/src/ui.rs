use std::path::{Component, Path, PathBuf};

pub struct StaticAsset {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
}

pub async fn static_asset(web_dist: &Path, request_path: &str) -> Result<StaticAsset, String> {
    let relative = normalize_request_path(request_path)?;
    let path = web_dist.join(relative);

    let bytes = tokio::fs::read(&path).await.map_err(|_| {
        format!(
            "viewer assets not found; run `trunk build --release` in {}",
            web_dist.parent().unwrap_or(web_dist).display()
        )
    })?;
    let content_type = mime_guess::from_path(path)
        .first_raw()
        .unwrap_or("application/octet-stream");

    Ok(StaticAsset {
        bytes,
        content_type,
    })
}

fn normalize_request_path(request_path: &str) -> Result<PathBuf, String> {
    let trimmed = request_path.trim_start_matches('/');
    let trimmed = if trimmed.is_empty() {
        "index.html"
    } else {
        trimmed
    };
    let candidate = PathBuf::from(trimmed);
    if candidate.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err("invalid asset path".to_string());
    }
    Ok(candidate)
}
