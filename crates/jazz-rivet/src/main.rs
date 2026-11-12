#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    // Runner-pool liveness endpoint. Actor readiness is checked by the provider
    // through /__jazz/host; lifecycle and graceful shutdown belong to RivetKit.
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    let health = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route("/", axum::routing::get(|| async { "ok" })),
        )
        .await
    });
    let result = jazz_rivet::actor::registry().start().await;
    health.abort();
    result
}
