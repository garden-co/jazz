mod actor;
mod storage;

#[tokio::main]
async fn main() -> rivetkit::prelude::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    actor::registry().start().await
}
