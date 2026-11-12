use anyhow::Context;
use futures::{SinkExt, StreamExt};
use reqwest::{Method, header::HeaderMap};

/// Alice's backend reaches the real JazzActor through Rivet's gateway. Mallory
/// cannot discover the core without a credential; Alice's account survives an
/// actor sleep/wake, proving the public lifecycle drains Jazz before SQLite.
#[tokio::test(flavor = "multi_thread")]
async fn gateway_authentication_and_sleep_wake() -> anyhow::Result<()> {
    let secret = std::env::var("JAZZ_CORE_SECRET")
        .context("set JAZZ_CORE_SECRET to a test-only secret of at least 32 characters")?;
    let mut registry = rivetkit::Registry::new();
    registry.register_actor_with::<jazz_rivet::actor::JazzActor>(
        "jazz",
        rivetkit::ActorConfig {
            sleep_timeout: std::time::Duration::from_millis(100),
            ..Default::default()
        },
    );
    let runtime = rivetkit::test::setup(registry).await?;
    let actor = runtime.client().get_or_create(
        "jazz",
        vec![uuid::Uuid::new_v4().to_string()],
        Default::default(),
    )?;
    let mut headers = HeaderMap::new();
    headers.insert("x-jazz-backend-credential", secret.parse()?);
    headers.insert("x-jazz-backend-access", "admin".parse()?);
    headers.insert("content-type", "application/json".parse()?);
    let response = actor
        .fetch("/__jazz/host", Method::GET, HeaderMap::new(), None)
        .await?;
    assert_eq!(response.status(), 401);
    let response = actor
        .fetch("/__jazz/host", Method::GET, headers.clone(), None)
        .await?;
    assert_eq!(response.status(), 200);
    let metadata: serde_json::Value = response.json().await?;
    let app = metadata["appId"].as_str().context("appId")?;
    let body = br#"{"identity":{"issuer":"test","subject":"alice"}}"#.to_vec();
    let response = actor
        .fetch(
            &format!("/apps/{app}/admin/accounts/login-or-register"),
            Method::POST,
            headers.clone(),
            Some(body.clone().into()),
        )
        .await?;
    assert_eq!(response.status(), 200, "{}", response.text().await?);
    let account: serde_json::Value = response.json().await?;
    let source = jazz::tools::public_schema::SchemaBuilder::new().build();
    let hash = jazz::tools::public_schema::SchemaHash::compute(&source).to_string();
    let deployed = actor.fetch(&format!("/apps/{app}/admin/bundles"), Method::POST, headers.clone(), Some(serde_json::to_vec(&serde_json::json!({
        "targetSchemaHash": hash, "schemas": [{"hash": hash, "schema": source}], "migrations": [], "permissions": {}
    }))?.into())).await?;
    assert_eq!(deployed.status(), 200, "{}", deployed.text().await?);
    let deployed: serde_json::Value = deployed.json().await?;
    let identity = jazz::ids::AuthorSubject::authenticated("test", "alice")?.with_account(
        jazz::account_registry::AccountId(uuid::Uuid::parse_str(
            account["account"].as_str().context("account")?,
        )?),
    );
    let mut socket = actor.web_socket(&format!("/apps/{app}/ws"), None).await?;
    use tokio_tungstenite::tungstenite::Message;
    socket.send(Message::Binary(serde_json::to_vec(&serde_json::json!({
        "peer_identity": identity.canonical(), "bundle": deployed["bundle"],
        "auth": {"backend_secret": secret, "backend_session": {
            "account_id": account["account"], "issuer": "test", "user_id": "alice", "claims": {}, "authMode": "external"
        }}
    }))?.into())).await?;
    use jazz::wire::{WireFrame, WireHello, WirePeerRole};
    let hello = jazz::wire::encode_frame(&WireFrame::Hello(WireHello::current(
        WirePeerRole::Client,
        jazz::wire::FEATURE_THIN_RESULTS,
    )))?;
    socket
        .send(Message::Binary(postcard::to_allocvec(&vec![hello])?.into()))
        .await?;
    let message = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .context("Jazz socket closed")??;
    let Message::Binary(bytes) = message else {
        anyhow::bail!("expected Jazz hello, got {message:?}");
    };
    let frames: Vec<Vec<u8>> = postcard::from_bytes(&bytes)?;
    assert!(matches!(
        jazz::wire::decode_postcard_frame(&frames[0])?,
        WireFrame::Hello(_)
    ));
    socket.close(None).await?;
    let starts = jazz_rivet::actor::test_start_count();
    // RivetKit's 100ms idle deadline lets the actor sleep between requests.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let response = actor
        .fetch(
            &format!("/apps/{app}/admin/accounts/resolve"),
            Method::POST,
            headers,
            Some(body.into()),
        )
        .await?;
    assert_eq!(response.status(), 200, "{}", response.text().await?);
    assert!(
        jazz_rivet::actor::test_start_count() > starts,
        "actor must actually restart after sleeping"
    );
    runtime.shutdown().await;
    Ok(())
}
