//! A server that trusts a workload admits only requests carrying that
//! workload's OIDC token, found through the issuer's OIDC discovery.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
use jazz_server::JazzServer;
use jazz_server::middleware::workload::TrustedWorkload;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;

const SIGNING_KEY: &str = "synthetic-workload-signing-key-0123456789";
const SUBJECT: &str = "owner:garden-co:project:app:environment:production";

async fn start_issuer() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let jwks_uri = format!("{base}/jwks");
    let router = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(move || {
                let jwks_uri = jwks_uri.clone();
                async move { Json(json!({ "jwks_uri": jwks_uri })) }
            }),
        )
        .route(
            "/jwks",
            get(|| async {
                let k = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(SIGNING_KEY);
                Json(json!({ "keys": [{ "kty": "oct", "kid": "w1", "alg": "HS256", "k": k }] }))
            }),
        );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    base
}

fn token(issuer: &str, subject: &str) -> String {
    let exp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(600);
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("w1".to_owned());
    encode(
        &header,
        &json!({ "iss": issuer, "aud": "https://vercel.com/garden-co", "sub": subject, "exp": exp.as_secs() }),
        &EncodingKey::from_secret(SIGNING_KEY.as_bytes()),
    )
    .unwrap()
}

#[tokio::test]
async fn admits_only_the_trusted_workload() {
    let issuer = start_issuer().await;
    let server = JazzServer::builder()
        .with_trusted_workload(TrustedWorkload {
            issuer: issuer.clone(),
            audience: "https://vercel.com/garden-co".to_owned(),
            subject: SUBJECT.to_owned(),
        })
        .start()
        .await
        .expect("server starts");
    let http = reqwest::Client::new();
    let app = format!("{}/apps/{}", server.base_url(), server.app_id());
    let good = token(&issuer, SUBJECT);
    let other = token(&issuer, "owner:garden-co:project:app:environment:preview");

    // Health stays open.
    let health = http
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    // Without a token, with another workload's token, or with a guessed
    // secret, nothing is admitted.
    let bare = http.get(format!("{app}/schemas")).send().await.unwrap();
    assert_eq!(bare.status(), 401);
    assert!(bare.text().await.unwrap().contains("workload"));
    let wrong = http
        .get(format!("{app}/schemas"))
        .header("X-Jazz-Workload-Token", &other)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401);
    assert!(wrong.text().await.unwrap().contains("workload"));
    let guessed = http
        .get(format!("{app}/admin/permissions"))
        .header("X-Jazz-Admin-Secret", "admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(guessed.status(), 401);

    // The workload's token passes the gate on its own (the route then asks
    // for its own credential), and stands in for the admin secret.
    let passed = http
        .get(format!("{app}/schemas"))
        .header("X-Jazz-Workload-Token", &good)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!passed.contains("workload"), "{passed}");
    let admin = http
        .get(format!("{app}/admin/permissions"))
        .header("X-Jazz-Admin-Secret", &good)
        .send()
        .await
        .unwrap();
    assert_eq!(admin.status(), 200, "{}", admin.text().await.unwrap());
}

#[tokio::test]
async fn opens_sockets_only_for_the_trusted_workload() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let issuer = start_issuer().await;
    let server = JazzServer::builder()
        .with_trusted_workload(TrustedWorkload {
            issuer: issuer.clone(),
            audience: "https://vercel.com/garden-co".to_owned(),
            subject: SUBJECT.to_owned(),
        })
        .start()
        .await
        .expect("server starts");
    let url = format!(
        "{}/apps/{}/ws",
        server.base_url().replacen("http", "ws", 1),
        server.app_id()
    );

    let refused = tokio_tungstenite::connect_async(url.as_str()).await;
    assert!(
        refused.is_err(),
        "a socket without the workload's token is refused"
    );

    // A client can't claim the gate's own vouching either.
    let mut forged = url.as_str().into_client_request().unwrap();
    forged.headers_mut().insert(
        "X-Jazz-Internal-Workload-Vouched",
        "anything".parse().unwrap(),
    );
    assert!(tokio_tungstenite::connect_async(forged).await.is_err());

    let mut request = url.as_str().into_client_request().unwrap();
    request.headers_mut().insert(
        "X-Jazz-Workload-Token",
        token(&issuer, SUBJECT).parse().unwrap(),
    );
    let (_socket, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("the workload opens a socket");
    assert_eq!(response.status(), 101);
}
