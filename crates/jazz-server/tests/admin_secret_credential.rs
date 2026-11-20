//! On a server with an admin secret, that secret doubles as the backend
//! credential: a backend (jazz-tools `remote()`) presents it in
//! `X-Jazz-Backend-Credential` and states its access, so it needs no other
//! secret. Requests without a credential are admitted as before.

use jazz_server::JazzServer;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[tokio::test]
async fn the_admin_secret_is_a_backend_credential() {
    let server = JazzServer::builder().start().await.expect("server starts");
    let http = reqwest::Client::new();
    let bundles = format!(
        "{}/apps/{}/admin/bundles",
        server.base_url(),
        server.app_id()
    );
    let admin = server.admin_secret().to_owned();
    let get = |headers: Vec<(&'static str, String)>| {
        let mut request = http.get(&bundles);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        async move { request.send().await.unwrap().status() }
    };

    // The admin secret as the credential, stating admin access.
    assert_eq!(
        get(vec![
            ("X-Jazz-Backend-Credential", admin.clone()),
            ("X-Jazz-Backend-Access", "admin".to_owned()),
        ])
        .await,
        200
    );
    // Backend access (or none) doesn't reach admin routes, and a secret the
    // request offers itself next to the credential never counts.
    for access in ["backend", ""] {
        let mut headers = vec![
            ("X-Jazz-Backend-Credential", admin.clone()),
            ("X-Jazz-Admin-Secret", admin.clone()),
        ];
        if !access.is_empty() {
            headers.push(("X-Jazz-Backend-Access", access.to_owned()));
        }
        assert_eq!(get(headers).await, 401, "access {access:?}");
    }
    // A wrong credential is refused outright, as is the backend secret.
    for wrong in [
        "not-the-secret".to_owned(),
        server.backend_secret().to_owned(),
    ] {
        assert_eq!(
            get(vec![
                ("X-Jazz-Backend-Credential", wrong),
                ("X-Jazz-Backend-Access", "admin".to_owned()),
            ])
            .await,
            401
        );
    }
    // Without a credential, the admin secret header works as before.
    assert_eq!(get(vec![("X-Jazz-Admin-Secret", admin.clone())]).await, 200);
    assert_eq!(get(vec![]).await, 401);
}

#[tokio::test]
async fn sockets_take_the_access_their_upgrade_states() {
    let server = JazzServer::builder().start().await.expect("server starts");
    let url = format!(
        "{}/apps/{}/ws",
        server.base_url().replacen("http", "ws", 1),
        server.app_id()
    );
    // A wrong credential on the upgrade is refused.
    let mut wrong = url.as_str().into_client_request().unwrap();
    wrong
        .headers_mut()
        .insert("X-Jazz-Backend-Credential", "nope".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(wrong).await.is_err());

    let mut request = url.as_str().into_client_request().unwrap();
    let headers = request.headers_mut();
    headers.insert(
        "X-Jazz-Backend-Credential",
        server.admin_secret().parse().unwrap(),
    );
    headers.insert("X-Jazz-Backend-Access", "backend".parse().unwrap());
    let (_socket, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("the backend opens a socket");
    assert_eq!(response.status(), 101);

    // Sockets without a credential still open as before.
    let (_plain, response) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .expect("a plain socket opens");
    assert_eq!(response.status(), 101);
}

// jazz#5553: a core configured with an admin secret derives the backend
// signing key from it, so it is the same on every core process; only the
// app's backend (the admin secret, or it as the credential stating admin
// access) gets it.
#[tokio::test]
async fn derives_the_backend_signing_key_from_the_admin_secret() {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    let server = JazzServer::builder().start().await.expect("server starts");
    let http = reqwest::Client::new();
    let url = format!(
        "{}/apps/{}/admin/backend-signing-key",
        server.base_url(),
        server.app_id()
    );
    let admin = server.admin_secret().to_owned();
    let get = |headers: Vec<(&'static str, String)>| {
        let mut request = http.get(&url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        async move {
            let response = request.send().await.unwrap();
            let status = response.status().as_u16();
            let cache = response
                .headers()
                .get("cache-control")
                .map(|value| value.to_str().unwrap().to_owned());
            (status, cache, response.text().await.unwrap())
        }
    };

    let mut expected = Hmac::<sha2::Sha256>::new_from_slice(admin.as_bytes()).unwrap();
    expected.update(format!("jazz-backend-signing-v1\0{}", server.app_id()).as_bytes());
    let expected = serde_json::json!({
        "key": base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(expected.finalize().into_bytes())
    })
    .to_string();
    for headers in [
        vec![("X-Jazz-Admin-Secret", admin.clone())],
        vec![
            ("X-Jazz-Backend-Credential", admin.clone()),
            ("X-Jazz-Backend-Access", "admin".to_owned()),
        ],
    ] {
        let (status, cache, body) = get(headers).await;
        assert_eq!((status, body.as_str()), (200, expected.as_str()));
        assert_eq!(cache.as_deref(), Some("no-store"));
    }
    for headers in [
        vec![],
        vec![("X-Jazz-Backend-Secret", server.backend_secret().to_owned())],
        vec![("X-Jazz-Admin-Secret", "not-the-secret".to_owned())],
        vec![
            ("X-Jazz-Backend-Credential", admin.clone()),
            ("X-Jazz-Backend-Access", "backend".to_owned()),
        ],
        vec![("X-Jazz-Backend-Credential", admin.clone())],
    ] {
        let (status, _, body) = get(headers.clone()).await;
        assert_eq!(status, 401, "{headers:?}");
        assert!(!body.contains("\"key\""), "{body}");
    }
}
