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
