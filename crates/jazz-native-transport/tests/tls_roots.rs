//! The roots of the native clients' HTTP clients. At this level because a
//! client's errors don't say why a certificate was refused.

mod tls;

use tls::{Store, Tls, with_os_store};

async fn plain_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let router = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    url
}

async fn refusal(http: reqwest::Client, url: &str) -> String {
    format!("{:?}", http.get(url).send().await.expect_err("is refused"))
}

/// Alice's client builds whatever is in the OS store, and has the webpki
/// roots beside it.
#[test]
fn the_webpki_roots_are_trusted_whatever_the_os_store_holds() {
    with_os_store(
        "the_webpki_roots_are_trusted_whatever_the_os_store_holds",
        &[
            Store::PrivateCa,
            Store::Empty,
            Store::Missing,
            Store::Unreadable,
            Store::AllInvalid,
        ],
        |tls: Tls| async move {
            let forged = tls.proxy_forging_a_webpki_root(&plain_server().await).await;
            let http = jazz_native_transport::http_client_builder()
                .build()
                .expect("builds");
            // It found the root, and so the forgery.
            let error = refusal(http, &forged.url).await;
            assert!(error.contains("BadSignature"), "{error}");
            // Without webpki's roots there is no such issuer.
            let no_roots = reqwest::Client::builder()
                .tls_built_in_root_certs(false)
                .build()
                .unwrap();
            let error = refusal(no_roots, &forged.url).await;
            assert!(error.contains("UnknownIssuer"), "{error}");
        },
    );
}

/// A CA the OS trusts is trusted, also beside one that doesn't parse.
#[test]
fn the_os_roots_are_trusted() {
    with_os_store(
        "the_os_roots_are_trusted",
        &[Store::PrivateCa, Store::PrivateCaAmongInvalid],
        |tls: Tls| async move {
            let https = tls.proxy(&plain_server().await).await;
            let http = jazz_native_transport::http_client_builder()
                .build()
                .expect("builds");
            assert!(
                http.get(&https.url)
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success()
            );
        },
    );
}

/// Mallory's CA, in no store, is refused as an unknown issuer.
#[test]
fn a_ca_nobody_trusts_is_refused() {
    with_os_store(
        "a_ca_nobody_trusts_is_refused",
        &[
            Store::AnotherCa,
            Store::Empty,
            Store::Missing,
            Store::Unreadable,
            Store::AllInvalid,
        ],
        |tls: Tls| async move {
            let https = tls.proxy(&plain_server().await).await;
            let http = jazz_native_transport::http_client_builder()
                .build()
                .expect("builds");
            let error = refusal(http, &https.url).await;
            assert!(error.contains("UnknownIssuer"), "{error}");
            assert_eq!(https.handshakes(), 0);
        },
    );
}
