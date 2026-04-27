use crate::transport_manager::StreamAdapter;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, OnceLock};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
};

pub struct NativeWsStream {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

// Build a rustls root store from the OS trust store, falling back to Mozilla's
// bundled webpki-roots only when the native store yields zero usable anchors.
//
// We do NOT union webpki-roots with a populated native store: doing so would
// silently re-introduce CAs that an administrator or enterprise policy has
// distrusted, undermining OS-level trust restrictions on desktop.
//
// The fallback exists for platforms where `rustls-native-certs` cannot reach
// the OS trust store at all — most notably Android (anchors live in
// `AndroidCAStore`, accessible only via JNI), and frequently iOS depending on
// linkage. Without the fallback, every `wss://` handshake on those platforms
// fails with `UnknownIssuer`.
fn root_store() -> rustls::RootCertStore {
    let native = rustls_native_certs::load_native_certs();
    let native_error_count = native.errors.len();
    build_root_store_from_native(native.certs, native_error_count)
}

fn build_root_store_from_native(
    native_certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    native_error_count: usize,
) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    let mut native_added = 0usize;
    for cert in native_certs {
        if roots.add(cert).is_ok() {
            native_added += 1;
        }
    }

    if native_added == 0 {
        tracing::debug!(
            errors = native_error_count,
            "native cert store returned no usable anchors; falling back to bundled webpki-roots"
        );
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    roots
}

fn rustls_connector() -> Connector {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    let config = CONFIG
        .get_or_init(|| {
            let cfg = rustls::ClientConfig::builder()
                .with_root_certificates(root_store())
                .with_no_client_auth();
            Arc::new(cfg)
        })
        .clone();
    Connector::Rustls(config)
}

impl StreamAdapter for NativeWsStream {
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (ws, _) =
            connect_async_tls_with_config(url, None, false, Some(rustls_connector())).await?;
        Ok(Self { inner: ws })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.inner.send(Message::Binary(data.to_owned())).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.inner.next().await {
                Some(Ok(Message::Binary(b))) => return Ok(Some(b)),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(Message::Text(_))) => {
                    tracing::debug!(
                        "received unexpected text frame on binary-only WS connection; ignoring"
                    );
                    continue;
                }
                Some(Ok(Message::Frame(_))) => continue, // raw frame: send-only, cannot arrive on read
                Some(Err(e)) => return Err(e),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.inner.close(None).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn empty_native_store_falls_back_to_bundled_webpki_roots() {
        // Mobile case: OS trust store unreachable / empty. The fallback must
        // populate the store with the bundled Mozilla anchors so wss:// works.
        let roots = build_root_store_from_native(vec![], 3);
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
    }

    #[test]
    fn populated_native_store_does_not_extend_with_bundled_webpki_roots() {
        // Desktop case: OS trust store has anchors. Unioning bundled
        // webpki-roots on top would silently re-introduce CAs that an admin
        // or enterprise policy has distrusted, so the resulting store must
        // contain only the native anchor we provided.
        let cert = rcgen::generate_simple_self_signed(vec!["jazz-tls-fixture.local".into()])
            .expect("rcgen self-signed");
        let native = vec![cert.cert.der().clone()];

        let roots = build_root_store_from_native(native, 0);
        assert_eq!(
            roots.len(),
            1,
            "populated native store must not pull in bundled webpki-roots"
        );
    }

    #[tokio::test]
    async fn native_ws_stream_send_recv_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            let _ = ready_tx.send(());
            while let Some(Ok(msg)) = ws.next().await {
                ws.send(msg).await.unwrap();
            }
        });
        let mut stream = NativeWsStream::connect(&format!("ws://{addr}"))
            .await
            .unwrap();
        ready_rx.await.unwrap();
        stream.send(b"hello ws").await.unwrap();
        assert_eq!(stream.recv().await.unwrap().unwrap(), b"hello ws".to_vec());
    }

    #[tokio::test]
    async fn native_ws_stream_server_close_yields_none_on_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            let _ = ws.close(None).await;
        });
        let mut stream = NativeWsStream::connect(&format!("ws://{addr}"))
            .await
            .unwrap();
        assert!(stream.recv().await.unwrap().is_none());
    }
}
