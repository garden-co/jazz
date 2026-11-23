//! HTTPS with a private CA, for the tests of which roots a client trusts
//! (shared with jazz-app's).
//!
//! The OS store is read once per process and is chosen by environment, so a
//! test runs its body in a child process of this test binary, started with
//! `SSL_CERT_FILE` naming the store under test:
//!
//! ```text
//! test ──writes CA, leaf, store──► tempdir
//!   └──spawns──► same test ──► client ──TLS──► proxy ──plain──► stand-in
//!                 (SSL_CERT_FILE)               (leaf)
//! ```
#![allow(dead_code)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// Set in a child: the directory its parent prepared.
const CHILD: &str = "JAZZ_TEST_TLS_DIR";

/// What the OS store holds.
#[derive(Clone, Copy, Debug)]
pub enum Store {
    PrivateCa,
    /// The private CA, among a certificate that does not parse.
    PrivateCaAmongInvalid,
    /// A CA that did not sign the server's certificate.
    AnotherCa,
    Empty,
    Missing,
    /// It is there, and reading it fails.
    Unreadable,
    AllInvalid,
}

/// The server's side: its certificate, issued by the private CA.
#[derive(Clone)]
pub struct Tls {
    dir: PathBuf,
}

/// A TLS terminator in front of a plain server.
pub struct Proxy {
    pub url: String,
    handshakes: Arc<AtomicUsize>,
}

impl Proxy {
    /// Connections whose client accepted the certificate.
    pub fn handshakes(&self) -> usize {
        self.handshakes.load(Ordering::SeqCst)
    }
}

/// Runs `body` with the OS store as each of `stores`, a child process each.
/// `test` is the calling test's name.
pub fn with_os_store<F: Future<Output = ()>>(
    test: &str,
    stores: &[Store],
    body: impl FnOnce(Tls) -> F,
) {
    if let Some(dir) = std::env::var_os(CHILD) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        return tokio::task::LocalSet::new().block_on(&runtime, body(Tls { dir: dir.into() }));
    }
    for &store in stores {
        let dir = tempfile::tempdir().unwrap();
        let store_file = prepare(dir.path(), store);
        let output = Command::new(std::env::current_exe().unwrap())
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD, dir.path())
            .env("SSL_CERT_FILE", store_file)
            .env_remove("SSL_CERT_DIR")
            .output()
            .unwrap();
        let (stdout, stderr) = (
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            output.status.success(),
            "{test}, OS store {store:?}:\n{stdout}\n{stderr}"
        );
        // A name that matches no test passes, having run none.
        assert!(
            stdout.contains(" 1 passed"),
            "{test} did not run:\n{stdout}"
        );
    }
}

fn prepare(dir: &Path, store: Store) -> PathBuf {
    let ca = authority("Jazz test CA");
    let key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .unwrap()
        .signed_by(&key, &ca.0, &ca.1)
        .unwrap();
    std::fs::write(dir.join("leaf.der"), leaf.der()).unwrap();
    std::fs::write(dir.join("leaf.key"), key.serialize_der()).unwrap();

    let path = dir.join("store.pem");
    let invalid = pem(b"not a certificate");
    let contents = match store {
        Store::PrivateCa => pem(ca.0.der()),
        Store::PrivateCaAmongInvalid => invalid + &pem(ca.0.der()),
        Store::AnotherCa => pem(authority("Another CA").0.der()),
        Store::Empty => String::new(),
        Store::Missing => return path,
        // A directory opens and fails to read, whoever runs the test; file
        // modes wouldn't stop root.
        Store::Unreadable => return dir.to_owned(),
        Store::AllInvalid => invalid,
    };
    std::fs::write(&path, contents).unwrap();
    path
}

fn authority(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
    let mut params = rcgen::CertificateParams::default();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    let key = rcgen::KeyPair::generate().unwrap();
    (params.self_signed(&key).unwrap(), key)
}

fn pem(der: &[u8]) -> String {
    let base64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut pem = "-----BEGIN CERTIFICATE-----\n".to_owned();
    for line in base64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(line).unwrap());
        pem.push('\n');
    }
    pem + "-----END CERTIFICATE-----\n"
}

impl Tls {
    /// Serves `plain` (an `http://host:port[/path]` URL) over HTTPS, with
    /// the private CA's certificate.
    pub async fn proxy(&self, plain: &str) -> Proxy {
        let read = |name| std::fs::read(self.dir.join(name)).unwrap();
        proxy(
            plain,
            CertificateDer::from(read("leaf.der")),
            PrivatePkcs8KeyDer::from(read("leaf.key")).into(),
        )
        .await
    }

    /// Serves `plain` with a forgery in the name of ISRG Root X2, one of
    /// the webpki roots. A client refuses it either way, but says why: a bad
    /// signature if it trusts that root, an unknown issuer if it does not.
    /// That tells whether the webpki roots are there, without a network.
    pub async fn proxy_forging_a_webpki_root(&self, plain: &str) -> Proxy {
        use rcgen::{DnType, DnValue};
        let mut root = rcgen::CertificateParams::default();
        root.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        root.distinguished_name = rcgen::DistinguishedName::new();
        // Names compare as DER, and this root's are printable strings.
        for (kind, value) in [
            (DnType::CountryName, "US"),
            (DnType::OrganizationName, "Internet Security Research Group"),
            (DnType::CommonName, "ISRG Root X2"),
        ] {
            let value = DnValue::PrintableString(value.try_into().unwrap());
            root.distinguished_name.push(kind, value);
        }
        // The root's algorithm (P-384), so it's the signature that is wrong.
        let root_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P384_SHA384).unwrap();
        let root = root.self_signed(&root_key).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
            .unwrap()
            .signed_by(&key, &root, &root_key)
            .unwrap();
        proxy(
            plain,
            leaf.der().clone(),
            PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .await
    }
}

async fn proxy(plain: &str, cert: CertificateDer<'static>, key: PrivateKeyDer<'static>) -> Proxy {
    // Named, as a test build may have more than one crypto provider.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let plain = plain.strip_prefix("http://").expect("a plain URL");
    let (upstream, path) = plain.split_at(plain.find('/').unwrap_or(plain.len()));
    let upstream = upstream.to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}{path}", listener.local_addr().unwrap());
    let handshakes = Arc::new(AtomicUsize::new(0));
    let accepted = handshakes.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let (acceptor, upstream) = (acceptor.clone(), upstream.clone());
            let accepted = accepted.clone();
            tokio::spawn(async move {
                let Ok(mut client) = acceptor.accept(socket).await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                let Ok(mut server) = tokio::net::TcpStream::connect(upstream).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            });
        }
    });
    Proxy { url, handshakes }
}
