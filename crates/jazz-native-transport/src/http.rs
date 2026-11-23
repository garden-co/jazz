//! The HTTP clients native Jazz clients make their requests with, and the
//! roots those trust: the OS's beside webpki's, as the WebSocket does.
//!
//! Not reqwest's `rustls-tls-native-roots`: Cargo would turn that on for
//! every reqwest client in the program, the app's own included, and with it
//! building a client fails (`Client::new()` panics) when no certificate in
//! the OS store parses. These roots are only added to Jazz's clients, and
//! only those that parse.

use std::sync::OnceLock;

/// A builder for a client that trusts the OS's roots and webpki's. Every
/// HTTP client of the native client crates starts here.
///
/// The OS store is read once per process, as reading it can be slow; only
/// the roots are shared, as a client's pool belongs to the runtime it is
/// used on.
#[doc(hidden)]
pub fn http_client_builder() -> reqwest::ClientBuilder {
    static OS_ROOTS: OnceLock<Vec<reqwest::Certificate>> = OnceLock::new();
    OS_ROOTS
        .get_or_init(load_os_roots)
        .iter()
        .fold(reqwest::Client::builder(), |builder, root| {
            builder.add_root_certificate(root.clone())
        })
}

/// The OS store's usable roots. None is no error: there is no store to read
/// on Android, iOS or a distroless image, and webpki's roots remain.
fn load_os_roots() -> Vec<reqwest::Certificate> {
    let loaded = rustls_native_certs::load_native_certs();
    let found = loaded.certs.len();
    let mut parsed = rustls::RootCertStore::empty();
    // reqwest fails the whole build on one root that doesn't parse, and OS
    // stores often hold a few.
    let roots: Vec<_> = loaded
        .certs
        .into_iter()
        .filter(|cert| parsed.add(cert.clone()).is_ok())
        .filter_map(|cert| reqwest::Certificate::from_der(&cert).ok())
        .collect();
    if !loaded.errors.is_empty() || (found > 0 && roots.is_empty()) {
        tracing::warn!(
            found,
            usable = roots.len(),
            errors = ?loaded.errors,
            "cannot use all of the OS certificate store: HTTPS requests trust the webpki roots and what could be read"
        );
    }
    roots
}
