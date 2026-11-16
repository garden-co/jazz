use std::path::PathBuf;

use jazz::tools::AppId;

use crate::auth::Auth;

/// How to open an app.
///
/// ```no_run
/// # async fn open() -> jazz_app_support::Result<()> {
/// use jazz_app_support::{AppFiles, Backend, Config, Storage};
/// let config = Config::new(jazz_app_support::AppId::from_name("todos"))
///     .server("http://localhost:1625")
///     .storage(Storage::Dir("./todo-data".into()))
///     .backend(Backend::new("<backend secret>", "<admin secret>", "todo-service"));
/// let jazz = jazz_app_support::Jazz::open(AppFiles::read(".")?, config).await?;
/// # Ok(()) }
/// ```
///
/// An end user's client connects to the app's Jazz endpoint (its backend's
/// `createJazz` handler) as a browser does, never to the core:
///
/// ```no_run
/// # async fn open() -> jazz_app_support::Result<()> {
/// use jazz_app_support::{AppFiles, Auth, Config, DeviceKey, Storage};
/// let key = DeviceKey::generate()?; // keep it: it is the account
/// let config = Config::new(jazz_app_support::AppId::from_name("todos"))
///     .server("https://todos.example/jazz")
///     .storage(Storage::Dir("./todo-data".into()))
///     .user(Auth::Device(key));
/// let jazz = jazz_app_support::Jazz::open(AppFiles::read(".")?, config).await?;
/// let session = jazz.session().expect("an end user's session");
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    pub app_id: AppId,
    /// The sync server: the core (`http://localhost:1625`) for a backend,
    /// the app's Jazz endpoint (`https://app.example/jazz`) for an end user.
    /// Without one the app runs locally only.
    pub server_url: Option<String>,
    pub storage: Storage,
    /// Open as the app's backend.
    pub backend: Option<Backend>,
    /// Open as an end user of the app, signed in with this.
    pub user: Option<Auth>,
    /// Lets an end user's credentials travel over plain `http://` to a
    /// host other than this machine (a dev server on the LAN). Off: they
    /// go over `https://`, or to `localhost`.
    pub allow_insecure_http: bool,
}

impl Config {
    /// A local-only app kept in memory.
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            server_url: None,
            storage: Storage::Memory,
            backend: None,
            user: None,
            allow_insecure_http: false,
        }
    }

    pub fn server(mut self, url: impl Into<String>) -> Self {
        self.server_url = Some(url.into());
        self
    }

    pub fn storage(mut self, storage: Storage) -> Self {
        self.storage = storage;
        self
    }

    pub fn backend(mut self, backend: Backend) -> Self {
        self.backend = Some(backend);
        self
    }

    pub fn user(mut self, auth: Auth) -> Self {
        self.user = Some(auth);
        self
    }

    /// Opens as a backend client of the app's main backend
    /// (`Config::server` is its `/jazz`), proving itself with this machine
    /// credential, as TypeScript's `createJazzClient({ credential })`: see
    /// [`crate::BackendClientKind`]. The provider is asked for a fresh token
    /// for every connection and request, so short-lived tokens just work.
    pub fn credential(self, provider: impl crate::auth::TokenProvider + 'static) -> Self {
        let Auth::Bearer(bearer) = Auth::bearer(provider) else {
            unreachable!("Auth::bearer is a bearer")
        };
        self.user(Auth::Credential(bearer))
    }

    /// See [`Config::allow_insecure_http`].
    pub fn allow_insecure_http(mut self) -> Self {
        self.allow_insecure_http = true;
        self
    }

    /// Whether an end user's credentials would cross the network in plain
    /// text: `http://` or `ws://` to a host other than this machine.
    pub(crate) fn sends_credentials_in_plain_text(&self) -> bool {
        let Some(url) = self.server_url.as_deref() else {
            return false;
        };
        let Ok(url) = reqwest::Url::parse(url) else {
            return false;
        };
        let host = url.host_str().unwrap_or_default();
        let loopback = host == "localhost"
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        matches!(url.scheme(), "http" | "ws") && !loopback
    }
}

/// Where the client keeps its local store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    /// In memory, for the life of the process.
    Memory,
    /// `jazz.sqlite` in this directory.
    Dir(PathBuf),
}

/// An app's backend, as `createJazz` opens one: it holds the sync server's
/// secrets, publishes the app's permissions bundle at startup, and acts as
/// one user of the app in a session pinned to that bundle.
#[derive(Clone)]
pub struct Backend {
    pub backend_secret: String,
    /// Publishes the bundle (`POST /apps/<id>/admin/bundles`).
    pub admin_secret: String,
    /// The user this backend acts as, named `jazz:backend` as `createJazz`
    /// names its users.
    pub user_id: String,
    /// Confirms switching tables of the published schema to latest-only
    /// history, which drops their past versions for good.
    pub allow_dropping_history: bool,
    /// The app's `jazz.permissions.json`, when the app files opened with
    /// do not carry it (generated bindings embed only the schema):
    /// `include_bytes!("../jazz.permissions.json")`.
    pub permissions: Option<Vec<u8>>,
}

impl Backend {
    pub fn new(
        backend_secret: impl Into<String>,
        admin_secret: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            backend_secret: backend_secret.into(),
            admin_secret: admin_secret.into(),
            user_id: user_id.into(),
            allow_dropping_history: false,
            permissions: None,
        }
    }

    pub fn permissions(mut self, permissions: impl Into<Vec<u8>>) -> Self {
        self.permissions = Some(permissions.into());
        self
    }
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("user_id", &self.user_id)
            .field("allow_dropping_history", &self.allow_dropping_history)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_cross_the_network_in_plain_text_only_off_this_machine() {
        let plain = |url: &str| {
            Config::new(AppId::random())
                .server(url)
                .sends_credentials_in_plain_text()
        };
        for url in [
            "http://app.example/jazz",
            "ws://app.example/jazz",
            "ws://10.0.0.2:4000/jazz",
        ] {
            assert!(plain(url), "{url}");
        }
        for url in [
            "https://app.example/jazz",
            "wss://app.example/jazz",
            "http://localhost:3000/jazz",
            "ws://localhost:3000/jazz",
            "http://127.0.0.1:3000/jazz",
            "ws://[::1]:3000/jazz",
        ] {
            assert!(!plain(url), "{url}");
        }
    }
}
