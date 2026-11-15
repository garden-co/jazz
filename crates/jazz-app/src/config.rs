use std::path::PathBuf;

use jazz::tools::AppId;

/// How to open an app.
///
/// ```no_run
/// # async fn open() -> jazz_app::Result<()> {
/// use jazz_app::{AppFiles, Backend, Config, Storage};
/// let config = Config::new(jazz_app::AppId::from_name("todos"))
///     .server("http://localhost:1625")
///     .storage(Storage::Dir("./todo-data".into()))
///     .backend(Backend::new("<backend secret>", "<admin secret>", "todo-service"));
/// let jazz = jazz_app::Jazz::open(AppFiles::read(".")?, config).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    pub app_id: AppId,
    /// The sync server, e.g. `http://localhost:1625`. Without one the app
    /// runs locally only.
    pub server_url: Option<String>,
    pub storage: Storage,
    /// Open as the app's backend.
    pub backend: Option<Backend>,
}

impl Config {
    /// A local-only app kept in memory.
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            server_url: None,
            storage: Storage::Memory,
            backend: None,
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
