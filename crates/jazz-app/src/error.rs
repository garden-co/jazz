use jazz::tools::JazzError;

/// Errors from opening or using a Jazz app. More may come: match it with a
/// wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The app's files (`jazz.app.json`, `jazz.permissions.json`) or the
    /// bindings generated from them are missing, stale or invalid.
    #[error("app files: {0}")]
    App(String),

    /// The configuration asks for something this crate cannot do.
    #[error("configuration: {0}")]
    Config(String),

    /// The sync server refused or failed a request made while opening.
    #[error("server: {0}")]
    Server(String),

    /// There is no such row (or it isn't readable).
    #[error("no {table} row {id}")]
    NotFound { table: &'static str, id: String },

    /// A row did not have the shape its generated type expects.
    #[error("row of {table}: {message}")]
    Decode {
        table: &'static str,
        message: String,
    },

    /// The serving peer rejected a subscription.
    #[error("subscription rejected: {0}")]
    Rejected(String),

    /// Something this crate does not support yet.
    #[error("not supported yet: {0}")]
    Unsupported(String),

    /// A streamed value's source failed; the upload was abandoned.
    #[error("upload: {0}")]
    Upload(String),

    /// The call panicked on the client's thread. The client runs on.
    #[error("the call panicked: {0}")]
    Panicked(String),

    /// The local store is in use by another open app (in this process or
    /// another), so it can't be deleted (TS's `StoreHeldByAnotherTabError`).
    #[error("the local store is held: {0}")]
    StoreHeld(String),

    /// The client's worker thread stopped.
    #[error("the Jazz client stopped")]
    Stopped,

    /// The client reported an error.
    #[error(transparent)]
    Jazz(#[from] JazzError),
}

pub type Result<T> = std::result::Result<T, Error>;
