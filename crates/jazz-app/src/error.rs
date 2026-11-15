use jazz::tools::JazzError;

/// Errors from opening or using a Jazz app.
#[derive(Debug, thiserror::Error)]
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

    /// A row did not have the shape its generated type expects.
    #[error("row of {table}: {message}")]
    Decode {
        table: &'static str,
        message: String,
    },

    /// The serving peer rejected a subscription.
    #[error("subscription rejected: {0}")]
    Rejected(String),

    /// The app's backend does not accept the end user's bearer: their
    /// sign-in ended or the bearer is not one its `authenticate` accepts.
    #[error("signed out: the app's backend does not accept the end user's bearer")]
    SignedOut,

    /// Something this crate does not support yet.
    #[error("not supported yet: {0}")]
    Unsupported(String),

    /// The client's worker thread stopped.
    #[error("the Jazz client stopped")]
    Stopped,

    /// The client reported an error.
    #[error(transparent)]
    Jazz(#[from] JazzError),
}

pub type Result<T> = std::result::Result<T, Error>;
