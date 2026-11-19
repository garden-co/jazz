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

    /// A text splice offset is not on a char boundary of the value: Rust
    /// text offsets are UTF-8 bytes (`str` indices), and one inside a code
    /// point is refused rather than rounded.
    #[error("text offset: {0}")]
    TextOffset(String),

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

    /// The server rejected a write: a [`durable`](crate::Jazz::durable)
    /// write's, a transaction's or [`wait_for_sync`](crate::Jazz::wait_for_sync)'s.
    /// `code` and `reason` are TS's (`PersistedWriteRejectedError`): `code`
    /// is `permission_denied`, `serialization_conflict`,
    /// `transaction_conflict`, `client_clock_too_far_ahead` or
    /// `write_rejected`; `reason` says why, readably.
    #[error("write rejected ({code}): {reason}")]
    #[non_exhaustive]
    WriteRejected { code: String, reason: String },

    /// The app's backend refused the request for good: retrying it as it is
    /// will not help (e.g. linking a device account to a user who already
    /// has another account).
    #[error("refused: {0}")]
    Refused(String),

    /// No one is signed in: the token provider gave no bearer, or the app's
    /// backend named no user from it.
    #[error("signed out")]
    SignedOut,

    /// The user has no Jazz account yet: they get one when they sign in
    /// from a client, or with `Endpoint::create_account_for` (feature `unstable-endpoint`).
    #[error("user {0:?} has no Jazz account yet")]
    NoAccount(String),

    /// Closing left writes the server never got, and the store kept them
    /// only in memory: they are lost.
    #[error("closed with {pending} write(s) the server never got, lost with the memory store")]
    Unsynced { pending: usize },

    /// A [`crate::JazzSession`] has no usable client now: it is `status`
    /// (starting, signed out, switching users, failed or closed).
    #[error("no signed-in user: the session is {status}")]
    NoUser {
        status: crate::jazz_session::SessionStatus,
    },

    /// This [`crate::Jazz`] was replaced: its [`crate::JazzSession`]'s
    /// user signed out or changed (TS's `DbReplacedError`). Fetch the
    /// current one with [`crate::JazzSession::jazz`].
    #[error("this client was replaced: the session's user signed out or changed")]
    Replaced,

    /// A trusted backend client's permissions are not the ones its main
    /// backend serves, so it acts for no user ([`crate::Jazz::for_user`]):
    /// it would evaluate their reads by the wrong policies. Open it with
    /// the app's published `jazz.permissions.json`. TS's `bundle_mismatch`.
    #[error(
        "this client's permissions (bundle {own}) are not the ones the main backend serves \
         (bundle {served}); open it with the app's published jazz.permissions.json"
    )]
    BundleMismatch { own: String, served: String },

    /// An end-to-end encryption call failed. `code` is the runtime's
    /// (`approval-answered`, `no-challenge`, `approval-code-mismatch`,
    /// `device-no-longer-active`, `unsupported`, `store`, `closed`,
    /// `database`, ...), or `keyless` / `not-declared` for an app or
    /// backend that has no keys to use.
    #[error("encryption ({code}): {message}")]
    Encryption { code: String, message: String },

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
    Jazz(JazzError),
}

impl From<JazzError> for Error {
    fn from(error: JazzError) -> Self {
        match error {
            JazzError::WriteRejected { code, reason, .. } => Error::WriteRejected { code, reason },
            error => Error::Jazz(error),
        }
    }
}

/// A result whose error is this crate's [`Error`] unless named. The default
/// keeps std's two-parameter form working where the crate (or the generated
/// bindings, which re-export it) is glob-imported: `Result<(), String>`.
pub type Result<T, E = Error> = std::result::Result<T, E>;
