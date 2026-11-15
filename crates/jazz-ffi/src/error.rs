//! The one error type both surfaces report.

/// What a call failed with. The field is `reason`, not `message`: in
/// Kotlin, `message` would clash with `Throwable.message`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, uniffi::Error)]
pub enum JazzError {
    /// Validation, permissions, or something not supported yet.
    #[error("{reason}")]
    Refused { reason: String },
    /// Storage, the network or the sync server failed.
    #[error("{reason}")]
    Io { reason: String },
    /// The app was closed.
    #[error("the app is closed")]
    Closed,
}

pub(crate) type FfiError = JazzErrorKind;

/// Internal shorthand, converted into [`JazzError`] at the surfaces.
#[derive(Debug)]
pub(crate) enum JazzErrorKind {
    Refused(String),
    Io(String),
    Closed,
}

impl From<JazzErrorKind> for JazzError {
    fn from(kind: JazzErrorKind) -> Self {
        match kind {
            JazzErrorKind::Refused(reason) => Self::Refused { reason },
            JazzErrorKind::Io(reason) => Self::Io { reason },
            JazzErrorKind::Closed => Self::Closed,
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, JazzErrorKind>;

pub(crate) fn refused(message: impl Into<String>) -> JazzErrorKind {
    JazzErrorKind::Refused(message.into())
}

/// `Refused` for what jazz-app (or this layer) cannot do yet.
pub(crate) fn unsupported(what: impl std::fmt::Display) -> JazzErrorKind {
    JazzErrorKind::Refused(format!("not supported yet: {what}"))
}

impl From<jazz_app::Error> for JazzErrorKind {
    fn from(error: jazz_app::Error) -> Self {
        use jazz_app::Error as E;
        use jazz_app::jazz::tools::JazzError as J;
        let message = error.to_string();
        match error {
            E::App(_) | E::Config(_) | E::Decode { .. } | E::Rejected(_) => Self::Refused(message),
            E::Unsupported(what) => unsupported(what),
            E::Server(_) | E::Panicked(_) => Self::Io(message),
            E::Stopped => Self::Closed,
            E::Jazz(J::ChannelClosed) => Self::Closed,
            E::Jazz(J::Query(_) | J::Write(_) | J::Schema(_) | J::Json(_) | J::Identity(_)) => {
                Self::Refused(message)
            }
            E::Jazz(_) => Self::Io(message),
        }
    }
}
