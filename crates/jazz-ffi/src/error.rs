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
    /// The app was closed, or (for a subscription) the subscription ended.
    #[error("the app is closed")]
    Closed,
    /// Contract 2: the app's permissions don't allow it.
    #[error("{reason}")]
    Permission { reason: String },
    /// Contract 2: something this layer or jazz-app doesn't support yet.
    #[error("{reason}")]
    NotSupported { reason: String },
    /// Contract 2: a serializable transaction conflicted with another write;
    /// retrying it may succeed.
    #[error("{reason}")]
    Conflict { reason: String },
}

impl JazzError {
    /// For a host speaking contract 1, the kinds it knows: the contract 2
    /// kinds are refusals.
    pub(crate) fn for_contract(self, contract: u64) -> Self {
        if contract >= 2 {
            return self;
        }
        match self {
            Self::Permission { reason }
            | Self::NotSupported { reason }
            | Self::Conflict { reason } => Self::Refused { reason },
            other => other,
        }
    }

    /// The error JSON's `kind`.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Refused { .. } => "refused",
            Self::Io { .. } => "io",
            Self::Closed => "closed",
            Self::Permission { .. } => "permission",
            Self::NotSupported { .. } => "notSupported",
            Self::Conflict { .. } => "conflict",
        }
    }
}

pub(crate) type FfiError = JazzErrorKind;

/// Internal shorthand, converted into [`JazzError`] at the surfaces.
#[derive(Debug)]
pub(crate) enum JazzErrorKind {
    Refused(String),
    Io(String),
    Closed,
    Permission(String),
    NotSupported(String),
    Conflict(String),
}

impl From<JazzErrorKind> for JazzError {
    fn from(kind: JazzErrorKind) -> Self {
        match kind {
            JazzErrorKind::Refused(reason) => Self::Refused { reason },
            JazzErrorKind::Io(reason) => Self::Io { reason },
            JazzErrorKind::Closed => Self::Closed,
            JazzErrorKind::Permission(reason) => Self::Permission { reason },
            JazzErrorKind::NotSupported(reason) => Self::NotSupported { reason },
            JazzErrorKind::Conflict(reason) => Self::Conflict { reason },
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, JazzErrorKind>;

pub(crate) fn refused(message: impl Into<String>) -> JazzErrorKind {
    JazzErrorKind::Refused(message.into())
}

/// `Refused` for what jazz-app (or this layer) cannot do yet.
pub(crate) fn unsupported(what: impl std::fmt::Display) -> JazzErrorKind {
    let what = what.to_string();
    // jazz-app's own messages may already say it.
    if what.starts_with("not supported yet") {
        JazzErrorKind::NotSupported(what)
    } else {
        JazzErrorKind::NotSupported(format!("not supported yet: {what}"))
    }
}

/// Whether a rejection's reason is a permission denial, or a serializable
/// transaction's conflict. The engine reports a write's rejection reason
/// (`Reason::as_str`) inside its message; a typed reason is a follow-up.
pub(crate) fn rejection(reason: &str, otherwise: String) -> JazzErrorKind {
    if reason.contains("authorization_denied") || reason.contains("AuthorizationDenied") {
        JazzErrorKind::Permission(otherwise)
    } else if reason.contains("serialization_conflict")
        || reason.contains("SerializationConflict")
        || reason.contains("transaction_conflict")
    {
        JazzErrorKind::Conflict(otherwise)
    } else {
        JazzErrorKind::Refused(otherwise)
    }
}

/// jazz-app's `NotFound` displays as `no {table} row {id}`.
fn is_not_found(message: &str) -> bool {
    message.contains("not found")
        || message
            .strip_prefix("no ")
            .is_some_and(|rest| rest.contains(" row "))
}

impl From<jazz_app::Error> for JazzErrorKind {
    fn from(error: jazz_app::Error) -> Self {
        use jazz_app::Error as E;
        use jazz_app::jazz::tools::JazzError as J;
        let message = error.to_string();
        // Only what contract 1 reports as `Refused` is told apart, so a
        // contract 1 host sees exactly what it did before.
        #[allow(unreachable_patterns)]
        match error {
            E::Rejected(reason) => rejection(&reason, message),
            E::Unsupported(what) => unsupported(what),
            E::App(_) | E::Config(_) | E::Decode { .. } => Self::Refused(message),
            E::Server(_) | E::Panicked(_) => Self::Io(message),
            E::Stopped => Self::Closed,
            E::Jazz(J::ChannelClosed) => Self::Closed,
            // The authority rejected a write that was waiting for a tier:
            // a refusal, not a transport failure.
            E::Jazz(J::Sync(reason)) if reason.contains("was rejected") => {
                rejection(&reason, message)
            }
            E::Jazz(J::Write(reason)) => rejection(&reason, message),
            E::Jazz(J::Query(_) | J::Schema(_) | J::Json(_) | J::Identity(_)) => {
                Self::Refused(message)
            }
            E::Jazz(_) => Self::Io(message),
            // Variants jazz-app adds (it is becoming `#[non_exhaustive]`)
            // until this layer names them: an edit of a missing row
            // (`NotFound`) is a refusal; anything else, such as a host
            // stream's failure (`Upload`), is I/O.
            _ if is_not_found(&message) => Self::Refused(message),
            _ => Self::Io(message),
        }
    }
}

/// White-box: jazz-app's `NotFound` doesn't exist on main yet, so its
/// message shape is checked directly.
#[cfg(test)]
mod tests {
    #[test]
    fn not_found_messages_are_recognised() {
        assert!(super::is_not_found(
            "no tasks row 01a1e10a-d57a-7b12-830f-67c6d339645c"
        ));
        assert!(super::is_not_found("row not found"));
        assert!(!super::is_not_found("no connection to the server"));
    }
}
