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
    /// Contract 2: something this layer or jazz-app-support doesn't support yet.
    #[error("{reason}")]
    NotSupported { reason: String },
    /// Contract 2: a serializable transaction conflicted with another write;
    /// retrying it may succeed.
    #[error("{reason}")]
    Conflict { reason: String },
    /// Contract 2: a trusted backend client acted for a user who has no
    /// account yet (TS's `NoJazzAccountError`, `code: "no_account"`).
    #[error("{reason}")]
    NoAccount { user_id: String, reason: String },
    /// Contract 2: closing with a timeout left `pending` writes unsent
    /// (memory storage, where they are lost).
    #[error("{reason}")]
    Unsynced { pending: u64, reason: String },
    /// Contract 2: this handle's client was replaced, as its session's user
    /// signed out or changed (TS's `DbReplacedError`): fetch the current
    /// one from the session.
    #[error("{reason}")]
    Replaced { reason: String },
    /// Contract 2: the session has no usable client now; `status` is the
    /// session's (`signedOut`, `transitioning`, …).
    #[error("{reason}")]
    NoUser { status: String, reason: String },
    /// Contract 2: a trusted backend client's `for_user` refused because
    /// the client's permissions bundle (`own`) isn't the one the main
    /// backend serves (`served`): reopen it with the app's published
    /// `jazz.permissions.json` (TS's `code: "bundle_mismatch"`).
    #[error("{reason}")]
    BundleMismatch {
        own: String,
        served: String,
        reason: String,
    },
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
            | Self::Conflict { reason }
            | Self::NoAccount { reason, .. } => Self::Refused { reason },
            Self::Unsynced { reason, .. } => Self::Io { reason },
            Self::Replaced { .. } => Self::Closed,
            Self::NoUser { reason, .. } => Self::Refused { reason },
            Self::BundleMismatch { reason, .. } => Self::Refused { reason },
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
            Self::NoAccount { .. } => "noAccount",
            Self::Unsynced { .. } => "unsynced",
            Self::Replaced { .. } => "replaced",
            Self::NoUser { .. } => "noUser",
            Self::BundleMismatch { .. } => "bundleMismatch",
        }
    }
}

/// A foreign callback that failed in a way its signature doesn't name.
impl From<uniffi::UnexpectedUniFFICallbackError> for JazzError {
    fn from(error: uniffi::UnexpectedUniFFICallbackError) -> Self {
        Self::Io {
            reason: error.reason,
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
    NoAccount {
        user_id: String,
        reason: String,
    },
    Unsynced {
        pending: u64,
        reason: String,
    },
    BundleMismatch {
        own: String,
        served: String,
        reason: String,
    },
    Replaced(String),
    NoUser {
        status: String,
        reason: String,
    },
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
            JazzErrorKind::NoAccount { user_id, reason } => Self::NoAccount { user_id, reason },
            JazzErrorKind::Unsynced { pending, reason } => Self::Unsynced { pending, reason },
            JazzErrorKind::Replaced(reason) => Self::Replaced { reason },
            JazzErrorKind::NoUser { status, reason } => Self::NoUser { status, reason },
            JazzErrorKind::BundleMismatch {
                own,
                served,
                reason,
            } => Self::BundleMismatch {
                own,
                served,
                reason,
            },
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, JazzErrorKind>;

pub(crate) fn refused(message: impl Into<String>) -> JazzErrorKind {
    JazzErrorKind::Refused(message.into())
}

/// `Refused` for what jazz-app-support (or this layer) cannot do yet.
pub(crate) fn unsupported(what: impl std::fmt::Display) -> JazzErrorKind {
    let what = what.to_string();
    // jazz-app-support's own messages may already say it.
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

/// jazz-app-support's `NotFound` displays as `no {table} row {id}`.
fn is_not_found(message: &str) -> bool {
    message.contains("not found")
        || message
            .strip_prefix("no ")
            .is_some_and(|rest| rest.contains(" row "))
}

impl From<jazz_app_support::Error> for JazzErrorKind {
    fn from(error: jazz_app_support::Error) -> Self {
        use jazz_app_support::Error as E;
        use jazz_app_support::jazz::tools::JazzError as J;
        let message = error.to_string();
        // Only what contract 1 reports as `Refused` is told apart, so a
        // contract 1 host sees exactly what it did before.
        match error {
            E::Rejected(reason) => rejection(&reason, message),
            E::WriteRejected { code, .. } => match code.as_str() {
                "permission_denied" => Self::Permission(message),
                "serialization_conflict" | "transaction_conflict" => Self::Conflict(message),
                _ => Self::Refused(message),
            },
            E::Unsupported(what) => unsupported(what),
            // An encryption call's failure keeps its code in the message
            // (`encryption (approval-code-mismatch): ...`) for the host's
            // wrapper to name; a store or database failure may pass.
            E::Encryption { code, .. } if code == "store" || code == "database" => {
                Self::Io(message)
            }
            E::Encryption { .. } => Self::Refused(message),
            // E::Refused: the backend refused who the client is (a credential
            // trusted neither way, a refused link); `for_user` reports its
            // 403 as `Permission` itself.
            E::App(_) | E::Config(_) | E::Decode { .. } | E::Refused(_) => Self::Refused(message),
            E::Server(_) | E::Panicked(_) => Self::Io(message),
            E::Stopped => Self::Closed,
            // Another open app holds the store (deleteClientStorage): a
            // refusal, not a failure to retry.
            E::StoreHeld(_) => Self::Refused(message),
            E::NoAccount(user_id) => Self::NoAccount {
                user_id,
                reason: message,
            },
            E::Replaced => Self::Replaced(message),
            E::NoUser { status } => Self::NoUser {
                status: status.to_string(),
                reason: message,
            },
            E::BundleMismatch { own, served } => Self::BundleMismatch {
                own,
                served,
                reason: message,
            },
            E::Unsynced { pending } => Self::Unsynced {
                pending: pending as u64,
                reason: message,
            },
            // A bearer the backend names no one by (e.g. a rejected link):
            // a refusal, not a network blip to retry.
            E::SignedOut => Self::Refused(message),
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
            // Variants jazz-app-support adds (it is becoming `#[non_exhaustive]`)
            // until this layer names them: an edit of a missing row
            // (`NotFound`) is a refusal; anything else, such as a host
            // stream's failure (`Upload`), is I/O.
            _ if is_not_found(&message) => Self::Refused(message),
            _ => Self::Io(message),
        }
    }
}

/// White-box: jazz-app-support's `NotFound` doesn't exist on main yet, so its
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
