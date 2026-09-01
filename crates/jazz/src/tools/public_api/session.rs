//! Session context for policy evaluation.
//!
//! A Session represents the authenticated user's context, containing:
//! - `issuer` + `user_id`: Required authenticated JWT `iss` and `sub`
//! - `claims`: JSON object with user-defined claims (roles, teams, etc.)
//! - `auth_mode`: First-class auth-mode discriminator derived from the JWT `iss`

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::ids::AuthorSubject;
use crate::tools::metadata::SYSTEM_PRINCIPAL_ID;
use crate::tools::transaction::OpenTransactionId;

/// Auth mode derived from the JWT's `iss` claim.
///
/// Mirrors the TS `authMode` union used by the client context.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    #[default]
    External,
    LocalFirst,
    Anonymous,
}

/// Construct the one canonical policy-binding vocabulary for an admitted
/// identity and its provider claims.
///
/// Provider values always remain namespaced below `session.claims.*`. The
/// identity-derived `session.claims.iss`/`sub`, `session.user`, and
/// `session.authMode` values are overwritten here, after provider claims, so a
/// transport caller cannot create a competing identity revision by spelling a
/// reserved field in its claim object.

/// Session context for policy evaluation.
///
/// Contains the authenticated user's identity and claims. Used by policy
/// expressions to check row access permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// Validated JWT issuer (`iss`).
    pub issuer: String,
    /// Required user identifier.
    pub user_id: String,
    /// User-defined claims as a JSON object (e.g., `{"teams": ["eng", "design"]}`).
    pub claims: JsonValue,
    /// Auth mode (external / local-first / anonymous). Derived from JWT `iss`.
    /// Accepts `authMode` (camelCase) on the wire for parity with the TS client.
    #[serde(default, alias = "authMode")]
    pub auth_mode: AuthMode,
}

impl Session {
    /// Return the canonical author subject admitted by this session's auth mode.
    /// Reserved issuers are valid only for their matching first-party modes.
    pub fn author_subject(&self) -> Result<AuthorSubject, crate::ids::AuthorSubjectError> {
        match self.auth_mode {
            AuthMode::External => AuthorSubject::authenticated(&self.issuer, self.get_user_id()),
            AuthMode::LocalFirst if self.issuer == AuthorSubject::LOCAL_FIRST_ISSUER => {
                AuthorSubject::reserved(&self.issuer, self.get_user_id())
            }
            AuthMode::Anonymous if self.issuer == AuthorSubject::ANONYMOUS_ISSUER => {
                AuthorSubject::reserved(&self.issuer, self.get_user_id())
            }
            _ => Err(crate::ids::AuthorSubjectError::ReservedIssuer(
                self.issuer.clone(),
            )),
        }
    }

    fn is_auth_mode_path(path: &[String]) -> bool {
        matches!(path, [segment] if segment == "auth_mode" || segment == "authMode")
    }

    /// Create a session from a validated issuer and subject.
    pub fn new(issuer: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            user_id: user_id.into(),
            claims: JsonValue::Object(serde_json::Map::new()),
            auth_mode: AuthMode::External,
        }
    }

    /// Create a session with user ID and claims.
    pub fn with_claims(mut self, claims: JsonValue) -> Self {
        self.claims = claims;
        self
    }

    /// Set the auth mode.
    pub fn with_auth_mode(mut self, auth_mode: AuthMode) -> Self {
        self.auth_mode = auth_mode;
        self
    }

    /// Get a value at the given path.
    ///
    /// Path segments:
    /// - `["claims", "key"]` -> returns claims.key
    /// - `["claims", "nested", "key"]` -> returns claims.nested.key
    pub fn get_path(&self, path: &[String]) -> Option<&JsonValue> {
        if path.is_empty() {
            return None;
        }

        if Self::is_auth_mode_path(path) {
            // auth_mode is enum-typed, not JsonValue — use get_string() instead
            return None;
        }

        if path[0] == "claims" {
            let mut current = &self.claims;
            for segment in &path[1..] {
                match current {
                    JsonValue::Object(map) => {
                        current = map.get(segment)?;
                    }
                    _ => return None,
                }
            }
            return Some(current);
        }

        None
    }

    /// Get the user_id value.
    pub fn get_user_id(&self) -> &str {
        &self.user_id
    }

    /// Get an array at the given path.
    ///
    /// Returns None if the path doesn't exist or isn't an array.
    pub fn get_array(&self, path: &[String]) -> Option<&Vec<JsonValue>> {
        self.get_path(path).and_then(|v| v.as_array())
    }

    /// Check if a value exists at the given path.
    pub fn has_path(&self, path: &[String]) -> bool {
        if path.is_empty() {
            return false;
        }
        if Self::is_auth_mode_path(path) {
            return true;
        }
        self.get_path(path).is_some()
    }

    /// Get a string value at the given path.
    ///
    /// Returns the JSON string if present.
    pub fn get_string(&self, path: &[String]) -> Option<&str> {
        if path.is_empty() {
            return None;
        }
        if Self::is_auth_mode_path(path) {
            return Some(match self.auth_mode {
                AuthMode::External => "external",
                AuthMode::LocalFirst => "local-first",
                AuthMode::Anonymous => "anonymous",
            });
        }
        self.get_path(path).and_then(|v| v.as_str())
    }

    /// Check if an array at the given path contains a specific string value.
    pub fn array_contains_string(&self, path: &[String], value: &str) -> bool {
        self.get_array(path)
            .map(|arr| arr.iter().any(|v| v.as_str() == Some(value)))
            .unwrap_or(false)
    }
}

/// Write-scoped context for mutations.
///
/// `session` controls permission evaluation. `attribution`, when present,
/// controls who is recorded as the commit author without changing permission
/// identity. `updated_at`, when present, overrides the row provenance
/// physical-millisecond timestamp recorded for update-like writes. Core packs
/// this input into an HLC with a zero logical counter. Public reads expose the
/// same Unix-millisecond value; packed HLCs remain internal ordering state. It
/// cannot be combined with `transaction_id`, whose staged-write API has no
/// per-write timestamp slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WriteContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<Session>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
    #[serde(
        rename = "transaction_id",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub transaction_id: Option<OpenTransactionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_branch_name: Option<String>,
}

impl WriteContext {
    pub fn from_session(session: Session) -> Self {
        Self {
            session: Some(session),
            attribution: None,
            updated_at: None,
            transaction_id: None,
            target_branch_name: None,
        }
    }

    /// Override provenance for a non-transactional upsert or update.
    ///
    /// The value is physical milliseconds, not a packed HLC value. Combining
    /// this with [`Self::with_transaction_id`] is rejected because staged
    /// writes do not yet carry a per-write timestamp override.
    pub fn with_updated_at(mut self, updated_at: u64) -> Self {
        self.updated_at = Some(updated_at);
        self
    }

    pub fn with_transaction_id(mut self, transaction_id: OpenTransactionId) -> Self {
        self.transaction_id = Some(transaction_id);
        self
    }

    pub fn with_target_branch_name(mut self, target_branch_name: impl Into<String>) -> Self {
        self.target_branch_name = Some(target_branch_name.into());
        self
    }

    pub fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    pub fn transaction_id(&self) -> Option<OpenTransactionId> {
        self.transaction_id
    }

    pub fn target_branch_name(&self) -> Option<&str> {
        self.target_branch_name.as_deref()
    }

    pub fn updated_at(&self) -> Option<u64> {
        self.updated_at
    }

    pub fn author_principal(&self) -> &str {
        self.attribution
            .as_deref()
            .or_else(|| {
                self.session
                    .as_ref()
                    .map(|session| session.user_id.as_str())
            })
            .unwrap_or(SYSTEM_PRINCIPAL_ID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_session_user_id() {
        let session = Session::new("urn:jazz:test", "user123");
        assert_eq!(session.get_user_id(), "user123");
        assert_eq!(session.get_string(&["user_id".into()]), None);
        assert_eq!(session.get_string(&["userId".into()]), None);
        assert!(!session.has_path(&["user_id".into()]));
        assert!(!session.has_path(&["userId".into()]));
    }

    #[test]
    fn test_session_claims() {
        let session = Session::new("urn:jazz:test", "user123").with_claims(json!({
            "teams": ["eng", "design"],
            "role": "admin",
            "nested": {
                "value": 42
            }
        }));

        // Simple claim
        assert_eq!(
            session.get_string(&["claims".into(), "role".into()]),
            Some("admin")
        );

        // Nested claim
        let nested_path = vec!["claims".into(), "nested".into(), "value".into()];
        assert_eq!(session.get_path(&nested_path), Some(&json!(42)));

        // Array claim
        let teams_path = vec!["claims".into(), "teams".into()];
        assert!(session.has_path(&teams_path));
        let teams = session.get_array(&teams_path).unwrap();
        assert_eq!(teams.len(), 2);

        // Array contains
        assert!(session.array_contains_string(&teams_path, "eng"));
        assert!(session.array_contains_string(&teams_path, "design"));
        assert!(!session.array_contains_string(&teams_path, "sales"));
    }

    #[test]
    fn test_session_missing_paths() {
        let session = Session::new("urn:jazz:test", "user123");

        // Non-existent claim
        assert!(!session.has_path(&["claims".into(), "missing".into()]));
        assert_eq!(
            session.get_string(&["claims".into(), "missing".into()]),
            None
        );

        // Invalid path
        assert!(!session.has_path(&[]));
        assert_eq!(session.get_path(&[]), None);
    }

    #[test]
    fn test_write_context_author_principal_prefers_attribution() {
        let context = WriteContext {
            session: Some(Session::new("urn:jazz:test", "session-user")),
            attribution: Some("attributed-user".into()),
            updated_at: None,
            transaction_id: None,
            target_branch_name: None,
        };

        assert_eq!(context.author_principal(), "attributed-user");
    }

    #[test]
    fn test_write_context_author_principal_falls_back_to_session_then_system() {
        let session_context =
            WriteContext::from_session(Session::new("urn:jazz:test", "session-user"));
        assert_eq!(session_context.author_principal(), "session-user");

        let system_context = WriteContext::default();
        assert_eq!(system_context.author_principal(), SYSTEM_PRINCIPAL_ID);
    }

    #[test]
    fn test_write_context_transaction_id_override() {
        let transaction_id = OpenTransactionId::new();
        let context = WriteContext::from_session(Session::new("urn:jazz:test", "session-user"))
            .with_transaction_id(transaction_id);

        assert_eq!(context.transaction_id(), Some(transaction_id));
    }

    #[test]
    fn test_write_context_target_branch_name_override() {
        let context = WriteContext::from_session(Session::new("urn:jazz:test", "session-user"))
            .with_target_branch_name("dev-111111111111-main");

        assert_eq!(context.target_branch_name(), Some("dev-111111111111-main"));
    }

    #[test]
    fn test_write_context_updated_at_override() {
        let context = WriteContext::from_session(Session::new("urn:jazz:test", "session-user"))
            .with_updated_at(42);

        assert_eq!(context.updated_at(), Some(42));
    }

    #[test]
    fn session_auth_mode_defaults_to_external() {
        let session = Session::new("urn:jazz:test", "user-1");
        assert_eq!(session.auth_mode, AuthMode::External);
    }

    #[test]
    fn session_auth_mode_roundtrips_through_serde() {
        for mode in [
            AuthMode::External,
            AuthMode::LocalFirst,
            AuthMode::Anonymous,
        ] {
            let issuer = match mode {
                AuthMode::External => "https://issuer.example",
                AuthMode::LocalFirst => AuthorSubject::LOCAL_FIRST_ISSUER,
                AuthMode::Anonymous => AuthorSubject::ANONYMOUS_ISSUER,
            };
            let session = Session::new(issuer, "user-1").with_auth_mode(mode);
            let json = serde_json::to_string(&session).unwrap();
            let back: Session = serde_json::from_str(&json).unwrap();
            assert_eq!(back.auth_mode, mode);
        }
    }

    #[test]
    fn session_auth_mode_accepts_camel_case_alias() {
        let json =
            r#"{"issuer":"urn:jazz:anonymous","user_id":"u","claims":{},"authMode":"anonymous"}"#;
        let session: Session = serde_json::from_str(json).unwrap();
        assert_eq!(session.auth_mode, AuthMode::Anonymous);
    }

    #[test]
    fn session_get_string_returns_auth_mode_as_kebab_string() {
        let s = Session::new("urn:jazz:test", "u").with_auth_mode(AuthMode::LocalFirst);
        assert_eq!(s.get_string(&["authMode".into()]), Some("local-first"));
        assert_eq!(s.get_string(&["auth_mode".into()]), Some("local-first"));
        assert!(s.has_path(&["authMode".into()]));
    }
}
