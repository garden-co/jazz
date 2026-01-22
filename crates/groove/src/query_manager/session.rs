//! Session context for policy evaluation.
//!
//! A Session represents the authenticated user's context, containing:
//! - `user_id`: Required unique identifier for the user
//! - `claims`: Optional JSON object with additional claims (roles, teams, etc.)

use serde_json::Value as JsonValue;

/// Session context for policy evaluation.
///
/// Contains the authenticated user's identity and claims. Used by policy
/// expressions to check row access permissions.
#[derive(Debug, Clone)]
pub struct Session {
    /// Required user identifier.
    pub user_id: String,
    /// Additional claims as a JSON object (e.g., `{"teams": ["eng", "design"]}`).
    pub claims: JsonValue,
}

impl Session {
    /// Create a new session with just a user ID.
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            claims: JsonValue::Object(serde_json::Map::new()),
        }
    }

    /// Create a session with user ID and claims.
    pub fn with_claims(mut self, claims: JsonValue) -> Self {
        self.claims = claims;
        self
    }

    /// Get a value at the given path.
    ///
    /// Path segments:
    /// - `["user_id"]` -> returns the user_id as a string
    /// - `["claims", "key"]` -> returns claims.key
    /// - `["claims", "nested", "key"]` -> returns claims.nested.key
    pub fn get_path(&self, path: &[String]) -> Option<&JsonValue> {
        if path.is_empty() {
            return None;
        }

        if path[0] == "user_id" {
            // Special case: user_id is stored as a String, not JsonValue
            // Return None here; use get_user_id() instead
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
        if path[0] == "user_id" && path.len() == 1 {
            return true;
        }
        self.get_path(path).is_some()
    }

    /// Get a string value at the given path.
    ///
    /// For `["user_id"]`, returns the user_id.
    /// For other paths, returns the JSON string if present.
    pub fn get_string(&self, path: &[String]) -> Option<&str> {
        if path.is_empty() {
            return None;
        }
        if path[0] == "user_id" && path.len() == 1 {
            return Some(&self.user_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_session_user_id() {
        let session = Session::new("user123");
        assert_eq!(session.get_user_id(), "user123");
        assert_eq!(session.get_string(&["user_id".into()]), Some("user123"));
        assert!(session.has_path(&["user_id".into()]));
    }

    #[test]
    fn test_session_claims() {
        let session = Session::new("user123").with_claims(json!({
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
        let session = Session::new("user123");

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
}
