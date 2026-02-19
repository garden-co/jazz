//! Types for schema/lens catalogue management.

use uuid::Uuid;

use crate::object::ObjectId;

/// Identifier for an application's schema family.
///
/// All schemas and lenses for an app share the same AppId. Used to:
/// - Filter catalogue queries by app
/// - Associate related schemas across clients
///
/// # Example
///
/// ```ignore
/// let app_id = AppId::from_name("my-todo-app");
/// let manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main")?;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppId(pub ObjectId);

impl AppId {
    /// Create a random AppId.
    pub fn random() -> Self {
        Self(ObjectId::new())
    }

    /// Create an AppId from a string identifier.
    ///
    /// Uses UUIDv5 with DNS namespace for deterministic generation.
    /// Same name always produces the same AppId.
    pub fn from_name(name: &str) -> Self {
        Self(ObjectId::from_uuid(Uuid::new_v5(
            &Uuid::NAMESPACE_DNS,
            name.as_bytes(),
        )))
    }

    /// Parse an AppId from a UUID string.
    pub fn from_string(s: &str) -> Result<Self, uuid::Error> {
        let uuid = Uuid::parse_str(s)?;
        Ok(Self(ObjectId::from_uuid(uuid)))
    }

    /// Create an AppId from an existing ObjectId.
    pub fn from_object_id(id: ObjectId) -> Self {
        Self(id)
    }

    /// Get the underlying ObjectId.
    pub fn as_object_id(&self) -> ObjectId {
        self.0
    }

    /// Get the UUID representation.
    pub fn uuid(&self) -> &Uuid {
        self.0.uuid()
    }
}

impl std::fmt::Display for AppId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.uuid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_id_from_name_deterministic() {
        let id1 = AppId::from_name("my-app");
        let id2 = AppId::from_name("my-app");
        assert_eq!(id1, id2);
    }

    #[test]
    fn app_id_different_names_different_ids() {
        let id1 = AppId::from_name("app-a");
        let id2 = AppId::from_name("app-b");
        assert_ne!(id1, id2);
    }

    #[test]
    fn app_id_display() {
        let id = AppId::from_name("test-app");
        let s = format!("{}", id);
        // Should be a valid UUID string
        assert!(s.contains('-'));
        assert_eq!(s.len(), 36);
    }
}
