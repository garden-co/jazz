//! User auto-provisioning for JWT-authenticated users.
//!
//! This module handles mapping external user IDs (from JWT tokens) to
//! Jazz ObjectIds, optionally creating new user records on first login.

use std::collections::HashMap;
use std::sync::RwLock;

use groove::ObjectId;
use groove::sync::{ClaimValue, ClientIdentity, TokenValidator};

/// Trait for resolving external user IDs to Jazz ObjectIds.
pub trait UserResolver: Send + Sync {
    /// Try to find an existing Jazz user ID for the given external ID.
    fn resolve(&self, external_id: &str) -> Option<ObjectId>;

    /// Provision a new user and return their Jazz ObjectId.
    /// Called when a user logs in for the first time if auto-provisioning is enabled.
    fn provision(&self, external_id: &str, claims: &HashMap<String, ClaimValue>) -> ObjectId;
}

/// A simple in-memory user resolver for development/testing.
///
/// Maps external IDs to Jazz ObjectIds, creating deterministic IDs
/// based on the external ID if not found.
#[derive(Debug, Default)]
pub struct InMemoryUserResolver {
    /// Map from external ID to Jazz ObjectId.
    users: RwLock<HashMap<String, ObjectId>>,
}

impl InMemoryUserResolver {
    /// Create a new in-memory user resolver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-register a user mapping.
    pub fn register(&self, external_id: impl Into<String>, user_id: ObjectId) {
        self.users
            .write()
            .unwrap()
            .insert(external_id.into(), user_id);
    }
}

impl UserResolver for InMemoryUserResolver {
    fn resolve(&self, external_id: &str) -> Option<ObjectId> {
        self.users.read().unwrap().get(external_id).copied()
    }

    fn provision(&self, external_id: &str, _claims: &HashMap<String, ClaimValue>) -> ObjectId {
        let mut users = self.users.write().unwrap();

        // Check again in case another thread provisioned
        if let Some(&id) = users.get(external_id) {
            return id;
        }

        // Create a deterministic ID from the external ID
        let user_id = ObjectId::from_key(&format!("user:{}", external_id));
        users.insert(external_id.to_string(), user_id);
        user_id
    }
}

/// Token validator that wraps another validator and adds user resolution.
///
/// This validator:
/// 1. Validates the JWT token using the inner validator
/// 2. Resolves the external user ID to a Jazz ObjectId
/// 3. Optionally provisions new users on first login
pub struct ProvisioningTokenValidator<V, R> {
    inner: V,
    resolver: R,
    auto_provision: bool,
}

impl<V, R> ProvisioningTokenValidator<V, R>
where
    V: TokenValidator,
    R: UserResolver,
{
    /// Create a new provisioning validator.
    pub fn new(inner: V, resolver: R, auto_provision: bool) -> Self {
        Self {
            inner,
            resolver,
            auto_provision,
        }
    }

    /// Create a validator with auto-provisioning enabled.
    pub fn with_auto_provision(inner: V, resolver: R) -> Self {
        Self::new(inner, resolver, true)
    }

    /// Create a validator without auto-provisioning.
    pub fn without_auto_provision(inner: V, resolver: R) -> Self {
        Self::new(inner, resolver, false)
    }
}

impl<V, R> TokenValidator for ProvisioningTokenValidator<V, R>
where
    V: TokenValidator,
    R: UserResolver,
{
    fn validate(&self, token: &str) -> Option<ClientIdentity> {
        // First validate the JWT
        let mut identity = self.inner.validate(token)?;

        // Try to resolve existing user
        if let Some(user_id) = self.resolver.resolve(&identity.external_id) {
            identity.user_id = Some(user_id);
        } else if self.auto_provision {
            // Provision new user
            let user_id = self
                .resolver
                .provision(&identity.external_id, &identity.claims);
            identity.user_id = Some(user_id);
        }
        // If no resolution and no auto-provision, user_id stays None
        // Policy evaluation will use effective_user_id() which creates a deterministic ID

        Some(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::sync::AcceptAllTokens;

    #[test]
    fn test_in_memory_resolver() {
        let resolver = InMemoryUserResolver::new();

        // Initially no user
        assert!(resolver.resolve("user123").is_none());

        // Provision creates a user
        let user_id = resolver.provision("user123", &HashMap::new());
        assert!(user_id.to_string().len() > 0);

        // Now resolves to the same ID
        assert_eq!(resolver.resolve("user123"), Some(user_id));

        // Provisioning again returns the same ID
        let user_id2 = resolver.provision("user123", &HashMap::new());
        assert_eq!(user_id, user_id2);
    }

    #[test]
    fn test_pre_registered_user() {
        let resolver = InMemoryUserResolver::new();
        let preset_id = ObjectId::new(12345);

        resolver.register("alice", preset_id);

        assert_eq!(resolver.resolve("alice"), Some(preset_id));
    }

    #[test]
    fn test_provisioning_validator_with_auto_provision() {
        let inner = AcceptAllTokens;
        let resolver = InMemoryUserResolver::new();
        let validator = ProvisioningTokenValidator::with_auto_provision(inner, resolver);

        let identity = validator.validate("some-token").unwrap();

        // Should have a user_id assigned
        assert!(identity.user_id.is_some());
    }

    #[test]
    fn test_provisioning_validator_without_auto_provision() {
        let inner = AcceptAllTokens;
        let resolver = InMemoryUserResolver::new();
        let validator = ProvisioningTokenValidator::without_auto_provision(inner, resolver);

        let identity = validator.validate("some-token").unwrap();

        // Should not have a user_id (no auto-provision)
        assert!(identity.user_id.is_none());
    }

    #[test]
    fn test_provisioning_validator_with_existing_user() {
        let inner = AcceptAllTokens;
        let resolver = InMemoryUserResolver::new();
        let preset_id = ObjectId::new(99999);
        resolver.register("existing-user", preset_id);

        let validator = ProvisioningTokenValidator::without_auto_provision(inner, resolver);

        // The token value is the external_id for AcceptAllTokens
        let identity = validator.validate("existing-user").unwrap();

        // Should have the preset user_id
        assert_eq!(identity.user_id, Some(preset_id));
    }
}
