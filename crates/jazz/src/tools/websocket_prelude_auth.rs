// I-4: hand-written Debug that redacts secret fields.
#[derive(Clone, Default, serde::Deserialize)]
pub struct AuthConfig {
    pub jwt_token: Option<String>,
    pub backend_secret: Option<String>,
    pub admin_secret: Option<String>,
    #[serde(default, with = "auth_backend_session_serde")]
    pub backend_session: Option<serde_json::Value>,
    #[serde(default)]
    pub inspector_token: Option<String>,
}

// Preserve the established JSON prelude when no Inspector token is supplied.
// Binary serializers still need every positional field, including None.
impl serde::Serialize for AuthConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        struct BackendSession<'a>(&'a Option<serde_json::Value>);
        impl serde::Serialize for BackendSession<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                auth_backend_session_serde::serialize(self.0, serializer)
            }
        }
        let include_inspector = !serializer.is_human_readable() || self.inspector_token.is_some();
        let mut state =
            serializer.serialize_struct("AuthConfig", if include_inspector { 5 } else { 4 })?;
        state.serialize_field("jwt_token", &self.jwt_token)?;
        state.serialize_field("backend_secret", &self.backend_secret)?;
        state.serialize_field("admin_secret", &self.admin_secret)?;
        state.serialize_field("backend_session", &BackendSession(&self.backend_session))?;
        if include_inspector {
            state.serialize_field("inspector_token", &self.inspector_token)?;
        }
        state.end()
    }
}

mod auth_backend_session_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<serde_json::Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            return value.serialize(serializer);
        }

        let json = value
            .as_ref()
            .map(|session| serde_json::to_string(session).map_err(serde::ser::Error::custom))
            .transpose()?;

        json.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            return Option::<serde_json::Value>::deserialize(deserializer);
        }

        let json = Option::<String>::deserialize(deserializer)?;
        json.map(|session| serde_json::from_str(&session).map_err(serde::de::Error::custom))
            .transpose()
    }
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field(
                "inspector_token",
                &self.inspector_token.as_ref().map(|_| "<redacted>"),
            )
            .field("jwt_token", &self.jwt_token.as_ref().map(|_| "<redacted>"))
            .field(
                "backend_secret",
                &self.backend_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "admin_secret",
                &self.admin_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "backend_session",
                &self.backend_session.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::AuthConfig;

    #[test]
    fn inspector_auth_preserves_json_and_binary_optional_fields() {
        for token in [None, Some("scoped-test-token".to_owned())] {
            let auth = AuthConfig {
                inspector_token: token.clone(),
                backend_session: Some(
                    serde_json::json!({"issuer":"https://auth.example", "user_id":"test"}),
                ),
                ..Default::default()
            };
            let json = serde_json::to_value(&auth).unwrap();
            assert_eq!(
                json.get("inspector_token").and_then(|value| value.as_str()),
                token.as_deref()
            );
            assert_eq!(json.get("inspector_token").is_some(), token.is_some());
            let decoded: AuthConfig = serde_json::from_value(json).unwrap();
            assert_eq!(decoded.inspector_token, token);
            let bytes = postcard::to_allocvec(&auth).unwrap();
            let decoded: AuthConfig = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.inspector_token, token);
            assert_eq!(decoded.backend_session, auth.backend_session);
        }
    }
}
