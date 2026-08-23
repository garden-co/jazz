// I-4: hand-written Debug that redacts secret fields.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuthConfig {
    pub jwt_token: Option<String>,
    pub backend_secret: Option<String>,
    pub admin_secret: Option<String>,
    #[serde(default, with = "auth_backend_session_serde")]
    pub backend_session: Option<serde_json::Value>,
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
