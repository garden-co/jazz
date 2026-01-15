//! Server configuration for groove-server.
//!
//! Configuration can be loaded from TOML files or environment variables.
//!
//! # Example Configuration
//!
//! ```toml
//! host = "0.0.0.0"
//! port = 8080
//!
//! [auth]
//! provider = "betterauth"  # or "workos", "accept_all"
//!
//! [auth.jwt]
//! jwks_url = "https://auth.example.com/.well-known/jwks.json"
//! issuer = "https://auth.example.com"
//! audience = "jazz-app"
//! user_id_claim = "sub"
//! ```

use std::path::Path;

use serde::Deserialize;

/// Server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Host address to bind to (default: "0.0.0.0")
    #[serde(default = "default_host")]
    pub host: String,

    /// Port to listen on (default: 8080)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,

    /// Database path for persistence (default: "groove.db")
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_db_path() -> String {
    "groove.db".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            auth: AuthConfig::default(),
            db_path: default_db_path(),
        }
    }
}

/// Authentication provider type.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AuthProvider {
    /// Accept all tokens (for development/testing only)
    #[default]
    AcceptAll,
    /// BetterAuth provider
    BetterAuth,
    /// WorkOS provider
    WorkOS,
    /// Custom JWT provider
    Jwt,
}

/// Authentication configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthConfig {
    /// The authentication provider to use
    #[serde(default)]
    pub provider: AuthProvider,

    /// JWT validation settings
    #[serde(default)]
    pub jwt: JwtAuthConfig,

    /// User provisioning settings
    #[serde(default)]
    pub provisioning: ProvisioningConfig,
}

/// JWT authentication configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct JwtAuthConfig {
    /// JWKS URL for RS256/RS384/RS512 validation.
    /// Required for BetterAuth and WorkOS providers.
    pub jwks_url: Option<String>,

    /// Secret key for HS256 validation (development only).
    pub secret: Option<String>,

    /// Expected token issuer (`iss` claim).
    pub issuer: Option<String>,

    /// Expected token audience (`aud` claim).
    pub audience: Option<String>,

    /// Claim name containing the user ID (default: "sub").
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,

    /// Optional claim containing a pre-resolved Jazz ObjectId.
    pub jazz_user_id_claim: Option<String>,

    /// Whether to validate token expiration (default: true).
    #[serde(default = "default_true")]
    pub validate_exp: bool,
}

fn default_user_id_claim() -> String {
    "sub".to_string()
}

fn default_true() -> bool {
    true
}

impl JwtAuthConfig {
    /// Convert to JwtConfig for the JWT validator.
    pub fn to_jwt_config(&self) -> groove::sync::jwt::JwtConfig {
        groove::sync::jwt::JwtConfig {
            jwks_url: self.jwks_url.clone(),
            secret: self.secret.clone(),
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            user_id_claim: self.user_id_claim.clone(),
            jazz_user_id_claim: self.jazz_user_id_claim.clone(),
            extract_claims: vec![], // Extract all claims
            validate_exp: self.validate_exp,
        }
    }
}

/// User provisioning configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProvisioningConfig {
    /// Whether to automatically provision users on first login.
    #[serde(default)]
    pub auto_provision: bool,

    /// Table name for user storage (default: "users").
    #[serde(default = "default_users_table")]
    pub users_table: String,

    /// Column name for external user ID (default: "external_id").
    #[serde(default = "default_external_id_column")]
    pub external_id_column: String,
}

fn default_users_table() -> String {
    "users".to_string()
}

fn default_external_id_column() -> String {
    "external_id".to_string()
}

impl ServerConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::IoError {
            path: path.as_ref().display().to_string(),
            source: e,
        })?;

        Self::from_toml(&content)
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(content: &str) -> Result<Self, ConfigError> {
        toml::from_str(content).map_err(ConfigError::ParseError)
    }

    /// Load configuration from default locations.
    ///
    /// Searches for config files in order:
    /// 1. `./groove-server.toml`
    /// 2. `./config/groove-server.toml`
    /// 3. Returns default config if no file found
    pub fn load() -> Self {
        let paths = ["groove-server.toml", "config/groove-server.toml"];

        for path in paths {
            if Path::new(path).exists() {
                match Self::from_file(path) {
                    Ok(config) => return config,
                    Err(e) => {
                        eprintln!("Warning: Failed to load config from {}: {}", path, e);
                    }
                }
            }
        }

        Self::default()
    }

    /// Get the socket address for binding.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Configuration error.
#[derive(Debug)]
pub enum ConfigError {
    /// Failed to read config file.
    IoError {
        path: String,
        source: std::io::Error,
    },
    /// Failed to parse TOML.
    ParseError(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::IoError { path, source } => {
                write!(f, "failed to read config file '{}': {}", path, source)
            }
            ConfigError::ParseError(e) => write!(f, "failed to parse config: {}", e),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8080);
        assert_eq!(config.auth.provider, AuthProvider::AcceptAll);
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
            port = 3000
        "#;

        let config = ServerConfig::from_toml(toml).unwrap();
        assert_eq!(config.port, 3000);
        assert_eq!(config.host, "0.0.0.0"); // default
    }

    #[test]
    fn test_parse_betterauth_config() {
        let toml = r#"
            host = "127.0.0.1"
            port = 8080

            [auth]
            provider = "betterauth"

            [auth.jwt]
            jwks_url = "http://localhost:3001/api/auth/jwks"
            issuer = "http://localhost:3001"
            user_id_claim = "sub"

            [auth.provisioning]
            auto_provision = true
            users_table = "users"
        "#;

        let config = ServerConfig::from_toml(toml).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.auth.provider, AuthProvider::BetterAuth);
        assert_eq!(
            config.auth.jwt.jwks_url,
            Some("http://localhost:3001/api/auth/jwks".to_string())
        );
        assert_eq!(
            config.auth.jwt.issuer,
            Some("http://localhost:3001".to_string())
        );
        assert!(config.auth.provisioning.auto_provision);
    }

    #[test]
    fn test_parse_workos_config() {
        let toml = r#"
            port = 8080

            [auth]
            provider = "workos"

            [auth.jwt]
            jwks_url = "https://api.workos.com/sso/jwks/client_123"
            issuer = "https://api.workos.com/"
            user_id_claim = "sub"
        "#;

        let config = ServerConfig::from_toml(toml).unwrap();
        assert_eq!(config.auth.provider, AuthProvider::WorkOS);
        assert_eq!(
            config.auth.jwt.issuer,
            Some("https://api.workos.com/".to_string())
        );
    }

    #[test]
    fn test_parse_jwt_config_with_secret() {
        let toml = r#"
            [auth]
            provider = "jwt"

            [auth.jwt]
            secret = "my-super-secret-key-for-development"
            validate_exp = false
        "#;

        let config = ServerConfig::from_toml(toml).unwrap();
        assert_eq!(config.auth.provider, AuthProvider::Jwt);
        assert_eq!(
            config.auth.jwt.secret,
            Some("my-super-secret-key-for-development".to_string())
        );
        assert!(!config.auth.jwt.validate_exp);
    }
}
