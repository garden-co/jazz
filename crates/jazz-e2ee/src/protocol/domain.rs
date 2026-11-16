//! Application domains (`E2EE_PROTOCOL.md` §0, "Application domains").
//!
//! The `application` field of every context is the shared domain
//! `json([appId, env])` or an account domain `json([appId, env, account])`.
//! The application ID comes from the application's configuration; no server
//! URL or registry path takes part, so data survives a move to another host.

use super::error::{ProtocolError, ProtocolResult};
use super::ids::{self, IdForm};
use super::json;

/// The environment label when the configuration sets none.
pub const DEFAULT_ENV: &str = "dev";

/// One application in one environment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppDomain {
    app_id: String,
    env: String,
}

impl AppDomain {
    /// `app_id` is the application's canonical lowercase UUID; `env` is the
    /// configured environment label, `"dev"` when unset. An empty label is
    /// refused rather than read as unset.
    pub fn new(app_id: &str, env: Option<&str>) -> ProtocolResult<Self> {
        let env = env.unwrap_or(DEFAULT_ENV);
        if !ids::is_id(app_id, IdForm::Any) || env.is_empty() {
            return Err(ProtocolError::InvalidRecord("invalid application domain"));
        }
        Ok(Self {
            app_id: app_id.to_owned(),
            env: env.to_owned(),
        })
    }

    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    pub fn env(&self) -> &str {
        &self.env
    }

    /// `Dₛ = json([appId, env])`: content, group public keys and rotation
    /// requests.
    pub fn shared(&self) -> SharedDomain {
        SharedDomain(json::strings(&[&self.app_id, &self.env]))
    }

    /// `Dₐ(account) = json([appId, env, account])`: the account's ledger, the
    /// groups and spaces it creates, and this device's private store scope.
    pub fn account(&self, account_id: &str) -> ProtocolResult<AccountDomain> {
        if account_id.is_empty() {
            return Err(ProtocolError::InvalidRecord("empty account ID"));
        }
        Ok(AccountDomain(json::strings(&[
            &self.app_id,
            &self.env,
            account_id,
        ])))
    }
}

/// The shared domain `Dₛ`, the `application` of content, group public-key
/// records and rotation requests. Distinct from [`AccountDomain`] so that
/// one cannot be passed where the other is required.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedDomain(String);

/// An account domain `Dₐ(account)`, the `application` of the account's
/// ledger records and of the groups and spaces it creates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountDomain(String);

impl SharedDomain {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AccountDomain {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
