//! Account operations shared by native HTTP and managed hosts.
//! Hosts authenticate access before calling these methods and serialize writes.
pub(crate) mod sql;
use jazz::account_registry::{
    self, AccountCommand, AccountCommandResult, AccountId, Assignment, Principal,
};
use jazz::host::HostTables;
use serde::{Deserialize, Serialize};
pub use sql::RegistryError;
use uuid::Uuid;

#[allow(async_fn_in_trait)]
pub trait Registry {
    async fn login(&self, principal: Principal) -> Result<Assignment, RegistryError>;
    async fn login_or_register(&self, principal: Principal) -> Result<Assignment, RegistryError>;
    async fn execute(&self, command: AccountCommand)
    -> Result<AccountCommandResult, RegistryError>;
}

/// A managed host borrows its owning connection for each short operation.
pub struct LocalRegistry<'a>(pub &'a dyn HostTables);
impl LocalRegistry<'_> {
    pub fn initialize(&self) -> Result<(), RegistryError> {
        self.0
            .host_sql(&mut |conn| conn.execute_batch(sql::CREATE).map_err(sql::storage_error))
            .map_err(sql::unavailable)
    }
    fn with<T>(
        &self,
        mut f: impl FnMut(&mut sql::Sql<'_>) -> Result<T, RegistryError>,
    ) -> Result<T, RegistryError> {
        let mut result = None;
        self.0
            .host_sql(&mut |conn| match f(&mut sql::Sql(conn)) {
                Err(RegistryError::Unavailable(error)) => Err(sql::storage_error(error)),
                decided => {
                    result = Some(decided);
                    Ok(())
                }
            })
            .map_err(sql::unavailable)?;
        result.expect("host_sql runs its callback")
    }
    pub fn command(&self, command: &AccountCommand) -> Result<AccountCommandResult, RegistryError> {
        self.with(|sql| account_registry::apply(sql, command))
    }
    pub fn lookup(&self, principal: &Principal) -> Result<Assignment, RegistryError> {
        self.with(|sql| account_registry::login(sql, principal))
    }
}
impl Registry for LocalRegistry<'_> {
    async fn login(&self, principal: Principal) -> Result<Assignment, RegistryError> {
        self.lookup(&principal)
    }
    async fn login_or_register(&self, principal: Principal) -> Result<Assignment, RegistryError> {
        principal.validate(false)?;
        self.with(|sql| match account_registry::login(sql, &principal) {
            Ok(assignment) => Ok(assignment),
            Err(RegistryError::Decision(account_registry::AccountError::NotAssigned)) => {
                match account_registry::apply(
                    sql,
                    &AccountCommand::Register {
                        principal: principal.clone(),
                        account: AccountId(Uuid::new_v4()),
                    },
                )? {
                    AccountCommandResult::Assignment(assignment) => Ok(assignment),
                    _ => unreachable!("register returns assignment"),
                }
            }
            Err(error) => Err(error),
        })
    }
    async fn execute(
        &self,
        command: AccountCommand,
    ) -> Result<AccountCommandResult, RegistryError> {
        self.with(|sql| account_registry::apply(sql, &command))
    }
}

pub type Failure = (u16, &'static str);
pub fn failure(error: RegistryError) -> Failure {
    use account_registry::AccountError;
    match error {
        RegistryError::Decision(AccountError::NotAssigned) => (404, "identity_not_assigned"),
        RegistryError::Decision(AccountError::AlreadyAssigned) => {
            (409, "identity_already_assigned")
        }
        RegistryError::Decision(AccountError::NotAuthorized) => (403, "identity_not_authorized"),
        RegistryError::Decision(_) => (400, "invalid_link_intent"),
        RegistryError::Unavailable(_) => (503, "account_registry_unavailable"),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityRequest {
    pub identity: Principal,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountResponse {
    pub account: Uuid,
    pub identity: Principal,
    pub generation: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminFoundLocalFirst {
    pub identity: Principal,
    #[serde(default)]
    pub audience: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminLink {
    pub from: Principal,
    pub to: Principal,
    #[serde(default)]
    pub nonce: Option<Uuid>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRevoke {
    pub approver: Principal,
    pub target: Principal,
}
fn response(identity: Principal, assignment: Assignment) -> AccountResponse {
    AccountResponse {
        account: assignment.account.0,
        identity,
        generation: assignment.generation,
    }
}
/// Read an existing assignment without creating one.
pub async fn resolve(
    registry: &impl Registry,
    request: IdentityRequest,
) -> Result<AccountResponse, Failure> {
    let assignment = registry
        .login(request.identity.clone())
        .await
        .map_err(failure)?;
    Ok(response(request.identity, assignment))
}
/// A verified backend vouches for the external identity; revocation is permanent.
pub async fn register(
    registry: &impl Registry,
    request: IdentityRequest,
) -> Result<AccountResponse, Failure> {
    jazz::ids::AuthorSubject::authenticated(&request.identity.issuer, &request.identity.subject)
        .map_err(|_| (400, "invalid_external_identity"))?;
    let assignment = registry
        .login_or_register(request.identity.clone())
        .await
        .map_err(failure)?;
    Ok(response(request.identity, assignment))
}
/// Found a backend-verified local-first identity in its proof audience namespace.
pub async fn found(
    registry: &impl Registry,
    request: AdminFoundLocalFirst,
    app: Uuid,
    allow: bool,
) -> Result<AccountResponse, Failure> {
    if request.identity.issuer != jazz::tools::identity::LOCAL_FIRST_ISSUER {
        return Err((400, "local_first_identity_required"));
    }
    if !allow {
        return Err((403, "local_first_auth_disabled"));
    }
    let app = match request.audience.as_deref().map(str::trim) {
        Some("") => return Err((400, "invalid_local_first_audience")),
        Some(audience) => *jazz::app_id::AppId::from_string(audience)
            .unwrap_or_else(|_| jazz::app_id::AppId::from_name(audience))
            .uuid(),
        None => app,
    };
    let result = registry
        .execute(AccountCommand::FoundLocalFirst {
            principal: request.identity.clone(),
            app,
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("founding result")
    };
    Ok(response(request.identity, assignment))
}
/// Admin-only: request and accept a single-use link for two verified identities.
/// The source must already exist; a link never creates its account.
pub async fn link(
    registry: &impl Registry,
    request: AdminLink,
    now: u64,
) -> Result<AccountResponse, Failure> {
    jazz::ids::AuthorSubject::authenticated(&request.to.issuer, &request.to.subject)
        .map_err(|_| (400, "invalid_external_identity"))?;
    registry
        .login(request.from.clone())
        .await
        .map_err(failure)?;
    let expires_at = now.checked_add(300).ok_or((503, "invalid_clock"))?;
    let nonce = request.nonce.unwrap_or_else(Uuid::new_v4);
    registry
        .execute(AccountCommand::RequestLink {
            approver: request.from,
            candidate: request.to.clone(),
            nonce,
            now,
            expires_at,
        })
        .await
        .map_err(failure)?;
    let result = registry
        .execute(AccountCommand::AcceptLink {
            candidate: request.to.clone(),
            nonce,
            now,
        })
        .await
        .map_err(failure)?;
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("accept result")
    };
    Ok(response(request.to, assignment))
}
/// Admin-only: retain the assignment but permanently disable the target identity.
pub async fn revoke(registry: &impl Registry, request: AdminRevoke) -> Result<(), Failure> {
    registry
        .execute(AccountCommand::Revoke {
            approver: request.approver,
            target: request.target,
        })
        .await
        .map_err(failure)?;
    Ok(())
}
