//! One application's account registry and its admin HTTP routes
//! (`/apps/{id}/admin/accounts/*`), over a [`Storage`] any host gives it.
//!
//! The registry is kept as its current state in host tables of the app's
//! store. Nothing is cached: a login is one primary-key read of committed
//! state, and a command reads what it needs, decides and writes in one
//! [`Storage::scope`] that is durable when the scope's owner commits. A
//! second owner of the same store cannot decide from stale state.
//!
//! The routes are split the same way: what a host does (extract the
//! request, check the secret [`AccountRoute::authority`] names, carry the
//! reply) stays with it; validation, the registry calls and the mapping of
//! their outcome to a status and body are here. A host that holds the
//! store calls [`serve`] (or the typed route functions); one that reaches
//! the registry another way (native's owner thread) composes the same
//! pieces in the same order.

use jazz::account_registry::{
    self, AccountCommand, AccountCommandResult, AccountError, AccountId, AccountState, Assignment,
    LinkIntent, Principal,
};
use jazz::storage::{OptionalExtension, SqlError, Storage, ToSql};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The tables, created by the registry's first open. Principals keep their
/// exact issuer and subject text; ids are 16 uuid bytes.
const CREATE: &str = "
CREATE TABLE IF NOT EXISTS _jazz_host_account_identities (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    account BLOB NOT NULL,
    active INTEGER NOT NULL,
    manage_identities INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    PRIMARY KEY (issuer, subject)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_host_account_identities_by_account
    ON _jazz_host_account_identities (account);
CREATE TABLE IF NOT EXISTS _jazz_host_account_links (
    nonce BLOB PRIMARY KEY,
    account BLOB NOT NULL,
    approver_issuer TEXT NOT NULL,
    approver_subject TEXT NOT NULL,
    approver_generation INTEGER NOT NULL,
    candidate_issuer TEXT NOT NULL,
    candidate_subject TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    completed INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_host_account_links_open_by_expiry
    ON _jazz_host_account_links (expires_at) WHERE completed = 0;
CREATE TABLE IF NOT EXISTS _jazz_host_account_session_revocations (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    revoked_before_ms INTEGER NOT NULL,
    PRIMARY KEY (issuer, subject)
) WITHOUT ROWID;
";

/// Outcome of an atomic login-or-register decision; creation is reported only
/// after its durable boundary so owners can notify admission observers once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginOrRegisterResult {
    pub assignment: Assignment,
    pub created: bool,
}

/// Registry denial or an unavailable/corrupt durable authority.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// A valid request did not meet registry preconditions.
    #[error(transparent)]
    Decision(#[from] AccountError),
    /// The store could not be read or written; the outcome of a write that
    /// fails this way is unknown until the next read.
    #[error("account registry unavailable: {0}")]
    Unavailable(String),
}

/// How long a session revocation mark is kept. A mark only refuses an
/// admission whose `authenticatedAt` is older than it, and an admission
/// states the time its own `authenticate` started, seconds before: live
/// sessions are ended when the mark is recorded, not from the table. A day
/// covers that and any clock skew between backend instances many times over.
pub const REVOCATION_MARK_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

/// Create the registry's tables in `storage` if they are missing. Reads
/// nothing; run it once before the other operations.
pub fn open(storage: &Storage) -> Result<(), RegistryError> {
    storage
        .scope("jazz_account_registry_open", |storage| {
            storage.execute_batch(CREATE).map_err(storage_error)
        })
        .map_err(unavailable)
}

/// Resolve current admission from committed state.
pub fn login(storage: &Storage, principal: &Principal) -> Result<Assignment, RegistryError> {
    account_registry::login(&mut Sql(storage), principal)
}

/// Atomically resolve an external identity or durably create its first
/// assignment, as `new_account` (called only when one is created). Only an
/// unassigned identity may register; revocation is never undone.
pub fn login_or_register(
    storage: &Storage,
    principal: &Principal,
    new_account: impl FnOnce() -> AccountId,
) -> Result<LoginOrRegisterResult, RegistryError> {
    login_or_register_by(
        principal,
        new_account,
        |principal| login(storage, principal),
        |command| execute(storage, command),
    )
}

/// [`login_or_register`] over a host's own `login` and `execute` (native
/// reads committed state on its read connection and writes on the store's
/// thread).
pub(crate) fn login_or_register_by(
    principal: &Principal,
    new_account: impl FnOnce() -> AccountId,
    login: impl FnOnce(&Principal) -> Result<Assignment, RegistryError>,
    execute: impl FnOnce(&AccountCommand) -> Result<AccountCommandResult, RegistryError>,
) -> Result<LoginOrRegisterResult, RegistryError> {
    principal.validate(false)?;
    match login(principal) {
        Ok(assignment) => Ok(LoginOrRegisterResult {
            assignment,
            created: false,
        }),
        Err(RegistryError::Decision(AccountError::NotAssigned)) => {
            let result = execute(&AccountCommand::Register {
                principal: principal.clone(),
                account: new_account(),
            })?;
            Ok(LoginOrRegisterResult {
                assignment: assignment_of(result),
                created: true,
            })
        }
        Err(error) => Err(error),
    }
}

/// Decide and write in one scope: the result is kept when it returns `Ok`,
/// and a refused or failed command writes nothing.
pub fn execute(
    storage: &Storage,
    command: &AccountCommand,
) -> Result<AccountCommandResult, RegistryError> {
    storage
        .scope(
            "jazz_account_registry_command",
            |storage| match account_registry::apply(&mut Sql(storage), command) {
                Err(RegistryError::Unavailable(error)) => Err(storage_error(error)),
                decided => Ok(decided),
            },
        )
        .map_err(unavailable)?
}

/// Record that `principal`'s sessions authenticated before `before_ms`
/// (Unix milliseconds, the app backend's clock) are revoked. A later mark
/// never moves back: the stored mark is the latest one given. Returns the
/// mark now in force. Kept in its own table, which a core without session
/// revocation never reads; recording a mark forgets those older than
/// [`REVOCATION_MARK_RETENTION_MS`] before `now_ms` (this host's clock).
pub fn revoke_sessions(
    storage: &Storage,
    principal: &Principal,
    before_ms: u64,
    now_ms: u64,
) -> Result<u64, RegistryError> {
    let forget_before = now_ms.saturating_sub(REVOCATION_MARK_RETENTION_MS);
    storage
        .scope("jazz_account_session_revocation", |storage| {
            storage
                .prepare_cached(
                    "DELETE FROM _jazz_host_account_session_revocations \
                     WHERE revoked_before_ms < ?1",
                )
                .and_then(|mut statement| statement.execute([signed(forget_before)?]))
                .map_err(storage_error)?;
            // No `RETURNING`: a Durable Object's SQL can't read the rows of
            // a data-modifying statement. The scope reads its own write.
            let before = signed(before_ms).map_err(storage_error)?;
            storage
                .prepare_cached(
                    "INSERT INTO _jazz_host_account_session_revocations \
                     (issuer, subject, revoked_before_ms) VALUES (?1, ?2, ?3) \
                     ON CONFLICT (issuer, subject) DO UPDATE SET revoked_before_ms = \
                     max(revoked_before_ms, excluded.revoked_before_ms)",
                )
                .and_then(|mut statement| {
                    statement.execute([
                        &principal.issuer as &dyn ToSql,
                        &principal.subject,
                        &before,
                    ])
                })
                .map_err(storage_error)?;
            stored_mark(storage, principal)
                .map_err(storage_error)?
                .ok_or_else(|| storage_error("the recorded revocation mark is missing"))
        })
        .map_err(unavailable)
        .and_then(unsigned)
}

/// The latest session revocation mark recorded for `principal`.
pub fn sessions_revoked_before(
    storage: &Storage,
    principal: &Principal,
) -> Result<Option<u64>, RegistryError> {
    stored_mark(storage, principal)
        .map_err(unavailable)?
        .map(unsigned)
        .transpose()
}

/// Whether a session authenticated at `authenticated_at_ms` is refused by
/// its user's revocation `mark` ([`sessions_revoked_before`]): only one
/// authenticated strictly before the mark is.
pub fn session_revoked(mark: Option<u64>, authenticated_at_ms: u64) -> bool {
    mark.is_some_and(|before| before > authenticated_at_ms)
}

fn stored_mark(storage: &Storage, principal: &Principal) -> Result<Option<i64>, SqlError> {
    storage
        .prepare_cached(
            "SELECT revoked_before_ms FROM _jazz_host_account_session_revocations \
             WHERE issuer = ?1 AND subject = ?2",
        )
        .and_then(|mut statement| {
            statement
                .query_row([&principal.issuer, &principal.subject], |row| {
                    row.get::<_, i64>(0)
                })
                .optional()
        })
}

/// The registry state in the store, inside one read or write.
struct Sql<'a>(&'a Storage);

impl AccountState for Sql<'_> {
    type Error = RegistryError;

    fn assignment(&mut self, principal: &Principal) -> Result<Option<Assignment>, RegistryError> {
        self.0
            .prepare_cached(
                "SELECT account, active, manage_identities, generation \
                 FROM _jazz_host_account_identities WHERE issuer = ?1 AND subject = ?2",
            )
            .and_then(|mut statement| {
                statement
                    .query_row([&principal.issuer, &principal.subject], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, bool>(1)?,
                            row.get::<_, bool>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })
                    .optional()
            })
            .map_err(unavailable)?
            .map(|(account, active, manage_identities, generation)| {
                Ok(Assignment {
                    account: AccountId(uuid(&account)?),
                    active,
                    manage_identities,
                    generation: unsigned(generation)?,
                })
            })
            .transpose()
    }

    fn account_assigned(&mut self, account: AccountId) -> Result<bool, RegistryError> {
        self.0
            .prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM _jazz_host_account_identities WHERE account = ?1)",
            )
            .and_then(|mut statement| statement.query_row([account.0.as_bytes()], |row| row.get(0)))
            .map_err(unavailable)
    }

    fn link(&mut self, nonce: Uuid) -> Result<Option<LinkIntent>, RegistryError> {
        self.0
            .prepare_cached(
                "SELECT account, approver_issuer, approver_subject, approver_generation, \
                 candidate_issuer, candidate_subject, expires_at, completed \
                 FROM _jazz_host_account_links WHERE nonce = ?1",
            )
            .and_then(|mut statement| {
                statement
                    .query_row([nonce.as_bytes()], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            Principal {
                                issuer: row.get(1)?,
                                subject: row.get(2)?,
                            },
                            row.get::<_, i64>(3)?,
                            Principal {
                                issuer: row.get(4)?,
                                subject: row.get(5)?,
                            },
                            row.get::<_, i64>(6)?,
                            row.get::<_, bool>(7)?,
                        ))
                    })
                    .optional()
            })
            .map_err(unavailable)?
            .map(
                |(account, approver, approver_generation, candidate, expires_at, completed)| {
                    Ok(LinkIntent {
                        account: AccountId(uuid(&account)?),
                        approver,
                        approver_generation: unsigned(approver_generation)?,
                        candidate,
                        expires_at: unsigned(expires_at)?,
                        completed,
                    })
                },
            )
            .transpose()
    }

    fn put_assignment(
        &mut self,
        principal: &Principal,
        assignment: &Assignment,
    ) -> Result<(), RegistryError> {
        let generation = signed(assignment.generation).map_err(unavailable)?;
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_host_account_identities \
                 (issuer, subject, account, active, manage_identities, generation) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .and_then(|mut statement| {
                statement.execute([
                    &principal.issuer as &dyn ToSql,
                    &principal.subject,
                    &assignment.account.0.as_bytes().as_slice(),
                    &assignment.active,
                    &assignment.manage_identities,
                    &generation,
                ])
            })
            .map(|_| ())
            .map_err(unavailable)
    }

    fn put_link(&mut self, nonce: Uuid, link: &LinkIntent) -> Result<(), RegistryError> {
        let approver_generation = signed(link.approver_generation).map_err(unavailable)?;
        let expires_at = signed(link.expires_at).map_err(unavailable)?;
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_host_account_links \
                 (nonce, account, approver_issuer, approver_subject, approver_generation, \
                 candidate_issuer, candidate_subject, expires_at, completed) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .and_then(|mut statement| {
                statement.execute([
                    &nonce.as_bytes().as_slice() as &dyn ToSql,
                    &link.account.0.as_bytes().as_slice(),
                    &link.approver.issuer,
                    &link.approver.subject,
                    &approver_generation,
                    &link.candidate.issuer,
                    &link.candidate.subject,
                    &expires_at,
                    &link.completed,
                ])
            })
            .map(|_| ())
            .map_err(unavailable)
    }

    fn prune_links(&mut self, now: u64) -> Result<(), RegistryError> {
        self.0
            .prepare_cached(
                "DELETE FROM _jazz_host_account_links WHERE completed = 0 AND expires_at <= ?1",
            )
            .and_then(|mut statement| statement.execute([signed(now)?]))
            .map(|_| ())
            .map_err(unavailable)
    }
}

fn uuid(bytes: &[u8]) -> Result<Uuid, RegistryError> {
    Uuid::from_slice(bytes).map_err(unavailable)
}

/// Counters and Unix seconds are stored as SQLite's signed integers.
fn unsigned(value: i64) -> Result<u64, RegistryError> {
    u64::try_from(value).map_err(unavailable)
}

fn signed(value: u64) -> Result<i64, SqlError> {
    i64::try_from(value).map_err(|error| SqlError::ToSqlConversionFailure(Box::new(error)))
}

/// A store failure, as the error that undoes a scope.
pub(crate) fn storage_error(error: impl std::fmt::Display) -> jazz::db::Error {
    jazz::db::Error::new(jazz::db::ErrorCode::Storage, error.to_string())
}

pub(crate) fn unavailable(error: impl std::fmt::Display) -> RegistryError {
    RegistryError::Unavailable(error.to_string())
}

// ---------------------------------------------------------------------------
// Routes: `/apps/{id}/admin/accounts/{route}`, all `POST` with a JSON body.
// ---------------------------------------------------------------------------

/// The account routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountRoute {
    /// `resolve`: [`IdentityRequest`] → [`AccountResponse`].
    Resolve,
    /// `login-or-register`: [`IdentityRequest`] → [`AccountResponse`].
    LoginOrRegister,
    /// `link`: [`LinkRequest`] → [`AccountResponse`] (the account `to` now uses).
    Link,
    /// `found-local-first`: [`FoundLocalFirstRequest`] → [`AccountResponse`].
    FoundLocalFirst,
    /// `revoke`: [`RevokeRequest`] → 204.
    Revoke,
    /// `revoke-sessions`: [`RevokeSessionsRequest`] → [`RevokeSessionsResponse`].
    RevokeSessions,
}

/// Who may call a route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteAuthority {
    /// The app's backend, vouching for the identities it names: its admin
    /// secret (`x-jazz-admin-secret`) or, when that header is absent, the
    /// backend secret a backend client holds (`x-jazz-backend-secret`). A
    /// backend client may already act as any user on its sockets, so naming
    /// the user's account here grants it nothing more.
    VouchingBackend,
    /// The admin secret only: linking and revoking.
    Admin,
}

impl AccountRoute {
    /// The route named by the path segment after `admin/accounts/`.
    pub fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "resolve" => Self::Resolve,
            "login-or-register" => Self::LoginOrRegister,
            "link" => Self::Link,
            "found-local-first" => Self::FoundLocalFirst,
            "revoke" => Self::Revoke,
            "revoke-sessions" => Self::RevokeSessions,
            _ => return None,
        })
    }

    /// Its path segment after `admin/accounts/`.
    pub fn path(self) -> &'static str {
        match self {
            Self::Resolve => "resolve",
            Self::LoginOrRegister => "login-or-register",
            Self::Link => "link",
            Self::FoundLocalFirst => "found-local-first",
            Self::Revoke => "revoke",
            Self::RevokeSessions => "revoke-sessions",
        }
    }

    /// The secret a host checks before calling the route.
    pub fn authority(self) -> RouteAuthority {
        match self {
            Self::Resolve | Self::LoginOrRegister | Self::FoundLocalFirst => {
                RouteAuthority::VouchingBackend
            }
            Self::Link | Self::Revoke | Self::RevokeSessions => RouteAuthority::Admin,
        }
    }
}

/// A refused account request: its status and its `text/plain` body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteFailure {
    pub status: u16,
    pub body: &'static str,
}

const fn refuse(status: u16, body: &'static str) -> RouteFailure {
    RouteFailure { status, body }
}

/// How a registry outcome answers an account request.
pub fn registry_failure(error: RegistryError) -> RouteFailure {
    match error {
        RegistryError::Decision(AccountError::NotAssigned) => refuse(404, "identity_not_assigned"),
        RegistryError::Decision(AccountError::AlreadyAssigned) => {
            refuse(409, "identity_already_assigned")
        }
        RegistryError::Decision(AccountError::NotAuthorized) => {
            refuse(403, "identity_not_authorized")
        }
        RegistryError::Decision(_) => refuse(400, "invalid_link_intent"),
        RegistryError::Unavailable(_) => refuse(503, "account_registry_unavailable"),
    }
}

/// `resolve` and `login-or-register`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityRequest {
    pub identity: Principal,
}

/// The account an identity uses.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountResponse {
    pub account: Uuid,
    pub identity: Principal,
    pub generation: u64,
}

impl AccountResponse {
    pub fn new(identity: Principal, assignment: Assignment) -> Self {
        Self {
            account: assignment.account.0,
            identity,
            generation: assignment.generation,
        }
    }
}

/// `found-local-first`: the app's backend vouches for a local-first guest:
/// it checked the guest's proof of its key itself (`createJazz` links a
/// guest only on a fresh, single-use challenge the guest signed). The core
/// founds the guest's account deterministically, as `found-local-first`
/// would for the guest's own token.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FoundLocalFirstRequest {
    pub identity: Principal,
    /// The namespace the guest's account was founded in, when it is not
    /// this app: the endpoint an app-less client's proofs are for.
    #[serde(default)]
    pub audience: Option<String>,
}

/// `link`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkRequest {
    pub from: Principal,
    pub to: Principal,
    /// The link's single-use intent id, when the backend has one (the nonce
    /// of the challenge a guest signed): a second link with the same nonce
    /// is refused, whoever it names.
    #[serde(default)]
    pub nonce: Option<Uuid>,
}

/// `revoke`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeRequest {
    pub approver: Principal,
    pub target: Principal,
}

/// `revoke-sessions`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeSessionsRequest {
    pub identity: Principal,
    /// Unix milliseconds on the backend's clock: sessions its `authenticate`
    /// admitted before this end.
    #[serde(rename = "revokedBefore")]
    pub revoked_before: u64,
}

/// What `revoke-sessions` answers.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeSessionsResponse {
    pub identity: Principal,
    /// The mark now in force: never earlier than one recorded before.
    #[serde(rename = "revokedBefore")]
    pub revoked_before: u64,
}

/// How long a backend-vouched link stays open between its request and its
/// accept (both made by one `link` call).
pub const LINK_TTL_SECONDS: u64 = 300;

/// `login-or-register` and `link` (`to`) name an external identity: the
/// backend's own auth verified it, so it must be one a session can be.
pub fn require_external_identity(identity: &Principal) -> Result<(), RouteFailure> {
    jazz::ids::AuthorSubject::authenticated(&identity.issuer, &identity.subject)
        .map(|_| ())
        .map_err(|_| refuse(400, "invalid_external_identity"))
}

/// `found-local-first` founds only local-first guests.
pub fn require_local_first_identity(identity: &Principal) -> Result<(), RouteFailure> {
    if identity.issuer != jazz::tools::identity::DEVICE_ACCOUNT_ISSUER {
        return Err(refuse(400, "local_first_identity_required"));
    }
    Ok(())
}

/// The namespace a local-first founder's account id derives from: the
/// audience its proof was verified against, an app id or any other name.
pub fn local_first_founding_app(audience: &str) -> jazz::tools::AppId {
    jazz::tools::AppId::from_string(audience)
        .unwrap_or_else(|_| jazz::tools::AppId::from_name(audience))
}

/// The founding namespace `found-local-first` names: its `audience`, or
/// this app (`app_id`) without one.
pub fn founding_app(audience: Option<&str>, app_id: Uuid) -> Result<Uuid, RouteFailure> {
    match audience.map(str::trim) {
        Some("") => Err(refuse(400, "invalid_local_first_audience")),
        Some(audience) => Ok(*local_first_founding_app(audience).uuid()),
        None => Ok(app_id),
    }
}

/// The request-and-accept steps of `link`, the same the two users' own
/// credentials would take, once `from`'s account was found: a host runs
/// `request` and then `accept`, and answers with the account `accept`
/// assigned.
pub struct LinkSteps {
    pub request: AccountCommand,
    pub accept: AccountCommand,
}

/// [`LinkSteps`] for `request` at `now_seconds`, with `fresh_nonce` when the
/// request names none.
pub fn link_steps(
    request: &LinkRequest,
    now_seconds: u64,
    fresh_nonce: impl FnOnce() -> Uuid,
) -> Result<LinkSteps, RouteFailure> {
    let expires_at = now_seconds
        .checked_add(LINK_TTL_SECONDS)
        .ok_or(refuse(503, "invalid_clock"))?;
    let nonce = request.nonce.unwrap_or_else(fresh_nonce);
    Ok(LinkSteps {
        request: AccountCommand::RequestLink {
            approver: request.from.clone(),
            candidate: request.to.clone(),
            nonce,
            now: now_seconds,
            expires_at,
        },
        accept: AccountCommand::AcceptLink {
            candidate: request.to.clone(),
            nonce,
            now: now_seconds,
        },
    })
}

/// The assignment a register, found, link or accept command answers with.
pub fn assignment_of(result: AccountCommandResult) -> Assignment {
    let AccountCommandResult::Assignment(assignment) = result else {
        unreachable!("an assigning command returns an assignment")
    };
    assignment
}

/// `resolve`: read-only administrative lookup; service authority can resolve
/// an assignment but cannot create or link one.
pub fn resolve(
    storage: &Storage,
    request: IdentityRequest,
) -> Result<AccountResponse, RouteFailure> {
    let assignment = login(storage, &request.identity).map_err(registry_failure)?;
    Ok(AccountResponse::new(request.identity, assignment))
}

/// `login-or-register`: the app's backend vouches for this identity
/// (`createJazz` in front of a remote core): its own auth verified the user,
/// so the core resolves the assignment or creates the account (as
/// `new_account`), as `login-or-register` would for the user's own
/// credential. Also answers whether the account was created, for a host
/// that tells its live sessions.
pub fn login_or_register_route(
    storage: &Storage,
    request: IdentityRequest,
    new_account: impl FnOnce() -> AccountId,
) -> Result<(AccountResponse, bool), RouteFailure> {
    require_external_identity(&request.identity)?;
    let result =
        login_or_register(storage, &request.identity, new_account).map_err(registry_failure)?;
    Ok((
        AccountResponse::new(request.identity, result.assignment),
        result.created,
    ))
}

/// `found-local-first`, in the app `app_id` unless the request names
/// another audience.
pub fn found_local_first(
    storage: &Storage,
    request: FoundLocalFirstRequest,
    app_id: Uuid,
) -> Result<AccountResponse, RouteFailure> {
    require_local_first_identity(&request.identity)?;
    let app = founding_app(request.audience.as_deref(), app_id)?;
    let result = execute(
        storage,
        &AccountCommand::FoundDeviceAccount {
            principal: request.identity.clone(),
            app,
        },
    )
    .map_err(registry_failure)?;
    Ok(AccountResponse::new(
        request.identity,
        assignment_of(result),
    ))
}

/// `link`: the app's backend vouches for both identities: link `to` to the
/// account `from` uses. `from` must already have one (a user the backend
/// signed in, or a local-first guest founded from its own key proof):
/// linking never creates it.
pub fn link(
    storage: &Storage,
    request: LinkRequest,
    now_seconds: u64,
    fresh_nonce: impl FnOnce() -> Uuid,
) -> Result<AccountResponse, RouteFailure> {
    require_external_identity(&request.to)?;
    login(storage, &request.from).map_err(registry_failure)?;
    let steps = link_steps(&request, now_seconds, fresh_nonce)?;
    execute(storage, &steps.request).map_err(registry_failure)?;
    let accepted = execute(storage, &steps.accept).map_err(registry_failure)?;
    Ok(AccountResponse::new(request.to, assignment_of(accepted)))
}

/// `revoke`: the app's backend vouches for `approver`, an identity of the
/// account, and revokes `target`, another of its identities, as the
/// approver's own credential would. The assignment stays: the target can no
/// longer sign in, and is never reassigned.
pub fn revoke(storage: &Storage, request: RevokeRequest) -> Result<(), RouteFailure> {
    execute(
        storage,
        &AccountCommand::Revoke {
            approver: request.approver,
            target: request.target,
        },
    )
    .map(|_| ())
    .map_err(registry_failure)
}

/// `revoke-sessions` (`jazz.revokeUser`): end the identity's sessions
/// authenticated before `revokedBefore`. The host closes the user's live
/// sessions with the mark this answers, and refuses those stated later with
/// an older `authenticatedAt` ([`session_revoked`]). The assignment is
/// untouched: the user signs in again as before.
pub fn revoke_sessions_route(
    storage: &Storage,
    request: RevokeSessionsRequest,
    now_ms: u64,
) -> Result<RevokeSessionsResponse, RouteFailure> {
    let revoked_before =
        revoke_sessions(storage, &request.identity, request.revoked_before, now_ms)
            .map_err(registry_failure)?;
    Ok(RevokeSessionsResponse {
        identity: request.identity,
        revoked_before,
    })
}

/// The rule that an encrypting app serves only local-first accounts founded
/// in its own namespace (E2EE protocol R5, R6): the registry keeps no record
/// of which namespace founded an account, so an account founded for another
/// audience could not be told from the app's own, and no account record is
/// accepted for it. Founding one in `founding` (the namespace the founding
/// names; `own` is the app's) is refused at the first point a host sees it,
/// and the registry view refuses its records (`founded_for_another_audience`)
/// for any that exist already. `encrypts` says whether the app has Jazz's
/// system tables, asked only for a foreign namespace; a host that cannot tell
/// answers true.
pub fn refuse_foreign_founding(
    own: Uuid,
    founding: Uuid,
    encrypts: impl FnOnce() -> bool,
) -> Result<(), &'static str> {
    if founding == own || !encrypts() {
        return Ok(());
    }
    Err("an app that encrypts columns serves local-first accounts founded for its own app id only")
}

/// What a host needs besides the store to [`serve`] an account route.
pub struct RouteContext<'a> {
    /// The app the core serves: where a local-first guest is founded when
    /// the request names no other audience.
    pub app_id: Uuid,
    /// This host's clock, Unix milliseconds.
    pub now_ms: u64,
    /// A fresh random id: a new account, or a link nonce the request doesn't
    /// name.
    pub fresh_id: &'a mut dyn FnMut() -> Uuid,
    /// Whether the app has Jazz's system tables ([`refuse_foreign_founding`]):
    /// true when the host cannot tell.
    pub encrypts: &'a dyn Fn() -> bool,
}

/// An account route's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteReply {
    pub status: u16,
    pub body: RouteBody,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteBody {
    /// No body (204).
    Empty,
    /// `application/json`.
    Json(String),
    /// `text/plain; charset=utf-8`: a refusal.
    Text(String),
}

/// What the account route also changed, for a host that tells its live
/// sessions (native's owner announces both).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteEffects {
    /// An assignment may have changed: live sessions re-check theirs.
    pub assignments_changed: bool,
    /// Sessions of this identity authenticated before the mark are revoked.
    pub sessions_revoked: Option<(Principal, u64)>,
}

/// Serve `route` with its JSON `body`, after the host checked the secret
/// [`AccountRoute::authority`] names: the status and body to answer, as the
/// native server answers them, and what changed. A body that is not JSON is
/// refused with 400, one that is not the route's request with 422 (as
/// native's request extraction does; the message text is this module's).
pub fn serve(
    storage: &Storage,
    route: AccountRoute,
    body: &[u8],
    context: RouteContext<'_>,
) -> (RouteReply, RouteEffects) {
    let mut effects = RouteEffects::default();
    let reply = (|| -> Result<RouteReply, RouteReply> {
        Ok(match route {
            AccountRoute::Resolve => json(resolve(storage, parse(body)?)),
            AccountRoute::LoginOrRegister => {
                let result = login_or_register_route(storage, parse(body)?, || {
                    AccountId((context.fresh_id)())
                });
                effects.assignments_changed = match &result {
                    Ok((_, created)) => *created,
                    Err(failure) => failure.status == 503,
                };
                json(result.map(|(response, _)| response))
            }
            AccountRoute::Link => {
                let result = link(storage, parse(body)?, context.now_ms / 1000, || {
                    (context.fresh_id)()
                });
                effects.assignments_changed = changed(&result);
                json(result)
            }
            AccountRoute::FoundLocalFirst => {
                let request: FoundLocalFirstRequest = parse(body)?;
                let result = founding_app(request.audience.as_deref(), context.app_id)
                    .and_then(|app| {
                        refuse_foreign_founding(context.app_id, app, context.encrypts)
                            .map_err(|body| refuse(400, body))
                    })
                    .and_then(|()| found_local_first(storage, request, context.app_id));
                effects.assignments_changed = changed(&result);
                json(result)
            }
            AccountRoute::Revoke => {
                let result = revoke(storage, parse(body)?);
                effects.assignments_changed = changed(&result);
                match result {
                    Ok(()) => RouteReply {
                        status: 204,
                        body: RouteBody::Empty,
                    },
                    Err(failure) => failed(failure),
                }
            }
            AccountRoute::RevokeSessions => {
                let request: RevokeSessionsRequest = parse(body)?;
                let (identity, before) = (request.identity.clone(), request.revoked_before);
                let result = revoke_sessions_route(storage, request, context.now_ms);
                // A failed write may still have committed: end the sessions
                // on the mark asked for.
                effects.sessions_revoked = Some(match &result {
                    Ok(response) => (identity, response.revoked_before),
                    Err(_) => (identity, before),
                });
                json(result)
            }
        })
    })()
    .unwrap_or_else(|refused| refused);
    (reply, effects)
}

/// Whether a command route's outcome may have changed an assignment: a
/// success, or a store failure (which may come after the write committed).
/// A refusal changes nothing.
fn changed<T>(result: &Result<T, RouteFailure>) -> bool {
    match result {
        Ok(_) => true,
        Err(failure) => failure.status == 503,
    }
}

fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, RouteReply> {
    serde_json::from_slice(body).map_err(|error| {
        let (status, what) = match error.classify() {
            serde_json::error::Category::Data => (
                422,
                "Failed to deserialize the JSON body into the target type",
            ),
            _ => (400, "Failed to parse the request body as JSON"),
        };
        RouteReply {
            status,
            body: RouteBody::Text(format!("{what}: {error}")),
        }
    })
}

fn json<T: Serialize>(result: Result<T, RouteFailure>) -> RouteReply {
    match result {
        Ok(value) => match serde_json::to_string(&value) {
            Ok(body) => RouteReply {
                status: 200,
                body: RouteBody::Json(body),
            },
            Err(_) => failed(refuse(500, "response_not_serializable")),
        },
        Err(failure) => failed(failure),
    }
}

fn failed(failure: RouteFailure) -> RouteReply {
    RouteReply {
        status: failure.status,
        body: RouteBody::Text(failure.body.to_owned()),
    }
}

/// A read of the registry's tables on a connection that sees what the
/// registry committed (a native host's read-only connection, a Durable
/// Object's own SQL). A table no write created yet is missing: `f` must not
/// assume it exists.
pub trait RegistryRead {
    fn read<T>(
        &self,
        f: impl FnOnce(&Storage) -> Result<T, jazz::db::Error>,
    ) -> Result<T, jazz::db::Error>;
}

/// A host whose SQL is one connection (a Durable Object's) reads the
/// registry on it, from inside any scope the core has open.
impl RegistryRead for Storage {
    fn read<T>(
        &self,
        f: impl FnOnce(&Storage) -> Result<T, jazz::db::Error>,
    ) -> Result<T, jazz::db::Error> {
        f(self)
    }
}

impl RegistryRead for jazz::host::HostReader {
    fn read<T>(
        &self,
        f: impl FnOnce(&Storage) -> Result<T, jazz::db::Error>,
    ) -> Result<T, jazz::db::Error> {
        self.sql(f)
    }
}

/// The registry as core's checks beside the account tables' policies read
/// it (E2EE protocol R5, R6): on a connection of its own, which sees what
/// the registry committed, so a check never waits for the store's thread.
pub struct RegistryFounders<R> {
    reader: R,
    app: Uuid,
}

impl<R: RegistryRead> RegistryFounders<R> {
    /// `app` is the namespace device accounts' ids derive from
    /// ([`local_first_founding_app`]).
    pub fn new(reader: R, app: Uuid) -> Self {
        Self { reader, app }
    }
}

impl<R: RegistryRead> jazz::db::AccountRegistryView for RegistryFounders<R> {
    /// An account was founded by a local-first identity when one of its
    /// local-first identities derives its id: linking a local-first
    /// identity into another account keeps that account's id.
    fn founded_local_first(&self, account: AccountId) -> Result<Option<bool>, String> {
        let issuers = self.reader.read(|conn| {
            let mut statement = conn
                .prepare_cached(
                    "SELECT issuer, subject FROM _jazz_host_account_identities WHERE account = ?1",
                )
                .map_err(storage_error)?;
            let rows = statement
                .query_map([account.0.as_bytes()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(storage_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(storage_error)
        });
        let identities = match issuers {
            Ok(identities) => identities,
            // A store whose registry never opened knows no account.
            Err(error) if error.to_string().contains("no such table") => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        if identities.is_empty() {
            return Ok(None);
        }
        Ok(Some(identities.iter().any(|(issuer, subject)| {
            issuer == jazz::identity::DEVICE_ACCOUNT_ISSUER
                && jazz::account_registry::device_account_id(self.app, subject) == account
        })))
    }

    /// An account whose identities are all local-first and none of which
    /// derives its id in this app's namespace was founded for another
    /// audience (`found-local-first` and a guest's first admission may name
    /// one). An encrypting app does not serve such accounts: this rule is
    /// the documented one (E2EE protocol R5, R6), and the founding routes
    /// refuse to create them. An account with an external identity is not
    /// told apart from one the app founded, so it reads as the app's.
    fn founded_for_another_audience(&self, account: AccountId) -> Result<bool, String> {
        let Some(founded) = self.founded_local_first(account)? else {
            return Ok(false);
        };
        if founded {
            return Ok(false);
        }
        let external = self.reader.read(|conn| {
            let mut statement = conn
                .prepare_cached(
                    "SELECT COUNT(*) FROM _jazz_host_account_identities \
                     WHERE account = ?1 AND issuer <> ?2",
                )
                .map_err(storage_error)?;
            statement
                .query_row(
                    [
                        &account.0.as_bytes().as_slice() as &dyn ToSql,
                        &jazz::identity::DEVICE_ACCOUNT_ISSUER,
                    ],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(storage_error)
        });
        external
            .map(|external| external == 0)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    //! The registry over the store any host gives it: the native SQLite
    //! owner, and the emulated Durable Object SQL (callback scopes only, no
    //! `RETURNING`), so a Durable Object host serves what the native server
    //! serves.

    use super::*;

    fn principal(subject: &str) -> Principal {
        Principal {
            issuer: "jazz:app:test".into(),
            subject: subject.into(),
        }
    }

    /// Each test runs on both store owners, its registry freshly opened.
    fn stores() -> [(&'static str, Storage); 2] {
        let stores = [
            ("native", Storage::open_in_memory().unwrap()),
            (
                "durable object emulation",
                Storage::managed_emulation_in_memory().unwrap(),
            ),
        ];
        for (_, storage) in &stores {
            open(storage).unwrap();
            // Opening again finds the tables and keeps them.
            open(storage).unwrap();
        }
        stores
    }

    fn ids() -> impl FnMut() -> Uuid {
        let mut next = 0u128;
        move || {
            next += 1;
            Uuid::from_u128(next)
        }
    }

    fn post(
        storage: &Storage,
        route: &str,
        body: serde_json::Value,
        fresh_id: &mut dyn FnMut() -> Uuid,
        now_ms: u64,
    ) -> (RouteReply, RouteEffects) {
        serve(
            storage,
            AccountRoute::from_path(route).unwrap(),
            body.to_string().as_bytes(),
            RouteContext {
                app_id: Uuid::from_u128(0xa11),
                now_ms,
                fresh_id,
                encrypts: &|| false,
            },
        )
    }

    fn post_raw(
        storage: &Storage,
        body: &[u8],
        fresh_id: &mut dyn FnMut() -> Uuid,
    ) -> (RouteReply, RouteEffects) {
        let context = RouteContext {
            app_id: Uuid::nil(),
            now_ms: 0,
            fresh_id,
            encrypts: &|| false,
        };
        serve(storage, AccountRoute::Resolve, body, context)
    }

    fn account_of(reply: &RouteReply) -> Uuid {
        let RouteBody::Json(body) = &reply.body else {
            panic!("expected a JSON body: {reply:?}")
        };
        serde_json::from_str::<AccountResponse>(body)
            .unwrap()
            .account
    }

    fn text(status: u16, body: &str) -> RouteReply {
        RouteReply {
            status,
            body: RouteBody::Text(body.into()),
        }
    }

    /// alice's first `login-or-register` creates her account and answers it
    /// as native does; a repeat (a backend that lost the response) and a
    /// `resolve` find the same account and create nothing. bob, never
    /// registered, resolves to 404.
    ///
    /// ```
    /// backend ──login-or-register(alice)──► registry: created  (account A)
    /// backend ──login-or-register(alice)──► registry: found    (account A)
    /// backend ──resolve(alice)────────────► A      resolve(bob) ──► 404
    /// ```
    #[test]
    fn register_then_login_finds_the_same_account() {
        for (store, storage) in stores() {
            let mut fresh_id = ids();
            let alice = serde_json::json!({ "identity": principal("alice") });
            let (created, effects) = post(
                &storage,
                "login-or-register",
                alice.clone(),
                &mut fresh_id,
                0,
            );
            assert_eq!(
                created.body,
                RouteBody::Json(format!(
                    r#"{{"account":"{}","identity":{{"issuer":"jazz:app:test","subject":"alice"}},"generation":0}}"#,
                    Uuid::from_u128(1)
                )),
                "{store}"
            );
            assert_eq!(created.status, 200, "{store}");
            assert!(
                effects.assignments_changed,
                "{store}: a creation is announced"
            );

            let (again, effects) = post(
                &storage,
                "login-or-register",
                alice.clone(),
                &mut fresh_id,
                0,
            );
            assert_eq!(again, created, "{store}");
            assert!(
                !effects.assignments_changed,
                "{store}: a found login changes nothing"
            );
            let (resolved, _) = post(&storage, "resolve", alice, &mut fresh_id, 0);
            assert_eq!(resolved, created, "{store}");
            assert_eq!(
                login(&storage, &principal("alice")).unwrap().account,
                AccountId(Uuid::from_u128(1)),
                "{store}"
            );

            let (bob, _) = post(
                &storage,
                "resolve",
                serde_json::json!({ "identity": principal("bob") }),
                &mut fresh_id,
                0,
            );
            assert_eq!(bob, text(404, "identity_not_assigned"), "{store}");
        }
    }

    /// The backend links bob to alice's account with a nonce: bob resolves
    /// to it. The same nonce again, for mallory, is refused and links
    /// nothing; linking from carol, who has no account, never creates one;
    /// linking alice to bob, who already has an account, is a conflict.
    ///
    /// ```
    /// alice(A) ──link(nonce n)──► bob ⇒ A
    /// alice(A) ──link(nonce n)──► mallory ✗ 400   carol ──link──► dave ✗ 404
    /// ```
    #[test]
    fn a_link_shares_one_account_and_its_nonce_is_single_use() {
        for (store, storage) in stores() {
            let mut fresh_id = ids();
            let (alice, _) = post(
                &storage,
                "login-or-register",
                serde_json::json!({ "identity": principal("alice") }),
                &mut fresh_id,
                0,
            );
            let nonce = Uuid::from_u128(0x7e57);
            let link_to = |to: &str| serde_json::json!({ "from": principal("alice"), "to": principal(to), "nonce": nonce });
            let (linked, effects) = post(&storage, "link", link_to("bob"), &mut fresh_id, 1_000);
            assert_eq!(linked.status, 200, "{store}: {linked:?}");
            assert_eq!(account_of(&linked), account_of(&alice), "{store}");
            assert!(effects.assignments_changed, "{store}");

            let (replayed, effects) =
                post(&storage, "link", link_to("mallory"), &mut fresh_id, 2_000);
            assert_eq!(replayed, text(400, "invalid_link_intent"), "{store}");
            assert!(
                !effects.assignments_changed,
                "{store}: a refusal changes nothing"
            );
            assert!(
                matches!(
                    login(&storage, &principal("mallory")),
                    Err(RegistryError::Decision(AccountError::NotAssigned))
                ),
                "{store}"
            );

            let (nobody, _) = post(
                &storage,
                "link",
                serde_json::json!({ "from": principal("carol"), "to": principal("dave") }),
                &mut fresh_id,
                2_000,
            );
            assert_eq!(nobody, text(404, "identity_not_assigned"), "{store}");
            let (merge, _) = post(
                &storage,
                "link",
                serde_json::json!({ "from": principal("alice"), "to": principal("bob") }),
                &mut fresh_id,
                2_000,
            );
            assert_eq!(merge, text(409, "identity_already_assigned"), "{store}");
        }
    }

    /// alice revokes bob, another identity of her account: bob no longer
    /// signs in (403), login-or-register can't revive him, and alice still
    /// signs in. A local-first guest is founded deterministically, in this
    /// app or the audience named.
    #[test]
    fn revoking_an_identity_is_permanent_and_founding_is_deterministic() {
        for (store, storage) in stores() {
            let mut fresh_id = ids();
            post(
                &storage,
                "login-or-register",
                serde_json::json!({ "identity": principal("alice") }),
                &mut fresh_id,
                0,
            );
            post(
                &storage,
                "link",
                serde_json::json!({ "from": principal("alice"), "to": principal("bob") }),
                &mut fresh_id,
                0,
            );
            let (revoked, effects) = post(
                &storage,
                "revoke",
                serde_json::json!({ "approver": principal("alice"), "target": principal("bob") }),
                &mut fresh_id,
                0,
            );
            assert_eq!(
                revoked,
                RouteReply {
                    status: 204,
                    body: RouteBody::Empty
                },
                "{store}"
            );
            assert!(effects.assignments_changed, "{store}");
            for route in ["resolve", "login-or-register"] {
                let (bob, _) = post(
                    &storage,
                    route,
                    serde_json::json!({ "identity": principal("bob") }),
                    &mut fresh_id,
                    0,
                );
                assert_eq!(
                    bob,
                    text(403, "identity_not_authorized"),
                    "{store}: {route}"
                );
            }
            assert!(login(&storage, &principal("alice")).is_ok(), "{store}");

            let guest = Principal {
                issuer: jazz::tools::identity::DEVICE_ACCOUNT_ISSUER.into(),
                subject: "6f9619ff-8b86-5011-b42d-00c04fc964ff".into(),
            };
            let found = |audience: Option<&str>, fresh_id: &mut dyn FnMut() -> Uuid| {
                let mut body = serde_json::json!({ "identity": guest });
                if let Some(audience) = audience {
                    body["audience"] = audience.into();
                }
                post(&storage, "found-local-first", body, fresh_id, 0).0
            };
            let here = found(None, &mut fresh_id);
            assert_eq!(here.status, 200, "{store}: {here:?}");
            assert_eq!(found(None, &mut fresh_id), here, "{store}");
            assert_eq!(
                found(Some(" "), &mut fresh_id),
                text(400, "invalid_local_first_audience"),
                "{store}"
            );
            let (external, _) = post(
                &storage,
                "found-local-first",
                serde_json::json!({ "identity": principal("carol") }),
                &mut fresh_id,
                0,
            );
            assert_eq!(
                external,
                text(400, "local_first_identity_required"),
                "{store}"
            );
        }
    }

    /// `jazz.revokeUser` for alice records a mark that refuses her sessions
    /// authenticated before it and admits later ones; bob's are untouched.
    /// The mark never moves back, and recording a mark forgets those past
    /// their retention on this host's clock.
    ///
    /// ```
    /// backend ──revoke-sessions(alice, T)──► mark T
    /// alice session authenticated T-1 ──✗ refused     at T ──✓ admitted
    /// ```
    #[test]
    fn a_revocation_mark_refuses_an_older_admission() {
        for (store, storage) in stores() {
            let mut fresh_id = ids();
            let now = 10 * REVOCATION_MARK_RETENTION_MS;
            let (alice, bob) = (principal("alice"), principal("bob"));
            let (reply, effects) = post(
                &storage,
                "revoke-sessions",
                serde_json::json!({ "identity": alice, "revokedBefore": now }),
                &mut fresh_id,
                now,
            );
            assert_eq!(
                reply.body,
                RouteBody::Json(format!(
                    r#"{{"identity":{{"issuer":"jazz:app:test","subject":"alice"}},"revokedBefore":{now}}}"#
                )),
                "{store}"
            );
            assert_eq!(
                effects.sessions_revoked,
                Some((alice.clone(), now)),
                "{store}"
            );

            let mark = sessions_revoked_before(&storage, &alice).unwrap();
            assert!(
                session_revoked(mark, now - 1),
                "{store}: older admission refused"
            );
            assert!(
                !session_revoked(mark, now),
                "{store}: a later one is admitted"
            );
            let bob_mark = sessions_revoked_before(&storage, &bob).unwrap();
            assert!(!session_revoked(bob_mark, 0), "{store}: bob has no mark");

            // An earlier mark keeps the one in force.
            assert_eq!(
                revoke_sessions(&storage, &alice, now - 5, now).unwrap(),
                now,
                "{store}"
            );
            // A mark past its retention is forgotten when the next is recorded.
            let long_ago = now - REVOCATION_MARK_RETENTION_MS - 1;
            revoke_sessions(&storage, &bob, long_ago, now).unwrap();
            revoke_sessions(&storage, &alice, now, now).unwrap();
            assert_eq!(
                sessions_revoked_before(&storage, &bob).unwrap(),
                None,
                "{store}"
            );
            assert_eq!(
                sessions_revoked_before(&storage, &alice).unwrap(),
                Some(now),
                "{store}"
            );
        }
    }

    /// A refused command writes nothing, also where the only transactions
    /// are scopes: a link request whose nonce is taken leaves the first
    /// intent as it was, and the candidate unassigned.
    #[test]
    fn a_refused_command_writes_nothing() {
        for (store, storage) in stores() {
            let alice = principal("alice");
            login_or_register(&storage, &alice, || AccountId(Uuid::from_u128(1))).unwrap();
            let request = |candidate: &str, now: u64| AccountCommand::RequestLink {
                approver: alice.clone(),
                candidate: principal(candidate),
                nonce: Uuid::from_u128(9),
                now,
                expires_at: now + 10,
            };
            execute(&storage, &request("bob", 0)).unwrap();
            assert!(
                matches!(
                    execute(&storage, &request("mallory", 1)),
                    Err(RegistryError::Decision(AccountError::IdentifierCollision))
                ),
                "{store}"
            );
            let accepted = execute(
                &storage,
                &AccountCommand::AcceptLink {
                    candidate: principal("bob"),
                    nonce: Uuid::from_u128(9),
                    now: 2,
                },
            )
            .unwrap();
            assert_eq!(
                assignment_of(accepted).account,
                AccountId(Uuid::from_u128(1)),
                "{store}"
            );
        }
    }

    /// A body that isn't JSON is refused 400, one that isn't the route's
    /// request 422, before the registry is read. Routes name the secret a
    /// host checks: vouching routes accept a backend client, link and
    /// revoke only the admin.
    #[test]
    fn bad_bodies_are_refused_and_routes_name_their_authority() {
        let storage = Storage::managed_emulation_in_memory().unwrap();
        open(&storage).unwrap();
        let mut fresh_id = ids();
        let (syntax, _) = post_raw(&storage, b"{", &mut fresh_id);
        assert_eq!(syntax.status, 400);
        let (shape, _) = post_raw(
            &storage,
            br#"{"identity":{"issuer":"i","subject":"s"},"extra":1}"#,
            &mut fresh_id,
        );
        assert_eq!(shape.status, 422);

        for route in [
            AccountRoute::Resolve,
            AccountRoute::LoginOrRegister,
            AccountRoute::Link,
            AccountRoute::FoundLocalFirst,
            AccountRoute::Revoke,
            AccountRoute::RevokeSessions,
        ] {
            assert_eq!(AccountRoute::from_path(route.path()), Some(route));
        }
        assert_eq!(AccountRoute::from_path("merge"), None);
        assert_eq!(
            AccountRoute::LoginOrRegister.authority(),
            RouteAuthority::VouchingBackend
        );
        assert_eq!(AccountRoute::Link.authority(), RouteAuthority::Admin);
        assert_eq!(
            AccountRoute::RevokeSessions.authority(),
            RouteAuthority::Admin
        );
    }

    /// `found-local-first` for another audience is refused by an encrypting
    /// app (E2EE protocol R5, R6), whether the audience is named or is
    /// another namespace's app id; the app's own namespace, and any
    /// namespace of an app that does not encrypt, are founded.
    #[test]
    fn found_local_first_in_another_namespace_is_refused_by_an_encrypting_app() {
        let storage = Storage::open_in_memory().unwrap();
        open(&storage).unwrap();
        let identity = |byte: u8| serde_json::json!({ "issuer": jazz::identity::DEVICE_ACCOUNT_ISSUER, "subject": Uuid::from_bytes([byte; 16]).to_string() });
        let found = |body: serde_json::Value, encrypts: bool| {
            let mut ids = Uuid::new_v4;
            let (reply, _) = serve(
                &storage,
                AccountRoute::FoundLocalFirst,
                body.to_string().as_bytes(),
                RouteContext {
                    app_id: Uuid::from_u128(0xa11),
                    now_ms: 0,
                    fresh_id: &mut ids,
                    encrypts: &|| encrypts,
                },
            );
            reply
        };
        let foreign =
            serde_json::json!({ "identity": identity(1), "audience": "https://other.example" });
        let refused = found(foreign.clone(), true);
        assert_eq!(refused.status, 400, "{refused:?}");
        assert!(
            matches!(&refused.body, RouteBody::Text(text) if text.contains("its own app id only")),
            "{refused:?}"
        );
        login(&storage, &serde_json::from_value(identity(1)).unwrap())
            .expect_err("nothing was founded");

        assert_eq!(
            found(foreign, false).status,
            200,
            "an app without encryption"
        );
        let own = serde_json::json!({ "identity": identity(2) });
        assert_eq!(found(own, true).status, 200, "the app's own namespace");
    }
}
