//! Shared durable account rows. Storage bytes and SQL layout are unchanged.
use jazz::account_registry::{
    AccountError, AccountId, AccountState, Assignment, LinkIntent, Principal,
};
use jazz::storage::{OptionalExtension, Storage, params};
use uuid::Uuid;
/// The tables, created by the registry's first open. Principals keep their
/// exact issuer and subject text; ids are 16 uuid bytes.
pub(crate) const CREATE: &str = "
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
";

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

/// The registry state in the store, inside one read or write.
pub(crate) struct Sql<'a>(pub(crate) &'a Storage);

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
                    .query_row(params![principal.issuer, principal.subject], |row| {
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
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_host_account_identities \
                 (issuer, subject, account, active, manage_identities, generation) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .and_then(|mut statement| {
                statement.execute(params![
                    principal.issuer,
                    principal.subject,
                    assignment.account.0.as_bytes(),
                    assignment.active,
                    assignment.manage_identities,
                    signed(assignment.generation)?,
                ])
            })
            .map(|_| ())
            .map_err(unavailable)
    }

    fn put_link(&mut self, nonce: Uuid, link: &LinkIntent) -> Result<(), RegistryError> {
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_host_account_links \
                 (nonce, account, approver_issuer, approver_subject, approver_generation, \
                 candidate_issuer, candidate_subject, expires_at, completed) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .and_then(|mut statement| {
                statement.execute(params![
                    nonce.as_bytes(),
                    link.account.0.as_bytes(),
                    link.approver.issuer,
                    link.approver.subject,
                    signed(link.approver_generation)?,
                    link.candidate.issuer,
                    link.candidate.subject,
                    signed(link.expires_at)?,
                    link.completed,
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

fn signed(value: u64) -> jazz::storage::Result<i64> {
    i64::try_from(value)
        .map_err(|error| jazz::storage::Error::ToSqlConversionFailure(Box::new(error)))
}

pub(crate) fn storage_error(error: impl std::fmt::Display) -> jazz::db::Error {
    jazz::db::Error::new(jazz::db::ErrorCode::Storage, error.to_string())
}

pub(crate) fn unavailable(error: impl std::fmt::Display) -> RegistryError {
    RegistryError::Unavailable(error.to_string())
}
