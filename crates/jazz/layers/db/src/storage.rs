//! The store's SQL owner: one connection and its transactions together.
//!
//! A [`Storage`] runs the store's SQL, and its only transactions are
//! callback scopes ([`Storage::scope`]): nothing stays open after the call
//! that opened it returns. That is the one shape every storage Jazz runs on
//! supports, including Cloudflare Durable Objects, whose SQL
//! (`ctx.storage.sql`) offers no `BEGIN` or `SAVEPOINT`, only
//! `transactionSync(fn)` with a synchronous `fn`.
//!
//! Today it owns a native SQLite connection only. While call sites move
//! onto its own SQL surface, it dereferences to that connection; a managed
//! variant (Durable Objects) follows once nothing needs the connection
//! itself.

use crate::options::Error;

/// One SQL owner, whose only transactions are callback scopes.
pub struct Storage {
    owner: Owner,
}

enum Owner {
    Native(rusqlite::Connection),
}

impl Storage {
    /// Own a native SQLite connection.
    pub fn native(conn: rusqlite::Connection) -> Self {
        Self {
            owner: Owner::Native(conn),
        }
    }

    /// Run `f` atomically: what it wrote is kept if it returns `Ok`, and
    /// undone if it returns `Err`. Scopes nest; an inner scope's writes
    /// commit with the outermost one. `name` labels the scope (on native
    /// SQLite, its savepoint).
    ///
    /// `f` is synchronous, so a scope can never stay open across an await
    /// or past the call that opened it.
    pub fn scope<T>(
        &self,
        name: &str,
        f: impl FnOnce(&Storage) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.scope_then(name, |storage| match f(storage) {
            Ok(value) => Scoped::Keep(Ok(value)),
            Err(error) => Scoped::Undo(Err(error)),
        })?
    }

    /// Run `f` atomically, keeping or undoing what it wrote as it decides
    /// ([`Scoped`]), and return its value either way: for a dry run (undo
    /// always) or a unit whose failure carries more than an [`Error`].
    /// `Err` only when the scope itself cannot open or close.
    pub fn scope_then<T>(
        &self,
        name: &str,
        f: impl FnOnce(&Storage) -> Scoped<T>,
    ) -> Result<T, Error> {
        match &self.owner {
            Owner::Native(_) => {
                crate::values::begin_savepoint(self, name)?;
                let (value, keep) = match f(self) {
                    Scoped::Keep(value) => (value, true),
                    Scoped::Undo(value) => (value, false),
                };
                crate::values::end_savepoint(self, name, keep)?;
                Ok(value)
            }
        }
    }
}

/// The SQL surface jazz-db runs its statements through. Each method is the
/// rusqlite method of the same name, and returns rusqlite's types under
/// this module's names, so call sites written against [`Storage`] don't
/// name rusqlite. A managed variant (Durable Objects) answers the same
/// calls from its own cursor.
impl Storage {
    /// Open a private in-memory database.
    pub fn open_in_memory() -> SqlResult<Self> {
        rusqlite::Connection::open_in_memory().map(Self::native)
    }

    /// Open (creating) the database file at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        rusqlite::Connection::open(path).map(Self::native)
    }

    /// Open `path` with `flags`.
    pub fn open_with_flags(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
    ) -> SqlResult<Self> {
        rusqlite::Connection::open_with_flags(path, flags).map(Self::native)
    }

    /// Open `path` with `flags` on the SQLite VFS named `vfs`.
    pub fn open_with_flags_and_vfs(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
        vfs: &str,
    ) -> SqlResult<Self> {
        rusqlite::Connection::open_with_flags_and_vfs(path, flags, vfs).map(Self::native)
    }

    /// Identifies this store among the open ones, for state kept beside it
    /// (a savepoint's tag-undo log, its rollback count): stable while it
    /// lives, possibly reused after it closes.
    pub(crate) fn key(&self) -> usize {
        match &self.owner {
            // SAFETY: the handle is only compared, never dereferenced.
            Owner::Native(conn) => unsafe { conn.handle() as usize },
        }
    }

    /// The native connection, for what only native SQLite has (its
    /// planner, limits and tracing).
    pub(crate) fn native_conn(&self) -> &rusqlite::Connection {
        match &self.owner {
            Owner::Native(conn) => conn,
        }
    }

    /// Prepare `sql` through the statement cache.
    pub fn prepare_cached(&self, sql: &str) -> SqlResult<CachedStatement<'_>> {
        self.native_conn().prepare_cached(sql)
    }

    /// Prepare `sql`, uncached.
    pub fn prepare(&self, sql: &str) -> SqlResult<Statement<'_>> {
        self.native_conn().prepare(sql)
    }

    /// Run one statement; how many rows it changed.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> SqlResult<usize> {
        self.native_conn().execute(sql, params)
    }

    /// Run statements with no parameters and no results.
    pub fn execute_batch(&self, sql: &str) -> SqlResult<()> {
        self.native_conn().execute_batch(sql)
    }

    /// Run a query expected to return a row; `f` reads the first.
    pub fn query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        f: impl FnOnce(&Row<'_>) -> SqlResult<T>,
    ) -> SqlResult<T> {
        self.native_conn().query_row(sql, params, f)
    }

    /// Whether no transaction or savepoint is open.
    pub fn is_autocommit(&self) -> bool {
        self.native_conn().is_autocommit()
    }

    /// How long a statement waits for another connection's lock.
    pub fn busy_timeout(&self, timeout: std::time::Duration) -> SqlResult<()> {
        self.native_conn().busy_timeout(timeout)
    }

    /// The rowid of the last row inserted.
    pub fn last_insert_rowid(&self) -> i64 {
        self.native_conn().last_insert_rowid()
    }

    /// How many rows all statements so far changed.
    pub fn total_changes(&self) -> u64 {
        self.native_conn().total_changes()
    }

    /// The current value of a SQLite limit.
    pub fn limit(&self, limit: rusqlite::limits::Limit) -> SqlResult<i32> {
        self.native_conn().limit(limit)
    }

    /// How many prepared statements the cache keeps.
    pub fn set_prepared_statement_cache_capacity(&self, capacity: usize) {
        self.native_conn()
            .set_prepared_statement_cache_capacity(capacity)
    }

    /// Run `hook` as each outermost transaction commits; `true` turns the
    /// commit into a rollback.
    pub fn commit_hook(
        &self,
        hook: Option<impl FnMut() -> bool + Send + 'static>,
    ) -> SqlResult<()> {
        self.native_conn().commit_hook(hook)
    }

    /// Run `hook` as each outermost transaction rolls back, including
    /// SQLite's own rollbacks after an error.
    pub fn rollback_hook(&self, hook: Option<impl FnMut() + Send + 'static>) -> SqlResult<()> {
        self.native_conn().rollback_hook(hook)
    }
}

impl crate::values::CachedRow for Storage {
    fn cached_row<T, P, F>(&self, sql: &str, params: P, f: F) -> SqlResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqlResult<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f)
    }
}

pub use rusqlite::{
    CachedStatement, Error as SqlError, OptionalExtension, Params, Result as SqlResult, Row, Rows,
    Statement, ToSql, params, params_from_iter,
};

/// What a scope ([`Storage::scope_then`]) does with what it wrote.
pub enum Scoped<T> {
    /// Keep it (committed with the outermost scope).
    Keep(T),
    /// Undo it.
    Undo(T),
}

impl From<rusqlite::Connection> for Storage {
    fn from(conn: rusqlite::Connection) -> Self {
        Self::native(conn)
    }
}

// Lower-level tests: `Storage::scope` is the contract every storage variant
// (native here, Durable Objects later) must meet, and no store path uses it
// yet, so no public Db API reaches it.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::ErrorCode;

    fn storage() -> Storage {
        let storage = Storage::native(rusqlite::Connection::open_in_memory().unwrap());
        storage.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        storage
    }

    fn values(storage: &Storage) -> Vec<i64> {
        let mut statement = storage.prepare("SELECT v FROM t ORDER BY v").unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn insert(storage: &Storage, v: i64) -> Result<(), Error> {
        storage
            .execute("INSERT INTO t (v) VALUES (?1)", [v])
            .map(drop)
            .map_err(crate::values::storage)
    }

    fn fail() -> Error {
        Error::new(ErrorCode::Storage, "refused")
    }

    /// ```text
    /// scope ── insert 1 ── Ok   → kept
    /// scope ── insert 2 ── Err  → undone
    /// ```
    #[test]
    fn a_scope_keeps_its_writes_on_ok_and_undoes_them_on_err() {
        let storage = storage();
        storage.scope("a", |s| insert(s, 1)).unwrap();
        let failed = storage.scope("b", |s| {
            insert(s, 2)?;
            Err::<(), _>(fail())
        });
        assert!(failed.is_err());
        assert_eq!(values(&storage), [1]);
        assert!(storage.is_autocommit(), "nothing stays open");
    }

    /// An inner scope's failure undoes only its own writes; the outer
    /// scope's commit keeps the rest. An outer failure undoes everything,
    /// including inner scopes that succeeded.
    ///
    /// ```text
    /// outer ── 1 ── inner(2, Err) ── 3 ── Ok   → [1, 3]
    /// outer ── 4 ── inner(5, Ok)  ── Err       → nothing more
    /// ```
    #[test]
    fn scopes_nest() {
        let storage = storage();
        storage
            .scope("outer", |s| {
                insert(s, 1)?;
                let inner = s.scope("inner", |s| {
                    insert(s, 2)?;
                    Err::<(), _>(fail())
                });
                assert!(inner.is_err());
                assert!(!s.is_autocommit(), "the outer scope is still open");
                insert(s, 3)
            })
            .unwrap();
        let failed = storage.scope("outer", |s| {
            insert(s, 4)?;
            s.scope("inner", |s| insert(s, 5))?;
            Err::<(), _>(fail())
        });
        assert!(failed.is_err());
        assert_eq!(values(&storage), [1, 3]);
        assert!(storage.is_autocommit());
    }

    /// A dry run returns what it found and undoes what it wrote; a unit
    /// that decides to keep returns its value too.
    ///
    /// ```text
    /// scope_then ── insert 1 ── count → 1 ── Undo  → [] and 1 returned
    /// scope_then ── insert 2 ──────────────── Keep  → [2]
    /// ```
    #[test]
    fn scope_then_keeps_or_undoes_as_its_body_decides_and_returns_its_value() {
        let storage = storage();
        let seen = storage
            .scope_then("dry", |s| {
                insert(s, 1).unwrap();
                Scoped::Undo(values(s).len())
            })
            .unwrap();
        assert_eq!(seen, 1);
        assert_eq!(values(&storage), Vec::<i64>::new());
        let kept = storage
            .scope_then("unit", |s| Scoped::Keep(insert(s, 2)))
            .unwrap();
        assert!(kept.is_ok());
        assert_eq!(values(&storage), [2]);
        assert!(storage.is_autocommit());
    }
}
