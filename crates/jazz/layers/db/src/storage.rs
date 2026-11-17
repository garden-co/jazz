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
            Owner::Native(conn) => {
                crate::values::begin_savepoint(conn, name)?;
                let (value, keep) = match f(self) {
                    Scoped::Keep(value) => (value, true),
                    Scoped::Undo(value) => (value, false),
                };
                crate::values::end_savepoint(conn, name, keep)?;
                Ok(value)
            }
        }
    }
}

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

/// Transitional: call sites still written against rusqlite reach the
/// native connection. Removed once they all use `Storage`'s own surface.
impl std::ops::Deref for Storage {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &rusqlite::Connection {
        match &self.owner {
            Owner::Native(conn) => conn,
        }
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
