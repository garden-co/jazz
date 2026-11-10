//! Backend conformance probes, compiled only for tests/testing hosts.
use super::*;

impl Db {
    /// A rejected write by Mallory leaves no row, history or queued row
    /// invalidation at Alice's authority. This lower-level probe checks
    /// durable history and speculative bookkeeping, not just query visibility.
    pub fn check_rejected_row_for_test(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        let inner = self.inner();
        let stored = inner.schema.stored_table(table)?;
        for table in [stored.to_owned(), format!("{stored}__h")] {
            let count: i64 = inner
                .conn
                .query_row(
                    &format!("SELECT count(*) FROM {} WHERE id = ?1", quote(&table)),
                    [row.to_bytes().as_slice()],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            assert_eq!(count, 0, "rejected row remains in {table}");
        }
        assert!(!inner.changed.borrow().iter().any(|(_, id, _)| *id == row));
        assert!(!inner.since_tick.iter().any(|(_, id, _)| *id == row));
        Ok(())
    }

    /// Alice's speculative nested write must undo SQL and enum tags while
    /// returning its preview; a later failed child must leave Bob's parent
    /// write intact. This lower-level probe runs against actual managed SQL:
    /// public APIs cannot inspect scope nesting or tag/cache undo bookkeeping.
    pub fn check_sql_scopes_for_test(&self) -> Result<(), Error> {
        use crate::values::{EnumTags, rollbacks, tags_changing};
        let tags = Rc::new(RefCell::new(EnumTags::default()));
        self.inner().conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _jazz_scope_probe (id INTEGER PRIMARY KEY); DELETE FROM _jazz_scope_probe"
        ).map_err(storage)?;
        let before = rollbacks(&self.inner().conn);
        let preview = self.scope("preview", || {
            let mut inner = self.inner();
            inner.scope("child", |inner| {
                inner
                    .conn
                    .execute("INSERT INTO _jazz_scope_probe VALUES (7)", [])
                    .map_err(storage)?;
                tags_changing(&inner.conn, &tags);
                tags.borrow_mut().insert("temporary".into(), 0);
                Ok(ScopeOutcome::Commit(()))
            })?;
            let value: i64 = inner
                .conn
                .query_row("SELECT id FROM _jazz_scope_probe", [], |r| r.get(0))
                .map_err(storage)?;
            Ok(ScopeOutcome::Rollback(value))
        })?;
        assert_eq!(preview, 7);
        assert_eq!(tags.borrow().tag("temporary"), None);
        assert!(rollbacks(&self.inner().conn) > before);
        assert_eq!(
            self.inner()
                .conn
                .query_row("SELECT count(*) FROM _jazz_scope_probe", [], |r| r
                    .get::<_, i64>(0))
                .map_err(storage)?,
            0
        );
        self.scope("parent", || {
            let mut inner = self.inner();
            inner
                .conn
                .execute("INSERT INTO _jazz_scope_probe VALUES (1)", [])
                .map_err(storage)?;
            let rejected = inner.scope("rejected_child", |inner| {
                inner
                    .conn
                    .execute("INSERT INTO _jazz_scope_probe VALUES (2)", [])
                    .map_err(storage)?;
                inner
                    .conn
                    .execute("INSERT INTO _jazz_scope_probe VALUES (1)", [])
                    .map_err(storage)?;
                Ok(ScopeOutcome::Commit(()))
            });
            assert!(rejected.is_err());
            assert_eq!(
                inner
                    .conn
                    .query_row("SELECT count(*) FROM _jazz_scope_probe", [], |r| r
                        .get::<_, i64>(0))
                    .map_err(storage)?,
                1
            );
            inner
                .conn
                .execute("INSERT INTO _jazz_scope_probe VALUES (3)", [])
                .map_err(storage)?;
            Ok(ScopeOutcome::Commit(()))
        })?;
        assert_eq!(
            self.inner()
                .conn
                .query_row("SELECT sum(id) FROM _jazz_scope_probe", [], |r| r
                    .get::<_, i64>(0))
                .map_err(storage)?,
            4
        );
        assert!(self.inner().conn.is_autocommit());
        Ok(())
    }
}
