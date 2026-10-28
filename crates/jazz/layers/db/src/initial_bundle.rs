//! Experimental fast initial load: an authority writes, for one reader and
//! a set of queries, the replica that reader's first sync would build, as a
//! ready-made SQLite file. A client opens the file instead of receiving
//! every row over sync, connects, and resumes from it.
//!
//! The rows are selected set-based: each query compiles, with the reader's
//! read policies, to the SQL a sync session's answer runs, and its result's
//! row ids drive one `INSERT … SELECT` per table into the attached bundle.

use super::*;

/// What [`Db::export_initial_bundle`] wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitialBundle {
    /// The authority's commit seq the bundle is current through.
    pub through: u64,
    /// Rows written into the bundle.
    pub rows: u64,
}

impl Db {
    /// Write to `path` the replica `reader` would hold after a first sync of
    /// `queries`: their visible rows, current images only, as the reader's
    /// policies allow, and the authors those rows name. `client` is the
    /// identity the bundle's store is created with. Experimental: large
    /// values and queries over history or other branches are refused.
    pub async fn export_initial_bundle(
        &self,
        queries: &[PreparedQuery],
        reader: AuthorSubject,
        client: DbIdentity,
        path: &std::path::Path,
    ) -> Result<InitialBundle, Error> {
        if self.is_client() {
            return Err(Error::new(
                ErrorCode::Query,
                "Building an initial bundle on a replica is not supported yet",
            ));
        }
        // The bundle's store gets this schema's tables and bookkeeping.
        let schema = self.inner().owner.jazz.clone();
        let bundle =
            Db::open_at(DbConfig::new(schema, client), path, FileOptions::default()).await?;
        // It opens as the reader's replica.
        bundle.become_replica();
        bundle.close().await?;
        drop(bundle);

        let inner = self.inner();
        if !inner.conn.is_autocommit() {
            return Err(Error::new(
                ErrorCode::Query,
                "Building an initial bundle inside a transaction is not supported yet",
            ));
        }
        inner
            .conn
            .execute(
                "ATTACH DATABASE ?1 AS bundle",
                [path.to_string_lossy().as_ref()],
            )
            .map_err(storage)?;
        // One self-contained file to ship: no write-ahead log beside it.
        inner
            .conn
            .query_row("PRAGMA bundle.journal_mode = DELETE", [], |_| Ok(()))
            .map_err(storage)?;
        inner
            .conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(storage)?;
        let written = inner.fill_initial_bundle(queries, reader);
        let ended = match &written {
            Ok(_) => inner.conn.execute_batch("COMMIT"),
            Err(_) => inner.conn.execute_batch("ROLLBACK"),
        };
        let detached = inner.conn.execute_batch("DETACH DATABASE bundle");
        let rows = written?;
        ended.map_err(storage)?;
        detached.map_err(storage)?;
        Ok(InitialBundle {
            through: inner.seq,
            rows,
        })
    }
}

impl Inner {
    fn fill_initial_bundle(
        &self,
        queries: &[PreparedQuery],
        reader: AuthorSubject,
    ) -> Result<u64, Error> {
        let mut rows = 0u64;
        let mut tables = BTreeSet::new();
        for prepared in queries {
            let read = ReadSpec::default();
            let compiled = self.compile(prepared, Some(reader), &read)?;
            if compiled.aggregate || compiled.union || compiled.versioned || compiled.joined > 0 {
                return Err(Error::new(
                    ErrorCode::Query,
                    format!(
                        "An initial bundle of {} is not supported yet: only plain table queries are",
                        prepared.query.table
                    ),
                ));
            }
            let stored = self.owner.table(&prepared.query.table)?.stored.clone();
            let columns = self.bundle_columns(&stored)?;
            let list = columns
                .iter()
                .map(|column| quote(column))
                .collect::<Vec<_>>()
                .join(", ");
            rows += self.with_preps(&compiled.preps, |_| {
                // The compiled read's first column is the row id.
                let id = self
                    .conn
                    .prepare(&compiled.sql)
                    .map_err(storage)?
                    .column_name(0)
                    .map_err(storage)?
                    .to_owned();
                let sql = format!(
                    "INSERT OR IGNORE INTO bundle.{table} ({list}) \
                     SELECT {list} FROM main.{table} \
                     WHERE _b = X'' AND id IN (SELECT {id} FROM ({read}))",
                    table = quote(&stored),
                    id = quote(&id),
                    read = compiled.sql,
                );
                self.conn
                    .execute(&sql, params_from_iter(compiled.params.iter()))
                    .map(|n| n as u64)
                    .map_err(storage)
            })?;
            tables.insert(stored);
        }
        for table in &tables {
            self.refuse_bundle_large_values(table)?;
        }
        self.copy_bundle_authors(&tables)?;
        // The bundle has seen the authority through its current seq, as a
        // replica that settled these answers has, and its first subscribe of
        // each query resumes from there.
        let settled = queries
            .iter()
            .map(|prepared| {
                sync::bundle_key(&sync::remote_query(&prepared.query), &prepared.binding)
            })
            .collect::<Vec<_>>();
        self.conn
            .execute(
                "INSERT OR REPLACE INTO bundle._jazz_meta (k, v) VALUES ('known', ?1), ('bundle', ?2)",
                [
                    postcard::to_allocvec(&self.seq).expect("encode seq"),
                    postcard::to_allocvec(&(self.seq, settled)).expect("encode bundled queries"),
                ],
            )
            .map_err(storage)?;
        Ok(rows)
    }

    /// The bundle table's columns but its rowid alias.
    fn bundle_columns(&self, table: &str) -> Result<Vec<String>, Error> {
        let mut statement = self
            .conn
            .prepare(&format!("PRAGMA bundle.table_info({})", quote(table)))
            .map_err(storage)?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        Ok(columns
            .into_iter()
            .filter(|column| column != "rid")
            .collect())
    }

    /// A chunked cell names large-value nodes the bundle would also have to
    /// carry.
    fn refuse_bundle_large_values(&self, table: &str) -> Result<(), Error> {
        let prefix = format!("{table}__lv_");
        let mut statement = self
            .conn
            .prepare("SELECT name FROM bundle.sqlite_master WHERE type = 'index' AND tbl_name = ?1")
            .map_err(storage)?;
        let indexes = statement
            .query_map([table], |row| row.get::<_, String>(0))
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        for column in indexes
            .iter()
            .filter_map(|index| index.strip_prefix(&prefix))
        {
            let chunked: bool = self
                .conn
                .query_row(
                    &format!(
                        "SELECT EXISTS (SELECT 1 FROM bundle.{} WHERE typeof({}) = 'integer')",
                        quote(table),
                        quote(column)
                    ),
                    [],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            if chunked {
                return Err(Error::new(
                    ErrorCode::Query,
                    format!(
                        "Large values in an initial bundle ({table}.{column}) are not supported yet"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The interned authors the bundled rows name, under the authority's
    /// codes: a code the bundle's store already holds must mean the same
    /// author.
    fn copy_bundle_authors(&self, tables: &BTreeSet<String>) -> Result<(), Error> {
        let mut referenced = Vec::new();
        for table in tables {
            for column in ["$createdBy", "$updatedBy"] {
                referenced.push(format!(
                    "SELECT {column} FROM bundle.{table}",
                    column = quote(column),
                    table = quote(table)
                ));
            }
        }
        if referenced.is_empty() {
            return Ok(());
        }
        let referenced = referenced.join(" UNION ");
        let clashes: i64 = self
            .conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM bundle._jazz_authors b JOIN main._jazz_authors m ON m.n = b.n \
                     WHERE b.n IN ({referenced}) \
                     AND (b.account IS NOT m.account OR b.issuer IS NOT m.issuer OR b.subject IS NOT m.subject)"
                ),
                [],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if clashes > 0 {
            return Err(Error::new(
                ErrorCode::Query,
                "Remapping author codes into an initial bundle is not supported yet",
            ));
        }
        self.conn
            .execute(
                &format!(
                    "INSERT OR IGNORE INTO bundle._jazz_authors (n, account, issuer, subject) \
                     SELECT n, account, issuer, subject FROM main._jazz_authors WHERE n IN ({referenced})"
                ),
                [],
            )
            .map_err(storage)?;
        Ok(())
    }
}
