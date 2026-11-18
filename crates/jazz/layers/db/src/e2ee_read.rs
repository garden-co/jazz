//! Opening rows with the client's encryption hooks ([`crate::e2ee`])
//! before they reach a reader.

use std::collections::BTreeMap;

use super::{Db, Inner, PreparedQuery, ReadSpec};
use crate::e2ee::{E2eeRefusal, ReadSite};
use crate::options::{Error, ErrorCode};
use crate::records::Value;
use crate::values::CurrentRow;

impl Db {
    /// Re-run every live result that reads one of `tables`, so rows the
    /// encryption hooks can open now (a key arrived) reach their
    /// subscribers as additions, and rows they no longer can leave.
    pub fn refresh_reads_of(&self, tables: &[&str]) -> Result<(), Error> {
        let mut inner = self.inner();
        let stored = tables
            .iter()
            .map(|table| inner.schema.stored_table(table).map(str::to_owned))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        inner.refresh_reading(&stored);
        Ok(())
    }
}

impl Inner {
    /// Open `rows`, which `prepared` read under `read`, before they leave
    /// toward a reader, then apply a rewritten query's residual. A strict
    /// read fails rather than leave a row out (protocol §4.4).
    pub(crate) fn open_read(
        &self,
        prepared: &PreparedQuery,
        read: &ReadSpec,
        strict: bool,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<(), Error> {
        let Some(hooks) = self.e2ee.clone() else {
            return Ok(());
        };
        let table = prepared.query.table.as_str();
        let lens_defaults = if hooks.declarations().tables.contains_key(table) {
            self.lens_defaults(table)
        } else {
            BTreeMap::new()
        };
        let site = ReadSite {
            strict,
            table,
            query: &prepared.query,
            shape: prepared.shape,
            splices: &read.splices,
            lens_defaults: &lens_defaults,
        };
        let outcome = hooks.open_rows(&site, rows)?;
        if strict && outcome.leaves_rows_out() {
            if let Some((space, _)) = outcome.missing_epochs.first() {
                return Err(E2eeRefusal::NeedsKey {
                    space: space.clone(),
                }
                .error());
            }
            return Err(Error::new(
                ErrorCode::Query,
                format!("a strict read of `{table}` would leave out rows it cannot open"),
            ));
        }
        if let Some(planned) = &prepared.planned {
            planned.residual.apply(rows)?;
        }
        Ok(())
    }

    /// The columns of `table` that a migration added to rows already
    /// stored, with the value such a row reads: a stored column's default
    /// that the running schema does not declare (see `Schema::stored_view`).
    fn lens_defaults(&self, table: &str) -> BTreeMap<String, Value> {
        let mut out = BTreeMap::new();
        let Ok(t) = self.schema.table(table) else {
            return out;
        };
        let Some(stored) = self.physical.tables.get(&t.stored) else {
            return out;
        };
        let declared = self
            .schema
            .jazz
            .tables()
            .iter()
            .find(|declared| declared.name == table);
        for column in &t.columns {
            let Some(default) = stored
                .column(&column.stored)
                .and_then(|stored| stored.default.as_ref())
            else {
                continue;
            };
            let own = declared
                .and_then(|declared| declared.columns.iter().find(|c| c.name == column.name))
                .and_then(|declared| declared.default.as_ref());
            if own != Some(default) {
                out.insert(column.name.clone(), default.clone());
            }
        }
        out
    }
}
