//! Opening rows with the client's encryption hooks ([`crate::e2ee`])
//! before they reach a reader.

use std::collections::BTreeMap;

use super::{Db, PreparedQuery, ReadSpec, Store, UnreadableRow};
#[cfg(feature = "e2ee-client")]
use crate::e2ee::UnreadableCode;
use crate::e2ee::{E2eeRefusal, EpochId, OpenOutcome, ReadSite, SpaceAddress};
use crate::ids::RowUuid;
use crate::options::{Error, ErrorCode};
use crate::records::Value;
use crate::values::{CurrentRow, Schema};

/// The missing keys behind a subscription's unreadable rows, by space and row.
pub(super) type MissingReadKeys = BTreeMap<SpaceAddress, BTreeMap<RowUuid, EpochId>>;

impl Db {
    /// Update the reasons already reported for `space` in live results.
    #[cfg(feature = "e2ee-client")]
    pub async fn update_unreadable_reasons(
        &self,
        space: &SpaceAddress,
        reason: impl Fn(&str) -> UnreadableCode,
    ) {
        let _operation = self.write_operation().await;
        self.inner
            .without_overlay()
            .update_unreadable(space, reason);
    }

    /// Re-run every live result that reads one of `tables`, so rows the
    /// encryption hooks can open now (a key arrived) reach their
    /// subscribers as additions, and rows they no longer can leave.
    #[cfg(feature = "e2ee-client")]
    pub async fn refresh_reads_of(&self, tables: &[&str]) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        let schema = self.operation_schema();
        let stored = tables
            .iter()
            .map(|table| schema.stored_table(table).map(str::to_owned))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        self.inner.without_overlay().refresh_reading(&stored).await;
        Ok(())
    }
}

impl Db {
    /// The columns of `table` that a schema migration added to rows stored
    /// before them, each read as the migration's value until written: the
    /// columns [`ReadSite::is_lens_default`] can hold. A column created
    /// with its table, or one this schema declares itself, is not among
    /// them.
    #[cfg(feature = "e2ee-client")]
    pub fn lens_added_columns(
        &self,
        table: &str,
    ) -> Result<std::collections::BTreeSet<String>, Error> {
        let schema = self.operation_schema();
        schema.table(table)?;
        Ok(self
            .inner
            .lens_defaults(&schema, table)
            .into_keys()
            .collect())
    }
}

impl Store {
    /// Open `rows`, which `prepared` read under `read`, before they leave
    /// toward a reader, then apply a rewritten query's residual. A strict
    /// read fails rather than leave a row out (protocol §4.4); any other
    /// read answers the rows it left out, one entry per row, so its reader
    /// can tell the result is incomplete (a window applied before opening
    /// returns a short page, which these entries make up).
    pub(crate) fn open_read(
        &self,
        prepared: &PreparedQuery,
        read: &ReadSpec,
        strict: bool,
        rows: &mut Vec<CurrentRow>,
        missing: Option<&mut MissingReadKeys>,
    ) -> Result<Vec<UnreadableRow>, Error> {
        let Some(hooks) = self.e2ee_hooks().cloned() else {
            return Ok(Vec::new());
        };
        let table = prepared.query.table.as_str();
        let lens_defaults = if hooks.declarations().tables.contains_key(table) {
            let owner = self.owner();
            self.lens_defaults(prepared.source_schema(&owner), table)
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
        // The outcome's reports (protocol §4.3) stay with the hooks that
        // built them: their runtime delivers them. Core reads the outcome
        // to fail a strict read and to tell a reader which rows it left out.
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
        if let Some(space) = prepared
            .planned
            .as_ref()
            .and_then(|plan| plan.incomplete.as_ref())
        {
            hooks.report_incomplete_query(table, space);
        }
        Ok(left_out(&outcome, missing))
    }

    /// The columns of `table` that a migration added to rows already
    /// stored, with the value such a row reads: a stored column's default
    /// that the running schema does not declare (see `Schema::stored_view`).
    fn lens_defaults(&self, schema: &Schema, table: &str) -> BTreeMap<String, Value> {
        let mut out = BTreeMap::new();
        let Ok(t) = schema.table(table) else {
            return out;
        };
        let physical = self.physical();
        let Some(stored) = physical.tables.get(&t.stored) else {
            return out;
        };
        let declared = schema
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

/// The rows `outcome` left out, once each, in the order first reported: a
/// row's reports that still return it (an empty cell with its default, a
/// stale space) are notices, not omissions.
fn left_out(
    outcome: &OpenOutcome,
    mut missing: Option<&mut MissingReadKeys>,
) -> Vec<UnreadableRow> {
    let mut seen = std::collections::BTreeSet::new();
    outcome
        .unreadable
        .iter()
        .enumerate()
        .filter(|(_, report)| !report.code.returns_row())
        .filter_map(|(index, report)| {
            let id = report.id?;
            if !seen.insert(id) {
                return None;
            }
            if let Some(missing) = missing.as_deref_mut()
                && let Some(space) = &report.space
                && let Some(epoch) = outcome.missing_reports.get(&index)
            {
                missing
                    .entry(space.clone())
                    .or_default()
                    .insert(id, epoch.clone());
            }
            Some(UnreadableRow {
                table: report.table.clone(),
                id,
                column: report.column.clone(),
                code: report.code.as_str(),
            })
        })
        .collect()
}
