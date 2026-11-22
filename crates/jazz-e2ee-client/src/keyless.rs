//! A keyless backend runs application queries on plain columns. A browser
//! worker instead serves a tab's physical queries over sealed rows. The
//! tab holds the keys and opens the rows it receives through sync.

use std::collections::BTreeSet;
use std::rc::Rc;

use jazz::db::{CurrentRow, Error, ErrorCode};
use jazz::e2ee::{
    E2eeDeclarations, E2eeHooks, OpenOutcome, QueryPlan, ReadSite, SealError, StreamSealer,
    StreamTarget, WriteCells, WriteKind,
};
use jazz::query::{Operand, Predicate, Query};
use jazz::records::Value;
use jazz::tools::deployment::artifact::encryption::{
    EQUALITY_PREFIX, equality_column, is_equality_token_column,
};

use crate::hooks::unsupported_query;

/// The hooks of a keyless database, built from the app's own declarations.
pub struct KeylessHooks {
    declarations: E2eeDeclarations,
    relay: bool,
}

impl KeylessHooks {
    pub fn hooks(declarations: E2eeDeclarations) -> Rc<dyn E2eeHooks> {
        Rc::new(Self {
            declarations,
            relay: false,
        })
    }

    /// The browser worker serves sealed rows to tabs that hold the keys.
    pub fn relay(declarations: E2eeDeclarations) -> Rc<dyn E2eeHooks> {
        Rc::new(Self {
            declarations,
            relay: true,
        })
    }

    /// The encrypted columns of `table` and the equality token columns that
    /// go with them; empty for a table that is not encrypted.
    fn sealed(&self, table: &str) -> BTreeSet<String> {
        let Some(entry) = self.declarations.tables.get(table) else {
            return BTreeSet::new();
        };
        entry
            .columns
            .keys()
            .cloned()
            .chain(entry.equality.iter().map(|name| equality_column(name)))
            .collect()
    }
}

fn refused(table: &str, column: &str) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!(
            "the backend holds no encryption keys; encrypted columns are written by clients \
             (`{table}.{column}`)"
        ),
    )
}

impl E2eeHooks for KeylessHooks {
    fn declarations(&self) -> &E2eeDeclarations {
        &self.declarations
    }

    /// Built from an encrypted app's declarations, even when the version
    /// declares no encrypted column (it still has Jazz's system tables).
    fn built_from_encrypted_app(&self) -> bool {
        true
    }

    fn relays_token_filter(&self, table: &str, filter: &Predicate) -> bool {
        self.relay
            && matches!(filter, Predicate::In(Operand::Column(name), tokens)
                if is_equality_token_column(self.declarations.tables.get(table), name)
                    && !tokens.is_empty()
                    && tokens.iter().all(|token| matches!(token, Operand::Literal(Value::Bytes(_)))))
    }

    fn seal_write(&self, write: &mut WriteCells<'_>) -> Result<(), SealError> {
        let sealed = self.sealed(write.table);
        if matches!(write.kind, WriteKind::Move { .. })
            && self.declarations.tables.contains_key(write.table)
        {
            return Err(SealError::Refused(refused(write.table, "(space)")));
        }
        match write.cells.keys().find(|name| sealed.contains(*name)) {
            Some(name) => Err(SealError::Refused(refused(write.table, name))),
            None => Ok(()),
        }
    }

    fn open_rows(
        &self,
        site: &ReadSite<'_>,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<OpenOutcome, Error> {
        if self.relay {
            return Ok(OpenOutcome::default());
        }
        let sealed = self.sealed(site.table);
        for row in rows.iter_mut() {
            for name in &sealed {
                row.remove_cell(name);
            }
            // A column with the equality-token prefix is reserved: a query
            // that selects one by name is refused, so a full-row read does
            // not answer it either, declared as a token or not.
            let reserved: Vec<String> = row
                .cells()
                .filter(|(name, _)| name.starts_with(EQUALITY_PREFIX))
                .map(|(name, _)| name.to_owned())
                .collect();
            for name in &reserved {
                row.remove_cell(name);
            }
        }
        Ok(OpenOutcome::default())
    }

    fn plan_query(&self, query: &Query) -> Result<QueryPlan, Error> {
        if self.relay {
            // The tab already planned the query. Core still checks that
            // a plain plan has no condition on an encrypted column.
            return Ok(QueryPlan::Plain);
        }
        let sealed = self.sealed(&query.table);
        if sealed.is_empty() {
            return Ok(QueryPlan::Plain);
        }
        let table = query.table.as_str();
        // What core leaves to the hook: a top-level equality of an
        // equality column, which a client turns into tokens. Without keys
        // there are no tokens to compare.
        let mut filtered = false;
        for filter in &query.filters {
            filter.any_operand(&mut |operand| {
                filtered |= matches!(operand, Operand::Column(name) if sealed.contains(name));
                filtered
            });
        }
        if filtered {
            return Err(unsupported_query(
                "an encrypted column filtered on a backend (it holds no keys)",
                table,
            ));
        }
        if query
            .select
            .as_ref()
            .is_some_and(|select| select.iter().any(|name| sealed.contains(name)))
        {
            return Err(unsupported_query(
                "an encrypted column selected from a backend (it holds no keys)",
                table,
            ));
        }
        Ok(QueryPlan::Plain)
    }

    fn seal_stream(&self, target: &StreamTarget<'_>) -> Result<Box<dyn StreamSealer>, SealError> {
        Err(SealError::Refused(refused(target.table, target.column)))
    }
}
