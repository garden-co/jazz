//! Planning queries with the client's encryption hooks ([`crate::e2ee`]):
//! equality filters on encrypted columns become token filters before a
//! query is validated, stored or sent.

use std::rc::Rc;

use super::{Inner, PreparedQuery};
use std::collections::BTreeMap;

use crate::e2ee::{E2eeRefusal, EncryptionEntry, EpochSetId, QueryPlan, Residual, SpaceAddress};
use crate::options::Error;
use crate::query::{ArraySubquery, Query};
use crate::schema::history::history_source_table;
use crate::values::unsupported;

/// What a rewritten query keeps beside its physical query.
pub(crate) struct Planned {
    pub(crate) residual: Residual,
    /// What tells this residual apart from another: a digest of its
    /// predicates and window keyed with the database's own random salt, so
    /// it confirms no guessed value outside the database. Never printed.
    pub(crate) residual_key: [u8; 32],
    pub(crate) epochs: EpochSetId,
    pub(crate) incomplete: Option<SpaceAddress>,
}

impl std::fmt::Debug for Planned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Planned")
            .field("residual", &self.residual)
            .field("epochs", &self.epochs)
            .field("incomplete", &self.incomplete)
            .finish_non_exhaustive()
    }
}

/// A digest of `residual`'s predicates, values and window, for the keys
/// that tell rewritten queries apart within one database (a live result's
/// group, the prepared query's shape), keyed with that database's random
/// `salt`. Values are hashed in their stable encoding.
fn residual_key(salt: &[u8; 32], residual: &Residual) -> [u8; 32] {
    let mut hasher = jazz_types::sha256::Hasher::new();
    hasher.update(b"jazz e2ee residual v1\0");
    hasher.update(salt);
    let mut field = |bytes: &[u8]| {
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };
    field(&(residual.preds.len() as u64).to_be_bytes());
    for pred in &residual.preds {
        field(pred.column.as_bytes());
        field(&postcard::to_allocvec(&pred.any_of).expect("encode residual values"));
    }
    field(
        &residual
            .limit
            .map_or(u64::MAX, |limit| limit as u64)
            .to_be_bytes(),
    );
    field(&(residual.offset as u64).to_be_bytes());
    *hasher.finalize().as_bytes()
}

impl PreparedQuery {
    /// For a query the encryption hooks rewrote: a space whose known chain
    /// has epochs this device does not hold, so the result may miss rows
    /// of it (protocol §4.5).
    pub fn e2ee_incomplete(&self) -> Option<&SpaceAddress> {
        self.planned.as_ref()?.incomplete.as_ref()
    }

    /// For a query the encryption hooks rewrote: the held epochs its
    /// tokens were computed for. A new held epoch needs a new preparation.
    pub fn e2ee_epochs(&self) -> Option<EpochSetId> {
        self.planned.as_ref().map(|planned| planned.epochs)
    }
}

impl Inner {
    /// Plan `query` with the encryption hooks: the query to prepare, and
    /// what its execution applies after opening its rows.
    pub(crate) fn plan_query(&self, query: &Query) -> Result<(Query, Option<Rc<Planned>>), Error> {
        let Some(hooks) = &self.e2ee else {
            return Ok((query.clone(), None));
        };
        self.refuse_unopened(&hooks.declarations().tables, query)?;
        match hooks.plan_query(query)? {
            QueryPlan::Plain => Ok((query.clone(), None)),
            QueryPlan::Rewritten {
                physical,
                residual,
                epochs,
                incomplete,
            } => Ok((
                *physical,
                Some(Rc::new(Planned {
                    residual_key: residual_key(&self.e2ee_salt, &residual),
                    residual,
                    epochs,
                    incomplete,
                })),
            )),
            QueryPlan::NeedsKey { space } => Err(E2eeRefusal::NeedsKey { space }.error()),
        }
    }

    /// Refuse a read that would return cells of an encrypted table that
    /// the hooks are never given to open: only the read's own rows are
    /// opened. Rows nested by a relation, rows an include joins, the
    /// sources of a flat join and history versions stay as stored, so such
    /// reads of an encrypted table are refused, naming it (protocol §4.5).
    fn refuse_unopened(
        &self,
        encrypted: &BTreeMap<String, EncryptionEntry>,
        query: &Query,
    ) -> Result<(), Error> {
        if let Some(table) = history_source_table(&query.table)
            && encrypted.contains_key(table)
        {
            return Err(unsupported(format!(
                "Reading the history of encrypted table `{table}`"
            )));
        }
        // A flat join's cells, the root's included, are named by source.
        let root = query.flat_join.is_some().then_some(query.table.as_str());
        let sources = query.flat_join.iter().flat_map(|join| &join.sources);
        for table in root
            .into_iter()
            .chain(sources.map(|source| source.table.as_str()))
        {
            let table = history_source_table(table).unwrap_or(table);
            if encrypted.contains_key(table) {
                return Err(unsupported(format!(
                    "Reading encrypted table `{table}` in a flat join"
                )));
            }
        }
        for include in &query.includes {
            let mut table = query.table.as_str();
            for column in include.path.split('.') {
                let Some(target) = self
                    .schema
                    .table(table)
                    .ok()
                    .and_then(|t| t.references.get(column))
                else {
                    break;
                };
                table = target;
                if encrypted.contains_key(table) {
                    return Err(unsupported(format!(
                        "Reading encrypted table `{table}` through an include (`{}`)",
                        include.path
                    )));
                }
            }
        }
        refuse_nested_encrypted(encrypted, &query.array_subqueries)
    }
}

/// Refuse a relation read that nests rows of an encrypted table: nested
/// rows are not opened (core hooks note, "Refused here"; protocol §4.5,
/// "Includes of encrypted child tables are refused"). Such rows are read
/// with a query of their own.
fn refuse_nested_encrypted(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    arrays: &[ArraySubquery],
) -> Result<(), Error> {
    for array in arrays {
        if encrypted.contains_key(&array.table) {
            return Err(unsupported(format!(
                "Reading encrypted table `{}` nested in a relation read (`{}`)",
                array.table, array.column_name
            )));
        }
        refuse_nested_encrypted(encrypted, &array.nested_arrays)?;
    }
    Ok(())
}
