//! Planning queries with the client's encryption hooks ([`crate::e2ee`]):
//! equality filters on encrypted columns become token filters before a
//! query is validated, stored or sent.

use std::rc::Rc;

use super::{Inner, PreparedQuery};
use std::collections::BTreeMap;

use crate::e2ee::{E2eeRefusal, EncryptionEntry, EpochSetId, QueryPlan, Residual, SpaceAddress};
use crate::options::{Error, ErrorCode};
use crate::query::{ArraySubquery, JoinVia, Operand, Predicate, Query, ReachableVia};
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
        let tables = &hooks.declarations().tables;
        self.refuse_reads(tables, query, true)?;
        let plan = hooks.plan_query(query)?;
        // Whatever the hook returns, a condition on an encrypted column must
        // not go upstream as written, and nothing but the conditions and
        // the window may differ from the query it was given.
        match &plan {
            QueryPlan::Plain => self.refuse_reads(tables, query, false)?,
            QueryPlan::Rewritten { physical, .. } => {
                refuse_changed_beyond_conditions(query, physical)?;
                self.refuse_reads(tables, physical, false)?
            }
            QueryPlan::NeedsKey { .. } => {}
        }
        match plan {
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

    /// Refuse the reads of `query` that no hook plans: those that return
    /// unopened cells ([`Self::refuse_unopened`]) and conditions on encrypted
    /// columns ([`refuse_unplannable`]). A union is a relation query that
    /// stays whole, so each of its arms is read the same way, with no
    /// plannable equality (the hook is not given an arm to plan), and its
    /// ordering columns are refused.
    fn refuse_reads(
        &self,
        encrypted: &BTreeMap<String, EncryptionEntry>,
        query: &Query,
        plannable: bool,
    ) -> Result<(), Error> {
        self.refuse_unopened(encrypted, query)?;
        refuse_unplannable(encrypted, query, plannable)?;
        if let Some(union) = crate::db::union_plan(query, &self.schema)? {
            let table = query.table.as_str();
            for term in &union.order_by {
                refuse_column(encrypted, table, &term.column, "a union's ORDER BY")?;
            }
            for (_, arm) in &union.arms {
                self.refuse_unopened(encrypted, arm)?;
                refuse_unplannable(encrypted, arm, false)?;
            }
        }
        Ok(())
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

/// Refuse every read shape on an encrypted column of `query`'s table except
/// what the hooks can plan: a top-level equality of an equality-indexed
/// column with a value (core hooks note, "Refused here"). Aggregates,
/// ORDER BY, ranges, IN, substring and null tests, a partial read, and an
/// equality nested in Any, Not or All would compare or return sealed
/// cells, or send the plaintext literal upstream, so they fail naming the
/// table and column.
///
/// Conditions of joins, nested joins, correlations, reachability, parent
/// inheritance and policy branches are refused whenever they name an
/// encrypted column of the table they run on: the hook plans none of them.
/// With `plannable` false even the top-level equality is refused, which is
/// how a query the hook returned is checked.
fn refuse_unplannable(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    query: &Query,
    plannable: bool,
) -> Result<(), Error> {
    refuse_in_relations(encrypted, query)?;
    let Some(entry) = encrypted.get(&query.table) else {
        return Ok(());
    };
    let table = query.table.as_str();
    let refuse = |column: &str, shape: &str| {
        entry.columns.contains_key(column).then(|| {
            unsupported(format!(
                "Reading encrypted column `{table}.{column}` {shape}"
            ))
        })
    };
    for filter in &query.filters {
        // A top-level equality of an equality column with a value is the
        // one shape the hook plans.
        if plannable
            && let Predicate::Eq(Operand::Column(column), value) = filter
            && entry.equality.contains(column)
            && matches!(
                value,
                Operand::Literal(_) | Operand::Param(_) | Operand::Claim(_)
            )
        {
            continue;
        }
        if let Some(error) = refused_in_predicate(entry, table, filter) {
            return Err(error);
        }
    }
    for branch in &query.policy_branches {
        for filter in &branch.filters {
            if let Some(error) = refused_in_predicate(entry, table, filter) {
                return Err(error);
            }
        }
    }
    for order in &query.order_by {
        if let Some(error) = refuse(&order.column, "in ORDER BY") {
            return Err(error);
        }
    }
    if let Some(aggregate) = &query.aggregate {
        let columns = aggregate
            .aggregates
            .iter()
            .filter_map(|a| a.column.as_deref())
            .chain(aggregate.group_by.as_deref());
        for column in columns {
            if let Some(error) = refuse(column, "in an aggregate") {
                return Err(error);
            }
        }
    }
    for partial in &query.partial {
        if let Some(error) = refuse(&partial.column, "in part") {
            return Err(error);
        }
    }
    for join in &query.joins {
        if let Some(column) = &join.source_column
            && let Some(error) = refuse(column, "in a join")
        {
            return Err(error);
        }
    }
    Ok(())
}

/// The error for the first encrypted column `predicate` names, if any.
fn refused_in_predicate(
    entry: &EncryptionEntry,
    table: &str,
    predicate: &Predicate,
) -> Option<Error> {
    let mut named = None;
    predicate.any_operand(&mut |operand| {
        if let Operand::Column(column) = operand
            && entry.columns.contains_key(column.as_str())
        {
            named = Some(column.clone());
        }
        named.is_some()
    });
    named.map(|column| {
        unsupported(format!(
            "Reading encrypted column `{table}.{column}` in a filter other than a top-level equality"
        ))
    })
}

/// Refuse a query the hook returned that differs from the one it was
/// given in anything but its conditions and the window the residual takes
/// over (no added relations or includes, the same table, order and
/// selection).
fn refuse_changed_beyond_conditions(original: &Query, returned: &Query) -> Result<(), Error> {
    let mut expected = returned.clone();
    expected.filters = original.filters.clone();
    let window_moved = returned.limit.is_none() && returned.offset == 0;
    if window_moved || (returned.limit == original.limit && returned.offset == original.offset) {
        expected.limit = original.limit;
        expected.offset = original.offset;
    }
    if expected != *original {
        return Err(Error::new(
            ErrorCode::Query,
            "the encryption hooks returned a query that changes more than its conditions",
        ));
    }
    Ok(())
}

/// Refuse a condition that names an encrypted column of `table` in one of
/// the places a query runs conditions on other tables than its own.
fn refuse_column(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    table: &str,
    column: &str,
    place: &str,
) -> Result<(), Error> {
    if encrypted
        .get(table)
        .is_some_and(|entry| entry.columns.contains_key(column))
    {
        return Err(unsupported(format!(
            "Reading encrypted column `{table}.{column}` in {place}"
        )));
    }
    Ok(())
}

fn refuse_filters(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    table: &str,
    filters: &[Predicate],
    place: &str,
) -> Result<(), Error> {
    let Some(entry) = encrypted.get(table) else {
        return Ok(());
    };
    for filter in filters {
        if let Some(column) = first_encrypted_column(entry, filter) {
            return Err(unsupported(format!(
                "Reading encrypted column `{table}.{column}` in {place}"
            )));
        }
    }
    Ok(())
}

fn first_encrypted_column(entry: &EncryptionEntry, predicate: &Predicate) -> Option<String> {
    let mut named = None;
    predicate.any_operand(&mut |operand| {
        if let Operand::Column(column) = operand
            && entry.columns.contains_key(column.as_str())
        {
            named = Some(column.clone());
        }
        named.is_some()
    });
    named
}

/// Refuse conditions of joins, reachability, inheritance and policy
/// branches that name an encrypted column of the table they run on.
fn refuse_in_relations(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    query: &Query,
) -> Result<(), Error> {
    let root = query.table.as_str();
    refuse_joins(encrypted, root, &query.joins)?;
    refuse_reachable(encrypted, root, &query.reachable)?;
    for inherit in &query.inherits {
        refuse_column(encrypted, root, &inherit.parent_column, "an inheritance")?;
    }
    for branch in &query.policy_branches {
        refuse_joins(encrypted, root, &branch.joins)?;
        refuse_reachable(encrypted, root, &branch.reachable)?;
        for inherit in &branch.inherits {
            refuse_column(encrypted, root, &inherit.parent_column, "an inheritance")?;
        }
    }
    refuse_correlations(encrypted, root, &query.array_subqueries)
}

/// The columns that correlate relation arrays with their parent rows.
fn refuse_correlations(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    parent: &str,
    arrays: &[ArraySubquery],
) -> Result<(), Error> {
    for array in arrays {
        let place = "a relation correlation";
        refuse_column(encrypted, parent, &array.outer_column, place)?;
        refuse_column(encrypted, &array.table, &array.inner_column, place)?;
        refuse_correlations(encrypted, &array.table, &array.nested_arrays)?;
    }
    Ok(())
}

/// `parent` is the table the joins are evaluated against.
fn refuse_joins(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    parent: &str,
    joins: &[JoinVia],
) -> Result<(), Error> {
    for join in joins {
        let table = join.table.as_str();
        refuse_column(encrypted, table, &join.on_column, "a join")?;
        if let Some(column) = &join.source_column {
            refuse_column(encrypted, parent, column, "a join")?;
        }
        if let Some(lookup) = &join.source_lookup {
            refuse_column(encrypted, parent, &lookup.row_id_source_column, "a join")?;
            refuse_column(encrypted, &lookup.table, &lookup.value_column, "a join")?;
        }
        for correlation in &join.correlated_filters {
            refuse_column(encrypted, table, &correlation.join_column, "a join")?;
            refuse_column(encrypted, parent, &correlation.source_column, "a join")?;
        }
        refuse_filters(encrypted, table, &join.filters, "a join filter")?;
        refuse_joins(encrypted, table, &join.nested_joins)?;
    }
    Ok(())
}

fn refuse_reachable(
    encrypted: &BTreeMap<String, EncryptionEntry>,
    root: &str,
    reachable: &[ReachableVia],
) -> Result<(), Error> {
    for via in reachable {
        let place = "a reachability";
        if let Operand::Column(column) = &via.from {
            refuse_column(encrypted, root, column, place)?;
        }
        let (access, edge) = (via.access_table.as_str(), via.edge_table.as_str());
        refuse_column(encrypted, access, &via.access_row_column, place)?;
        refuse_column(encrypted, access, &via.access_team_column, place)?;
        refuse_column(encrypted, edge, &via.edge_member_column, place)?;
        refuse_column(encrypted, edge, &via.edge_parent_column, place)?;
        refuse_filters(encrypted, access, &via.access_filters, place)?;
        refuse_filters(encrypted, edge, &via.edge_filters, place)?;
        if let Some(seed) = &via.seed {
            let table = seed.table.as_str();
            refuse_column(encrypted, table, &seed.team_column, place)?;
            if let Some(column) = &seed.user_column {
                refuse_column(encrypted, table, column, place)?;
            }
            refuse_filters(encrypted, table, &seed.filters, place)?;
        }
    }
    Ok(())
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
