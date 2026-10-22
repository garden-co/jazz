//! Jazz queries, predicates, policies and relation IR compiled to SQL.
//!
//! Every boolean this module emits is two-valued (0 or 1, never NULL), so
//! `NOT` behaves like Jazz's `Not` and a missing session value can never
//! match a NULL cell by accident.

use std::collections::{BTreeMap, BTreeSet};

use crate::ids::AuthorSubject;
use crate::options::Error;
use crate::query::{
    ArraySubquery, ArraySubqueryRequirement, JoinTarget, Operand, OrderDirection, Predicate, Query,
    RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr, RelationKeyRef,
    RelationPredicate, RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
use crate::records::{Value, ValueType};
use crate::tools::{CmpOp, Operation, PolicyExpr, PolicyValue};

use crate::values::{Schema, Sql, Table, query_error, quote, to_sql, unsupported};

/// Inherits chains with no explicit bound unroll this deep.
const DEFAULT_INHERITS_DEPTH: usize = 10;
/// Fixpoint gathers carry a depth column only as a runaway guard.
const FIXPOINT_GUARD: usize = 1_000;

/// The identity a read or write is authorized as.
#[derive(Clone, Debug, Default)]
pub struct Session {
    /// The internal authority capability: no policies apply.
    pub system: bool,
    pub issuer: Option<String>,
    pub subject: Option<String>,
    pub account: Option<uuid::Uuid>,
    /// Post-admission claims, keyed as `Db::set_identity_claims` receives them.
    pub claims: BTreeMap<String, Value>,
    /// Set when compilation read any session value: the SQL is then
    /// specific to this identity and cannot be shared across identities.
    pub used: std::cell::Cell<bool>,
}

impl Session {
    pub fn system() -> Self {
        Self {
            system: true,
            ..Self::default()
        }
    }

    pub fn for_author(author: AuthorSubject, claims: BTreeMap<String, Value>) -> Self {
        if author == AuthorSubject::SYSTEM {
            return Self {
                claims,
                ..Self::system()
            };
        }
        let (issuer, subject) = author.principal_parts();
        Self {
            system: false,
            issuer: Some(issuer),
            subject: Some(subject),
            account: author.account_id().map(|account| account.0),
            claims,
            used: Default::default(),
        }
    }

    /// `session.<path>` as a value, or `None` when the session has none.
    pub fn resolve(&self, path: &[String]) -> Option<Value> {
        self.used.set(true);
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        let value = match path.as_slice() {
            ["user", "identity", "subject"] => Value::String(self.subject.clone()?),
            ["user", "identity", "issuer"] => Value::String(self.issuer.clone()?),
            ["user", "account"] => Value::Uuid(self.account?),
            ["claims", name] => self
                .claims
                .get(&crate::query::provider_claim_key(name))?
                .clone(),
            _ => return None,
        };
        unwrap_nullable(value)
    }

    pub fn claim(&self, key: &str) -> Option<Value> {
        self.used.set(true);
        unwrap_nullable(self.claims.get(key)?.clone())
    }
}

fn unwrap_nullable(value: Value) -> Option<Value> {
    match value {
        Value::Nullable(None) => None,
        Value::Nullable(Some(inner)) => unwrap_nullable(*inner),
        value => Some(value),
    }
}

/// An operand ready for SQL.
enum Term {
    Sql(String),
    /// A large-capable column's cell: compared through its sort key, with
    /// what the key cannot answer decided in Rust ([`Prep`]).
    Large(LargeColumn),
    /// A bound value (literal, parameter, session or claim value): its
    /// placeholder and the value itself.
    Bound(String, Sql),
    /// A session or claim value the identity does not have.
    Missing,
    /// NaN: unordered against everything, SQLite cannot store it.
    NaN,
}

impl Term {
    /// The SQL of a present operand.
    fn sql(&self) -> Option<&str> {
        match self {
            Term::Sql(sql) | Term::Bound(sql, _) => Some(sql),
            Term::Large(column) => Some(&column.cell),
            Term::Missing | Term::NaN => None,
        }
    }
}

#[derive(Clone)]
struct LargeColumn {
    cell: String,
    table: String,
    column: String,
    kind: jazz_types::large_values::LargeValueKind,
}

/// A question about chunked cells that their sort key cannot answer,
/// answered in Rust before the statement runs. Each writes its answers to
/// `_jazz_lv_scratch` under its slot, which the statement reads. The slot
/// is a hash of the question, so the same query compiles to the same SQL.
#[derive(Clone, Debug)]
pub enum Prep {
    /// The chunked cells of `table.column` that pass `test`.
    Match {
        slot: i64,
        table: String,
        column: String,
        test: MatchTest,
    },
    /// Ranks for the chunked cells of `table.column` that share a sort key
    /// with another: their order by full logical value.
    Rank {
        slot: i64,
        table: String,
        column: String,
    },
    /// A content id for every chunked cell of these columns, so cells of
    /// one logical value (built differently) compare equal.
    Class {
        slot: i64,
        columns: Vec<(String, String)>,
    },
}

/// What a chunked cell is matched against: an inline-form cell.
#[derive(Clone, Debug)]
pub enum MatchTest {
    Contains(Vec<u8>),
    Cmp(CmpOp, Vec<u8>),
}

impl Prep {
    fn new(make: impl FnOnce(i64) -> Prep, question: &str) -> Prep {
        let hash = blake3::hash(question.as_bytes());
        let slot = i64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("8 bytes"));
        make(slot)
    }

    pub fn slot(&self) -> i64 {
        match self {
            Prep::Match { slot, .. } | Prep::Rank { slot, .. } | Prep::Class { slot, .. } => *slot,
        }
    }
}

/// SQL: whether `cell` is among a prep's answers.
fn scratch_has(slot: i64, cell: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM _jazz_lv_scratch WHERE slot = {slot} AND cell = CAST({cell} AS BLOB))"
    )
}

/// SQL: a prep's answer value for `cell`.
fn scratch_value(slot: i64, cell: &str) -> String {
    format!("(SELECT v FROM _jazz_lv_scratch WHERE slot = {slot} AND cell = CAST({cell} AS BLOB))")
}

/// SQL text plus its positional parameters.
pub struct Ctx<'a> {
    pub schema: &'a Schema,
    /// `Some` applies read policies as this session (unless it is system).
    pub policy: Option<&'a Session>,
    /// Supplies `Operand::Claim` values.
    pub claims: &'a Session,
    pub binding: &'a BTreeMap<String, Value>,
    pub params: Vec<Sql>,
    /// Every table the compiled SQL reads: the invalidation dependencies.
    pub tables: BTreeSet<String>,
    /// The branch view every branched table is read through.
    pub view: Option<View>,
    /// An as-of cut every table is read through.
    pub cut: Option<Cut>,
    /// Questions the statement needs answered first.
    pub preps: Vec<Prep>,
    next_alias: usize,
}

/// A frozen snapshot: the state as of commit `seq`, plus the open
/// transaction's own staged writes (written at `own`).
#[derive(Clone, Copy, Debug)]
pub struct Cut {
    pub seq: u64,
    pub own: u64,
}

/// A head branch, optionally composed over a live base branch.
#[derive(Clone, Debug)]
pub struct View {
    pub head: crate::protocol::BranchSelector,
    pub base: Option<crate::protocol::BranchSelector>,
}

/// A compiled root query.
#[derive(Clone)]
pub struct Compiled {
    pub sql: String,
    pub params: Vec<Sql>,
    pub projection: Vec<(String, ValueType)>,
    pub tables: BTreeSet<String>,
    /// Rows are aggregate groups: column 0 is the group key, hashed to an id.
    pub aggregate: bool,
    /// Run before the statement.
    pub preps: Vec<Prep>,
    /// The statement with prefix-tie ranks in its order, used instead when
    /// a rank prep finds chunked values sharing a sort key.
    pub ranked_sql: Option<String>,
}

impl<'a> Ctx<'a> {
    pub fn new(
        schema: &'a Schema,
        policy: Option<&'a Session>,
        claims: &'a Session,
        binding: &'a BTreeMap<String, Value>,
    ) -> Self {
        Self {
            schema,
            policy: policy.filter(|session| !session.system),
            claims,
            binding,
            params: Vec::new(),
            tables: BTreeSet::new(),
            view: None,
            cut: None,
            preps: Vec::new(),
            next_alias: 0,
        }
    }

    pub fn with_view(mut self, view: Option<View>) -> Self {
        self.view = view;
        self
    }

    pub fn with_cut(mut self, cut: Option<Cut>) -> Self {
        self.cut = cut;
        self
    }

    /// A table's rows, as of the cut when there is one. Current rows written
    /// at or before the cut (or by the transaction itself) are visible as
    /// they are; for rows changed since, the newest superseded image at or
    /// before the cut stands in. Rows created after the cut have neither.
    fn rows(&mut self, table: &Table, columns: &str) -> String {
        let name = quote(&table.name);
        let Some(cut) = self.cut else {
            return name;
        };
        let seq = self.bind(Sql::Integer(cut.seq as i64));
        let own = self.bind(Sql::Integer(cut.own as i64));
        let history = quote(&format!("{}__h", table.name));
        let h_columns = columns
            .replace("rid, ", "")
            .split(", ")
            .map(|c| format!("h.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT {columns} FROM {name} WHERE _seq <= {seq} OR _seq = {own} \
             UNION ALL SELECT NULL, {h_columns} FROM {history} AS h JOIN {name} AS c \
             ON c.id = h.id AND c._b = h._b WHERE c._seq > {seq} AND c._seq <> {own} \
             AND h._seq = (SELECT MAX(h2._seq) FROM {history} AS h2 \
             WHERE h2.id = h.id AND h2._b = h._b AND h2._seq <= {seq}))"
        )
    }

    /// The SQL source a table is read from. Under a branch view a branched
    /// table is its effective rows: head rows (tombstones included, so they
    /// mask), then base rows the head does not override. Filters, policies,
    /// ordering and windows all apply after this masking.
    fn source(&mut self, table: &Table) -> Result<String, Error> {
        // Explicit columns: a schema view reads its own columns of the
        // physical (unioned) table.
        let mut columns = "rid, id, _b, _seq, _del".to_owned();
        for column in table.stored() {
            columns.push_str(&format!(", {}", quote(&column.name)));
        }
        let view = self.view.clone().filter(|_| !table.branch_by.is_empty());
        let Some(view) = view else {
            if self.cut.is_none() {
                return Ok(quote(&table.name));
            }
            return Ok(self.rows(table, &columns));
        };
        let head = self.bind(Sql::Blob(table.selector_key(&view.head)?));
        let rows = self.rows(table, &columns);
        Ok(match view.base {
            None => format!("(SELECT {columns} FROM {rows} WHERE _b = {head})"),
            Some(base) => {
                // Inherited rows are read as head rows: their branch key and
                // branch columns are the head's, so predicates, policies and
                // projections see one effective branch.
                let head_cells = table.selector_cells(&view.head)?;
                let mut inherited = format!("m.rid, m.id, {head}, m._seq, m._del");
                for column in table.stored() {
                    match head_cells.get(&column.name) {
                        Some(value) => {
                            let term = match to_sql(value)? {
                                Some(sql) => self.bind(sql),
                                None => "NULL".to_owned(),
                            };
                            inherited.push_str(&format!(", {term}"));
                        }
                        None => inherited.push_str(&format!(", m.{}", quote(&column.name))),
                    }
                }
                let base = self.bind(Sql::Blob(table.selector_key(&base)?));
                let base_rows = self.rows(table, &columns);
                let head_rows = self.rows(table, &columns);
                format!(
                    "(SELECT {columns} FROM {rows} WHERE _b = {head} UNION ALL SELECT {inherited} \
                     FROM {base_rows} AS m WHERE m._b = {base} AND NOT EXISTS (SELECT 1 FROM {head_rows} AS x \
                     WHERE x.id = m.id AND x._b = {head}))"
                )
            }
        })
    }

    fn alias(&mut self, prefix: &str) -> String {
        self.next_alias += 1;
        format!("{prefix}{}", self.next_alias)
    }

    fn bind(&mut self, value: Sql) -> String {
        self.params.push(value);
        format!("?{}", self.params.len())
    }

    fn value_term(&mut self, value: &Value) -> Result<Term, Error> {
        Ok(match to_sql(value)? {
            None => Term::NaN,
            Some(sql) => Term::Bound(self.bind(sql.clone()), sql),
        })
    }

    fn present(&mut self, value: Option<Value>) -> Result<Term, Error> {
        match value {
            None => Ok(Term::Missing),
            Some(value) => self.value_term(&value),
        }
    }

    fn table(&mut self, name: &str) -> Result<&'a Table, Error> {
        self.tables.insert(name.to_owned());
        self.schema.table(name)
    }

    // ---- root queries ---------------------------------------------------

    /// `GROUP BY` over the effective, filtered, policy-checked rows. Each
    /// group is a synthetic row whose id is a hash of its group key (column
    /// 0 carries the raw key; the executor hashes it), so subscriptions diff
    /// groups like rows. Output cells: the group column, then aggregates.
    fn compile_aggregate(
        mut self,
        query: &Query,
        aggregate: &crate::query::AggregateQuery,
        table: &Table,
        alias: &str,
        wheres: &[String],
    ) -> Result<Compiled, Error> {
        use crate::query::AggregateFunction as F;
        let mut projection = Vec::new();
        let mut outputs = Vec::new();
        // (the group's cell, what rows group by, what groups order by)
        let group = match &aggregate.group_by {
            Some(column) => {
                let sql = column_sql(table, alias, column)?;
                let ty = table.column(column).expect("checked").ty.clone();
                projection.push((column.clone(), ty));
                outputs.push(sql.clone());
                Some(match self.column_term(table, alias, column)? {
                    // Cells of one logical value group together even when
                    // their chunk trees differ.
                    Term::Large(large) => {
                        let columns = vec![(large.table.clone(), large.column.clone())];
                        let question = format!("class {columns:?}");
                        let slot = self.prep(|slot| Prep::Class { slot, columns }, question);
                        let by = format!(
                            "(CASE WHEN COALESCE({}, 0) THEN {} ELSE {sql} END)",
                            crate::large::chunked_sql(&sql),
                            scratch_value(slot, &sql)
                        );
                        (sql.clone(), by, crate::large::key_sql(&sql))
                    }
                    _ => (sql.clone(), sql.clone(), sql),
                })
            }
            None => None,
        };
        for output in &aggregate.aggregates {
            let input = |column: &Option<String>| -> Result<(String, ValueType), Error> {
                let column = column
                    .as_deref()
                    .ok_or_else(|| query_error(format!("`{}` needs a column", output.alias)))?;
                let ty = table
                    .column(column)
                    .ok_or_else(|| {
                        query_error(format!("unknown column `{}.{column}`", table.name))
                    })?
                    .ty
                    .clone();
                let ty = match ty {
                    ValueType::Nullable(inner) => *inner,
                    ty => ty,
                };
                Ok((column_sql(table, alias, column)?, ty))
            };
            let (sql, ty) = match output.function {
                F::Count => match &output.column {
                    None => ("COUNT(*)".to_owned(), ValueType::U64),
                    Some(_) => (
                        format!("COUNT({})", input(&output.column)?.0),
                        ValueType::U64,
                    ),
                },
                F::Avg => (
                    format!("AVG({})", input(&output.column)?.0),
                    ValueType::Nullable(Box::new(ValueType::F64)),
                ),
                F::Sum | F::Min | F::Max => {
                    let (column, ty) = input(&output.column)?;
                    let name = match output.function {
                        F::Sum => "SUM",
                        F::Min => "MIN",
                        _ => "MAX",
                    };
                    (
                        format!("{name}({column})"),
                        ValueType::Nullable(Box::new(ty)),
                    )
                }
            };
            outputs.push(format!("{sql} AS {}", quote(&output.alias)));
            projection.push((output.alias.clone(), ty));
        }
        let mut keys = Vec::new();
        for key in &query.order_by {
            let direction = match key.direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            };
            let term = if aggregate.group_by.as_deref() == Some(key.column.as_str()) {
                group.clone().expect("grouped").2
            } else if aggregate.aggregates.iter().any(|a| a.alias == key.column) {
                quote(&key.column)
            } else {
                return Err(query_error(format!(
                    "aggregate queries order by a group column or an aggregate alias, not `{}`",
                    key.column
                )));
            };
            keys.push(format!("{term} {direction}"));
        }
        let (key, group_by) = match &group {
            Some((sql, by, order)) => {
                keys.push(format!("{order} ASC"));
                (sql.clone(), format!(" GROUP BY {by}"))
            }
            None => ("NULL".to_owned(), String::new()),
        };
        let order = if keys.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", keys.join(", "))
        };
        let limit = self.bind(Sql::Integer(query.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(query.offset as i64));
        let sql = format!(
            "SELECT {key}, 0, 0, {} FROM {} AS {alias} WHERE {}{group_by}{order} \
             LIMIT {limit} OFFSET {offset}",
            outputs.join(", "),
            self.source(table)?,
            and(wheres),
        );
        Ok(Compiled {
            sql,
            ranked_sql: None,
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: true,
            preps: self.preps,
        })
    }

    pub fn compile_query(
        mut self,
        query: &Query,
        include_deleted: bool,
    ) -> Result<Compiled, Error> {
        if query.flat_join.is_some()
            || !query.policy_branches.is_empty()
            || !query.inherits.is_empty()
            || query.relation.is_some()
        {
            return Err(unsupported(
                "this query form (flat join, inherits, relation) on SQLite",
            ));
        }
        let table = self.table(&query.table)?;
        let projection = projection(table, query.select.as_deref())?;
        let alias = "t";
        let mut wheres = Vec::new();
        if !include_deleted {
            wheres.push(format!("{alias}._del = 0"));
        }
        for filter in &query.filters {
            wheres.push(self.predicate(filter, table, alias)?);
        }
        for reachable in &query.reachable {
            wheres.push(self.reachable(reachable, table, alias)?);
        }
        for join in &query.joins {
            if !join.nested_joins.is_empty() || !join.correlated_filters.is_empty() {
                return Err(unsupported("nested or correlated join filters on SQLite"));
            }
            let target = self.table(&join.table)?;
            let j = self.alias("j");
            let mut conds = vec![format!("{j}._del = 0")];
            match join.target {
                JoinTarget::Uncorrelated => {}
                JoinTarget::Column | JoinTarget::RowId => {
                    let source = join.source_column.as_deref().unwrap_or("id");
                    conds.push(format!(
                        "{} = {}",
                        column_sql(target, &j, &join.on_column)?,
                        column_sql(table, alias, source)?
                    ));
                }
            }
            for filter in &join.filters {
                conds.push(self.predicate(filter, target, &j)?);
            }
            wheres.push(format!(
                "EXISTS (SELECT 1 FROM {} AS {j} WHERE {})",
                self.source(target)?,
                conds.join(" AND ")
            ));
        }
        for include in &query.includes {
            let column = include.path.split('.').next().unwrap_or_default();
            let target_name = table
                .references
                .get(column)
                .ok_or_else(|| query_error(format!("`{}` is not a reference", include.path)))?;
            let target = self.table(target_name)?;
            if include.path.contains('.') {
                return Err(unsupported("multi-hop include paths on SQLite"));
            }
            if include.require {
                let i = self.alias("i");
                wheres.push(format!(
                    "EXISTS (SELECT 1 FROM {} AS {i} WHERE {i}.id = {alias}.{} AND {i}._del = 0)",
                    self.source(target)?,
                    quote(column)
                ));
            }
        }
        for array in &query.array_subqueries {
            self.validate_array(array)?;
            match array.requirement {
                ArraySubqueryRequirement::Optional => {}
                ArraySubqueryRequirement::AtLeastOne => {
                    let child = self.table(&array.table)?;
                    let c = self.alias("c");
                    let mut conds = vec![
                        format!("{c}._del = 0"),
                        format!(
                            "{} = {}",
                            column_sql(child, &c, &array.inner_column)?,
                            column_sql(table, alias, &array.outer_column)?
                        ),
                    ];
                    for filter in &array.filters {
                        conds.push(self.predicate(filter, child, &c)?);
                    }
                    wheres.push(format!(
                        "EXISTS (SELECT 1 FROM {} AS {c} WHERE {})",
                        self.source(child)?,
                        conds.join(" AND ")
                    ));
                }
                ArraySubqueryRequirement::MatchCorrelationCardinality => {
                    return Err(unsupported(
                        "cardinality-matching array requirements on SQLite",
                    ));
                }
            }
        }
        if let Some(session) = self.policy {
            wheres.push(self.read_policy(table, alias, session)?);
        }
        if let Some(aggregate) = &query.aggregate {
            if !query.array_subqueries.is_empty() || query.select.is_some() {
                return Err(unsupported(
                    "aggregates with selections or array subqueries on SQLite",
                ));
            }
            return self.compile_aggregate(query, aggregate, table, alias, &wheres);
        }
        let (order, ranked_order) = self.order_sql(table, alias, &query.order_by)?;
        let columns = projection
            .iter()
            .map(|(name, _)| Ok(format!(", {}", column_sql(table, alias, name)?)))
            .collect::<Result<String, Error>>()?;
        let limit = self.bind(Sql::Integer(query.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(query.offset as i64));
        let source = self.source(table)?;
        let statement = |order: &str| {
            format!(
                "SELECT {alias}.id, {alias}._seq, {alias}._del{columns} FROM {source} AS {alias} \
                 WHERE {} ORDER BY {order} LIMIT {limit} OFFSET {offset}",
                and(&wheres),
            )
        };
        Ok(Compiled {
            sql: statement(&order),
            ranked_sql: ranked_order.as_deref().map(statement),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            preps: self.preps,
        })
    }

    fn validate_array(&mut self, array: &ArraySubquery) -> Result<(), Error> {
        let child = self.table(&array.table)?;
        column_sql(child, "c", &array.inner_column)?;
        projection(child, array.select.as_deref())?;
        for nested in &array.nested_arrays {
            self.validate_array(nested)?;
        }
        Ok(())
    }

    /// One array relation for one parent row.
    pub fn compile_children(
        mut self,
        array: &ArraySubquery,
        parent: &Value,
    ) -> Result<Compiled, Error> {
        let child = self.table(&array.table)?;
        let projection = projection(child, array.select.as_deref())?;
        let alias = "t";
        let parent = match self.value_term(parent)? {
            Term::Bound(sql, _) => sql,
            _ => "NULL".to_owned(),
        };
        let mut wheres = vec![
            format!("{alias}._del = 0"),
            format!(
                "{} = {parent}",
                column_sql(child, alias, &array.inner_column)?
            ),
        ];
        for filter in &array.filters {
            wheres.push(self.predicate(filter, child, alias)?);
        }
        if let Some(session) = self.policy {
            wheres.push(self.read_policy(child, alias, session)?);
        }
        let (order, ranked_order) = self.order_sql(child, alias, &array.order_by)?;
        let columns = projection
            .iter()
            .map(|(name, _)| Ok(format!(", {}", column_sql(child, alias, name)?)))
            .collect::<Result<String, Error>>()?;
        let limit = self.bind(Sql::Integer(array.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(array.offset as i64));
        let source = self.source(child)?;
        let statement = |order: &str| {
            format!(
                "SELECT {alias}.id, {alias}._seq, {alias}._del{columns} FROM {source} AS {alias} \
                 WHERE {} ORDER BY {order} LIMIT {limit} OFFSET {offset}",
                and(&wheres),
            )
        };
        Ok(Compiled {
            sql: statement(&order),
            ranked_sql: ranked_order.as_deref().map(statement),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            preps: self.preps,
        })
    }

    /// Root rows an access edge grants to a team reachable from the seed.
    /// The closure is `WITH RECURSIVE` over effective edge rows: `UNION`
    /// dedupes, so a fixpoint terminates on cycles; a depth bound carries
    /// the depth and stops expanding at it.
    fn reachable(
        &mut self,
        via: &crate::query::ReachableVia,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        if via.seed.is_some() || via.access_team_target != JoinTarget::Column {
            return Err(unsupported(
                "relation-seeded or row-id reachability on SQLite",
            ));
        }
        let access = self.table(&via.access_table)?;
        let edges = self.table(&via.edge_table)?;
        let seed = match self.operand(&via.from, table, alias)? {
            Term::Missing | Term::NaN => return Ok("0".to_owned()),
            term => term.sql().expect("present").to_owned(),
        };
        let r = self.alias("reach");
        let e = self.alias("e");
        let mut step = vec![format!("{e}._del = 0")];
        for filter in &via.edge_filters {
            step.push(self.predicate(filter, edges, &e)?);
        }
        let member = column_sql(edges, &e, &via.edge_member_column)?;
        let parent = column_sql(edges, &e, &via.edge_parent_column)?;
        let edge_source = self.source(edges)?;
        let cte = match via.bound {
            crate::query::RecursionBound::Fixpoint => format!(
                "{r}(team) AS (SELECT {seed} UNION SELECT {parent} FROM {edge_source} AS {e} \
                 JOIN {r} ON {member} = {r}.team WHERE {})",
                and(&step)
            ),
            crate::query::RecursionBound::MaxDepth(depth) => {
                step.push(format!("{r}.d < {depth}"));
                format!(
                    "{r}(team, d) AS (SELECT {seed}, 0 UNION SELECT {parent}, {r}.d + 1 \
                     FROM {edge_source} AS {e} JOIN {r} ON {member} = {r}.team WHERE {})",
                    and(&step)
                )
            }
        };
        let a = self.alias("a");
        let mut conds = vec![
            format!("{a}._del = 0"),
            format!(
                "{} = {alias}.id",
                column_sql(access, &a, &via.access_row_column)?
            ),
            format!(
                "{} IN (SELECT team FROM {r})",
                column_sql(access, &a, &via.access_team_column)?
            ),
        ];
        for filter in &via.access_filters {
            conds.push(self.predicate(filter, access, &a)?);
        }
        Ok(format!(
            "EXISTS (WITH RECURSIVE {cte} SELECT 1 FROM {} AS {a} WHERE {})",
            self.source(access)?,
            conds.join(" AND ")
        ))
    }

    // ---- query predicates ----------------------------------------------

    fn operand(&mut self, operand: &Operand, table: &Table, alias: &str) -> Result<Term, Error> {
        match operand {
            Operand::Column(column) => self.column_term(table, alias, column),
            Operand::Param(name) => {
                let value = self
                    .binding
                    .get(name)
                    .ok_or_else(|| query_error(format!("parameter `{name}` is not bound")))?
                    .clone();
                self.value_term(&value)
            }
            Operand::Claim(key) => {
                let value = self.claims.claim(key);
                self.present(value)
            }
            Operand::Literal(value) => self.value_term(value),
        }
    }

    pub fn predicate(
        &mut self,
        predicate: &Predicate,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        Ok(match predicate {
            Predicate::All(children) => {
                let parts = children
                    .iter()
                    .map(|child| self.predicate(child, table, alias))
                    .collect::<Result<Vec<_>, _>>()?;
                and(&parts)
            }
            Predicate::Any(children) => {
                let parts = children
                    .iter()
                    .map(|child| self.predicate(child, table, alias))
                    .collect::<Result<Vec<_>, _>>()?;
                or(&parts)
            }
            Predicate::Not(child) => format!(
                "(NOT COALESCE({}, 0))",
                self.predicate(child, table, alias)?
            ),
            Predicate::Eq(l, r) => self.cmp2(CmpOp::Eq, l, r, table, alias)?,
            Predicate::Ne(l, r) => self.cmp2(CmpOp::Ne, l, r, table, alias)?,
            Predicate::Lt(l, r) => self.cmp2(CmpOp::Lt, l, r, table, alias)?,
            Predicate::Lte(l, r) => self.cmp2(CmpOp::Le, l, r, table, alias)?,
            Predicate::Gt(l, r) => self.cmp2(CmpOp::Gt, l, r, table, alias)?,
            Predicate::Gte(l, r) => self.cmp2(CmpOp::Ge, l, r, table, alias)?,
            Predicate::In(left, options) => {
                let left = self.operand(left, table, alias)?;
                let options = options
                    .iter()
                    .map(|option| self.operand(option, table, alias))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            Predicate::Contains(l, r) => {
                let l = self.operand(l, table, alias)?;
                let r = self.operand(r, table, alias)?;
                self.contains(l, r)
            }
            Predicate::IsNull(operand) => match self.operand(operand, table, alias)? {
                Term::Missing => "1".to_owned(),
                Term::NaN => "0".to_owned(),
                term => format!("({} IS NULL)", term.sql().expect("present")),
            },
            Predicate::EnumMatch { .. } => {
                return Err(unsupported("enum payload predicates on SQLite"));
            }
        })
    }

    fn cmp2(
        &mut self,
        op: CmpOp,
        left: &Operand,
        right: &Operand,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        let left = self.operand(left, table, alias)?;
        let right = self.operand(right, table, alias)?;
        self.cmp(op, left, right)
    }

    // ---- policies -------------------------------------------------------

    /// The SELECT policy of `table` at `alias`, as `session`.
    pub fn read_policy(
        &mut self,
        table: &Table,
        alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        self.operation_policy(table, Operation::Select, alias, session)
    }

    pub fn operation_policy(
        &mut self,
        table: &Table,
        operation: Operation,
        alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        if session.system {
            return Ok("1".to_owned());
        }
        match table.policy(operation) {
            None => Ok("0".to_owned()),
            Some(policy) => {
                self.policy_expr(policy, table, alias, None, session, DEFAULT_INHERITS_DEPTH)
            }
        }
    }

    /// A specific policy clause (e.g. UPDATE's WITH CHECK) at `alias`.
    pub fn policy_clause(
        &mut self,
        policy: Option<&PolicyExpr>,
        table: &Table,
        alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        if session.system {
            return Ok("1".to_owned());
        }
        match policy {
            None => Ok("0".to_owned()),
            Some(policy) => {
                self.policy_expr(policy, table, alias, None, session, DEFAULT_INHERITS_DEPTH)
            }
        }
    }

    fn policy_value(
        &mut self,
        value: &PolicyValue,
        outer: Option<(&Table, &str)>,
        session: &Session,
    ) -> Result<Term, Error> {
        match value {
            PolicyValue::Literal(value) => self.value_term(&public_value(value)?),
            PolicyValue::SessionRef(path)
                if path.first().map(String::as_str) == Some("__jazz_outer_row") =>
            {
                let (table, alias) =
                    outer.ok_or_else(|| query_error("outer row reference outside EXISTS"))?;
                let column = path
                    .get(1)
                    .ok_or_else(|| query_error("outer row reference without a column"))?;
                self.column_term(table, alias, column)
            }
            PolicyValue::SessionRef(path) => {
                let value = session.resolve(path);
                self.present(value)
            }
        }
    }

    fn policy_expr(
        &mut self,
        policy: &PolicyExpr,
        table: &Table,
        alias: &str,
        outer: Option<(&Table, &str)>,
        session: &Session,
        depth: usize,
    ) -> Result<String, Error> {
        let recurse = |ctx: &mut Self, policy: &PolicyExpr| {
            ctx.policy_expr(policy, table, alias, outer, session, depth)
        };
        Ok(match policy {
            PolicyExpr::True => "1".to_owned(),
            PolicyExpr::False => "0".to_owned(),
            PolicyExpr::And(parts) => {
                let parts = parts
                    .iter()
                    .map(|p| recurse(self, p))
                    .collect::<Result<Vec<_>, _>>()?;
                and(&parts)
            }
            PolicyExpr::Or(parts) => {
                let parts = parts
                    .iter()
                    .map(|p| recurse(self, p))
                    .collect::<Result<Vec<_>, _>>()?;
                or(&parts)
            }
            PolicyExpr::Not(inner) => format!("(NOT COALESCE({}, 0))", recurse(self, inner)?),
            PolicyExpr::Cmp { column, op, value } => {
                let left = self.column_term(table, alias, column)?;
                let right = self.policy_value(value, outer, session)?;
                self.cmp(op.clone(), left, right)?
            }
            PolicyExpr::SessionCmp { path, op, value } => {
                let left = session.resolve(path);
                let left = self.present(left)?;
                let right = self.value_term(&public_value(value)?)?;
                self.cmp(op.clone(), left, right)?
            }
            PolicyExpr::IsNull { column } => {
                format!("({} IS NULL)", column_sql(table, alias, column)?)
            }
            PolicyExpr::IsNotNull { column } => {
                format!("({} IS NOT NULL)", column_sql(table, alias, column)?)
            }
            PolicyExpr::SessionIsNull { path } => bool_sql(session.resolve(path).is_none()),
            PolicyExpr::SessionIsNotNull { path } => bool_sql(session.resolve(path).is_some()),
            PolicyExpr::Contains { column, value } => {
                let left = self.column_term(table, alias, column)?;
                let right = self.policy_value(value, outer, session)?;
                self.contains(left, right)
            }
            PolicyExpr::In {
                column,
                session_path,
            } => {
                let left = self.column_term(table, alias, column)?;
                let options = match session.resolve(session_path) {
                    Some(Value::Array(values)) => values
                        .iter()
                        .map(|value| self.value_term(value))
                        .collect::<Result<Vec<_>, _>>()?,
                    Some(value) => vec![self.value_term(&value)?],
                    None => Vec::new(),
                };
                self.in_list(left, options)?
            }
            PolicyExpr::InList { column, values } => {
                let left = self.column_term(table, alias, column)?;
                let options = values
                    .iter()
                    .map(|value| self.policy_value(value, outer, session))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            PolicyExpr::SessionInList { path, values } => {
                let left = session.resolve(path);
                let left = self.present(left)?;
                let options = values
                    .iter()
                    .map(|value| self.value_term(&public_value(value)?))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            PolicyExpr::SessionContains { .. } => {
                return Err(unsupported("session-side CONTAINS policies on SQLite"));
            }
            // Policy EXISTS reads its table as raw evidence: the dependency
            // table's own policy is not composed in (INV-RLS-21).
            PolicyExpr::Exists {
                table: target,
                condition,
            } => {
                let target = self.table(target)?;
                let e = self.alias("e");
                let inner =
                    self.policy_expr(condition, target, &e, Some((table, alias)), session, depth)?;
                format!(
                    "EXISTS (SELECT 1 FROM {} AS {e} WHERE {e}._del = 0 AND {inner})",
                    self.source(target)?
                )
            }
            PolicyExpr::ExistsRel { rel } => self.exists_rel(rel, table, alias, session)?,
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok("0".to_owned());
                }
                let target_name = table.references.get(via_column).ok_or_else(|| {
                    query_error(format!(
                        "INHERITS via `{via_column}`, which is not a reference"
                    ))
                })?;
                let target = self.table(target_name)?;
                let p = self.alias("p");
                let inner = match target.policy(*operation) {
                    None => "0".to_owned(),
                    Some(policy) => {
                        self.policy_expr(policy, target, &p, None, session, depth - 1)?
                    }
                };
                format!(
                    "EXISTS (SELECT 1 FROM {} AS {p} WHERE {p}.id = {alias}.{} AND {p}._del = 0 AND {inner})",
                    self.source(target)?,
                    quote(via_column)
                )
            }
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok("0".to_owned());
                }
                let source = self.table(source_table)?;
                let s = self.alias("s");
                let inner = match source.policy(*operation) {
                    None => "0".to_owned(),
                    Some(policy) => {
                        self.policy_expr(policy, source, &s, None, session, depth - 1)?
                    }
                };
                format!(
                    "EXISTS (SELECT 1 FROM {} AS {s} WHERE {} = {alias}.id AND {s}._del = 0 AND {inner})",
                    self.source(source)?,
                    column_sql(source, &s, via_column)?
                )
            }
        })
    }

    // ---- relation IR ----------------------------------------------------

    fn exists_rel(
        &mut self,
        rel: &RelationExpr,
        outer: &Table,
        outer_alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        let scope = RelScope {
            outer: Some((outer, outer_alias)),
            frontier: None,
            session,
        };
        let rel = self.relation(rel, &scope)?;
        let with = if rel.ctes.is_empty() {
            String::new()
        } else {
            format!("WITH RECURSIVE {} ", rel.ctes.join(", "))
        };
        Ok(format!(
            "EXISTS ({with}SELECT 1 FROM {} WHERE {})",
            rel.sql_from_clause(),
            and(&rel.wheres)
        ))
    }

    fn relation(&mut self, expr: &RelationExpr, scope: &RelScope<'_>) -> Result<Rel, Error> {
        match expr {
            RelationExpr::TableScan { table, alias } => {
                let table = self.table(table)?;
                let r = self.alias("r");
                Ok(Rel {
                    ctes: Vec::new(),
                    wheres: vec![format!("{r}._del = 0")],
                    from: vec![Src {
                        sql: self.source(table)?,
                        alias: r,
                        scope: alias.clone().unwrap_or_else(|| table.name.clone()),
                        table: Some(table.name.clone()),
                    }],
                    id: None,
                })
            }
            RelationExpr::Filter { input, predicate } => {
                let mut rel = self.relation(input, scope)?;
                let condition = self.rel_predicate(predicate, &rel, scope)?;
                rel.wheres.push(condition);
                Ok(rel)
            }
            RelationExpr::Join {
                left,
                right,
                on,
                join_kind,
            } => {
                if format!("{join_kind:?}") != "Inner" {
                    return Err(unsupported("outer relation joins on SQLite"));
                }
                let mut left = self.relation(left, scope)?;
                let right = self.relation(right, scope)?;
                let mut conditions = Vec::new();
                for condition in on {
                    conditions.push(format!(
                        "{} = {}",
                        self.resolve(&left, &condition.left)?,
                        self.resolve(&right, &condition.right)?
                    ));
                }
                left.ctes.extend(right.ctes);
                left.from.extend(right.from);
                left.wheres.extend(right.wheres);
                left.wheres.extend(conditions);
                left.id = None;
                Ok(left)
            }
            RelationExpr::Project { input, columns } => {
                let mut rel = self.relation(input, scope)?;
                if let Some(column) = columns.iter().find(|column| column.alias == "id") {
                    rel.id = Some(match &column.expr {
                        RelationProjectExpr::Column(column) => self.resolve(&rel, column)?,
                        RelationProjectExpr::RowId(row) => self.row_id(&rel, *row, scope)?,
                    });
                }
                Ok(rel)
            }
            RelationExpr::Gather {
                seed,
                step,
                frontier_key,
                bound,
                dedupe_key,
            } => {
                let current = RelationKeyRef::RowId(RelationRowIdRef::Current);
                if *frontier_key != current || dedupe_key.iter().any(|key| *key != current) {
                    return Err(unsupported(
                        "gather frontiers other than the row id on SQLite",
                    ));
                }
                let g = self.alias("g");
                let seed = self.relation(seed, scope)?;
                let step_scope = RelScope {
                    frontier: Some(format!("{g}.id")),
                    ..*scope
                };
                let step = self.relation(step, &step_scope)?;
                let (max, guard) = match bound {
                    RecursionBound::MaxDepth(depth) => (*depth, false),
                    RecursionBound::Fixpoint => (FIXPOINT_GUARD, true),
                };
                let _ = guard;
                let cte = format!(
                    "{g}(id, d) AS (SELECT {}, 0 FROM {} WHERE {} \
                     UNION SELECT {}, {g}.d + 1 FROM {g}, {} WHERE {g}.d < {max} AND {})",
                    seed.output_id(),
                    seed.sql_from_clause(),
                    and(&seed.wheres),
                    step.output_id(),
                    step.sql_from_clause(),
                    and(&step.wheres),
                );
                let mut ctes = seed.ctes;
                ctes.extend(step.ctes);
                ctes.push(cte);
                let a = self.alias("ga");
                Ok(Rel {
                    ctes,
                    wheres: Vec::new(),
                    from: vec![Src {
                        sql: g,
                        alias: a,
                        scope: String::new(),
                        table: None,
                    }],
                    id: None,
                })
            }
            other => Err(unsupported(format!(
                "relation `{}` on SQLite",
                format!("{other:?}")
                    .split([' ', '{', '('])
                    .next()
                    .unwrap_or("?")
            ))),
        }
    }

    fn resolve(&self, rel: &Rel, column: &RelationColumnRef) -> Result<String, Error> {
        let has = |src: &Src| match &src.table {
            Some(table) => self.schema.tables[table].has_column(&column.column),
            None => column.column == "id",
        };
        let src = match &column.scope {
            Some(scope) => rel
                .from
                .iter()
                .find(|src| &src.scope == scope)
                .or_else(|| rel.from.iter().find(|src| src.table.is_none() && has(src))),
            None => rel.from.iter().find(|src| has(src)),
        }
        .ok_or_else(|| query_error(format!("relation column {column:?} does not resolve")))?;
        if !has(src) {
            return Err(query_error(format!(
                "relation column {column:?} does not exist"
            )));
        }
        Ok(match &src.table {
            Some(table) => column_sql(&self.schema.tables[table], &src.alias, &column.column)?,
            None if column.column == "id" => format!("{}.id", src.alias),
            None => format!("{}.{}", src.alias, quote(&column.column)),
        })
    }

    /// [`Self::resolve`] as an operand.
    fn resolve_term(&self, rel: &Rel, column: &RelationColumnRef) -> Result<Term, Error> {
        let sql = self.resolve(rel, column)?;
        let src = rel.from.iter().find(|src| {
            src.table.as_ref().is_some_and(|table| {
                sql.starts_with(&format!("{}.", src.alias))
                    && self.schema.tables[table].has_column(&column.column)
            })
        });
        Ok(match src.and_then(|src| src.table.as_ref()) {
            Some(table) => self.large_term(&self.schema.tables[table], sql, &column.column),
            None => Term::Sql(sql),
        })
    }

    fn row_id(
        &self,
        rel: &Rel,
        row: RelationRowIdRef,
        scope: &RelScope<'_>,
    ) -> Result<String, Error> {
        Ok(match row {
            RelationRowIdRef::Current => rel
                .from
                .first()
                .map(|src| format!("{}.id", src.alias))
                .ok_or_else(|| query_error("current row id of an empty relation"))?,
            RelationRowIdRef::Outer => scope
                .outer
                .map(|(_, alias)| format!("{alias}.id"))
                .ok_or_else(|| query_error("outer row id outside a policy"))?,
            RelationRowIdRef::Frontier => scope
                .frontier
                .clone()
                .ok_or_else(|| query_error("frontier row id outside a gather step"))?,
        })
    }

    fn rel_value(
        &mut self,
        value: &RelationValueRef,
        rel: &Rel,
        scope: &RelScope<'_>,
    ) -> Result<Term, Error> {
        match value {
            RelationValueRef::Literal(json) => self.value_term(&json_value(json)?),
            RelationValueRef::Param(name) => {
                let value = self
                    .binding
                    .get(name)
                    .ok_or_else(|| query_error(format!("parameter `{name}` is not bound")))?
                    .clone();
                self.value_term(&value)
            }
            RelationValueRef::SessionRef(path) => {
                let value = scope.session.resolve(path);
                self.present(value)
            }
            RelationValueRef::OuterColumn(column) => {
                let (table, alias) = scope
                    .outer
                    .ok_or_else(|| query_error("outer column outside a policy"))?;
                self.column_term(table, alias, &column.column)
            }
            RelationValueRef::FrontierColumn(_) => Err(unsupported("frontier columns on SQLite")),
            RelationValueRef::RowId(row) => Ok(Term::Sql(self.row_id(rel, *row, scope)?)),
        }
    }

    fn rel_predicate(
        &mut self,
        predicate: &RelationPredicate,
        rel: &Rel,
        scope: &RelScope<'_>,
    ) -> Result<String, Error> {
        Ok(match predicate {
            RelationPredicate::True => "1".to_owned(),
            RelationPredicate::False => "0".to_owned(),
            RelationPredicate::And(parts) => {
                let parts = parts
                    .iter()
                    .map(|part| self.rel_predicate(part, rel, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                and(&parts)
            }
            RelationPredicate::Or(parts) => {
                let parts = parts
                    .iter()
                    .map(|part| self.rel_predicate(part, rel, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                or(&parts)
            }
            RelationPredicate::Not(part) => format!(
                "(NOT COALESCE({}, 0))",
                self.rel_predicate(part, rel, scope)?
            ),
            RelationPredicate::Cmp { left, op, right } => {
                let left = self.resolve_term(rel, left)?;
                let right = self.rel_value(right, rel, scope)?;
                let op = match op {
                    RelationCmpOp::Eq => CmpOp::Eq,
                    RelationCmpOp::Ne => CmpOp::Ne,
                    RelationCmpOp::Lt => CmpOp::Lt,
                    RelationCmpOp::Le => CmpOp::Le,
                    RelationCmpOp::Gt => CmpOp::Gt,
                    RelationCmpOp::Ge => CmpOp::Ge,
                };
                self.cmp(op, left, right)?
            }
            RelationPredicate::IsNull { column } => {
                format!("({} IS NULL)", self.resolve(rel, column)?)
            }
            RelationPredicate::IsNotNull { column } => {
                format!("({} IS NOT NULL)", self.resolve(rel, column)?)
            }
            RelationPredicate::In { left, values } => {
                let left = self.resolve_term(rel, left)?;
                let options = values
                    .iter()
                    .map(|value| self.rel_value(value, rel, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            RelationPredicate::Contains { left, right } => {
                let left = self.resolve_term(rel, left)?;
                let right = self.rel_value(right, rel, scope)?;
                self.contains(left, right)
            }
            RelationPredicate::EnumMatch { .. } => {
                return Err(unsupported("enum payload relation predicates on SQLite"));
            }
        })
    }
}

struct RelScope<'s> {
    outer: Option<(&'s Table, &'s str)>,
    frontier: Option<String>,
    session: &'s Session,
}

impl Clone for RelScope<'_> {
    fn clone(&self) -> Self {
        Self {
            outer: self.outer,
            frontier: self.frontier.clone(),
            session: self.session,
        }
    }
}

struct Src {
    sql: String,
    alias: String,
    scope: String,
    /// `None` for a gather CTE, whose only column is `id`.
    table: Option<String>,
}

struct Rel {
    ctes: Vec<String>,
    from: Vec<Src>,
    wheres: Vec<String>,
    /// The projected `id`, when a projection named one.
    id: Option<String>,
}

impl Rel {
    fn sql_from_clause(&self) -> String {
        self.from
            .iter()
            .map(|src| format!("{} AS {}", src.sql, src.alias))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn output_id(&self) -> String {
        self.id
            .clone()
            .unwrap_or_else(|| format!("{}.id", self.from[0].alias))
    }
}

fn projection(table: &Table, select: Option<&[String]>) -> Result<Vec<(String, ValueType)>, Error> {
    match select {
        None => Ok(table
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.ty.clone()))
            .collect()),
        Some(select) => select
            .iter()
            .filter(|name| name.as_str() != "id")
            .map(|name| {
                table
                    .column(name)
                    .map(|column| (column.name.clone(), column.ty.clone()))
                    .ok_or_else(|| query_error(format!("unknown column `{}.{name}`", table.name)))
            })
            .collect(),
    }
}

pub(crate) fn column_sql(table: &Table, alias: &str, column: &str) -> Result<String, Error> {
    if column == "id" {
        return Ok(format!("{alias}.id"));
    }
    if let Some((stored, part)) = crate::values::author_part(column) {
        return Ok(format!(
            "(SELECT {part} FROM _jazz_authors WHERE n = {})",
            stored_sql(alias, stored)
        ));
    }
    if crate::values::elided_as(column).is_some() {
        return Ok(stored_sql(alias, column));
    }
    if table.column(column).is_none() {
        if column.starts_with("$createdBy") || column.starts_with("$updatedBy") {
            return Err(unsupported(format!(
                "`{column}` (author provenance is readable as `.identity.issuer` and `.identity.subject`)"
            )));
        }
        return Err(query_error(format!(
            "unknown column `{}.{column}`",
            table.name
        )));
    }
    Ok(format!("{alias}.{}", quote(column)))
}

/// A stored provenance column; an update stamp left NULL reads as the
/// create stamp it equals.
fn stored_sql(alias: &str, stored: &str) -> String {
    match crate::values::elided_as(stored) {
        Some(created) => format!(
            "COALESCE({alias}.{}, {alias}.{})",
            quote(stored),
            quote(created)
        ),
        None => format!("{alias}.{}", quote(stored)),
    }
}

fn and(parts: &[String]) -> String {
    if parts.is_empty() {
        "1".to_owned()
    } else {
        format!("({})", parts.join(" AND "))
    }
}

fn or(parts: &[String]) -> String {
    if parts.is_empty() {
        "0".to_owned()
    } else {
        format!("({})", parts.join(" OR "))
    }
}

fn bool_sql(value: bool) -> String {
    if value { "1" } else { "0" }.to_owned()
}

/// Jazz comparison semantics: equality treats null as a value (null = null),
/// orderings are false against null, and everything is false against NaN.
/// Orderings and `IN` stay bare so SQLite can use an index for them: their
/// SQL null means false, which AND, OR and WHERE already treat as false;
/// only NOT (and a condition read as a value) coalesces it.
fn cmp_plain(op: &CmpOp, l: &str, r: &str) -> String {
    match op {
        CmpOp::Eq => format!("({l} IS {r})"),
        CmpOp::Ne => format!("({l} IS NOT {r})"),
        CmpOp::Lt => format!("({l} < {r})"),
        CmpOp::Le => format!("({l} <= {r})"),
        CmpOp::Gt => format!("({l} > {r})"),
        CmpOp::Ge => format!("({l} >= {r})"),
    }
}

/// `a op b` as `b flip(op) a`.
fn flip(op: &CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        op => op.clone(),
    }
}

/// The logical bytes of an inline-form cell.
fn logical_of(cell: &[u8]) -> &[u8] {
    match cell.first() {
        Some(&crate::large::BYTES) => &cell[1..],
        _ => cell,
    }
}

impl Ctx<'_> {
    /// A column as an operand: a large-capable column compares by its key.
    fn column_term(&self, table: &Table, alias: &str, column: &str) -> Result<Term, Error> {
        let sql = column_sql(table, alias, column)?;
        Ok(self.large_term(table, sql, column))
    }

    fn large_term(&self, table: &Table, sql: String, column: &str) -> Term {
        match table.column(column) {
            Some(col)
                if column != "id"
                    && crate::values::elided_as(column).is_none()
                    && crate::values::author_part(column).is_none()
                    && crate::large::large_capable(col) =>
            {
                Term::Large(LargeColumn {
                    cell: sql,
                    table: table.name.clone(),
                    column: column.to_owned(),
                    kind: crate::large::kind(col),
                })
            }
            _ => Term::Sql(sql),
        }
    }

    fn prep(&mut self, make: impl FnOnce(i64) -> Prep, question: String) -> i64 {
        let prep = Prep::new(make, &question);
        let slot = prep.slot();
        if !self.preps.iter().any(|p| p.slot() == slot) {
            self.preps.push(prep);
        }
        slot
    }

    fn cmp(&mut self, op: CmpOp, left: Term, right: Term) -> Result<String, Error> {
        Ok(match (left, right) {
            (Term::Missing | Term::NaN, _) | (_, Term::Missing | Term::NaN) => "0".to_owned(),
            (Term::Large(column), Term::Bound(sql, value)) => {
                self.cmp_large(&op, &column, &sql, &value)
            }
            (Term::Bound(sql, value), Term::Large(column)) => {
                self.cmp_large(&flip(&op), &column, &sql, &value)
            }
            (Term::Large(a), Term::Large(b)) => self.cmp_large_pair(&op, &a, &b)?,
            (left, right) => cmp_plain(
                &op,
                left.sql().expect("present"),
                right.sql().expect("present"),
            ),
        })
    }

    /// A large-capable cell against a bound value.
    fn cmp_large(&mut self, op: &CmpOp, column: &LargeColumn, p: &str, value: &Sql) -> String {
        let c = &column.cell;
        let Sql::Text(literal) = value else {
            return cmp_plain(op, c, p);
        };
        let key = crate::large::key_sql(c);
        let logical = logical_of(literal);
        let chunked = format!("COALESCE({}, 0)", crate::large::chunked_sql(c));
        match op {
            // Every value up to the inline threshold has one inline cell, so
            // a chunked cell never equals such a literal. The key term lets
            // SQLite use an index on it.
            CmpOp::Eq | CmpOp::Ne
                if logical.len() <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES =>
            {
                let eq = format!("({key} IS {p} AND {c} IS {p})");
                if *op == CmpOp::Eq {
                    eq
                } else {
                    format!("(NOT {eq})")
                }
            }
            CmpOp::Eq | CmpOp::Ne => {
                let slot = self.prep_match(column, MatchTest::Cmp(CmpOp::Eq, literal.clone()));
                let prefix = crate::large::sort_prefix(column.kind, logical);
                let k = self.bind(Sql::Text(prefix));
                let eq = format!("({key} IS {k} AND {chunked} AND {})", scratch_has(slot, c));
                if *op == CmpOp::Eq {
                    eq
                } else {
                    format!("(NOT {eq})")
                }
            }
            // A chunked value's key is a prefix of it at least as long as
            // the literal: it orders against the literal as the value does,
            // except that a key equal to the literal is the longer value.
            _ if logical.len() <= crate::large::EXACT_RANGE_BYTES => match op {
                CmpOp::Lt => format!("({key} < {p})"),
                CmpOp::Ge => format!("({key} >= {p})"),
                CmpOp::Le => format!("({key} < {p} OR {c} = {p})"),
                CmpOp::Gt => format!("({key} >= {p} AND {c} IS NOT {p})"),
                CmpOp::Eq | CmpOp::Ne => unreachable!("handled above"),
            },
            _ => {
                let slot = self.prep_match(column, MatchTest::Cmp(op.clone(), literal.clone()));
                format!(
                    "(CASE WHEN {chunked} THEN {} ELSE {} END)",
                    scratch_has(slot, c),
                    cmp_plain(op, c, p)
                )
            }
        }
    }

    fn prep_match(&mut self, column: &LargeColumn, test: MatchTest) -> i64 {
        let question = format!("match {} {} {test:?}", column.table, column.column);
        let (table, name) = (column.table.clone(), column.column.clone());
        self.prep(
            |slot| Prep::Match {
                slot,
                table,
                column: name,
                test,
            },
            question,
        )
    }

    /// Two large-capable cells: equal when both inline and equal, or both
    /// chunked with one content id.
    fn cmp_large_pair(
        &mut self,
        op: &CmpOp,
        a: &LargeColumn,
        b: &LargeColumn,
    ) -> Result<String, Error> {
        if !matches!(op, CmpOp::Eq | CmpOp::Ne) {
            return Err(unsupported(format!(
                "ordering comparisons between two string or bytes columns (`{}.{}`, `{}.{}`)",
                a.table, a.column, b.table, b.column
            )));
        }
        let mut columns = vec![
            (a.table.clone(), a.column.clone()),
            (b.table.clone(), b.column.clone()),
        ];
        columns.sort();
        columns.dedup();
        let question = format!("class {columns:?}");
        let slot = self.prep(|slot| Prep::Class { slot, columns }, question);
        let eq = format!(
            "(CASE WHEN COALESCE({} OR {}, 0) THEN {} IS {} ELSE {} IS {} END)",
            crate::large::chunked_sql(&a.cell),
            crate::large::chunked_sql(&b.cell),
            scratch_value(slot, &a.cell),
            scratch_value(slot, &b.cell),
            a.cell,
            b.cell
        );
        Ok(if *op == CmpOp::Eq {
            eq
        } else {
            format!("(NOT {eq})")
        })
    }

    fn in_list(&mut self, left: Term, options: Vec<Term>) -> Result<String, Error> {
        let options: Vec<Term> = options
            .into_iter()
            .filter(|option| !matches!(option, Term::Missing | Term::NaN))
            .collect();
        if options.is_empty() || matches!(left, Term::Missing | Term::NaN) {
            return Ok("0".to_owned());
        }
        if let Term::Large(column) = &left {
            let small = |option: &Term| match option {
                Term::Bound(_, Sql::Text(cell)) => {
                    logical_of(cell).len() <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES
                }
                Term::Bound(_, _) => true,
                _ => false,
            };
            if !options.iter().all(small) {
                let parts = options
                    .into_iter()
                    .map(|option| self.cmp(CmpOp::Eq, Term::Large(column.clone()), option))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(or(&parts));
            }
            let list: Vec<&str> = options.iter().filter_map(Term::sql).collect();
            let list = list.join(", ");
            return Ok(format!(
                "({} IN ({list}) AND {} IN ({list}))",
                crate::large::key_sql(&column.cell),
                column.cell
            ));
        }
        let list: Vec<&str> = options.iter().filter_map(Term::sql).collect();
        Ok(format!(
            "({} IN ({}))",
            left.sql().expect("present"),
            list.join(", ")
        ))
    }

    fn contains(&mut self, left: Term, right: Term) -> String {
        let (Some(l), Some(r)) = (left.sql(), right.sql()) else {
            return "0".to_owned();
        };
        // Bytewise: TEXT `instr` steps by code point, and cells may hold
        // tagged bytes.
        let instr = |l: &str, r: &str| {
            format!("COALESCE(instr(CAST({l} AS BLOB), CAST({r} AS BLOB)) > 0, 0)")
        };
        let (Term::Large(column), Term::Bound(_, Sql::Text(needle))) = (&left, &right) else {
            return instr(l, r);
        };
        let inline = match column.kind {
            jazz_types::large_values::LargeValueKind::Bytes => instr(
                &format!("substr(CAST({l} AS BLOB), 2)"),
                &format!("substr(CAST({r} AS BLOB), 2)"),
            ),
            _ => instr(l, r),
        };
        let slot = self.prep_match(column, MatchTest::Contains(needle.clone()));
        format!(
            "(CASE WHEN COALESCE({}, 0) THEN {} ELSE {inline} END)",
            crate::large::chunked_sql(l),
            scratch_has(slot, l)
        )
    }

    /// `ORDER BY` keys, and the same keys with prefix-tie ranks (`None`
    /// when no key is large-capable). Ties break by row id in the last
    /// key's direction, so a declared index (which ends in `id`) supplies
    /// the whole order in one scan, in either direction, with no sort step.
    fn order_sql(
        &mut self,
        table: &Table,
        alias: &str,
        order: &[crate::query::OrderBy],
    ) -> Result<(String, Option<String>), Error> {
        let mut keys = Vec::new();
        let mut ranked = Vec::new();
        let mut any_large = false;
        for key in order {
            let direction = match key.direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            };
            match self.column_term(table, alias, &key.column)? {
                Term::Large(column) => {
                    any_large = true;
                    let sql = crate::large::key_sql(&column.cell);
                    keys.push(format!("{sql} {direction}"));
                    ranked.push(format!("{sql} {direction}"));
                    let question = format!("rank {} {}", column.table, column.column);
                    let slot = self.prep(
                        |slot| Prep::Rank {
                            slot,
                            table: column.table.clone(),
                            column: column.column.clone(),
                        },
                        question,
                    );
                    ranked.push(format!("{} {direction}", scratch_value(slot, &column.cell)));
                }
                term => {
                    let sql = format!("{} {direction}", term.sql().expect("column"));
                    keys.push(sql.clone());
                    ranked.push(sql);
                }
            }
        }
        let tie = match order.last().map(|key| key.direction) {
            Some(OrderDirection::Desc) => "DESC",
            _ => "ASC",
        };
        keys.push(format!("{alias}.id {tie}"));
        ranked.push(format!("{alias}.id {tie}"));
        Ok((keys.join(", "), any_large.then(|| ranked.join(", "))))
    }
}

/// A public schema value (policy literals, defaults) as a storage value.
pub(crate) fn public_value(value: &crate::tools::Value) -> Result<Value, Error> {
    use crate::tools::Value as P;
    Ok(match value {
        P::Integer(v) => Value::I32(*v),
        P::BigInt(v) => Value::I64(*v),
        P::Double(v) => Value::F64(*v),
        P::Boolean(v) => Value::Bool(*v),
        P::Text(v) => Value::String(v.clone()),
        P::Timestamp(v) => Value::U64(*v),
        P::Uuid(v) => Value::Uuid(*v.uuid()),
        P::Bytea(v) => Value::Bytes(v.clone()),
        P::Null => Value::Nullable(None),
        other => return Err(unsupported(format!("policy literal {other:?}"))),
    })
}

/// A relation-IR literal: a bare JSON value or a typed public-value envelope.
fn json_value(json: &serde_json::Value) -> Result<Value, Error> {
    use serde_json::Value as J;
    if let Ok(value) = serde_json::from_value::<crate::tools::Value>(json.clone()) {
        return public_value(&value);
    }
    Ok(match json {
        J::Null => Value::Nullable(None),
        J::Bool(v) => Value::Bool(*v),
        J::Number(n) if n.is_i64() => Value::I64(n.as_i64().unwrap_or_default()),
        J::Number(n) => Value::F64(n.as_f64().unwrap_or(f64::NAN)),
        J::String(s) => match uuid::Uuid::parse_str(s) {
            Ok(uuid) if s.len() == 36 => Value::Uuid(uuid),
            _ => Value::String(s.clone()),
        },
        other => return Err(unsupported(format!("relation literal {other}"))),
    })
}
