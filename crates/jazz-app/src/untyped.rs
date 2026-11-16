//! Queries by name rather than by type: what the typed [`crate::Query`]
//! builds on, and what bindings in other languages (through `jazz-ffi`)
//! build queries with.
//!
//! A [`Query`] reads rows of a [`Table`], named with its stored columns.
//! It filters, orders, windows, selects and includes as the core query
//! does, and lowers [`Query::hop_to`], [`Query::gather`] and
//! [`Query::union`] to the core's relation expressions as the TypeScript
//! client's query adapter lowers `hopTo`, `gather` and `union`. What
//! TypeScript refuses in its MVP is refused here too: [`Query::into_core`]
//! fails with [`Error::Unsupported`] naming the combination.
//!
//! History reads ([`Query::history`]) list a row's accepted versions. Each
//! carries the table's columns and two metadata fields:
//! `$transactionId` ([`transaction_id`] decodes it to the public
//! [`TransactionId`] a write reports) and `$acceptedAt` ([`accepted_at`]
//! decodes it to milliseconds since the Unix epoch).
//!
//! ```ignore
//! use jazz_app::untyped::{Hop, Query, Table};
//! let todos = Table::new("todos", ["title", "done", "project"]);
//! let projects = Table::new("projects", ["name"]);
//! let open_projects = Query::table(todos)
//!     .filter(q::eq(q::col("done"), q::lit(false)))
//!     .hop_to(Hop::Forward { column: "project".into(), to: projects })
//!     .into_core()?;
//! ```

use std::collections::BTreeSet;
use std::ops::Bound;

use jazz::query::{
    self as q, ArraySubquery, Operand, OrderDirection, Predicate, RecursionBound, RelationCmpOp,
    RelationColumnRef, RelationExpr, RelationJoinCondition, RelationJoinKind, RelationKeyRef,
    RelationOrderBy, RelationPredicate, RelationProjectColumn, RelationProjectExpr,
    RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
use jazz::records::Value as CoreValue;
use jazz::schema::history::{ACCEPTED_AT_COLUMN, HISTORY_PREFIX, TRANSACTION_ID_COLUMN};
use jazz::time::GlobalTime;
use jazz::tools::{ObjectId, TransactionId, Value};

use crate::error::{Error, Result};
use crate::value::Timestamp;

/// A table, by name, with its stored columns as `jazz.app.json` names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
}

impl Table {
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }
}

/// A relation [`Query::hop_to`] follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hop {
    /// To the row of `to` that each row's `column` references.
    Forward { column: String, to: Table },
    /// To the rows of `to` whose `column` references each row.
    Reverse { column: String, to: Table },
    /// To the rows of `to` that each row's array `column` (`uuid[]`)
    /// references.
    ForwardArray { column: String, to: Table },
}

/// How a [`Query::gather`] goes from a row to the next.
#[derive(Debug, Clone, PartialEq)]
pub enum GatherStep {
    /// To the row the row's `column` references, a row of the same table,
    /// stepping only from rows matching `filters`.
    Along {
        column: String,
        filters: Vec<Predicate>,
    },
    /// Through the rows of the table `edge` whose `current` column
    /// references the row and that match `filters`, to the rows their
    /// `next` column references.
    Via {
        edge: String,
        current: String,
        next: String,
        filters: Vec<Predicate>,
    },
}

/// What a query reads in place of its table's rows.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// Rows at the end of a path of joins (none for a table's own rows):
    /// the rows of the output table at `scope`, whose `columns` it projects.
    Path {
        expr: RelationExpr,
        scope: String,
        hops: usize,
        columns: Vec<String>,
    },
    /// The rows a gather reached, of the query's table.
    Gathered(RelationExpr),
    /// The rows of each arm, labelled.
    Union(Vec<RelationUnionArm>),
}

/// The scope a gather's step reaches the next rows at.
const RECURSIVE_HOP: &str = "__recursive_hop_0";

/// A query of rows of a table, by name.
#[derive(Debug, Clone)]
pub struct Query {
    /// Its ordinary clauses; `table` is the output table.
    pub(crate) query: q::Query,
    /// The output table's stored columns.
    columns: Vec<String>,
    source: Option<Source>,
    include_deleted: bool,
    /// Why the query cannot run, reported when it is read.
    refused: Option<String>,
}

impl Query {
    /// Every row of `table`.
    pub fn table(table: Table) -> Self {
        Self {
            query: q::Query::from(table.name),
            columns: table.columns,
            source: None,
            include_deleted: false,
            refused: None,
        }
    }

    /// Every accepted version of `table`'s rows, as TypeScript's
    /// `app.$history.<table>` reads them. Bound it to rows with an `id`
    /// equality or `IN` filter before reading it. It does not hop, gather,
    /// union or include.
    pub fn history(table: Table) -> Self {
        Self::table(Table {
            name: format!("{HISTORY_PREFIX}{}", table.name),
            columns: table.columns,
        })
    }

    /// The output table's name.
    pub fn table_name(&self) -> &str {
        &self.query.table
    }

    /// Refuses the query with `reason`, unless it already is.
    pub fn refuse(mut self, reason: impl Into<String>) -> Self {
        self.refused.get_or_insert_with(|| reason.into());
        self
    }

    fn is_union(&self) -> bool {
        matches!(self.source, Some(Source::Union(_)))
    }

    fn is_history(&self) -> bool {
        jazz::schema::history::is_history_table(&self.query.table)
    }

    /// A union takes only `order_by`, `limit` and `offset`, as in
    /// TypeScript.
    fn refuse_on_union(self, modifier: &str) -> Self {
        if self.is_union() {
            return self.refuse(format!(
                "union(...) supports only order_by, offset and limit; {modifier} is not supported"
            ));
        }
        self
    }

    /// Only rows matching `predicate` (conditions on the output table's
    /// columns compared with literals). After a gather, they narrow what
    /// it gathered.
    pub fn filter(self, predicate: Predicate) -> Self {
        let mut query = self.refuse_on_union("filter(...)");
        query.query.filters.push(predicate);
        query
    }

    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDirection) -> Self {
        self.query.order_by.push(q::OrderBy {
            column: column.into(),
            direction,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.query.offset = offset;
        self
    }

    /// Reads only these columns of each row (and its id).
    pub fn select(self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut query = self.refuse_on_union("select(...)");
        query.query.select = Some(columns.into_iter().map(Into::into).collect());
        query
    }

    /// Includes rows with each row, as `subquery` (named by its
    /// `column_name`) reads them: see [`Query::into_include`].
    pub fn include(self, subquery: Result<ArraySubquery>) -> Self {
        let mut query = self.refuse_on_union("include(...)");
        if matches!(
            query.source,
            Some(Source::Path { .. } | Source::Gathered(_))
        ) {
            query = query.refuse("include(...) on hop_to(...) or gather(...) is not supported yet");
        }
        if query.is_history() {
            query = query.refuse("include(...) on a history read is not supported");
        }
        match subquery {
            Ok(subquery) => query.query.array_subqueries.push(subquery),
            Err(Error::Unsupported(reason)) => query = query.refuse(reason),
            Err(error) => query = query.refuse(error.to_string()),
        }
        query
    }

    /// Reads deleted rows too, as TypeScript's `includeDeleted()` does:
    /// a read option, which [`Query::reads_deleted`] reports.
    pub fn include_deleted(self) -> Self {
        let mut query = self.refuse_on_union("include_deleted()");
        query.include_deleted = true;
        query
    }

    /// Whether the query reads deleted rows too.
    pub fn reads_deleted(&self) -> bool {
        self.include_deleted
    }

    /// Only history versions the authority accepted between `from` and
    /// `to`, in milliseconds since the Unix epoch.
    pub fn accepted(mut self, from: Bound<u64>, to: Bound<u64>) -> Self {
        if !self.is_history() {
            return self.refuse("accepted(...) filters history reads only");
        }
        // An acceptance time is an HLC: milliseconds over a logical
        // counter. Every HLC of a millisecond lies from its first on to
        // the next millisecond's first.
        let first = match from {
            Bound::Included(at) => Some(at),
            Bound::Excluded(at) => Some(at.saturating_add(1)),
            Bound::Unbounded => None,
        };
        let end = match to {
            Bound::Included(at) => Some(at.saturating_add(1)),
            Bound::Excluded(at) => Some(at),
            Bound::Unbounded => None,
        };
        let column = || q::col(ACCEPTED_AT_COLUMN);
        let at = |hlc: u64| q::lit(CoreValue::U64(hlc));
        if let Some(first) = first {
            self.query.filters.push(match hlc(first) {
                Some(first) => q::gte(column(), at(first)),
                // Past the last time there is: no version.
                None => Predicate::Any(Vec::new()),
            });
        }
        // Past the last time there is, every version is before it.
        if let Some(end) = end.and_then(hlc) {
            self.query.filters.push(q::lt(column(), at(end)));
        }
        self
    }

    /// Orders history versions by when they were accepted.
    pub fn order_by_accepted_at(self, direction: OrderDirection) -> Self {
        if !self.is_history() {
            return self.refuse("order_by_accepted_at(...) orders history reads only");
        }
        self.order_by(ACCEPTED_AT_COLUMN, direction)
    }

    /// The rows `hop` reaches from this query's rows. Hops chain; filters
    /// after a hop apply to the rows it reached. As in TypeScript, a query
    /// has one order and window, applied to the rows it ends on: an
    /// `order_by`, `limit` or `offset` before a hop orders and windows the
    /// rows the hop reaches (an order names a column of the table reached),
    /// and later ones add to or replace it as they would without the hop.
    pub fn hop_to(mut self, hop: Hop) -> Self {
        let include_deleted = self.include_deleted;
        let order_by = std::mem::take(&mut self.query.order_by);
        let limit = self.query.limit.take();
        let offset = std::mem::take(&mut self.query.offset);
        let (column, to, forward) = match hop {
            Hop::Forward { column, to } | Hop::ForwardArray { column, to } => (column, to, true),
            Hop::Reverse { column, to } => (column, to, false),
        };
        let mut query = Self::table(to);
        query.include_deleted = include_deleted;
        query.query.order_by = order_by;
        query.query.limit = limit;
        query.query.offset = offset;
        let (expr, scope, hops) = match self.path("hop_to(...)") {
            Ok(path) => path,
            Err(reason) => return query.refuse(reason),
        };
        let alias = format!("__hop_{hops}");
        let on = if forward {
            RelationJoinCondition {
                left: scoped(&scope, &column),
                right: scoped(&alias, "id"),
            }
        } else {
            RelationJoinCondition {
                left: scoped(&scope, "id"),
                right: scoped(&alias, &column),
            }
        };
        query.source = Some(Source::Path {
            expr: RelationExpr::Join {
                left: Box::new(expr),
                right: Box::new(RelationExpr::TableScan {
                    table: query.query.table.clone(),
                    alias: Some(alias.clone()),
                }),
                on: vec![on],
                join_kind: RelationJoinKind::Inner,
            },
            scope: alias,
            hops: hops + 1,
            columns: query.columns.clone(),
        });
        query
    }

    /// These rows and those `step` reaches from them, recursively, up to
    /// `max_depth` steps (zero reads these rows only), each once. The
    /// query's filters choose the rows it starts from; filters added after
    /// it narrow what it gathered, and `order_by`, `limit` and `offset`
    /// apply to what it gathered.
    pub fn gather(self, step: GatherStep, max_depth: usize) -> Self {
        let table = self.query.table.clone();
        let mut out = Self::table(Table {
            name: table.clone(),
            columns: self.columns.clone(),
        });
        out.query.order_by = self.query.order_by.clone();
        out.query.limit = self.query.limit;
        out.query.offset = self.query.offset;
        out.include_deleted = self.include_deleted;
        if self.is_union() {
            return out.refuse("gather(...) over a union(...) is not supported yet");
        }
        let mut seed = self;
        seed.query.order_by.clear();
        seed.query.limit = None;
        seed.query.offset = 0;
        let seed = match seed.path("gather(...)") {
            // A table's own rows seed the gather unprojected, as in
            // TypeScript.
            Ok((expr, _, 0)) => expr,
            Ok((expr, scope, _)) => project(expr, &scope, &out.columns),
            Err(reason) => return out.refuse(reason),
        };
        let (edge, current, next, filters) = match step {
            GatherStep::Along { column, filters } => {
                (table.clone(), "id".to_owned(), column, filters)
            }
            GatherStep::Via {
                edge,
                current,
                next,
                filters,
            } => (edge, current, next, filters),
        };
        let mut conditions = match predicates(&filters, &edge) {
            Ok(conditions) => conditions,
            Err(reason) => return out.refuse(reason),
        };
        conditions.push(RelationPredicate::Cmp {
            left: scoped(&edge, &current),
            op: RelationCmpOp::Eq,
            right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
        });
        let edges = filter(
            RelationExpr::TableScan {
                table: edge.clone(),
                alias: None,
            },
            conditions,
        );
        let step = project(
            RelationExpr::Join {
                left: Box::new(edges),
                right: Box::new(RelationExpr::TableScan {
                    table: table.clone(),
                    alias: Some(RECURSIVE_HOP.to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: scoped(&edge, &next),
                    right: scoped(RECURSIVE_HOP, "id"),
                }],
                join_kind: RelationJoinKind::Inner,
            },
            RECURSIVE_HOP,
            &out.columns,
        );
        let current = RelationKeyRef::RowId(RelationRowIdRef::Current);
        out.source = Some(Source::Gathered(RelationExpr::Gather {
            seed: Box::new(seed),
            step: Box::new(step),
            frontier_key: current.clone(),
            bound: RecursionBound::MaxDepth(max_depth),
            dedupe_key: vec![current],
        }));
        out
    }

    /// The rows of each labelled query of `table`, one after another: a row
    /// several match comes once for each. Labels are 1 to 4096 bytes, with
    /// no NUL, and each different. Like TypeScript's `union`, it takes
    /// `order_by`, `limit` and `offset`, and nothing else.
    pub fn union(table: Table, arms: impl IntoIterator<Item = (String, Query)>) -> Self {
        let mut union = Self::table(table);
        let mut labels = BTreeSet::new();
        let mut inputs = Vec::new();
        for (label, arm) in arms {
            if label.is_empty() || label.len() > 4096 || label.contains('\0') {
                return union.refuse(format!(
                    "union(...) labels must be 1..=4096 bytes and NUL-free, not {label:?}"
                ));
            }
            if !labels.insert(label.clone()) {
                return union.refuse(format!("union(...) labels must differ: {label:?} repeats"));
            }
            if arm.query.table != union.query.table {
                let reason = format!(
                    "union(...) arms must read {}, not {}",
                    union.query.table, arm.query.table
                );
                return union.refuse(reason);
            }
            match arm.into_arm() {
                Ok(input) => inputs.push(RelationUnionArm { label, input }),
                Err(reason) => return union.refuse(reason),
            }
        }
        if inputs.is_empty() {
            return union.refuse("union(...) needs at least one query");
        }
        union.source = Some(Source::Union(inputs));
        union
    }

    /// The core query to read, its relation lowered. Whether it reads
    /// deleted rows is a read option: see [`Query::reads_deleted`].
    ///
    /// Fails when the query combines what is not supported.
    pub fn into_core(self) -> Result<q::Query> {
        if let Some(reason) = self.refused {
            return Err(Error::Unsupported(reason));
        }
        let Some(source) = self.source else {
            return Ok(self.query);
        };
        let mut query = self.query;
        let rel = lower(&mut query, source).map_err(Error::Unsupported)?;
        query.relation = Some(q::RelationQuery { rel });
        Ok(query)
    }

    /// The query as the child of an include, correlated by its
    /// `inner_column` with the parent's `outer_column`, as `column_name`.
    /// A child filters, orders, windows, selects and includes; it does not
    /// hop, gather, union or read deleted rows, as in TypeScript. Without a
    /// selection it reads every stored column, by name.
    pub fn into_include(
        self,
        column_name: impl Into<String>,
        inner_column: &str,
        outer_column: &str,
    ) -> Result<ArraySubquery> {
        if let Some(reason) = self.refused {
            return Err(Error::Unsupported(reason));
        }
        if self.source.is_some() {
            return Err(Error::Unsupported(
                "include(...) of a hop_to(...), gather(...) or union(...) query is not supported"
                    .into(),
            ));
        }
        if self.include_deleted {
            return Err(Error::Unsupported(
                "include_deleted() in an include(...) is not supported yet".into(),
            ));
        }
        if self.is_history() {
            return Err(Error::Unsupported(
                "include(...) of a history read is not supported".into(),
            ));
        }
        let query = self.query;
        let mut subquery =
            ArraySubquery::new(column_name.into(), &query.table, inner_column, outer_column);
        subquery.filters = query.filters;
        // Included rows come as values in order: select every column by
        // name, so they decode by name.
        subquery.select = Some(query.select.unwrap_or(self.columns));
        subquery.order_by = query.order_by;
        subquery.limit = query.limit;
        subquery.offset = query.offset;
        subquery.nested_arrays = query.array_subqueries;
        Ok(subquery)
    }

    /// The rows the query reads, with its filters, as a path to extend:
    /// the expression, its output scope and how many hops it has.
    fn path(self, what: &str) -> std::result::Result<(RelationExpr, String, usize), String> {
        if let Some(reason) = self.refused {
            return Err(reason);
        }
        if self.is_history() {
            return Err(format!("{what} on a history read is not supported"));
        }
        if self.query.select.is_some() || !self.query.array_subqueries.is_empty() {
            return Err(format!(
                "{what} after select(...) or include(...) is not supported"
            ));
        }
        // Callers carry the order and window over to the rows they reach.
        debug_assert!(
            self.query.order_by.is_empty() && self.query.limit.is_none() && self.query.offset == 0
        );
        let table = self.query.table;
        let source = self.source.unwrap_or_else(|| Source::Path {
            expr: RelationExpr::TableScan {
                table: table.clone(),
                alias: None,
            },
            scope: table.clone(),
            hops: 0,
            columns: self.columns,
        });
        match source {
            Source::Path {
                expr, scope, hops, ..
            } => {
                let filters = predicates(&self.query.filters, &scope)?;
                Ok((filter(expr, filters), scope, hops))
            }
            Source::Gathered(expr) => {
                let filters = predicates(&self.query.filters, &table)?;
                Ok((filter(expr, filters), table, 0))
            }
            Source::Union(_) => Err(format!(
                "union(...) supports only order_by, offset and limit; {what} is not supported"
            )),
        }
    }

    /// The query as a union arm.
    fn into_arm(self) -> std::result::Result<RelationExpr, String> {
        if let Some(reason) = self.refused {
            return Err(reason);
        }
        if self.include_deleted {
            return Err("include_deleted() on a union(...) arm is not supported yet".to_owned());
        }
        if self.is_history() {
            return Err("union(...) of history reads is not supported".to_owned());
        }
        if self.query.select.is_some() || !self.query.array_subqueries.is_empty() {
            return Err(
                "select(...) or include(...) on a union(...) arm is not supported".to_owned(),
            );
        }
        let table = self.query.table.clone();
        let source = match self.source {
            Some(Source::Union(_)) => {
                return Err("union(...) nested in a union(...) is not supported yet".to_owned());
            }
            Some(source) => source,
            None => Source::Path {
                expr: RelationExpr::TableScan {
                    table: table.clone(),
                    alias: None,
                },
                scope: table,
                hops: 0,
                columns: self.columns,
            },
        };
        let mut query = self.query;
        lower(&mut query, source)
    }
}

/// A history row's `$transactionId` (as `JazzClient` reads it: its
/// transaction's time and node) as the public [`TransactionId`] a write
/// reports, which TypeScript's `$transactionId` is too.
pub fn transaction_id(value: &Value) -> Option<TransactionId> {
    let Value::Row { values, .. } = value else {
        return None;
    };
    let [Value::Timestamp(time), Value::Uuid(node)] = values.as_slice() else {
        return None;
    };
    Some(TransactionId::from_committed_tx(jazz::tx::TxId::new(
        jazz::time::TxTime(*time),
        jazz::ids::NodeUuid(*node.uuid()),
    )))
}

/// A history row's `$acceptedAt` (as `JazzClient` reads it: the full
/// acceptance HLC) in milliseconds since the Unix epoch, as TypeScript's
/// `$acceptedAt` `Date` holds it.
pub fn accepted_at(value: &Value) -> Option<Timestamp> {
    match value {
        Value::Timestamp(hlc) => Some(Timestamp(GlobalTime(*hlc).physical_ms())),
        _ => None,
    }
}

/// The names of a history row's metadata fields.
pub const HISTORY_FIELDS: [&str; 2] = [TRANSACTION_ID_COLUMN, ACCEPTED_AT_COLUMN];

/// The first HLC of millisecond `ms`; `None` past the last one there is.
fn hlc(ms: u64) -> Option<u64> {
    GlobalTime::new(ms, 0).map(|time| time.0)
}

/// Lowers `source`, with the query's filters, order and window, to the
/// relation expression the query reads, leaving the query only its
/// output table, selection and includes.
fn lower(query: &mut q::Query, source: Source) -> std::result::Result<RelationExpr, String> {
    let filters = std::mem::take(&mut query.filters);
    let (expr, scope) = match source {
        Source::Path {
            expr,
            scope,
            columns,
            ..
        } => {
            let filters = predicates(&filters, &scope)?;
            (project(filter(expr, filters), &scope, &columns), scope)
        }
        Source::Gathered(expr) => {
            let filters = predicates(&filters, &query.table)?;
            (filter(expr, filters), query.table.clone())
        }
        Source::Union(inputs) => {
            if !filters.is_empty() {
                return Err(
                    "union(...) supports only order_by, offset and limit; filter(...) is not supported"
                        .to_owned(),
                );
            }
            (RelationExpr::Union { inputs }, query.table.clone())
        }
    };
    let mut expr = expr;
    let order_by = std::mem::take(&mut query.order_by);
    if !order_by.is_empty() {
        expr = RelationExpr::OrderBy {
            input: Box::new(expr),
            terms: order_by
                .into_iter()
                .map(|order| RelationOrderBy {
                    column: scoped(&scope, &order.column),
                    direction: order.direction,
                })
                .collect(),
        };
    }
    let offset = std::mem::take(&mut query.offset);
    if offset > 0 {
        expr = RelationExpr::Offset {
            input: Box::new(expr),
            offset,
        };
    }
    if let Some(limit) = query.limit.take() {
        expr = RelationExpr::Limit {
            input: Box::new(expr),
            limit,
        };
    }
    Ok(expr)
}

fn scoped(scope: &str, column: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    }
}

/// The rows at `scope` as the output: their id and `columns`.
fn project(input: RelationExpr, scope: &str, columns: &[String]) -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(input),
        columns: std::iter::once("id")
            .chain(columns.iter().map(String::as_str))
            .map(|column| RelationProjectColumn {
                alias: column.to_owned(),
                expr: RelationProjectExpr::Column(scoped(scope, column)),
            })
            .collect(),
    }
}

fn filter(input: RelationExpr, mut predicates: Vec<RelationPredicate>) -> RelationExpr {
    let predicate = match predicates.len() {
        0 => return input,
        1 => predicates.pop().expect("one predicate"),
        _ => RelationPredicate::And(predicates),
    };
    RelationExpr::Filter {
        input: Box::new(input),
        predicate,
    }
}

fn predicates(
    filters: &[Predicate],
    scope: &str,
) -> std::result::Result<Vec<RelationPredicate>, String> {
    filters
        .iter()
        .map(|filter| predicate(filter, scope))
        .collect()
}

/// A condition's predicate as a relation predicate over the rows at
/// `scope`. Conditions compare a column with literals, so each lowers.
fn predicate(predicate: &Predicate, scope: &str) -> std::result::Result<RelationPredicate, String> {
    let column = |operand: &Operand| match operand {
        Operand::Column(name) => Ok(scoped(scope, name)),
        other => Err(format!("a condition on {other:?} cannot follow a relation")),
    };
    let value = |operand: &Operand| match operand {
        Operand::Literal(value) => Ok(RelationValueRef::literal(public(value.clone())?)),
        other => Err(format!("a condition on {other:?} cannot follow a relation")),
    };
    let cmp =
        |left: &Operand, op, right: &Operand| -> std::result::Result<RelationPredicate, String> {
            Ok(RelationPredicate::Cmp {
                left: column(left)?,
                op,
                right: value(right)?,
            })
        };
    Ok(match predicate {
        Predicate::All(children) => RelationPredicate::And(predicates(children, scope)?),
        Predicate::Any(children) => RelationPredicate::Or(predicates(children, scope)?),
        Predicate::Not(child) => RelationPredicate::Not(Box::new(self::predicate(child, scope)?)),
        Predicate::Eq(left, right) => cmp(left, RelationCmpOp::Eq, right)?,
        Predicate::Ne(left, right) => cmp(left, RelationCmpOp::Ne, right)?,
        Predicate::Gt(left, right) => cmp(left, RelationCmpOp::Gt, right)?,
        Predicate::Gte(left, right) => cmp(left, RelationCmpOp::Ge, right)?,
        Predicate::Lt(left, right) => cmp(left, RelationCmpOp::Lt, right)?,
        Predicate::Lte(left, right) => cmp(left, RelationCmpOp::Le, right)?,
        Predicate::In(left, values) => RelationPredicate::In {
            left: column(left)?,
            values: values
                .iter()
                .map(value)
                .collect::<std::result::Result<_, _>>()?,
        },
        Predicate::Contains(left, right) => RelationPredicate::Contains {
            left: column(left)?,
            right: value(right)?,
        },
        Predicate::IsNull(operand) => RelationPredicate::IsNull {
            column: column(operand)?,
        },
        Predicate::Removed(removed) => match *removed {},
    })
}

/// The public value a condition's literal came from (see
/// `crate::query::literal`).
fn public(value: CoreValue) -> std::result::Result<Value, String> {
    Ok(match value {
        CoreValue::Bool(value) => Value::Boolean(value),
        CoreValue::String(value) => Value::Text(value),
        CoreValue::I32(value) => Value::Integer(value),
        CoreValue::I64(value) => Value::BigInt(value),
        CoreValue::F64(value) => Value::Double(value),
        CoreValue::U64(value) => Value::Timestamp(value),
        CoreValue::Uuid(value) => Value::Uuid(ObjectId::from_uuid(value)),
        CoreValue::Bytes(value) => Value::Bytea(value),
        CoreValue::Nullable(None) => Value::Null,
        CoreValue::Array(values) => Value::Array(
            values
                .into_iter()
                .map(public)
                .collect::<std::result::Result<_, _>>()?,
        ),
        other => return Err(format!("a condition on {other:?} cannot follow a relation")),
    })
}
