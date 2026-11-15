//! Queries that read other rows than their table's: [`Query::hop_to`] a
//! relation's rows, [`Query::gather`] rows recursively, and
//! [`Query::union`] several queries' rows.
//!
//! They lower to the core's relation expressions as the TypeScript
//! client's query adapter lowers `hopTo`, `gather` and `union`.
//!
//! ```ignore
//! // The projects of the open todos: Query<Project>
//! todos::query()
//!     .filter(todos::done().eq(false))
//!     .hop_to(todos::project_relation());
//!
//! // A todo and every todo above it, up to ten levels: Query<Todo>
//! todos::query()
//!     .filter(todos::id().eq(leaf))
//!     .gather(todos::parent_relation(), 10);
//!
//! // The todos of either project: Query<Todo>
//! Query::union([
//!     todos::query().filter(todos::project_relation().referencing(home)),
//!     todos::query().filter(todos::project_relation().referencing(work)),
//! ]);
//! ```

use std::collections::BTreeSet;
use std::marker::PhantomData;

use jazz::query::{
    self as q, Operand, Predicate, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationOrderBy, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
    RelationValueRef,
};
use jazz::records::Value as CoreValue;
use jazz::tools::{ObjectId, Value};

use crate::query::{Condition, Many, One, Query};
use crate::table::Table;

/// What a query reads in place of its table's rows.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// Rows at the end of a path of joins (none for a table's own rows):
    /// the rows of the output table at `scope`, whose `columns` it projects.
    Path {
        expr: RelationExpr,
        scope: String,
        hops: usize,
        columns: &'static [&'static str],
    },
    /// The rows a gather reached, of the query's table.
    Gathered(RelationExpr),
    /// The rows of each arm, labelled.
    Union(Vec<RelationUnionArm>),
}

impl Source {
    /// A table's own rows.
    fn table<T: Table>() -> Self {
        Self::Path {
            expr: RelationExpr::TableScan {
                table: T::NAME.to_owned(),
                alias: None,
            },
            scope: T::NAME.to_owned(),
            hops: 0,
            columns: T::COLUMNS,
        }
    }
}

/// A relation [`Query::hop_to`] follows from rows of `T`: a forward
/// relation ([`One`]) to the row each references, or a reverse one
/// ([`Many`]) to the rows referencing each.
pub trait Hop<T>: Copy {
    /// The table it reaches.
    type To: Table;
    /// The join from rows at `from` to the rows it reaches at `to`.
    #[doc(hidden)]
    fn join(&self, from: &str, to: &str) -> RelationJoinCondition;
}

impl<T: Table, U: Table, N> Hop<T> for One<T, U, N> {
    type To = U;
    fn join(&self, from: &str, to: &str) -> RelationJoinCondition {
        RelationJoinCondition {
            left: scoped(from, self.column()),
            right: scoped(to, "id"),
        }
    }
}

impl<T: Table, U: Table> Hop<T> for Many<T, U> {
    type To = U;
    fn join(&self, from: &str, to: &str) -> RelationJoinCondition {
        RelationJoinCondition {
            left: scoped(from, "id"),
            right: scoped(to, self.column()),
        }
    }
}

/// How a [`Query::gather`] goes from a row of `T` to the next: along a
/// forward relation of `T` to itself ([`Step::along`], or the relation
/// itself), or through rows of an edge table `E` ([`Step::via`]).
#[derive(Debug, Clone)]
pub struct Step<T, E = T> {
    /// `E`'s column holding the current row.
    current: &'static str,
    /// `E`'s column holding the next row.
    next: &'static str,
    filters: Vec<Predicate>,
    types: PhantomData<fn() -> (T, E)>,
}

impl<T: Table> Step<T> {
    /// From each row to the row `relation` references: a todo to its parent.
    pub fn along<N>(relation: One<T, T, N>) -> Self {
        Self {
            current: "id",
            next: relation.column(),
            filters: Vec::new(),
            types: PhantomData,
        }
    }
}

impl<T: Table, N> From<One<T, T, N>> for Step<T> {
    fn from(relation: One<T, T, N>) -> Self {
        Self::along(relation)
    }
}

impl<T: Table, E: Table> Step<T, E> {
    /// From each row, through the rows of `E` that reference it by
    /// `current`, to the rows they reference by `next`: a team, through its
    /// `memberships` rows, to the teams it is a member of.
    pub fn via<N1, N2>(current: One<E, T, N1>, next: One<E, T, N2>) -> Self {
        Self {
            current: current.column(),
            next: next.column(),
            filters: Vec::new(),
            types: PhantomData,
        }
    }

    /// Steps only through the rows of `E` matching `condition`: for
    /// [`Step::along`], only from the rows matching it.
    pub fn filter(mut self, condition: Condition<E>) -> Self {
        self.filters.push(condition.predicate);
        self
    }
}

/// The scope a gather's step reaches the next rows at.
const RECURSIVE_HOP: &str = "__recursive_hop_0";

impl<T: Table> Query<T> {
    /// The rows `relation` reaches from this query's rows: the referenced
    /// row of a forward relation, the referencing rows of a reverse one.
    /// Hops chain, and filters after a hop narrow the rows it reached.
    ///
    /// ```ignore
    /// // Query<Project>
    /// todos::query().filter(todos::done().eq(false)).hop_to(todos::project_relation())
    /// ```
    ///
    /// Order and window the rows it reaches, not the rows it starts from:
    /// `order_by`, `limit` or `offset` before a hop is not supported.
    pub fn hop_to<H: Hop<T>>(self, relation: H) -> Query<H::To> {
        let include_deleted = self.include_deleted;
        let (expr, scope, hops) = match self.path() {
            Ok(path) => path,
            Err(reason) => {
                return Query::<H::To>::all().refuse(reason);
            }
        };
        let alias = format!("__hop_{hops}");
        let to = <H::To as Table>::NAME;
        let mut query = Query::<H::To>::all();
        query.source = Some(Source::Path {
            expr: RelationExpr::Join {
                left: Box::new(expr),
                right: Box::new(RelationExpr::TableScan {
                    table: to.to_owned(),
                    alias: Some(alias.clone()),
                }),
                on: vec![relation.join(&scope, &alias)],
                join_kind: RelationJoinKind::Inner,
            },
            scope: alias,
            hops: hops + 1,
            columns: <H::To as Table>::COLUMNS,
        });
        query.include_deleted = include_deleted;
        query
    }

    /// These rows and those `step` reaches from them, recursively, up to
    /// `max_depth` steps (zero reads these rows only), each once.
    /// The query's filters choose the rows it starts from; filters added
    /// after it narrow what it gathered, and `order_by`, `limit` and
    /// `offset` apply to what it gathered.
    ///
    /// ```ignore
    /// // A todo and its ancestors
    /// todos::query().filter(todos::id().eq(leaf)).gather(todos::parent_relation(), 10)
    /// ```
    pub fn gather<E: Table>(self, step: impl Into<Step<T, E>>, max_depth: usize) -> Self {
        let step = step.into();
        let mut out = Query::<T>::all();
        out.query.order_by = self.query.order_by.clone();
        out.query.limit = self.query.limit;
        out.query.offset = self.query.offset;
        out.include_deleted = self.include_deleted;
        out.refused = self.refused.clone();
        if matches!(self.source, Some(Source::Union(_))) {
            return out.refuse("gather(...) over a union(...) is not supported yet");
        }
        let mut seed = self;
        seed.query.order_by.clear();
        seed.query.limit = None;
        seed.query.offset = 0;
        let seed = match seed.path() {
            // A table's own rows seed the gather unprojected, as in
            // TypeScript.
            Ok((expr, _, 0)) => expr,
            Ok((expr, scope, _)) => project(expr, &scope, <T as Table>::COLUMNS),
            Err(reason) => return out.refuse(reason),
        };
        let edge = <E as Table>::NAME;
        let mut conditions = step
            .filters
            .iter()
            .map(|filter| predicate(filter, edge))
            .collect::<Result<Vec<_>, _>>();
        let conditions = match &mut conditions {
            Ok(conditions) => {
                conditions.push(RelationPredicate::Cmp {
                    left: scoped(edge, step.current),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                });
                std::mem::take(conditions)
            }
            Err(reason) => return out.refuse(reason.clone()),
        };
        let edges = filter(
            RelationExpr::TableScan {
                table: edge.to_owned(),
                alias: None,
            },
            conditions,
        );
        let step = project(
            RelationExpr::Join {
                left: Box::new(edges),
                right: Box::new(RelationExpr::TableScan {
                    table: T::NAME.to_owned(),
                    alias: Some(RECURSIVE_HOP.to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: scoped(edge, step.next),
                    right: scoped(RECURSIVE_HOP, "id"),
                }],
                join_kind: RelationJoinKind::Inner,
            },
            RECURSIVE_HOP,
            T::COLUMNS,
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

    /// The rows of each query, one after another: a row several match comes
    /// once for each. Like TypeScript's `union`, it takes `order_by`,
    /// `limit` and `offset`, and nothing else.
    ///
    /// ```ignore
    /// Query::union([
    ///     todos::query().filter(todos::done().eq(true)),
    ///     todos::query().filter(todos::title().contains("milk")),
    /// ])
    /// ```
    pub fn union(queries: impl IntoIterator<Item = Self>) -> Self {
        Self::union_named(
            queries
                .into_iter()
                .enumerate()
                .map(|(at, query)| (at.to_string(), query)),
        )
    }

    /// [`Query::union`] with a label of each query's own: 1 to 4096 bytes,
    /// with no NUL, and each different.
    pub fn union_named<L: Into<String>>(queries: impl IntoIterator<Item = (L, Self)>) -> Self {
        let mut union = Self::all();
        let mut labels = BTreeSet::new();
        let mut arms = Vec::new();
        for (label, query) in queries {
            let label = label.into();
            if label.is_empty() || label.len() > 4096 || label.contains('\0') {
                return union.refuse(format!(
                    "union(...) labels must be 1..=4096 bytes and NUL-free, not {label:?}"
                ));
            }
            if !labels.insert(label.clone()) {
                return union.refuse(format!("union(...) labels must differ: {label:?} repeats"));
            }
            match query.into_arm() {
                Ok(input) => arms.push(RelationUnionArm { label, input }),
                Err(reason) => return union.refuse(reason),
            }
        }
        if arms.is_empty() {
            return union.refuse("union(...) needs at least one query");
        }
        union.source = Some(Source::Union(arms));
        union
    }

    /// The rows the query reads, with its filters, as a path to extend:
    /// the expression, its output scope and how many hops it has.
    fn path(self) -> Result<(RelationExpr, String, usize), String> {
        if let Some(reason) = self.refused {
            return Err(reason);
        }
        if !self.query.order_by.is_empty() || self.query.limit.is_some() || self.query.offset > 0 {
            return Err(
                "hop_to(...) after order_by, limit or offset is not supported; \
                 order and window the rows it reaches"
                    .to_owned(),
            );
        }
        match self.source.unwrap_or_else(Source::table::<T>) {
            Source::Path {
                expr, scope, hops, ..
            } => {
                let filters = predicates(&self.query.filters, &scope)?;
                Ok((filter(expr, filters), scope, hops))
            }
            Source::Gathered(expr) => {
                let filters = predicates(&self.query.filters, T::NAME)?;
                Ok((filter(expr, filters), T::NAME.to_owned(), 0))
            }
            Source::Union(_) => Err(
                "union(...) supports only order_by, offset and limit; hop_to(...) is not supported"
                    .to_owned(),
            ),
        }
    }

    /// The query as a union arm.
    fn into_arm(self) -> Result<RelationExpr, String> {
        if let Some(reason) = self.refused {
            return Err(reason);
        }
        if self.include_deleted {
            return Err("include_deleted() on a union(...) arm is not supported yet".to_owned());
        }
        if matches!(self.source, Some(Source::Union(_))) {
            return Err("union(...) nested in a union(...) is not supported yet".to_owned());
        }
        let mut query = self.query;
        lower(&mut query, self.source.unwrap_or_else(Source::table::<T>))
    }
}

/// Lowers `source`, with the query's filters, order and window, to the
/// relation expression the query reads, leaving the query only its
/// output table, selection and includes.
pub(crate) fn lower(query: &mut q::Query, source: Source) -> Result<RelationExpr, String> {
    let filters = std::mem::take(&mut query.filters);
    let (expr, scope) = match source {
        Source::Path {
            expr,
            scope,
            columns,
            ..
        } => {
            let filters = predicates(&filters, &scope)?;
            (project(filter(expr, filters), &scope, columns), scope)
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
fn project(input: RelationExpr, scope: &str, columns: &[&str]) -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(input),
        columns: std::iter::once("id")
            .chain(columns.iter().copied())
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

fn predicates(filters: &[Predicate], scope: &str) -> Result<Vec<RelationPredicate>, String> {
    filters
        .iter()
        .map(|filter| predicate(filter, scope))
        .collect()
}

/// A condition's predicate as a relation predicate over the rows at
/// `scope`. Conditions compare a column with literals, so each lowers.
fn predicate(predicate: &Predicate, scope: &str) -> Result<RelationPredicate, String> {
    let column = |operand: &Operand| match operand {
        Operand::Column(name) => Ok(scoped(scope, name)),
        other => Err(format!("a condition on {other:?} cannot follow a relation")),
    };
    let value = |operand: &Operand| match operand {
        Operand::Literal(value) => Ok(RelationValueRef::literal(public(value.clone())?)),
        other => Err(format!("a condition on {other:?} cannot follow a relation")),
    };
    let cmp = |left: &Operand, op, right: &Operand| -> Result<RelationPredicate, String> {
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
            values: values.iter().map(value).collect::<Result<_, _>>()?,
        },
        Predicate::Contains(left, right) => RelationPredicate::Contains {
            left: column(left)?,
            right: value(right)?,
        },
        Predicate::IsNull(operand) => RelationPredicate::IsNull {
            column: column(operand)?,
        },
        Predicate::EnumMatch { .. } => {
            return Err("enum match conditions cannot follow a relation yet".to_owned());
        }
    })
}

/// The public value a condition's literal came from (see
/// [`crate::query::literal`]).
fn public(value: CoreValue) -> Result<Value, String> {
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
        CoreValue::Array(values) => {
            Value::Array(values.into_iter().map(public).collect::<Result<_, _>>()?)
        }
        other => return Err(format!("a condition on {other:?} cannot follow a relation")),
    })
}
