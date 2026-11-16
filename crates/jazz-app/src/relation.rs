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

use std::marker::PhantomData;

use jazz::query::Predicate;

use crate::query::{Condition, Many, One, Query, Refs, table};
use crate::table::Table;
use crate::untyped;

/// A relation [`Query::hop_to`] follows from rows of `T`: a forward
/// relation ([`One`]) to the row each references, an array one ([`Refs`])
/// to the rows each references, or a reverse one ([`Many`]) to the rows
/// referencing each.
pub trait Hop<T>: Copy {
    /// The table it reaches.
    type To: Table;
    /// The relation by name.
    fn into_untyped(self) -> untyped::Hop;
}

impl<T: Table, U: Table, N> Hop<T> for One<T, U, N> {
    type To = U;
    fn into_untyped(self) -> untyped::Hop {
        untyped::Hop::Forward {
            column: self.column().to_owned(),
            to: table::<U>(),
        }
    }
}

impl<T: Table, U: Table, N> Hop<T> for Refs<T, U, N> {
    type To = U;
    fn into_untyped(self) -> untyped::Hop {
        untyped::Hop::ForwardArray {
            column: self.column().to_owned(),
            to: table::<U>(),
        }
    }
}

impl<T: Table, U: Table> Hop<T> for Many<T, U> {
    type To = U;
    fn into_untyped(self) -> untyped::Hop {
        untyped::Hop::Reverse {
            column: self.column().to_owned(),
            to: table::<U>(),
        }
    }
}

/// How a [`Query::gather`] goes from a row of `T` to the next: along a
/// forward relation of `T` to itself ([`Step::along`], or the relation
/// itself), or through rows of an edge table `E` ([`Step::via`]).
#[derive(Debug, Clone)]
pub struct Step<T, E = T> {
    step: untyped::GatherStep,
    types: PhantomData<fn() -> (T, E)>,
}

impl<T: Table> Step<T> {
    /// From each row to the row `relation` references: a todo to its parent.
    pub fn along<N>(relation: One<T, T, N>) -> Self {
        Self {
            step: untyped::GatherStep::Along {
                column: relation.column().to_owned(),
                filters: Vec::new(),
            },
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
            step: untyped::GatherStep::Via {
                edge: E::NAME.to_owned(),
                current: current.column().to_owned(),
                next: next.column().to_owned(),
                filters: Vec::new(),
            },
            types: PhantomData,
        }
    }

    /// Steps only through the rows of `E` matching `condition`: for
    /// [`Step::along`], only from the rows matching it.
    pub fn filter(mut self, condition: Condition<E>) -> Self {
        filters(&mut self.step).push(condition.predicate);
        self
    }
}

fn filters(step: &mut untyped::GatherStep) -> &mut Vec<Predicate> {
    match step {
        untyped::GatherStep::Along { filters, .. } | untyped::GatherStep::Via { filters, .. } => {
            filters
        }
    }
}

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
    /// As in TypeScript, a query has one order and window, applied to the
    /// rows it ends on: `limit`, `offset` and `order_by` before a hop
    /// window and order the rows it reaches. An order before a hop names a
    /// column of the table it reaches by name, so order after the hop, with
    /// that table's columns, where you can.
    pub fn hop_to<H: Hop<T>>(self, relation: H) -> Query<H::To> {
        Query::wrap(self.inner.hop_to(relation.into_untyped()))
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
        let step = step.into().step;
        self.map(|inner| inner.gather(step, max_depth))
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
        Self::wrap(untyped::Query::union(
            table::<T>(),
            queries
                .into_iter()
                .map(|(label, query)| (label.into(), query.inner)),
        ))
    }
}
