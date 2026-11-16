//! Typed queries: what generated table modules build on.

use std::marker::PhantomData;

use jazz::query::{self as q, OrderDirection, Predicate};

/// What part of a large value a query reads: see [`Query::part`].
pub use jazz::query::Part as ValuePart;
use jazz::records::Value as CoreValue;
use jazz::tools::{ObjectId, Value};

use crate::error::Result;
use crate::include::{IntoInclude, Partial, Row, Selection, Values, With};
use crate::table::Table;
use crate::untyped;
use crate::value::{ColumnValue, Timestamp};

/// A query of rows of a table, yielding `T`s: the table's rows, or
/// [`Partial`] rows of it, with the rows it includes after them. It is an
/// [`untyped::Query`] with the type of what it yields.
#[derive(Debug)]
pub struct Query<T> {
    pub(crate) inner: untyped::Query,
    row: PhantomData<fn() -> T>,
}

// Not derived: a query of `T` clones whether or not `T` does.
impl<T> Clone for Query<T> {
    fn clone(&self) -> Self {
        Self::wrap(self.inner.clone())
    }
}

impl<T> Query<T> {
    pub(crate) fn wrap(inner: untyped::Query) -> Self {
        Self {
            inner,
            row: PhantomData,
        }
    }

    pub(crate) fn map(self, f: impl FnOnce(untyped::Query) -> untyped::Query) -> Self {
        Self::wrap(f(self.inner))
    }

    /// The query by name, without its type.
    pub fn into_inner(self) -> untyped::Query {
        self.inner
    }
}

/// `T`'s name and stored columns.
pub(crate) fn table<T: Table>() -> untyped::Table {
    untyped::Table::new(T::NAME, T::COLUMNS.iter().copied())
}

impl<T: Table> Query<T> {
    /// Every row of `T`.
    pub fn all() -> Self {
        Self::wrap(untyped::Query::table(table::<T>()))
    }

    /// Reads only the `columns` (a stored column, or a tuple of up to
    /// twelve): each row as a [`Partial`] holding its id and their values,
    /// in order. A column with a transform reads as its view value, as in
    /// a full row. The id is always there: don't select [`id`].
    ///
    /// ```ignore
    /// // Query<Partial<Todo, (String, bool)>>
    /// todos::query().select((todos::title(), todos::done()))
    /// ```
    pub fn select<S: Selection<T>>(
        self,
        columns: S,
    ) -> Query<Partial<T, <S::Values as Values>::Out, S::Values>> {
        Query::wrap(self.inner.select(columns.names()))
    }
}

impl<T: Row> Query<T> {
    /// Reads `column` only in `part`: a byte range of a bytes column, a
    /// UTF-8 or UTF-16 range of a text one, or the subtree at a JSON
    /// pointer of a json one, as TS's partial select does. The row's value
    /// for `column` holds that part, and only the chunks it covers are
    /// read. On a transformed column, the view transform applies to the
    /// part read. A column a computed field is computed from is read whole,
    /// so the computed field is right (TS re-reads it in full). Inside an
    /// include's query it reads the included rows' column in part.
    ///
    /// ```ignore
    /// let head = jazz.one(tasks::query().id(id).part(tasks::attachment(), ValuePart::Bytes { from: 0, to: 1024 })).await?;
    /// ```
    pub fn part<V: ColumnValue>(
        self,
        column: Column<<T as Row>::Table, V>,
        part: ValuePart,
    ) -> Self {
        // A computed field computes from the whole value: TS re-reads a
        // partially read dependency in full, so it is read whole here.
        if <<T as Row>::Table as Table>::COMPUTED_FROM.contains(&column.name()) {
            return self;
        }
        Query::wrap(self.inner.select_part(column.name(), part))
    }

    /// Includes the rows of a relation of the table with each row, after
    /// what the row already has: `Todo` becomes `(Todo, Option<Project>)`,
    /// `(Todo, Option<Project>)` becomes `(Todo, Option<Project>, Vec<Todo>)`.
    /// See [`crate::Include`] for what each relation includes.
    pub fn include<I>(self, relation: I) -> Query<<T as With<I::Output>>::Output>
    where
        I: IntoInclude<<T as Row>::Table>,
        T: With<<I as IntoInclude<<T as Row>::Table>>::Output>,
    {
        let at = self.inner.query.array_subqueries.len();
        let subquery = relation.into_include().into_subquery(at);
        Query::wrap(self.inner.include(subquery))
    }

    /// Only rows matching `condition`. Conditions add up: a row must match
    /// each. After a [`Query::gather`], they narrow what it gathered.
    pub fn filter(self, condition: Condition<T::Table>) -> Self {
        self.map(|inner| inner.filter(condition.predicate))
    }

    /// Reads deleted rows too, as TypeScript's `includeDeleted()` does.
    /// [`deleted_at`] tells them apart: it is `None` for a row that is not
    /// deleted.
    pub fn include_deleted(self) -> Self {
        self.map(untyped::Query::include_deleted)
    }

    /// The row with this id.
    pub fn id(self, id: ObjectId) -> Self {
        self.filter(Condition::new(q::eq(
            q::col("id"),
            literal(Value::Uuid(id)),
        )))
    }

    pub fn order_by<V, P>(self, column: Column<T::Table, V, P>, order: Order) -> Self {
        self.map(|inner| inner.order_by(column.name, order.direction()))
    }

    pub fn limit(self, limit: usize) -> Self {
        self.map(|inner| inner.limit(limit))
    }

    pub fn offset(self, offset: usize) -> Self {
        self.map(|inner| inner.offset(offset))
    }

    /// The untyped query, for [`crate::Jazz::with_client`]. Whether it
    /// reads deleted rows is a read option: see [`Query::reads_deleted`].
    ///
    /// Fails when the query combines what is not supported yet.
    pub fn into_untyped(self) -> Result<q::Query> {
        self.inner.into_core()
    }

    /// Whether the query reads deleted rows too ([`Query::include_deleted`]).
    pub fn reads_deleted(&self) -> bool {
        self.inner.reads_deleted()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

impl Order {
    pub(crate) fn direction(self) -> OrderDirection {
        match self {
            Order::Asc => OrderDirection::Asc,
            Order::Desc => OrderDirection::Desc,
        }
    }
}

/// A condition on rows of `T`.
#[derive(Debug, Clone)]
pub struct Condition<T> {
    pub(crate) predicate: Predicate,
    row: PhantomData<fn() -> T>,
}

impl<T> Condition<T> {
    pub(crate) fn new(predicate: Predicate) -> Self {
        Self {
            predicate,
            row: PhantomData,
        }
    }

    pub fn and(self, other: Self) -> Self {
        Self::new(q::all_of([self.predicate, other.predicate]))
    }

    pub fn or(self, other: Self) -> Self {
        Self::new(q::any_of([self.predicate, other.predicate]))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Self::new(q::not(self.predicate))
    }
}

/// Column `name` of `T`, holding values of `V` as stored: conditions
/// compare them. `P` is what [`Query::select`] reads it as: `V` itself, or
/// for a column with a transform, a generated [`Pick`] that reads its view
/// value through the app's transform.
#[derive(Debug)]
pub struct Column<T, V, P = V> {
    name: &'static str,
    types: PhantomData<fn() -> (T, V)>,
    pick: PhantomData<fn() -> P>,
}

impl<T, V, P> Clone for Column<T, V, P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T, V, P> Copy for Column<T, V, P> {}

impl<T, V: ColumnValue, P> Column<T, V, P> {
    /// Generated bindings name the column as `jazz.app.json` does.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            types: PhantomData,
            pick: PhantomData,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    fn compare(
        self,
        compare: fn(q::Operand, q::Operand) -> Predicate,
        value: impl Into<V>,
    ) -> Condition<T> {
        Condition::new(compare(
            q::col(self.name),
            literal(value.into().into_value()),
        ))
    }

    // Each takes what converts into the column's type: `title().eq("milk")`
    // on a `String` column, `priority().gte(3)` on an `Option<i32>` one.

    pub fn eq(self, value: impl Into<V>) -> Condition<T> {
        self.compare(q::eq, value)
    }
    pub fn ne(self, value: impl Into<V>) -> Condition<T> {
        self.compare(q::ne, value)
    }
    pub fn gt(self, value: impl Into<V>) -> Condition<T> {
        self.compare(q::gt, value)
    }
    pub fn gte(self, value: impl Into<V>) -> Condition<T> {
        self.compare(q::gte, value)
    }
    pub fn lt(self, value: impl Into<V>) -> Condition<T> {
        self.compare(q::lt, value)
    }
    pub fn lte(self, value: impl Into<V>) -> Condition<T> {
        self.compare(q::lte, value)
    }
    pub fn is_in<I>(self, values: I) -> Condition<T>
    where
        I: IntoIterator,
        I::Item: Into<V>,
    {
        Condition::new(q::in_list(
            q::col(self.name),
            values
                .into_iter()
                .map(|value| literal(value.into().into_value())),
        ))
    }
}

/// The row's id: a column of every table.
pub fn id<T>() -> Column<T, ObjectId> {
    Column::new("id")
}

/// When the row was created: a column of every table.
pub fn created_at<T>() -> Column<T, Timestamp> {
    Column::new("$createdAt")
}

/// When the row was last updated: a column of every table.
pub fn updated_at<T>() -> Column<T, Timestamp> {
    Column::new("$updatedAt")
}

/// When the row was deleted: `None` unless it is. Only a query that
/// [`Query::include_deleted`] reads deleted rows.
pub fn deleted_at<T>() -> Column<T, Option<Timestamp>> {
    Column::new("$deletedAt")
}

impl<T, P> Column<T, String, P> {
    /// The text contains `part`.
    pub fn contains(self, part: impl Into<String>) -> Condition<T> {
        contains(self.name, Value::Text(part.into()))
    }
}

impl<T, P> Column<T, Option<String>, P> {
    /// The text is set and contains `part`.
    pub fn contains(self, part: impl Into<String>) -> Condition<T> {
        contains(self.name, Value::Text(part.into()))
    }
}

impl<T, V: ColumnValue, P> Column<T, Vec<V>, P> {
    /// The array holds `element`.
    pub fn contains(self, element: impl Into<V>) -> Condition<T> {
        contains(self.name, element.into().into_value())
    }
}

fn contains<T>(column: &'static str, value: Value) -> Condition<T> {
    Condition::new(q::contains(q::col(column), literal(value)))
}

impl<T, V: ColumnValue, P> Column<T, Option<V>, P> {
    pub fn is_null(self) -> Condition<T> {
        Condition::new(q::is_null(q::col(self.name)))
    }

    pub fn is_not_null(self) -> Condition<T> {
        Condition::new(q::not(q::is_null(q::col(self.name))))
    }
}

/// A forward relation: `T`'s `column` references one row of `U`. `N` is
/// whether the column may be null: [`NotNull`] or [`Nullable`].
#[derive(Debug)]
pub struct One<T, U, N = NotNull> {
    column: &'static str,
    types: PhantomData<fn() -> (T, U)>,
    nullability: PhantomData<N>,
}

/// A forward relation's column always holds a reference.
#[derive(Debug, Clone, Copy)]
pub enum NotNull {}

/// A forward relation's column may be null.
#[derive(Debug, Clone, Copy)]
pub enum Nullable {}

impl<T, U, N> Clone for One<T, U, N> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T, U, N> Copy for One<T, U, N> {}

impl<T: Table, U: Table, N> One<T, U, N> {
    pub const fn new(column: &'static str) -> Self {
        Self {
            column,
            types: PhantomData,
            nullability: PhantomData,
        }
    }

    /// The column holding the reference.
    pub fn column(&self) -> &'static str {
        self.column
    }

    /// The row `id` references.
    pub fn of(self, id: ObjectId) -> Query<U> {
        Query::all().id(id)
    }

    /// Rows of `T` that reference the row of `U` with this id.
    pub fn referencing(self, id: ObjectId) -> Condition<T> {
        Condition::new(q::eq(q::col(self.column), literal(Value::Uuid(id))))
    }
}

/// A forward relation over an array column: `T`'s `column` holds the ids
/// of rows of `U`. `N` is whether the column may be null.
#[derive(Debug)]
pub struct Refs<T, U, N = NotNull> {
    column: &'static str,
    types: PhantomData<fn() -> (T, U)>,
    nullability: PhantomData<N>,
}

impl<T, U, N> Clone for Refs<T, U, N> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T, U, N> Copy for Refs<T, U, N> {}

impl<T: Table, U: Table, N> Refs<T, U, N> {
    pub const fn new(column: &'static str) -> Self {
        Self {
            column,
            types: PhantomData,
            nullability: PhantomData,
        }
    }

    /// The array column holding the references.
    pub fn column(&self) -> &'static str {
        self.column
    }

    /// Rows of `T` whose array references the row of `U` with this id.
    pub fn referencing(self, id: ObjectId) -> Condition<T> {
        Condition::new(q::contains(q::col(self.column), literal(Value::Uuid(id))))
    }
}

/// A reverse relation: the rows of `U` whose `column` references a row of `T`.
#[derive(Debug)]
pub struct Many<T, U> {
    column: &'static str,
    types: PhantomData<fn() -> (T, U)>,
}

impl<T, U> Clone for Many<T, U> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T, U> Copy for Many<T, U> {}

impl<T: Table, U: Table> Many<T, U> {
    /// `column` is `U`'s column of the forward relation this reverses.
    pub const fn new(column: &'static str) -> Self {
        Self {
            column,
            types: PhantomData,
        }
    }

    /// `U`'s column holding the reference.
    pub fn column(&self) -> &'static str {
        self.column
    }

    /// The rows of `U` referencing the row of `T` with this id.
    pub fn of(self, id: ObjectId) -> Query<U> {
        Query::all().filter(Condition::new(q::eq(
            q::col(self.column),
            literal(Value::Uuid(id)),
        )))
    }
}

/// A literal operand for a value a [`ColumnValue`] produced.
pub(crate) fn literal(value: Value) -> q::Operand {
    q::lit(core(value))
}

fn core(value: Value) -> CoreValue {
    match value {
        Value::Boolean(value) => CoreValue::Bool(value),
        Value::Text(value) => CoreValue::String(value),
        Value::Integer(value) => CoreValue::I32(value),
        Value::BigInt(value) => CoreValue::I64(value),
        Value::Double(value) => CoreValue::F64(value),
        Value::Timestamp(value) => CoreValue::U64(value),
        Value::Uuid(value) => CoreValue::Uuid(*value.uuid()),
        Value::Bytea(value) => CoreValue::Bytes(value),
        Value::Null => CoreValue::Nullable(None),
        Value::Array(values) => CoreValue::Array(values.into_iter().map(core).collect()),
        other => unreachable!("column values are scalars or arrays of them, not {other:?}"),
    }
}
