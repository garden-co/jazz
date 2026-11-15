//! What a query yields beyond a table's rows: related rows included with
//! them ([`Query::include`]) and partial rows ([`Query::select`]).
//!
//! A query of `T` yields `T`s. Each include adds the related rows to a tuple
//! after the row, in the order they were included:
//!
//! ```ignore
//! // (Todo, Option<Project>, Vec<Todo>)
//! let rows = jazz.all(
//!     todos::query()
//!         .include(todos::project_relation())
//!         .include(todos::todos_via_parent()),
//! ).await?;
//! for (todo, project, subtasks) in rows { /* … */ }
//! ```
//!
//! A forward relation ([`One`]) includes an `Option<U>`: `None` when the
//! row references nothing, or a row it cannot read. [`One::require`] makes
//! it a `U`, and keeps only the rows whose referenced row is there. A
//! reverse relation ([`Many`]) includes a `Vec<U>`. `with` narrows, orders,
//! limits, selects or nests what a relation includes:
//!
//! ```ignore
//! // (Project, Vec<(Todo, Option<Todo>)>)
//! projects::query().include(
//!     projects::todos_via_project().with(
//!         todos::query()
//!             .filter(todos::done().eq(false))
//!             .include(todos::parent_relation()),
//!     ),
//! )
//! ```

use std::marker::PhantomData;

use jazz::query::{ArraySubquery, ArraySubqueryRequirement};
use jazz::tools::{ObjectId, QueryResultField, Value};

use crate::error::{Error, Result};
use crate::query::{Column, Many, NotNull, Nullable, One, Query, Refs};
use crate::table::{Fields, Implementations, Table};
use crate::value::ColumnValue;

/// What a query yields: a row of a [`Table`], a [`Partial`] row, or either
/// with included rows after it, as a tuple.
pub trait Row: Sized + Send + 'static {
    /// The table the query reads.
    type Table: Table;

    /// The id of the table's row this is.
    fn row_id(&self) -> ObjectId;

    /// Reads it from a result's fields.
    #[doc(hidden)]
    fn decode_fields(fields: &mut Fields<'_>) -> Result<Self>;

    /// The names of an included row's values, in the order they come.
    #[doc(hidden)]
    fn value_names() -> Vec<String>;
}

impl<T: Table> Row for T {
    type Table = T;

    fn row_id(&self) -> ObjectId {
        Table::id(self)
    }

    fn decode_fields(fields: &mut Fields<'_>) -> Result<Self> {
        T::decode(fields)
    }

    fn value_names() -> Vec<String> {
        T::COLUMNS.iter().map(|&name| name.to_owned()).collect()
    }
}

/// What a tuple of included rows starts with: a row or a partial row.
#[doc(hidden)]
pub trait Base: Row {}

impl<T: Table> Base for T {}

/// A partial row of `T`: its id, and the values of the columns a
/// [`Query::select`] picked, as `V` (a value, or a tuple of them).
#[derive(Debug, Clone, PartialEq)]
pub struct Partial<T, V> {
    pub id: ObjectId,
    pub values: V,
    table: PhantomData<fn() -> T>,
}

impl<T, V> Partial<T, V> {
    pub fn new(id: ObjectId, values: V) -> Self {
        Self {
            id,
            values,
            table: PhantomData,
        }
    }

    pub fn into_values(self) -> V {
        self.values
    }
}

impl<T: Table, V: Values> Row for Partial<T, V> {
    type Table = T;

    fn row_id(&self) -> ObjectId {
        self.id
    }

    fn decode_fields(fields: &mut Fields<'_>) -> Result<Self> {
        Ok(Self::new(fields.id(), V::decode_values(fields)?))
    }

    fn value_names() -> Vec<String> {
        // Partial values are read in order, not by name.
        (0..V::LEN).map(|at| format!("$selected{at}")).collect()
    }
}

impl<T: Table, V: Values> Base for Partial<T, V> {}

/// The values of a selection: what [`Query::select`] picks.
pub trait Selection<T>: Send + 'static {
    /// The values a partial row holds for it.
    type Values: Values;
    #[doc(hidden)]
    fn names(&self) -> Vec<&'static str>;
}

/// Values read in the order a selection picked their columns.
#[doc(hidden)]
pub trait Values: Sized + Send + 'static {
    const LEN: usize;
    fn decode_values(fields: &mut Fields<'_>) -> Result<Self>;
}

impl<V: ColumnValue> Values for V {
    const LEN: usize = 1;
    fn decode_values(fields: &mut Fields<'_>) -> Result<Self> {
        fields.take_next::<V>()
    }
}

impl<T: Send + 'static, V: ColumnValue> Selection<T> for Column<T, V> {
    type Values = V;
    fn names(&self) -> Vec<&'static str> {
        vec![self.name()]
    }
}

macro_rules! selections {
    ($($value:ident $column:ident),+; $len:expr) => {
        impl<T: Send + 'static, $($value: ColumnValue),+> Selection<T> for ($(Column<T, $value>,)+) {
            type Values = ($($value,)+);
            fn names(&self) -> Vec<&'static str> {
                let ($($column,)+) = self;
                vec![$($column.name()),+]
            }
        }

        impl<$($value: ColumnValue),+> Values for ($($value,)+) {
            const LEN: usize = $len;
            fn decode_values(fields: &mut Fields<'_>) -> Result<Self> {
                Ok(($(fields.take_next::<$value>()?,)+))
            }
        }
    };
}

selections!(A a; 1);
selections!(A a, B b; 2);
selections!(A a, B b, C c; 3);
selections!(A a, B b, C c, D d; 4);
selections!(A a, B b, C c, D d, E e; 5);
selections!(A a, B b, C c, D d, E e, F f; 6);
selections!(A a, B b, C c, D d, E e, F f, G g; 7);
selections!(A a, B b, C c, D d, E e, F f, G g, H h; 8);
selections!(A a, B b, C c, D d, E e, F f, G g, H h, I i; 9);
selections!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j; 10);
selections!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k; 11);
selections!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l; 12);

/// What a relation includes with each row: `Option<U>`, `U` or `Vec<U>`.
#[doc(hidden)]
pub trait Included: Sized + Send + 'static {
    fn decode_included(value: Value, implementations: &Implementations) -> Result<Self>;
}

impl<R: Row> Included for Vec<R> {
    fn decode_included(value: Value, implementations: &Implementations) -> Result<Self> {
        rows::<R>(value, implementations)
    }
}

impl<R: Row> Included for Option<R> {
    fn decode_included(value: Value, implementations: &Implementations) -> Result<Self> {
        let mut rows = rows::<R>(value, implementations)?;
        if rows.len() > 1 {
            return Err(decode_error::<R>(format!(
                "a forward include holds {} rows, not at most one",
                rows.len()
            )));
        }
        Ok(rows.pop())
    }
}

/// A required forward include: the row, which the query made sure is there.
impl<R: Row> Included for R {
    fn decode_included(value: Value, implementations: &Implementations) -> Result<Self> {
        Option::<R>::decode_included(value, implementations)?
            .ok_or_else(|| decode_error::<R>("a required include holds no row".to_owned()))
    }
}

fn rows<R: Row>(value: Value, implementations: &Implementations) -> Result<Vec<R>> {
    let Value::Array(rows) = value else {
        return Err(decode_error::<R>(format!(
            "an include holds {value:?}, not rows"
        )));
    };
    let names = R::value_names();
    rows.into_iter()
        .map(|row| {
            let Value::Row {
                id: Some(id),
                values,
            } = row
            else {
                return Err(decode_error::<R>(format!(
                    "an include holds {row:?}, not a row"
                )));
            };
            if values.len() != names.len() {
                return Err(decode_error::<R>(format!(
                    "an included row holds {} values, not {}",
                    values.len(),
                    names.len()
                )));
            }
            let fields = names
                .iter()
                .cloned()
                .zip(values)
                .map(|(name, value)| QueryResultField { name, value })
                .collect();
            R::decode_fields(&mut Fields::new(
                <R::Table as Table>::NAME,
                id,
                fields,
                implementations,
            ))
        })
        .collect()
}

fn decode_error<R: Row>(message: String) -> Error {
    Error::Decode {
        table: <R::Table as Table>::NAME,
        message,
    }
}

/// The name of the `at`th include's output.
fn include_name(at: usize) -> String {
    format!("__jazz_include_{at}")
}

/// Rows of `U` included with each row of `T`, as `O`.
#[derive(Debug, Clone)]
pub struct Include<T, O> {
    subquery: ArraySubquery,
    types: PhantomData<fn() -> (T, O)>,
}

impl<T, O> Include<T, O> {
    fn new(subquery: ArraySubquery) -> Self {
        Self {
            subquery,
            types: PhantomData,
        }
    }
}

/// What [`Query::include`] takes: a relation of `T`, or an [`Include`]
/// made from one.
pub trait IntoInclude<T> {
    /// What each row gets: `Option<U>`, `U` or `Vec<U>`.
    type Output: Included;
    #[doc(hidden)]
    fn into_include(self) -> Include<T, Self::Output>;
}

impl<T, O: Included> IntoInclude<T> for Include<T, O> {
    type Output = O;
    fn into_include(self) -> Self {
        self
    }
}

/// The child query of an include, as the core runs it. A typed query
/// only filters, orders, limits, selects and includes, as a child can.
fn subquery<S: Row>(query: Query<S>, inner_column: &str, outer_column: &str) -> ArraySubquery {
    let query = query.into_untyped();
    let mut subquery = ArraySubquery::new(
        String::new(),
        <S::Table as Table>::NAME,
        inner_column,
        outer_column,
    );
    subquery.filters = query.filters;
    // Included rows come as values in order: select every column by name,
    // so they decode by name.
    subquery.select = Some(query.select.unwrap_or_else(|| {
        <S::Table as Table>::COLUMNS
            .iter()
            .map(|&name| name.to_owned())
            .collect()
    }));
    subquery.order_by = query.order_by;
    subquery.limit = query.limit;
    subquery.offset = query.offset;
    subquery.nested_arrays = query.array_subqueries;
    subquery.partial = query.partial;
    subquery
}

impl<T: Table, U: Table, N: Nullability> One<T, U, N> {
    /// Includes the referenced row as `query` narrows and shapes it: an
    /// `Option`, unless [`OneInclude::require`]d.
    pub fn with<S: Row<Table = U>>(self, query: Query<S>) -> OneInclude<T, S, N> {
        OneInclude {
            subquery: subquery(query, "id", self.column()),
            types: PhantomData,
            nullability: PhantomData,
        }
    }

    /// Requires the referenced row, as TypeScript's `requireIncludes`
    /// does: see [`OneInclude::require`].
    pub fn require(self) -> Include<T, N::Required<U>> {
        self.with(Query::all()).require()
    }
}

impl<T: Table, U: Table, N: Nullability> IntoInclude<T> for One<T, U, N> {
    type Output = Option<U>;
    fn into_include(self) -> Include<T, Option<U>> {
        self.with(Query::all()).into_include()
    }
}

/// Whether a forward relation's column may be null, and so what requiring
/// its row includes.
pub trait Nullability: Copy + Send + 'static {
    /// What a required include of `S` is.
    type Required<S: Row>: Included;
    #[doc(hidden)]
    const REQUIREMENT: ArraySubqueryRequirement;
    #[doc(hidden)]
    const ARRAY_REQUIREMENT: ArraySubqueryRequirement;
}

/// The column always holds a reference: requiring its row includes the
/// row itself and keeps only the rows whose referenced row is there.
impl Nullability for NotNull {
    type Required<S: Row> = S;
    const REQUIREMENT: ArraySubqueryRequirement = ArraySubqueryRequirement::AtLeastOne;
    const ARRAY_REQUIREMENT: ArraySubqueryRequirement =
        ArraySubqueryRequirement::MatchCorrelationCardinality;
}

/// The column may be null: requiring its row changes nothing, as in
/// TypeScript. The include stays an `Option`, and every row is kept.
impl Nullability for Nullable {
    type Required<S: Row> = Option<S>;
    const REQUIREMENT: ArraySubqueryRequirement = ArraySubqueryRequirement::Optional;
    const ARRAY_REQUIREMENT: ArraySubqueryRequirement = ArraySubqueryRequirement::Optional;
}

/// A forward relation's include: an `Option<S>`, unless required.
#[derive(Debug, Clone)]
pub struct OneInclude<T, S, N = NotNull> {
    subquery: ArraySubquery,
    types: PhantomData<fn() -> (T, S)>,
    nullability: PhantomData<N>,
}

impl<T, S: Row, N: Nullability> OneInclude<T, S, N> {
    /// Requires the row, as TypeScript's `requireIncludes` does. On a
    /// [`NotNull`] relation the include is `S`, and only the rows whose
    /// referenced row is there (and readable) are kept. On a [`Nullable`]
    /// one it stays an `Option<S>` and every row is kept.
    pub fn require(self) -> Include<T, N::Required<S>> {
        Include::new(ArraySubquery {
            requirement: N::REQUIREMENT,
            ..self.subquery
        })
    }
}

impl<T, S: Row, N> IntoInclude<T> for OneInclude<T, S, N> {
    type Output = Option<S>;
    fn into_include(self) -> Include<T, Option<S>> {
        Include::new(self.subquery)
    }
}

impl<T: Table, U: Table, N: Nullability> Refs<T, U, N> {
    /// Includes the referenced rows as `query` narrows and shapes them.
    pub fn with<S: Row<Table = U>>(self, query: Query<S>) -> RefsInclude<T, S, N> {
        RefsInclude {
            subquery: subquery(query, "id", self.column()),
            types: PhantomData,
            nullability: PhantomData,
        }
    }

    /// Requires the referenced rows: see [`RefsInclude::require`].
    pub fn require(self) -> Include<T, Vec<U>> {
        self.with(Query::all()).require()
    }
}

impl<T: Table, U: Table, N: Nullability> IntoInclude<T> for Refs<T, U, N> {
    type Output = Vec<U>;
    fn into_include(self) -> Include<T, Vec<U>> {
        self.with(Query::all()).into_include()
    }
}

/// An array forward relation's include: a `Vec<S>` of the rows the array
/// references that the query matches.
#[derive(Debug, Clone)]
pub struct RefsInclude<T, S, N = NotNull> {
    subquery: ArraySubquery,
    types: PhantomData<fn() -> (T, S)>,
    nullability: PhantomData<N>,
}

impl<T, S: Row, N: Nullability> RefsInclude<T, S, N> {
    /// Requires the rows, as TypeScript's `requireIncludes` does. On a
    /// [`NotNull`] column only the rows whose every referenced row is
    /// there (and readable) are kept. On a [`Nullable`] one every row is
    /// kept. The include is a `Vec<S>` either way.
    pub fn require(self) -> Include<T, Vec<S>> {
        Include::new(ArraySubquery {
            requirement: N::ARRAY_REQUIREMENT,
            ..self.subquery
        })
    }
}

impl<T, S: Row, N> IntoInclude<T> for RefsInclude<T, S, N> {
    type Output = Vec<S>;
    fn into_include(self) -> Include<T, Vec<S>> {
        Include::new(self.subquery)
    }
}

impl<T: Table, U: Table> Many<T, U> {
    /// Includes the referencing rows as `query` narrows and shapes them.
    pub fn with<S: Row<Table = U>>(self, query: Query<S>) -> Include<T, Vec<S>> {
        Include::new(subquery(query, self.column(), "id"))
    }
}

impl<T: Table, U: Table> IntoInclude<T> for Many<T, U> {
    type Output = Vec<U>;
    fn into_include(self) -> Include<T, Vec<U>> {
        self.with(Query::all())
    }
}

impl<T, O> Include<T, O> {
    pub(crate) fn into_subquery(self, at: usize) -> ArraySubquery {
        let mut subquery = self.subquery;
        subquery.column_name = include_name(at);
        subquery
    }
}

/// A row with one more include after it: `T` becomes `(T, I)`, `(T, A)`
/// becomes `(T, A, I)`, and so on, up to eight includes.
pub trait With<I>: Row {
    type Output: Row<Table = Self::Table>;
}

impl<T: Table, I: Included> With<I> for T {
    type Output = (T, I);
}

impl<T: Table, V: Values, I: Included> With<I> for Partial<T, V> {
    type Output = (Partial<T, V>, I);
}

macro_rules! includes {
    ($($include:ident $at:tt),+; $($next:ident)?) => {
        impl<R0: Base, $($include: Included),+> Row for (R0, $($include,)+) {
            type Table = R0::Table;

            fn row_id(&self) -> ObjectId {
                self.0.row_id()
            }

            fn decode_fields(fields: &mut Fields<'_>) -> Result<Self> {
                let base = R0::decode_fields(fields)?;
                let implementations = fields.implementations();
                Ok((
                    base,
                    $($include::decode_included(
                        fields.take_raw(&include_name($at - 1))?,
                        implementations,
                    )?,)+
                ))
            }

            fn value_names() -> Vec<String> {
                let mut names = R0::value_names();
                $(names.push(include_name($at - 1));)+
                names
            }
        }

        includes!(@with $($include $at),+; $($next)?);
    };
    (@with $($include:ident $at:tt),+; ) => {};
    (@with $($include:ident $at:tt),+; $next:ident) => {
        impl<R0: Base, $($include: Included,)+ $next: Included> With<$next> for (R0, $($include,)+) {
            type Output = (R0, $($include,)+ $next);
        }
    };
}

includes!(I1 1; I2);
includes!(I1 1, I2 2; I3);
includes!(I1 1, I2 2, I3 3; I4);
includes!(I1 1, I2 2, I3 3, I4 4; I5);
includes!(I1 1, I2 2, I3 3, I4 4, I5 5; I6);
includes!(I1 1, I2 2, I3 3, I4 4, I5 5, I6 6; I7);
includes!(I1 1, I2 2, I3 3, I4 4, I5 5, I6 6, I7 7; I8);
includes!(I1 1, I2 2, I3 3, I4 4, I5 5, I6 6, I7 7, I8 8; );
