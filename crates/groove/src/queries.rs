//! SQL-ish query AST accepted by the Groove facade.
//!
//! This module owns the user-facing structure of `SELECT`, `WITH`, joins,
//! predicates, projections, ordering, and set operations. It intentionally does
//! not resolve names, check schema types, optimize, or execute anything; those
//! responsibilities live in [`crate::ivm::planner`] and
//! [`crate::ivm::runtime`]. Builder helpers here preserve enough syntax shape
//! for tests and callers while leaving lowering decisions to the planner.

use crate::records::Value;

/// A SELECT query with an optional WITH clause or set operation wrapper.
#[derive(Clone, Debug, PartialEq)]
pub enum Query {
    /// A plain `SELECT ...`.
    Select(Box<Select>),
    /// Two queries combined with `UNION` / `EXCEPT` / `INTERSECT`.
    Set(Box<SetQuery>),
    /// A query preceded by a `WITH [RECURSIVE] ...` clause.
    With(Box<WithQuery>),
}

/// WITH wrapper around one or more common table expressions.
#[derive(Clone, Debug, PartialEq)]
pub struct WithQuery {
    /// `true` for `WITH RECURSIVE`, allowing a CTE to refer to itself.
    pub recursive: bool,
    /// The named subqueries, in declaration order; later CTEs may refer to
    /// earlier ones.
    pub ctes: Vec<Cte>,
    /// The main query that runs with the CTE names in scope.
    pub query: Query,
}

impl WithQuery {
    /// Builds a non-recursive `WITH` wrapper.
    ///
    /// * `ctes` — the named subqueries.
    /// * `query` — the main query using them.
    pub fn new(ctes: impl IntoIterator<Item = Cte>, query: Query) -> Self {
        Self {
            recursive: false,
            ctes: ctes.into_iter().collect(),
            query,
        }
    }

    /// Marks the clause as `WITH RECURSIVE` (builder style).
    pub fn recursive(mut self) -> Self {
        self.recursive = true;
        self
    }
}

/// Common table expression definition, including optional column aliases.
///
/// One `name [(columns...)] AS [MATERIALIZED] (query)` entry of a `WITH`
/// clause.
#[derive(Clone, Debug, PartialEq)]
pub struct Cte {
    /// The name the main query refers to this subquery by.
    pub name: String,
    /// Optional column aliases; empty means "use the subquery's own names".
    pub columns: Vec<String>,
    /// The subquery itself.
    pub query: Query,
    /// The `MATERIALIZED` / `NOT MATERIALIZED` hint, if any.
    pub materialized: CteMaterialization,
}

impl Cte {
    /// Builds a CTE with no column aliases and no materialization hint.
    ///
    /// * `name` — the CTE's name, for example `"recent"`.
    /// * `query` — the subquery it stands for.
    pub fn new(name: impl Into<String>, query: Query) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            query,
            materialized: CteMaterialization::Unspecified,
        }
    }

    /// Sets the column aliases, as in `recent(id, title) AS (...)`.
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Adds the `MATERIALIZED` hint (builder style).
    pub fn materialized(mut self) -> Self {
        self.materialized = CteMaterialization::Materialized;
        self
    }

    /// Adds the `NOT MATERIALIZED` hint (builder style).
    pub fn not_materialized(mut self) -> Self {
        self.materialized = CteMaterialization::NotMaterialized;
        self
    }
}

/// The `MATERIALIZED` hint on a CTE: whether the planner should compute the
/// CTE once and reuse it, or is free to inline it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CteMaterialization {
    /// No hint given; the planner decides.
    Unspecified,
    /// `AS MATERIALIZED (...)`.
    Materialized,
    /// `AS NOT MATERIALIZED (...)`.
    NotMaterialized,
}

/// SQL SELECT core, intentionally broader than what the planner lowers today.
///
/// Each field is one clause of the statement. Built with the chained helpers:
///
/// ```
/// use groove::queries::{BinaryOp, Expr, Select, SelectItem, TableRef};
/// use groove::records::Value;
///
/// // SELECT title AS album_title FROM albums WHERE id > 10
/// let select = Select::new([SelectItem::aliased(Expr::column("title"), "album_title")])
///     .from([TableRef::named("albums")])
///     .where_(Expr::binary(
///         Expr::column("id"),
///         BinaryOp::Gt,
///         Expr::Literal(Value::U64(10)),
///     ));
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    /// `ALL` (the default) or `DISTINCT`.
    pub quantifier: SelectQuantifier,
    /// The `SELECT` list: what each output column is.
    pub projection: Vec<SelectItem>,
    /// The `FROM` clause; multiple entries mean an implicit cross join.
    pub from: Vec<TableRef>,
    /// The `WHERE` predicate, if any.
    pub selection: Option<Expr>,
    /// The `GROUP BY` expressions, if any.
    pub group_by: Vec<Expr>,
    /// The `HAVING` predicate (filters groups), if any.
    pub having: Option<Expr>,
    /// The `ORDER BY` list, if any.
    pub order_by: Vec<OrderByExpr>,
    /// The `LIMIT` expression, if any.
    pub limit: Option<Expr>,
    /// The `OFFSET` expression, if any.
    pub offset: Option<Expr>,
}

impl Select {
    /// Starts a `SELECT` from its projection list; every other clause is
    /// empty until set with the builder methods below.
    pub fn new(projection: impl IntoIterator<Item = SelectItem>) -> Self {
        Self {
            quantifier: SelectQuantifier::All,
            projection: projection.into_iter().collect(),
            from: Vec::new(),
            selection: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Sets the `FROM` clause.
    pub fn from(mut self, from: impl IntoIterator<Item = TableRef>) -> Self {
        self.from = from.into_iter().collect();
        self
    }

    /// Sets the `WHERE` predicate. (Named `where_` because `where` is a Rust
    /// keyword.)
    pub fn where_(mut self, predicate: Expr) -> Self {
        self.selection = Some(predicate);
        self
    }

    /// Sets the `GROUP BY` expressions.
    pub fn group_by(mut self, expressions: impl IntoIterator<Item = Expr>) -> Self {
        self.group_by = expressions.into_iter().collect();
        self
    }

    /// Sets the `HAVING` predicate.
    pub fn having(mut self, predicate: Expr) -> Self {
        self.having = Some(predicate);
        self
    }

    /// Sets the `ORDER BY` list.
    pub fn order_by(mut self, expressions: impl IntoIterator<Item = OrderByExpr>) -> Self {
        self.order_by = expressions.into_iter().collect();
        self
    }

    /// Sets the `LIMIT` expression.
    pub fn limit(mut self, limit: Expr) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the `OFFSET` expression.
    pub fn offset(mut self, offset: Expr) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Switches the query to `SELECT DISTINCT`.
    pub fn distinct(mut self) -> Self {
        self.quantifier = SelectQuantifier::Distinct;
        self
    }
}

/// `ALL` vs `DISTINCT` on a `SELECT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectQuantifier {
    /// Keep duplicate rows (the SQL default).
    All,
    /// Collapse duplicate rows.
    Distinct,
}

/// One entry of the `SELECT` list.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    /// An expression, optionally renamed: `title` or `title AS album_title`.
    Expr { expr: Expr, alias: Option<String> },
    /// `*` — every column of every table in scope.
    Wildcard,
    /// `albums.*` — every column of one qualified name.
    QualifiedWildcard(Vec<String>),
}

impl SelectItem {
    /// An unaliased expression item.
    pub fn expr(expr: Expr) -> Self {
        Self::Expr { expr, alias: None }
    }

    /// An expression item with an `AS alias`.
    pub fn aliased(expr: Expr, alias: impl Into<String>) -> Self {
        Self::Expr {
            expr,
            alias: Some(alias.into()),
        }
    }
}

/// One source in the `FROM` clause: a table, a subquery, or a join tree.
#[derive(Clone, Debug, PartialEq)]
pub enum TableRef {
    /// A table (or CTE) referenced by name: `albums` or `albums AS a`.
    Named {
        name: ObjectName,
        alias: Option<TableAlias>,
    },
    /// A subquery in `FROM`: `(SELECT ...) AS d`, optionally `LATERAL` (able
    /// to see columns of tables to its left).
    Derived {
        lateral: bool,
        query: Query,
        alias: Option<TableAlias>,
    },
    /// Two sources joined together; joins nest, so the left side can itself
    /// be a `Join`.
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
}

impl TableRef {
    /// References a table by its bare (unqualified) name.
    pub fn named(name: impl Into<String>) -> Self {
        Self::Named {
            name: ObjectName::single(name),
            alias: None,
        }
    }

    /// Attaches an `AS alias`. On a `Join` this is a no-op: SQL puts aliases
    /// on the joined sides, not on the join itself.
    pub fn aliased(self, alias: impl Into<String>) -> Self {
        match self {
            Self::Named { name, .. } => Self::Named {
                name,
                alias: Some(TableAlias::new(alias)),
            },
            Self::Derived { lateral, query, .. } => Self::Derived {
                lateral,
                query,
                alias: Some(TableAlias::new(alias)),
            },
            Self::Join { .. } => self,
        }
    }
}

/// Table alias plus optional column-renaming list, as in
/// `albums AS a(album_id, album_title)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableAlias {
    /// The alias name (`a`).
    pub name: String,
    /// Optional column renames; empty keeps the original column names.
    pub columns: Vec<String>,
}

impl TableAlias {
    /// An alias with no column renames.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    /// Sets the column renames (builder style).
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }
}

/// Potentially-qualified SQL object name: `albums` is `["albums"]`,
/// `main.albums` is `["main", "albums"]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName(pub Vec<String>);

impl ObjectName {
    /// A one-part (unqualified) name.
    pub fn single(name: impl Into<String>) -> Self {
        Self(vec![name.into()])
    }
}

/// Which rows a join keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinKind {
    /// `INNER JOIN`: only matching left/right pairs.
    Inner,
    /// `LEFT JOIN`: every left row, with NULLs when nothing matches.
    Left,
    /// `RIGHT JOIN`: every right row, with NULLs when nothing matches.
    Right,
    /// `FULL JOIN`: every row from both sides.
    Full,
    /// `CROSS JOIN`: every left/right combination, no condition.
    Cross,
    /// Semi join: left rows that have at least one match; right columns are
    /// not produced. (The relational form of `WHERE EXISTS (...)`.)
    Semi,
    /// Anti join: left rows that have *no* match. (The relational form of
    /// `WHERE NOT EXISTS (...)`.)
    Anti,
}

/// How the joined rows are matched.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinConstraint {
    /// `ON <predicate>`.
    On(Expr),
    /// `USING (a, b)`: equality on the named columns, which appear once in
    /// the output.
    Using(Vec<String>),
    /// `NATURAL`: equality on every column name the two sides share.
    Natural,
    /// No constraint, as in `CROSS JOIN`.
    None,
}

/// Binary set operation between two queries, as in
/// `SELECT ... UNION ALL SELECT ...`.
#[derive(Clone, Debug, PartialEq)]
pub struct SetQuery {
    /// The left operand.
    pub left: Query,
    /// Which set operation combines the two sides.
    pub op: SetOperator,
    /// The right operand.
    pub right: Query,
    /// `ALL` keeps duplicates, `DISTINCT` collapses them.
    pub quantifier: SetQuantifier,
}

/// The set operation of a [`SetQuery`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetOperator {
    /// `UNION`: rows from either side.
    Union,
    /// `EXCEPT`: rows of the left side not present on the right.
    Except,
    /// `INTERSECT`: rows present on both sides.
    Intersect,
}

/// `ALL` vs `DISTINCT` on a set operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetQuantifier {
    /// Keep duplicates (`UNION ALL`, ...).
    All,
    /// Collapse duplicates (plain SQL `UNION`, ...).
    Distinct,
}

/// A scalar SQL expression, used in projections, predicates, and clauses.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// A constant, for example `10` or `'Yellow'`.
    Literal(Value),
    /// The SQL `NULL` literal.
    Null,
    /// A column reference, for example `title` or `albums.title`.
    Column(ColumnRef),
    /// A named placeholder (`:name`) bound at execution time; this is what
    /// prepared shapes parameterize over.
    Parameter(String),
    /// A one-operand operator, for example `NOT x` or `x IS NULL`.
    Unary { op: UnaryOp, expr: Box<Expr> },
    /// A two-operand operator, for example `id > 10` or `a AND b`.
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// `expr [NOT] BETWEEN low AND high` (bounds inclusive).
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// `expr [NOT] IN (a, b, c)` with a literal list.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `expr [NOT] IN (SELECT ...)`.
    InSubquery {
        expr: Box<Expr>,
        query: Query,
        negated: bool,
    },
    /// `[NOT] EXISTS (SELECT ...)`.
    Exists { query: Query, negated: bool },
    /// A function call, plain (`lower(title)`) or aggregate (`count(*)`).
    Function(FunctionCall),
    /// `CASE [operand] WHEN ... THEN ... [ELSE ...] END`.
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    /// `CAST(expr AS type)`.
    Cast {
        expr: Box<Expr>,
        data_type: QueryDataType,
    },
    /// A scalar subquery: `(SELECT ...)` used as a value.
    Subquery(Query),
    /// A scalar subquery that reads columns of the outer query; the captured
    /// outer columns are listed explicitly (see [`CorrelatedSubquery`]).
    CorrelatedSubquery(CorrelatedSubquery),
}

impl Expr {
    /// An unqualified column reference: `Expr::column("title")` is `title`.
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(ColumnRef::unqualified(name))
    }

    /// A named placeholder: `Expr::parameter("min_id")` is `:min_id`.
    pub fn parameter(name: impl Into<String>) -> Self {
        Self::Parameter(name.into())
    }

    /// A binary operation, boxing both operands.
    pub fn binary(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
}

/// Possibly-qualified column reference: `title` has an empty qualifier,
/// `albums.title` has qualifier `["albums"]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnRef {
    /// The table/alias path in front of the name; empty when unqualified.
    pub qualifier: Vec<String>,
    /// The column name itself.
    pub name: String,
}

impl ColumnRef {
    /// A bare column name with no qualifier.
    pub fn unqualified(name: impl Into<String>) -> Self {
        Self {
            qualifier: Vec::new(),
            name: name.into(),
        }
    }

    /// A qualified reference, for example
    /// `ColumnRef::qualified(["albums"], "title")` for `albums.title`.
    pub fn qualified(
        qualifier: impl IntoIterator<Item = impl Into<String>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            qualifier: qualifier.into_iter().map(Into::into).collect(),
            name: name.into(),
        }
    }
}

/// One-operand SQL operators.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// Logical `NOT x`.
    Not,
    /// Arithmetic negation `-x`.
    Neg,
    /// Unary plus `+x` (a no-op, kept for syntax fidelity).
    Plus,
    /// `x IS NULL`.
    IsNull,
    /// `x IS NOT NULL`.
    IsNotNull,
}

/// Two-operand SQL operators.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    /// `=`
    Eq,
    /// `<>`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// Logical `AND`.
    And,
    /// Logical `OR`.
    Or,
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%` (remainder).
    Mod,
    /// `LIKE` pattern match.
    Like,
    /// `NOT LIKE`.
    NotLike,
    /// `IS DISTINCT FROM`: like `<>` but treats two NULLs as equal.
    IsDistinctFrom,
    /// `IS NOT DISTINCT FROM`: like `=` but NULL-safe.
    IsNotDistinctFrom,
}

/// Function call expression, including SQL filter/window adornments.
#[derive(Clone, Debug, PartialEq)]
pub struct FunctionCall {
    /// The function's name, for example `count` or `lower`.
    pub name: ObjectName,
    /// The arguments, in order.
    pub args: Vec<FunctionArg>,
    /// `true` for `count(DISTINCT x)`-style calls.
    pub distinct: bool,
    /// An aggregate's `FILTER (WHERE ...)` predicate, if any.
    pub filter: Option<Box<Expr>>,
    /// The `OVER (...)` window, making this an analytic call, if any.
    pub over: Option<WindowSpec>,
}

impl FunctionCall {
    /// A plain call with no `DISTINCT`, `FILTER`, or `OVER`.
    ///
    /// * `name` — the function name, unqualified.
    /// * `args` — the arguments; `count(*)` is
    ///   `FunctionCall::new("count", [FunctionArg::Wildcard])`.
    pub fn new(name: impl Into<String>, args: impl IntoIterator<Item = FunctionArg>) -> Self {
        Self {
            name: ObjectName::single(name),
            args: args.into_iter().collect(),
            distinct: false,
            filter: None,
            over: None,
        }
    }
}

/// One argument of a function call.
#[derive(Clone, Debug, PartialEq)]
pub enum FunctionArg {
    /// An ordinary expression argument.
    Expr(Expr),
    /// The `*` argument of `count(*)`.
    Wildcard,
}

/// Window specification attached to an analytic function call.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    /// `PARTITION BY` expressions: rows are windowed within each partition.
    pub partition_by: Vec<Expr>,
    /// `ORDER BY` inside the window.
    pub order_by: Vec<OrderByExpr>,
}

/// Subquery plus the outer references it captures.
///
/// Listing `outer_refs` explicitly saves the planner from re-discovering
/// which outer columns the subquery depends on.
#[derive(Clone, Debug, PartialEq)]
pub struct CorrelatedSubquery {
    /// The inner query.
    pub query: Query,
    /// The outer-query columns the inner query reads.
    pub outer_refs: Vec<ColumnRef>,
}

/// Target type of a `CAST(expr AS type)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryDataType {
    /// Unsigned 8-bit integer.
    U8,
    /// Unsigned 16-bit integer.
    U16,
    /// Unsigned 32-bit integer.
    U32,
    /// Unsigned 64-bit integer.
    U64,
    /// 64-bit float.
    F64,
    /// `true`/`false`.
    Bool,
    /// UTF-8 text.
    String,
    /// Raw bytes.
    Bytes,
}

/// ORDER BY expression with direction and NULL ordering.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByExpr {
    /// What to sort by.
    pub expr: Expr,
    /// Ascending or descending.
    pub direction: SortDirection,
    /// Where NULLs go, when stated explicitly.
    pub nulls: NullsOrder,
}

impl OrderByExpr {
    /// `expr ASC` with unspecified NULL placement.
    pub fn asc(expr: Expr) -> Self {
        Self {
            expr,
            direction: SortDirection::Asc,
            nulls: NullsOrder::Unspecified,
        }
    }

    /// `expr DESC` with unspecified NULL placement.
    pub fn desc(expr: Expr) -> Self {
        Self {
            expr,
            direction: SortDirection::Desc,
            nulls: NullsOrder::Unspecified,
        }
    }
}

/// `ASC` vs `DESC`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortDirection {
    /// Smallest first.
    Asc,
    /// Largest first.
    Desc,
}

/// `NULLS FIRST` / `NULLS LAST` on an `ORDER BY` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullsOrder {
    /// `NULLS FIRST`.
    First,
    /// `NULLS LAST`.
    Last,
    /// Not stated; the engine's default applies.
    Unspecified,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_builder_accumulates_common_clauses() {
        let select = Select::new([SelectItem::aliased(Expr::column("title"), "album_title")])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::column("id"),
                BinaryOp::Gt,
                Expr::Literal(Value::U64(10)),
            ))
            .group_by([Expr::column("artist_id")])
            .having(Expr::binary(
                Expr::column("count"),
                BinaryOp::GtEq,
                Expr::Literal(Value::U64(2)),
            ))
            .order_by([OrderByExpr::desc(Expr::column("title"))])
            .limit(Expr::Literal(Value::U64(5)))
            .offset(Expr::Literal(Value::U64(10)))
            .distinct();

        assert_eq!(select.quantifier, SelectQuantifier::Distinct);
        assert_eq!(select.projection.len(), 1);
        assert_eq!(select.from, [TableRef::named("albums")]);
        assert!(select.selection.is_some());
        assert_eq!(select.group_by, [Expr::column("artist_id")]);
        assert!(select.having.is_some());
        assert_eq!(select.order_by[0].direction, SortDirection::Desc);
        assert_eq!(select.limit, Some(Expr::Literal(Value::U64(5))));
        assert_eq!(select.offset, Some(Expr::Literal(Value::U64(10))));
    }

    #[test]
    fn cte_builder_tracks_columns_materialization_and_recursion() {
        let cte = Cte::new(
            "recent",
            Query::Select(Box::new(
                Select::new([SelectItem::Wildcard]).from([TableRef::named("albums")]),
            )),
        )
        .with_columns(["id", "title"])
        .materialized();
        let with = WithQuery::new(
            [cte.clone()],
            Query::Select(Box::new(
                Select::new([SelectItem::Wildcard]).from([TableRef::named("recent")]),
            )),
        )
        .recursive();

        assert_eq!(cte.columns, ["id", "title"]);
        assert_eq!(cte.materialized, CteMaterialization::Materialized);
        assert!(with.recursive);
        assert_eq!(with.ctes, [cte]);
    }

    #[test]
    fn table_ref_aliases_named_and_derived_tables() {
        let named = TableRef::named("albums").aliased("a");
        let derived = TableRef::Derived {
            lateral: true,
            query: Query::Select(Box::new(Select::new([SelectItem::Wildcard]))),
            alias: None,
        }
        .aliased("d");

        assert!(matches!(
            named,
            TableRef::Named {
                alias: Some(TableAlias { name, .. }),
                ..
            } if name == "a"
        ));
        assert!(matches!(
            derived,
            TableRef::Derived {
                lateral: true,
                alias: Some(TableAlias { name, .. }),
                ..
            } if name == "d"
        ));
    }

    #[test]
    fn expression_and_order_builders_create_expected_nodes() {
        assert_eq!(
            Expr::binary(
                Expr::column("id"),
                BinaryOp::Eq,
                Expr::Literal(Value::U64(1))
            ),
            Expr::Binary {
                left: Box::new(Expr::Column(ColumnRef::unqualified("id"))),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Value::U64(1))),
            }
        );
        assert_eq!(
            ColumnRef::qualified(["album", "artist"], "name").qualifier,
            ["album", "artist"]
        );
        assert_eq!(
            FunctionCall::new("count", [FunctionArg::Wildcard]).name,
            ObjectName::single("count")
        );
        assert_eq!(
            OrderByExpr::asc(Expr::column("title")).nulls,
            NullsOrder::Unspecified
        );
    }
}
