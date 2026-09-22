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
    Select(Box<Select>),
    Set(Box<SetQuery>),
    With(Box<WithQuery>),
}

/// WITH wrapper around one or more common table expressions.
#[derive(Clone, Debug, PartialEq)]
pub struct WithQuery {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
    pub query: Query,
}

impl WithQuery {
    pub fn new(ctes: impl IntoIterator<Item = Cte>, query: Query) -> Self {
        Self {
            recursive: false,
            ctes: ctes.into_iter().collect(),
            query,
        }
    }

    pub fn recursive(mut self) -> Self {
        self.recursive = true;
        self
    }
}

/// Common table expression definition, including optional column aliases.
#[derive(Clone, Debug, PartialEq)]
pub struct Cte {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Query,
    pub materialized: CteMaterialization,
}

impl Cte {
    pub fn new(name: impl Into<String>, query: Query) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            query,
            materialized: CteMaterialization::Unspecified,
        }
    }

    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    pub fn materialized(mut self) -> Self {
        self.materialized = CteMaterialization::Materialized;
        self
    }

    pub fn not_materialized(mut self) -> Self {
        self.materialized = CteMaterialization::NotMaterialized;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CteMaterialization {
    Unspecified,
    Materialized,
    NotMaterialized,
}

/// SQL SELECT core, intentionally broader than what the planner lowers today.
#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub quantifier: SelectQuantifier,
    pub projection: Vec<SelectItem>,
    pub from: Vec<TableRef>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

impl Select {
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

    pub fn from(mut self, from: impl IntoIterator<Item = TableRef>) -> Self {
        self.from = from.into_iter().collect();
        self
    }

    pub fn where_(mut self, predicate: Expr) -> Self {
        self.selection = Some(predicate);
        self
    }

    pub fn group_by(mut self, expressions: impl IntoIterator<Item = Expr>) -> Self {
        self.group_by = expressions.into_iter().collect();
        self
    }

    pub fn having(mut self, predicate: Expr) -> Self {
        self.having = Some(predicate);
        self
    }

    pub fn order_by(mut self, expressions: impl IntoIterator<Item = OrderByExpr>) -> Self {
        self.order_by = expressions.into_iter().collect();
        self
    }

    pub fn limit(mut self, limit: Expr) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: Expr) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn distinct(mut self) -> Self {
        self.quantifier = SelectQuantifier::Distinct;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectQuantifier {
    All,
    Distinct,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    Expr { expr: Expr, alias: Option<String> },
    Wildcard,
    QualifiedWildcard(Vec<String>),
}

impl SelectItem {
    pub fn expr(expr: Expr) -> Self {
        Self::Expr { expr, alias: None }
    }

    pub fn aliased(expr: Expr, alias: impl Into<String>) -> Self {
        Self::Expr {
            expr,
            alias: Some(alias.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableRef {
    Named {
        name: ObjectName,
        alias: Option<TableAlias>,
    },
    Derived {
        lateral: bool,
        query: Query,
        alias: Option<TableAlias>,
    },
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
}

impl TableRef {
    pub fn named(name: impl Into<String>) -> Self {
        Self::Named {
            name: ObjectName::single(name),
            alias: None,
        }
    }

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

/// Table alias plus optional column-renaming list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

impl TableAlias {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }
}

/// Potentially-qualified SQL object name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName(pub Vec<String>);

impl ObjectName {
    pub fn single(name: impl Into<String>) -> Self {
        Self(vec![name.into()])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
    Natural,
    None,
}

/// Binary set operation between two queries.
#[derive(Clone, Debug, PartialEq)]
pub struct SetQuery {
    pub left: Query,
    pub op: SetOperator,
    pub right: Query,
    pub quantifier: SetQuantifier,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetQuantifier {
    All,
    Distinct,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Literal(Value),
    Null,
    Column(ColumnRef),
    Parameter(String),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        query: Query,
        negated: bool,
    },
    Exists {
        query: Query,
        negated: bool,
    },
    Function(FunctionCall),
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: QueryDataType,
    },
    Subquery(Query),
    CorrelatedSubquery(CorrelatedSubquery),
}

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(ColumnRef::unqualified(name))
    }

    pub fn parameter(name: impl Into<String>) -> Self {
        Self::Parameter(name.into())
    }

    pub fn binary(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
}

/// Possibly-qualified column reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnRef {
    pub qualifier: Vec<String>,
    pub name: String,
}

impl ColumnRef {
    pub fn unqualified(name: impl Into<String>) -> Self {
        Self {
            qualifier: Vec::new(),
            name: name.into(),
        }
    }

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    Plus,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
    NotLike,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

/// Function call expression, including SQL filter/window adornments.
#[derive(Clone, Debug, PartialEq)]
pub struct FunctionCall {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub over: Option<WindowSpec>,
}

impl FunctionCall {
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

#[derive(Clone, Debug, PartialEq)]
pub enum FunctionArg {
    Expr(Expr),
    Wildcard,
}

/// Window specification attached to an analytic function call.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

/// Subquery plus the outer references it captures.
#[derive(Clone, Debug, PartialEq)]
pub struct CorrelatedSubquery {
    pub query: Query,
    pub outer_refs: Vec<ColumnRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryDataType {
    U8,
    U16,
    U32,
    U64,
    F64,
    Bool,
    String,
    Bytes,
}

/// ORDER BY expression with direction and NULL ordering.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub direction: SortDirection,
    pub nulls: NullsOrder,
}

impl OrderByExpr {
    pub fn asc(expr: Expr) -> Self {
        Self {
            expr,
            direction: SortDirection::Asc,
            nulls: NullsOrder::Unspecified,
        }
    }

    pub fn desc(expr: Expr) -> Self {
        Self {
            expr,
            direction: SortDirection::Desc,
            nulls: NullsOrder::Unspecified,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
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
