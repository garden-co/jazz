use std::fmt;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArguments, GroupByExpr, Ident, LimitClause, ObjectName,
    OrderByKind, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Value as AstValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::MAX_RESULT_COLUMNS;

const MAX_SQL_TOKENS: usize = 2_048;
const MAX_EXPRESSION_DEPTH: usize = 64;
const MAX_PREFIX_OPERATOR_DEPTH: usize = 64;
const MAX_FILTER_NODES: usize = 512;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParsedStatement {
    Select(SelectPlan),
    Command(Command),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Command {
    Show(String),
    Begin,
    Commit,
    Rollback,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectPlan {
    pub(crate) source: SelectSource,
    pub(crate) projection: Vec<Projection>,
    pub(crate) filter: Option<FilterExpr>,
    pub(crate) order_by: Vec<OrderTerm>,
    pub(crate) limit: Option<PageValue>,
    pub(crate) offset: PageValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SelectSource {
    Table(String),
    Databases,
    Tables,
    Columns,
    Session,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Projection {
    pub(crate) expr: ProjectedExpr,
    pub(crate) alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjectedExpr {
    Wildcard,
    Column(String),
    SessionFunction(SessionFunction),
    Literal(SqlLiteral),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionFunction {
    Version,
    CurrentDatabase,
    CurrentSchema,
    CurrentUser,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FilterExpr {
    Compare {
        column: String,
        op: CompareOp,
        literal: SqlLiteral,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    In {
        column: String,
        values: Vec<SqlLiteral>,
        negated: bool,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SqlLiteral {
    String(String),
    Number(String),
    Boolean(bool),
    Null,
    Placeholder(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderTerm {
    pub(crate) column: String,
    pub(crate) ascending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PageValue {
    Literal(usize),
    Placeholder(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlError(pub(crate) String);

impl fmt::Display for SqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SqlError {}

pub(crate) fn parse_sql(sql: &str) -> Result<ParsedStatement, SqlError> {
    let mut statements = parse_sql_batch(sql)?;
    if statements.len() != 1 {
        return Err(SqlError(
            "exactly one SQL statement is supported per request".to_owned(),
        ));
    }
    Ok(statements.remove(0))
}

pub(crate) fn parse_sql_batch(sql: &str) -> Result<Vec<ParsedStatement>, SqlError> {
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| SqlError(format!("invalid SQL: {error}")))?;
    if tokens.len() > MAX_SQL_TOKENS {
        return Err(SqlError(format!(
            "SQL cannot contain more than {MAX_SQL_TOKENS} tokens"
        )));
    }
    let mut nesting = 0_usize;
    let mut prefix_operators = 0_usize;
    for token in &tokens {
        match token {
            Token::LParen => {
                nesting += 1;
                if nesting > MAX_EXPRESSION_DEPTH {
                    return Err(SqlError(format!(
                        "SQL nesting cannot exceed {MAX_EXPRESSION_DEPTH} levels"
                    )));
                }
            }
            Token::RParen => nesting = nesting.saturating_sub(1),
            _ => {}
        }
        match token {
            Token::Whitespace(_) => {}
            Token::Plus
            | Token::Minus
            | Token::Word(sqlparser::tokenizer::Word {
                keyword: Keyword::NOT,
                ..
            }) => {
                prefix_operators += 1;
                if prefix_operators > MAX_PREFIX_OPERATOR_DEPTH {
                    return Err(SqlError(format!(
                        "SQL prefix operators cannot exceed {MAX_PREFIX_OPERATOR_DEPTH} levels"
                    )));
                }
            }
            _ => prefix_operators = 0,
        }
    }
    Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_statements()
        .map_err(|error| SqlError(format!("invalid SQL: {error}")))?
        .into_iter()
        .map(parse_statement)
        .collect()
}

fn parse_statement(statement: Statement) -> Result<ParsedStatement, SqlError> {
    match statement {
        Statement::Query(query) => parse_query(*query).map(ParsedStatement::Select),
        Statement::ShowVariable { variable } => Ok(ParsedStatement::Command(Command::Show(
            variable
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join("."),
        ))),
        Statement::Set(_) => Err(unsupported(
            "SET (session settings are not applied by this read-only interface)",
        )),
        Statement::StartTransaction {
            modes,
            modifier,
            statements,
            exception,
            has_end_keyword,
            ..
        } if modes.is_empty()
            && modifier.is_none()
            && statements.is_empty()
            && exception.is_none()
            && !has_end_keyword =>
        {
            Ok(ParsedStatement::Command(Command::Begin))
        }
        Statement::StartTransaction { .. } => Err(unsupported(
            "transaction modes, modifiers, and procedural BEGIN blocks",
        )),
        Statement::Commit {
            chain: false,
            end: false,
            modifier: None,
        } => Ok(ParsedStatement::Command(Command::Commit)),
        Statement::Commit { .. } => Err(unsupported("COMMIT modifiers and chaining")),
        Statement::Rollback {
            chain: false,
            savepoint: None,
        } => Ok(ParsedStatement::Command(Command::Rollback)),
        Statement::Rollback { .. } => {
            Err(unsupported("ROLLBACK savepoints, modifiers, and chaining"))
        }
        other => Err(SqlError(format!(
            "unsupported read-only PostgreSQL statement: {other}"
        ))),
    }
}

fn parse_query(query: sqlparser::ast::Query) -> Result<SelectPlan, SqlError> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(unsupported("CTEs, locks, fetch, and query extensions"));
    }

    let SetExpr::Select(select) = *query.body else {
        return Err(unsupported("set operations and nested queries"));
    };
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.prewhere.is_some()
        || !group_by_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || !select.connect_by.is_empty()
        || !select.lateral_views.is_empty()
    {
        return Err(unsupported(
            "DISTINCT, grouping, windows, and SELECT extensions",
        ));
    }

    let source = parse_source(&select.from)?;
    if select.projection.len() > MAX_RESULT_COLUMNS {
        return Err(SqlError(format!(
            "SELECT cannot return more than {MAX_RESULT_COLUMNS} columns"
        )));
    }
    let projection = select
        .projection
        .iter()
        .map(|item| parse_projection(item, &source))
        .collect::<Result<Vec<_>, _>>()?;
    if projection.is_empty() {
        return Err(SqlError("SELECT projection cannot be empty".to_owned()));
    }
    if let Some(filter) = &select.selection {
        validate_filter_complexity(filter)?;
    }
    let filter = select.selection.as_ref().map(parse_filter).transpose()?;
    let order_by = parse_order_by(query.order_by.as_ref())?;
    let (limit, offset) = parse_limit(query.limit_clause.as_ref())?;

    Ok(SelectPlan {
        source,
        projection,
        filter,
        order_by,
        limit,
        offset,
    })
}

fn validate_filter_complexity(root: &Expr) -> Result<(), SqlError> {
    let mut stack = vec![(root, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((expr, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_FILTER_NODES {
            return Err(SqlError(format!(
                "WHERE expressions cannot contain more than {MAX_FILTER_NODES} nodes"
            )));
        }
        if depth > MAX_EXPRESSION_DEPTH {
            return Err(SqlError(format!(
                "WHERE expressions cannot exceed {MAX_EXPRESSION_DEPTH} levels"
            )));
        }
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                stack.push((right, depth + 1));
                stack.push((left, depth + 1));
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Nested(expr) => stack.push((expr, depth + 1)),
            Expr::InList { expr, list, .. } => {
                stack.push((expr, depth + 1));
                stack.extend(list.iter().map(|item| (item, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_source(from: &[sqlparser::ast::TableWithJoins]) -> Result<SelectSource, SqlError> {
    if from.is_empty() {
        return Ok(SelectSource::Session);
    }
    if from.len() != 1 || !from[0].joins.is_empty() {
        return Err(unsupported("joins and multiple FROM relations"));
    }
    let TableFactor::Table {
        name,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from[0].relation
    else {
        return Err(unsupported("derived tables and table functions"));
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(unsupported("table functions, hints, and table versions"));
    }

    let parts = object_name_parts(name)?;
    match parts.as_slice() {
        [table] if table == "pg_database" => Ok(SelectSource::Databases),
        [table] => Ok(SelectSource::Table(table.clone())),
        [schema, table] if schema == "public" => Ok(SelectSource::Table(table.clone())),
        [schema, table] if schema == "pg_catalog" && table == "pg_database" => {
            Ok(SelectSource::Databases)
        }
        [schema, table] if schema == "information_schema" && table == "tables" => {
            Ok(SelectSource::Tables)
        }
        [schema, table] if schema == "information_schema" && table == "columns" => {
            Ok(SelectSource::Columns)
        }
        _ => Err(SqlError(format!("unsupported schema or relation {}", name))),
    }
}

fn parse_projection(item: &SelectItem, source: &SelectSource) -> Result<Projection, SqlError> {
    let (expr, alias) = match item {
        SelectItem::Wildcard(options) if options.to_string().is_empty() => {
            (ProjectedExpr::Wildcard, None)
        }
        SelectItem::QualifiedWildcard(kind, options) if options.to_string().is_empty() => {
            let rendered = kind.to_string();
            if !rendered.ends_with(".*") {
                return Err(unsupported("expression wildcards"));
            }
            (ProjectedExpr::Wildcard, None)
        }
        SelectItem::UnnamedExpr(expr) => (parse_projected_expr(expr, source)?, None),
        SelectItem::ExprWithAlias { expr, alias } => (
            parse_projected_expr(expr, source)?,
            Some(normalize_ident(alias)),
        ),
        _ => return Err(unsupported("this SELECT projection")),
    };
    Ok(Projection { expr, alias })
}

fn parse_projected_expr(expr: &Expr, source: &SelectSource) -> Result<ProjectedExpr, SqlError> {
    match expr {
        Expr::Identifier(identifier) => {
            let name = normalize_ident(identifier);
            if matches!(source, SelectSource::Session) {
                match name.as_str() {
                    "current_user" | "session_user" => {
                        return Ok(ProjectedExpr::SessionFunction(SessionFunction::CurrentUser));
                    }
                    _ => {}
                }
            }
            Ok(ProjectedExpr::Column(name))
        }
        Expr::CompoundIdentifier(parts) => Ok(ProjectedExpr::Column(normalize_ident(
            parts
                .last()
                .ok_or_else(|| SqlError("empty qualified column".to_owned()))?,
        ))),
        Expr::Function(function) if matches!(source, SelectSource::Session) => {
            if !function_has_no_arguments(function) {
                return Err(unsupported("arguments to session functions"));
            }
            let name = function.name.to_string().to_ascii_lowercase();
            let function = match name.as_str() {
                "version" => SessionFunction::Version,
                "current_database" => SessionFunction::CurrentDatabase,
                "current_schema" => SessionFunction::CurrentSchema,
                "current_user" | "session_user" => SessionFunction::CurrentUser,
                _ => return Err(SqlError(format!("unsupported PostgreSQL function {name}"))),
            };
            Ok(ProjectedExpr::SessionFunction(function))
        }
        Expr::Value(value) if matches!(source, SelectSource::Session) => {
            Ok(ProjectedExpr::Literal(parse_literal_value(&value.value)?))
        }
        Expr::Nested(inner) => parse_projected_expr(inner, source),
        _ => Err(unsupported("computed SELECT expressions")),
    }
}

fn parse_filter(expr: &Expr) -> Result<FilterExpr, SqlError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(FilterExpr::And(
                Box::new(parse_filter(left)?),
                Box::new(parse_filter(right)?),
            )),
            BinaryOperator::Or => Ok(FilterExpr::Or(
                Box::new(parse_filter(left)?),
                Box::new(parse_filter(right)?),
            )),
            _ => parse_comparison(left, op, right),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(FilterExpr::Not(Box::new(parse_filter(expr)?))),
        Expr::IsNull(expr) => Ok(FilterExpr::IsNull {
            column: parse_column(expr)?,
            negated: false,
        }),
        Expr::IsNotNull(expr) => Ok(FilterExpr::IsNull {
            column: parse_column(expr)?,
            negated: true,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(FilterExpr::In {
            column: parse_column(expr)?,
            values: list.iter().map(parse_literal).collect::<Result<_, _>>()?,
            negated: *negated,
        }),
        Expr::Nested(inner) => parse_filter(inner),
        _ => Err(unsupported("this WHERE expression")),
    }
}

fn parse_comparison(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
) -> Result<FilterExpr, SqlError> {
    let parsed_op = compare_op(op)?;
    if let Ok(column) = parse_column(left) {
        return Ok(FilterExpr::Compare {
            column,
            op: parsed_op,
            literal: parse_literal(right)?,
        });
    }
    Ok(FilterExpr::Compare {
        column: parse_column(right)?,
        op: reverse_compare_op(parsed_op),
        literal: parse_literal(left)?,
    })
}

fn compare_op(op: &BinaryOperator) -> Result<CompareOp, SqlError> {
    match op {
        BinaryOperator::Eq => Ok(CompareOp::Eq),
        BinaryOperator::NotEq => Ok(CompareOp::NotEq),
        BinaryOperator::Lt => Ok(CompareOp::Lt),
        BinaryOperator::LtEq => Ok(CompareOp::LtEq),
        BinaryOperator::Gt => Ok(CompareOp::Gt),
        BinaryOperator::GtEq => Ok(CompareOp::GtEq),
        _ => Err(unsupported("this comparison operator")),
    }
}

fn reverse_compare_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::NotEq => CompareOp::NotEq,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::LtEq => CompareOp::GtEq,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::GtEq => CompareOp::LtEq,
    }
}

fn parse_order_by(order_by: Option<&sqlparser::ast::OrderBy>) -> Result<Vec<OrderTerm>, SqlError> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(unsupported("ORDER BY ALL"));
    };
    if order_by.interpolate.is_some() {
        return Err(unsupported("ORDER BY interpolation"));
    }
    expressions
        .iter()
        .map(|term| {
            if term.options.nulls_first.is_some() || term.with_fill.is_some() {
                return Err(unsupported("NULLS FIRST/LAST and WITH FILL"));
            }
            Ok(OrderTerm {
                column: parse_column(&term.expr)?,
                ascending: term.options.asc.unwrap_or(true),
            })
        })
        .collect()
}

fn parse_limit(limit: Option<&LimitClause>) -> Result<(Option<PageValue>, PageValue), SqlError> {
    let Some(limit) = limit else {
        return Ok((None, PageValue::Literal(0)));
    };
    let LimitClause::LimitOffset {
        limit,
        offset,
        limit_by,
    } = limit
    else {
        return Err(unsupported("comma-style LIMIT"));
    };
    if !limit_by.is_empty() {
        return Err(unsupported("LIMIT BY"));
    }
    let limit = limit.as_ref().map(parse_page_value).transpose()?;
    let offset = offset
        .as_ref()
        .map(|offset| parse_page_value(&offset.value))
        .transpose()?
        .unwrap_or(PageValue::Literal(0));
    Ok((limit, offset))
}

fn parse_page_value(expr: &Expr) -> Result<PageValue, SqlError> {
    match parse_literal(expr)? {
        SqlLiteral::Number(value) => value
            .parse::<usize>()
            .map(PageValue::Literal)
            .map_err(|_| SqlError("LIMIT and OFFSET must be non-negative integers".to_owned())),
        SqlLiteral::Placeholder(position) => Ok(PageValue::Placeholder(position)),
        _ => Err(SqlError(
            "LIMIT and OFFSET must be integers or PostgreSQL $n placeholders".to_owned(),
        )),
    }
}

fn parse_column(expr: &Expr) -> Result<String, SqlError> {
    match expr {
        Expr::Identifier(identifier) => Ok(normalize_ident(identifier)),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(normalize_ident)
            .ok_or_else(|| SqlError("empty qualified column".to_owned())),
        Expr::Nested(inner) => parse_column(inner),
        _ => Err(SqlError("expected a column reference".to_owned())),
    }
}

fn parse_literal(expr: &Expr) -> Result<SqlLiteral, SqlError> {
    match expr {
        Expr::Value(value) => parse_literal_value(&value.value),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match parse_literal(expr)? {
            SqlLiteral::Number(value) => Ok(SqlLiteral::Number(format!("-{value}"))),
            _ => Err(SqlError("unary minus requires a number".to_owned())),
        },
        Expr::Nested(inner) => parse_literal(inner),
        _ => Err(SqlError("expected a literal value".to_owned())),
    }
}

fn parse_literal_value(value: &AstValue) -> Result<SqlLiteral, SqlError> {
    match value {
        AstValue::Number(value, _) => Ok(SqlLiteral::Number(value.clone())),
        AstValue::SingleQuotedString(value)
        | AstValue::EscapedStringLiteral(value)
        | AstValue::UnicodeStringLiteral(value)
        | AstValue::NationalStringLiteral(value) => Ok(SqlLiteral::String(value.clone())),
        AstValue::Boolean(value) => Ok(SqlLiteral::Boolean(*value)),
        AstValue::Null => Ok(SqlLiteral::Null),
        AstValue::Placeholder(value) => {
            let position = value
                .strip_prefix('$')
                .ok_or_else(|| {
                    SqlError("only PostgreSQL $n placeholders are supported".to_owned())
                })?
                .parse::<usize>()
                .map_err(|_| SqlError("invalid PostgreSQL placeholder".to_owned()))?;
            if position == 0 {
                return Err(SqlError("PostgreSQL placeholders start at $1".to_owned()));
            }
            if position > super::MAX_PARAMETER_COUNT {
                return Err(SqlError(format!(
                    "PostgreSQL placeholder position cannot exceed ${}",
                    super::MAX_PARAMETER_COUNT
                )));
            }
            Ok(SqlLiteral::Placeholder(position))
        }
        _ => Err(unsupported("this literal type")),
    }
}

fn function_has_no_arguments(function: &sqlparser::ast::Function) -> bool {
    let args_are_empty = match &function.args {
        FunctionArguments::None => true,
        FunctionArguments::List(arguments) => {
            arguments.args.is_empty()
                && arguments.clauses.is_empty()
                && arguments.duplicate_treatment.is_none()
        }
        FunctionArguments::Subquery(_) => false,
    };
    matches!(function.parameters, FunctionArguments::None)
        && args_are_empty
        && function.filter.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>, SqlError> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(normalize_ident)
                .ok_or_else(|| unsupported("dynamic relation names"))
        })
        .collect()
}

fn normalize_ident(identifier: &Ident) -> String {
    if identifier.quote_style.is_some() {
        identifier.value.clone()
    } else {
        identifier.value.to_ascii_lowercase()
    }
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
}

fn unsupported(feature: &str) -> SqlError {
    SqlError(format!("unsupported PostgreSQL feature: {feature}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_table_pagination() {
        let ParsedStatement::Select(plan) = parse_sql(
            "SELECT id, title FROM public.documents WHERE team_id = 'team' ORDER BY created_at DESC LIMIT 100 OFFSET 200",
        )
        .expect("parse")
        else {
            panic!("expected select");
        };
        assert_eq!(plan.source, SelectSource::Table("documents".to_owned()));
        assert_eq!(plan.limit, Some(PageValue::Literal(100)));
        assert_eq!(plan.offset, PageValue::Literal(200));
        assert_eq!(plan.order_by[0].column, "created_at");
        assert!(!plan.order_by[0].ascending);
    }

    #[test]
    fn recognizes_catalogues_and_session_functions() {
        let ParsedStatement::Select(databases) =
            parse_sql("SELECT datname FROM pg_catalog.pg_database").expect("parse")
        else {
            panic!("expected select");
        };
        assert_eq!(databases.source, SelectSource::Databases);

        let ParsedStatement::Select(session) =
            parse_sql("SELECT current_database(), version()").expect("parse")
        else {
            panic!("expected select");
        };
        assert_eq!(session.source, SelectSource::Session);
    }

    #[test]
    fn rejects_writes_and_joins() {
        assert!(parse_sql("DELETE FROM documents").is_err());
        assert!(parse_sql("SELECT * FROM a JOIN b ON a.id = b.id").is_err());
        assert!(parse_sql("SET statement_timeout = '1s'").is_err());
    }

    #[test]
    fn parses_parameterized_pagination_and_safe_batches() {
        let ParsedStatement::Select(plan) =
            parse_sql("SELECT id FROM documents LIMIT $1 OFFSET $2").expect("parse")
        else {
            panic!("expected select");
        };
        assert_eq!(plan.limit, Some(PageValue::Placeholder(1)));
        assert_eq!(plan.offset, PageValue::Placeholder(2));

        let batch = parse_sql_batch("SELECT 1; SELECT 2").expect("parse safe batch");
        assert_eq!(batch.len(), 2);
        assert!(parse_sql_batch("SELECT 1; DELETE FROM documents").is_err());
    }

    #[test]
    fn rejects_sql_that_would_build_pathologically_deep_parser_state() {
        let flat_filter = std::iter::repeat_n("title = 'x'", 3_500)
            .collect::<Vec<_>>()
            .join(" OR ");
        assert!(
            parse_sql(&format!(
                "SELECT id FROM documents WHERE {flat_filter} LIMIT 1"
            ))
            .is_err()
        );

        let unary_chain = format!("SELECT {}1", "-".repeat(MAX_PREFIX_OPERATOR_DEPTH + 1));
        assert!(parse_sql(&unary_chain).is_err());

        let nested = format!(
            "SELECT {}1{}",
            "(".repeat(MAX_EXPRESSION_DEPTH + 1),
            ")".repeat(MAX_EXPRESSION_DEPTH + 1)
        );
        assert!(parse_sql(&nested).is_err());
    }
}
