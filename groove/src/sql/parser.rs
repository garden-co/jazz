use crate::sql::policy::{Policy, PolicyAction, PolicyColumnRef, PolicyExpr, PolicyValue};
use crate::sql::query_graph::PredicateValue;
use crate::sql::schema::{ColumnDef, ColumnType};

/// Parsed SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    CreatePolicy(Policy),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Select(Select),
}

/// DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: String,
    pub where_clause: Vec<Condition>,
    /// If true, this is a "hard" delete that truncates history.
    pub hard: bool,
}

/// CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// INSERT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<PredicateValue>,
}

/// UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, PredicateValue)>,
    pub where_clause: Vec<Condition>,
}

/// SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub projection: Projection,
    pub from: FromClause,
    pub where_clause: Vec<Condition>,
    /// Maximum number of rows to return.
    pub limit: Option<u64>,
    /// Number of rows to skip from the start.
    pub offset: Option<u64>,
}

/// SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// SELECT *
    All,
    /// SELECT table.*
    TableAll(String),
    /// SELECT col1, col2, ... (simple columns only, legacy)
    Columns(Vec<QualifiedColumn>),
    /// SELECT expr1, expr2, ... (expressions including ARRAY subqueries)
    Expressions(Vec<SelectExpr>),
}

/// An expression in a SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectExpr {
    /// A column reference: `name` or `t.name`
    Column(QualifiedColumn),
    /// A table alias as composite type: `t` (returns full row)
    /// Distinct from Column because it references the whole row, not a column
    TableRow(String),
    /// An ARRAY subquery: `ARRAY(SELECT ...)`
    ArraySubquery(Box<Select>),
    /// An aliased expression: `expr AS alias`
    Aliased {
        expr: Box<SelectExpr>,
        alias: String,
    },
}

/// Qualified column name (optional table prefix).
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedColumn {
    pub table: Option<String>,
    pub column: String,
}

/// FROM clause with optional JOINs.
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table: String,
    /// Optional table alias (e.g., FROM notes n)
    pub alias: Option<String>,
    pub joins: Vec<Join>,
}

/// JOIN clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub table: String,
    pub on: JoinCondition,
}

/// JOIN ON condition (left = right, both are columns).
#[derive(Debug, Clone, PartialEq)]
pub struct JoinCondition {
    pub left: QualifiedColumn,
    pub right: QualifiedColumn,
}

/// WHERE condition (column = value or column = column).
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub column: QualifiedColumn,
    pub right: ConditionValue,
}

/// Right-hand side of a condition.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionValue {
    /// A literal value
    Literal(PredicateValue),
    /// A column reference (for correlated subqueries)
    Column(QualifiedColumn),
}

impl Condition {
    /// Get the value if this condition has a literal right-hand side.
    pub fn value(&self) -> Option<&PredicateValue> {
        match &self.right {
            ConditionValue::Literal(v) => Some(v),
            ConditionValue::Column(_) => None,
        }
    }
}

/// Parse error.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "parse error at position {}: {}",
            self.position, self.message
        )
    }
}

impl std::error::Error for ParseError {}

/// SQL parser.
struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Parser { input, pos: 0 }
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError {
            message: message.into(),
            position: self.pos,
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.pos += 1;
            } else if self.remaining().starts_with("--") {
                // Skip line comment
                while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn peek_char(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn consume_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn expect_char(&mut self, expected: char) -> Result<(), ParseError> {
        self.skip_whitespace();
        match self.consume_char() {
            Some(c) if c == expected => Ok(()),
            Some(c) => Err(self.error(format!("expected '{}', found '{}'", expected, c))),
            None => Err(self.error(format!("expected '{}', found end of input", expected))),
        }
    }

    fn try_keyword(&mut self, keyword: &str) -> bool {
        self.skip_whitespace();
        let remaining = self.remaining().to_uppercase();
        if remaining.starts_with(&keyword.to_uppercase()) {
            let after = remaining.as_bytes().get(keyword.len());
            // Ensure it's a full word match
            if after
                .map(|&c| c.is_ascii_alphanumeric() || c == b'_')
                .unwrap_or(false)
            {
                return false;
            }
            self.pos += keyword.len();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), ParseError> {
        if self.try_keyword(keyword) {
            Ok(())
        } else {
            Err(self.error(format!("expected keyword '{}'", keyword)))
        }
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        self.skip_whitespace();

        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }

        if self.pos == start {
            return Err(self.error("expected identifier"));
        }

        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_string_literal(&mut self) -> Result<String, ParseError> {
        self.skip_whitespace();
        self.expect_char('\'')?;

        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c == b'\'' {
                let s = self.input[start..self.pos].to_string();
                self.pos += 1; // consume closing quote
                return Ok(s);
            }
            self.pos += 1;
        }

        Err(self.error("unterminated string literal"))
    }

    /// Parse a number literal.
    fn parse_number(&mut self) -> Result<PredicateValue, ParseError> {
        self.skip_whitespace();

        let start = self.pos;
        let mut has_dot = false;
        // Optional minus
        if self.peek_char() == Some('-') {
            self.pos += 1;
        }

        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c.is_ascii_digit() {
                self.pos += 1;
            } else if c == b'.' && !has_dot {
                has_dot = true;
                self.pos += 1;
            } else {
                break;
            }
        }

        let num_str = &self.input[start..self.pos];
        if num_str.is_empty() || num_str == "-" {
            return Err(self.error("expected number"));
        }

        if has_dot {
            let n: f64 = num_str.parse().map_err(|_| self.error("invalid float"))?;
            Ok(PredicateValue::F64(n))
        } else {
            let n: i64 = num_str.parse().map_err(|_| self.error("invalid integer"))?;
            Ok(PredicateValue::I64(n))
        }
    }

    /// Parse an unsigned 64-bit integer (for LIMIT/OFFSET values).
    fn parse_u64(&mut self) -> Result<u64, ParseError> {
        self.skip_whitespace();
        let start = self.pos;

        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }

        if self.pos == start {
            return Err(self.error("expected number"));
        }

        let num_str = &self.input[start..self.pos];
        num_str.parse().map_err(|_| self.error("invalid number"))
    }

    /// Parse a SQL value (used in INSERT/UPDATE statements).
    fn parse_value(&mut self) -> Result<PredicateValue, ParseError> {
        self.skip_whitespace();

        // NULL
        if self.try_keyword("NULL") {
            return Ok(PredicateValue::Null);
        }

        // Boolean
        if self.try_keyword("true") {
            return Ok(PredicateValue::Bool(true));
        }
        if self.try_keyword("false") {
            return Ok(PredicateValue::Bool(false));
        }

        // String literal - always parse as String.
        // The database executor coerces to ObjectId when inserting into Ref columns.
        if self.peek_char() == Some('\'') {
            let s = self.parse_string_literal()?;
            return Ok(PredicateValue::String(s));
        }

        // Number
        if self
            .peek_char()
            .map(|c| c.is_ascii_digit() || c == '-')
            .unwrap_or(false)
        {
            return self.parse_number();
        }

        Err(self.error("expected value"))
    }

    fn parse_column_type(&mut self) -> Result<ColumnType, ParseError> {
        self.skip_whitespace();

        if self.try_keyword("BOOL") {
            return Ok(ColumnType::Bool);
        }
        if self.try_keyword("I32") {
            return Ok(ColumnType::I32);
        }
        if self.try_keyword("U32") {
            return Ok(ColumnType::U32);
        }
        if self.try_keyword("I64") {
            return Ok(ColumnType::I64);
        }
        if self.try_keyword("F64") {
            return Ok(ColumnType::F64);
        }
        if self.try_keyword("STRING") {
            return Ok(ColumnType::String);
        }
        if self.try_keyword("BYTES") {
            return Ok(ColumnType::Bytes);
        }
        if self.try_keyword("BLOB") {
            // Check for array suffix []
            self.skip_whitespace();
            if self.peek_char() == Some('[') {
                self.consume_char();
                self.skip_whitespace();
                if self.peek_char() != Some(']') {
                    return Err(self.error("expected ']' after '[' in BLOB[]"));
                }
                self.consume_char();
                return Ok(ColumnType::BlobArray);
            }
            return Ok(ColumnType::Blob);
        }
        if self.try_keyword("REFERENCES") {
            let target = self.parse_identifier()?;
            return Ok(ColumnType::Ref(target));
        }

        Err(self.error("expected column type"))
    }

    fn parse_qualified_column(&mut self) -> Result<QualifiedColumn, ParseError> {
        let first = self.parse_identifier()?;

        self.skip_whitespace();
        if self.peek_char() == Some('.') {
            self.consume_char();
            let second = self.parse_identifier()?;
            Ok(QualifiedColumn {
                table: Some(first),
                column: second,
            })
        } else {
            Ok(QualifiedColumn {
                table: None,
                column: first,
            })
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTable, ParseError> {
        self.expect_keyword("TABLE")?;
        let name = self.parse_identifier()?;
        self.expect_char('(')?;

        let mut columns = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek_char() == Some(')') {
                break;
            }

            if !columns.is_empty() {
                self.expect_char(',')?;
            }

            let col_name = self.parse_identifier()?;
            let col_type = self.parse_column_type()?;

            // Check for NOT NULL or NULL
            let nullable = if self.try_keyword("NOT") {
                self.expect_keyword("NULL")?;
                false
            } else if self.try_keyword("NULL") {
                true
            } else {
                true // Default to nullable
            };

            columns.push(ColumnDef::new(col_name, col_type, nullable));
        }

        self.expect_char(')')?;

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(CreateTable { name, columns })
    }

    fn parse_insert(&mut self) -> Result<Insert, ParseError> {
        self.expect_keyword("INTO")?;
        let table = self.parse_identifier()?;

        self.expect_char('(')?;
        let mut columns = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek_char() == Some(')') {
                break;
            }
            if !columns.is_empty() {
                self.expect_char(',')?;
            }
            columns.push(self.parse_identifier()?);
        }
        self.expect_char(')')?;

        self.expect_keyword("VALUES")?;
        self.expect_char('(')?;

        let mut values = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek_char() == Some(')') {
                break;
            }
            if !values.is_empty() {
                self.expect_char(',')?;
            }
            values.push(self.parse_value()?);
        }
        self.expect_char(')')?;

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(Insert {
            table,
            columns,
            values,
        })
    }

    fn parse_update(&mut self) -> Result<Update, ParseError> {
        let table = self.parse_identifier()?;

        self.expect_keyword("SET")?;

        let mut assignments = Vec::new();
        loop {
            let col = self.parse_identifier()?;
            self.expect_char('=')?;
            let val = self.parse_value()?;
            assignments.push((col, val));

            self.skip_whitespace();
            if self.peek_char() != Some(',') {
                break;
            }
            self.consume_char();
        }

        let where_clause = self.parse_where_clause()?;

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(Update {
            table,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Delete, ParseError> {
        self.expect_keyword("FROM")?;
        let table = self.parse_identifier()?;
        let where_clause = self.parse_where_clause()?;

        // Check for optional HARD keyword
        let hard = self.try_keyword("HARD");

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(Delete {
            table,
            where_clause,
            hard,
        })
    }

    fn parse_select(&mut self) -> Result<Select, ParseError> {
        // Projection
        self.skip_whitespace();
        let projection = if self.peek_char() == Some('*') {
            self.consume_char();
            Projection::All
        } else {
            // Parse projection expressions
            let mut exprs = Vec::new();

            loop {
                let expr = self.parse_select_expr()?;
                exprs.push(expr);

                self.skip_whitespace();
                if self.peek_char() == Some(',') {
                    self.consume_char();
                } else {
                    break;
                }
            }

            // Check if this is a simple table.* projection
            if exprs.len() == 1 {
                if let SelectExpr::Column(qc) = &exprs[0] {
                    if qc.column == "*" {
                        if let Some(table) = &qc.table {
                            let from = self.parse_from_clause()?;
                            let where_clause = self.parse_where_clause()?;
                            let (limit, offset) = self.parse_limit_offset()?;
                            self.skip_whitespace();
                            if self.peek_char() == Some(';') {
                                self.consume_char();
                            }
                            return Ok(Select {
                                projection: Projection::TableAll(table.clone()),
                                from,
                                where_clause,
                                limit,
                                offset,
                            });
                        }
                    }
                }
            }

            Projection::Expressions(exprs)
        };

        let from = self.parse_from_clause()?;
        let where_clause = self.parse_where_clause()?;
        let (limit, offset) = self.parse_limit_offset()?;

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(Select {
            projection,
            from,
            where_clause,
            limit,
            offset,
        })
    }

    /// Parse optional LIMIT and OFFSET clauses.
    fn parse_limit_offset(&mut self) -> Result<(Option<u64>, Option<u64>), ParseError> {
        let limit = if self.try_keyword("LIMIT") {
            Some(self.parse_u64()?)
        } else {
            None
        };

        let offset = if self.try_keyword("OFFSET") {
            Some(self.parse_u64()?)
        } else {
            None
        };

        Ok((limit, offset))
    }

    /// Parse a single SELECT expression (column, table row, ARRAY subquery, or aliased expression).
    fn parse_select_expr(&mut self) -> Result<SelectExpr, ParseError> {
        self.skip_whitespace();

        // Check for ARRAY(SELECT ...)
        if self.try_keyword("ARRAY") {
            self.expect_char('(')?;
            self.expect_keyword("SELECT")?;
            let subquery = self.parse_select()?;
            self.expect_char(')')?;

            // Check for optional AS alias
            let expr = SelectExpr::ArraySubquery(Box::new(subquery));
            return self.maybe_parse_alias(expr);
        }

        // Parse identifier (could be column, table.column, or table alias for row)
        let first_ident = self.parse_identifier()?;
        self.skip_whitespace();

        if self.peek_char() == Some('.') {
            self.consume_char();
            self.skip_whitespace();

            if self.peek_char() == Some('*') {
                self.consume_char();
                // table.* - represented as Column with "*" as column name
                let expr = SelectExpr::Column(QualifiedColumn {
                    table: Some(first_ident),
                    column: "*".to_string(),
                });
                return self.maybe_parse_alias(expr);
            } else {
                // table.column
                let col_name = self.parse_identifier()?;
                let expr = SelectExpr::Column(QualifiedColumn {
                    table: Some(first_ident),
                    column: col_name,
                });
                return self.maybe_parse_alias(expr);
            }
        }

        // Just an identifier - could be:
        // 1. A table alias (returns whole row) if it matches FROM alias
        // 2. A column name
        // We'll distinguish at execution time based on FROM clause
        // For now, if it's followed by FROM/WHERE/,/)/; treat as potential table row
        // Actually, we can't know here - we'll use TableRow for bare identifiers
        // and resolve at execution time

        // Check if this looks like a table alias (bare identifier before FROM, comma, or end)
        self.skip_whitespace();
        let next = self.peek_char();
        let is_keyword = self.is_keyword_next(&["FROM", "WHERE", "AND", "AS"]);

        let expr = if next == Some(',')
            || next == Some(')')
            || next == Some(';')
            || next.is_none()
            || is_keyword
        {
            // Bare identifier - could be column or table alias for row
            // We'll mark it as Column and let execution decide based on FROM
            SelectExpr::Column(QualifiedColumn {
                table: None,
                column: first_ident,
            })
        } else {
            SelectExpr::Column(QualifiedColumn {
                table: None,
                column: first_ident,
            })
        };

        self.maybe_parse_alias(expr)
    }

    /// Parse optional AS alias after an expression.
    fn maybe_parse_alias(&mut self, expr: SelectExpr) -> Result<SelectExpr, ParseError> {
        self.skip_whitespace();
        if self.try_keyword("AS") {
            let alias = self.parse_identifier()?;
            Ok(SelectExpr::Aliased {
                expr: Box::new(expr),
                alias,
            })
        } else {
            Ok(expr)
        }
    }

    fn parse_from_clause(&mut self) -> Result<FromClause, ParseError> {
        self.expect_keyword("FROM")?;
        let table = self.parse_identifier()?;

        // Check for optional table alias (e.g., FROM notes n)
        // Must not be a keyword like JOIN, WHERE, etc.
        self.skip_whitespace();
        let alias = if !self.is_keyword_next(&[
            "JOIN", "WHERE", "AND", "OR", "ON", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING",
        ]) && self
            .peek_char()
            .map(|c| c.is_ascii_alphabetic())
            .unwrap_or(false)
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let mut joins = Vec::new();
        while self.try_keyword("JOIN") {
            let join_table = self.parse_identifier()?;
            self.expect_keyword("ON")?;

            let left = self.parse_qualified_column()?;
            self.expect_char('=')?;
            let right = self.parse_qualified_column()?;

            joins.push(Join {
                table: join_table,
                on: JoinCondition { left, right },
            });
        }

        Ok(FromClause {
            table,
            alias,
            joins,
        })
    }

    /// Check if the next token is one of the given keywords (without consuming).
    fn is_keyword_next(&self, keywords: &[&str]) -> bool {
        let remaining = self.remaining().to_uppercase();
        for kw in keywords {
            if remaining.starts_with(&kw.to_uppercase()) {
                let after = remaining.as_bytes().get(kw.len());
                if after
                    .map(|&c| !c.is_ascii_alphanumeric() && c != b'_')
                    .unwrap_or(true)
                {
                    return true;
                }
            }
        }
        false
    }

    fn parse_where_clause(&mut self) -> Result<Vec<Condition>, ParseError> {
        let mut conditions = Vec::new();

        if !self.try_keyword("WHERE") {
            return Ok(conditions);
        }

        loop {
            let column = self.parse_qualified_column()?;
            self.expect_char('=')?;
            let right = self.parse_condition_value()?;

            conditions.push(Condition { column, right });

            if !self.try_keyword("AND") {
                break;
            }
        }

        Ok(conditions)
    }

    /// Parse the right-hand side of a WHERE condition (value or column reference).
    fn parse_condition_value(&mut self) -> Result<ConditionValue, ParseError> {
        self.skip_whitespace();

        // Try to parse as a literal value first
        // Check for NULL, true, false, string literal, or number
        if self.try_keyword("NULL") {
            return Ok(ConditionValue::Literal(PredicateValue::Null));
        }
        if self.try_keyword("true") {
            return Ok(ConditionValue::Literal(PredicateValue::Bool(true)));
        }
        if self.try_keyword("false") {
            return Ok(ConditionValue::Literal(PredicateValue::Bool(false)));
        }
        if self.peek_char() == Some('\'') {
            let s = self.parse_string_literal()?;
            return Ok(ConditionValue::Literal(PredicateValue::String(s)));
        }
        if self
            .peek_char()
            .map(|c| c.is_ascii_digit() || c == '-')
            .unwrap_or(false)
        {
            let val = self.parse_number()?;
            return Ok(ConditionValue::Literal(val));
        }

        // Otherwise, it must be a column reference
        let col = self.parse_qualified_column()?;
        Ok(ConditionValue::Column(col))
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        self.skip_whitespace();

        if self.try_keyword("CREATE") {
            self.skip_whitespace();
            if self.try_keyword("POLICY") {
                return Ok(Statement::CreatePolicy(self.parse_create_policy()?));
            }
            return Ok(Statement::CreateTable(self.parse_create_table()?));
        }
        if self.try_keyword("INSERT") {
            return Ok(Statement::Insert(self.parse_insert()?));
        }
        if self.try_keyword("UPDATE") {
            return Ok(Statement::Update(self.parse_update()?));
        }
        if self.try_keyword("DELETE") {
            return Ok(Statement::Delete(self.parse_delete()?));
        }
        if self.try_keyword("SELECT") {
            return Ok(Statement::Select(self.parse_select()?));
        }

        Err(self.error("expected CREATE, INSERT, UPDATE, DELETE, or SELECT"))
    }

    // ========== Policy Parsing ==========

    fn parse_create_policy(&mut self) -> Result<Policy, ParseError> {
        // CREATE POLICY ON <table> FOR <action> [WHERE <expr>] [CHECK (<expr>)]
        self.expect_keyword("ON")?;
        let table = self.parse_identifier()?;

        self.expect_keyword("FOR")?;
        let action = self.parse_policy_action()?;

        let mut policy = Policy::new(table, action);

        // Parse WHERE clause (for SELECT, UPDATE, DELETE)
        if self.try_keyword("WHERE") {
            let expr = self.parse_policy_expr()?;
            policy.where_clause = Some(expr);
        }

        // Parse CHECK clause (for INSERT, UPDATE)
        if self.try_keyword("CHECK") {
            self.expect_char('(')?;
            let expr = self.parse_policy_expr()?;
            self.expect_char(')')?;
            policy.check_clause = Some(expr);
        }

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(policy)
    }

    fn parse_policy_action(&mut self) -> Result<PolicyAction, ParseError> {
        self.skip_whitespace();

        if self.try_keyword("SELECT") {
            return Ok(PolicyAction::Select);
        }
        if self.try_keyword("INSERT") {
            return Ok(PolicyAction::Insert);
        }
        if self.try_keyword("UPDATE") {
            return Ok(PolicyAction::Update);
        }
        if self.try_keyword("DELETE") {
            return Ok(PolicyAction::Delete);
        }

        Err(self.error("expected SELECT, INSERT, UPDATE, or DELETE"))
    }

    fn parse_policy_expr(&mut self) -> Result<PolicyExpr, ParseError> {
        self.parse_policy_or_expr()
    }

    fn parse_policy_or_expr(&mut self) -> Result<PolicyExpr, ParseError> {
        let mut left = self.parse_policy_and_expr()?;

        while self.try_keyword("OR") {
            let right = self.parse_policy_and_expr()?;
            left = match left {
                PolicyExpr::Or(mut exprs) => {
                    exprs.push(right);
                    PolicyExpr::Or(exprs)
                }
                _ => PolicyExpr::Or(vec![left, right]),
            };
        }

        Ok(left)
    }

    fn parse_policy_and_expr(&mut self) -> Result<PolicyExpr, ParseError> {
        let mut left = self.parse_policy_unary_expr()?;

        while self.try_keyword("AND") {
            let right = self.parse_policy_unary_expr()?;
            left = match left {
                PolicyExpr::And(mut exprs) => {
                    exprs.push(right);
                    PolicyExpr::And(exprs)
                }
                _ => PolicyExpr::And(vec![left, right]),
            };
        }

        Ok(left)
    }

    fn parse_policy_unary_expr(&mut self) -> Result<PolicyExpr, ParseError> {
        self.skip_whitespace();

        // NOT expression
        if self.try_keyword("NOT") {
            let expr = self.parse_policy_unary_expr()?;
            return Ok(PolicyExpr::Not(Box::new(expr)));
        }

        // Parenthesized expression
        if self.peek_char() == Some('(') {
            self.consume_char();
            let expr = self.parse_policy_expr()?;
            self.expect_char(')')?;
            return Ok(expr);
        }

        // INHERITS clause
        if self.try_keyword("INHERITS") {
            let action = self.parse_policy_action()?;
            self.expect_keyword("FROM")?;
            let column = self.parse_policy_column_ref()?;
            return Ok(PolicyExpr::Inherits { action, column });
        }

        // Primary expression (comparison or IS NULL)
        self.parse_policy_primary_expr()
    }

    fn parse_policy_primary_expr(&mut self) -> Result<PolicyExpr, ParseError> {
        let left = self.parse_policy_value()?;

        self.skip_whitespace();

        // Check for IS NULL / IS NOT NULL
        if self.try_keyword("IS") {
            if self.try_keyword("NOT") {
                self.expect_keyword("NULL")?;
                return Ok(PolicyExpr::IsNotNull(left));
            }
            self.expect_keyword("NULL")?;
            return Ok(PolicyExpr::IsNull(left));
        }

        // Check for comparison operators
        let op = self.parse_comparison_op()?;
        let right = self.parse_policy_value()?;

        Ok(match op {
            CompOp::Eq => PolicyExpr::Eq(left, right),
            CompOp::Ne => PolicyExpr::Ne(left, right),
            CompOp::Lt => PolicyExpr::Lt(left, right),
            CompOp::Le => PolicyExpr::Le(left, right),
            CompOp::Gt => PolicyExpr::Gt(left, right),
            CompOp::Ge => PolicyExpr::Ge(left, right),
        })
    }

    fn parse_comparison_op(&mut self) -> Result<CompOp, ParseError> {
        self.skip_whitespace();

        // Check two-character operators first
        if self.remaining().starts_with("!=") {
            self.pos += 2;
            return Ok(CompOp::Ne);
        }
        if self.remaining().starts_with("<>") {
            self.pos += 2;
            return Ok(CompOp::Ne);
        }
        if self.remaining().starts_with("<=") {
            self.pos += 2;
            return Ok(CompOp::Le);
        }
        if self.remaining().starts_with(">=") {
            self.pos += 2;
            return Ok(CompOp::Ge);
        }

        // Single-character operators
        match self.peek_char() {
            Some('=') => {
                self.consume_char();
                Ok(CompOp::Eq)
            }
            Some('<') => {
                self.consume_char();
                Ok(CompOp::Lt)
            }
            Some('>') => {
                self.consume_char();
                Ok(CompOp::Gt)
            }
            _ => Err(self.error("expected comparison operator (=, !=, <, <=, >, >=)")),
        }
    }

    fn parse_policy_value(&mut self) -> Result<PolicyValue, ParseError> {
        self.skip_whitespace();

        // @viewer, @old, @new
        if self.peek_char() == Some('@') {
            self.consume_char();
            let ident = self.parse_identifier()?;

            match ident.to_lowercase().as_str() {
                "viewer" => return Ok(PolicyValue::Viewer),
                "old" => {
                    self.expect_char('.')?;
                    let col = self.parse_identifier()?;
                    return Ok(PolicyValue::OldColumn(col));
                }
                "new" => {
                    self.expect_char('.')?;
                    let col = self.parse_identifier()?;
                    return Ok(PolicyValue::NewColumn(col));
                }
                _ => return Err(self.error(format!("unknown special variable @{}", ident))),
            }
        }

        // Literal values
        if self.try_keyword("NULL") {
            return Ok(PolicyValue::Literal(PredicateValue::Null));
        }
        if self.try_keyword("true") {
            return Ok(PolicyValue::Literal(PredicateValue::Bool(true)));
        }
        if self.try_keyword("false") {
            return Ok(PolicyValue::Literal(PredicateValue::Bool(false)));
        }

        // String literal
        if self.peek_char() == Some('\'') {
            let s = self.parse_string_literal()?;
            return Ok(PolicyValue::Literal(PredicateValue::String(s)));
        }

        // Number
        if self
            .peek_char()
            .map(|c| c.is_ascii_digit() || c == '-')
            .unwrap_or(false)
        {
            let val = self.parse_number()?;
            return Ok(PolicyValue::Literal(val));
        }

        // Column reference (identifier)
        let col = self.parse_identifier()?;
        Ok(PolicyValue::Column(col))
    }

    fn parse_policy_column_ref(&mut self) -> Result<PolicyColumnRef, ParseError> {
        self.skip_whitespace();

        // @new.column
        if self.peek_char() == Some('@') {
            self.consume_char();
            let ident = self.parse_identifier()?;

            if ident.to_lowercase() != "new" {
                return Err(
                    self.error("INHERITS FROM only supports @new.column or column references")
                );
            }

            self.expect_char('.')?;
            let col = self.parse_identifier()?;
            return Ok(PolicyColumnRef::New(col));
        }

        // Plain column name
        let col = self.parse_identifier()?;
        Ok(PolicyColumnRef::Current(col))
    }
}

/// Comparison operator (internal use).
#[derive(Debug, Clone, Copy)]
enum CompOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Parse a SQL string into a statement.
pub fn parse(sql: &str) -> Result<Statement, ParseError> {
    let mut parser = Parser::new(sql);
    parser.parse_statement()
}

// Tests have been moved to tests/sql_parser.rs
