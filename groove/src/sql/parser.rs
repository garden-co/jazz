use crate::sql::schema::{ColumnDef, ColumnType};
use crate::sql::row::Value;

/// Parsed SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    Insert(Insert),
    Update(Update),
    Select(Select),
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
    pub values: Vec<Value>,
}

/// UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, Value)>,
    pub where_clause: Vec<Condition>,
}

/// SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub projection: Projection,
    pub from: FromClause,
    pub where_clause: Vec<Condition>,
}

/// SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// SELECT *
    All,
    /// SELECT table.*
    TableAll(String),
    /// SELECT col1, col2, ...
    Columns(Vec<QualifiedColumn>),
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

/// WHERE condition (column = value).
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub column: QualifiedColumn,
    pub value: Value,
}

/// Parse error.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parse error at position {}: {}", self.position, self.message)
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
            if after.map(|&c| c.is_ascii_alphanumeric() || c == b'_').unwrap_or(false) {
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

    fn parse_number(&mut self) -> Result<Value, ParseError> {
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
            Ok(Value::F64(n))
        } else {
            let n: i64 = num_str.parse().map_err(|_| self.error("invalid integer"))?;
            Ok(Value::I64(n))
        }
    }

    fn parse_uuid_literal(&mut self) -> Result<u128, ParseError> {
        self.skip_whitespace();

        // Expect x'...'
        if !self.try_keyword("x") {
            return Err(self.error("expected x'...' for UUID literal"));
        }
        self.expect_char('\'')?;

        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c == b'\'' {
                break;
            }
            if !c.is_ascii_hexdigit() {
                return Err(self.error("invalid hex character in UUID"));
            }
            self.pos += 1;
        }

        let hex_str = &self.input[start..self.pos];
        self.expect_char('\'')?;

        // Parse as u128
        let value = u128::from_str_radix(hex_str, 16)
            .map_err(|_| self.error("invalid UUID hex value"))?;

        Ok(value)
    }

    fn parse_value(&mut self) -> Result<Value, ParseError> {
        self.skip_whitespace();

        // NULL
        if self.try_keyword("NULL") {
            return Ok(Value::Null);
        }

        // Boolean
        if self.try_keyword("true") {
            return Ok(Value::Bool(true));
        }
        if self.try_keyword("false") {
            return Ok(Value::Bool(false));
        }

        // UUID literal x'...'
        if self.remaining().to_uppercase().starts_with("X'") {
            let id = self.parse_uuid_literal()?;
            return Ok(Value::Ref(id));
        }

        // String literal
        if self.peek_char() == Some('\'') {
            let s = self.parse_string_literal()?;
            return Ok(Value::String(s));
        }

        // Number
        if self.peek_char().map(|c| c.is_ascii_digit() || c == '-').unwrap_or(false) {
            return self.parse_number();
        }

        Err(self.error("expected value"))
    }

    fn parse_column_type(&mut self) -> Result<ColumnType, ParseError> {
        self.skip_whitespace();

        if self.try_keyword("BOOL") {
            return Ok(ColumnType::Bool);
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

        Ok(Insert { table, columns, values })
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

    fn parse_select(&mut self) -> Result<Select, ParseError> {
        // Projection
        self.skip_whitespace();
        let projection = if self.peek_char() == Some('*') {
            self.consume_char();
            Projection::All
        } else {
            // Check for table.* pattern first
            let first_ident = self.parse_identifier()?;
            self.skip_whitespace();

            if self.peek_char() == Some('.') {
                self.consume_char();
                self.skip_whitespace();
                if self.peek_char() == Some('*') {
                    self.consume_char();
                    // This is table.* projection
                    let from = self.parse_from_clause()?;
                    let where_clause = self.parse_where_clause()?;

                    // Optional semicolon
                    self.skip_whitespace();
                    if self.peek_char() == Some(';') {
                        self.consume_char();
                    }

                    return Ok(Select {
                        projection: Projection::TableAll(first_ident),
                        from,
                        where_clause,
                    });
                } else {
                    // It's table.column, parse the column name
                    let col_name = self.parse_identifier()?;
                    let mut cols = vec![QualifiedColumn {
                        table: Some(first_ident),
                        column: col_name,
                    }];

                    // Continue parsing more columns
                    self.skip_whitespace();
                    while self.peek_char() == Some(',') {
                        self.consume_char();
                        cols.push(self.parse_qualified_column()?);
                        self.skip_whitespace();
                    }

                    Projection::Columns(cols)
                }
            } else if self.peek_char() == Some(',') {
                // Multiple columns starting with simple identifier
                let mut cols = vec![QualifiedColumn {
                    table: None,
                    column: first_ident,
                }];

                while self.peek_char() == Some(',') {
                    self.consume_char();
                    cols.push(self.parse_qualified_column()?);
                    self.skip_whitespace();
                }

                Projection::Columns(cols)
            } else {
                // Single column or keyword (like FROM)
                // Check if next is FROM
                if self.remaining().to_uppercase().starts_with("FROM") {
                    Projection::Columns(vec![QualifiedColumn {
                        table: None,
                        column: first_ident,
                    }])
                } else {
                    Projection::Columns(vec![QualifiedColumn {
                        table: None,
                        column: first_ident,
                    }])
                }
            }
        };

        let from = self.parse_from_clause()?;
        let where_clause = self.parse_where_clause()?;

        // Optional semicolon
        self.skip_whitespace();
        if self.peek_char() == Some(';') {
            self.consume_char();
        }

        Ok(Select {
            projection,
            from,
            where_clause,
        })
    }

    fn parse_from_clause(&mut self) -> Result<FromClause, ParseError> {
        self.expect_keyword("FROM")?;
        let table = self.parse_identifier()?;

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

        Ok(FromClause { table, joins })
    }

    fn parse_where_clause(&mut self) -> Result<Vec<Condition>, ParseError> {
        let mut conditions = Vec::new();

        if !self.try_keyword("WHERE") {
            return Ok(conditions);
        }

        loop {
            let column = self.parse_qualified_column()?;
            self.expect_char('=')?;
            let value = self.parse_value()?;

            conditions.push(Condition { column, value });

            if !self.try_keyword("AND") {
                break;
            }
        }

        Ok(conditions)
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        self.skip_whitespace();

        if self.try_keyword("CREATE") {
            return Ok(Statement::CreateTable(self.parse_create_table()?));
        }
        if self.try_keyword("INSERT") {
            return Ok(Statement::Insert(self.parse_insert()?));
        }
        if self.try_keyword("UPDATE") {
            return Ok(Statement::Update(self.parse_update()?));
        }
        if self.try_keyword("SELECT") {
            return Ok(Statement::Select(self.parse_select()?));
        }

        Err(self.error("expected CREATE, INSERT, UPDATE, or SELECT"))
    }
}

/// Parse a SQL string into a statement.
pub fn parse(sql: &str) -> Result<Statement, ParseError> {
    let mut parser = Parser::new(sql);
    parser.parse_statement()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table_simple() {
        let sql = "CREATE TABLE users (name STRING NOT NULL, age I64)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "name");
                assert_eq!(ct.columns[0].ty, ColumnType::String);
                assert!(!ct.columns[0].nullable);
                assert_eq!(ct.columns[1].name, "age");
                assert_eq!(ct.columns[1].ty, ColumnType::I64);
                assert!(ct.columns[1].nullable);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_with_ref() {
        let sql = "CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "posts");
                assert_eq!(ct.columns[0].ty, ColumnType::Ref("users".into()));
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_insert() {
        let sql = "INSERT INTO users (name, age, active) VALUES ('Alice', 30, true)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns, vec!["name", "age", "active"]);
                assert_eq!(ins.values, vec![
                    Value::String("Alice".into()),
                    Value::I64(30),
                    Value::Bool(true),
                ]);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_with_null() {
        let sql = "INSERT INTO users (name, email) VALUES ('Bob', NULL)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.values, vec![
                    Value::String("Bob".into()),
                    Value::Null,
                ]);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_with_uuid() {
        let sql = "INSERT INTO posts (author, title) VALUES (x'0192abcd12345678', 'Hello')";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.values[0], Value::Ref(0x0192abcd12345678));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_update() {
        let sql = "UPDATE users SET email = 'new@example.com', age = 31 WHERE id = x'abc123'";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Update(upd) => {
                assert_eq!(upd.table, "users");
                assert_eq!(upd.assignments.len(), 2);
                assert_eq!(upd.assignments[0], ("email".into(), Value::String("new@example.com".into())));
                assert_eq!(upd.assignments[1], ("age".into(), Value::I64(31)));
                assert_eq!(upd.where_clause.len(), 1);
                assert_eq!(upd.where_clause[0].column.column, "id");
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn parse_select_star() {
        let sql = "SELECT * FROM users";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.projection, Projection::All);
                assert_eq!(sel.from.table, "users");
                assert!(sel.from.joins.is_empty());
                assert!(sel.where_clause.is_empty());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_columns() {
        let sql = "SELECT name, email FROM users";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(sel) => {
                match sel.projection {
                    Projection::Columns(cols) => {
                        assert_eq!(cols.len(), 2);
                        assert_eq!(cols[0].column, "name");
                        assert_eq!(cols[1].column, "email");
                    }
                    _ => panic!("expected Columns"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let sql = "SELECT * FROM users WHERE active = true AND age = 30";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.where_clause.len(), 2);
                assert_eq!(sel.where_clause[0].column.column, "active");
                assert_eq!(sel.where_clause[0].value, Value::Bool(true));
                assert_eq!(sel.where_clause[1].column.column, "age");
                assert_eq!(sel.where_clause[1].value, Value::I64(30));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_join() {
        let sql = "SELECT * FROM comments JOIN users ON comments.author = users.id";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.from.table, "comments");
                assert_eq!(sel.from.joins.len(), 1);
                assert_eq!(sel.from.joins[0].table, "users");
                assert_eq!(sel.from.joins[0].on.left.table, Some("comments".into()));
                assert_eq!(sel.from.joins[0].on.left.column, "author");
                assert_eq!(sel.from.joins[0].on.right.table, Some("users".into()));
                assert_eq!(sel.from.joins[0].on.right.column, "id");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_table_star() {
        let sql = "SELECT users.* FROM comments JOIN users ON comments.author = users.id";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.projection, Projection::TableAll("users".into()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_float() {
        let sql = "INSERT INTO data (value) VALUES (3.14)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.values[0], Value::F64(3.14));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_negative_number() {
        let sql = "INSERT INTO data (value) VALUES (-42)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.values[0], Value::I64(-42));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_with_comments() {
        let sql = "-- This is a comment\nSELECT * FROM users";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(_) => {}
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_with_semicolon() {
        let sql = "SELECT * FROM users;";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(_) => {}
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn case_insensitive_keywords() {
        let sql = "select * from users where active = TRUE";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.where_clause[0].value, Value::Bool(true));
            }
            _ => panic!("expected Select"),
        }
    }
}
