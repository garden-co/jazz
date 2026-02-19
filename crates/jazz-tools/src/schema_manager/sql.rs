//! SQL parsing and generation for schema definitions.
//!
//! Hand-rolled parser for the minimal SQL subset we support.
//!
//! # Supported SQL
//!
//! ## Schema DDL (CREATE TABLE)
//! ```sql
//! CREATE TABLE todos (
//!     title TEXT NOT NULL,
//!     completed BOOLEAN NOT NULL
//! );
//! ```
//!
//! ## Lens DDL (ALTER TABLE)
//! ```sql
//! ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
//! ALTER TABLE users DROP COLUMN deprecated_field;
//! ALTER TABLE users RENAME COLUMN email TO email_address;
//! CREATE TABLE new_table (id TEXT NOT NULL);
//! DROP TABLE old_table;
//! ```

use std::collections::HashMap;

use crate::query_manager::types::{
    ColumnDescriptor, ColumnName, ColumnType, RowDescriptor, Schema, TableName, TableSchema, Value,
};

use super::lens::{LensOp, LensTransform};

/// Errors that can occur during SQL parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParseError {
    /// SQL syntax error.
    SyntaxError(String),
    /// Unsupported SQL statement type.
    UnsupportedStatement(String),
    /// Unsupported column type.
    UnsupportedType(String),
    /// Invalid value in DEFAULT clause.
    InvalidDefaultValue(String),
    /// Unexpected end of input.
    UnexpectedEnd,
    /// Expected a specific token.
    Expected(String),
}

impl std::fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlParseError::SyntaxError(msg) => write!(f, "SQL syntax error: {}", msg),
            SqlParseError::UnsupportedStatement(msg) => write!(f, "Unsupported statement: {}", msg),
            SqlParseError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
            SqlParseError::InvalidDefaultValue(msg) => write!(f, "Invalid default value: {}", msg),
            SqlParseError::UnexpectedEnd => write!(f, "Unexpected end of input"),
            SqlParseError::Expected(msg) => write!(f, "Expected: {}", msg),
        }
    }
}

impl std::error::Error for SqlParseError {}

// ============================================================================
// Tokenizer
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Keywords
    Create,
    Table,
    Alter,
    Add,
    Drop,
    Column,
    Rename,
    To,
    Not,
    Null,
    Default,
    True,
    False,
    References,
    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Semicolon,
    // Literals
    Ident(String),
    Number(String),
    StringLit(String),
}

struct Tokenizer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek_char() {
            self.pos += c.len_utf8();
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.advance();
            } else if c == '-' && self.input[self.pos..].starts_with("--") {
                // Skip line comment
                while let Some(c) = self.peek_char() {
                    self.advance();
                    if c == '\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_string()
    }

    fn read_number(&mut self) -> String {
        let start = self.pos;
        if self.peek_char() == Some('-') {
            self.advance();
        }
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() || c == '.' {
                self.advance();
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_string()
    }

    fn read_string(&mut self) -> Result<String, SqlParseError> {
        let quote = self.peek_char().unwrap();
        self.advance(); // consume opening quote
        let mut s = String::new();
        loop {
            match self.peek_char() {
                None => return Err(SqlParseError::SyntaxError("Unterminated string".into())),
                Some(c) if c == quote => {
                    self.advance();
                    // Check for escaped quote ('')
                    if self.peek_char() == Some(quote) {
                        s.push(quote);
                        self.advance();
                    } else {
                        break;
                    }
                }
                Some(c) => {
                    s.push(c);
                    self.advance();
                }
            }
        }
        Ok(s)
    }

    fn next_token(&mut self) -> Result<Option<Token>, SqlParseError> {
        self.skip_whitespace();

        let c = match self.peek_char() {
            None => return Ok(None),
            Some(c) => c,
        };

        let tok = match c {
            '(' => {
                self.advance();
                Token::LParen
            }
            ')' => {
                self.advance();
                Token::RParen
            }
            '[' => {
                self.advance();
                Token::LBracket
            }
            ']' => {
                self.advance();
                Token::RBracket
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            ';' => {
                self.advance();
                Token::Semicolon
            }
            '\'' | '"' => Token::StringLit(self.read_string()?),
            '-' if self.input[self.pos..].starts_with("-") && {
                let next = self.input[self.pos + 1..].chars().next();
                next.map(|c| c.is_ascii_digit()).unwrap_or(false)
            } =>
            {
                Token::Number(self.read_number())
            }
            c if c.is_ascii_digit() => Token::Number(self.read_number()),
            c if c.is_alphabetic() || c == '_' => {
                let ident = self.read_ident();
                match ident.to_uppercase().as_str() {
                    "CREATE" => Token::Create,
                    "TABLE" => Token::Table,
                    "ALTER" => Token::Alter,
                    "ADD" => Token::Add,
                    "DROP" => Token::Drop,
                    "COLUMN" => Token::Column,
                    "RENAME" => Token::Rename,
                    "TO" => Token::To,
                    "NOT" => Token::Not,
                    "NULL" => Token::Null,
                    "DEFAULT" => Token::Default,
                    "TRUE" => Token::True,
                    "FALSE" => Token::False,
                    "REFERENCES" => Token::References,
                    _ => Token::Ident(ident),
                }
            }
            _ => {
                return Err(SqlParseError::SyntaxError(format!(
                    "Unexpected char: {}",
                    c
                )));
            }
        };

        Ok(Some(tok))
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, SqlParseError> {
        let mut tokens = Vec::new();
        while let Some(tok) = self.next_token()? {
            tokens.push(tok);
        }
        Ok(tokens)
    }
}

// ============================================================================
// Parser
// ============================================================================

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), SqlParseError> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(t) => Err(SqlParseError::Expected(format!(
                "{:?}, got {:?}",
                expected, t
            ))),
            None => Err(SqlParseError::UnexpectedEnd),
        }
    }

    fn expect_ident(&mut self) -> Result<String, SqlParseError> {
        match self.advance() {
            Some(Token::Ident(s)) => Ok(s.clone()),
            Some(t) => Err(SqlParseError::Expected(format!("identifier, got {:?}", t))),
            None => Err(SqlParseError::UnexpectedEnd),
        }
    }

    fn parse_column_type(&mut self) -> Result<ColumnType, SqlParseError> {
        let type_name = self.expect_ident()?;

        // Skip optional size like VARCHAR(255)
        if self.peek() == Some(&Token::LParen) {
            self.advance();
            // Skip until closing paren
            while self.peek() != Some(&Token::RParen) {
                if self.advance().is_none() {
                    return Err(SqlParseError::UnexpectedEnd);
                }
            }
            self.advance(); // consume RParen
        }

        let mut col_type = match type_name.to_uppercase().as_str() {
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" => Ok(ColumnType::Text),
            "INTEGER" | "INT" | "SMALLINT" | "TINYINT" => Ok(ColumnType::Integer),
            "BIGINT" => Ok(ColumnType::BigInt),
            "BOOLEAN" | "BOOL" => Ok(ColumnType::Boolean),
            "TIMESTAMP" => Ok(ColumnType::Timestamp),
            "UUID" => Ok(ColumnType::Uuid),
            _ => Err(SqlParseError::UnsupportedType(type_name)),
        }?;

        // Optional array suffixes: UUID[], TEXT[][], etc.
        while self.peek() == Some(&Token::LBracket) {
            self.advance(); // consume '['
            self.expect(&Token::RBracket)?;
            col_type = ColumnType::Array(Box::new(col_type));
        }

        Ok(col_type)
    }

    fn parse_value(&mut self) -> Result<Value, SqlParseError> {
        match self.advance() {
            Some(Token::Null) => Ok(Value::Null),
            Some(Token::True) => Ok(Value::Boolean(true)),
            Some(Token::False) => Ok(Value::Boolean(false)),
            Some(Token::Number(n)) => {
                if let Ok(i) = n.parse::<i32>() {
                    Ok(Value::Integer(i))
                } else if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::BigInt(i))
                } else {
                    Err(SqlParseError::InvalidDefaultValue(format!(
                        "Cannot parse number: {}",
                        n
                    )))
                }
            }
            Some(Token::StringLit(s)) => Ok(Value::Text(s.clone())),
            Some(t) => Err(SqlParseError::InvalidDefaultValue(format!("{:?}", t))),
            None => Err(SqlParseError::UnexpectedEnd),
        }
    }

    fn parse_column_def(&mut self) -> Result<ColumnDescriptor, SqlParseError> {
        let name = self.expect_ident()?;
        let column_type = self.parse_column_type()?;

        let mut nullable = true;
        let mut references = None;

        // Parse optional modifiers: REFERENCES, NOT NULL, DEFAULT (in any order)
        loop {
            match self.peek() {
                Some(Token::Not) => {
                    self.advance();
                    self.expect(&Token::Null)?;
                    nullable = false;
                }
                Some(Token::Null) => {
                    self.advance();
                    nullable = true;
                }
                Some(Token::References) => {
                    self.advance();
                    let ref_table = self.expect_ident()?;
                    references = Some(TableName::new(ref_table));
                }
                Some(Token::Default) => {
                    // Skip DEFAULT in schema (we don't store defaults in schema, only in lenses)
                    self.advance();
                    self.parse_value()?; // consume and discard
                }
                _ => break,
            }
        }

        let mut desc = ColumnDescriptor::new(ColumnName::new(&name), column_type);
        if nullable {
            desc = desc.nullable();
        }
        if let Some(ref_table) = references {
            desc = desc.references(ref_table);
        }
        Ok(desc)
    }

    fn parse_column_def_with_default(
        &mut self,
    ) -> Result<(ColumnDescriptor, Value), SqlParseError> {
        let name = self.expect_ident()?;
        let column_type = self.parse_column_type()?;

        let mut nullable = true;
        let mut default = Value::Null;

        loop {
            match self.peek() {
                Some(Token::Not) => {
                    self.advance();
                    self.expect(&Token::Null)?;
                    nullable = false;
                }
                Some(Token::Null) => {
                    self.advance();
                    nullable = true;
                }
                Some(Token::Default) => {
                    self.advance();
                    default = self.parse_value()?;
                }
                _ => break,
            }
        }

        let mut desc = ColumnDescriptor::new(ColumnName::new(&name), column_type);
        if nullable {
            desc = desc.nullable();
        }
        Ok((desc, default))
    }

    fn parse_create_table(&mut self) -> Result<(String, TableSchema), SqlParseError> {
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            if self.peek() == Some(&Token::RParen) {
                break;
            }

            columns.push(self.parse_column_def()?);

            match self.peek() {
                Some(Token::Comma) => {
                    self.advance();
                }
                Some(Token::RParen) => break,
                Some(t) => return Err(SqlParseError::Expected(format!(", or ), got {:?}", t))),
                None => return Err(SqlParseError::UnexpectedEnd),
            }
        }

        self.expect(&Token::RParen)?;

        // Optional semicolon
        if self.peek() == Some(&Token::Semicolon) {
            self.advance();
        }

        Ok((table_name, TableSchema::new(RowDescriptor::new(columns))))
    }

    fn parse_alter_table(&mut self) -> Result<(LensOp, bool), SqlParseError> {
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;

        match self.peek() {
            Some(Token::Add) => {
                self.advance();
                self.expect(&Token::Column)?;
                let (col, default) = self.parse_column_def_with_default()?;

                // Optional semicolon
                if self.peek() == Some(&Token::Semicolon) {
                    self.advance();
                }

                Ok((
                    LensOp::AddColumn {
                        table: table_name,
                        column: col.name.as_str().to_string(),
                        column_type: col.column_type,
                        default,
                    },
                    false,
                ))
            }
            Some(Token::Drop) => {
                self.advance();
                self.expect(&Token::Column)?;
                let col_name = self.expect_ident()?;

                // Optional semicolon
                if self.peek() == Some(&Token::Semicolon) {
                    self.advance();
                }

                Ok((
                    LensOp::RemoveColumn {
                        table: table_name,
                        column: col_name,
                        column_type: ColumnType::Text, // Placeholder
                        default: Value::Null,
                    },
                    true, // Draft - needs type info
                ))
            }
            Some(Token::Rename) => {
                self.advance();
                self.expect(&Token::Column)?;
                let old_name = self.expect_ident()?;
                self.expect(&Token::To)?;
                let new_name = self.expect_ident()?;

                // Optional semicolon
                if self.peek() == Some(&Token::Semicolon) {
                    self.advance();
                }

                Ok((
                    LensOp::RenameColumn {
                        table: table_name,
                        old_name,
                        new_name,
                    },
                    false,
                ))
            }
            Some(t) => Err(SqlParseError::Expected(format!(
                "ADD, DROP, or RENAME, got {:?}",
                t
            ))),
            None => Err(SqlParseError::UnexpectedEnd),
        }
    }

    fn parse_drop_table(&mut self) -> Result<String, SqlParseError> {
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;

        // Optional semicolon
        if self.peek() == Some(&Token::Semicolon) {
            self.advance();
        }

        Ok(table_name)
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Parse a schema SQL file into a Schema.
pub fn parse_schema(sql: &str) -> Result<Schema, SqlParseError> {
    let tokens = Tokenizer::new(sql).tokenize()?;
    let mut parser = Parser::new(tokens);
    let mut schema = HashMap::new();

    while parser.peek().is_some() {
        match parser.peek() {
            Some(Token::Create) => {
                parser.advance();
                let (name, table_schema) = parser.parse_create_table()?;
                schema.insert(TableName::new(name), table_schema);
            }
            Some(t) => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "Only CREATE TABLE allowed in schema files, got {:?}",
                    t
                )));
            }
            None => break,
        }
    }

    Ok(schema)
}

/// Parse a lens SQL file into a LensTransform.
pub fn parse_lens(sql: &str) -> Result<LensTransform, SqlParseError> {
    let tokens = Tokenizer::new(sql).tokenize()?;
    let mut parser = Parser::new(tokens);
    let mut transform = LensTransform::new();

    while parser.peek().is_some() {
        match parser.peek() {
            Some(Token::Alter) => {
                parser.advance();
                let (op, is_draft) = parser.parse_alter_table()?;
                transform.push(op, is_draft);
            }
            Some(Token::Create) => {
                parser.advance();
                let (name, table_schema) = parser.parse_create_table()?;
                transform.push(
                    LensOp::AddTable {
                        table: name,
                        schema: table_schema,
                    },
                    false,
                );
            }
            Some(Token::Drop) => {
                parser.advance();
                let name = parser.parse_drop_table()?;
                transform.push(
                    LensOp::RemoveTable {
                        table: name,
                        schema: TableSchema::new(RowDescriptor::new(vec![])),
                    },
                    true, // Draft - needs schema info
                );
            }
            Some(t) => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "Only ALTER TABLE, CREATE TABLE, DROP TABLE allowed in lens files, got {:?}",
                    t
                )));
            }
            None => break,
        }
    }

    Ok(transform)
}

/// Generate SQL CREATE TABLE statements from a Schema.
pub fn schema_to_sql(schema: &Schema) -> String {
    let mut lines = Vec::new();

    // Sort tables for deterministic output
    let mut table_names: Vec<_> = schema.keys().collect();
    table_names.sort_by_key(|t| t.as_str());

    for table_name in table_names {
        let table_schema = &schema[table_name];
        lines.push(table_schema_to_sql(table_name.as_str(), table_schema));
    }

    lines.join("\n\n")
}

fn table_schema_to_sql(table_name: &str, schema: &TableSchema) -> String {
    let mut columns = Vec::new();

    for col in &schema.descriptor.columns {
        let col_sql = column_descriptor_to_sql(col);
        columns.push(format!("    {}", col_sql));
    }

    format!("CREATE TABLE {} (\n{}\n);", table_name, columns.join(",\n"))
}

fn column_descriptor_to_sql(col: &ColumnDescriptor) -> String {
    let type_str = column_type_to_sql(&col.column_type);
    let ref_str = match &col.references {
        Some(table) => format!(" REFERENCES {}", table.as_str()),
        None => String::new(),
    };
    let nullable_str = if col.nullable { "" } else { " NOT NULL" };

    format!(
        "{} {}{}{}",
        col.name.as_str(),
        type_str,
        ref_str,
        nullable_str
    )
}

/// Generate SQL ALTER TABLE statements from a LensTransform.
pub fn lens_to_sql(transform: &LensTransform) -> String {
    let mut lines = Vec::new();

    for (idx, op) in transform.ops.iter().enumerate() {
        let is_draft = transform.draft_ops.contains(&idx);
        let sql = lens_op_to_sql(op);

        if is_draft {
            lines.push(format!("-- TODO: Review\n{}", sql));
        } else {
            lines.push(sql);
        }
    }

    lines.join("\n")
}

fn lens_op_to_sql(op: &LensOp) -> String {
    match op {
        LensOp::AddColumn {
            table,
            column,
            column_type,
            default,
        } => {
            let type_str = column_type_to_sql(column_type);
            let default_str = value_to_sql(default);
            format!(
                "ALTER TABLE {} ADD COLUMN {} {} DEFAULT {};",
                table, column, type_str, default_str
            )
        }
        LensOp::RemoveColumn { table, column, .. } => {
            format!("ALTER TABLE {} DROP COLUMN {};", table, column)
        }
        LensOp::RenameColumn {
            table,
            old_name,
            new_name,
        } => {
            format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {};",
                table, old_name, new_name
            )
        }
        LensOp::AddTable { table, schema } => table_schema_to_sql(table, schema),
        LensOp::RemoveTable { table, .. } => {
            format!("DROP TABLE {};", table)
        }
    }
}

pub(crate) fn column_type_to_sql(ct: &ColumnType) -> String {
    match ct {
        ColumnType::Integer => "INTEGER".to_string(),
        ColumnType::BigInt => "BIGINT".to_string(),
        ColumnType::Boolean => "BOOLEAN".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Timestamp => "TIMESTAMP".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Array(elem) => format!("{}[]", column_type_to_sql(elem)),
        ColumnType::Row(_) => "TEXT".to_string(),
    }
}

fn value_to_sql(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Timestamp(t) => t.to_string(),
        Value::Uuid(id) => format!("'{:?}'", id),
        Value::Array(_) => "'[]'".to_string(),
        Value::Row(_) => "'{}'".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_create_table() {
        let sql = r#"
            CREATE TABLE todos (
                title TEXT NOT NULL,
                completed BOOLEAN NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.len(), 1);

        let todos = schema.get(&TableName::new("todos")).unwrap();
        assert_eq!(todos.descriptor.columns.len(), 2);

        let title = &todos.descriptor.columns[0];
        assert_eq!(title.name.as_str(), "title");
        assert_eq!(title.column_type, ColumnType::Text);
        assert!(!title.nullable);

        let completed = &todos.descriptor.columns[1];
        assert_eq!(completed.name.as_str(), "completed");
        assert_eq!(completed.column_type, ColumnType::Boolean);
        assert!(!completed.nullable);
    }

    #[test]
    fn parse_nullable_columns() {
        let sql = r#"
            CREATE TABLE users (
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let users = schema.get(&TableName::new("users")).unwrap();

        assert!(!users.descriptor.columns[0].nullable); // name
        assert!(users.descriptor.columns[1].nullable); // email
        assert!(users.descriptor.columns[2].nullable); // age
    }

    #[test]
    fn parse_multiple_tables() {
        let sql = r#"
            CREATE TABLE users (
                id UUID NOT NULL,
                name TEXT NOT NULL
            );

            CREATE TABLE posts (
                id UUID NOT NULL,
                title TEXT NOT NULL,
                author_id UUID NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.len(), 2);
        assert!(schema.contains_key(&TableName::new("users")));
        assert!(schema.contains_key(&TableName::new("posts")));
    }

    #[test]
    fn parse_add_column_lens() {
        let sql = "ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);

        match &transform.ops[0] {
            LensOp::AddColumn {
                table,
                column,
                column_type,
                default,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "age");
                assert_eq!(*column_type, ColumnType::Integer);
                assert_eq!(*default, Value::Integer(0));
            }
            _ => panic!("Expected AddColumn"),
        }
    }

    #[test]
    fn parse_drop_column_lens() {
        let sql = "ALTER TABLE users DROP COLUMN deprecated_field;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);
        assert!(transform.has_drafts());

        match &transform.ops[0] {
            LensOp::RemoveColumn { table, column, .. } => {
                assert_eq!(table, "users");
                assert_eq!(column, "deprecated_field");
            }
            _ => panic!("Expected RemoveColumn"),
        }
    }

    #[test]
    fn parse_rename_column_lens() {
        let sql = "ALTER TABLE users RENAME COLUMN email TO email_address;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);
        assert!(!transform.has_drafts());

        match &transform.ops[0] {
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => {
                assert_eq!(table, "users");
                assert_eq!(old_name, "email");
                assert_eq!(new_name, "email_address");
            }
            _ => panic!("Expected RenameColumn"),
        }
    }

    #[test]
    fn parse_create_table_lens() {
        let sql = r#"
            CREATE TABLE new_table (
                id TEXT NOT NULL,
                value INTEGER
            );
        "#;

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);

        match &transform.ops[0] {
            LensOp::AddTable { table, schema } => {
                assert_eq!(table, "new_table");
                assert_eq!(schema.descriptor.columns.len(), 2);
            }
            _ => panic!("Expected AddTable"),
        }
    }

    #[test]
    fn parse_drop_table_lens() {
        let sql = "DROP TABLE old_table;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);
        assert!(transform.has_drafts());

        match &transform.ops[0] {
            LensOp::RemoveTable { table, .. } => {
                assert_eq!(table, "old_table");
            }
            _ => panic!("Expected RemoveTable"),
        }
    }

    #[test]
    fn parse_multiple_lens_ops() {
        let sql = r#"
            ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;
            ALTER TABLE users DROP COLUMN deprecated_field;
            ALTER TABLE users RENAME COLUMN email TO email_address;
        "#;

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 3);

        assert!(matches!(&transform.ops[0], LensOp::AddColumn { .. }));
        assert!(matches!(&transform.ops[1], LensOp::RemoveColumn { .. }));
        assert!(matches!(&transform.ops[2], LensOp::RenameColumn { .. }));
    }

    #[test]
    fn schema_to_sql_roundtrip() {
        let sql = r#"CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);"#;

        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);
        let reparsed = parse_schema(&regenerated).unwrap();

        assert_eq!(schema.len(), reparsed.len());
        let todos = schema.get(&TableName::new("todos")).unwrap();
        let todos2 = reparsed.get(&TableName::new("todos")).unwrap();
        assert_eq!(
            todos.descriptor.columns.len(),
            todos2.descriptor.columns.len()
        );
    }

    #[test]
    fn lens_to_sql_add_column() {
        let transform = LensTransform::with_ops(vec![LensOp::AddColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            column_type: ColumnType::Integer,
            default: Value::Integer(0),
        }]);

        let sql = lens_to_sql(&transform);
        assert!(sql.contains("ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;"));
    }

    #[test]
    fn lens_to_sql_with_draft() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RemoveColumn {
                table: "users".to_string(),
                column: "old".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            true,
        );

        let sql = lens_to_sql(&transform);
        assert!(sql.contains("-- TODO: Review"));
        assert!(sql.contains("ALTER TABLE users DROP COLUMN old;"));
    }

    #[test]
    fn parse_default_values() {
        let sql = r#"
            ALTER TABLE users ADD COLUMN count INTEGER DEFAULT 42;
            ALTER TABLE users ADD COLUMN name TEXT DEFAULT 'unknown';
            ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE;
        "#;

        let transform = parse_lens(sql).unwrap();

        match &transform.ops[0] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Integer(42));
            }
            _ => panic!(),
        }

        match &transform.ops[1] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Text("unknown".to_string()));
            }
            _ => panic!(),
        }

        match &transform.ops[2] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Boolean(true));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_various_column_types() {
        let sql = r#"
            CREATE TABLE test (
                a TEXT NOT NULL,
                b INTEGER NOT NULL,
                c BIGINT NOT NULL,
                d BOOLEAN NOT NULL,
                e TIMESTAMP NOT NULL,
                f UUID NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("test")).unwrap();

        assert_eq!(table.descriptor.columns[0].column_type, ColumnType::Text);
        assert_eq!(table.descriptor.columns[1].column_type, ColumnType::Integer);
        assert_eq!(table.descriptor.columns[2].column_type, ColumnType::BigInt);
        assert_eq!(table.descriptor.columns[3].column_type, ColumnType::Boolean);
        assert_eq!(
            table.descriptor.columns[4].column_type,
            ColumnType::Timestamp
        );
        assert_eq!(table.descriptor.columns[5].column_type, ColumnType::Uuid);
    }

    #[test]
    fn parse_uuid_array_column_with_references() {
        let sql = "CREATE TABLE files (parts UUID[] REFERENCES file_parts NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("files"))
            .unwrap()
            .descriptor
            .columns[0];

        assert_eq!(col.name.as_str(), "parts");
        assert_eq!(
            col.column_type,
            ColumnType::Array(Box::new(ColumnType::Uuid))
        );
        assert_eq!(col.references, Some(TableName::new("file_parts")));
        assert!(!col.nullable);
    }

    #[test]
    fn parse_nested_array_column_type() {
        let sql = "CREATE TABLE t (matrix INTEGER[][] NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema.get(&TableName::new("t")).unwrap().descriptor.columns[0];
        assert_eq!(
            col.column_type,
            ColumnType::Array(Box::new(ColumnType::Array(Box::new(ColumnType::Integer))))
        );
        assert!(!col.nullable);
    }

    #[test]
    fn reject_non_create_in_schema() {
        let sql = "ALTER TABLE users ADD COLUMN age INTEGER;";

        let result = parse_schema(sql);
        assert!(result.is_err());
    }

    #[test]
    fn reject_select_in_lens() {
        let sql = "SELECT * FROM users;";

        let result = parse_lens(sql);
        assert!(result.is_err());
    }

    #[test]
    fn parse_with_comments() {
        let sql = r#"
            -- This is a comment
            CREATE TABLE todos (
                title TEXT NOT NULL -- inline comment not supported but line comments are
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn parse_negative_default() {
        let sql = "ALTER TABLE t ADD COLUMN x INTEGER DEFAULT -42;";
        let transform = parse_lens(sql).unwrap();

        match &transform.ops[0] {
            LensOp::AddColumn { default, .. } => {
                assert_eq!(*default, Value::Integer(-42));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_column_with_references() {
        let sql = "CREATE TABLE todos (owner_id UUID REFERENCES users NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("todos"))
            .unwrap()
            .descriptor
            .columns[0];

        assert_eq!(col.name.as_str(), "owner_id");
        assert_eq!(col.column_type, ColumnType::Uuid);
        assert_eq!(col.references, Some(TableName::new("users")));
        assert!(!col.nullable);
    }

    #[test]
    fn parse_nullable_column_with_references() {
        let sql = "CREATE TABLE todos (parent_id UUID REFERENCES todos);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("todos"))
            .unwrap()
            .descriptor
            .columns[0];

        assert_eq!(col.references, Some(TableName::new("todos")));
        assert!(col.nullable);
    }

    #[test]
    fn parse_references_before_not_null() {
        // Order: REFERENCES then NOT NULL
        let sql = "CREATE TABLE t (fk UUID REFERENCES other NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema.get(&TableName::new("t")).unwrap().descriptor.columns[0];

        assert_eq!(col.references, Some(TableName::new("other")));
        assert!(!col.nullable);
    }

    #[test]
    fn parse_not_null_before_references() {
        // Order: NOT NULL then REFERENCES (should also work)
        let sql = "CREATE TABLE t (fk UUID NOT NULL REFERENCES other);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema.get(&TableName::new("t")).unwrap().descriptor.columns[0];

        assert_eq!(col.references, Some(TableName::new("other")));
        assert!(!col.nullable);
    }

    #[test]
    fn schema_to_sql_includes_references() {
        let mut schema = HashMap::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new(ColumnName::new("owner_id"), ColumnType::Uuid)
                    .references(TableName::new("users")),
            ])),
        );
        let sql = schema_to_sql(&schema);

        assert!(sql.contains("owner_id UUID REFERENCES users NOT NULL"));
    }

    #[test]
    fn schema_to_sql_nullable_with_references() {
        let mut schema = HashMap::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new(ColumnName::new("parent_id"), ColumnType::Uuid)
                    .nullable()
                    .references(TableName::new("todos")),
            ])),
        );
        let sql = schema_to_sql(&schema);

        assert!(sql.contains("parent_id UUID REFERENCES todos"));
        assert!(!sql.contains("parent_id UUID REFERENCES todos NOT NULL"));
    }

    #[test]
    fn sql_round_trip_with_references() {
        let sql = "CREATE TABLE todos (owner_id UUID REFERENCES users NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);

        assert!(regenerated.contains("REFERENCES users"));
        assert!(regenerated.contains("NOT NULL"));

        // Parse regenerated SQL and verify
        let reparsed = parse_schema(&regenerated).unwrap();
        let col = &reparsed
            .get(&TableName::new("todos"))
            .unwrap()
            .descriptor
            .columns[0];
        assert_eq!(col.references, Some(TableName::new("users")));
        assert!(!col.nullable);
    }

    #[test]
    fn sql_round_trip_with_array_references() {
        let sql = "CREATE TABLE files (parts UUID[] REFERENCES file_parts NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);

        assert!(regenerated.contains("parts UUID[] REFERENCES file_parts NOT NULL"));

        let reparsed = parse_schema(&regenerated).unwrap();
        let col = &reparsed
            .get(&TableName::new("files"))
            .unwrap()
            .descriptor
            .columns[0];
        assert_eq!(
            col.column_type,
            ColumnType::Array(Box::new(ColumnType::Uuid))
        );
        assert_eq!(col.references, Some(TableName::new("file_parts")));
        assert!(!col.nullable);
    }

    #[test]
    fn parse_table_with_mixed_columns_and_refs() {
        let sql = r#"
            CREATE TABLE todos (
                title TEXT NOT NULL,
                parent_id UUID REFERENCES todos,
                owner_id UUID REFERENCES users NOT NULL
            );
        "#;
        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("todos")).unwrap();

        assert_eq!(table.descriptor.columns.len(), 3);

        let title = &table.descriptor.columns[0];
        assert_eq!(title.name.as_str(), "title");
        assert_eq!(title.column_type, ColumnType::Text);
        assert!(title.references.is_none());
        assert!(!title.nullable);

        let parent = &table.descriptor.columns[1];
        assert_eq!(parent.name.as_str(), "parent_id");
        assert_eq!(parent.references, Some(TableName::new("todos")));
        assert!(parent.nullable);

        let owner = &table.descriptor.columns[2];
        assert_eq!(owner.name.as_str(), "owner_id");
        assert_eq!(owner.references, Some(TableName::new("users")));
        assert!(!owner.nullable);
    }
}
