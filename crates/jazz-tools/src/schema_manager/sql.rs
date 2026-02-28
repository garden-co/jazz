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

use crate::query_manager::{
    policy::{CmpOp, Operation, PolicyExpr, PolicyValue},
    types::{
        ColumnDescriptor, ColumnName, ColumnType, OperationPolicy, RowDescriptor, Schema,
        TableName, TablePolicies, TableSchema, Value,
    },
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
    Policy,
    On,
    For,
    Using,
    With,
    Check,
    Session,
    Inherits,
    Via,
    Referencing,
    Select,
    Insert,
    Update,
    Delete,
    And,
    Or,
    In,
    Contains,
    Is,
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
    Dot,
    At,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
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
            '.' => {
                self.advance();
                Token::Dot
            }
            '@' => {
                self.advance();
                Token::At
            }
            '=' => {
                self.advance();
                Token::Eq
            }
            '!' if self.input[self.pos..].starts_with("!=") => {
                self.advance();
                self.advance();
                Token::Ne
            }
            '<' if self.input[self.pos..].starts_with("<=") => {
                self.advance();
                self.advance();
                Token::Le
            }
            '>' if self.input[self.pos..].starts_with(">=") => {
                self.advance();
                self.advance();
                Token::Ge
            }
            '<' => {
                self.advance();
                Token::Lt
            }
            '>' => {
                self.advance();
                Token::Gt
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
                    "POLICY" => Token::Policy,
                    "ON" => Token::On,
                    "FOR" => Token::For,
                    "USING" => Token::Using,
                    "WITH" => Token::With,
                    "CHECK" => Token::Check,
                    "SESSION" => Token::Session,
                    "INHERITS" | "INHERIT" => Token::Inherits,
                    "VIA" => Token::Via,
                    "REFERENCING" => Token::Referencing,
                    "SELECT" => Token::Select,
                    "INSERT" => Token::Insert,
                    "UPDATE" => Token::Update,
                    "DELETE" => Token::Delete,
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "IN" => Token::In,
                    "CONTAINS" => Token::Contains,
                    "IS" => Token::Is,
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
        let upper = type_name.to_uppercase();

        let mut col_type = if upper == "ENUM" {
            self.expect(&Token::LParen)?;
            let mut variants = Vec::new();
            loop {
                match self.advance() {
                    Some(Token::StringLit(variant)) => variants.push(variant.clone()),
                    Some(t) => {
                        return Err(SqlParseError::Expected(format!(
                            "enum variant string literal, got {:?}",
                            t
                        )));
                    }
                    None => return Err(SqlParseError::UnexpectedEnd),
                }

                match self.peek() {
                    Some(Token::Comma) => {
                        self.advance();
                    }
                    Some(Token::RParen) => {
                        self.advance();
                        break;
                    }
                    Some(t) => {
                        return Err(SqlParseError::Expected(format!(
                            ", or ) in ENUM type, got {:?}",
                            t
                        )));
                    }
                    None => return Err(SqlParseError::UnexpectedEnd),
                }
            }
            if variants.is_empty() {
                return Err(SqlParseError::SyntaxError(
                    "ENUM type requires at least one variant".to_string(),
                ));
            }
            ColumnType::Enum { variants }
        } else if upper == "JSON" {
            if self.peek() == Some(&Token::LParen) {
                self.advance();
                let raw_schema = match self.advance() {
                    Some(Token::StringLit(value)) => value.clone(),
                    Some(t) => {
                        return Err(SqlParseError::Expected(format!(
                            "JSON schema string literal, got {:?}",
                            t
                        )));
                    }
                    None => return Err(SqlParseError::UnexpectedEnd),
                };
                self.expect(&Token::RParen)?;
                let parsed_schema = serde_json::from_str(&raw_schema).map_err(|err| {
                    SqlParseError::SyntaxError(format!("Invalid JSON schema payload: {err}"))
                })?;
                ColumnType::Json {
                    schema: Some(parsed_schema),
                }
            } else {
                ColumnType::Json { schema: None }
            }
        } else {
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

            match upper.as_str() {
                "TEXT" | "VARCHAR" | "CHAR" | "STRING" => ColumnType::Text,
                "INTEGER" | "INT" | "SMALLINT" | "TINYINT" => ColumnType::Integer,
                "BIGINT" => ColumnType::BigInt,
                "REAL" | "FLOAT" | "DOUBLE" => ColumnType::Double,
                "BOOLEAN" | "BOOL" => ColumnType::Boolean,
                "TIMESTAMP" => ColumnType::Timestamp,
                "UUID" => ColumnType::Uuid,
                "BYTEA" => ColumnType::Bytea,
                _ => return Err(SqlParseError::UnsupportedType(type_name)),
            }
        };

        // Optional array suffixes: UUID[], TEXT[][], etc.
        while self.peek() == Some(&Token::LBracket) {
            self.advance(); // consume '['
            self.expect(&Token::RBracket)?;
            col_type = ColumnType::Array {
                element: Box::new(col_type),
            };
        }

        Ok(col_type)
    }

    fn parse_policy_operation(&mut self) -> Result<Operation, SqlParseError> {
        match self.advance() {
            Some(Token::Select) => Ok(Operation::Select),
            Some(Token::Insert) => Ok(Operation::Insert),
            Some(Token::Update) => Ok(Operation::Update),
            Some(Token::Delete) => Ok(Operation::Delete),
            Some(t) => Err(SqlParseError::Expected(format!(
                "SELECT, INSERT, UPDATE, or DELETE, got {:?}",
                t
            ))),
            None => Err(SqlParseError::UnexpectedEnd),
        }
    }

    fn parse_session_path(&mut self) -> Result<Vec<String>, SqlParseError> {
        self.expect(&Token::At)?;
        match self.advance() {
            Some(Token::Session) => {}
            Some(Token::Ident(name)) if name.eq_ignore_ascii_case("session") => {}
            Some(t) => {
                return Err(SqlParseError::Expected(format!(
                    "session reference, got {:?}",
                    t
                )));
            }
            None => return Err(SqlParseError::UnexpectedEnd),
        }

        self.expect(&Token::Dot)?;
        let mut path = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Dot) {
            self.advance();
            path.push(self.expect_ident()?);
        }
        Ok(path)
    }

    fn parse_policy_value(&mut self) -> Result<PolicyValue, SqlParseError> {
        if self.peek() == Some(&Token::At) {
            return Ok(PolicyValue::SessionRef(self.parse_session_path()?));
        }

        Ok(PolicyValue::Literal(self.parse_value()?))
    }

    fn parse_policy_in_list(&mut self) -> Result<Vec<PolicyValue>, SqlParseError> {
        self.expect(&Token::LParen)?;
        if self.peek() == Some(&Token::RParen) {
            return Err(SqlParseError::Expected(
                "at least one value in IN (...) policy list".to_string(),
            ));
        }

        let mut values = vec![self.parse_policy_value()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            values.push(self.parse_policy_value()?);
        }

        self.expect(&Token::RParen)?;
        Ok(values)
    }

    fn parse_policy_expr(&mut self) -> Result<PolicyExpr, SqlParseError> {
        self.parse_policy_or()
    }

    fn parse_policy_or(&mut self) -> Result<PolicyExpr, SqlParseError> {
        let mut exprs = vec![self.parse_policy_and()?];

        while self.peek() == Some(&Token::Or) {
            self.advance();
            exprs.push(self.parse_policy_and()?);
        }

        if exprs.len() == 1 {
            Ok(exprs.pop().unwrap())
        } else {
            Ok(PolicyExpr::Or(exprs))
        }
    }

    fn parse_policy_and(&mut self) -> Result<PolicyExpr, SqlParseError> {
        let mut exprs = vec![self.parse_policy_primary()?];

        while self.peek() == Some(&Token::And) {
            self.advance();
            exprs.push(self.parse_policy_primary()?);
        }

        if exprs.len() == 1 {
            Ok(exprs.pop().unwrap())
        } else {
            Ok(PolicyExpr::And(exprs))
        }
    }

    fn parse_policy_primary(&mut self) -> Result<PolicyExpr, SqlParseError> {
        match self.peek() {
            Some(Token::LParen) => {
                self.advance();
                let expr = self.parse_policy_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::Not) => {
                self.advance();
                Ok(PolicyExpr::Not(Box::new(self.parse_policy_primary()?)))
            }
            Some(Token::True) => {
                self.advance();
                Ok(PolicyExpr::True)
            }
            Some(Token::False) => {
                self.advance();
                Ok(PolicyExpr::False)
            }
            Some(Token::Inherits) => {
                self.advance();
                let operation = self.parse_policy_operation()?;
                if self.peek() == Some(&Token::Referencing) {
                    self.advance();
                    let source_table = self.expect_ident()?;
                    self.expect(&Token::Via)?;
                    let via_column = self.expect_ident()?;
                    Ok(PolicyExpr::InheritsReferencing {
                        operation,
                        source_table,
                        via_column,
                        max_depth: None,
                    })
                } else {
                    self.expect(&Token::Via)?;
                    let via_column = self.expect_ident()?;
                    Ok(PolicyExpr::Inherits {
                        operation,
                        via_column,
                        max_depth: None,
                    })
                }
            }
            Some(Token::Ident(_)) => {
                let column = self.expect_ident()?;

                match self.peek() {
                    Some(Token::Eq) => {
                        self.advance();
                        Ok(PolicyExpr::Cmp {
                            column,
                            op: CmpOp::Eq,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::Ne) => {
                        self.advance();
                        Ok(PolicyExpr::Cmp {
                            column,
                            op: CmpOp::Ne,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::Lt) => {
                        self.advance();
                        Ok(PolicyExpr::Cmp {
                            column,
                            op: CmpOp::Lt,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::Le) => {
                        self.advance();
                        Ok(PolicyExpr::Cmp {
                            column,
                            op: CmpOp::Le,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::Gt) => {
                        self.advance();
                        Ok(PolicyExpr::Cmp {
                            column,
                            op: CmpOp::Gt,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::Ge) => {
                        self.advance();
                        Ok(PolicyExpr::Cmp {
                            column,
                            op: CmpOp::Ge,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::Contains) => {
                        self.advance();
                        Ok(PolicyExpr::Contains {
                            column,
                            value: self.parse_policy_value()?,
                        })
                    }
                    Some(Token::In) => {
                        self.advance();
                        match self.peek() {
                            Some(Token::At) => Ok(PolicyExpr::In {
                                column,
                                session_path: self.parse_session_path()?,
                            }),
                            Some(Token::LParen) => Ok(PolicyExpr::InList {
                                column,
                                values: self.parse_policy_in_list()?,
                            }),
                            Some(t) => Err(SqlParseError::Expected(format!(
                                "session reference or IN (...) value list after IN, got {:?}",
                                t
                            ))),
                            None => Err(SqlParseError::UnexpectedEnd),
                        }
                    }
                    Some(Token::Is) => {
                        self.advance();
                        if self.peek() == Some(&Token::Not) {
                            self.advance();
                            self.expect(&Token::Null)?;
                            Ok(PolicyExpr::IsNotNull { column })
                        } else {
                            self.expect(&Token::Null)?;
                            Ok(PolicyExpr::IsNull { column })
                        }
                    }
                    Some(t) => Err(SqlParseError::Expected(format!(
                        "policy operator after column, got {:?}",
                        t
                    ))),
                    None => Err(SqlParseError::UnexpectedEnd),
                }
            }
            Some(t) => Err(SqlParseError::Expected(format!(
                "policy expression, got {:?}",
                t
            ))),
            None => Err(SqlParseError::UnexpectedEnd),
        }
    }

    fn parse_policy_clause_expr(&mut self) -> Result<PolicyExpr, SqlParseError> {
        if self.peek() == Some(&Token::LParen) {
            self.advance();
            let expr = self.parse_policy_expr()?;
            self.expect(&Token::RParen)?;
            Ok(expr)
        } else {
            self.parse_policy_expr()
        }
    }

    fn parse_value(&mut self) -> Result<Value, SqlParseError> {
        match self.advance() {
            Some(Token::Null) => Ok(Value::Null),
            Some(Token::True) => Ok(Value::Boolean(true)),
            Some(Token::False) => Ok(Value::Boolean(false)),
            Some(Token::Number(n)) => {
                if n.contains('.') {
                    if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::Double(f))
                    } else {
                        Err(SqlParseError::InvalidDefaultValue(format!(
                            "Cannot parse float: {}",
                            n
                        )))
                    }
                } else if let Ok(i) = n.parse::<i32>() {
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

    fn parse_create_policy(
        &mut self,
    ) -> Result<(String, Operation, OperationPolicy), SqlParseError> {
        self.expect(&Token::Policy)?;
        // Policy name is currently informational only.
        let _policy_name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table_name = self.expect_ident()?;
        self.expect(&Token::For)?;
        let operation = self.parse_policy_operation()?;

        let mut using = None;
        let mut with_check = None;

        loop {
            match self.peek() {
                Some(Token::Using) => {
                    self.advance();
                    using = Some(self.parse_policy_clause_expr()?);
                }
                Some(Token::With) => {
                    self.advance();
                    self.expect(&Token::Check)?;
                    with_check = Some(self.parse_policy_clause_expr()?);
                }
                _ => break,
            }
        }

        // Optional semicolon
        if self.peek() == Some(&Token::Semicolon) {
            self.advance();
        }

        Ok((table_name, operation, OperationPolicy { using, with_check }))
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

fn is_valid_reference_column_type(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Uuid => true,
        ColumnType::Array {
            element: element_type,
        } => matches!(element_type.as_ref(), ColumnType::Uuid),
        _ => false,
    }
}

fn validate_schema_references(schema: &Schema) -> Result<(), SqlParseError> {
    for (table_name, table_schema) in schema {
        for column in &table_schema.columns.columns {
            let Some(referenced_table) = column.references else {
                continue;
            };

            if !is_valid_reference_column_type(&column.column_type) {
                return Err(SqlParseError::SyntaxError(format!(
                    "column '{}.{}' declares REFERENCES but has type {:?}; only UUID and UUID[] support REFERENCES",
                    table_name.as_str(),
                    column.name.as_str(),
                    column.column_type
                )));
            }

            let _ = referenced_table;
        }
    }

    Ok(())
}

/// Parse a schema SQL file into a Schema.
pub fn parse_schema(sql: &str) -> Result<Schema, SqlParseError> {
    let tokens = Tokenizer::new(sql).tokenize()?;
    let mut parser = Parser::new(tokens);
    let mut schema = HashMap::new();

    fn validate_policy_expr_for_bytea(
        descriptor: &RowDescriptor,
        expr: &PolicyExpr,
    ) -> Result<(), SqlParseError> {
        match expr {
            PolicyExpr::Cmp { column, op, .. } => {
                if matches!(op, CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge)
                    && descriptor
                        .column(column)
                        .is_some_and(|col| matches!(col.column_type, ColumnType::Bytea))
                {
                    return Err(SqlParseError::UnsupportedType(format!(
                        "BYTEA column '{}' only supports '=' and '!=' comparisons",
                        column
                    )));
                }
                Ok(())
            }
            PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => {
                for inner in exprs {
                    validate_policy_expr_for_bytea(descriptor, inner)?;
                }
                Ok(())
            }
            PolicyExpr::Not(inner) => validate_policy_expr_for_bytea(descriptor, inner),
            PolicyExpr::IsNull { .. }
            | PolicyExpr::IsNotNull { .. }
            | PolicyExpr::Contains { .. }
            | PolicyExpr::In { .. }
            | PolicyExpr::InList { .. }
            | PolicyExpr::Exists { .. }
            | PolicyExpr::ExistsRel { .. }
            | PolicyExpr::Inherits { .. }
            | PolicyExpr::InheritsReferencing { .. }
            | PolicyExpr::True
            | PolicyExpr::False => Ok(()),
        }
    }

    fn validate_operation_policy_for_bytea(
        descriptor: &RowDescriptor,
        policy: &OperationPolicy,
    ) -> Result<(), SqlParseError> {
        if let Some(using_expr) = &policy.using {
            validate_policy_expr_for_bytea(descriptor, using_expr)?;
        }
        if let Some(with_check_expr) = &policy.with_check {
            validate_policy_expr_for_bytea(descriptor, with_check_expr)?;
        }
        Ok(())
    }

    fn apply_policy(
        schema: &mut Schema,
        table_name: String,
        operation: Operation,
        policy: OperationPolicy,
    ) -> Result<(), SqlParseError> {
        let table_key = TableName::new(table_name.clone());
        let table_schema = schema.get_mut(&table_key).ok_or_else(|| {
            SqlParseError::SyntaxError(format!(
                "CREATE POLICY references unknown table '{}'",
                table_name
            ))
        })?;
        validate_operation_policy_for_bytea(&table_schema.columns, &policy)?;

        match operation {
            Operation::Select => table_schema.policies.select = policy,
            Operation::Insert => table_schema.policies.insert = policy,
            Operation::Update => table_schema.policies.update = policy,
            Operation::Delete => table_schema.policies.delete = policy,
        }

        Ok(())
    }

    while parser.peek().is_some() {
        match parser.peek() {
            Some(Token::Create) => {
                parser.advance();
                match parser.peek() {
                    Some(Token::Table) => {
                        let (name, table_schema) = parser.parse_create_table()?;
                        schema.insert(TableName::new(name), table_schema);
                    }
                    Some(Token::Policy) => {
                        let (table_name, operation, policy) = parser.parse_create_policy()?;
                        apply_policy(&mut schema, table_name, operation, policy)?;
                    }
                    Some(t) => {
                        return Err(SqlParseError::UnsupportedStatement(format!(
                            "Only CREATE TABLE and CREATE POLICY allowed in schema files, got {:?}",
                            t
                        )));
                    }
                    None => return Err(SqlParseError::UnexpectedEnd),
                }
            }
            Some(t) => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "Only CREATE TABLE and CREATE POLICY allowed in schema files, got {:?}",
                    t
                )));
            }
            None => break,
        }
    }

    validate_schema_references(&schema)?;

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
    let mut blocks = Vec::new();

    // Sort tables for deterministic output
    let mut table_names: Vec<_> = schema.keys().collect();
    table_names.sort_by_key(|t| t.as_str());

    for table_name in table_names {
        let table_schema = &schema[table_name];
        let mut block = vec![table_schema_to_sql(table_name.as_str(), table_schema)];
        block.extend(table_policies_to_sql(
            table_name.as_str(),
            &table_schema.policies,
        ));
        blocks.push(block.join("\n"));
    }

    blocks.join("\n\n")
}

fn table_schema_to_sql(table_name: &str, schema: &TableSchema) -> String {
    let mut columns = Vec::new();

    for col in &schema.columns.columns {
        let col_sql = column_descriptor_to_sql(col);
        columns.push(format!("    {}", col_sql));
    }

    format!("CREATE TABLE {} (\n{}\n);", table_name, columns.join(",\n"))
}

fn table_policies_to_sql(table_name: &str, policies: &TablePolicies) -> Vec<String> {
    let mut statements = Vec::new();

    let ops = [
        ("select", "SELECT", &policies.select),
        ("insert", "INSERT", &policies.insert),
        ("update", "UPDATE", &policies.update),
        ("delete", "DELETE", &policies.delete),
    ];

    for (name, sql_op, policy) in ops {
        let clauses = operation_policy_to_sql(policy);
        if clauses.is_empty() {
            continue;
        }
        statements.push(format!(
            "CREATE POLICY {}_{}_policy ON {} FOR {} {};",
            table_name,
            name,
            table_name,
            sql_op,
            clauses.join(" ")
        ));
    }

    statements
}

fn operation_policy_to_sql(policy: &OperationPolicy) -> Vec<String> {
    let mut clauses = Vec::new();
    if let Some(expr) = &policy.using {
        clauses.push(format!("USING ({})", policy_expr_to_sql(expr)));
    }
    if let Some(expr) = &policy.with_check {
        clauses.push(format!("WITH CHECK ({})", policy_expr_to_sql(expr)));
    }
    clauses
}

fn policy_expr_to_sql(expr: &PolicyExpr) -> String {
    match expr {
        PolicyExpr::Cmp { column, op, value } => {
            format!(
                "{} {} {}",
                column,
                cmp_op_to_sql(op),
                policy_value_to_sql(value)
            )
        }
        PolicyExpr::IsNull { column } => format!("{} IS NULL", column),
        PolicyExpr::IsNotNull { column } => format!("{} IS NOT NULL", column),
        PolicyExpr::Contains { column, value } => {
            format!("{} CONTAINS {}", column, policy_value_to_sql(value))
        }
        PolicyExpr::In {
            column,
            session_path,
        } => format!("{} IN @session.{}", column, session_path.join(".")),
        PolicyExpr::InList { column, values } => format!(
            "{} IN ({})",
            column,
            values
                .iter()
                .map(policy_value_to_sql)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PolicyExpr::Exists { table, condition } => {
            format!(
                "EXISTS (SELECT FROM {} WHERE {})",
                table,
                policy_expr_to_sql(condition)
            )
        }
        PolicyExpr::ExistsRel { .. } => "EXISTS_REL(<relation_ir>)".to_string(),
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => match max_depth {
            Some(depth) => format!(
                "INHERITS {} VIA {} MAX DEPTH {}",
                operation_to_sql(*operation),
                via_column,
                depth
            ),
            None => format!(
                "INHERITS {} VIA {}",
                operation_to_sql(*operation),
                via_column
            ),
        },
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => match max_depth {
            Some(depth) => format!(
                "INHERITS {} REFERENCING {} VIA {} MAX DEPTH {}",
                operation.to_string().to_uppercase(),
                source_table,
                via_column,
                depth
            ),
            None => format!(
                "INHERITS {} REFERENCING {} VIA {}",
                operation.to_string().to_uppercase(),
                source_table,
                via_column
            ),
        },
        PolicyExpr::And(exprs) => exprs
            .iter()
            .map(|e| format!("({})", policy_expr_to_sql(e)))
            .collect::<Vec<_>>()
            .join(" AND "),
        PolicyExpr::Or(exprs) => exprs
            .iter()
            .map(|e| format!("({})", policy_expr_to_sql(e)))
            .collect::<Vec<_>>()
            .join(" OR "),
        PolicyExpr::Not(expr) => format!("NOT ({})", policy_expr_to_sql(expr)),
        PolicyExpr::True => "TRUE".to_string(),
        PolicyExpr::False => "FALSE".to_string(),
    }
}

fn policy_value_to_sql(value: &PolicyValue) -> String {
    match value {
        PolicyValue::Literal(value) => value_to_sql(value),
        PolicyValue::SessionRef(path) => format!("@session.{}", path.join(".")),
    }
}

fn cmp_op_to_sql(op: &CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    }
}

fn operation_to_sql(operation: Operation) -> &'static str {
    match operation {
        Operation::Select => "SELECT",
        Operation::Insert => "INSERT",
        Operation::Update => "UPDATE",
        Operation::Delete => "DELETE",
    }
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
        ColumnType::Double => "REAL".to_string(),
        ColumnType::Boolean => "BOOLEAN".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Enum { variants } => {
            let variants = variants
                .iter()
                .map(|variant| format!("'{}'", variant.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");
            format!("ENUM({variants})")
        }
        ColumnType::Timestamp => "TIMESTAMP".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Bytea => "BYTEA".to_string(),
        ColumnType::Json { schema } => {
            if let Some(schema) = schema {
                format!("JSON('{}')", schema.to_string().replace('\'', "''"))
            } else {
                "JSON".to_string()
            }
        }
        ColumnType::Array { element: elem } => format!("{}[]", column_type_to_sql(elem)),
        ColumnType::Row { columns: _ } => "TEXT".to_string(),
    }
}

fn value_to_sql(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Double(f) => {
            assert!(f.is_finite(), "non-finite float in value_to_sql: {f}");
            format!("{f:?}")
        }
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Timestamp(t) => t.to_string(),
        Value::Uuid(id) => format!("'{:?}'", id),
        Value::Bytea(bytes) => format!("'\\\\x{}'", hex::encode(bytes)),
        Value::Array(_) => "'[]'".to_string(),
        Value::Row(_) => "'{}'".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        assert_eq!(todos.columns.columns.len(), 2);

        let title = &todos.columns.columns[0];
        assert_eq!(title.name.as_str(), "title");
        assert_eq!(title.column_type, ColumnType::Text);
        assert!(!title.nullable);

        let completed = &todos.columns.columns[1];
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

        assert!(!users.columns.columns[0].nullable); // name
        assert!(users.columns.columns[1].nullable); // email
        assert!(users.columns.columns[2].nullable); // age
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
    fn parse_create_policy_statements() {
        let sql = r#"
            CREATE TABLE todos (
                title TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                project_id UUID REFERENCES projects
            );

            CREATE POLICY todos_select_policy ON todos FOR SELECT
                USING (owner_id = @session.user_id OR INHERITS SELECT VIA project_id);
            CREATE POLICY todos_insert_policy ON todos FOR INSERT
                WITH CHECK (owner_id = @session.user_id);
            CREATE POLICY todos_update_policy ON todos FOR UPDATE
                USING (owner_id = @session.user_id)
                WITH CHECK (owner_id = @session.user_id);
            CREATE POLICY todos_delete_policy ON todos FOR DELETE
                USING (owner_id = @session.user_id);
        "#;

        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("todos")).unwrap();

        match &table.policies.select.using {
            Some(PolicyExpr::Or(exprs)) => {
                assert_eq!(exprs.len(), 2);
            }
            other => panic!("expected SELECT OR policy, got {:?}", other),
        }

        match &table.policies.insert.with_check {
            Some(PolicyExpr::Cmp { column, .. }) => assert_eq!(column, "owner_id"),
            other => panic!("expected INSERT CHECK policy, got {:?}", other),
        }

        assert!(table.policies.update.using.is_some());
        assert!(table.policies.update.with_check.is_some());
        assert!(table.policies.delete.using.is_some());
    }

    #[test]
    fn parse_policy_rejects_range_comparison_on_bytea() {
        let sql = r#"
            CREATE TABLE files (
                id UUID NOT NULL,
                data BYTEA NOT NULL
            );
            CREATE POLICY files_select_policy ON files FOR SELECT
                USING (data < 'abc');
        "#;

        let err = parse_schema(sql).unwrap_err();
        assert!(matches!(err, SqlParseError::UnsupportedType(_)));
        assert!(
            err.to_string()
                .contains("BYTEA column 'data' only supports '=' and '!='")
        );
    }

    #[test]
    fn parse_create_policy_with_inherits_referencing() {
        let sql = r#"
            CREATE TABLE files (
                owner_id TEXT NOT NULL
            );

            CREATE TABLE todos (
                owner_id TEXT NOT NULL,
                image UUID REFERENCES files
            );

            CREATE POLICY files_select_policy ON files FOR SELECT
                USING (owner_id = @session.user_id OR INHERITS SELECT REFERENCING todos VIA image);
        "#;

        let schema = parse_schema(sql).unwrap();
        let files = schema.get(&TableName::new("files")).unwrap();
        let using = files
            .policies
            .select
            .using
            .as_ref()
            .expect("missing select policy");
        match using {
            PolicyExpr::Or(exprs) => {
                assert!(exprs.iter().any(|expr| {
                    matches!(
                        expr,
                        PolicyExpr::InheritsReferencing {
                            operation: Operation::Select,
                            source_table,
                            via_column,
                            max_depth: None,
                        } if source_table == "todos" && via_column == "image"
                    )
                }));
            }
            other => panic!("expected OR policy, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_policy_with_contains_and_in_list() {
        let sql = r#"
            CREATE TABLE todos (
                owner_id TEXT NOT NULL,
                status TEXT NOT NULL
            );

            CREATE POLICY todos_select_policy ON todos FOR SELECT
                USING (owner_id CONTAINS 'ali' AND status IN ('active', @session.user_id));
        "#;

        let schema = parse_schema(sql).unwrap();
        let using = schema
            .get(&TableName::new("todos"))
            .unwrap()
            .policies
            .select
            .using
            .as_ref()
            .expect("missing select policy");

        let PolicyExpr::And(exprs) = using else {
            panic!("expected AND policy, got {using:?}");
        };
        assert_eq!(exprs.len(), 2);
        assert!(matches!(
            &exprs[0],
            PolicyExpr::Contains {
                column,
                value: PolicyValue::Literal(Value::Text(value)),
            } if column == "owner_id" && value == "ali"
        ));
        assert!(matches!(
            &exprs[1],
            PolicyExpr::InList { column, values }
                if column == "status"
                    && values
                        == &vec![
                            PolicyValue::Literal(Value::Text("active".into())),
                            PolicyValue::SessionRef(vec!["user_id".into()])
                        ]
        ));
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
                assert_eq!(schema.columns.columns.len(), 2);
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
        assert_eq!(todos.columns.columns.len(), todos2.columns.columns.len());
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
                f UUID NOT NULL,
                g BYTEA NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("test")).unwrap();

        assert_eq!(table.columns.columns[0].column_type, ColumnType::Text);
        assert_eq!(table.columns.columns[1].column_type, ColumnType::Integer);
        assert_eq!(table.columns.columns[2].column_type, ColumnType::BigInt);
        assert_eq!(table.columns.columns[3].column_type, ColumnType::Boolean);
        assert_eq!(table.columns.columns[4].column_type, ColumnType::Timestamp);
        assert_eq!(table.columns.columns[5].column_type, ColumnType::Uuid);
        assert_eq!(table.columns.columns[6].column_type, ColumnType::Bytea);
    }

    #[test]
    fn parse_bytea_array_column_type() {
        let sql = "CREATE TABLE chunks (parts BYTEA[] NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("chunks"))
            .unwrap()
            .columns
            .columns[0];

        assert_eq!(
            col.column_type,
            ColumnType::Array {
                element: Box::new(ColumnType::Bytea)
            }
        );
    }

    #[test]
    fn parse_json_column_types() {
        let sql = r#"
            CREATE TABLE docs (
                payload JSON NOT NULL,
                typed_payload JSON('{"type":"object","properties":{"name":{"type":"string"}}}') NULL
            );
        "#;
        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("docs")).unwrap();

        assert_eq!(
            table.columns.columns[0].column_type,
            ColumnType::Json { schema: None }
        );
        assert_eq!(
            table.columns.columns[1].column_type,
            ColumnType::Json {
                schema: Some(json!({
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    }
                }))
            }
        );
        assert!(table.columns.columns[1].nullable);
    }

    #[test]
    fn reject_invalid_json_schema_payload() {
        let sql = "CREATE TABLE docs (payload JSON('{not-json}') NOT NULL);";
        let err = parse_schema(sql).expect_err("invalid schema payload should fail");
        assert!(
            err.to_string().contains("Invalid JSON schema payload"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_uuid_array_column_with_references() {
        let sql = "CREATE TABLE files (parts UUID[] REFERENCES file_parts NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("files"))
            .unwrap()
            .columns
            .columns[0];

        assert_eq!(col.name.as_str(), "parts");
        assert_eq!(
            col.column_type,
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid)
            }
        );
        assert_eq!(col.references, Some(TableName::new("file_parts")));
        assert!(!col.nullable);
    }

    #[test]
    fn reject_non_uuid_array_references() {
        let sql = "CREATE TABLE files (parts TEXT[] REFERENCES file_parts NOT NULL);";
        let error = parse_schema(sql).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only UUID and UUID[] support REFERENCES"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn reject_non_uuid_scalar_references() {
        let sql = "CREATE TABLE files (part TEXT REFERENCES file_parts NOT NULL);";
        let error = parse_schema(sql).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only UUID and UUID[] support REFERENCES"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_nested_array_column_type() {
        let sql = "CREATE TABLE t (matrix INTEGER[][] NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema.get(&TableName::new("t")).unwrap().columns.columns[0];
        assert_eq!(
            col.column_type,
            ColumnType::Array {
                element: Box::new(ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                }),
            }
        );
        assert!(!col.nullable);
    }

    #[test]
    fn parse_enum_column_type() {
        let sql = "CREATE TABLE todos (status ENUM('todo','in_progress','done') NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .columns[0];

        assert_eq!(
            col.column_type,
            ColumnType::Enum {
                variants: vec![
                    "todo".to_string(),
                    "in_progress".to_string(),
                    "done".to_string(),
                ]
            }
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
    fn parse_lens_add_enum_column_with_default() {
        let sql = "ALTER TABLE todos ADD COLUMN status ENUM('todo','done') DEFAULT 'todo';";
        let transform = parse_lens(sql).unwrap();

        match &transform.ops[0] {
            LensOp::AddColumn {
                column,
                column_type,
                default,
                ..
            } => {
                assert_eq!(column, "status");
                assert_eq!(
                    *column_type,
                    ColumnType::Enum {
                        variants: vec!["todo".to_string(), "done".to_string()]
                    }
                );
                assert_eq!(*default, Value::Text("todo".to_string()));
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
            .columns
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
            .columns
            .columns[0];

        assert_eq!(col.references, Some(TableName::new("todos")));
        assert!(col.nullable);
    }

    #[test]
    fn parse_references_before_not_null() {
        // Order: REFERENCES then NOT NULL
        let sql = "CREATE TABLE t (fk UUID REFERENCES other NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema.get(&TableName::new("t")).unwrap().columns.columns[0];

        assert_eq!(col.references, Some(TableName::new("other")));
        assert!(!col.nullable);
    }

    #[test]
    fn parse_not_null_before_references() {
        // Order: NOT NULL then REFERENCES (should also work)
        let sql = "CREATE TABLE t (fk UUID NOT NULL REFERENCES other);";
        let schema = parse_schema(sql).unwrap();
        let col = &schema.get(&TableName::new("t")).unwrap().columns.columns[0];

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
    fn schema_to_sql_includes_policies() {
        let mut schema = HashMap::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::with_policies(
                RowDescriptor::new(vec![ColumnDescriptor::new(
                    ColumnName::new("owner_id"),
                    ColumnType::Text,
                )]),
                TablePolicies::new()
                    .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                    .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                    .with_update(
                        Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                    )
                    .with_delete(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
            ),
        );

        let sql = schema_to_sql(&schema);

        assert!(sql.contains("CREATE POLICY todos_select_policy ON todos FOR SELECT"));
        assert!(sql.contains("CREATE POLICY todos_insert_policy ON todos FOR INSERT"));
        assert!(sql.contains("CREATE POLICY todos_update_policy ON todos FOR UPDATE"));
        assert!(sql.contains("CREATE POLICY todos_delete_policy ON todos FOR DELETE"));
    }

    #[test]
    fn schema_to_sql_includes_contains_and_in_list_policies() {
        let mut schema = HashMap::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::with_policies(
                RowDescriptor::new(vec![
                    ColumnDescriptor::new(ColumnName::new("owner_id"), ColumnType::Text),
                    ColumnDescriptor::new(ColumnName::new("status"), ColumnType::Text),
                ]),
                TablePolicies::new().with_select(PolicyExpr::And(vec![
                    PolicyExpr::Contains {
                        column: "owner_id".into(),
                        value: PolicyValue::Literal(Value::Text("ali".into())),
                    },
                    PolicyExpr::InList {
                        column: "status".into(),
                        values: vec![
                            PolicyValue::Literal(Value::Text("active".into())),
                            PolicyValue::Literal(Value::Text("trial".into())),
                        ],
                    },
                ])),
            ),
        );

        let sql = schema_to_sql(&schema);
        assert!(sql.contains("owner_id CONTAINS 'ali'"));
        assert!(sql.contains("status IN ('active', 'trial')"));
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
            .columns
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
            .columns
            .columns[0];
        assert_eq!(
            col.column_type,
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid)
            }
        );
        assert_eq!(col.references, Some(TableName::new("file_parts")));
        assert!(!col.nullable);
    }

    #[test]
    fn sql_round_trip_with_enum() {
        let sql = "CREATE TABLE todos (status ENUM('todo','done') NOT NULL);";
        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);

        assert!(regenerated.contains("status ENUM('todo','done') NOT NULL"));

        let reparsed = parse_schema(&regenerated).unwrap();
        let col = &reparsed
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .columns[0];
        assert_eq!(
            col.column_type,
            ColumnType::Enum {
                variants: vec!["todo".to_string(), "done".to_string()]
            }
        );
        assert!(!col.nullable);
    }

    #[test]
    fn sql_round_trip_with_policies() {
        let sql = r#"
            CREATE TABLE todos (
                owner_id TEXT NOT NULL
            );
            CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);
        "#;
        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);
        let reparsed = parse_schema(&regenerated).unwrap();

        let table = reparsed.get(&TableName::new("todos")).unwrap();
        assert!(table.policies.select.using.is_some());
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

        assert_eq!(table.columns.columns.len(), 3);

        let title = &table.columns.columns[0];
        assert_eq!(title.name.as_str(), "title");
        assert_eq!(title.column_type, ColumnType::Text);
        assert!(title.references.is_none());
        assert!(!title.nullable);

        let parent = &table.columns.columns[1];
        assert_eq!(parent.name.as_str(), "parent_id");
        assert_eq!(parent.references, Some(TableName::new("todos")));
        assert!(parent.nullable);

        let owner = &table.columns.columns[2];
        assert_eq!(owner.name.as_str(), "owner_id");
        assert_eq!(owner.references, Some(TableName::new("users")));
        assert!(!owner.nullable);
    }

    #[test]
    fn parse_real_column_type() {
        let sql = r#"
            CREATE TABLE measurements (
                temperature REAL NOT NULL,
                humidity REAL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("measurements")).unwrap();

        assert_eq!(table.columns.columns.len(), 2);

        let temp = &table.columns.columns[0];
        assert_eq!(temp.name.as_str(), "temperature");
        assert_eq!(temp.column_type, ColumnType::Double);
        assert!(!temp.nullable);

        let humidity = &table.columns.columns[1];
        assert_eq!(humidity.name.as_str(), "humidity");
        assert_eq!(humidity.column_type, ColumnType::Double);
        assert!(humidity.nullable);
    }

    #[test]
    fn parse_float_and_double_as_real_aliases() {
        let sql = r#"
            CREATE TABLE sensors (
                pressure FLOAT NOT NULL,
                altitude DOUBLE NOT NULL
            );
        "#;

        let schema = parse_schema(sql).unwrap();
        let table = schema.get(&TableName::new("sensors")).unwrap();

        assert_eq!(table.columns.columns[0].column_type, ColumnType::Double);
        assert_eq!(table.columns.columns[1].column_type, ColumnType::Double);
    }

    #[test]
    fn sql_round_trip_with_real() {
        let sql = r#"CREATE TABLE measurements (
    temperature REAL NOT NULL,
    humidity REAL
);"#;

        let schema = parse_schema(sql).unwrap();
        let regenerated = schema_to_sql(&schema);
        let reparsed = parse_schema(&regenerated).unwrap();

        let orig = schema.get(&TableName::new("measurements")).unwrap();
        let round = reparsed.get(&TableName::new("measurements")).unwrap();

        assert_eq!(orig.columns.columns.len(), round.columns.columns.len());
        assert_eq!(
            orig.columns.columns[0].column_type,
            round.columns.columns[0].column_type
        );
        assert_eq!(
            orig.columns.columns[1].column_type,
            round.columns.columns[1].column_type
        );
        assert_eq!(
            orig.columns.columns[1].nullable,
            round.columns.columns[1].nullable
        );
    }

    #[test]
    fn parse_lens_add_real_column_with_default() {
        let sql = "ALTER TABLE sensors ADD COLUMN calibration REAL DEFAULT 0.0;";

        let transform = parse_lens(sql).unwrap();
        assert_eq!(transform.ops.len(), 1);

        match &transform.ops[0] {
            LensOp::AddColumn {
                table,
                column,
                column_type,
                default,
            } => {
                assert_eq!(table, "sensors");
                assert_eq!(column, "calibration");
                assert_eq!(*column_type, ColumnType::Double);
                assert_eq!(*default, Value::Double(0.0));
            }
            _ => panic!("Expected AddColumn"),
        }
    }

    #[test]
    fn parse_non_finite_float_default_rejected() {
        // The tokeniser treats inf/NaN as identifiers, not numbers, so these
        // are rejected at parse time before reaching the value constructor.
        for literal in &["inf", "-inf", "NaN"] {
            let sql = format!("ALTER TABLE t ADD COLUMN x REAL DEFAULT {literal};");
            assert!(parse_lens(&sql).is_err(), "should reject {literal}");
        }
    }

    #[test]
    #[should_panic(expected = "non-finite float")]
    fn value_to_sql_rejects_infinity() {
        value_to_sql(&Value::Double(f64::INFINITY));
    }

    #[test]
    #[should_panic(expected = "non-finite float")]
    fn value_to_sql_rejects_nan() {
        value_to_sql(&Value::Double(f64::NAN));
    }

    #[test]
    fn parse_real_array_column() {
        let sql = "CREATE TABLE timeseries (samples REAL[] NOT NULL);";

        let schema = parse_schema(sql).unwrap();
        let col = &schema
            .get(&TableName::new("timeseries"))
            .unwrap()
            .columns
            .columns[0];

        assert_eq!(
            col.column_type,
            ColumnType::Array {
                element: Box::new(ColumnType::Double)
            }
        );
        assert!(!col.nullable);
    }
}
