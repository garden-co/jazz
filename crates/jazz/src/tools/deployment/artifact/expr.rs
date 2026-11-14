//! The expression grammar of `jazz.permissions.json` (SPEC §10.10): a
//! hand-written lexer and parser with byte spans, the compile to the
//! engine's [`PolicyExpr`], and the printer back to canonical text.

use std::collections::BTreeMap;

use crate::tools::ObjectId;
use crate::tools::public_schema::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, Schema, TableName, Value,
};

use super::CodegenRelation;

/// Where the grammar is documented: every expression error links it.
pub const GRAMMAR_URL: &str = "https://jazz.tools/docs/reference/app-files#expressions";

/// The session path TS uses for a column of the row being checked, inside
/// `exists`: `this.col` there.
const OUTER_ROW: &str = "__jazz_outer_row";

/// Columns every table has, which policies may name.
const MAGIC_COLUMNS: &[&str] = &[
    "id",
    "$createdBy",
    "$createdBy.account",
    "$createdBy.identity",
    "$updatedBy",
    "$updatedBy.account",
    "$updatedBy.identity",
    "$deletedBy",
    "$deletedBy.account",
    "$deletedBy.identity",
    "$createdAt",
    "$updatedAt",
    "$deletedAt",
];

/// Why an expression was refused, at `span` (byte offsets into it).
#[derive(Debug, Clone, PartialEq)]
pub struct ExprError {
    pub span: (usize, usize),
    pub message: String,
    /// A correct example of the construct, when one helps.
    pub example: Option<&'static str>,
}

impl ExprError {
    fn new(span: (usize, usize), message: impl Into<String>) -> Self {
        Self {
            span,
            message: message.into(),
            example: None,
        }
    }

    fn example(mut self, example: &'static str) -> Self {
        self.example = Some(example);
        self
    }

    /// The refusal as the files report it: the expression with a caret under
    /// the span, the message, the example and the grammar's link.
    pub fn render(&self, source: &str) -> String {
        let (start, end) = (self.span.0.min(source.len()), self.span.1.min(source.len()));
        let caret_at = source[..start].chars().count();
        let width = source[start..end].chars().count().max(1);
        let mut out = format!(
            "\n  {source}\n  {}{}\n  {}",
            " ".repeat(caret_at),
            "^".repeat(width),
            self.message
        );
        if let Some(example) = self.example {
            out.push_str(&format!("\n  e.g. {example}"));
        }
        out.push_str(&format!("\n  Grammar: {GRAMMAR_URL}"));
        out
    }
}

// ---------------------------------------------------------------------------
// Lexing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    /// `$name`, `$createdBy`, …
    Dollar(String),
    Str(String),
    Num(String),
    Op(&'static str),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    Colon,
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    span: (usize, usize),
}

fn lex(source: &str) -> Result<Vec<Token>, ExprError> {
    let bytes = source.as_bytes();
    let mut at = 0;
    let mut out = Vec::new();
    while at < bytes.len() {
        let start = at;
        let c = bytes[at];
        let single = |tok: Tok, at: &mut usize| {
            *at += 1;
            Token {
                tok,
                span: (start, *at),
            }
        };
        let token = match c {
            b' ' | b'\t' | b'\n' | b'\r' => {
                at += 1;
                continue;
            }
            b'(' => single(Tok::LParen, &mut at),
            b')' => single(Tok::RParen, &mut at),
            b'[' => single(Tok::LBracket, &mut at),
            b']' => single(Tok::RBracket, &mut at),
            b',' => single(Tok::Comma, &mut at),
            b'.' => single(Tok::Dot, &mut at),
            b':' => single(Tok::Colon, &mut at),
            b'=' => {
                if bytes.get(at + 1) == Some(&b'=') {
                    return Err(
                        ExprError::new((at, at + 2), "comparisons use `=`, not `==`")
                            .example("userId = session.user"),
                    );
                }
                single(Tok::Op("="), &mut at)
            }
            b'!' => {
                if bytes.get(at + 1) == Some(&b'=') {
                    at += 2;
                    Token {
                        tok: Tok::Op("!="),
                        span: (start, at),
                    }
                } else {
                    return Err(ExprError::new((at, at + 1), "negation is `not`, not `!`")
                        .example("not isArchived = true"));
                }
            }
            b'<' | b'>' => {
                let eq = bytes.get(at + 1) == Some(&b'=');
                at += if eq { 2 } else { 1 };
                let op = match (c, eq) {
                    (b'<', false) => "<",
                    (b'<', true) => "<=",
                    (b'>', false) => ">",
                    _ => ">=",
                };
                if c == b'<' && bytes.get(at) == Some(&b'>') {
                    return Err(ExprError::new(
                        (start, at + 1),
                        "inequality is `!=`, not `<>`",
                    ));
                }
                Token {
                    tok: Tok::Op(op),
                    span: (start, at),
                }
            }
            b'\'' => {
                at += 1;
                let mut text = String::new();
                loop {
                    match bytes.get(at) {
                        None => {
                            return Err(ExprError::new((start, at), "unterminated string")
                                .example("role = 'admin'"));
                        }
                        Some(b'\'') if bytes.get(at + 1) == Some(&b'\'') => {
                            text.push('\'');
                            at += 2;
                        }
                        Some(b'\'') => {
                            at += 1;
                            break;
                        }
                        Some(_) => {
                            let ch = source[at..].chars().next().expect("in bounds");
                            text.push(ch);
                            at += ch.len_utf8();
                        }
                    }
                }
                Token {
                    tok: Tok::Str(text),
                    span: (start, at),
                }
            }
            b'"' => {
                return Err(ExprError::new(
                    (at, at + 1),
                    "strings use single quotes inside an expression",
                )
                .example("role = 'admin'"));
            }
            b'-' | b'0'..=b'9' => {
                at += 1;
                while at < bytes.len() && (bytes[at].is_ascii_digit() || bytes[at] == b'.') {
                    // `1.5` is a number; `x.y` after a number is not a thing.
                    if bytes[at] == b'.' && !bytes.get(at + 1).is_some_and(u8::is_ascii_digit) {
                        break;
                    }
                    at += 1;
                }
                if source[start..at] == *"-" {
                    return Err(ExprError::new((start, at), "a number needs digits"));
                }
                Token {
                    tok: Tok::Num(source[start..at].to_string()),
                    span: (start, at),
                }
            }
            b'$' => {
                at += 1;
                while at < bytes.len() && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'_') {
                    at += 1;
                }
                if at == start + 1 {
                    return Err(ExprError::new((start, at), "`$` must start a name")
                        .example("isChatMember($chat)"));
                }
                Token {
                    tok: Tok::Dollar(source[start + 1..at].to_string()),
                    span: (start, at),
                }
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                at += 1;
                while at < bytes.len() && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'_') {
                    at += 1;
                }
                Token {
                    tok: Tok::Ident(source[start..at].to_string()),
                    span: (start, at),
                }
            }
            _ => {
                let ch = source[at..].chars().next().expect("in bounds");
                return Err(ExprError::new(
                    (at, at + ch.len_utf8()),
                    format!("unexpected `{ch}`"),
                ));
            }
        };
        out.push(token);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

type Span = (usize, usize);

/// A parsed expression, before names are resolved.
#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    Or(Vec<Ast>),
    And(Vec<Ast>),
    Not(Box<Ast>),
    Bool(bool),
    Cmp {
        left: Operand,
        op: CmpOp,
        right: Operand,
        span: Span,
    },
    InList {
        left: Operand,
        values: Vec<Operand>,
        span: Span,
    },
    InSession {
        left: Operand,
        path: Vec<String>,
        span: Span,
    },
    Contains {
        left: Operand,
        right: Operand,
        span: Span,
    },
    Exists {
        table: String,
        table_span: Span,
        conditions: Vec<Ast>,
        hops: Vec<(String, Span)>,
    },
    Allowed {
        op: Operation,
        relation: String,
        relation_span: Span,
        max_depth: Option<usize>,
    },
    Call {
        name: String,
        span: Span,
        args: Vec<Operand>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// A column, `this.` or bare.
    Column {
        this: bool,
        name: String,
        span: Span,
    },
    Literal(Literal, Span),
    Null(Span),
    Session(Vec<String>, Span),
    Param(String, Span),
}

impl Operand {
    fn span(&self) -> Span {
        match self {
            Operand::Column { span, .. }
            | Operand::Literal(_, span)
            | Operand::Null(span)
            | Operand::Session(_, span)
            | Operand::Param(_, span) => *span,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Str(String),
    Num(String),
    Bool(bool),
}

struct Parser<'a> {
    tokens: &'a [Token],
    at: usize,
    end: usize,
    depth: usize,
}

/// Nesting (`not`, parentheses, `exists`) deeper than this is refused.
const MAX_DEPTH: usize = 64;

/// Parse an expression string.
pub fn parse(source: &str) -> Result<Ast, ExprError> {
    let tokens = lex(source)?;
    let mut parser = Parser {
        tokens: &tokens,
        at: 0,
        end: source.len(),
        depth: 0,
    };
    if tokens.is_empty() {
        return Err(ExprError::new((0, source.len()), "an empty expression")
            .example("userId = session.user"));
    }
    let ast = parser.expr()?;
    if let Some(token) = parser.peek() {
        return Err(ExprError::new(
            token.span,
            "unexpected text after the expression; join conditions with `and` or `or`",
        )
        .example("isPublic = true or userId = session.user"));
    }
    Ok(ast)
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn here(&self) -> Span {
        self.peek().map_or((self.end, self.end), |t| t.span)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }

    fn is_word(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Token { tok: Tok::Ident(w), .. }) if w == word)
    }

    fn expect(&mut self, tok: Tok, what: &str) -> Result<Span, ExprError> {
        match self.peek() {
            Some(token) if token.tok == tok => {
                let span = token.span;
                self.at += 1;
                Ok(span)
            }
            _ => Err(ExprError::new(self.here(), format!("expected {what}"))),
        }
    }

    fn expr(&mut self) -> Result<Ast, ExprError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(ExprError::new(
                self.here(),
                "the expression nests too deeply",
            ));
        }
        let result = self.disjunction();
        self.depth -= 1;
        result
    }

    fn disjunction(&mut self) -> Result<Ast, ExprError> {
        let mut terms = vec![self.conjunction()?];
        while self.is_word("or") {
            self.at += 1;
            terms.push(self.conjunction()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().expect("one")
        } else {
            Ast::Or(terms)
        })
    }

    fn conjunction(&mut self) -> Result<Ast, ExprError> {
        let mut terms = vec![self.negation()?];
        while self.is_word("and") {
            self.at += 1;
            terms.push(self.negation()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().expect("one")
        } else {
            Ast::And(terms)
        })
    }

    fn negation(&mut self) -> Result<Ast, ExprError> {
        if self.is_word("not") {
            self.at += 1;
            self.depth += 1;
            if self.depth > MAX_DEPTH {
                return Err(ExprError::new(
                    self.here(),
                    "the expression nests too deeply",
                ));
            }
            let inner = self.negation();
            self.depth -= 1;
            return Ok(Ast::Not(Box::new(inner?)));
        }
        self.atom()
    }

    fn atom(&mut self) -> Result<Ast, ExprError> {
        let Some(token) = self.peek().cloned() else {
            return Err(ExprError::new(self.here(), "the expression ends too early"));
        };
        match &token.tok {
            Tok::LParen => {
                self.at += 1;
                let inner = self.expr()?;
                self.expect(Tok::RParen, "`)`")?;
                Ok(inner)
            }
            Tok::Ident(word) if word == "true" || word == "false" => {
                // `true` alone is a policy; `true = x` is a comparison.
                if matches!(
                    self.tokens.get(self.at + 1),
                    Some(Token {
                        tok: Tok::Op(_),
                        ..
                    })
                ) {
                    return self.comparison();
                }
                self.at += 1;
                Ok(Ast::Bool(word == "true"))
            }
            Tok::Ident(word) if word == "exists" => self.exists(),
            Tok::Ident(word)
                if word == "allowedTo"
                    && matches!(
                        self.tokens.get(self.at + 1),
                        Some(Token { tok: Tok::Dot, .. })
                    ) =>
            {
                self.allowed()
            }
            Tok::Ident(word)
                if !matches!(word.as_str(), "this" | "session" | "null")
                    && matches!(
                        self.tokens.get(self.at + 1),
                        Some(Token {
                            tok: Tok::LParen,
                            ..
                        })
                    ) =>
            {
                self.call()
            }
            _ => self.comparison(),
        }
    }

    fn exists(&mut self) -> Result<Ast, ExprError> {
        self.at += 1;
        let Some(Token {
            tok: Tok::Ident(table),
            span: table_span,
        }) = self.next()
        else {
            return Err(
                ExprError::new(self.here(), "`exists` takes a table and its conditions")
                    .example("exists chatMembers(chatId = this.id, userId = session.user)"),
            );
        };
        let conditions =
            self.conditions("exists chatMembers(chatId = this.id, userId = session.user)")?;
        let mut hops = Vec::new();
        while matches!(self.peek(), Some(Token { tok: Tok::Dot, .. })) {
            self.at += 1;
            let Some(Token {
                tok: Tok::Ident(relation),
                span,
            }) = self.next()
            else {
                return Err(ExprError::new(
                    self.here(),
                    "expected a relation name after `.`",
                ));
            };
            hops.push((relation, span));
            self.conditions("exists files(id = this.id).folder(ownerId = session.user)")?;
        }
        Ok(Ast::Exists {
            table,
            table_span,
            conditions,
            hops,
        })
    }

    fn conditions(&mut self, example: &'static str) -> Result<Vec<Ast>, ExprError> {
        if !matches!(
            self.peek(),
            Some(Token {
                tok: Tok::LParen,
                ..
            })
        ) {
            return Err(
                ExprError::new(self.here(), "expected `(` and the conditions").example(example),
            );
        }
        self.at += 1;
        let mut conditions = Vec::new();
        if matches!(
            self.peek(),
            Some(Token {
                tok: Tok::RParen,
                ..
            })
        ) {
            self.at += 1;
            return Ok(conditions);
        }
        loop {
            conditions.push(self.expr()?);
            match self.next() {
                Some(Token {
                    tok: Tok::Comma, ..
                }) => continue,
                Some(Token {
                    tok: Tok::RParen, ..
                }) => return Ok(conditions),
                _ => {
                    return Err(ExprError::new(self.here(), "expected `,` or `)`").example(example));
                }
            }
        }
    }

    fn allowed(&mut self) -> Result<Ast, ExprError> {
        const EXAMPLE: &str = "allowedTo.read(chat)";
        self.at += 2;
        let Some(Token {
            tok: Tok::Ident(op_name),
            span: op_span,
        }) = self.next()
        else {
            return Err(ExprError::new(
                self.here(),
                "expected `read`, `insert`, `update` or `delete`",
            )
            .example(EXAMPLE));
        };
        let op = match op_name.as_str() {
            "read" => Operation::Select,
            "insert" => Operation::Insert,
            "update" => Operation::Update,
            "delete" => Operation::Delete,
            other => {
                return Err(ExprError::new(
                    op_span,
                    format!(
                        "unknown operation `{other}`{}; allowedTo takes read, insert, update or delete",
                        did_you_mean(other, ["read", "insert", "update", "delete"])
                    ),
                )
                .example(EXAMPLE));
            }
        };
        self.expect(Tok::LParen, "`(` and a relation")?;
        let Some(Token {
            tok: Tok::Ident(relation),
            span: relation_span,
        }) = self.next()
        else {
            return Err(ExprError::new(self.here(), "expected a relation name").example(EXAMPLE));
        };
        let mut max_depth = None;
        if matches!(
            self.peek(),
            Some(Token {
                tok: Tok::Comma,
                ..
            })
        ) {
            self.at += 1;
            match (self.next(), self.next(), self.next()) {
                (
                    Some(Token {
                        tok: Tok::Ident(key),
                        ..
                    }),
                    Some(Token {
                        tok: Tok::Colon, ..
                    }),
                    Some(Token {
                        tok: Tok::Num(n),
                        span,
                    }),
                ) if key == "maxDepth" => {
                    max_depth =
                        Some(n.parse::<usize>().map_err(|_| {
                            ExprError::new(span, "`maxDepth` takes a whole number")
                        })?);
                }
                _ => {
                    return Err(ExprError::new(self.here(), "expected `maxDepth: <number>`")
                        .example("allowedTo.read(parent, maxDepth: 8)"));
                }
            }
        }
        self.expect(Tok::RParen, "`)`")?;
        Ok(Ast::Allowed {
            op,
            relation,
            relation_span,
            max_depth,
        })
    }

    fn call(&mut self) -> Result<Ast, ExprError> {
        let Some(Token {
            tok: Tok::Ident(name),
            span,
        }) = self.next()
        else {
            unreachable!("checked by atom");
        };
        self.at += 1;
        let mut args = Vec::new();
        if matches!(
            self.peek(),
            Some(Token {
                tok: Tok::RParen,
                ..
            })
        ) {
            self.at += 1;
        } else {
            loop {
                args.push(self.operand()?);
                match self.next() {
                    Some(Token {
                        tok: Tok::Comma, ..
                    }) => continue,
                    Some(Token {
                        tok: Tok::RParen, ..
                    }) => break,
                    _ => {
                        return Err(ExprError::new(self.here(), "expected `,` or `)`")
                            .example("isChatMember(this.id)"));
                    }
                }
            }
        }
        Ok(Ast::Call { name, span, args })
    }

    fn comparison(&mut self) -> Result<Ast, ExprError> {
        let left = self.operand()?;
        let start = left.span().0;
        if self.is_word("in") {
            self.at += 1;
            if matches!(
                self.peek(),
                Some(Token {
                    tok: Tok::LBracket,
                    ..
                })
            ) {
                self.at += 1;
                let mut values = Vec::new();
                if !matches!(
                    self.peek(),
                    Some(Token {
                        tok: Tok::RBracket,
                        ..
                    })
                ) {
                    loop {
                        values.push(self.operand()?);
                        match self.peek() {
                            Some(Token {
                                tok: Tok::Comma, ..
                            }) => self.at += 1,
                            _ => break,
                        }
                    }
                }
                let end = self.expect(Tok::RBracket, "`]`")?.1;
                return Ok(Ast::InList {
                    left,
                    values,
                    span: (start, end),
                });
            }
            let right = self.operand()?;
            let Operand::Session(path, span) = right else {
                return Err(
                    ExprError::new(right.span(), "`in` takes a list or a session claim")
                        .example("role in ['admin', 'editor']"),
                );
            };
            return Ok(Ast::InSession {
                left,
                path,
                span: (start, span.1),
            });
        }
        if self.is_word("contains") {
            self.at += 1;
            let right = self.operand()?;
            let end = right.span().1;
            return Ok(Ast::Contains {
                left,
                right,
                span: (start, end),
            });
        }
        let op = match self.peek() {
            Some(Token {
                tok: Tok::Op(op), ..
            }) => match *op {
                "=" => CmpOp::Eq,
                "!=" => CmpOp::Ne,
                "<" => CmpOp::Lt,
                "<=" => CmpOp::Le,
                ">" => CmpOp::Gt,
                _ => CmpOp::Ge,
            },
            _ => {
                return Err(ExprError::new(
                    self.here(),
                    "expected a comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`, `in` or `contains`)",
                )
                .example("userId = session.user"));
            }
        };
        self.at += 1;
        let right = self.operand()?;
        let end = right.span().1;
        Ok(Ast::Cmp {
            left,
            op,
            right,
            span: (start, end),
        })
    }

    fn operand(&mut self) -> Result<Operand, ExprError> {
        let Some(token) = self.next() else {
            return Err(ExprError::new(self.here(), "expected a column or a value"));
        };
        let span = token.span;
        match token.tok {
            Tok::Str(text) => Ok(Operand::Literal(Literal::Str(text), span)),
            Tok::Num(n) => Ok(Operand::Literal(Literal::Num(n), span)),
            Tok::Dollar(name) => {
                let column = format!("${name}");
                if MAGIC_COLUMNS.iter().any(|m| m.starts_with(&column)) {
                    let (name, end) = self.dotted(column, span)?;
                    return Ok(Operand::Column {
                        this: false,
                        name,
                        span: (span.0, end),
                    });
                }
                Ok(Operand::Param(name, span))
            }
            Tok::Ident(word) => match word.as_str() {
                "null" => Ok(Operand::Null(span)),
                "true" => Ok(Operand::Literal(Literal::Bool(true), span)),
                "false" => Ok(Operand::Literal(Literal::Bool(false), span)),
                "this" => {
                    self.expect(Tok::Dot, "`.` and a column after `this`")?;
                    let Some(Token {
                        tok,
                        span: name_span,
                    }) = self.next()
                    else {
                        return Err(ExprError::new(
                            self.here(),
                            "expected a column after `this.`",
                        ));
                    };
                    let name = match tok {
                        Tok::Ident(name) => name,
                        Tok::Dollar(name) => format!("${name}"),
                        _ => {
                            return Err(ExprError::new(
                                name_span,
                                "expected a column after `this.`",
                            ));
                        }
                    };
                    let (name, end) = self.dotted(name, name_span)?;
                    Ok(Operand::Column {
                        this: true,
                        name,
                        span: (span.0, end),
                    })
                }
                "session" => {
                    self.expect(Tok::Dot, "`.user` or `.claims.<name>` after `session`")?;
                    let mut path = Vec::new();
                    let mut end;
                    loop {
                        match self.next() {
                            Some(Token {
                                tok: Tok::Ident(part),
                                span,
                            }) => {
                                end = span.1;
                                path.push(part);
                            }
                            _ => {
                                return Err(ExprError::new(self.here(), "expected a name")
                                    .example("session.claims.role = 'admin'"));
                            }
                        }
                        if matches!(self.peek(), Some(Token { tok: Tok::Dot, .. })) {
                            self.at += 1;
                        } else {
                            break;
                        }
                    }
                    let valid = path == ["user"] || (path.len() >= 2 && path[0] == "claims");
                    if !valid {
                        return Err(ExprError::new(
                            (span.0, end),
                            "a session value is `session.user` or `session.claims.<name>`",
                        )
                        .example("session.claims.role = 'admin'"));
                    }
                    Ok(Operand::Session(path, (span.0, end)))
                }
                _ => Ok(Operand::Column {
                    this: false,
                    name: word,
                    span,
                }),
            },
            _ => Err(ExprError::new(span, "expected a column or a value")
                .example("userId = session.user")),
        }
    }

    /// `$createdBy` then `.account`: magic names have one dotted part.
    fn dotted(&mut self, name: String, span: Span) -> Result<(String, usize), ExprError> {
        if name.starts_with('$')
            && matches!(self.peek(), Some(Token { tok: Tok::Dot, .. }))
            && matches!(
                self.tokens.get(self.at + 1),
                Some(Token {
                    tok: Tok::Ident(_),
                    ..
                })
            )
        {
            self.at += 1;
            let Some(Token {
                tok: Tok::Ident(part),
                span: part_span,
            }) = self.next()
            else {
                unreachable!("peeked");
            };
            return Ok((format!("{name}.{part}"), part_span.1));
        }
        Ok((name, span.1))
    }
}

// ---------------------------------------------------------------------------
// Compiling
// ---------------------------------------------------------------------------

/// Everything names resolve against.
pub struct Context<'a> {
    pub schema: &'a Schema,
    pub relations: &'a BTreeMap<String, BTreeMap<String, CodegenRelation>>,
    pub defs: &'a Defs,
}

/// Parsed `defs`: name → (parameters, body).
pub type Defs = BTreeMap<String, Def>;

#[derive(Debug, Clone)]
pub struct Def {
    pub params: Vec<String>,
    /// The body's policy, already parsed; its source for error reports.
    pub body: DefBody,
}

#[derive(Debug, Clone)]
pub enum DefBody {
    Expr(Ast, String),
    /// A structured policy (SPEC §10.10): the engine's form, as written.
    Fixed(PolicyExpr),
    AnyOf(Vec<DefBody>),
    AllOf(Vec<DefBody>),
}

/// Parse a def signature `name($a, $b)`.
pub fn parse_signature(signature: &str) -> Result<(String, Vec<String>), ExprError> {
    const EXAMPLE: &str = "isChatMember($chat)";
    let tokens = lex(signature)?;
    let mut iter = tokens.iter();
    let Some(Token {
        tok: Tok::Ident(name),
        ..
    }) = iter.next()
    else {
        return Err(
            ExprError::new((0, signature.len()), "a definition is `name($param, …)`")
                .example(EXAMPLE),
        );
    };
    let mut params = Vec::new();
    match iter.next() {
        None => return Ok((name.clone(), params)),
        Some(Token {
            tok: Tok::LParen, ..
        }) => {}
        Some(t) => return Err(ExprError::new(t.span, "expected `(`").example(EXAMPLE)),
    }
    loop {
        match iter.next() {
            Some(Token {
                tok: Tok::RParen, ..
            }) if params.is_empty() => break,
            Some(Token {
                tok: Tok::Dollar(p),
                span,
            }) => {
                if params.contains(p) {
                    return Err(ExprError::new(*span, format!("parameter `${p}` repeats")));
                }
                params.push(p.clone());
            }
            Some(t) => {
                return Err(
                    ExprError::new(t.span, "parameters are written `$name`").example(EXAMPLE)
                );
            }
            None => {
                return Err(ExprError::new(
                    (signature.len(), signature.len()),
                    "expected `)`",
                ));
            }
        }
        match iter.next() {
            Some(Token {
                tok: Tok::Comma, ..
            }) => continue,
            Some(Token {
                tok: Tok::RParen, ..
            }) => break,
            _ => {
                return Err(
                    ExprError::new((0, signature.len()), "expected `,` or `)`").example(EXAMPLE)
                );
            }
        }
    }
    if let Some(t) = iter.next() {
        return Err(ExprError::new(
            t.span,
            "unexpected text after the parameters",
        ));
    }
    Ok((name.clone(), params))
}

/// Where a compile is: which table bare columns name, and what `this.` means.
#[derive(Clone)]
struct Scope<'a> {
    /// The table the policy is about.
    table: &'a str,
    /// `exists t(…)`: bare columns name `t`'s, `this.` the checked row's.
    inner: Option<&'a str>,
    /// Arguments of the definition being expanded.
    params: BTreeMap<String, Operand>,
    /// Definitions being expanded, to refuse recursion.
    calling: Vec<String>,
}

/// Compile `ast` (its text is `source`) as a policy on `table`.
pub fn compile(ast: &Ast, context: &Context<'_>, table: &str) -> Result<PolicyExpr, ExprError> {
    let scope = Scope {
        table,
        inner: None,
        params: BTreeMap::new(),
        calling: Vec::new(),
    };
    compile_in(ast, context, &scope)
}

/// Compile a def body (or any [`DefBody`]) in `scope`.
fn compile_body(
    body: &DefBody,
    context: &Context<'_>,
    scope: &Scope<'_>,
) -> Result<PolicyExpr, ExprError> {
    match body {
        DefBody::Expr(ast, _) => compile_in(ast, context, scope),
        DefBody::Fixed(expr) => Ok(expr.clone()),
        DefBody::AnyOf(items) => Ok(PolicyExpr::Or(
            items
                .iter()
                .map(|item| compile_body(item, context, scope))
                .collect::<Result<_, _>>()?,
        )),
        DefBody::AllOf(items) => Ok(PolicyExpr::And(
            items
                .iter()
                .map(|item| compile_body(item, context, scope))
                .collect::<Result<_, _>>()?,
        )),
    }
}

fn columns_of<'a>(context: &'a Context<'_>, table: &str) -> Option<Vec<(&'a str, &'a ColumnType)>> {
    let schema = context.schema.get(&TableName::from(table))?;
    Some(
        schema
            .columns
            .columns
            .iter()
            .map(|c| (c.name_str(), &c.column_type))
            .collect(),
    )
}

/// The type of `column` of `table`, or the error naming the nearest ones.
fn column_type<'a>(
    context: &'a Context<'_>,
    table: &str,
    column: &str,
    span: Span,
) -> Result<Option<&'a ColumnType>, ExprError> {
    if MAGIC_COLUMNS.contains(&column) {
        return Ok(None);
    }
    let columns = columns_of(context, table).unwrap_or_default();
    if let Some((_, ty)) = columns.iter().find(|(name, _)| *name == column) {
        return Ok(Some(ty));
    }
    let names: Vec<&str> = columns
        .iter()
        .map(|(n, _)| *n)
        .chain(MAGIC_COLUMNS.iter().copied())
        .collect();
    Err(ExprError::new(
        span,
        format!(
            "unknown column `{column}` in {table}{}",
            did_you_mean(column, names)
        ),
    ))
}

fn compile_in(
    ast: &Ast,
    context: &Context<'_>,
    scope: &Scope<'_>,
) -> Result<PolicyExpr, ExprError> {
    match ast {
        Ast::Bool(true) => Ok(PolicyExpr::True),
        Ast::Bool(false) => Ok(PolicyExpr::False),
        Ast::Or(items) => Ok(PolicyExpr::Or(
            items
                .iter()
                .map(|i| compile_in(i, context, scope))
                .collect::<Result<_, _>>()?,
        )),
        Ast::And(items) => Ok(PolicyExpr::And(
            items
                .iter()
                .map(|i| compile_in(i, context, scope))
                .collect::<Result<_, _>>()?,
        )),
        Ast::Not(inner) => Ok(PolicyExpr::Not(Box::new(compile_in(
            inner, context, scope,
        )?))),
        Ast::Cmp {
            left,
            op,
            right,
            span,
        } => compile_cmp(left, op.clone(), right, *span, context, scope),
        Ast::InList { left, values, span } => {
            let left = resolve(left, scope);
            match column_of(&left, context, scope)? {
                Some((column, ty)) => Ok(PolicyExpr::InList {
                    column,
                    values: values
                        .iter()
                        .map(|v| policy_value(&resolve(v, scope), ty.as_ref(), context, scope))
                        .collect::<Result<_, _>>()?,
                }),
                None => match left {
                    Operand::Session(path, _) => Ok(PolicyExpr::SessionInList {
                        path,
                        values: values
                            .iter()
                            .map(|v| literal_value(&resolve(v, scope), None))
                            .collect::<Result<_, _>>()?,
                    }),
                    other => Err(ExprError::new(
                        other.span().min(*span),
                        "`in` tests a column or a session claim",
                    )
                    .example("role in ['admin', 'editor']")),
                },
            }
        }
        Ast::InSession { left, path, .. } => {
            let left = resolve(left, scope);
            match column_of(&left, context, scope)? {
                Some((column, _)) => Ok(PolicyExpr::In {
                    column,
                    session_path: path.clone(),
                }),
                None => Err(
                    ExprError::new(left.span(), "`in session.claims…` tests a column")
                        .example("teamId in session.claims.teams"),
                ),
            }
        }
        Ast::Contains { left, right, .. } => {
            let left = resolve(left, scope);
            let right = resolve(right, scope);
            match column_of(&left, context, scope)? {
                Some((column, ty)) => {
                    let element = match ty {
                        Some(ColumnType::Array { element }) => Some(*element),
                        _ => None,
                    };
                    Ok(PolicyExpr::Contains {
                        column,
                        value: policy_value(&right, element.as_ref(), context, scope)?,
                    })
                }
                None => match left {
                    Operand::Session(path, _) => Ok(PolicyExpr::SessionContains {
                        path,
                        value: literal_value(&right, None)?,
                    }),
                    other => Err(
                        ExprError::new(other.span(), "`contains` tests an array column")
                            .example("tags contains 'urgent'"),
                    ),
                },
            }
        }
        Ast::Exists {
            table,
            table_span,
            conditions,
            hops,
        } => {
            if let Some((_, span)) = hops.first() {
                return Err(ExprError::new(
                    *span,
                    "hopping along a relation inside `exists` is not supported in jazz.permissions.json yet",
                ));
            }
            if scope.inner.is_some() {
                return Err(ExprError::new(
                    *table_span,
                    "`exists` inside `exists` is not supported in jazz.permissions.json yet",
                ));
            }
            if context
                .schema
                .get(&TableName::from(table.as_str()))
                .is_none()
            {
                let names: Vec<String> = context
                    .schema
                    .keys()
                    .map(|t| t.as_str().to_string())
                    .collect();
                return Err(ExprError::new(
                    *table_span,
                    format!(
                        "unknown table `{table}`{}",
                        did_you_mean(table, names.iter().map(String::as_str))
                    ),
                )
                .example("exists chatMembers(chatId = this.id, userId = session.user)"));
            }
            let inner = Scope {
                inner: Some(table.as_str()),
                ..scope.clone()
            };
            let mut compiled = conditions
                .iter()
                .map(|c| compile_in(c, context, &inner))
                .collect::<Result<Vec<_>, _>>()?;
            let condition = match compiled.len() {
                0 => PolicyExpr::True,
                1 => compiled.pop().expect("one"),
                _ => PolicyExpr::And(compiled),
            };
            Ok(PolicyExpr::Exists {
                table: table.clone(),
                condition: Box::new(condition),
            })
        }
        Ast::Allowed {
            op,
            relation,
            relation_span,
            max_depth,
        } => {
            let table = scope.inner.unwrap_or(scope.table);
            let relations = context.relations.get(table);
            match relations.and_then(|r| r.get(relation)) {
                Some(CodegenRelation::Forward { column, .. }) => Ok(PolicyExpr::Inherits {
                    operation: *op,
                    via_column: column.clone(),
                    max_depth: *max_depth,
                }),
                Some(CodegenRelation::Reverse {
                    table: source,
                    relation: forward,
                }) => {
                    let via = context
                        .relations
                        .get(source)
                        .and_then(|r| r.get(forward))
                        .and_then(|r| match r {
                            CodegenRelation::Forward { column, .. } => Some(column.clone()),
                            CodegenRelation::Reverse { .. } => None,
                        })
                        .ok_or_else(|| {
                            ExprError::new(
                                *relation_span,
                                format!("relation `{relation}` leads back through `{source}.{forward}`, which is not a forward relation"),
                            )
                        })?;
                    Ok(PolicyExpr::InheritsReferencing {
                        operation: *op,
                        source_table: source.clone(),
                        via_column: via,
                        max_depth: *max_depth,
                    })
                }
                None => {
                    let names: Vec<&str> = relations
                        .map(|r| r.keys().map(String::as_str).collect())
                        .unwrap_or_default();
                    Err(ExprError::new(
                        *relation_span,
                        format!(
                            "unknown relation `{relation}` on {table}{}",
                            did_you_mean(relation, names)
                        ),
                    )
                    .example("allowedTo.read(chat)"))
                }
            }
        }
        Ast::Call { name, span, args } => {
            let Some(def) = context.defs.get(name) else {
                let names: Vec<&str> = context.defs.keys().map(String::as_str).collect();
                return Err(ExprError::new(
                    *span,
                    format!("unknown definition `{name}`{}", did_you_mean(name, names)),
                )
                .example("isChatMember(this.id)"));
            };
            if scope.calling.contains(name) {
                return Err(ExprError::new(
                    *span,
                    format!(
                        "`{name}` calls itself ({} → {name}): definitions may not be recursive",
                        scope.calling.join(" → ")
                    ),
                ));
            }
            if def.params.len() != args.len() {
                return Err(ExprError::new(
                    *span,
                    format!(
                        "`{name}` takes {} argument(s), not {}",
                        def.params.len(),
                        args.len()
                    ),
                ));
            }
            let mut calling = scope.calling.clone();
            calling.push(name.clone());
            let params = def
                .params
                .iter()
                .cloned()
                .zip(args.iter().map(|a| resolve(a, scope)))
                .collect();
            let inner = Scope {
                params,
                calling,
                ..scope.clone()
            };
            compile_body(&def.body, context, &inner)
        }
    }
}

/// `operand` with a `$param` replaced by its argument.
fn resolve(operand: &Operand, scope: &Scope<'_>) -> Operand {
    match operand {
        Operand::Param(name, _) => scope
            .params
            .get(name)
            .cloned()
            .unwrap_or_else(|| operand.clone()),
        other => other.clone(),
    }
}

/// When `operand` is a column of the row the condition filters, its name and
/// type (`None` for a magic column).
fn column_of(
    operand: &Operand,
    context: &Context<'_>,
    scope: &Scope<'_>,
) -> Result<Option<(String, Option<ColumnType>)>, ExprError> {
    let Operand::Column { this, name, span } = operand else {
        return Ok(None);
    };
    // Inside `exists`, `this.` is the outer row: a value, not a column.
    let table = match (scope.inner, this) {
        (Some(_), true) => return Ok(None),
        (Some(inner), false) => inner,
        (None, _) => scope.table,
    };
    let ty = column_type(context, table, name, *span)?.cloned();
    Ok(Some((name.clone(), ty)))
}

fn compile_cmp(
    left: &Operand,
    op: CmpOp,
    right: &Operand,
    span: Span,
    context: &Context<'_>,
    scope: &Scope<'_>,
) -> Result<PolicyExpr, ExprError> {
    let (left, right) = (resolve(left, scope), resolve(right, scope));
    if let Some((column, ty)) = column_of(&left, context, scope)? {
        return column_cmp(column, ty, op, &right, context, scope);
    }
    if let Some((column, ty)) = column_of(&right, context, scope)? {
        let flipped = match op {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            same => same,
        };
        return column_cmp(column, ty, flipped, &left, context, scope);
    }
    if let Operand::Session(path, _) = &left {
        return match &right {
            Operand::Null(_) => match op {
                CmpOp::Eq => Ok(PolicyExpr::SessionIsNull { path: path.clone() }),
                CmpOp::Ne => Ok(PolicyExpr::SessionIsNotNull { path: path.clone() }),
                _ => Err(ExprError::new(
                    right.span(),
                    "only `=` and `!=` compare with null",
                )),
            },
            other => Ok(PolicyExpr::SessionCmp {
                path: path.clone(),
                op,
                value: literal_value(other, None)?,
            }),
        };
    }
    Err(ExprError::new(
        span,
        "a comparison needs a column of the row, or a session claim, on one side",
    )
    .example("userId = session.user"))
}

fn column_cmp(
    column: String,
    ty: Option<ColumnType>,
    op: CmpOp,
    value: &Operand,
    context: &Context<'_>,
    scope: &Scope<'_>,
) -> Result<PolicyExpr, ExprError> {
    if let Operand::Null(span) = value {
        return match op {
            CmpOp::Eq => Ok(PolicyExpr::IsNull { column }),
            CmpOp::Ne => Ok(PolicyExpr::IsNotNull { column }),
            _ => Err(ExprError::new(*span, "only `=` and `!=` compare with null")
                .example("joinCode != null")),
        };
    }
    Ok(PolicyExpr::Cmp {
        column,
        op,
        value: policy_value(value, ty.as_ref(), context, scope)?,
    })
}

/// A value a column is compared with: a literal, a session value, or (inside
/// `exists`) a column of the checked row.
fn policy_value(
    operand: &Operand,
    ty: Option<&ColumnType>,
    context: &Context<'_>,
    scope: &Scope<'_>,
) -> Result<PolicyValue, ExprError> {
    match operand {
        Operand::Session(path, _) => Ok(PolicyValue::SessionRef(path.clone())),
        Operand::Column { this, name, span } => {
            if scope.inner.is_some() && *this {
                column_type(context, scope.table, name, *span)?;
                return Ok(PolicyValue::SessionRef(vec![
                    OUTER_ROW.to_string(),
                    name.clone(),
                ]));
            }
            Err(ExprError::new(
                *span,
                "comparing two columns is not supported in jazz.permissions.json yet; inside `exists`, `this.col` is the checked row's column",
            )
            .example("exists chats(id = this.chatId, isPublic = true)"))
        }
        Operand::Param(name, span) => Err(ExprError::new(
            *span,
            format!("`${name}` is not a parameter of this definition"),
        )),
        other => Ok(PolicyValue::Literal(literal_value(other, ty)?)),
    }
}

/// A literal as the engine's [`Value`], typed by the column it is compared
/// with when there is one.
fn literal_value(operand: &Operand, ty: Option<&ColumnType>) -> Result<Value, ExprError> {
    let (literal, span) = match operand {
        Operand::Literal(literal, span) => (literal, *span),
        Operand::Null(_) => return Ok(Value::Null),
        other => {
            return Err(ExprError::new(other.span(), "expected a literal value")
                .example("session.claims.role = 'admin'"));
        }
    };
    let mismatch =
        |what: &str| ExprError::new(span, format!("{what} doesn't fit this column's type"));
    match (literal, ty) {
        (Literal::Bool(b), None | Some(ColumnType::Boolean)) => Ok(Value::Boolean(*b)),
        (Literal::Bool(_), Some(_)) => Err(mismatch("a boolean")),
        (Literal::Str(s), Some(ColumnType::Uuid)) => uuid::Uuid::parse_str(s)
            .map(|u| Value::Uuid(ObjectId::from_uuid(u)))
            .map_err(|_| ExprError::new(span, format!("`{s}` is not a UUID"))),
        (
            Literal::Str(s),
            None | Some(ColumnType::Text | ColumnType::Enum { .. } | ColumnType::Json { .. }),
        ) => Ok(Value::Text(s.clone())),
        (Literal::Str(_), Some(_)) => Err(mismatch("a string")),
        (Literal::Num(n), Some(ColumnType::Double)) => n
            .parse::<f64>()
            .map(Value::Double)
            .map_err(|_| mismatch("this number")),
        (Literal::Num(n), Some(ColumnType::BigInt)) => n
            .parse::<i64>()
            .map(Value::BigInt)
            .map_err(|_| mismatch("this number")),
        (Literal::Num(n), Some(ColumnType::Timestamp)) => n
            .parse::<u64>()
            .map(Value::Timestamp)
            .map_err(|_| mismatch("this number")),
        (Literal::Num(n), None | Some(ColumnType::Integer)) => {
            if n.contains('.') {
                n.parse::<f64>()
                    .map(Value::Double)
                    .map_err(|_| mismatch("this number"))
            } else {
                n.parse::<i32>()
                    .map(Value::Integer)
                    .map_err(|_| mismatch("this number"))
            }
        }
        (Literal::Num(_), Some(_)) => Err(mismatch("a number")),
    }
}

/// `, did you mean `x`?` when a candidate is close to `name`.
pub fn did_you_mean<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> String {
    let lower = name.to_lowercase();
    let best = candidates
        .into_iter()
        .map(|c| (edit_distance(&lower, &c.to_lowercase()), c))
        .filter(|(d, c)| *d <= 2.max(c.len() / 4))
        .min_by_key(|(d, _)| *d);
    match best {
        Some((_, c)) => format!("; did you mean `{c}`?"),
        None => String::new(),
    }
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let next = (prev + usize::from(ca != *cb))
                .min(row[j] + 1)
                .min(row[j + 1] + 1);
            prev = row[j + 1];
            row[j + 1] = next;
        }
    }
    row[b.len()]
}

// ---------------------------------------------------------------------------
// Printing
// ---------------------------------------------------------------------------

/// Why an engine policy can't be printed.
pub fn unsupported(what: &str) -> String {
    format!("{what} is not supported in jazz.permissions.json yet")
}

/// Print `expr`, a policy on `table`, as expression text.
pub fn print(expr: &PolicyExpr, context: &Context<'_>, table: &str) -> Result<String, String> {
    print_in(expr, context, table, None, 0)
}

/// Precedence: 0 any, 1 inside `or`, 2 inside `and`, 3 inside `not`.
fn print_in(
    expr: &PolicyExpr,
    context: &Context<'_>,
    table: &str,
    inner: Option<&str>,
    precedence: u8,
) -> Result<String, String> {
    let wrap = |text: String, mine: u8| {
        if mine < precedence {
            format!("({text})")
        } else {
            text
        }
    };
    Ok(match expr {
        PolicyExpr::True => "true".into(),
        PolicyExpr::False => "false".into(),
        PolicyExpr::Or(items) if items.len() >= 2 => wrap(
            items
                .iter()
                .map(|i| print_in(i, context, table, inner, 1))
                .collect::<Result<Vec<_>, _>>()?
                .join(" or "),
            1,
        ),
        PolicyExpr::And(items) if items.len() >= 2 => wrap(
            items
                .iter()
                .map(|i| print_in(i, context, table, inner, 2))
                .collect::<Result<Vec<_>, _>>()?
                .join(" and "),
            2,
        ),
        PolicyExpr::Or(_) | PolicyExpr::And(_) => {
            return Err(unsupported("an `or` or `and` of fewer than two policies"));
        }
        PolicyExpr::Not(inner_expr) => {
            format!("not {}", print_in(inner_expr, context, table, inner, 3)?)
        }
        PolicyExpr::Cmp { column, op, value } => {
            format!("{column} {} {}", cmp_text(op), value_text(value, inner)?)
        }
        PolicyExpr::SessionCmp { path, op, value } => {
            format!(
                "{} {} {}",
                session_text(path),
                cmp_text(op),
                literal_text(value)?
            )
        }
        PolicyExpr::IsNull { column } => format!("{column} = null"),
        PolicyExpr::IsNotNull { column } => format!("{column} != null"),
        PolicyExpr::SessionIsNull { path } => format!("{} = null", session_text(path)),
        PolicyExpr::SessionIsNotNull { path } => format!("{} != null", session_text(path)),
        PolicyExpr::Contains { column, value } => {
            format!("{column} contains {}", value_text(value, inner)?)
        }
        PolicyExpr::SessionContains { path, value } => {
            format!("{} contains {}", session_text(path), literal_text(value)?)
        }
        PolicyExpr::In {
            column,
            session_path,
        } => format!("{column} in {}", session_text(session_path)),
        PolicyExpr::InList { column, values } => format!(
            "{column} in [{}]",
            values
                .iter()
                .map(|v| value_text(v, inner))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        PolicyExpr::SessionInList { path, values } => format!(
            "{} in [{}]",
            session_text(path),
            values
                .iter()
                .map(literal_text)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        PolicyExpr::Exists {
            table: target,
            condition,
        } => {
            if inner.is_some() {
                return Err(unsupported("`exists` inside `exists`"));
            }
            let conditions = match condition.as_ref() {
                PolicyExpr::True => Vec::new(),
                PolicyExpr::And(items) => items
                    .iter()
                    .map(|i| print_in(i, context, table, Some(target), 0))
                    .collect::<Result<Vec<_>, _>>()?,
                other => vec![print_in(other, context, table, Some(target), 0)?],
            };
            format!("exists {target}({})", conditions.join(", "))
        }
        PolicyExpr::ExistsRel { .. } => {
            return Err(unsupported(
                "a relation expression (`policy.exists(…)`, `hopTo`, joins, unions)",
            ));
        }
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => {
            let on = inner.unwrap_or(table);
            let relation = context
                .relations
                .get(on)
                .and_then(|r| {
                    r.iter().find(|(_, rel)| {
                        matches!(rel, CodegenRelation::Forward { column, .. } if column == via_column)
                    })
                })
                .map(|(name, _)| name.clone())
                .ok_or_else(|| unsupported(&format!("inheriting through `{on}.{via_column}`, which no relation names,")))?;
            allowed_text(*operation, &relation, *max_depth)
        }
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => {
            let on = inner.unwrap_or(table);
            let relation = context
                .relations
                .get(on)
                .and_then(|r| {
                    r.iter().find(|(_, rel)| match rel {
                        CodegenRelation::Reverse { table: source, relation } => {
                            source == source_table
                                && context.relations.get(source).and_then(|s| s.get(relation)).is_some_and(
                                    |f| matches!(f, CodegenRelation::Forward { column, .. } if column == via_column),
                                )
                        }
                        CodegenRelation::Forward { .. } => false,
                    })
                })
                .map(|(name, _)| name.clone())
                .ok_or_else(|| {
                    unsupported(&format!(
                        "inheriting from `{source_table}.{via_column}`, which no reverse relation on {on} names,"
                    ))
                })?;
            allowed_text(*operation, &relation, *max_depth)
        }
    })
}

fn allowed_text(operation: Operation, relation: &str, max_depth: Option<usize>) -> String {
    let op = match operation {
        Operation::Select => "read",
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    };
    match max_depth {
        Some(depth) => format!("allowedTo.{op}({relation}, maxDepth: {depth})"),
        None => format!("allowedTo.{op}({relation})"),
    }
}

fn cmp_text(op: &CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    }
}

fn session_text(path: &[String]) -> String {
    format!("session.{}", path.join("."))
}

fn value_text(value: &PolicyValue, inner: Option<&str>) -> Result<String, String> {
    match value {
        PolicyValue::Literal(value) => literal_text(value),
        PolicyValue::SessionRef(path) if path.first().is_some_and(|p| p == OUTER_ROW) => {
            if inner.is_none() || path.len() != 2 {
                return Err(unsupported(
                    "a reference to the checked row outside `exists`",
                ));
            }
            Ok(format!("this.{}", path[1]))
        }
        PolicyValue::SessionRef(path) => {
            let valid = path == &["user"] || (path.len() >= 2 && path[0] == "claims");
            if !valid {
                return Err(unsupported(&format!(
                    "the session value `{}`",
                    path.join(".")
                )));
            }
            Ok(session_text(path))
        }
    }
}

fn literal_text(value: &Value) -> Result<String, String> {
    Ok(match value {
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Uuid(id) => format!("'{id}'"),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::BigInt(n) => n.to_string(),
        Value::Timestamp(n) => n.to_string(),
        Value::Double(d) => {
            let text = d.to_string();
            if text.contains('.') || text.contains('e') || !d.is_finite() {
                text
            } else {
                format!("{text}.0")
            }
        }
        Value::Null => "null".into(),
        other => return Err(unsupported(&format!("the literal {other:?}"))),
    })
}
