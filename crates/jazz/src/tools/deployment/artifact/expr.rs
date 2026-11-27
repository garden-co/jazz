//! The expression grammar of `jazz.permissions.json` (SPEC §10.10): a
//! hand-written lexer and parser with byte spans, the compile to the
//! engine's [`PolicyExpr`], and the printer back to canonical text.

use std::collections::{BTreeMap, BTreeSet};

use crate::query::{
    OrderDirection, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationOrderBy, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
    RelationValueRef,
};
use crate::tools::ObjectId;
use crate::tools::public_schema::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, Schema, TableName, Value,
};

use super::CodegenRelation;

/// Where the grammar is documented: every expression error links it.
pub const GRAMMAR_URL: &str = "https://jazz.tools/docs/reference/app-files#expressions";

/// The session path TS uses for a column of the row being checked, inside
/// `exists`: `this.col` there.
pub(super) const OUTER_ROW: &str = "__jazz_outer_row";

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
    /// A backtick-quoted name: never a keyword.
    QIdent(String),
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
            b'`' => {
                at += 1;
                let mut text = String::new();
                loop {
                    match bytes.get(at) {
                        None => {
                            return Err(ExprError::new((start, at), "unterminated `name`")
                                .example("`order` = 1"));
                        }
                        Some(b'`') if bytes.get(at + 1) == Some(&b'`') => {
                            text.push('`');
                            at += 2;
                        }
                        Some(b'`') => {
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
                    tok: Tok::QIdent(text),
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
        alias: Option<String>,
        conditions: Vec<Ast>,
        hops: Vec<Hop>,
    },
    /// `exists ( from … )`: any relation, as the engine holds it.
    ExistsRel(RelationExpr),
    Allowed {
        op: Operation,
        target: AllowedTarget,
        /// The bound and where `maxDepth: n` is written.
        max_depth: Option<(usize, Span)>,
        /// From `allowedTo` to the closing `)`.
        span: Span,
    },
}

/// `.rel(…)` after `exists t(…)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub relation: String,
    pub span: Span,
    pub conditions: Vec<Ast>,
}

/// What `allowedTo.op(…)` inherits through.
#[derive(Debug, Clone, PartialEq)]
pub enum AllowedTarget {
    /// A relation of the table, by name.
    Relation(String, Span),
    /// `via col` (this table's forward column) or `via t.col` (rows of `t`
    /// whose `col` points here).
    Via {
        table: Option<String>,
        column: String,
        span: Span,
    },
}

/// What qualifies a column: `this.`, `outer.`, a table or alias, or nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum Qual {
    None,
    This,
    Outer,
    Named(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Column {
        qual: Qual,
        name: String,
        span: Span,
    },
    Literal(Literal, Span),
    /// A typed literal (`int(5)`, `text('a')`, `literal(null)`): exactly
    /// that value, whatever it is compared with.
    Typed(Value, Span),
    Null(Span),
    Session(Vec<String>, Span),
    /// `table_id('T')`: table `T`'s catalogue identity.
    TableId(String, Span),
}

impl Operand {
    fn span(&self) -> Span {
        match self {
            Operand::Column { span, .. }
            | Operand::Literal(_, span)
            | Operand::Typed(_, span)
            | Operand::Null(span)
            | Operand::Session(_, span)
            | Operand::TableId(_, span) => *span,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Str(String),
    Num(String),
    Bool(bool),
}

/// Names of typed literals: `int(5)` and so on.
const TYPED: &[&str] = &[
    "literal",
    "int",
    "bigint",
    "double",
    "timestamp",
    "text",
    "uuid",
    "bytes",
    "txid",
    "bool",
    "array",
    "row",
    "json",
];

/// The deepest a recursive relation (`recursive(…, maxDepth: n)`, the
/// DSL's `gather`) may go, as the TS DSL capped it.
pub const RECURSIVE_MAX_DEPTH_CAP: usize = 64;

/// Words a name must be quoted (`` `order` ``) to be read as a name.
pub(crate) const KEYWORDS: &[&str] = &[
    "and",
    "or",
    "not",
    "true",
    "false",
    "null",
    "exists",
    "in",
    "contains",
    "as",
    "is",
    "this",
    "outer",
    "session",
    "frontier",
    "param",
    "from",
    "join",
    "left",
    "on",
    "where",
    "select",
    "distinct",
    "order",
    "by",
    "limit",
    "offset",
    "union",
    "recursive",
    "asc",
    "desc",
    "via",
    "allowedTo",
    "anyOf",
    "allOf",
    "fixpoint",
];

/// `name` as the grammar writes it: bare when it is a plain identifier and
/// no keyword, otherwise in backticks.
pub fn name_text(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain && !KEYWORDS.contains(&name) {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

struct Parser<'a> {
    tokens: &'a [Token],
    at: usize,
    end: usize,
    depth: usize,
}

/// Nesting (`not`, parentheses, `exists`) deeper than this is refused.
pub const MAX_DEPTH: usize = 64;

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

    fn peek_at(&self, offset: usize) -> Option<&Tok> {
        self.tokens.get(self.at + offset).map(|t| &t.tok)
    }

    /// Whether a `,` continues a query's list (`select`, `order by`) rather
    /// than ending the query: inside `recursive(…)` and `union(…)` a query
    /// is followed by `, <field>: …`.
    fn list_continues(&self) -> bool {
        self.is_tok(&Tok::Comma)
            && !(matches!(self.peek_at(1), Some(Tok::Ident(_)))
                && self.peek_at(2) == Some(&Tok::Colon))
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

    /// `word(`: a keyword or literal name used as a function.
    fn is_call_of(&self, word: &str) -> bool {
        self.is_word(word) && self.peek_at(1) == Some(&Tok::LParen)
    }

    fn is_tok(&self, tok: &Tok) -> bool {
        self.peek_at(0) == Some(tok)
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

    fn expect_word(&mut self, word: &str) -> Result<Span, ExprError> {
        if self.is_word(word) {
            Ok(self.next().expect("peeked").span)
        } else {
            Err(ExprError::new(self.here(), format!("expected `{word}`")))
        }
    }

    /// A name: an identifier or a backtick-quoted name.
    fn name(&mut self, what: &str) -> Result<(String, Span), ExprError> {
        match self.peek().cloned() {
            Some(Token {
                tok: Tok::Ident(n) | Tok::QIdent(n),
                span,
            }) => {
                self.at += 1;
                Ok((n, span))
            }
            _ => Err(ExprError::new(self.here(), format!("expected {what}"))),
        }
    }

    fn deeper<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ExprError>,
    ) -> Result<T, ExprError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(ExprError::new(
                self.here(),
                "the expression nests too deeply",
            ));
        }
        let result = f(self);
        self.depth -= 1;
        result
    }

    fn expr(&mut self) -> Result<Ast, ExprError> {
        self.deeper(|p| p.disjunction())
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
            let inner = self.deeper(|p| p.negation())?;
            return Ok(Ast::Not(Box::new(inner)));
        }
        self.atom()
    }

    /// `(a, b, …)` of expressions, possibly empty.
    fn expr_list(&mut self, example: &'static str) -> Result<Vec<Ast>, ExprError> {
        self.expect(Tok::LParen, "`(`")?;
        let mut items = Vec::new();
        if self.is_tok(&Tok::RParen) {
            self.at += 1;
            return Ok(items);
        }
        loop {
            items.push(self.expr()?);
            match self.next() {
                Some(Token {
                    tok: Tok::Comma, ..
                }) => continue,
                Some(Token {
                    tok: Tok::RParen, ..
                }) => return Ok(items),
                _ => {
                    return Err(ExprError::new(self.here(), "expected `,` or `)`").example(example));
                }
            }
        }
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
                if matches!(self.peek_at(1), Some(Tok::Op(_))) {
                    return self.comparison();
                }
                self.at += 1;
                Ok(Ast::Bool(word == "true"))
            }
            Tok::Ident(word) if (word == "anyOf" || word == "allOf") && self.is_call_of(word) => {
                let any = word == "anyOf";
                self.at += 1;
                let items =
                    self.deeper(|p| p.expr_list("anyOf(isPublic = true, userId = session.user)"))?;
                Ok(if any { Ast::Or(items) } else { Ast::And(items) })
            }
            Tok::Ident(word) if word == "exists" => self.exists(),
            Tok::Ident(word) if word == "allowedTo" && self.peek_at(1) == Some(&Tok::Dot) => {
                self.allowed()
            }
            Tok::Ident(word)
                if !matches!(word.as_str(), "this" | "session" | "null" | "outer")
                    && !TYPED.contains(&word.as_str())
                    && self.peek_at(1) == Some(&Tok::LParen) =>
            {
                Err(ExprError::new(
                    token.span,
                    format!(
                        "unknown function `{word}`; a policy calls only `anyOf`, `allOf` and typed literals such as `int(5)`"
                    ),
                )
                .example("anyOf(isPublic = true, userId = session.user)"))
            }
            _ => self.comparison(),
        }
    }

    fn exists(&mut self) -> Result<Ast, ExprError> {
        const EXAMPLE: &str = "exists chatMembers(chatId = this.id, userId = session.user)";
        self.at += 1;
        if self.is_tok(&Tok::LParen) {
            self.at += 1;
            let rel = self.deeper(|p| p.query())?;
            self.expect(Tok::RParen, "`)` after the relation")?;
            return Ok(Ast::ExistsRel(rel));
        }
        let (table, table_span) = self
            .name("a table after `exists`")
            .map_err(|e| e.example(EXAMPLE))?;
        let mut alias = None;
        if self.is_word("as") {
            self.at += 1;
            alias = Some(self.name("an alias after `as`")?.0);
        }
        if !self.is_tok(&Tok::LParen) {
            return Err(
                ExprError::new(self.here(), "expected `(` and the conditions").example(EXAMPLE),
            );
        }
        let conditions = self.deeper(|p| p.expr_list(EXAMPLE))?;
        let mut hops = Vec::new();
        while self.is_tok(&Tok::Dot) {
            self.at += 1;
            let (relation, span) = self.name("a relation name after `.`")?;
            if !self.is_tok(&Tok::LParen) {
                return Err(
                    ExprError::new(self.here(), "expected `(` and the conditions")
                        .example("exists files(id = this.id).folder(ownerId = session.user)"),
                );
            }
            let conditions = self.deeper(|p| {
                p.expr_list("exists files(id = this.id).folder(ownerId = session.user)")
            })?;
            hops.push(Hop {
                relation,
                span,
                conditions,
            });
        }
        Ok(Ast::Exists {
            table,
            table_span,
            alias,
            conditions,
            hops,
        })
    }

    fn allowed(&mut self) -> Result<Ast, ExprError> {
        const EXAMPLE: &str = "allowedTo.read(chat)";
        let start = self.here().0;
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
        let target = if self.is_word("via") {
            self.at += 1;
            let (first, span) = self.name("a column after `via`")?;
            if self.is_tok(&Tok::Dot) {
                self.at += 1;
                let (column, end) = self.name("a column after `.`")?;
                AllowedTarget::Via {
                    table: Some(first),
                    column,
                    span: (span.0, end.1),
                }
            } else {
                AllowedTarget::Via {
                    table: None,
                    column: first,
                    span,
                }
            }
        } else {
            let (relation, span) = self
                .name("a relation name")
                .map_err(|e| e.example(EXAMPLE))?;
            AllowedTarget::Relation(relation, span)
        };
        let mut max_depth = None;
        if self.is_tok(&Tok::Comma) {
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
                    max_depth = Some((
                        n.parse::<usize>()
                            .map_err(|_| ExprError::new(span, "`maxDepth` takes a whole number"))?,
                        span,
                    ));
                }
                _ => {
                    return Err(ExprError::new(self.here(), "expected `maxDepth: <number>`")
                        .example("allowedTo.read(parent, maxDepth: 8)"));
                }
            }
        }
        let end = self.expect(Tok::RParen, "`)`")?.1;
        Ok(Ast::Allowed {
            op,
            target,
            max_depth,
            span: (start, end),
        })
    }

    fn comparison(&mut self) -> Result<Ast, ExprError> {
        let left = self.operand()?;
        let start = left.span().0;
        if self.is_word("in") {
            self.at += 1;
            if self.is_tok(&Tok::LBracket) {
                self.at += 1;
                let mut values = Vec::new();
                if !self.is_tok(&Tok::RBracket) {
                    loop {
                        values.push(self.operand()?);
                        if self.is_tok(&Tok::Comma) {
                            self.at += 1;
                        } else {
                            break;
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
        let op = self.cmp_op()?;
        let right = self.operand()?;
        let end = right.span().1;
        Ok(Ast::Cmp {
            left,
            op,
            right,
            span: (start, end),
        })
    }

    fn cmp_op(&mut self) -> Result<CmpOp, ExprError> {
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
        Ok(op)
    }

    /// The name after `this.` / `outer.` / `t.`: a column, magic or not.
    fn column_name(&mut self) -> Result<(String, usize), ExprError> {
        match self.next() {
            Some(Token {
                tok: Tok::Ident(name) | Tok::QIdent(name),
                span,
            }) => Ok((name, span.1)),
            Some(Token {
                tok: Tok::Dollar(name),
                span,
            }) => self.dotted(format!("${name}"), span),
            _ => Err(ExprError::new(self.here(), "expected a column")),
        }
    }

    /// `session.a.b`: a session path.
    fn session_path(&mut self, start: usize) -> Result<(Vec<String>, Span), ExprError> {
        self.expect(Tok::Dot, "`.user` or `.claims.<name>` after `session`")?;
        let mut path = Vec::new();
        let mut end;
        loop {
            let (part, span) = self
                .name("a name")
                .map_err(|e| e.example("session.claims.role = 'admin'"))?;
            end = span.1;
            path.push(part);
            if self.is_tok(&Tok::Dot) {
                self.at += 1;
            } else {
                break;
            }
        }
        let parts: Vec<&str> = path.iter().map(String::as_str).collect();
        let known = matches!(
            parts.as_slice(),
            ["user"]
                | ["identity"]
                | ["identity", "subject"]
                | ["identity", "issuer"]
                | ["authMode"]
                | ["claims", _, ..]
        );
        if parts.first() == Some(&OUTER_ROW) {
            return Err(ExprError::new(
                (start, end),
                "`session.__jazz_outer_row` is not a session value; inside `exists`, the checked row's columns are `this.col`",
            )
            .example("exists shares(todoId = this.id)"));
        }
        if !known {
            return Err(ExprError::new(
                (start, end),
                "a session value is `session.user` or `session.claims.<name>` (or `session.identity`, `session.identity.subject`, `session.identity.issuer`, `session.authMode`)",
            )
            .example("session.claims.role = 'admin'"));
        }
        Ok((path, (start, end)))
    }

    fn operand(&mut self) -> Result<Operand, ExprError> {
        let Some(token) = self.peek().cloned() else {
            return Err(ExprError::new(self.here(), "expected a column or a value"));
        };
        let span = token.span;
        if let Tok::Ident(word) = &token.tok
            && TYPED.contains(&word.as_str())
            && word != "json"
            && self.peek_at(1) == Some(&Tok::LParen)
        {
            let (value, span) = self.typed_literal()?;
            return Ok(Operand::Typed(value, span));
        }
        if let Tok::Ident(word) = &token.tok
            && word == "table_id"
            && self.peek_at(1) == Some(&Tok::LParen)
        {
            self.at += 1;
            self.expect(Tok::LParen, "`(`")?;
            let (table, _) = self.string_arg().map_err(|error| {
                error
                    .example("exists projects(id = this.projectId, scopeId = table_id('projects'))")
            })?;
            let end = self.expect(Tok::RParen, "`)`")?;
            return Ok(Operand::TableId(table, (span.0, end.1)));
        }
        self.at += 1;
        match token.tok {
            Tok::Str(text) => Ok(Operand::Literal(Literal::Str(text), span)),
            Tok::Num(n) => Ok(Operand::Literal(Literal::Num(n), span)),
            Tok::Dollar(name) => {
                let column = format!("${name}");
                if MAGIC_COLUMNS.iter().any(|m| m.starts_with(&column)) {
                    let (name, end) = self.dotted(column, span)?;
                    return Ok(Operand::Column {
                        qual: Qual::None,
                        name,
                        span: (span.0, end),
                    });
                }
                Err(ExprError::new(
                    span,
                    format!(
                        "unknown column `{column}`{}",
                        did_you_mean(&column, MAGIC_COLUMNS.iter().copied())
                    ),
                ))
            }
            Tok::Ident(word) if word == "null" => Ok(Operand::Null(span)),
            Tok::Ident(word) if word == "true" => Ok(Operand::Literal(Literal::Bool(true), span)),
            Tok::Ident(word) if word == "false" => Ok(Operand::Literal(Literal::Bool(false), span)),
            Tok::Ident(word) if word == "session" && self.is_tok(&Tok::Dot) => {
                let (path, span) = self.session_path(span.0)?;
                Ok(Operand::Session(path, span))
            }
            Tok::Ident(word) if (word == "this" || word == "outer") && self.is_tok(&Tok::Dot) => {
                self.at += 1;
                let (name, end) = self.column_name()?;
                Ok(Operand::Column {
                    qual: if word == "this" {
                        Qual::This
                    } else {
                        Qual::Outer
                    },
                    name,
                    span: (span.0, end),
                })
            }
            Tok::Ident(word) | Tok::QIdent(word) => {
                if self.is_tok(&Tok::Dot)
                    && matches!(
                        self.peek_at(1),
                        Some(Tok::Ident(_) | Tok::QIdent(_) | Tok::Dollar(_))
                    )
                {
                    self.at += 1;
                    let (name, end) = self.column_name()?;
                    return Ok(Operand::Column {
                        qual: Qual::Named(word),
                        name,
                        span: (span.0, end),
                    });
                }
                Ok(Operand::Column {
                    qual: Qual::None,
                    name: word,
                    span,
                })
            }
            _ => Err(ExprError::new(span, "expected a column or a value")
                .example("userId = session.user")),
        }
    }

    /// `$createdBy` then `.account`: magic names have one dotted part.
    fn dotted(&mut self, name: String, span: Span) -> Result<(String, usize), ExprError> {
        if name.starts_with('$')
            && self.is_tok(&Tok::Dot)
            && matches!(self.peek_at(1), Some(Tok::Ident(_)))
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

    // -- Typed literals ------------------------------------------------------

    fn string_arg(&mut self) -> Result<(String, Span), ExprError> {
        match self.next() {
            Some(Token {
                tok: Tok::Str(s),
                span,
            }) => Ok((s, span)),
            _ => Err(ExprError::new(self.here(), "expected a string `'…'`")),
        }
    }

    fn number_arg(&mut self) -> Result<(String, Span), ExprError> {
        match self.next() {
            Some(Token {
                tok: Tok::Num(n),
                span,
            }) => Ok((n, span)),
            Some(Token {
                tok: Tok::Str(s),
                span,
            }) => Ok((s, span)),
            _ => Err(ExprError::new(self.here(), "expected a number")),
        }
    }

    /// `int(5)`, `text('a')`, `array(int(1), null)`, `literal(null)`, …
    fn typed_literal(&mut self) -> Result<(Value, Span), ExprError> {
        let (kind, start) = self.name("a literal")?;
        self.expect(Tok::LParen, "`(`")?;
        let bad =
            |span: Span, what: &str| ExprError::new(span, format!("`{kind}(…)` takes {what}"));
        let value = match kind.as_str() {
            "literal" => match self.next() {
                Some(Token {
                    tok: Tok::Str(s), ..
                }) => Value::Text(s),
                Some(Token {
                    tok: Tok::Num(n),
                    span,
                }) => {
                    if n.contains('.') {
                        Value::Double(n.parse().map_err(|_| bad(span, "a number"))?)
                    } else {
                        Value::Integer(n.parse().map_err(|_| bad(span, "an int"))?)
                    }
                }
                Some(Token {
                    tok: Tok::Ident(w),
                    span,
                }) => match w.as_str() {
                    "true" => Value::Boolean(true),
                    "false" => Value::Boolean(false),
                    "null" => Value::Null,
                    _ => return Err(bad(span, "a literal")),
                },
                _ => return Err(bad(self.here(), "a literal")),
            },
            "int" => {
                let (n, span) = self.number_arg()?;
                Value::Integer(n.parse().map_err(|_| bad(span, "a 32-bit integer"))?)
            }
            "bigint" => {
                let (n, span) = self.number_arg()?;
                Value::BigInt(n.parse().map_err(|_| bad(span, "a 64-bit integer"))?)
            }
            "double" => {
                let (n, span) = self.number_arg()?;
                Value::Double(n.parse().map_err(|_| bad(span, "a number"))?)
            }
            "timestamp" => {
                let (n, span) = self.number_arg()?;
                Value::Timestamp(n.parse().map_err(|_| bad(span, "a whole number"))?)
            }
            "text" => Value::Text(self.string_arg()?.0),
            "uuid" => {
                let (s, span) = self.string_arg()?;
                Value::Uuid(ObjectId::from_uuid(
                    uuid::Uuid::parse_str(&s).map_err(|_| bad(span, "a UUID"))?,
                ))
            }
            "bytes" => {
                let (s, span) = self.string_arg()?;
                Value::Bytea(hex::decode(&s).map_err(|_| bad(span, "hex bytes"))?)
            }
            "txid" => {
                let (s, span) = self.string_arg()?;
                let bytes = hex::decode(&s).map_err(|_| bad(span, "16 hex bytes"))?;
                Value::TransactionId(bytes.try_into().map_err(|_| bad(span, "16 hex bytes"))?)
            }
            "bool" => match self.next() {
                Some(Token {
                    tok: Tok::Ident(w), ..
                }) if w == "true" => Value::Boolean(true),
                Some(Token {
                    tok: Tok::Ident(w), ..
                }) if w == "false" => Value::Boolean(false),
                _ => return Err(bad(self.here(), "`true` or `false`")),
            },
            "array" => {
                let mut items = Vec::new();
                if !self.is_tok(&Tok::RParen) {
                    loop {
                        items.push(self.typed_arg()?);
                        if self.is_tok(&Tok::Comma) {
                            self.at += 1;
                        } else {
                            break;
                        }
                    }
                }
                Value::Array(items)
            }
            "row" => {
                let id = match self.typed_arg()? {
                    Value::Null => None,
                    Value::Uuid(id) => Some(id),
                    _ => return Err(bad(self.here(), "its id (`uuid('…')` or `null`) first")),
                };
                let mut values = Vec::new();
                while self.is_tok(&Tok::Comma) {
                    self.at += 1;
                    values.push(self.typed_arg()?);
                }
                Value::Row { id, values }
            }
            other => {
                return Err(ExprError::new(
                    start,
                    format!("unknown literal `{other}(…)`"),
                ));
            }
        };
        let end = self.expect(Tok::RParen, "`)`")?.1;
        Ok((value, (start.0, end)))
    }

    fn typed_arg(&mut self) -> Result<Value, ExprError> {
        if self.is_word("null") {
            self.at += 1;
            return Ok(Value::Null);
        }
        if let Some(Tok::Ident(w)) = self.peek_at(0)
            && TYPED.contains(&w.as_str())
        {
            return Ok(self.deeper(|p| p.typed_literal())?.0);
        }
        Err(ExprError::new(
            self.here(),
            "inside a typed literal, every value is typed too",
        )
        .example("array(int(1), int(2))"))
    }

    // -- Relations -----------------------------------------------------------

    /// `from … { clause }`.
    fn query(&mut self) -> Result<RelationExpr, ExprError> {
        const EXAMPLE: &str = "exists (from files join folders as f on files.folderId = f.id where f.ownerId = session.user)";
        if !self.is_word("from") {
            return Err(
                ExprError::new(self.here(), "a relation starts with `from`").example(EXAMPLE)
            );
        }
        self.at += 1;
        let mut rel = self.source()?;
        loop {
            if self.is_word("left") || self.is_word("join") {
                let join_kind = if self.is_word("left") {
                    self.at += 1;
                    RelationJoinKind::Left
                } else {
                    RelationJoinKind::Inner
                };
                self.expect_word("join")?;
                let right = self.source()?;
                let mut on = Vec::new();
                if self.is_word("on") {
                    self.at += 1;
                    loop {
                        let left = self.colref()?;
                        match self.next() {
                            Some(Token {
                                tok: Tok::Op("="), ..
                            }) => {}
                            _ => {
                                return Err(ExprError::new(
                                    self.here(),
                                    "join conditions are `a.col = b.col`",
                                ));
                            }
                        }
                        let right = self.colref()?;
                        on.push(RelationJoinCondition { left, right });
                        if self.is_word("and") {
                            self.at += 1;
                        } else {
                            break;
                        }
                    }
                }
                rel = RelationExpr::Join {
                    left: Box::new(rel),
                    right: Box::new(right),
                    on,
                    join_kind,
                };
            } else if self.is_word("where") {
                self.at += 1;
                let predicate = self.deeper(|p| p.predicate())?;
                rel = RelationExpr::Filter {
                    input: Box::new(rel),
                    predicate,
                };
            } else if self.is_word("select") {
                self.at += 1;
                let mut columns = Vec::new();
                if self.is_tok(&Tok::LParen) && self.peek_at(1) == Some(&Tok::RParen) {
                    self.at += 2;
                } else {
                    loop {
                        let expr = match self.key()? {
                            RelationKeyRef::Column(c) => RelationProjectExpr::Column(c),
                            RelationKeyRef::RowId(r) => RelationProjectExpr::RowId(r),
                        };
                        self.expect_word("as")?;
                        let (alias, _) = self.name("a name after `as`")?;
                        columns.push(RelationProjectColumn { alias, expr });
                        if self.list_continues() {
                            self.at += 1;
                        } else {
                            break;
                        }
                    }
                }
                rel = RelationExpr::Project {
                    input: Box::new(rel),
                    columns,
                };
            } else if self.is_word("distinct") {
                self.at += 1;
                self.expect_word("on")?;
                let key = self.key_list()?;
                rel = RelationExpr::Distinct {
                    input: Box::new(rel),
                    key,
                };
            } else if self.is_word("order") {
                self.at += 1;
                self.expect_word("by")?;
                let mut terms = Vec::new();
                if self.is_tok(&Tok::LParen) && self.peek_at(1) == Some(&Tok::RParen) {
                    self.at += 2;
                } else {
                    loop {
                        let column = self.colref()?;
                        let direction = if self.is_word("desc") {
                            self.at += 1;
                            OrderDirection::Desc
                        } else {
                            if self.is_word("asc") {
                                self.at += 1;
                            }
                            OrderDirection::Asc
                        };
                        terms.push(RelationOrderBy { column, direction });
                        if self.list_continues() {
                            self.at += 1;
                        } else {
                            break;
                        }
                    }
                }
                rel = RelationExpr::OrderBy {
                    input: Box::new(rel),
                    terms,
                };
            } else if self.is_word("limit") || self.is_word("offset") {
                let limit = self.is_word("limit");
                self.at += 1;
                let n = match self.next() {
                    Some(Token {
                        tok: Tok::Num(n),
                        span,
                    }) => n
                        .parse::<usize>()
                        .map_err(|_| ExprError::new(span, "expected a whole number"))?,
                    _ => return Err(ExprError::new(self.here(), "expected a whole number")),
                };
                rel = if limit {
                    RelationExpr::Limit {
                        input: Box::new(rel),
                        limit: n,
                    }
                } else {
                    RelationExpr::Offset {
                        input: Box::new(rel),
                        offset: n,
                    }
                };
            } else {
                return Ok(rel);
            }
        }
    }

    /// A relation `from`/`join` reads: a table, a parenthesised query, a
    /// union or a recursive relation.
    fn source(&mut self) -> Result<RelationExpr, ExprError> {
        if self.is_tok(&Tok::LParen) {
            self.at += 1;
            let rel = self.deeper(|p| p.query())?;
            self.expect(Tok::RParen, "`)`")?;
            return Ok(rel);
        }
        if self.is_call_of("union") {
            self.at += 2;
            let mut inputs = Vec::new();
            if !self.is_tok(&Tok::RParen) {
                loop {
                    let (label, _) = self.name("a label")?;
                    self.expect(Tok::Colon, "`:`")?;
                    let input = self.deeper(|p| p.query())?;
                    inputs.push(RelationUnionArm { label, input });
                    if self.is_tok(&Tok::Comma) {
                        self.at += 1;
                    } else {
                        break;
                    }
                }
            }
            self.expect(Tok::RParen, "`)`")?;
            return Ok(RelationExpr::Union { inputs });
        }
        if self.is_call_of("recursive") {
            self.at += 2;
            let field = |p: &mut Self, name: &str| -> Result<(), ExprError> {
                p.expect_word(name)?;
                p.expect(Tok::Colon, "`:`")?;
                Ok(())
            };
            field(self, "seed")?;
            let seed = self.deeper(|p| p.query())?;
            self.expect(Tok::Comma, "`,`")?;
            field(self, "step")?;
            let step = self.deeper(|p| p.query())?;
            self.expect(Tok::Comma, "`,`")?;
            field(self, "frontier")?;
            let frontier_key = self.key()?;
            self.expect(Tok::Comma, "`,`")?;
            field(self, "dedupe")?;
            let dedupe_key = self.key_list()?;
            self.expect(Tok::Comma, "`,`")?;
            field(self, "maxDepth")?;
            let bound = if self.is_word("fixpoint") {
                self.at += 1;
                RecursionBound::Fixpoint
            } else {
                match self.next() {
                    Some(Token {
                        tok: Tok::Num(n),
                        span,
                    }) => {
                        let depth: usize = n
                            .parse()
                            .map_err(|_| ExprError::new(span, "expected a whole number"))?;
                        if depth > RECURSIVE_MAX_DEPTH_CAP {
                            return Err(ExprError::new(
                                span,
                                format!(
                                    "`maxDepth` {depth} exceeds the cap of {RECURSIVE_MAX_DEPTH_CAP} for a recursive relation"
                                ),
                            )
                            .example("recursive(…, maxDepth: 8)"));
                        }
                        RecursionBound::MaxDepth(depth)
                    }
                    _ => {
                        return Err(ExprError::new(
                            self.here(),
                            "expected a number or `fixpoint`",
                        ));
                    }
                }
            };
            self.expect(Tok::RParen, "`)`")?;
            return Ok(RelationExpr::Gather {
                seed: Box::new(seed),
                step: Box::new(step),
                frontier_key,
                bound,
                dedupe_key,
            });
        }
        let (table, _) = self.name("a table")?;
        let mut alias = None;
        if self.is_word("as") {
            self.at += 1;
            alias = Some(self.name("an alias after `as`")?.0);
        }
        Ok(RelationExpr::TableScan { table, alias })
    }

    fn colref(&mut self) -> Result<RelationColumnRef, ExprError> {
        let (first, _) = self.name("a column")?;
        if self.is_tok(&Tok::Dot) && matches!(self.peek_at(1), Some(Tok::Ident(_) | Tok::QIdent(_)))
        {
            self.at += 1;
            let (column, _) = self.name("a column")?;
            return Ok(RelationColumnRef {
                scope: Some(first),
                column,
            });
        }
        Ok(RelationColumnRef {
            scope: None,
            column: first,
        })
    }

    fn row_id(&mut self) -> Result<RelationRowIdRef, ExprError> {
        self.at += 2;
        let (which, span) = self.name("`row`, `outer` or `frontier`")?;
        let row = match which.as_str() {
            "row" => RelationRowIdRef::Current,
            "outer" => RelationRowIdRef::Outer,
            "frontier" => RelationRowIdRef::Frontier,
            _ => {
                return Err(ExprError::new(
                    span,
                    "`id(…)` takes `row`, `outer` or `frontier`",
                ));
            }
        };
        self.expect(Tok::RParen, "`)`")?;
        Ok(row)
    }

    fn key(&mut self) -> Result<RelationKeyRef, ExprError> {
        if self.is_call_of("id") {
            return Ok(RelationKeyRef::RowId(self.row_id()?));
        }
        Ok(RelationKeyRef::Column(self.colref()?))
    }

    fn key_list(&mut self) -> Result<Vec<RelationKeyRef>, ExprError> {
        self.expect(Tok::LParen, "`(`")?;
        let mut keys = Vec::new();
        if !self.is_tok(&Tok::RParen) {
            loop {
                keys.push(self.key()?);
                if self.is_tok(&Tok::Comma) {
                    self.at += 1;
                } else {
                    break;
                }
            }
        }
        self.expect(Tok::RParen, "`)`")?;
        Ok(keys)
    }

    fn predicate(&mut self) -> Result<RelationPredicate, ExprError> {
        let mut terms = vec![self.p_and()?];
        while self.is_word("or") {
            self.at += 1;
            terms.push(self.p_and()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().expect("one")
        } else {
            RelationPredicate::Or(terms)
        })
    }

    fn p_and(&mut self) -> Result<RelationPredicate, ExprError> {
        let mut terms = vec![self.p_not()?];
        while self.is_word("and") {
            self.at += 1;
            terms.push(self.p_not()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().expect("one")
        } else {
            RelationPredicate::And(terms)
        })
    }

    fn p_not(&mut self) -> Result<RelationPredicate, ExprError> {
        if self.is_word("not") {
            self.at += 1;
            let inner = self.deeper(|p| p.p_not())?;
            return Ok(RelationPredicate::Not(Box::new(inner)));
        }
        self.p_atom()
    }

    fn p_atom(&mut self) -> Result<RelationPredicate, ExprError> {
        if self.is_tok(&Tok::LParen) {
            self.at += 1;
            let inner = self.deeper(|p| p.predicate())?;
            self.expect(Tok::RParen, "`)`")?;
            return Ok(inner);
        }
        if self.is_word("allowedTo") && self.peek_at(1) == Some(&Tok::Dot) {
            // Read it whole, to point at all of it.
            if let Ast::Allowed { op, span, .. } = self.allowed()? {
                return Err(ExprError::new(span, allowed_in_exists(op)));
            }
        }
        if self.is_word("true") {
            self.at += 1;
            return Ok(RelationPredicate::True);
        }
        if self.is_word("false") {
            self.at += 1;
            return Ok(RelationPredicate::False);
        }
        if self.is_call_of("anyOf") || self.is_call_of("allOf") {
            let any = self.is_word("anyOf");
            self.at += 2;
            let mut items = Vec::new();
            if !self.is_tok(&Tok::RParen) {
                loop {
                    items.push(self.deeper(|p| p.predicate())?);
                    if self.is_tok(&Tok::Comma) {
                        self.at += 1;
                    } else {
                        break;
                    }
                }
            }
            self.expect(Tok::RParen, "`)`")?;
            return Ok(if any {
                RelationPredicate::Or(items)
            } else {
                RelationPredicate::And(items)
            });
        }
        let left = self.colref()?;
        if self.is_word("in") {
            self.at += 1;
            self.expect(Tok::LBracket, "`[`")?;
            let mut values = Vec::new();
            if !self.is_tok(&Tok::RBracket) {
                loop {
                    values.push(self.value_ref()?);
                    if self.is_tok(&Tok::Comma) {
                        self.at += 1;
                    } else {
                        break;
                    }
                }
            }
            self.expect(Tok::RBracket, "`]`")?;
            return Ok(RelationPredicate::In { left, values });
        }
        if self.is_word("contains") {
            self.at += 1;
            let right = self.value_ref()?;
            return Ok(RelationPredicate::Contains { left, right });
        }
        let op = self.cmp_op()?;
        if self.is_word("null") && matches!(op, CmpOp::Eq | CmpOp::Ne) {
            self.at += 1;
            return Ok(if op == CmpOp::Eq {
                RelationPredicate::IsNull { column: left }
            } else {
                RelationPredicate::IsNotNull { column: left }
            });
        }
        let right = self.value_ref()?;
        let op = match op {
            CmpOp::Eq => RelationCmpOp::Eq,
            CmpOp::Ne => RelationCmpOp::Ne,
            CmpOp::Lt => RelationCmpOp::Lt,
            CmpOp::Le => RelationCmpOp::Le,
            CmpOp::Gt => RelationCmpOp::Gt,
            CmpOp::Ge => RelationCmpOp::Ge,
        };
        Ok(RelationPredicate::Cmp { left, op, right })
    }

    fn value_ref(&mut self) -> Result<RelationValueRef, ExprError> {
        let Some(token) = self.peek().cloned() else {
            return Err(ExprError::new(self.here(), "expected a value"));
        };
        if let Tok::Ident(word) = &token.tok {
            match word.as_str() {
                "id" if self.peek_at(1) == Some(&Tok::LParen) => {
                    return Ok(RelationValueRef::RowId(self.row_id()?));
                }
                "json" if self.peek_at(1) == Some(&Tok::LParen) => {
                    self.at += 2;
                    let (text, span) = self.string_arg()?;
                    self.expect(Tok::RParen, "`)`")?;
                    let value = serde_json::from_str(&text)
                        .map_err(|e| ExprError::new(span, format!("not JSON: {e}")))?;
                    return Ok(RelationValueRef::Literal(value));
                }
                w if TYPED.contains(&w) && self.peek_at(1) == Some(&Tok::LParen) => {
                    let (value, _) = self.typed_literal()?;
                    return Ok(RelationValueRef::Literal(
                        serde_json::to_value(value).expect("values serialize"),
                    ));
                }
                "true" | "false" => {
                    self.at += 1;
                    return Ok(RelationValueRef::Literal(serde_json::Value::Bool(
                        word == "true",
                    )));
                }
                "session" if self.peek_at(1) == Some(&Tok::Dot) => {
                    self.at += 1;
                    let (path, _) = self.session_path(token.span.0)?;
                    return Ok(RelationValueRef::SessionRef(path));
                }
                "param" if self.peek_at(1) == Some(&Tok::Dot) => {
                    self.at += 2;
                    let (name, _) = self.name("a parameter name")?;
                    return Ok(RelationValueRef::Param(name));
                }
                "outer" | "frontier" if self.peek_at(1) == Some(&Tok::Dot) => {
                    let outer = word == "outer";
                    self.at += 2;
                    // `outer.col` or `outer.scope.col`.
                    let (first, _) = self.name("a column")?;
                    let column = if self.is_tok(&Tok::Dot) {
                        self.at += 1;
                        let (column, _) = self.name("a column")?;
                        RelationColumnRef {
                            scope: Some(first),
                            column,
                        }
                    } else {
                        RelationColumnRef {
                            scope: None,
                            column: first,
                        }
                    };
                    return Ok(if outer {
                        RelationValueRef::OuterColumn(column)
                    } else {
                        RelationValueRef::FrontierColumn(column)
                    });
                }
                _ => {}
            }
        }
        self.at += 1;
        match token.tok {
            Tok::Str(s) => Ok(RelationValueRef::Literal(serde_json::Value::String(s))),
            Tok::Num(n) => serde_json::from_str(&n)
                .map(RelationValueRef::Literal)
                .map_err(|_| ExprError::new(token.span, "not a number")),
            _ => Err(ExprError::new(
                token.span,
                "expected a value: a literal, `session.…`, `param.…`, `outer.…`, `frontier.…` or `id(…)`",
            )
            .example("f.ownerId = session.user")),
        }
    }
}

// ---------------------------------------------------------------------------
// Compiling
// ---------------------------------------------------------------------------

/// Everything names resolve against.
pub struct Context<'a> {
    pub schema: &'a Schema,
    pub relations: &'a BTreeMap<String, BTreeMap<String, CodegenRelation>>,
    /// Each table's computed fields: not stored, so no policy reads them;
    /// named only to say so when one does.
    pub computed: &'a ComputedNames,
}

/// Table → the names of its computed fields.
pub type ComputedNames = BTreeMap<String, BTreeSet<String>>;

/// One `exists` level a compile is in: its table, and the alias that names it.
#[derive(Clone)]
struct Level {
    table: String,
    alias: Option<String>,
}

impl Level {
    fn name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

/// Where a compile is: the checked table, then one level per `exists`.
#[derive(Clone)]
struct Scope {
    levels: Vec<Level>,
}

impl Scope {
    fn depth(&self) -> usize {
        self.levels.len() - 1
    }

    fn current(&self) -> &Level {
        self.levels.last().expect("a scope has the checked table")
    }
}

/// Compile `ast` as a policy on `table`.
pub fn compile(ast: &Ast, context: &Context<'_>, table: &str) -> Result<PolicyExpr, ExprError> {
    let scope = Scope {
        levels: vec![Level {
            table: table.to_string(),
            alias: None,
        }],
    };
    compile_in(ast, context, &scope)
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
    if context
        .computed
        .get(table)
        .is_some_and(|fields| fields.contains(column))
    {
        return Err(ExprError::new(
            span,
            format!(
                "unknown column `{column}` in {table}: `{column}` is a computed field, which isn't \
                 stored, so a permission can't read it; write the policy over the columns it's \
                 computed from"
            ),
        ));
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

fn known_table(context: &Context<'_>, table: &str, span: Span) -> Result<(), ExprError> {
    if context.schema.get(&TableName::from(table)).is_some() {
        return Ok(());
    }
    let names: Vec<String> = context
        .schema
        .keys()
        .map(|t| t.as_str().to_string())
        .collect();
    Err(ExprError::new(
        span,
        format!(
            "unknown table `{table}`{}",
            did_you_mean(table, names.iter().map(String::as_str))
        ),
    )
    .example("exists chatMembers(chatId = this.id, userId = session.user)"))
}

fn compile_all(
    items: &[Ast],
    context: &Context<'_>,
    scope: &Scope,
) -> Result<Vec<PolicyExpr>, ExprError> {
    items
        .iter()
        .map(|i| compile_in(i, context, scope))
        .collect()
}

fn compile_in(ast: &Ast, context: &Context<'_>, scope: &Scope) -> Result<PolicyExpr, ExprError> {
    match ast {
        Ast::Bool(true) => Ok(PolicyExpr::True),
        Ast::Bool(false) => Ok(PolicyExpr::False),
        Ast::Or(items) => Ok(PolicyExpr::Or(compile_all(items, context, scope)?)),
        Ast::And(items) => Ok(PolicyExpr::And(compile_all(items, context, scope)?)),
        Ast::Not(inner) => {
            // Inside an `exists`, its own refusal names the problem.
            if let Some((op, span)) = negated_allowed(inner).filter(|_| scope.depth() == 0) {
                return Err(ExprError::new(span, not_allowed(op)));
            }
            Ok(PolicyExpr::Not(Box::new(compile_in(
                inner, context, scope,
            )?)))
        }
        Ast::Cmp {
            left,
            op,
            right,
            span,
        } => compile_cmp(left, op.clone(), right, *span, context, scope),
        Ast::InList { left, values, span } => {
            let left = left.clone();
            match classify(&left, context, scope)? {
                Ref::Column(column, ty) => Ok(PolicyExpr::InList {
                    column,
                    values: values
                        .iter()
                        .map(|v| policy_value(v, ty.as_ref(), context, scope))
                        .collect::<Result<_, _>>()?,
                }),
                _ => match left {
                    Operand::Session(path, _) => Ok(PolicyExpr::SessionInList {
                        values: values
                            .iter()
                            .map(|v| session_literal(&path, v))
                            .collect::<Result<_, _>>()?,
                        path,
                    }),
                    other => Err(ExprError::new(
                        other.span().min(*span),
                        "`in` tests a column of the row or a session claim",
                    )
                    .example("role in ['admin', 'editor']")),
                },
            }
        }
        Ast::InSession { left, path, .. } => {
            let left = left.clone();
            match classify(&left, context, scope)? {
                Ref::Column(column, _) => Ok(PolicyExpr::In {
                    column,
                    session_path: path.clone(),
                }),
                _ => Err(
                    ExprError::new(left.span(), "`in session.…` tests a column of the row")
                        .example("teamId in session.claims.teams"),
                ),
            }
        }
        Ast::Contains { left, right, .. } => {
            let left = left.clone();
            let right = right.clone();
            match classify(&left, context, scope)? {
                Ref::Column(column, ty) => {
                    let element = match ty {
                        Some(ColumnType::Array { element }) => Some(*element),
                        _ => None,
                    };
                    Ok(PolicyExpr::Contains {
                        column,
                        value: policy_value(&right, element.as_ref(), context, scope)?,
                    })
                }
                _ => match left {
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
            alias,
            conditions,
            hops,
        } => {
            known_table(context, table, *table_span)?;
            if !hops.is_empty() {
                return compile_hops(table, alias.as_deref(), conditions, hops, context, scope);
            }
            let mut inner = scope.clone();
            inner.levels.push(Level {
                table: table.clone(),
                alias: alias.clone(),
            });
            let mut compiled = compile_all(conditions, context, &inner)?;
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
        Ast::ExistsRel(rel) => Ok(PolicyExpr::ExistsRel { rel: rel.clone() }),
        Ast::Allowed { op, span, .. } if scope.depth() > 0 => {
            Err(ExprError::new(*span, allowed_in_exists(*op)))
        }
        Ast::Allowed {
            op,
            target,
            max_depth,
            ..
        } => compile_allowed(*op, target, *max_depth, context, scope),
    }
}

/// The first `allowedTo` under a `not`, through `anyOf` / `allOf` / `not`
/// (an `exists` refuses its own), with its operation and span.
fn negated_allowed(ast: &Ast) -> Option<(Operation, Span)> {
    match ast {
        Ast::Allowed { op, span, .. } => Some((*op, *span)),
        Ast::Or(items) | Ast::And(items) => items.iter().find_map(negated_allowed),
        Ast::Not(inner) => negated_allowed(inner),
        _ => None,
    }
}

/// Why `not allowedTo…` is refused: the engine checks inheritance only as
/// a grant, never negated. (Negating the other table's rule would take
/// `not exists`, which the engine refuses too.)
pub fn not_allowed(op: Operation) -> String {
    let op = operation_name(op);
    format!(
        "`not allowedTo.{op}(…)` is not supported yet: write the rule without negating \
         inheritance (`not` works on this row's columns and session values)"
    )
}

fn operation_name(op: Operation) -> &'static str {
    match op {
        Operation::Select => "read",
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    }
}

/// Why `allowedTo` inside an `exists` is refused: the engine checks
/// inheritance only on the row a rule is for, not on rows an `exists`
/// reaches, so it would refuse the policy when the schema compiles.
pub fn allowed_in_exists(op: Operation) -> String {
    let op = operation_name(op);
    format!(
        "`allowedTo.{op}(…)` inside `exists` is not supported yet: repeat the other table's \
         {op} rule there instead (e.g. share it as a helper function)"
    )
}

/// Why `allowedTo` along a reverse relation takes no `maxDepth`, as the TS
/// DSL refused it: the engine checks every row pointing here, and refuses
/// reverse inheritance cycles when it compiles the schema, so a bound
/// would be dropped rather than applied.
const REVERSE_MAX_DEPTH: &str = "`allowedTo` along a reverse relation takes no `maxDepth`; omit it (reverse inheritance cycles are refused when the schema compiles, so the rows pointing here are always all checked)";

fn compile_allowed(
    op: Operation,
    target: &AllowedTarget,
    bound: Option<(usize, Span)>,
    context: &Context<'_>,
    scope: &Scope,
) -> Result<PolicyExpr, ExprError> {
    let table = scope.current().table.as_str();
    let max_depth = bound.map(|(depth, _)| depth);
    let no_depth = || match bound {
        Some((_, span)) => Err(ExprError::new(span, REVERSE_MAX_DEPTH)),
        None => Ok(()),
    };
    match target {
        AllowedTarget::Via {
            table: None,
            column,
            span,
        } => {
            column_type(context, table, column, *span)?;
            Ok(PolicyExpr::Inherits {
                operation: op,
                via_column: column.clone(),
                max_depth,
            })
        }
        AllowedTarget::Via {
            table: Some(source),
            column,
            span,
        } => {
            known_table(context, source, *span)?;
            column_type(context, source, column, *span)?;
            no_depth()?;
            Ok(PolicyExpr::InheritsReferencing {
                operation: op,
                source_table: source.clone(),
                via_column: column.clone(),
                max_depth: None,
            })
        }
        AllowedTarget::Relation(relation, relation_span) => {
            let relations = context.relations.get(table);
            match relations.and_then(|r| r.get(relation)) {
                Some(CodegenRelation::Forward { column, .. }) => Ok(PolicyExpr::Inherits {
                    operation: op,
                    via_column: column.clone(),
                    max_depth,
                }),
                Some(CodegenRelation::Reverse {
                    table: source,
                    relation: forward,
                }) => {
                    let via = forward_column(context, source, forward).ok_or_else(|| {
                        ExprError::new(
                            *relation_span,
                            format!("relation `{relation}` leads back through `{source}.{forward}`, which is not a forward relation"),
                        )
                    })?;
                    no_depth()?;
                    Ok(PolicyExpr::InheritsReferencing {
                        operation: op,
                        source_table: source.clone(),
                        via_column: via,
                        max_depth: None,
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
    }
}

fn forward_column(context: &Context<'_>, table: &str, relation: &str) -> Option<String> {
    match context.relations.get(table)?.get(relation)? {
        CodegenRelation::Forward { column, .. } => Some(column.clone()),
        CodegenRelation::Reverse { .. } => None,
    }
}

/// What a column operand names, from where the compile is.
enum Ref {
    /// A column of the row the condition filters (type `None`: magic).
    Column(String, Option<ColumnType>),
    /// A column of the row enclosing the innermost `exists`: a value.
    Outer(String),
    /// Not a column.
    Other,
}

fn classify(operand: &Operand, context: &Context<'_>, scope: &Scope) -> Result<Ref, ExprError> {
    let Operand::Column { qual, name, span } = operand else {
        return Ok(Ref::Other);
    };
    let depth = scope.depth();
    let column = |level: usize| -> Result<Ref, ExprError> {
        let table = &scope.levels[level].table;
        let ty = column_type(context, table, name, *span)?.cloned();
        Ok(if level == depth {
            Ref::Column(name.clone(), ty)
        } else {
            Ref::Outer(name.clone())
        })
    };
    // In an `exists` inside another, the engine refuses an outer-row
    // reference (and the TS DSL never builds one).
    let nested = || {
        ExprError::new(
            *span,
            "a condition of an `exists` inside another `exists` names only its own row's columns",
        )
        .example("exists a(x = this.id, exists b(y = 1))")
    };
    match qual {
        Qual::None => column(depth),
        Qual::This | Qual::Outer if depth >= 2 => Err(nested()),
        Qual::This => column(0),
        Qual::Outer => {
            if depth == 0 {
                return Err(ExprError::new(
                    *span,
                    "`outer.` names the row enclosing an `exists`; outside `exists`, name the column alone",
                ));
            }
            column(0)
        }
        Qual::Named(qualifier) => {
            let found = scope
                .levels
                .iter()
                .rposition(|level| level.name() == qualifier);
            match found {
                Some(level) if level == depth || depth == 1 => column(level),
                Some(_) => Err(nested()),
                None => {
                    let names: Vec<&str> = scope.levels.iter().map(Level::name).collect();
                    Err(ExprError::new(
                        *span,
                        format!(
                            "`{qualifier}` names no table here{}",
                            did_you_mean(qualifier, names)
                        ),
                    ))
                }
            }
        }
    }
}

/// A literal compared with `session.<path>`. `session.authMode` is never
/// `"local-first"` since device accounts were renamed, so comparing with it is
/// refused rather than left to quietly never match.
fn session_literal(path: &[String], literal: &Operand) -> Result<Value, ExprError> {
    let value = literal_value(literal, None)?;
    match crate::model::public_api::policy::removed_auth_mode_literal(path, &value) {
        Some(message) => {
            Err(ExprError::new(literal.span(), message)
                .example("session.authMode = 'device-account'"))
        }
        None => Ok(value),
    }
}

fn outer_ref(column: String) -> PolicyValue {
    PolicyValue::SessionRef(vec![OUTER_ROW.to_string(), column])
}

fn compile_cmp(
    left: &Operand,
    op: CmpOp,
    right: &Operand,
    span: Span,
    context: &Context<'_>,
    scope: &Scope,
) -> Result<PolicyExpr, ExprError> {
    let (left, right) = (left.clone(), right.clone());
    if let Ref::Column(column, ty) = classify(&left, context, scope)? {
        return column_cmp(column, ty, op, &right, context, scope);
    }
    if let Ref::Column(column, ty) = classify(&right, context, scope)? {
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
                value: session_literal(path, other)?,
            }),
        };
    }
    Err(ExprError::new(
        span,
        "a comparison needs a column of the row, or a session claim, on its left",
    )
    .example("userId = session.user"))
}

fn column_cmp(
    column: String,
    ty: Option<ColumnType>,
    op: CmpOp,
    value: &Operand,
    context: &Context<'_>,
    scope: &Scope,
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
/// `exists`) a column of the enclosing row.
fn policy_value(
    operand: &Operand,
    ty: Option<&ColumnType>,
    context: &Context<'_>,
    scope: &Scope,
) -> Result<PolicyValue, ExprError> {
    match operand {
        Operand::Session(path, _) => Ok(PolicyValue::SessionRef(path.clone())),
        Operand::TableId(table, span) => {
            if !context.schema.contains_key(&TableName::new(table.clone())) {
                let names = context.schema.keys().map(TableName::as_str);
                return Err(ExprError::new(
                    *span,
                    format!(
                        "`table_id('{table}')` names no table{}",
                        did_you_mean(table, names)
                    ),
                ));
            }
            if !matches!(ty, Some(ColumnType::Uuid) | None) {
                return Err(ExprError::new(
                    *span,
                    "a table identity is a uuid: compare it with an `s.uuid()` column",
                ));
            }
            Ok(PolicyValue::TableIdentity {
                table: table.clone(),
            })
        }
        Operand::Column { span, .. } => match classify(operand, context, scope)? {
            Ref::Outer(column) => Ok(outer_ref(column)),
            _ => Err(ExprError::new(
                *span,
                "a condition compares a column with a value; two columns of the same row can't be compared",
            )
            .example("exists chats(id = this.chatId, isPublic = true)")),
        },
        other => Ok(PolicyValue::Literal(literal_value(other, ty)?)),
    }
}

/// A literal as the engine's [`Value`], typed by the column it is compared
/// with when there is one.
fn literal_value(operand: &Operand, ty: Option<&ColumnType>) -> Result<Value, ExprError> {
    let (literal, span) = match operand {
        Operand::Literal(literal, span) => (literal, *span),
        Operand::Typed(value, _) => return Ok(value.clone()),
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

// -- `exists t(…).rel(…)`: a hop along relations ------------------------------

/// Compile `exists t(…).rel(…)…` to the relation TS's `hopTo` builds: `t`
/// joined to each hop's table as `__hop_<i>`, filtered by every hop's
/// conditions, yielding the last hop's ids.
fn compile_hops(
    table: &str,
    alias: Option<&str>,
    conditions: &[Ast],
    hops: &[Hop],
    context: &Context<'_>,
    scope: &Scope,
) -> Result<PolicyExpr, ExprError> {
    let base_scope = alias.unwrap_or(table).to_string();
    let mut rel = RelationExpr::TableScan {
        table: table.to_string(),
        alias: alias.map(str::to_string),
    };
    let mut predicates = Vec::new();
    for c in conditions {
        predicates.push(relation_predicate(c, context, scope, table, &base_scope)?);
    }
    let (mut at_table, mut at_scope) = (table.to_string(), base_scope);
    for (i, hop) in hops.iter().enumerate() {
        let hop_scope = format!("__hop_{i}");
        let relation = context
            .relations
            .get(&at_table)
            .and_then(|r| r.get(&hop.relation));
        let (target, on) = match relation {
            Some(CodegenRelation::Forward {
                table: target,
                column,
            }) => (
                target.clone(),
                RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some(at_scope.clone()),
                        column: column.clone(),
                    },
                    right: RelationColumnRef {
                        scope: Some(hop_scope.clone()),
                        column: "id".into(),
                    },
                },
            ),
            Some(CodegenRelation::Reverse {
                table: source,
                relation: forward,
            }) => {
                let column = forward_column(context, source, forward).ok_or_else(|| {
                    ExprError::new(hop.span, format!("relation `{}` leads back through `{source}.{forward}`, which is not a forward relation", hop.relation))
                })?;
                (
                    source.clone(),
                    RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some(at_scope.clone()),
                            column: "id".into(),
                        },
                        right: RelationColumnRef {
                            scope: Some(hop_scope.clone()),
                            column,
                        },
                    },
                )
            }
            None => {
                let names: Vec<&str> = context
                    .relations
                    .get(&at_table)
                    .map(|r| r.keys().map(String::as_str).collect())
                    .unwrap_or_default();
                return Err(ExprError::new(
                    hop.span,
                    format!(
                        "unknown relation `{}` on {at_table}{}",
                        hop.relation,
                        did_you_mean(&hop.relation, names)
                    ),
                ));
            }
        };
        rel = RelationExpr::Join {
            left: Box::new(rel),
            right: Box::new(RelationExpr::TableScan {
                table: target.clone(),
                alias: Some(hop_scope.clone()),
            }),
            on: vec![on],
            join_kind: RelationJoinKind::Inner,
        };
        for c in &hop.conditions {
            predicates.push(relation_predicate(c, context, scope, &target, &hop_scope)?);
        }
        (at_table, at_scope) = (target, hop_scope);
    }
    if !predicates.is_empty() {
        rel = RelationExpr::Filter {
            input: Box::new(rel),
            predicate: if predicates.len() == 1 {
                predicates.pop().expect("one")
            } else {
                RelationPredicate::And(predicates)
            },
        };
    }
    Ok(PolicyExpr::ExistsRel {
        rel: RelationExpr::Project {
            input: Box::new(rel),
            columns: vec![RelationProjectColumn {
                alias: "id".into(),
                expr: RelationProjectExpr::Column(RelationColumnRef {
                    scope: Some(at_scope),
                    column: "id".into(),
                }),
            }],
        },
    })
}

/// A hop condition as a relation predicate: bare columns are `table`'s (as
/// `rel_scope`), `this.`/`outer.` the row the relation is correlated with.
fn relation_predicate(
    ast: &Ast,
    context: &Context<'_>,
    scope: &Scope,
    table: &str,
    rel_scope: &str,
) -> Result<RelationPredicate, ExprError> {
    let again = |a: &Ast| relation_predicate(a, context, scope, table, rel_scope);
    let column = |operand: &Operand| -> Result<Option<RelationColumnRef>, ExprError> {
        match operand {
            Operand::Column {
                qual: Qual::None,
                name,
                span,
            } => {
                column_type(context, table, name, *span)?;
                Ok(Some(RelationColumnRef {
                    scope: Some(rel_scope.to_string()),
                    column: name.clone(),
                }))
            }
            _ => Ok(None),
        }
    };
    let value = |operand: &Operand| -> Result<RelationValueRef, ExprError> {
        Ok(match operand {
            Operand::Column { qual, name, span } => {
                // The relation's outer row: the checked row at the top, else
                // the row around the innermost `exists` (as `this.` there).
                let depth = scope.depth();
                let outer = depth.saturating_sub(1);
                let ok = match qual {
                    Qual::This => outer == 0,
                    Qual::Outer => depth == 0,
                    Qual::Named(n) => outer == 0 && scope.levels[0].name() == n,
                    Qual::None => false,
                };
                if !ok {
                    return Err(ExprError::new(
                        *span,
                        "a hop's conditions compare its columns with values and with `this.` columns",
                    )
                    .example("exists files(id = this.fileId).folder(ownerId = session.user)"));
                }
                column_type(context, &scope.levels[outer].table, name, *span)?;
                RelationValueRef::OuterColumn(RelationColumnRef {
                    scope: None,
                    column: name.clone(),
                })
            }
            Operand::Session(path, _) => RelationValueRef::SessionRef(path.clone()),
            Operand::TableId(_, span) => {
                return Err(ExprError::new(
                    *span,
                    "`table_id(…)` is not supported in a hop's conditions yet: compare it in \
                     an `exists` condition",
                )
                .example("exists members(scopeId = table_id('projects'), identifier = this.id)"));
            }
            Operand::Literal(Literal::Str(s), _) => {
                RelationValueRef::Literal(serde_json::Value::String(s.clone()))
            }
            Operand::Literal(Literal::Bool(b), _) => {
                RelationValueRef::Literal(serde_json::Value::Bool(*b))
            }
            Operand::Literal(Literal::Num(n), span) => RelationValueRef::Literal(
                serde_json::from_str(n).map_err(|_| ExprError::new(*span, "not a number"))?,
            ),
            Operand::Typed(v, _) => {
                RelationValueRef::Literal(serde_json::to_value(v).expect("values serialize"))
            }
            Operand::Null(span) => {
                return Err(ExprError::new(*span, "compare with null by `col = null`"));
            }
        })
    };
    Ok(match ast {
        Ast::Bool(true) => RelationPredicate::True,
        Ast::Bool(false) => RelationPredicate::False,
        Ast::Or(items) => RelationPredicate::Or(items.iter().map(again).collect::<Result<_, _>>()?),
        Ast::And(items) => {
            RelationPredicate::And(items.iter().map(again).collect::<Result<_, _>>()?)
        }
        Ast::Not(inner) => RelationPredicate::Not(Box::new(again(inner)?)),
        Ast::Cmp {
            left,
            op,
            right,
            span,
        } => {
            let (left, right) = (left.clone(), right.clone());
            let (col, op, other) = if let Some(c) = column(&left)? {
                (c, op.clone(), right)
            } else if let Some(c) = column(&right)? {
                let flipped = match op {
                    CmpOp::Lt => CmpOp::Gt,
                    CmpOp::Le => CmpOp::Ge,
                    CmpOp::Gt => CmpOp::Lt,
                    CmpOp::Ge => CmpOp::Le,
                    same => same.clone(),
                };
                (c, flipped, left)
            } else {
                return Err(ExprError::new(
                    *span,
                    "a hop's condition needs a column of its table on one side",
                ));
            };
            if let Operand::Null(span) = other {
                return match op {
                    CmpOp::Eq => Ok(RelationPredicate::IsNull { column: col }),
                    CmpOp::Ne => Ok(RelationPredicate::IsNotNull { column: col }),
                    _ => Err(ExprError::new(span, "only `=` and `!=` compare with null")),
                };
            }
            RelationPredicate::Cmp {
                left: col,
                op: relation_op(&op),
                right: value(&other)?,
            }
        }
        Ast::InList { left, values, span } => {
            let Some(col) = column(left)? else {
                return Err(ExprError::new(
                    *span,
                    "`in` tests a column of the hop's table",
                ));
            };
            RelationPredicate::In {
                left: col,
                values: values.iter().map(&value).collect::<Result<_, _>>()?,
            }
        }
        Ast::Contains { left, right, span } => {
            let Some(col) = column(left)? else {
                return Err(ExprError::new(
                    *span,
                    "`contains` tests a column of the hop's table",
                ));
            };
            RelationPredicate::Contains {
                left: col,
                right: value(right)?,
            }
        }
        Ast::InSession { span, .. }
        | Ast::Exists {
            table_span: span, ..
        } => {
            return Err(ExprError::new(
                *span,
                "a hop's conditions are comparisons; write this one as `exists (from …)`",
            ));
        }
        Ast::Allowed { op, span, .. } => {
            return Err(ExprError::new(*span, allowed_in_exists(*op)));
        }
        Ast::ExistsRel(_) => {
            return Err(ExprError::new(
                (0, 0),
                "a hop's conditions are comparisons; write this one as `exists (from …)`",
            ));
        }
    })
}

fn relation_op(op: &CmpOp) -> RelationCmpOp {
    match op {
        CmpOp::Eq => RelationCmpOp::Eq,
        CmpOp::Ne => RelationCmpOp::Ne,
        CmpOp::Lt => RelationCmpOp::Lt,
        CmpOp::Le => RelationCmpOp::Le,
        CmpOp::Gt => RelationCmpOp::Gt,
        CmpOp::Ge => RelationCmpOp::Ge,
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

/// The wording of everything the app files can't express (yet).
pub const UNSUPPORTED: &str = "is not supported in jazz app files yet";

/// Why something can't be written in an app file.
pub fn unsupported(what: &str) -> String {
    format!("{what} {UNSUPPORTED}")
}

/// Print `expr`, a policy on `table`, as the text that reads back as exactly
/// `expr`: the shortest spelling when it does, else the explicit one (typed
/// literals, `literal(null)`, `exists (from …)`, `via`), which always does.
pub fn print(expr: &PolicyExpr, context: &Context<'_>, table: &str) -> Result<String, String> {
    if too_deep(expr, 0) {
        return Err(unsupported(&format!(
            "a policy nested more than {MAX_DEPTH} levels deep"
        )));
    }
    if let Some(refusal) = outer_row_refusal(expr, 0) {
        return Err(refusal.into());
    }
    if let Some(node) = derived_only(expr) {
        return Err(unsupported(&format!(
            "`{node}` in `jazz.permissions.json` (Jazz derives it for its system tables)"
        )));
    }
    let mut last = String::new();
    // Shortest first (hops as `exists t(…).rel(…)`), then without hops, then
    // explicit.
    for (explicit, hops) in [(false, true), (false, false), (true, false)] {
        let printer = Printer {
            context,
            explicit,
            hops,
        };
        let text = printer.policy(expr, &mut vec![table.to_string()], 0);
        let back = parse(&text).and_then(|ast| compile(&ast, context, table));
        if back.as_ref() == Ok(expr) {
            return Ok(text);
        }
        last = match back {
            Ok(other) => format!("`{text}` reads back as {other:?}"),
            Err(e) => format!("`{text}` doesn't read back:{}", e.render(&text)),
        };
    }
    Err(format!(
        "the policy {expr:?} has no exact spelling ({last})"
    ))
}

/// The engine refuses an outer-row reference inside an `exists` within
/// another; so does the writer, in the engine's words.
pub const NESTED_OUTER_ROW: &str = "not supported in policies yet: an `exists` inside another `exists` can't refer to the checked row's columns";
/// And one outside any `exists`.
pub const OUTSIDE_OUTER_ROW: &str = "outer row reference outside EXISTS: only a condition of `exists` can refer to the checked row's columns";

/// A node only Jazz's derived policies hold, which the permissions file
/// cannot spell.
fn derived_only(expr: &PolicyExpr) -> Option<&'static str> {
    match expr {
        PolicyExpr::InheritsFrom { .. } => Some("InheritsFrom"),
        PolicyExpr::InsertedByThisTransaction => Some("InsertedByThisTransaction"),
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().find_map(derived_only),
        PolicyExpr::Not(inner) => derived_only(inner),
        PolicyExpr::Exists { condition, .. } => derived_only(condition),
        _ => None,
    }
}

fn outer_row_refusal(expr: &PolicyExpr, depth: usize) -> Option<&'static str> {
    let refuse = |v: &PolicyValue| match v {
        PolicyValue::SessionRef(path)
            if path.first().map(String::as_str) == Some(OUTER_ROW) && depth != 1 =>
        {
            Some(if depth == 0 {
                OUTSIDE_OUTER_ROW
            } else {
                NESTED_OUTER_ROW
            })
        }
        _ => None,
    };
    match expr {
        PolicyExpr::Cmp { value, .. } | PolicyExpr::Contains { value, .. } => refuse(value),
        PolicyExpr::InList { values, .. } => values.iter().find_map(refuse),
        PolicyExpr::Exists { condition, .. } => outer_row_refusal(condition, depth + 1),
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            items.iter().find_map(|i| outer_row_refusal(i, depth))
        }
        PolicyExpr::Not(inner) => outer_row_refusal(inner, depth),
        _ => None,
    }
}

/// Whether `expr` nests deeper than the grammar reads, checked before
/// anything recurses through it all (the walk stops at the limit).
pub fn too_deep(expr: &PolicyExpr, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return true;
    }
    let value = |v: &PolicyValue| match v {
        PolicyValue::Literal(v) => value_too_deep(v, depth + 1),
        PolicyValue::SessionRef(_) | PolicyValue::TableIdentity { .. } => false,
    };
    match expr {
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            items.iter().any(|i| too_deep(i, depth + 1))
        }
        PolicyExpr::Not(inner) => too_deep(inner, depth + 1),
        PolicyExpr::Exists { condition, .. } => too_deep(condition, depth + 1),
        PolicyExpr::ExistsRel { rel } => relation_too_deep(rel, depth + 1),
        PolicyExpr::Cmp { value: v, .. } | PolicyExpr::Contains { value: v, .. } => value(v),
        PolicyExpr::InList { values, .. } => values.iter().any(value),
        PolicyExpr::SessionCmp { value: v, .. } | PolicyExpr::SessionContains { value: v, .. } => {
            value_too_deep(v, depth + 1)
        }
        PolicyExpr::SessionInList { values, .. } => {
            values.iter().any(|v| value_too_deep(v, depth + 1))
        }
        _ => false,
    }
}

fn value_too_deep(value: &Value, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return true;
    }
    match value {
        Value::Array(values) | Value::Row { values, .. } => {
            values.iter().any(|v| value_too_deep(v, depth + 1))
        }
        _ => false,
    }
}

fn json_too_deep(value: &serde_json::Value, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return true;
    }
    match value {
        serde_json::Value::Array(items) => items.iter().any(|v| json_too_deep(v, depth + 1)),
        serde_json::Value::Object(entries) => entries.values().any(|v| json_too_deep(v, depth + 1)),
        _ => false,
    }
}

fn relation_too_deep(rel: &RelationExpr, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return true;
    }
    let next = depth + 1;
    match rel {
        RelationExpr::TableScan { .. } => false,
        RelationExpr::Filter { input, predicate } => {
            relation_too_deep(input, next) || predicate_too_deep(predicate, next)
        }
        RelationExpr::Union { inputs } => inputs.iter().any(|a| relation_too_deep(&a.input, next)),
        RelationExpr::Join { left, right, .. } => {
            relation_too_deep(left, next) || relation_too_deep(right, next)
        }
        RelationExpr::Gather { seed, step, .. } => {
            relation_too_deep(seed, next) || relation_too_deep(step, next)
        }
        RelationExpr::Project { input, .. }
        | RelationExpr::Distinct { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => relation_too_deep(input, next),
    }
}

fn predicate_too_deep(p: &RelationPredicate, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return true;
    }
    let next = depth + 1;
    let value = |v: &RelationValueRef| match v {
        RelationValueRef::Literal(json) => json_too_deep(json, next),
        _ => false,
    };
    match p {
        RelationPredicate::And(items) | RelationPredicate::Or(items) => {
            items.iter().any(|i| predicate_too_deep(i, next))
        }
        RelationPredicate::Not(inner) => predicate_too_deep(inner, next),
        RelationPredicate::Removed(removed) => match *removed {},
        RelationPredicate::Cmp { right, .. } | RelationPredicate::Contains { right, .. } => {
            value(right)
        }
        RelationPredicate::In { values, .. } => values.iter().any(value),
        _ => false,
    }
}

struct Printer<'a, 'c> {
    context: &'a Context<'c>,
    explicit: bool,
    /// Print relations of the shape `exists t(…).rel(…)` compiles to (TS's
    /// `hopTo`) in that form.
    hops: bool,
}

/// Precedence: `or`'s items 2, `and`'s 3, `not`'s 3 (`not not x` reads fine).
fn wrap(text: String, mine: u8, precedence: u8) -> String {
    if mine < precedence {
        format!("({text})")
    } else {
        text
    }
}

fn column_text(column: &str) -> String {
    if column.starts_with('$') {
        column.to_string()
    } else {
        name_text(column)
    }
}

fn session_text(path: &[String]) -> String {
    let parts: Vec<String> = path.iter().map(|p| name_text(p)).collect();
    format!("session.{}", parts.join("."))
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

fn quoted(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn double_text(d: f64) -> String {
    if d.is_finite() {
        let text = d.to_string();
        if text.contains('.') {
            text
        } else {
            format!("{text}.0")
        }
    } else {
        quoted(&d.to_string())
    }
}

/// A value as a typed literal: `int(5)`, `text('a')`; `null` bare (inside a
/// typed literal).
fn typed_text(value: &Value) -> String {
    let list = |values: &[Value]| values.iter().map(typed_text).collect::<Vec<_>>();
    match value {
        Value::Integer(n) => format!("int({n})"),
        Value::BigInt(n) => format!("bigint({n})"),
        Value::Double(d) => format!("double({})", double_text(*d)),
        Value::Boolean(b) => format!("bool({b})"),
        Value::Text(s) => format!("text({})", quoted(s)),
        Value::Timestamp(n) => format!("timestamp({n})"),
        Value::Uuid(id) => format!("uuid('{id}')"),
        Value::TransactionId(bytes) => format!("txid('{}')", hex::encode(bytes)),
        Value::Bytea(bytes) => format!("bytes('{}')", hex::encode(bytes)),
        Value::Array(items) => format!("array({})", list(items).join(", ")),
        Value::Row { id, values } => {
            let mut parts = vec![match id {
                Some(id) => format!("uuid('{id}')"),
                None => "null".into(),
            }];
            parts.extend(list(values));
            format!("row({})", parts.join(", "))
        }
        Value::Null => "null".into(),
    }
}

impl Printer<'_, '_> {
    /// A literal: plain where the column's type reads it back, typed when
    /// explicit (or when there is no plain spelling).
    fn literal(&self, value: &Value) -> String {
        if !self.explicit {
            match value {
                Value::Text(s) => return quoted(s),
                Value::Uuid(id) => return format!("'{id}'"),
                Value::Boolean(b) => return b.to_string(),
                Value::Integer(n) => return n.to_string(),
                Value::BigInt(n) => return n.to_string(),
                Value::Timestamp(n) => return n.to_string(),
                Value::Double(d) if d.is_finite() => return double_text(*d),
                _ => {}
            }
        }
        match value {
            Value::Null => "literal(null)".into(),
            other => typed_text(other),
        }
    }

    fn value(&self, value: &PolicyValue, levels: &[String]) -> String {
        match value {
            PolicyValue::Literal(v) => self.literal(v),
            PolicyValue::SessionRef(path)
                if path.len() == 2 && path[0] == OUTER_ROW && levels.len() == 2 =>
            {
                format!("this.{}", column_text(&path[1]))
            }
            PolicyValue::SessionRef(path) => session_text(path),
            PolicyValue::TableIdentity { table } => format!("table_id({})", quoted(table)),
        }
    }

    fn all(&self, items: &[PolicyExpr], levels: &mut Vec<String>, precedence: u8) -> Vec<String> {
        items
            .iter()
            .map(|i| self.policy(i, levels, precedence))
            .collect()
    }

    fn policy(&self, expr: &PolicyExpr, levels: &mut Vec<String>, precedence: u8) -> String {
        let here = levels.last().expect("a level").clone();
        match expr {
            PolicyExpr::True => "true".into(),
            PolicyExpr::False => "false".into(),
            PolicyExpr::Or(items) if items.len() >= 2 => {
                wrap(self.all(items, levels, 2).join(" or "), 1, precedence)
            }
            PolicyExpr::And(items) if items.len() >= 2 => {
                wrap(self.all(items, levels, 3).join(" and "), 2, precedence)
            }
            PolicyExpr::Or(items) => format!("anyOf({})", self.all(items, levels, 0).join(", ")),
            PolicyExpr::And(items) => format!("allOf({})", self.all(items, levels, 0).join(", ")),
            PolicyExpr::Not(inner) => wrap(
                format!("not {}", self.policy(inner, levels, 3)),
                3,
                precedence,
            ),
            PolicyExpr::Cmp { column, op, value } => format!(
                "{} {} {}",
                column_text(column),
                cmp_text(op),
                self.value(value, levels)
            ),
            PolicyExpr::SessionCmp { path, op, value } => format!(
                "{} {} {}",
                session_text(path),
                cmp_text(op),
                self.literal(value)
            ),
            PolicyExpr::IsNull { column } => format!("{} = null", column_text(column)),
            PolicyExpr::IsNotNull { column } => format!("{} != null", column_text(column)),
            PolicyExpr::SessionIsNull { path } => format!("{} = null", session_text(path)),
            PolicyExpr::SessionIsNotNull { path } => format!("{} != null", session_text(path)),
            PolicyExpr::Contains { column, value } => format!(
                "{} contains {}",
                column_text(column),
                self.value(value, levels)
            ),
            PolicyExpr::SessionContains { path, value } => {
                format!("{} contains {}", session_text(path), self.literal(value))
            }
            PolicyExpr::In {
                column,
                session_path,
            } => format!("{} in {}", column_text(column), session_text(session_path)),
            PolicyExpr::InList { column, values } => format!(
                "{} in [{}]",
                column_text(column),
                values
                    .iter()
                    .map(|v| self.value(v, levels))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            PolicyExpr::SessionInList { path, values } => format!(
                "{} in [{}]",
                session_text(path),
                values
                    .iter()
                    .map(|v| self.literal(v))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            PolicyExpr::Exists { table, condition } => {
                levels.push(table.clone());
                let conditions = match condition.as_ref() {
                    PolicyExpr::True => Vec::new(),
                    PolicyExpr::And(items) if items.len() >= 2 => self.all(items, levels, 0),
                    other => vec![self.policy(other, levels, 0)],
                };
                levels.pop();
                format!("exists {}({})", name_text(table), conditions.join(", "))
            }
            PolicyExpr::ExistsRel { rel } => self
                .hops
                .then(|| self.hop_text(rel, levels))
                .flatten()
                .unwrap_or_else(|| format!("exists ({})", query_text(rel))),
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => {
                let relation = (!self.explicit)
                    .then(|| {
                        self.context.relations.get(&here)?.iter().find_map(|(name, rel)| {
                            matches!(rel, CodegenRelation::Forward { column, .. } if column == via_column)
                                .then(|| name_text(name))
                        })
                    })
                    .flatten()
                    .unwrap_or_else(|| format!("via {}", column_text(via_column)));
                allowed_text(*operation, &relation, *max_depth)
            }
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => {
                let relation = (!self.explicit)
                    .then(|| {
                        self.context.relations.get(&here)?.iter().find_map(
                            |(name, rel)| match rel {
                                CodegenRelation::Reverse { table, relation }
                                    if table == source_table
                                        && forward_column(self.context, table, relation)
                                            .as_ref()
                                            == Some(via_column) =>
                                {
                                    Some(name_text(name))
                                }
                                _ => None,
                            },
                        )
                    })
                    .flatten()
                    .unwrap_or_else(|| {
                        format!(
                            "via {}.{}",
                            name_text(source_table),
                            column_text(via_column)
                        )
                    });
                allowed_text(*operation, &relation, *max_depth)
            }
            // No spelling: Jazz derives these for its system tables, and
            // `print` refuses them first (see `derived_only`).
            PolicyExpr::InheritsFrom { .. } => "inheritsFrom(…)".into(),
            PolicyExpr::InsertedByThisTransaction => "insertedByThisTransaction".into(),
        }
    }
}

/// `scope.column` when `c` is a column of `scope`.
fn column_in<'r>(c: &'r RelationColumnRef, scope: &str) -> Option<&'r str> {
    (c.scope.as_deref() == Some(scope)).then_some(c.column.as_str())
}

impl Printer<'_, '_> {
    /// `rel` as `exists t(…).rel(…)…` when it is exactly what that compiles
    /// to: `t` joined along relations to `__hop_<i>`, filtered by each
    /// level's conditions in order, yielding the last hop's ids.
    fn hop_text(&self, rel: &RelationExpr, levels: &[String]) -> Option<String> {
        let RelationExpr::Project { input, columns } = rel else {
            return None;
        };
        let [column] = columns.as_slice() else {
            return None;
        };
        let (input, predicates) = match input.as_ref() {
            RelationExpr::Filter { input, predicate } => (
                input.as_ref(),
                match predicate {
                    RelationPredicate::And(items) if items.len() >= 2 => items.iter().collect(),
                    RelationPredicate::And(_) => return None,
                    other => vec![other],
                },
            ),
            other => (other, Vec::new()),
        };
        // Peel the joins: the last is the outermost.
        let mut joins = Vec::new();
        let mut at = input;
        while let RelationExpr::Join {
            left,
            right,
            on,
            join_kind: RelationJoinKind::Inner,
        } = at
        {
            let RelationExpr::TableScan {
                table,
                alias: Some(alias),
            } = right.as_ref()
            else {
                return None;
            };
            let [on] = on.as_slice() else {
                return None;
            };
            joins.push((table, alias, on));
            at = left;
        }
        joins.reverse();
        let RelationExpr::TableScan { table, alias } = at else {
            return None;
        };
        if joins.is_empty() {
            return None;
        }
        let base_scope = alias.as_deref().unwrap_or(table);
        // Each hop: the relation that joins the previous level to it.
        let mut scopes = vec![(table.clone(), base_scope.to_string())];
        let mut names = Vec::new();
        for (i, (target, hop_alias, on)) in joins.iter().enumerate() {
            let hop_scope = format!("__hop_{i}");
            if **hop_alias != hop_scope {
                return None;
            }
            let (at_table, at_scope) = scopes.last().expect("the base").clone();
            let name =
                self.context
                    .relations
                    .get(&at_table)?
                    .iter()
                    .find_map(|(name, relation)| {
                        let (left, right) = match relation {
                            CodegenRelation::Forward { table: t, column } if t == *target => {
                                (column.clone(), "id".to_string())
                            }
                            CodegenRelation::Reverse { table: t, relation } if t == *target => {
                                ("id".to_string(), forward_column(self.context, t, relation)?)
                            }
                            _ => return None,
                        };
                        (column_in(&on.left, &at_scope) == Some(left.as_str())
                            && column_in(&on.right, &hop_scope) == Some(right.as_str()))
                        .then(|| name.clone())
                    })?;
            names.push(name);
            scopes.push(((*target).clone(), hop_scope));
        }
        let RelationProjectExpr::Column(out) = &column.expr else {
            return None;
        };
        if column.alias != "id" || column_in(out, &scopes.last()?.1) != Some("id") {
            return None;
        }
        // Conditions by level, in level order.
        let mut conditions: Vec<Vec<String>> = vec![Vec::new(); scopes.len()];
        let mut level = 0;
        for predicate in predicates {
            let (at, text) =
                scopes
                    .iter()
                    .enumerate()
                    .skip(level)
                    .find_map(|(i, (_, scope))| {
                        Some((i, self.hop_condition(predicate, scope, levels, 0)?))
                    })?;
            level = at;
            conditions[at].push(text);
        }
        let mut text = format!("exists {}", name_text(table));
        if let Some(alias) = alias {
            text.push_str(&format!(" as {}", name_text(alias)));
        }
        text.push_str(&format!("({})", conditions[0].join(", ")));
        for (name, conditions) in names.iter().zip(&conditions[1..]) {
            text.push_str(&format!(".{}({})", name_text(name), conditions.join(", ")));
        }
        Some(text)
    }

    /// A relation predicate whose columns are all `scope`'s, as a hop's
    /// condition.
    fn hop_condition(
        &self,
        p: &RelationPredicate,
        scope: &str,
        levels: &[String],
        precedence: u8,
    ) -> Option<String> {
        let all = |items: &[RelationPredicate], precedence: u8| {
            items
                .iter()
                .map(|i| self.hop_condition(i, scope, levels, precedence))
                .collect::<Option<Vec<_>>>()
        };
        let column = |c: &RelationColumnRef| column_in(c, scope).map(column_text);
        let value = |v: &RelationValueRef| -> Option<String> {
            Some(match v {
                RelationValueRef::OuterColumn(RelationColumnRef {
                    scope: None,
                    column,
                }) if levels.len() <= 2 => format!("this.{}", column_text(column)),
                RelationValueRef::SessionRef(path) => session_text(path),
                RelationValueRef::Literal(json) => match json {
                    serde_json::Value::String(s) => quoted(s),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(_) => match value_ref_text(v) {
                        text if !text.starts_with("json(") => text,
                        _ => return None,
                    },
                    serde_json::Value::Null => return None,
                    other => match serde_json::from_value::<Value>(other.clone()) {
                        Ok(Value::Null) | Err(_) => return None,
                        Ok(value) => typed_text(&value),
                    },
                },
                _ => return None,
            })
        };
        Some(match p {
            RelationPredicate::True => "true".into(),
            RelationPredicate::False => "false".into(),
            RelationPredicate::Or(items) if items.len() >= 2 => {
                wrap(all(items, 2)?.join(" or "), 1, precedence)
            }
            RelationPredicate::And(items) if items.len() >= 2 => {
                wrap(all(items, 3)?.join(" and "), 2, precedence)
            }
            RelationPredicate::Or(items) => format!("anyOf({})", all(items, 0)?.join(", ")),
            RelationPredicate::And(items) => format!("allOf({})", all(items, 0)?.join(", ")),
            RelationPredicate::Not(inner) => wrap(
                format!("not {}", self.hop_condition(inner, scope, levels, 3)?),
                3,
                precedence,
            ),
            RelationPredicate::Cmp { left, op, right } => {
                let op = match op {
                    RelationCmpOp::Eq => "=",
                    RelationCmpOp::Ne => "!=",
                    RelationCmpOp::Lt => "<",
                    RelationCmpOp::Le => "<=",
                    RelationCmpOp::Gt => ">",
                    RelationCmpOp::Ge => ">=",
                };
                format!("{} {op} {}", column(left)?, value(right)?)
            }
            RelationPredicate::IsNull { column: c } => format!("{} = null", column(c)?),
            RelationPredicate::IsNotNull { column: c } => format!("{} != null", column(c)?),
            RelationPredicate::In { left, values } => format!(
                "{} in [{}]",
                column(left)?,
                values
                    .iter()
                    .map(value)
                    .collect::<Option<Vec<_>>>()?
                    .join(", ")
            ),
            RelationPredicate::Contains { left, right } => {
                format!("{} contains {}", column(left)?, value(right)?)
            }
            // The policy language has no prefix/suffix test, so no policy
            // holds one; there is no text to give back.
            RelationPredicate::StartsWith { .. } | RelationPredicate::EndsWith { .. } => {
                return None;
            }
            RelationPredicate::Removed(removed) => match *removed {},
        })
    }
}

fn allowed_text(operation: Operation, target: &str, max_depth: Option<usize>) -> String {
    let op = match operation {
        Operation::Select => "read",
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    };
    match max_depth {
        Some(depth) => format!("allowedTo.{op}({target}, maxDepth: {depth})"),
        None => format!("allowedTo.{op}({target})"),
    }
}

// -- Relations ---------------------------------------------------------------

fn colref_text(c: &RelationColumnRef) -> String {
    match &c.scope {
        Some(scope) => format!("{}.{}", name_text(scope), column_text(&c.column)),
        None => column_text(&c.column),
    }
}

fn row_id_text(r: &RelationRowIdRef) -> &'static str {
    match r {
        RelationRowIdRef::Current => "id(row)",
        RelationRowIdRef::Outer => "id(outer)",
        RelationRowIdRef::Frontier => "id(frontier)",
    }
}

fn key_text(k: &RelationKeyRef) -> String {
    match k {
        RelationKeyRef::Column(c) => colref_text(c),
        RelationKeyRef::RowId(r) => row_id_text(r).into(),
    }
}

fn keys_text(keys: &[RelationKeyRef]) -> String {
    format!(
        "({})",
        keys.iter().map(key_text).collect::<Vec<_>>().join(", ")
    )
}

/// `from …`, the relation's pipeline read from its innermost source out.
pub fn query_text(rel: &RelationExpr) -> String {
    let mut clauses = Vec::new();
    let mut at = rel;
    loop {
        match at {
            RelationExpr::Filter { input, predicate } => {
                clauses.push(format!("where {}", predicate_text(predicate, 0)));
                at = input;
            }
            RelationExpr::Project { input, columns } => {
                let columns: Vec<String> = columns
                    .iter()
                    .map(|c| {
                        let expr = match &c.expr {
                            RelationProjectExpr::Column(col) => colref_text(col),
                            RelationProjectExpr::RowId(r) => row_id_text(r).into(),
                        };
                        format!("{expr} as {}", name_text(&c.alias))
                    })
                    .collect();
                clauses.push(if columns.is_empty() {
                    "select ()".into()
                } else {
                    format!("select {}", columns.join(", "))
                });
                at = input;
            }
            RelationExpr::Distinct { input, key } => {
                clauses.push(format!("distinct on {}", keys_text(key)));
                at = input;
            }
            RelationExpr::OrderBy { input, terms } => {
                let terms: Vec<String> = terms
                    .iter()
                    .map(|t| match t.direction {
                        OrderDirection::Asc => colref_text(&t.column),
                        OrderDirection::Desc => format!("{} desc", colref_text(&t.column)),
                    })
                    .collect();
                clauses.push(if terms.is_empty() {
                    "order by ()".into()
                } else {
                    format!("order by {}", terms.join(", "))
                });
                at = input;
            }
            RelationExpr::Limit { input, limit } => {
                clauses.push(format!("limit {limit}"));
                at = input;
            }
            RelationExpr::Offset { input, offset } => {
                clauses.push(format!("offset {offset}"));
                at = input;
            }
            RelationExpr::Join {
                left,
                right,
                on,
                join_kind,
            } => {
                let kind = match join_kind {
                    RelationJoinKind::Inner => "join",
                    RelationJoinKind::Left => "left join",
                };
                let mut clause = format!("{kind} {}", source_text(right));
                if !on.is_empty() {
                    let on: Vec<String> = on
                        .iter()
                        .map(|c| format!("{} = {}", colref_text(&c.left), colref_text(&c.right)))
                        .collect();
                    clause.push_str(&format!(" on {}", on.join(" and ")));
                }
                clauses.push(clause);
                at = left;
            }
            source => {
                clauses.push(format!("from {}", source_text(source)));
                break;
            }
        }
    }
    clauses.reverse();
    clauses.join(" ")
}

/// What `from`/`join` read: a table, a union, a recursive relation, or a
/// parenthesised query.
fn source_text(rel: &RelationExpr) -> String {
    match rel {
        RelationExpr::TableScan { table, alias } => match alias {
            Some(alias) => format!("{} as {}", name_text(table), name_text(alias)),
            None => name_text(table),
        },
        RelationExpr::Union { inputs } => format!(
            "union({})",
            inputs
                .iter()
                .map(|arm| format!("{}: {}", name_text(&arm.label), query_text(&arm.input)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RelationExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => format!(
            "recursive(seed: {}, step: {}, frontier: {}, dedupe: {}, maxDepth: {})",
            query_text(seed),
            query_text(step),
            key_text(frontier_key),
            keys_text(dedupe_key),
            match bound {
                RecursionBound::Fixpoint => "fixpoint".to_string(),
                RecursionBound::MaxDepth(n) => n.to_string(),
            }
        ),
        other => format!("({})", query_text(other)),
    }
}

fn predicate_text(p: &RelationPredicate, precedence: u8) -> String {
    let all = |items: &[RelationPredicate], precedence: u8| {
        items
            .iter()
            .map(|i| predicate_text(i, precedence))
            .collect::<Vec<_>>()
    };
    match p {
        RelationPredicate::True => "true".into(),
        RelationPredicate::False => "false".into(),
        RelationPredicate::Or(items) if items.len() >= 2 => {
            wrap(all(items, 2).join(" or "), 1, precedence)
        }
        RelationPredicate::And(items) if items.len() >= 2 => {
            wrap(all(items, 3).join(" and "), 2, precedence)
        }
        RelationPredicate::Or(items) => format!("anyOf({})", all(items, 0).join(", ")),
        RelationPredicate::And(items) => format!("allOf({})", all(items, 0).join(", ")),
        RelationPredicate::Not(inner) => {
            wrap(format!("not {}", predicate_text(inner, 3)), 3, precedence)
        }
        RelationPredicate::Cmp { left, op, right } => {
            let op = match op {
                RelationCmpOp::Eq => "=",
                RelationCmpOp::Ne => "!=",
                RelationCmpOp::Lt => "<",
                RelationCmpOp::Le => "<=",
                RelationCmpOp::Gt => ">",
                RelationCmpOp::Ge => ">=",
            };
            format!("{} {op} {}", colref_text(left), value_ref_text(right))
        }
        RelationPredicate::IsNull { column } => format!("{} = null", colref_text(column)),
        RelationPredicate::IsNotNull { column } => format!("{} != null", colref_text(column)),
        RelationPredicate::In { left, values } => format!(
            "{} in [{}]",
            colref_text(left),
            values
                .iter()
                .map(value_ref_text)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RelationPredicate::Contains { left, right } => {
            format!("{} contains {}", colref_text(left), value_ref_text(right))
        }
        // Not policy syntax (policies cannot hold these); a readable form for diagnostics.
        RelationPredicate::StartsWith { left, right } => {
            format!("{} startsWith {}", colref_text(left), value_ref_text(right))
        }
        RelationPredicate::EndsWith { left, right } => {
            format!("{} endsWith {}", colref_text(left), value_ref_text(right))
        }
        RelationPredicate::Removed(removed) => match *removed {},
    }
}

fn value_ref_text(v: &RelationValueRef) -> String {
    match v {
        RelationValueRef::Literal(json) => match json {
            serde_json::Value::String(s) => quoted(s),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Number(n) => {
                let text = n.to_string();
                let plain = text.strip_prefix('-').unwrap_or(&text);
                let mut parts = plain.splitn(2, '.');
                let digits = |s: Option<&str>| {
                    s.is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                };
                let whole = digits(parts.next());
                let fraction = parts.next();
                if whole && (fraction.is_none() || digits(fraction)) {
                    text
                } else {
                    format!("json({})", quoted(&text))
                }
            }
            // An engine value's JSON reads best as its typed literal.
            other => match serde_json::from_value::<Value>(other.clone()) {
                Ok(Value::Null) => "literal(null)".into(),
                Ok(value) if serde_json::to_value(&value).ok().as_ref() == Some(other) => {
                    typed_text(&value)
                }
                _ => format!("json({})", quoted(&other.to_string())),
            },
        },
        RelationValueRef::Param(name) => format!("param.{}", name_text(name)),
        RelationValueRef::SessionRef(path) => session_text(path),
        RelationValueRef::OuterColumn(c) => format!("outer.{}", colref_text(c)),
        RelationValueRef::FrontierColumn(c) => format!("frontier.{}", colref_text(c)),
        RelationValueRef::RowId(r) => row_id_text(r).into(),
    }
}
