/**
 * A reader for the `jazz.permissions.json` expression grammar (SPEC §10.10),
 * ported from the Rust reader (`crates/jazz/src/tools/deployment/artifact/expr.rs`)
 * so the JSON → TS printer works on what the file says, names and all.
 *
 * It only parses: names are resolved, and the file checked, by Rust `load`.
 */

export type CmpOp = "=" | "!=" | "<" | "<=" | ">" | ">=";
export type Operation = "read" | "insert" | "update" | "delete";

export type Qual =
  | { kind: "none" }
  | { kind: "this" }
  | { kind: "outer" }
  | { kind: "named"; name: string };

/** A typed literal (`int(5)`, `text('a')`, `literal(null)`). */
export type Typed =
  | { kind: "literal"; value: string | number | boolean | null; raw?: string }
  | { kind: "int" | "bigint" | "timestamp" | "double"; value: string }
  | { kind: "text" | "uuid" | "bytes" | "txid"; value: string }
  | { kind: "bool"; value: boolean }
  | { kind: "null" }
  | { kind: "array"; items: Typed[] }
  | { kind: "row"; id: string | null; values: Typed[] };

export type Operand =
  | { kind: "column"; qual: Qual; name: string }
  | { kind: "string"; value: string }
  | { kind: "number"; raw: string }
  | { kind: "bool"; value: boolean }
  | { kind: "typed"; value: Typed }
  | { kind: "null" }
  | { kind: "session"; path: string[] }
  | { kind: "param"; name: string };

export interface Hop {
  relation: string;
  conditions: Ast[];
}

export type Ast =
  | { kind: "or"; items: Ast[] }
  | { kind: "and"; items: Ast[] }
  | { kind: "not"; inner: Ast }
  | { kind: "bool"; value: boolean }
  | { kind: "cmp"; left: Operand; op: CmpOp; right: Operand }
  | { kind: "inList"; left: Operand; values: Operand[] }
  | { kind: "inSession"; left: Operand; path: string[] }
  | { kind: "contains"; left: Operand; right: Operand }
  | { kind: "exists"; table: string; alias?: string; conditions: Ast[]; hops: Hop[] }
  | { kind: "existsRel"; relation: Rel }
  | {
      kind: "allowed";
      op: Operation;
      target: { kind: "relation"; name: string } | { kind: "via"; table?: string; column: string };
      maxDepth?: number;
    }
  | { kind: "call"; name: string; args: Operand[] };

// The relation IR, inside `exists ( … )`.
export interface ColRef {
  scope?: string;
  column: string;
}
export type RowId = "row" | "outer" | "frontier";
export type Key = { kind: "column"; column: ColRef } | { kind: "rowId"; row: RowId };
export type ValueRef =
  | { kind: "literal"; value: unknown; text: string }
  | { kind: "session"; path: string[] }
  | { kind: "param"; name: string }
  | { kind: "outer" | "frontier"; column: ColRef }
  | { kind: "rowId"; row: RowId };
export type Pred =
  | { kind: "true" }
  | { kind: "false" }
  | { kind: "or"; items: Pred[] }
  | { kind: "and"; items: Pred[] }
  | { kind: "not"; inner: Pred }
  | { kind: "cmp"; left: ColRef; op: CmpOp; right: ValueRef }
  | { kind: "isNull" | "isNotNull"; column: ColRef }
  | { kind: "in"; left: ColRef; values: ValueRef[] }
  | { kind: "contains"; left: ColRef; right: ValueRef };
export type Rel =
  | { kind: "scan"; table: string; alias?: string }
  | {
      kind: "join";
      left: Rel;
      right: Rel;
      on: { left: ColRef; right: ColRef }[];
      joinKind: "inner" | "left";
    }
  | { kind: "filter"; input: Rel; predicate: Pred }
  | { kind: "project"; input: Rel; columns: { alias: string; expr: Key }[] }
  | { kind: "distinct"; input: Rel; key: Key[] }
  | { kind: "orderBy"; input: Rel; terms: { column: ColRef; direction: "asc" | "desc" }[] }
  | { kind: "limit"; input: Rel; limit: number }
  | { kind: "offset"; input: Rel; offset: number }
  | { kind: "union"; inputs: { label: string; input: Rel }[] }
  | {
      kind: "gather";
      seed: Rel;
      step: Rel;
      frontierKey: Key;
      dedupeKey: Key[];
      maxDepth: number | "fixpoint";
    };

export class ExprSyntaxError extends Error {
  constructor(
    message: string,
    readonly source: string,
    readonly at: number,
  ) {
    super(`${message} at ${at} in \`${source}\``);
  }
}

type Tok =
  | { t: "ident"; v: string }
  | { t: "qident"; v: string }
  | { t: "dollar"; v: string }
  | { t: "str"; v: string }
  | { t: "num"; v: string }
  | { t: "op"; v: string }
  | { t: "(" | ")" | "[" | "]" | "," | "." | ":" };

interface Token {
  tok: Tok;
  start: number;
}

const TYPED = new Set([
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
]);

const MAGIC_COLUMNS = [
  "$createdBy",
  "$updatedBy",
  "$deletedBy",
  "$createdAt",
  "$updatedAt",
  "$deletedAt",
];

export const KEYWORDS = new Set([
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
]);

function lex(source: string): Token[] {
  const out: Token[] = [];
  let at = 0;
  const isDigit = (c: string | undefined) => c !== undefined && c >= "0" && c <= "9";
  const isAlpha = (c: string | undefined) => c !== undefined && /[A-Za-z_]/.test(c);
  const isAlnum = (c: string | undefined) => c !== undefined && /[A-Za-z0-9_]/.test(c);
  const fail = (msg: string, where: number) => {
    throw new ExprSyntaxError(msg, source, where);
  };
  while (at < source.length) {
    const start = at;
    const c = source[at]!;
    if (c === " " || c === "\t" || c === "\n" || c === "\r") {
      at++;
      continue;
    }
    if ("()[],.:".includes(c)) {
      out.push({ tok: { t: c as "(" }, start });
      at++;
      continue;
    }
    if (c === "=") {
      if (source[at + 1] === "=") fail("comparisons use `=`, not `==`", at);
      out.push({ tok: { t: "op", v: "=" }, start });
      at++;
      continue;
    }
    if (c === "!") {
      if (source[at + 1] !== "=") fail("negation is `not`, not `!`", at);
      out.push({ tok: { t: "op", v: "!=" }, start });
      at += 2;
      continue;
    }
    if (c === "<" || c === ">") {
      const eq = source[at + 1] === "=";
      at += eq ? 2 : 1;
      out.push({ tok: { t: "op", v: c + (eq ? "=" : "") }, start });
      continue;
    }
    if (c === "'" || c === "`") {
      at++;
      let text = "";
      for (;;) {
        if (at >= source.length) fail("unterminated quote", start);
        if (source[at] === c && source[at + 1] === c) {
          text += c;
          at += 2;
        } else if (source[at] === c) {
          at++;
          break;
        } else {
          text += source[at];
          at++;
        }
      }
      out.push({ tok: c === "'" ? { t: "str", v: text } : { t: "qident", v: text }, start });
      continue;
    }
    if (c === "-" || isDigit(c)) {
      at++;
      while (at < source.length && (isDigit(source[at]) || source[at] === ".")) {
        if (source[at] === "." && !isDigit(source[at + 1])) break;
        at++;
      }
      if (source.slice(start, at) === "-") fail("a number needs digits", start);
      out.push({ tok: { t: "num", v: source.slice(start, at) }, start });
      continue;
    }
    if (c === "$") {
      at++;
      while (isAlnum(source[at])) at++;
      if (at === start + 1) fail("`$` must start a name", start);
      out.push({ tok: { t: "dollar", v: source.slice(start + 1, at) }, start });
      continue;
    }
    if (isAlpha(c)) {
      at++;
      while (isAlnum(source[at])) at++;
      out.push({ tok: { t: "ident", v: source.slice(start, at) }, start });
      continue;
    }
    fail(`unexpected \`${c}\``, at);
  }
  return out;
}

class Parser {
  at = 0;
  constructor(
    readonly tokens: Token[],
    readonly source: string,
  ) {}

  fail(msg: string): never {
    throw new ExprSyntaxError(msg, this.source, this.tokens[this.at]?.start ?? this.source.length);
  }
  peek(offset = 0): Tok | undefined {
    return this.tokens[this.at + offset]?.tok;
  }
  next(): Tok | undefined {
    return this.tokens[this.at++]?.tok;
  }
  isTok(t: string, offset = 0): boolean {
    return this.peek(offset)?.t === t;
  }
  isWord(word: string, offset = 0): boolean {
    const tok = this.peek(offset);
    return tok?.t === "ident" && tok.v === word;
  }
  isCallOf(word: string): boolean {
    return this.isWord(word) && this.isTok("(", 1);
  }
  expect(t: string, what: string): void {
    if (!this.isTok(t)) this.fail(`expected ${what}`);
    this.at++;
  }
  expectWord(word: string): void {
    if (!this.isWord(word)) this.fail(`expected \`${word}\``);
    this.at++;
  }
  name(what: string): string {
    const tok = this.peek();
    if (tok?.t === "ident" || tok?.t === "qident") {
      this.at++;
      return tok.v;
    }
    return this.fail(`expected ${what}`);
  }
  listContinues(): boolean {
    return this.isTok(",") && !(this.peek(1)?.t === "ident" && this.isTok(":", 2));
  }

  expr(): Ast {
    const terms = [this.conjunction()];
    while (this.isWord("or")) {
      this.at++;
      terms.push(this.conjunction());
    }
    return terms.length === 1 ? terms[0]! : { kind: "or", items: terms };
  }
  conjunction(): Ast {
    const terms = [this.negation()];
    while (this.isWord("and")) {
      this.at++;
      terms.push(this.negation());
    }
    return terms.length === 1 ? terms[0]! : { kind: "and", items: terms };
  }
  negation(): Ast {
    if (this.isWord("not")) {
      this.at++;
      return { kind: "not", inner: this.negation() };
    }
    return this.atom();
  }
  exprList(): Ast[] {
    this.expect("(", "`(`");
    const items: Ast[] = [];
    if (this.isTok(")")) {
      this.at++;
      return items;
    }
    for (;;) {
      items.push(this.expr());
      const tok = this.next();
      if (tok?.t === ",") continue;
      if (tok?.t === ")") return items;
      this.at--;
      this.fail("expected `,` or `)`");
    }
  }
  atom(): Ast {
    const tok = this.peek();
    if (!tok) this.fail("the expression ends too early");
    if (tok.t === "(") {
      this.at++;
      const inner = this.expr();
      this.expect(")", "`)`");
      return inner;
    }
    if (tok.t === "ident") {
      const word = tok.v;
      if (word === "true" || word === "false") {
        if (this.peek(1)?.t === "op") return this.comparison();
        this.at++;
        return { kind: "bool", value: word === "true" };
      }
      if ((word === "anyOf" || word === "allOf") && this.isCallOf(word)) {
        this.at++;
        const items = this.exprList();
        return { kind: word === "anyOf" ? "or" : "and", items };
      }
      if (word === "exists") return this.exists();
      if (word === "allowedTo" && this.isTok(".", 1)) return this.allowed();
      if (
        !["this", "session", "null", "outer"].includes(word) &&
        !TYPED.has(word) &&
        this.isTok("(", 1)
      ) {
        return this.call();
      }
    }
    return this.comparison();
  }
  exists(): Ast {
    this.at++;
    if (this.isTok("(")) {
      this.at++;
      const relation = this.query();
      this.expect(")", "`)` after the relation");
      return { kind: "existsRel", relation };
    }
    const table = this.name("a table after `exists`");
    let alias: string | undefined;
    if (this.isWord("as")) {
      this.at++;
      alias = this.name("an alias after `as`");
    }
    const conditions = this.exprList();
    const hops: Hop[] = [];
    while (this.isTok(".")) {
      this.at++;
      const relation = this.name("a relation name after `.`");
      hops.push({ relation, conditions: this.exprList() });
    }
    return { kind: "exists", table, ...(alias ? { alias } : {}), conditions, hops };
  }
  allowed(): Ast {
    this.at += 2;
    const opTok = this.next();
    if (opTok?.t !== "ident" || !["read", "insert", "update", "delete"].includes(opTok.v)) {
      this.at--;
      this.fail("expected `read`, `insert`, `update` or `delete`");
    }
    const op = opTok.v as Operation;
    this.expect("(", "`(` and a relation");
    let target: Extract<Ast, { kind: "allowed" }>["target"];
    if (this.isWord("via")) {
      this.at++;
      const first = this.name("a column after `via`");
      if (this.isTok(".")) {
        this.at++;
        target = { kind: "via", table: first, column: this.name("a column after `.`") };
      } else {
        target = { kind: "via", column: first };
      }
    } else {
      target = { kind: "relation", name: this.name("a relation name") };
    }
    let maxDepth: number | undefined;
    if (this.isTok(",")) {
      this.at++;
      this.expectWord("maxDepth");
      this.expect(":", "`:`");
      const n = this.next();
      if (n?.t !== "num") this.fail("expected `maxDepth: <number>`");
      maxDepth = Number(n.v);
    }
    this.expect(")", "`)`");
    return { kind: "allowed", op, target, ...(maxDepth !== undefined ? { maxDepth } : {}) };
  }
  call(): Ast {
    const name = (this.next() as { v: string }).v;
    this.at++;
    const args: Operand[] = [];
    if (this.isTok(")")) {
      this.at++;
    } else {
      for (;;) {
        args.push(this.operand());
        const tok = this.next();
        if (tok?.t === ",") continue;
        if (tok?.t === ")") break;
        this.at--;
        this.fail("expected `,` or `)`");
      }
    }
    return { kind: "call", name, args };
  }
  comparison(): Ast {
    const left = this.operand();
    if (this.isWord("in")) {
      this.at++;
      if (this.isTok("[")) {
        this.at++;
        const values: Operand[] = [];
        if (!this.isTok("]")) {
          for (;;) {
            values.push(this.operand());
            if (this.isTok(",")) this.at++;
            else break;
          }
        }
        this.expect("]", "`]`");
        return { kind: "inList", left, values };
      }
      const right = this.operand();
      if (right.kind !== "session") this.fail("`in` takes a list or a session claim");
      return { kind: "inSession", left, path: right.path };
    }
    if (this.isWord("contains")) {
      this.at++;
      return { kind: "contains", left, right: this.operand() };
    }
    const op = this.cmpOp();
    return { kind: "cmp", left, op, right: this.operand() };
  }
  cmpOp(): CmpOp {
    const tok = this.peek();
    if (tok?.t !== "op") this.fail("expected a comparison");
    this.at++;
    return tok.v as CmpOp;
  }
  columnName(): string {
    const tok = this.next();
    if (tok?.t === "ident" || tok?.t === "qident") return tok.v;
    if (tok?.t === "dollar") return this.dotted(`$${tok.v}`);
    this.at--;
    return this.fail("expected a column");
  }
  dotted(name: string): string {
    if (name.startsWith("$") && this.isTok(".") && this.peek(1)?.t === "ident") {
      this.at++;
      return `${name}.${(this.next() as { v: string }).v}`;
    }
    return name;
  }
  sessionPath(): string[] {
    this.expect(".", "`.user` or `.claims.<name>` after `session`");
    const path = [this.name("a name")];
    while (this.isTok(".")) {
      this.at++;
      path.push(this.name("a name"));
    }
    return path;
  }
  operand(): Operand {
    const tok = this.peek();
    if (!tok) this.fail("expected a column or a value");
    if (tok.t === "ident" && TYPED.has(tok.v) && tok.v !== "json" && this.isTok("(", 1)) {
      return { kind: "typed", value: this.typedLiteral() };
    }
    this.at++;
    switch (tok.t) {
      case "str":
        return { kind: "string", value: tok.v };
      case "num":
        return { kind: "number", raw: tok.v };
      case "dollar": {
        const column = `$${tok.v}`;
        if (MAGIC_COLUMNS.some((m) => m.startsWith(column))) {
          return { kind: "column", qual: { kind: "none" }, name: this.dotted(column) };
        }
        return { kind: "param", name: tok.v };
      }
      case "ident":
      case "qident": {
        const word = tok.v;
        if (tok.t === "ident") {
          if (word === "null") return { kind: "null" };
          if (word === "true" || word === "false") return { kind: "bool", value: word === "true" };
          if (word === "session" && this.isTok("."))
            return { kind: "session", path: this.sessionPath() };
          if ((word === "this" || word === "outer") && this.isTok(".")) {
            this.at++;
            return { kind: "column", qual: { kind: word }, name: this.columnName() };
          }
        }
        const after = this.peek(1)?.t;
        if (this.isTok(".") && (after === "ident" || after === "qident" || after === "dollar")) {
          this.at++;
          return { kind: "column", qual: { kind: "named", name: word }, name: this.columnName() };
        }
        return { kind: "column", qual: { kind: "none" }, name: word };
      }
      default:
        this.at--;
        return this.fail("expected a column or a value");
    }
  }
  stringArg(): string {
    const tok = this.next();
    if (tok?.t !== "str") this.fail("expected a string `'…'`");
    return tok.v;
  }
  numberArg(): string {
    const tok = this.next();
    if (tok?.t !== "num" && tok?.t !== "str") this.fail("expected a number");
    return tok.v;
  }
  typedLiteral(): Typed {
    const kind = this.name("a literal");
    this.expect("(", "`(`");
    let value: Typed;
    switch (kind) {
      case "literal": {
        const tok = this.next();
        if (tok?.t === "str") value = { kind: "literal", value: tok.v };
        else if (tok?.t === "num") value = { kind: "literal", value: Number(tok.v), raw: tok.v };
        else if (tok?.t === "ident" && (tok.v === "true" || tok.v === "false"))
          value = { kind: "literal", value: tok.v === "true" };
        else if (tok?.t === "ident" && tok.v === "null") value = { kind: "literal", value: null };
        else this.fail("`literal(…)` takes a literal");
        break;
      }
      case "int":
      case "bigint":
      case "timestamp":
      case "double":
        value = { kind, value: this.numberArg() };
        break;
      case "text":
      case "uuid":
      case "bytes":
      case "txid":
        value = { kind, value: this.stringArg() };
        break;
      case "bool": {
        const tok = this.next();
        if (tok?.t !== "ident" || (tok.v !== "true" && tok.v !== "false"))
          this.fail("expected a bool");
        value = { kind: "bool", value: tok.v === "true" };
        break;
      }
      case "array": {
        const items: Typed[] = [];
        if (!this.isTok(")")) {
          for (;;) {
            items.push(this.typedArg());
            if (this.isTok(",")) this.at++;
            else break;
          }
        }
        value = { kind: "array", items };
        break;
      }
      case "row": {
        const id = this.typedArg();
        if (id.kind !== "null" && id.kind !== "uuid") this.fail("`row(…)` takes its id first");
        const values: Typed[] = [];
        while (this.isTok(",")) {
          this.at++;
          values.push(this.typedArg());
        }
        value = { kind: "row", id: id.kind === "uuid" ? id.value : null, values };
        break;
      }
      default:
        return this.fail(`unknown literal \`${kind}(…)\``);
    }
    this.expect(")", "`)`");
    return value;
  }
  typedArg(): Typed {
    if (this.isWord("null")) {
      this.at++;
      return { kind: "null" };
    }
    const tok = this.peek();
    if (tok?.t === "ident" && TYPED.has(tok.v)) return this.typedLiteral();
    return this.fail("inside a typed literal, every value is typed too");
  }

  // The relation IR.
  query(): Rel {
    if (!this.isWord("from")) this.fail("a relation starts with `from`");
    this.at++;
    let rel = this.relSource();
    for (;;) {
      if (this.isWord("left") || this.isWord("join")) {
        let joinKind: "inner" | "left" = "inner";
        if (this.isWord("left")) {
          this.at++;
          joinKind = "left";
        }
        this.expectWord("join");
        const right = this.relSource();
        const on: { left: ColRef; right: ColRef }[] = [];
        if (this.isWord("on")) {
          this.at++;
          for (;;) {
            const left = this.colref();
            const op = this.next();
            if (op?.t !== "op" || op.v !== "=") this.fail("join conditions are `a.col = b.col`");
            on.push({ left, right: this.colref() });
            if (this.isWord("and")) this.at++;
            else break;
          }
        }
        rel = { kind: "join", left: rel, right, on, joinKind };
      } else if (this.isWord("where")) {
        this.at++;
        rel = { kind: "filter", input: rel, predicate: this.predicate() };
      } else if (this.isWord("select")) {
        this.at++;
        const columns: { alias: string; expr: Key }[] = [];
        if (this.isTok("(") && this.isTok(")", 1)) {
          this.at += 2;
        } else {
          for (;;) {
            const expr = this.key();
            this.expectWord("as");
            columns.push({ alias: this.name("a name after `as`"), expr });
            if (this.listContinues()) this.at++;
            else break;
          }
        }
        rel = { kind: "project", input: rel, columns };
      } else if (this.isWord("distinct")) {
        this.at++;
        this.expectWord("on");
        rel = { kind: "distinct", input: rel, key: this.keyList() };
      } else if (this.isWord("order")) {
        this.at++;
        this.expectWord("by");
        const terms: { column: ColRef; direction: "asc" | "desc" }[] = [];
        if (this.isTok("(") && this.isTok(")", 1)) {
          this.at += 2;
        } else {
          for (;;) {
            const column = this.colref();
            let direction: "asc" | "desc" = "asc";
            if (this.isWord("desc")) {
              this.at++;
              direction = "desc";
            } else if (this.isWord("asc")) {
              this.at++;
            }
            terms.push({ column, direction });
            if (this.listContinues()) this.at++;
            else break;
          }
        }
        rel = { kind: "orderBy", input: rel, terms };
      } else if (this.isWord("limit") || this.isWord("offset")) {
        const limit = this.isWord("limit");
        this.at++;
        const n = this.next();
        if (n?.t !== "num") this.fail("expected a whole number");
        rel = limit
          ? { kind: "limit", input: rel, limit: Number(n.v) }
          : { kind: "offset", input: rel, offset: Number(n.v) };
      } else {
        return rel;
      }
    }
  }
  relSource(): Rel {
    if (this.isTok("(")) {
      this.at++;
      const rel = this.query();
      this.expect(")", "`)`");
      return rel;
    }
    if (this.isCallOf("union")) {
      this.at += 2;
      const inputs: { label: string; input: Rel }[] = [];
      if (!this.isTok(")")) {
        for (;;) {
          const label = this.name("a label");
          this.expect(":", "`:`");
          inputs.push({ label, input: this.query() });
          if (this.isTok(",")) this.at++;
          else break;
        }
      }
      this.expect(")", "`)`");
      return { kind: "union", inputs };
    }
    if (this.isCallOf("recursive")) {
      this.at += 2;
      const field = (name: string) => {
        this.expectWord(name);
        this.expect(":", "`:`");
      };
      field("seed");
      const seed = this.query();
      this.expect(",", "`,`");
      field("step");
      const step = this.query();
      this.expect(",", "`,`");
      field("frontier");
      const frontierKey = this.key();
      this.expect(",", "`,`");
      field("dedupe");
      const dedupeKey = this.keyList();
      this.expect(",", "`,`");
      field("maxDepth");
      let maxDepth: number | "fixpoint";
      if (this.isWord("fixpoint")) {
        this.at++;
        maxDepth = "fixpoint";
      } else {
        const n = this.next();
        if (n?.t !== "num") this.fail("expected a number or `fixpoint`");
        maxDepth = Number(n.v);
      }
      this.expect(")", "`)`");
      return { kind: "gather", seed, step, frontierKey, dedupeKey, maxDepth };
    }
    const table = this.name("a table");
    if (this.isWord("as")) {
      this.at++;
      return { kind: "scan", table, alias: this.name("an alias after `as`") };
    }
    return { kind: "scan", table };
  }
  colref(): ColRef {
    const first = this.name("a column");
    const after = this.peek(1)?.t;
    if (this.isTok(".") && (after === "ident" || after === "qident")) {
      this.at++;
      return { scope: first, column: this.name("a column") };
    }
    return { column: first };
  }
  rowId(): RowId {
    this.at += 2;
    const which = this.name("`row`, `outer` or `frontier`");
    if (which !== "row" && which !== "outer" && which !== "frontier") {
      this.fail("`id(…)` takes `row`, `outer` or `frontier`");
    }
    this.expect(")", "`)`");
    return which;
  }
  key(): Key {
    if (this.isCallOf("id")) return { kind: "rowId", row: this.rowId() };
    return { kind: "column", column: this.colref() };
  }
  keyList(): Key[] {
    this.expect("(", "`(`");
    const keys: Key[] = [];
    if (!this.isTok(")")) {
      for (;;) {
        keys.push(this.key());
        if (this.isTok(",")) this.at++;
        else break;
      }
    }
    this.expect(")", "`)`");
    return keys;
  }
  predicate(): Pred {
    const terms = [this.pAnd()];
    while (this.isWord("or")) {
      this.at++;
      terms.push(this.pAnd());
    }
    return terms.length === 1 ? terms[0]! : { kind: "or", items: terms };
  }
  pAnd(): Pred {
    const terms = [this.pNot()];
    while (this.isWord("and")) {
      this.at++;
      terms.push(this.pNot());
    }
    return terms.length === 1 ? terms[0]! : { kind: "and", items: terms };
  }
  pNot(): Pred {
    if (this.isWord("not")) {
      this.at++;
      return { kind: "not", inner: this.pNot() };
    }
    return this.pAtom();
  }
  pAtom(): Pred {
    if (this.isTok("(")) {
      this.at++;
      const inner = this.predicate();
      this.expect(")", "`)`");
      return inner;
    }
    if (this.isWord("true")) {
      this.at++;
      return { kind: "true" };
    }
    if (this.isWord("false")) {
      this.at++;
      return { kind: "false" };
    }
    if (this.isCallOf("anyOf") || this.isCallOf("allOf")) {
      const any = this.isWord("anyOf");
      this.at += 2;
      const items: Pred[] = [];
      if (!this.isTok(")")) {
        for (;;) {
          items.push(this.predicate());
          if (this.isTok(",")) this.at++;
          else break;
        }
      }
      this.expect(")", "`)`");
      return { kind: any ? "or" : "and", items };
    }
    const left = this.colref();
    if (this.isWord("in")) {
      this.at++;
      this.expect("[", "`[`");
      const values: ValueRef[] = [];
      if (!this.isTok("]")) {
        for (;;) {
          values.push(this.valueRef());
          if (this.isTok(",")) this.at++;
          else break;
        }
      }
      this.expect("]", "`]`");
      return { kind: "in", left, values };
    }
    if (this.isWord("contains")) {
      this.at++;
      return { kind: "contains", left, right: this.valueRef() };
    }
    const op = this.cmpOp();
    if (this.isWord("null") && (op === "=" || op === "!=")) {
      this.at++;
      return { kind: op === "=" ? "isNull" : "isNotNull", column: left };
    }
    return { kind: "cmp", left, op, right: this.valueRef() };
  }
  valueRef(): ValueRef {
    const tok = this.peek();
    if (!tok) this.fail("expected a value");
    const start = this.tokens[this.at]!.start;
    if (tok.t === "ident") {
      const word = tok.v;
      if (word === "id" && this.isTok("(", 1)) return { kind: "rowId", row: this.rowId() };
      if (word === "json" && this.isTok("(", 1)) {
        this.at += 2;
        const text = this.stringArg();
        this.expect(")", "`)`");
        return { kind: "literal", value: JSON.parse(text), text: this.textFrom(start) };
      }
      if (TYPED.has(word) && this.isTok("(", 1)) {
        const typed = this.typedLiteral();
        return { kind: "literal", value: typed, text: this.textFrom(start) };
      }
      if (word === "true" || word === "false") {
        this.at++;
        return { kind: "literal", value: word === "true", text: word };
      }
      if (word === "session" && this.isTok(".", 1)) {
        this.at++;
        return { kind: "session", path: this.sessionPath() };
      }
      if (word === "param" && this.isTok(".", 1)) {
        this.at += 2;
        return { kind: "param", name: this.name("a parameter name") };
      }
      if ((word === "outer" || word === "frontier") && this.isTok(".", 1)) {
        this.at += 2;
        const first = this.name("a column");
        const column: ColRef = this.isTok(".")
          ? (this.at++, { scope: first, column: this.name("a column") })
          : { column: first };
        return { kind: word, column };
      }
    }
    this.at++;
    if (tok.t === "str") return { kind: "literal", value: tok.v, text: this.textFrom(start) };
    if (tok.t === "num") return { kind: "literal", value: Number(tok.v), text: tok.v };
    this.at--;
    return this.fail("expected a value");
  }
  textFrom(start: number): string {
    const end = this.tokens[this.at]?.start ?? this.source.length;
    return this.source.slice(start, end).trim();
  }
}

/** The tokens of an expression, with their source spans (for text-level rewrites). */
export function tokens(
  source: string,
): { kind: Tok["t"]; value?: string; start: number; end: number }[] {
  const toks = lex(source);
  return toks.map((t, i) => {
    let end = i + 1 < toks.length ? toks[i + 1]!.start : source.length;
    while (end > t.start && /\s/.test(source[end - 1]!)) end--;
    return { kind: t.tok.t, value: "v" in t.tok ? t.tok.v : undefined, start: t.start, end };
  });
}

/** Parse an expression string of `jazz.permissions.json`. */
export function parseExpr(source: string): Ast {
  const tokens = lex(source);
  if (tokens.length === 0) throw new ExprSyntaxError("an empty expression", source, 0);
  const parser = new Parser(tokens, source);
  const ast = parser.expr();
  if (parser.peek()) parser.fail("unexpected text after the expression");
  return ast;
}
