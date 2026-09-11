import { describeTable, listTables } from "./schema.js";
import type { ColumnDescriptor, WasmSchema } from "../drivers/types.js";
import type { Db, QueryBuilder, TableProxy } from "../runtime/db.js";
import { TypedTableQueryBuilder, type TableMeta } from "../typed-app.js";
import {
  getSupportedWhereOperatorsForSchemaColumn,
  type WhereOperator,
} from "../where-operators.js";

type Literal =
  | { kind: "string" | "number"; text: string }
  | { kind: "boolean"; value: boolean }
  | { kind: "null" };
type Predicate = { column: string; op: WhereOperator; value: Literal | boolean };
type Selection = {
  columns: string[] | "*";
  where: Predicate[];
  order: [string, "asc" | "desc"][];
  limit?: number;
  offset?: number;
};
export type Statement =
  | { kind: "show-tables" }
  | { kind: "describe"; table: string }
  | ({ kind: "select"; table: string } & Selection)
  | { kind: "insert"; table: string; values: [string, Literal][] }
  | { kind: "update"; table: string; values: [string, Literal][]; id: Literal }
  | { kind: "delete"; table: string; id: Literal };
type Token = {
  kind: "word" | "identifier" | "string" | "number" | "symbol";
  text: string;
  at: number;
};

// A deliberately bounded grammar. No SQL is forwarded to the engine, and no
// unsupported clause is silently dropped. Numeric spelling survives lexing so
// BIGINT literals never pass through an imprecise JavaScript Number.
function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let at = 0;
  while (at < sql.length) {
    const rest = sql.slice(at);
    const whitespace = /^(?:\s+|--[^\n]*(?:\n|$))/.exec(rest);
    if (whitespace) {
      at += whitespace[0].length;
      continue;
    }
    if (rest.startsWith("/*")) {
      const end = sql.indexOf("*/", at + 2);
      if (end < 0) throw new Error(`SQL at ${at + 1}: unterminated comment`);
      at = end + 2;
      continue;
    }
    const quote = sql[at];
    if (quote === "'" || quote === '"') {
      const start = at++;
      let value = "";
      let closed = false;
      while (at < sql.length) {
        const char = sql[at++]!;
        if (char === quote) {
          if (sql[at] === quote) {
            value += quote;
            at++;
          } else {
            closed = true;
            break;
          }
        } else value += char;
      }
      if (!closed) throw new Error(`SQL at ${start + 1}: unterminated quoted value`);
      tokens.push({ kind: quote === "'" ? "string" : "identifier", text: value, at: start });
      continue;
    }
    const number = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/.exec(rest);
    const word = /^[A-Za-z_][A-Za-z_0-9]*/.exec(rest);
    const symbol = /^(?:<=|>=|<>|!=|[=<>*,();])/.exec(rest);
    const match = number ?? word ?? symbol;
    if (!match)
      throw new Error(`SQL at ${at + 1}: unsupported character ${JSON.stringify(sql[at])}`);
    tokens.push({ kind: number ? "number" : word ? "word" : "symbol", text: match[0], at });
    at += match[0].length;
  }
  return tokens;
}

class Parser {
  private index = 0;
  constructor(private tokens: Token[]) {}
  private fail(expected: string): never {
    const token = this.tokens[this.index];
    throw new Error(`SQL ${token ? `at ${token.at + 1}` : "at end"}: expected ${expected}`);
  }
  private take(text: string): boolean {
    const token = this.tokens[this.index];
    if (
      token &&
      (token.kind === "word" || token.kind === "symbol") &&
      token.text.toUpperCase() === text
    ) {
      this.index++;
      return true;
    }
    return false;
  }
  private expect(text: string): void {
    if (!this.take(text)) this.fail(text);
  }
  private identifier(): string {
    const token = this.tokens[this.index];
    if (!token || (token.kind !== "word" && token.kind !== "identifier") || !token.text)
      this.fail("table or column name");
    this.index++;
    return token.text;
  }
  private literal(): Literal {
    if (this.take("NULL")) return { kind: "null" };
    if (this.take("TRUE")) return { kind: "boolean", value: true };
    if (this.take("FALSE")) return { kind: "boolean", value: false };
    const token = this.tokens[this.index];
    if (!token || (token.kind !== "number" && token.kind !== "string"))
      this.fail("string, number, TRUE, FALSE, or NULL literal");
    this.index++;
    return { kind: token.kind, text: token.text };
  }
  private list<T>(item: () => T): T[] {
    const values = [item()];
    while (this.take(",")) values.push(item());
    return values;
  }
  private count(): number {
    const value = this.literal();
    if (
      value.kind !== "number" ||
      !/^\d+$/.test(value.text) ||
      !Number.isSafeInteger(Number(value.text))
    )
      this.fail("nonnegative safe integer for LIMIT/OFFSET");
    return Number(value.text);
  }
  private predicate(): Predicate {
    const column = this.identifier();
    if (this.take("IS")) {
      const not = this.take("NOT");
      this.expect("NULL");
      return { column, op: "isNull", value: !not };
    }
    const operators: [string, WhereOperator][] = [
      ["=", "eq"],
      ["!=", "ne"],
      ["<>", "ne"],
      ["<", "lt"],
      ["<=", "lte"],
      [">", "gt"],
      [">=", "gte"],
    ];
    const op = operators.find(([token]) => this.take(token))?.[1];
    if (!op) this.fail("comparison operator or IS [NOT] NULL");
    const value = this.literal();
    if (value.kind === "null")
      throw new Error("Use IS NULL or IS NOT NULL instead of comparing with NULL");
    return { column, op, value };
  }
  private rowId(): Literal {
    this.expect("WHERE");
    if (this.identifier() !== "id") this.fail("WHERE id = 'uuid' for UPDATE/DELETE");
    this.expect("=");
    return this.literal();
  }
  parse(): Statement {
    let statement: Statement;
    if (this.take("SHOW")) {
      this.expect("TABLES");
      statement = { kind: "show-tables" };
    } else if (this.take("DESCRIBE")) {
      statement = { kind: "describe", table: this.identifier() };
    } else if (this.take("SELECT")) {
      const columns = this.take("*") ? "*" : this.list(() => this.identifier());
      this.expect("FROM");
      const table = this.identifier();
      const where: Predicate[] = [];
      if (this.take("WHERE")) {
        where.push(this.predicate());
        while (this.take("AND")) where.push(this.predicate());
      }
      let order: Selection["order"] = [];
      if (this.take("ORDER")) {
        this.expect("BY");
        order = this.list(() => {
          const column = this.identifier();
          const direction = this.take("DESC") ? "desc" : (this.take("ASC"), "asc");
          return [column, direction];
        });
      }
      const limit = this.take("LIMIT") ? this.count() : undefined;
      const offset = this.take("OFFSET") ? this.count() : undefined;
      statement = { kind: "select", table, columns, where, order, limit, offset };
    } else if (this.take("INSERT")) {
      this.expect("INTO");
      const table = this.identifier();
      this.expect("(");
      const columns = this.list(() => this.identifier());
      this.expect(")");
      this.expect("VALUES");
      this.expect("(");
      const values = this.list(() => this.literal());
      this.expect(")");
      if (columns.length !== values.length)
        throw new Error("INSERT column and value counts differ");
      statement = {
        kind: "insert",
        table,
        values: columns.map((column, i) => [column, values[i]!]),
      };
    } else if (this.take("UPDATE")) {
      const table = this.identifier();
      this.expect("SET");
      const values = this.list<[string, Literal]>(() => {
        const column = this.identifier();
        this.expect("=");
        return [column, this.literal()];
      });
      statement = { kind: "update", table, values, id: this.rowId() };
    } else if (this.take("DELETE")) {
      this.expect("FROM");
      statement = { kind: "delete", table: this.identifier(), id: this.rowId() };
    } else this.fail("SHOW TABLES, DESCRIBE, SELECT, INSERT, UPDATE, or DELETE");
    this.take(";");
    if (this.index !== this.tokens.length)
      this.fail("end of statement (unsupported clause or multiple statements)");
    return statement;
  }
}

export function isSchemaStatement(statement: Statement): boolean {
  return statement.kind === "show-tables" || statement.kind === "describe";
}

export function parseSql(sql: string, write = false): Statement {
  const statement = new Parser(tokenize(sql)).parse();
  if (statement.kind !== "select" && !isSchemaStatement(statement) && !write)
    throw new Error("Read-only mode: mutations require --write");
  return statement;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function literalValue(literal: Literal, column: ColumnDescriptor): unknown {
  const type = column.column_type;
  const mismatch = (): never => {
    throw new Error(
      `Column ${JSON.stringify(column.name)} expects ${type.type}${column.nullable ? " or NULL" : ""}`,
    );
  };
  if (literal.kind === "null") return column.nullable ? null : mismatch();
  if (type.type === "Boolean") return literal.kind === "boolean" ? literal.value : mismatch();
  if (type.type === "Text" || type.type === "Enum" || type.type === "Uuid") {
    if (literal.kind !== "string") return mismatch();
    if (type.type === "Uuid" && !UUID.test(literal.text)) return mismatch();
    if (type.type === "Enum" && !type.variants.includes(literal.text))
      throw new Error(
        `Column ${JSON.stringify(column.name)} expects one of: ${type.variants.join(", ")}`,
      );
    return literal.text;
  }
  if (type.type === "Timestamp" && literal.kind === "string") {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(literal.text)) return mismatch();
    const date = new Date(literal.text);
    if (
      !Number.isFinite(date.getTime()) ||
      date.toISOString() !== literal.text.replace(/(?<=:\d{2})Z$/, ".000Z")
    )
      return mismatch();
    return date.getTime();
  }
  if (["Integer", "BigInt", "Double", "Timestamp"].includes(type.type)) {
    if (literal.kind !== "number") return mismatch();
    if (type.type === "BigInt") {
      if (!/^[+-]?\d+$/.test(literal.text)) return mismatch();
      const value = BigInt(literal.text);
      return value >= -(1n << 63n) && value < 1n << 63n ? value : mismatch();
    }
    const value = Number(literal.text);
    if (!Number.isFinite(value)) return mismatch();
    if (
      type.type === "Integer" &&
      (!/^[+-]?\d+$/.test(literal.text) ||
        !Number.isInteger(value) ||
        value < -2147483648 ||
        value > 2147483647)
    )
      return mismatch();
    if (
      type.type === "Timestamp" &&
      (!Number.isSafeInteger(value) || Math.abs(value) > 8640000000000000)
    )
      return mismatch();
    return value;
  }
  throw new Error(`SQL literals for ${type.type} columns are not supported; use the Jazz API`);
}

type Row = Record<string, unknown>;
type Table = TableProxy<Row, Row>;
export type DataResult = { columns: string[]; rows: Row[] };
export type CompiledStatement =
  | { kind: "schema"; result: DataResult }
  | { kind: "select"; query: QueryBuilder<Row>; columns: string[] }
  | { kind: "insert"; table: Table; values: Row }
  | { kind: "update" | "delete"; table: Table; query: QueryBuilder<Row>; id: string; values: Row };

export function compileSql(statement: Statement, schema: WasmSchema): CompiledStatement {
  if (statement.kind === "show-tables") return { kind: "schema", result: listTables(schema) };
  if (!Object.hasOwn(schema, statement.table))
    throw new Error(`Unknown table ${JSON.stringify(statement.table)}`);
  const definition = schema[statement.table]!;
  if (statement.kind === "describe") return { kind: "schema", result: describeTable(definition) };
  if (definition.branchBy?.length)
    throw new Error("SQL access to branch-keyed tables is not supported; use the Jazz API");
  const column = (name: string): ColumnDescriptor => {
    if (name === "id") return { name, column_type: { type: "Uuid" }, nullable: false };
    // The runtime treats dots and $-prefixes as query paths, not literal names.
    const found = definition.columns.find((candidate) => candidate.name === name);
    if (!found || name.includes(".") || name.startsWith("$"))
      throw new Error(
        `Unknown or unsupported column ${JSON.stringify(name)} on ${JSON.stringify(statement.table)}`,
      );
    return found;
  };
  // Runtime schema validation replaces compile-time table metadata at this CLI
  // boundary. All query operations still use the SDK's existing fluent builder.
  const table = new TypedTableQueryBuilder<TableMeta<string, Row & { id: string }, Row, Row>>(
    statement.table,
    schema,
  );
  if (statement.kind === "select") {
    const columns =
      statement.columns === "*"
        ? ["id", ...definition.columns.filter((c) => !c.sparse).map((c) => c.name)]
        : statement.columns;
    if (new Set(columns).size !== columns.length) throw new Error("Duplicate projection column");
    for (const name of columns) column(name);
    let query = table.select<string, string[]>(columns[0]!, ...columns.slice(1));
    for (const condition of statement.where) {
      const descriptor = column(condition.column);
      if (condition.op === "isNull") {
        if (!descriptor.nullable)
          throw new Error(`Column ${JSON.stringify(condition.column)} is not nullable`);
        query = query.where({ [condition.column]: { isNull: condition.value } });
      } else {
        const supported = getSupportedWhereOperatorsForSchemaColumn(condition.column, descriptor);
        if (!supported?.includes(condition.op))
          throw new Error(
            `Operator ${condition.op} is not supported for ${descriptor.column_type.type} column ${JSON.stringify(condition.column)}`,
          );
        query = query.where({
          [condition.column]: {
            [condition.op]: literalValue(condition.value as Literal, descriptor),
          },
        });
      }
    }
    for (const [name, direction] of statement.order) {
      const type = column(name).column_type.type;
      if (["Row", "Array", "Json", "EnumPayload", "Bytea"].includes(type))
        throw new Error(`ORDER BY is not supported for ${type} columns`);
      query = query.orderBy(name, direction);
    }
    if (statement.limit !== undefined) query = query.limit(statement.limit);
    if (statement.offset !== undefined) query = query.offset(statement.offset);
    return { kind: "select", query, columns };
  }
  const values: Row = Object.create(null);
  if (statement.kind !== "delete") {
    for (const [name, literal] of statement.values) {
      if (name === "id") throw new Error("id is generated by Jazz and cannot be assigned in SQL");
      if (Object.hasOwn(values, name))
        throw new Error(`Duplicate assignment for ${JSON.stringify(name)}`);
      values[name] = literalValue(literal, column(name));
    }
  }
  if (statement.kind === "insert") {
    for (const descriptor of definition.columns) {
      if (
        !descriptor.nullable &&
        !descriptor.sparse &&
        descriptor.default === undefined &&
        !Object.hasOwn(values, descriptor.name)
      )
        throw new Error(`INSERT is missing required column ${JSON.stringify(descriptor.name)}`);
    }
    return { kind: "insert", table, values };
  }
  const id = literalValue(statement.id, column("id")) as string;
  return { kind: statement.kind, table, id, values, query: table.where({ id }) };
}

export async function executeSql(db: Db, statement: CompiledStatement): Promise<DataResult> {
  if (statement.kind === "schema") return statement.result;
  if (statement.kind === "select") {
    const rows = await db.all(statement.query, { tier: "remote" });
    return {
      columns: statement.columns,
      rows: rows.map((row) =>
        Object.fromEntries(statement.columns.map((name) => [name, row[name]])),
      ),
    };
  }
  const columns = ["operation", "affectedRows", "id", "txId"];
  if (statement.kind !== "insert" && !(await db.one(statement.query, { tier: "remote" }))) {
    return {
      columns,
      rows: [{ operation: statement.kind, affectedRows: 0, id: statement.id, txId: null }],
    };
  }
  const handle =
    statement.kind === "insert"
      ? db.insert(statement.table, statement.values)
      : statement.kind === "update"
        ? db.update(statement.table, statement.id, statement.values)
        : db.delete(statement.table, statement.id);
  // Report success only after the configured authority has acknowledged the
  // transaction. SQL does not add retries or stronger concurrency semantics.
  await handle.wait({ tier: "global" });
  const id = statement.kind === "insert" ? (handle.value as Row).id : statement.id;
  return {
    columns,
    rows: [{ operation: statement.kind, affectedRows: 1, id, txId: await handle.txId }],
  };
}
