import type { ColumnDescriptor, WasmSchema } from "../drivers/types.js";
import type { Db, QueryBuilder, TableProxy } from "../runtime/db.js";
import { TypedTableQueryBuilder, type TableMeta } from "../typed-app.js";
import {
  getSupportedWhereOperatorsForSchemaColumn,
  type WhereOperator,
} from "../where-operators.js";
import { DataError, nearestName } from "./errors.js";
import { idFromSeed, schemaDescribeResult, schemaListResult } from "./schema.js";

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
  | { kind: "insert"; table: string; values: [string, Literal][]; idSeed?: string }
  | { kind: "update"; table: string; values: [string, Literal][]; id: Literal }
  | { kind: "delete"; table: string; id: Literal };
type Token = {
  kind: "word" | "identifier" | "string" | "number" | "symbol";
  text: string;
  at: number;
};

export interface ParseOptions {
  /** Allow INSERT/UPDATE/DELETE. */
  write?: boolean;
  /**
   * Seed for a client-generated id. A stable seed makes a retried INSERT
   * idempotent; without one the id is random and the retry duplicates the row.
   */
  idSeed?: string;
}

const dialectError = (message: string, at?: number): DataError =>
  new DataError("SQL_SYNTAX", at === undefined ? message : `SQL at ${at + 1}: ${message}`, {
    hint: "Run `jazz-tools sql --capabilities` for the supported dialect.",
  });

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
      if (end < 0) throw dialectError("unterminated comment", at);
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
      if (!closed) throw dialectError("unterminated quoted value", start);
      tokens.push({ kind: quote === "'" ? "string" : "identifier", text: value, at: start });
      continue;
    }
    const number = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/.exec(rest);
    const word = /^[A-Za-z_][A-Za-z_0-9]*/.exec(rest);
    const symbol = /^(?:<=|>=|<>|!=|[=<>*,();])/.exec(rest);
    const match = number ?? word ?? symbol;
    if (!match)
      throw new DataError(
        "UNSUPPORTED",
        `SQL at ${at + 1}: unsupported character ${JSON.stringify(sql[at])}`,
        { hint: "Run `jazz-tools sql --capabilities` for the supported dialect." },
      );
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
    throw dialectError(`expected ${expected}`, token ? token.at : undefined);
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
      throw new DataError(
        "SQL_SYNTAX",
        "Use IS NULL or IS NOT NULL instead of comparing with NULL",
      );
    return { column, op, value };
  }
  private orderBy(): Selection["order"] {
    this.expect("BY");
    return this.list(() => {
      const column = this.identifier();
      let direction: "asc" | "desc" = "asc";
      if (this.take("DESC")) direction = "desc";
      else this.take("ASC");
      return [column, direction] as [string, "asc" | "desc"];
    });
  }
  private rowId(): Literal {
    this.expect("WHERE");
    if (this.identifier() !== "id") this.fail("WHERE id = 'uuid' for UPDATE/DELETE");
    this.expect("=");
    return this.literal();
  }
  private idSeed(): string | undefined {
    if (!this.take("WITH")) return undefined;
    this.expect("ID");
    this.expect("SEED");
    const seed = this.literal();
    if (seed.kind !== "string") this.fail("string literal for WITH ID SEED");
    return seed.text;
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
      const order = this.take("ORDER") ? this.orderBy() : [];
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
        throw new DataError("SQL_SYNTAX", "INSERT column and value counts differ");
      statement = {
        kind: "insert",
        table,
        values: columns.map((column, i) => [column, values[i]!]),
        idSeed: this.idSeed(),
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

export function parseSql(sql: string, options: boolean | ParseOptions = {}): Statement {
  const { write = false, idSeed } = typeof options === "boolean" ? { write: options } : options;
  const statement = new Parser(tokenize(sql)).parse();
  if (statement.kind !== "select" && !isSchemaStatement(statement) && !write)
    throw new DataError("READ_ONLY", "Read-only mode: mutations require --write", {
      hint: "Add --write (and --explain to preview the change first).",
    });
  if (statement.kind === "insert" && idSeed !== undefined) statement.idSeed ??= idSeed;
  return statement;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function mismatch(column: ColumnDescriptor, expected?: string): never {
  const type = column.column_type;
  const detail = expected ?? `${type.type}${column.nullable ? " or NULL" : ""}`;
  throw new DataError("TYPE_MISMATCH", `Column ${JSON.stringify(column.name)} expects ${detail}`);
}

function literalValue(literal: Literal, column: ColumnDescriptor): unknown {
  const type = column.column_type;
  if (literal.kind === "null") return column.nullable ? null : mismatch(column);
  if (type.type === "Boolean") return literal.kind === "boolean" ? literal.value : mismatch(column);
  if (type.type === "Text" || type.type === "Enum" || type.type === "Uuid") {
    if (literal.kind !== "string") return mismatch(column);
    if (type.type === "Uuid" && !UUID.test(literal.text)) return mismatch(column, "a UUID string");
    if (type.type === "Enum" && !type.variants.includes(literal.text))
      throw new DataError(
        "TYPE_MISMATCH",
        `Column ${JSON.stringify(column.name)} expects one of: ${type.variants.join(", ")}`,
      );
    return literal.text;
  }
  if (type.type === "Timestamp" && literal.kind === "string") {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(literal.text))
      return mismatch(column, "a UTC timestamp (YYYY-MM-DDTHH:mm:ss[.SSS]Z) or Unix milliseconds");
    const date = new Date(literal.text);
    if (
      !Number.isFinite(date.getTime()) ||
      date.toISOString() !== literal.text.replace(/(?<=:\d{2})Z$/, ".000Z")
    )
      return mismatch(column, "a real UTC timestamp");
    return date.getTime();
  }
  if (["Integer", "BigInt", "Double", "Timestamp"].includes(type.type)) {
    if (literal.kind !== "number") return mismatch(column);
    if (type.type === "BigInt") {
      if (!/^[+-]?\d+$/.test(literal.text)) return mismatch(column, "a signed 64-bit integer");
      const value = BigInt(literal.text);
      return value >= -(1n << 63n) && value < 1n << 63n
        ? value
        : mismatch(column, "a signed 64-bit integer");
    }
    const value = Number(literal.text);
    if (!Number.isFinite(value)) return mismatch(column);
    if (
      type.type === "Integer" &&
      (!/^[+-]?\d+$/.test(literal.text) ||
        !Number.isInteger(value) ||
        value < -2147483648 ||
        value > 2147483647)
    )
      return mismatch(column, "a signed 32-bit integer");
    if (
      type.type === "Timestamp" &&
      (!Number.isSafeInteger(value) || Math.abs(value) > 8640000000000000)
    )
      return mismatch(column, "Unix milliseconds within the representable range");
    return value;
  }
  throw new DataError(
    "UNSUPPORTED",
    `SQL literals for ${type.type} columns are not supported; use the Jazz API`,
  );
}

type Row = Record<string, unknown>;
type Table = TableProxy<Row, Row>;
/** A tabular result: one column list plus rows. */
export type RowResult = { kind: "rows"; columns: string[]; rows: Row[] };
/** A row result, or a structured object result for --explain/--capabilities. */
export type DataResult = RowResult | { kind: "object"; value: unknown };
export type CompiledStatement =
  | { kind: "schema"; result: RowResult }
  | { kind: "select"; query: QueryBuilder<Row>; columns: string[] }
  | { kind: "insert"; table: Table; values: Row; id?: string; idSource: "client" | "server" }
  | {
      kind: "update" | "delete";
      table: Table;
      query: QueryBuilder<Row>;
      id: string;
      values: Row;
    };

/**
 * Resolve a SQL column name against a table definition.
 *
 * `id` is implicit on every table; dotted and `$`-prefixed names are reserved by
 * the runtime for query paths, so they are never treated as literal column names.
 */
export function resolveColumn(schema: WasmSchema, table: string, name: string): ColumnDescriptor {
  if (name === "id") return { name, column_type: { type: "Uuid" }, nullable: false };
  const definition = schema[table];
  if (!definition) throw new DataError("UNKNOWN_TABLE", `Unknown table ${JSON.stringify(table)}`);
  const found = definition.columns.find((candidate) => candidate.name === name);
  if (!found || name.includes(".") || name.startsWith("$")) {
    const suggestion = nearestName(name, [
      "id",
      ...definition.columns.map((candidate) => candidate.name),
    ]);
    throw new DataError(
      "UNKNOWN_COLUMN",
      `Unknown or unsupported column ${JSON.stringify(name)} on ${JSON.stringify(table)}`,
      {
        hint: suggestion
          ? `Did you mean ${JSON.stringify(suggestion)}? Run \`jazz-tools sql 'DESCRIBE ${table}'\` to see columns.`
          : `Run \`jazz-tools sql 'DESCRIBE ${table}'\` to list columns.`,
      },
    );
  }
  return found;
}

/** Effective schema context for a compiled statement, surfaced by --explain. */
export interface CompileContext {
  schemaHash: string;
  schemaSource: string;
}

export function compileSql(
  statement: Statement,
  schema: WasmSchema,
  context: CompileContext,
): CompiledStatement {
  if (statement.kind === "show-tables")
    return { kind: "schema", result: schemaListResult(schema, context.schemaSource) };
  if (!Object.hasOwn(schema, statement.table)) {
    const suggestion = nearestName(statement.table, Object.keys(schema));
    throw new DataError("UNKNOWN_TABLE", `Unknown table ${JSON.stringify(statement.table)}`, {
      hint: suggestion
        ? `Did you mean ${JSON.stringify(suggestion)}? Run \`jazz-tools sql 'SHOW TABLES'\`.`
        : "Run `jazz-tools sql 'SHOW TABLES'` to list tables.",
    });
  }
  const definition = schema[statement.table]!;
  if (statement.kind === "describe")
    return {
      kind: "schema",
      result: schemaDescribeResult(schema, statement.table, context.schemaSource),
    };
  if (definition.branchBy?.length)
    throw new DataError(
      "UNSUPPORTED",
      "SQL access to branch-keyed tables is not supported; use the Jazz API",
    );
  const column = (name: string): ColumnDescriptor => resolveColumn(schema, statement.table, name);
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
    if (new Set(columns).size !== columns.length)
      throw new DataError("SQL_SYNTAX", "Duplicate projection column");
    for (const name of columns) column(name);
    let query = table.select<string, string[]>(columns[0]!, ...columns.slice(1));
    for (const condition of statement.where) {
      const descriptor = column(condition.column);
      if (condition.op === "isNull") {
        if (!descriptor.nullable)
          throw new DataError(
            "TYPE_MISMATCH",
            `Column ${JSON.stringify(condition.column)} is not nullable`,
          );
        query = query.where({ [condition.column]: { isNull: condition.value } });
      } else {
        const supported = getSupportedWhereOperatorsForSchemaColumn(condition.column, descriptor);
        if (!supported?.includes(condition.op))
          throw new DataError(
            "UNSUPPORTED",
            `Operator ${condition.op} is not supported for ${descriptor.column_type.type} column ${JSON.stringify(condition.column)}`,
            {
              hint: `Supported operators here: ${supported?.length ? supported.join(", ") : "none (use IS NULL / IS NOT NULL only if nullable)"}.`,
            },
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
        throw new DataError("UNSUPPORTED", `ORDER BY is not supported for ${type} columns`);
      query = query.orderBy(name, direction);
    }
    if (statement.limit !== undefined) query = query.limit(statement.limit);
    if (statement.offset !== undefined) query = query.offset(statement.offset);
    return { kind: "select", query, columns };
  }
  const values: Row = Object.create(null);
  if (statement.kind !== "delete") {
    for (const [name, literal] of statement.values) {
      if (name === "id")
        throw new DataError(
          "UNSUPPORTED",
          "id is generated by Jazz and cannot be assigned in SQL",
          { hint: "Use `INSERT ... WITH ID SEED '<seed>'` to control the generated id." },
        );
      if (Object.hasOwn(values, name))
        throw new DataError("SQL_SYNTAX", `Duplicate assignment for ${JSON.stringify(name)}`);
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
        throw new DataError(
          "SQL_SYNTAX",
          `INSERT is missing required column ${JSON.stringify(descriptor.name)}`,
        );
    }
    const id = statement.idSeed === undefined ? undefined : idFromSeed(statement.idSeed);
    return {
      kind: "insert",
      table,
      values,
      ...(id === undefined ? {} : { id }),
      idSource: id === undefined ? "server" : "client",
    };
  }
  const id = literalValue(statement.id, column("id")) as string;
  return { kind: statement.kind, table, id, values, query: table.where({ id }) };
}

export interface WriteOutcome {
  operation: string;
  affectedRows: number;
  id: string | null;
  idSource: "client" | "server" | null;
  txId: string | null;
  atomic: false;
  /**
   * True when a client id makes a retry unable to duplicate the row. An
   * ALREADY_EXISTS failure then means the earlier attempt committed.
   */
  safeToRetry: boolean;
}

export async function executeSql(db: Db, statement: CompiledStatement): Promise<RowResult> {
  if (statement.kind === "schema") return statement.result;
  if (statement.kind === "select") {
    const rows = await db.all(statement.query, { tier: "remote" });
    return {
      kind: "rows",
      columns: statement.columns,
      rows: rows.map((row) =>
        Object.fromEntries(statement.columns.map((name) => [name, row[name]])),
      ),
    };
  }
  const columns = ["operation", "affectedRows", "id", "idSource", "txId", "atomic", "safeToRetry"];
  if (statement.kind !== "insert" && !(await db.one(statement.query, { tier: "remote" }))) {
    return {
      kind: "rows",
      columns,
      rows: [
        {
          operation: statement.kind,
          affectedRows: 0,
          id: statement.id,
          idSource: null,
          txId: null,
          // Writes are a read followed by a write, never a compare-and-swap.
          atomic: false,
          safeToRetry: false,
        },
      ],
    };
  }
  const handle =
    statement.kind === "insert"
      ? db.insert(
          statement.table,
          statement.values,
          // The id is part of the compiled statement, so execution cannot drop
          // the retry guarantee that `WITH ID SEED` established.
          statement.id === undefined ? undefined : { id: statement.id },
        )
      : statement.kind === "update"
        ? db.update(statement.table, statement.id, statement.values)
        : db.delete(statement.table, statement.id);
  // Report success only after the configured authority has acknowledged the
  // transaction. SQL does not add retries or stronger concurrency semantics.
  await handle.wait({ tier: "global" });
  const id = statement.kind === "insert" ? ((handle.value as Row).id as string) : statement.id;
  return {
    kind: "rows",
    columns,
    rows: [
      {
        operation: statement.kind,
        affectedRows: 1,
        id,
        idSource: statement.kind === "insert" ? statement.idSource : null,
        txId: await handle.txId,
        // The initial existence read and the mutation are separate operations.
        atomic: false,
        // A caller-chosen id makes a retried INSERT fail instead of duplicating.
        safeToRetry: statement.kind === "insert" && statement.idSource === "client",
      },
    ],
  };
}

/** Schema context and identity reported by --explain. */
export interface ExplainContext {
  schemaSource: string;
  schemaHash: string;
  auth: string;
  appId: string | null;
  serverUrl: string | null;
}

/**
 * Summarize a fully validated statement without executing it.
 *
 * The plan is produced after schema resolution and compilation, so it reports
 * exactly what the command would send: the resolved projection, the operator and
 * resolved column type of every predicate, and the id strategy for an INSERT.
 */
export function describePlan(
  statement: Statement,
  compiled: CompiledStatement,
  schema: WasmSchema,
  context: ExplainContext,
): Record<string, unknown> {
  const shared = {
    executes: false,
    connects: false,
    schemaSource: context.schemaSource,
    schemaHash: context.schemaHash,
    auth: context.auth,
    appId: context.appId,
    serverUrl: context.serverUrl,
    // Writes are a read followed by a write; the CLI adds no compare-and-swap.
    atomic: false,
  };
  if (statement.kind === "show-tables")
    return { ...shared, statement: statement.kind, table: null };
  if (statement.kind === "describe")
    return { ...shared, statement: statement.kind, table: statement.table };
  if (statement.kind === "select") {
    const projection = compiled.kind === "select" ? compiled.columns : [];
    return {
      ...shared,
      statement: "select",
      table: statement.table,
      projection,
      where: statement.where.map((condition) => ({
        column: condition.column,
        operator: condition.op,
        resolvedType: resolveColumn(schema, statement.table, condition.column).column_type.type,
      })),
      order: statement.order,
      limit: statement.limit ?? null,
      offset: statement.offset ?? null,
      readTier: "remote",
    };
  }
  if (statement.kind === "insert")
    return {
      ...shared,
      statement: "insert",
      table: statement.table,
      values: compiled.kind === "insert" ? compiled.values : {},
      generatedId: compiled.kind === "insert" ? (compiled.id ?? null) : null,
      idSource: compiled.kind === "insert" ? compiled.idSource : "server",
      safeToRetry: compiled.kind === "insert" && compiled.idSource === "client",
    };
  return {
    ...shared,
    statement: statement.kind,
    table: statement.table,
    id: compiled.kind === "update" || compiled.kind === "delete" ? compiled.id : null,
    values: compiled.kind === "update" || compiled.kind === "delete" ? compiled.values : {},
    requiresExistingVisibleRow: true,
  };
}
