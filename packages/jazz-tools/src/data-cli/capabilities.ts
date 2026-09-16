import { WHERE_OPERATORS, type WhereOperator } from "../where-operators.js";
import type { ColumnType } from "../drivers/types.js";

export interface CapabilityColumnType {
  type: ColumnType["type"];
  operators: WhereOperator[];
}

/**
 * Machine-readable description of the accepted dialect.
 *
 * This is generated from the same tables the parser and compiler use, so it
 * cannot drift from what the command actually accepts. `truthfulWhereOperators`
 * are the comparison operators parsed from SQL; `contains`/`in`/`notIn` are SDK
 * operators that this dialect has no syntax for yet.
 */
export function capabilities(): Record<string, unknown> {
  const parsedComparisons: WhereOperator[] = ["eq", "ne", "gt", "gte", "lt", "lte"];
  const unresolved: WhereOperator[] = ["contains", "in", "notIn"];
  return {
    version: 1,
    statements: [
      "SHOW TABLES",
      "DESCRIBE <table>",
      "SELECT <*|columns> FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n]",
      "INSERT INTO <table> (columns) VALUES (literals) [WITH ID SEED '<seed>']",
      "UPDATE <table> SET column = literal [, ...] WHERE id = '<uuid>'",
      "DELETE FROM <table> WHERE id = '<uuid>'",
    ],
    supported: {
      projection: true,
      whereAnd: true,
      nullTests: ["IS NULL", "IS NOT NULL"],
      orderBy: true,
      limitOffset: true,
      singleRowInsert: true,
      idSeed: true,
      comments: ["-- line", "/* block (non-nested) */"],
      identifierQuoting: '"double" (case-sensitive)',
      stringQuoting: "'single' ('' escapes)",
    },
    unsupported: [
      "joins",
      "OR / NOT / parenthesised expressions",
      "column aliases (AS)",
      "aggregates and functions",
      "parameters / placeholders",
      "multi-row VALUES",
      "SQL transactions and compare-and-swap",
      "schema changes (DDL)",
      "branch-keyed tables",
      "structured literals (JSON, arrays, bytes, payload enums)",
    ],
    whereOperators: {
      parsed: parsedComparisons,
      parsedPlusNullTests: [...parsedComparisons, "isNull"],
      allSdkOperators: [...WHERE_OPERATORS],
      unsupportedSyntax: unresolved,
    },
    columnTypes: columnTypeOperators(),
    literals: {
      string: "single-quoted; no implicit coercion to numbers",
      boolean: ["TRUE", "FALSE"],
      null: "NULL (never compared with =)",
      integer: "signed 32-bit",
      bigint: "signed 64-bit, exact (never through a JS Number)",
      double: "finite",
      timestamp: "Unix milliseconds or 'YYYY-MM-DDTHH:mm:ss[.SSS]Z'",
      uuid: "canonical 8-4-4-4-12 hex",
      enum: "one of the declared variants",
    },
    limits: {
      maxStatementCount: 1,
      limitOffset: "nonnegative safe integers",
      timeoutMs: { default: 30000, max: 2147483647 },
    },
    guarantees: {
      atomicWrites: false,
      readTier: "remote (no offline cache fallback)",
      writeAcknowledgement: "global",
      retries:
        "none; `INSERT ... WITH ID SEED` pins the row id so a retry fails with ALREADY_EXISTS (exit 6) instead of duplicating the row",
      truncatedInTableFormat: true,
    },
    exitCodes: {
      "0": "success",
      "1": "internal failure",
      "2": "usage or invalid input",
      "3": "missing or ambiguous credentials",
      "4": "denied by the server",
      "5": "timeout (a write may already have committed)",
      "6": "the row already exists (a previous attempt committed)",
    },
  };
}

function columnTypeOperators(): Record<string, CapabilityColumnType> {
  const types: ColumnType["type"][] = [
    "Text",
    "Boolean",
    "Integer",
    "BigInt",
    "Double",
    "Timestamp",
    "Uuid",
    "Bytea",
    "Json",
    "Enum",
    "EnumPayload",
    "Array",
    "Row",
  ];
  const parsed: WhereOperator[] = ["eq", "ne", "gt", "gte", "lt", "lte"];
  const result = {} as Record<string, CapabilityColumnType>;
  for (const type of types) {
    result[type] = { type, operators: operatorsForType(type, parsed) };
  }
  return result;
}

function operatorsForType(type: ColumnType["type"], parsed: WhereOperator[]): WhereOperator[] {
  switch (type) {
    case "Integer":
    case "BigInt":
    case "Double":
    case "Timestamp":
      return parsed;
    case "Text":
    case "Boolean":
    case "Uuid":
    case "Bytea":
    case "Json":
    case "Enum":
      return parsed.filter((operator) => operator === "eq" || operator === "ne");
    // Payload enums and nested rows are not comparable through this dialect.
    case "EnumPayload":
    case "Row":
    case "Array":
      return [];
  }
}
