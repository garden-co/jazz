export type RelColumnRef = {
  scope?: string;
  column: string;
};

export type RelRowIdRef = "Current" | "Outer" | "Frontier";

export type RelValueRef =
  | { Literal: unknown }
  | { Param: string }
  | { SessionRef: string[] }
  | { OuterColumn: RelColumnRef }
  | { FrontierColumn: RelColumnRef }
  | { RowId: RelRowIdRef };

export type RelPredicateCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type RelPredicateExpr =
  | {
      Cmp: {
        left: RelColumnRef;
        op: RelPredicateCmpOp;
        right: RelValueRef;
      };
    }
  | { IsNull: { column: RelColumnRef } }
  | { IsNotNull: { column: RelColumnRef } }
  | { In: { left: RelColumnRef; values: RelValueRef[] } }
  | { Contains: { left: RelColumnRef; right: RelValueRef } }
  | { EnumMatch: { column: RelColumnRef; case: string; payload: RelPredicateExpr } }
  | { And: RelPredicateExpr[] }
  | { Or: RelPredicateExpr[] }
  | { Not: RelPredicateExpr }
  | "True"
  | "False";

export type RelJoinKind = "Inner" | "Left";

export type RelJoinCondition = {
  left: RelColumnRef;
  right: RelColumnRef;
};

export type RelKeyRef = { Column: RelColumnRef } | { RowId: RelRowIdRef };

export type RelProjectExpr = { Column: RelColumnRef } | { RowId: RelRowIdRef };

export type RelProjectColumn = {
  alias: string;
  expr: RelProjectExpr;
};

export type RelOrderDirection = "Asc" | "Desc";

export type RelOrderByExpr = {
  column: RelColumnRef;
  direction: RelOrderDirection;
};

/**
 * A semantic recursion bound. `MaxDepth: 0` includes the seed and no recursive hop.
 */
export type RelRecursionBound = "Fixpoint" | { MaxDepth: number };

export type RelExpr =
  | { TableScan: { table: string; alias?: string } }
  | { Filter: { input: RelExpr; predicate: RelPredicateExpr } }
  | { Union: { inputs: Array<{ label: string; input: RelExpr }> } }
  | { Join: { left: RelExpr; right: RelExpr; on: RelJoinCondition[]; join_kind: RelJoinKind } }
  | { Project: { input: RelExpr; columns: RelProjectColumn[] } }
  | {
      Gather: {
        seed: RelExpr;
        step: RelExpr;
        frontier_key: RelKeyRef;
        bound: RelRecursionBound;
        dedupe_key: RelKeyRef[];
      };
    }
  | { Distinct: { input: RelExpr; key: RelKeyRef[] } }
  | { OrderBy: { input: RelExpr; terms: RelOrderByExpr[] } }
  | { Offset: { input: RelExpr; offset: number } }
  | { Limit: { input: RelExpr; limit: number } };

export type PolicyOperation = "Select" | "Insert" | "Update" | "Delete";

/** A relational policy expression used by the query IR, distinct from the schema DSL AST. */
export type PolicyIRExpr =
  | { Predicate: RelPredicateExpr }
  | { ExistsRel: { rel: RelExpr } }
  | {
      Inherits: {
        operation: PolicyOperation;
        via_column: string;
        max_depth?: number;
      };
    }
  | { And: PolicyIRExpr[] }
  | { Or: PolicyIRExpr[] }
  | { Not: PolicyIRExpr }
  | "True"
  | "False";

/** Encode a relation IR using the Rust-owned JRQ v1 binary value grammar. */
export function encodeRelationQueryV1(relation: RelExpr): Uint8Array {
  const bytes: number[] = [0x4a, 0x52, 0x51, 0x01];
  const writeLength = (value: number) => {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error("invalid JRQ length");
    do {
      let byte = value & 0x7f;
      value = Math.floor(value / 128);
      if (value) byte |= 0x80;
      bytes.push(byte);
    } while (value);
  };
  const text = new TextEncoder();
  const writeBytes = (value: Uint8Array) => {
    writeLength(value.length);
    bytes.push(...value);
  };
  const write = (value: unknown): void => {
    if (value === null) {
      bytes.push(0);
      return;
    }
    if (value === false) {
      bytes.push(1);
      return;
    }
    if (value === true) {
      bytes.push(2);
      return;
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value)) throw new Error("invalid JRQ number");
      bytes.push(3);
      writeBytes(text.encode(String(value)));
      return;
    }
    if (typeof value === "bigint") {
      bytes.push(4);
      writeBytes(text.encode(value.toString()));
      return;
    }
    if (typeof value === "string") {
      bytes.push(4);
      writeBytes(text.encode(value));
      return;
    }
    if (Array.isArray(value)) {
      bytes.push(5);
      writeLength(value.length);
      value.forEach(write);
      return;
    }
    if (value && typeof value === "object") {
      bytes.push(6);
      const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
        a < b ? -1 : a > b ? 1 : 0,
      );
      writeLength(entries.length);
      for (const [key, child] of entries) {
        writeBytes(text.encode(key));
        write(child);
      }
      return;
    }
    throw new Error("unsupported JRQ value");
  };
  write(relation);
  return Uint8Array.from(bytes);
}
