// Policy expressions for row-level security (ReBAC). The TS shape matches the
// serde JSON shape (`{type: <variant>, ...fields}`), so no codec is needed.
import type { RelExpr } from "./relation_ir.js";
import { type Value, valueEquals } from "./types/value.js";

export type CmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type PolicyValue =
  | { type: "Literal"; value: Value }
  | { type: "SessionRef"; path: string[] };

export type Operation = "Select" | "Insert" | "Update" | "Delete";
export const operationDisplay = (op: Operation): string => op.toUpperCase();

export type PolicyExpr =
  | { type: "Cmp"; column: string; op: CmpOp; value: PolicyValue }
  | { type: "SessionCmp"; path: string[]; op: CmpOp; value: Value }
  | { type: "IsNull"; column: string }
  | { type: "SessionIsNull"; path: string[] }
  | { type: "IsNotNull"; column: string }
  | { type: "SessionIsNotNull"; path: string[] }
  | { type: "Contains"; column: string; value: PolicyValue }
  | { type: "SessionContains"; path: string[]; value: Value }
  | { type: "In"; column: string; session_path: string[] }
  | { type: "InList"; column: string; values: PolicyValue[] }
  | { type: "SessionInList"; path: string[]; values: Value[] }
  | { type: "Exists"; table: string; condition: PolicyExpr }
  | { type: "ExistsRel"; rel: RelExpr }
  | { type: "Inherits"; operation: Operation; via_column: string; max_depth?: number }
  | {
      type: "InheritsReferencing";
      operation: Operation;
      source_table: string;
      via_column: string;
      max_depth?: number;
    }
  | { type: "And"; exprs: PolicyExpr[] }
  | { type: "Or"; exprs: PolicyExpr[] }
  | { type: "Not"; expr: PolicyExpr }
  | { type: "True" }
  | { type: "False" };

export const PolicyExprs = {
  eqSession: (column: string, path: string[]): PolicyExpr => ({
    type: "Cmp",
    column,
    op: "Eq",
    value: { type: "SessionRef", path },
  }),
  eqLiteral: (column: string, value: Value): PolicyExpr => ({
    type: "Cmp",
    column,
    op: "Eq",
    value: { type: "Literal", value },
  }),
  inSession: (column: string, session_path: string[]): PolicyExpr => ({
    type: "In",
    column,
    session_path,
  }),
  inherits: (operation: Operation, via_column: string, max_depth?: number): PolicyExpr => ({
    type: "Inherits",
    operation,
    via_column,
    max_depth,
  }),
  inheritsReferencing: (
    operation: Operation,
    source_table: string,
    via_column: string,
    max_depth?: number,
  ): PolicyExpr => ({
    type: "InheritsReferencing",
    operation,
    source_table,
    via_column,
    max_depth,
  }),
  and: (exprs: PolicyExpr[]): PolicyExpr =>
    exprs.length === 0 ? { type: "True" } : exprs.length === 1 ? exprs[0] : { type: "And", exprs },
  or: (exprs: PolicyExpr[]): PolicyExpr =>
    exprs.length === 0 ? { type: "False" } : exprs.length === 1 ? exprs[0] : { type: "Or", exprs },
  not: (expr: PolicyExpr): PolicyExpr => ({ type: "Not", expr }),
};

/** Structural equality (needed because Value compares doubles bitwise). */
export function policyExprEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (a instanceof Uint8Array || b instanceof Uint8Array) return false;
  if (Array.isArray(a))
    return (
      Array.isArray(b) && a.length === b.length && a.every((x, i) => policyExprEquals(x, b[i]))
    );
  const ka = Object.keys(a).filter((k) => (a as any)[k] !== undefined);
  const kb = Object.keys(b).filter((k) => (b as any)[k] !== undefined);
  if (ka.length !== kb.length) return false;
  const av = a as any;
  if (typeof av.type === "string" && "value" in av && av.type in VALUE_TYPES)
    return valueEquals(av, b as any);
  return ka.every((k) => policyExprEquals(av[k], (b as any)[k]));
}
const VALUE_TYPES = { Double: 1, Bytea: 1, TransactionId: 1 } as Record<string, number>;
