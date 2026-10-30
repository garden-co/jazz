import type {
  PolicyExpr as WasmPolicyExpr,
  PolicyValue as WasmPolicyValue,
  Value as WasmValue,
} from "./drivers/types.js";
import type { RelExpr } from "./ir.js";
import type { PolicyExpr, PolicyLiteralValue, PolicyValue } from "./schema.js";

/** How {@link mapPolicyExpr} lowers the leaves of a policy expression. */
export interface PolicyLeafMappers {
  value(value: PolicyValue): WasmPolicyValue;
  literal(value: PolicyLiteralValue): WasmValue;
  rel(rel: RelExpr): unknown;
}

/**
 * Lower a DSL policy expression to its wasm form. Every node is rebuilt and
 * session paths are copied, so the result shares no mutable state with `expr`.
 */
export function mapPolicyExpr(expr: PolicyExpr, leaf: PolicyLeafMappers): WasmPolicyExpr {
  const map = (child: PolicyExpr) => mapPolicyExpr(child, leaf);
  switch (expr.type) {
    case "Cmp":
    case "Contains":
      return { ...expr, value: leaf.value(expr.value) };
    case "SessionCmp":
    case "SessionContains":
      return { ...expr, path: [...expr.path], value: leaf.literal(expr.value) };
    case "SessionIsNull":
    case "SessionIsNotNull":
      return { ...expr, path: [...expr.path] };
    case "In":
      return { ...expr, session_path: [...expr.session_path] };
    case "InList":
      return { ...expr, values: expr.values.map((value) => leaf.value(value)) };
    case "SessionInList":
      return {
        ...expr,
        path: [...expr.path],
        values: expr.values.map((value) => leaf.literal(value)),
      };
    case "Exists":
      return { ...expr, condition: map(expr.condition) };
    case "ExistsRel":
      return { type: "ExistsRel", rel: leaf.rel(expr.rel) };
    case "And":
    case "Or":
      return { type: expr.type, exprs: expr.exprs.map(map) };
    case "Not":
      return { type: "Not", expr: map(expr.expr) };
    case "IsNull":
    case "IsNotNull":
    case "Inherits":
    case "InheritsReferencing":
    case "True":
    case "False":
      return { ...expr };
  }
}
