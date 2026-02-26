# Policy Clause Contains/In Filter Parity Design

## Overview

Enable `contains` and `in` in permission policy `where(...)` clauses so policy filtering matches normal query filter behavior for supported operators.

Today, `definePermissions(...).where({ ... })` rejects these operators in `packages/jazz-tools/src/permissions/index.ts`, even though normal query filters already support them through relation IR/runtime predicates.

This design introduces first-class policy expression variants for `contains` and literal-list `in`, wires them across TypeScript and Rust representations, and updates SQL serialization/parsing so migration output stays round-trippable.

## Architecture / Components

### 1. Permissions DSL compiler (`packages/jazz-tools/src/permissions/index.ts`)

Update `columnFilterToExprs(...)`:

- `contains` compiles to `PolicyExpr::{ type: "Contains", column, value }`.
- `in` compiles to:
  - existing session-array policy form (`type: "In"`) when value is a session ref,
  - new list-membership form (`type: "InList"`) when value is an array.
- empty `in: []` compiles to `{ type: "False" }` to keep deterministic "matches nothing" behavior.
- invalid `in` values (neither array nor session ref) throw a targeted error.

Core snippet:

```ts
case "contains":
  exprs.push({
    type: "Contains",
    column,
    value: toPolicyValue(value, options),
  });
  break;
case "in":
  if (isSessionRefValue(value)) {
    exprs.push({ type: "In", column, session_path: value.path });
    break;
  }
  if (!Array.isArray(value)) {
    throw new Error(`"${column}.in" expects an array or session reference.`);
  }
  if (value.length === 0) {
    exprs.push({ type: "False" });
    break;
  }
  exprs.push({
    type: "InList",
    column,
    values: value.map((entry) => toPolicyValue(entry, options)),
  });
  break;
```

### 2. Policy expression type surface (TS)

Extend both:

- `packages/jazz-tools/src/schema.ts`
- `packages/jazz-tools/src/drivers/types.ts`

with:

- `Contains { column: string; value: PolicyValue }`
- `InList { column: string; values: PolicyValue[] }`

Keep existing `In { column; session_path }` for backward compatibility and for session-array membership semantics.

### 3. TS conversion/serialization paths

Update:

- `packages/jazz-tools/src/codegen/schema-reader.ts` (`clonePolicyExpr`)
- `packages/jazz-tools/src/sql-gen.ts` (`policyExprToSql`)

SQL rendering additions:

- `Contains` -> `"<column> CONTAINS <value>"`
- `InList` -> `"<column> IN (<v1>, <v2>, ...)"`

### 4. WASM bridge types/conversions

Update `crates/jazz-wasm/src/types.rs`:

- add `WasmPolicyExpr::Contains` and `WasmPolicyExpr::InList`
- update `From<PolicyExpr> for WasmPolicyExpr`
- update `TryFrom<WasmPolicyExpr> for PolicyExpr`

### 5. Rust policy core (`crates/jazz-tools/src/query_manager/policy.rs`)

Add `PolicyExpr` variants:

- `Contains { column: String, value: PolicyValue }`
- `InList { column: String, values: Vec<PolicyValue> }`

Evaluation additions across all match sites:

- `evaluate_with_context` / `evaluate_expr_simple` / `evaluate_simple_recursive`
- `bind_outer_row_refs` (resolve outer-row refs inside `Contains`/`InList` values)

Implementation shape:

```rust
fn evaluate_contains(
    column: &str,
    value: &PolicyValue,
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    // Resolve right-hand value (literal/session ref), decode row column,
    // then apply same semantics as query filter: text substring OR array membership.
}

fn evaluate_in_list(
    column: &str,
    values: &[PolicyValue],
    content: &[u8],
    descriptor: &RowDescriptor,
    session: &Session,
) -> bool {
    // Resolve each candidate value and return true if any equals decoded column value.
}
```

### 6. SQL parser/generator parity in Rust schema manager

Update `crates/jazz-tools/src/schema_manager/sql.rs`:

- tokenizer: add `CONTAINS` keyword token
- parser: support
  - `column CONTAINS <policy_value>` -> `PolicyExpr::Contains`
  - `column IN (<policy_value>, ...)` -> `PolicyExpr::InList`
  - keep `column IN @session.path` -> existing `PolicyExpr::In`
- generator: serialize new variants.

This ensures policy SQL emitted by JS tooling remains parseable by Rust schema manager.

## Data Models

Unified policy expression shape after change:

```ts
type PolicyExpr =
  | { type: "Cmp"; column: string; op: PolicyCmpOp; value: PolicyValue }
  | { type: "IsNull"; column: string }
  | { type: "IsNotNull"; column: string }
  | { type: "Contains"; column: string; value: PolicyValue }
  | { type: "In"; column: string; session_path: string[] }
  | { type: "InList"; column: string; values: PolicyValue[] }
  | { type: "Exists"; table: string; condition: PolicyExpr }
  | { type: "ExistsRel"; rel: RelExpr }
  | { type: "Inherits"; operation: PolicyOperation; via_column: string; max_depth?: number }
  | {
      type: "InheritsReferencing";
      operation: PolicyOperation;
      source_table: string;
      via_column: string;
      max_depth?: number;
    }
  | { type: "And"; exprs: PolicyExpr[] }
  | { type: "Or"; exprs: PolicyExpr[] }
  | { type: "Not"; expr: PolicyExpr }
  | { type: "True" }
  | { type: "False" };
```

Parity target versus normal query filters:

- already supported in policy DSL: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `isNull`
- added by this design: `contains`, `in`
- not in normal generated `WhereInput`: `between` (no action required for parity)

## Testing Strategy

### TypeScript unit tests

`packages/jazz-tools/src/permissions/index.test.ts`

- replace current `contains` rejection assertion with successful compile assertion.
- add `in` compile assertions for:
  - literal arrays (`InList`)
  - session-ref arrays (`In`)
  - empty list (`False`)
- keep invalid-input guard tests (`in` non-array/non-session-ref throws).

`packages/jazz-tools/src/sql-gen.test.ts`

- add policy SQL generation tests for `Contains` and `InList`.

### Rust unit tests

`crates/jazz-tools/src/query_manager/policy.rs`

- add evaluation tests for:
  - text `Contains`
  - array `Contains`
  - `InList` literal values
  - `InList` with session/outer-row refs

`crates/jazz-tools/src/schema_manager/sql.rs`

- parser tests for `CONTAINS` and `IN (...)` policy clauses.
- round-trip generation tests for new variants.

Representative integration test snippet:

```rust
// policy: tags CONTAINS "admin" AND status IN ("active", "trial")
let policy = PolicyExpr::And(vec![
    PolicyExpr::Contains {
        column: "tags".into(),
        value: PolicyValue::Literal(Value::Text("admin".into())),
    },
    PolicyExpr::InList {
        column: "status".into(),
        values: vec![
            PolicyValue::Literal(Value::Text("active".into())),
            PolicyValue::Literal(Value::Text("trial".into())),
        ],
    },
]);
assert!(evaluate_policy_expr(&policy, &row_content, &descriptor, &session));
```

## Ambiguities And Assumptions

### Semantics

- Assumption: policy `in` should support both literal arrays and session-array refs (`@session.*`) because policy DSL already supports session-driven comparisons.
  Impact: keeps existing `PolicyExpr::In` useful while adding literal-list parity.

- Assumption: `in: []` should deterministically evaluate false.
  Impact: avoids emitting invalid `IN ()` SQL and matches expected "no candidate values" behavior.

### Scope

- Assumption: parity target is the generated query `WhereInput` operator set (not ad-hoc internal `Condition::Between`).
  Impact: no extra policy operator is introduced beyond `contains`/`in`.
