function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getVariantEntry(value: unknown): [string, unknown] | null {
  if (!isRecord(value)) return null;
  const entries = Object.entries(value);
  if (entries.length !== 1) return null;
  return entries[0] ?? null;
}

function toAssertionValueForTest(value: unknown): unknown {
  if (!isRecord(value) || typeof value.type !== "string") {
    return value;
  }

  const variant = value.type;
  if (variant === "Null") {
    return { Null: null };
  }

  const payload = value.value;
  if (variant === "Array" && Array.isArray(payload)) {
    return { Array: payload.map(toAssertionValueForTest) };
  }
  if (variant === "Row" && Array.isArray(payload)) {
    return { Row: payload.map(toAssertionValueForTest) };
  }
  if (variant === "Bytea" && payload instanceof Uint8Array) {
    return { Bytea: [...payload] };
  }
  return { [variant]: payload };
}

function toAssertionRelValueRefForTest(value: unknown): unknown {
  const entry = getVariantEntry(value);
  if (!entry) return value;

  const [variant, payload] = entry;
  switch (variant) {
    case "Literal":
      return { type: "Literal", value: toAssertionValueForTest(payload) };
    case "SessionRef":
      return { type: "SessionRef", path: payload };
    case "OuterColumn":
      return { type: "OuterColumn", column: payload };
    case "FrontierColumn":
      return { type: "FrontierColumn", column: payload };
    case "RowId":
      return { type: "RowId", source: payload };
    default:
      return value;
  }
}

function toAssertionRelPredicateForTest(value: unknown): unknown {
  if (value === "True" || value === "False") {
    return { type: value };
  }

  const entry = getVariantEntry(value);
  if (!entry) return value;

  const [variant, payload] = entry;
  if (!isRecord(payload)) {
    if (variant === "And" || variant === "Or") {
      return {
        type: variant,
        exprs: Array.isArray(payload)
          ? payload.map((item) => toAssertionRelPredicateForTest(item))
          : [],
      };
    }
    if (variant === "Not") {
      return { type: "Not", expr: toAssertionRelPredicateForTest(payload) };
    }
    return value;
  }

  switch (variant) {
    case "Cmp":
      return {
        type: "Cmp",
        left: payload.left,
        op: payload.op,
        right: toAssertionRelValueRefForTest(payload.right),
      };
    case "IsNull":
      return { type: "IsNull", column: payload.column };
    case "IsNotNull":
      return { type: "IsNotNull", column: payload.column };
    case "In":
      return {
        type: "In",
        left: payload.left,
        values: Array.isArray(payload.values)
          ? payload.values.map((item) => toAssertionRelValueRefForTest(item))
          : [],
      };
    case "Contains":
      return {
        type: "Contains",
        left: payload.left,
        value: toAssertionRelValueRefForTest(payload.right),
      };
    case "And":
      return {
        type: "And",
        exprs: Array.isArray(payload)
          ? payload.map((item) => toAssertionRelPredicateForTest(item))
          : [],
      };
    case "Or":
      return {
        type: "Or",
        exprs: Array.isArray(payload)
          ? payload.map((item) => toAssertionRelPredicateForTest(item))
          : [],
      };
    case "Not":
      return {
        type: "Not",
        expr: toAssertionRelPredicateForTest(payload),
      };
    default:
      return value;
  }
}

function toAssertionRelKeyRefForTest(value: unknown): unknown {
  const entry = getVariantEntry(value);
  if (!entry) return value;
  const [variant, payload] = entry;
  if (variant === "Column") {
    return { type: "Column", column: payload };
  }
  if (variant === "RowId") {
    return { type: "RowId", source: payload };
  }
  return value;
}

function toAssertionRelProjectExprForTest(value: unknown): unknown {
  const entry = getVariantEntry(value);
  if (!entry) return value;
  const [variant, payload] = entry;
  if (variant === "Column") {
    return { type: "Column", column: payload };
  }
  if (variant === "RowId") {
    return { type: "RowId", source: payload };
  }
  return value;
}

export function toAssertionRelExprForTest(value: unknown): any {
  const entry = getVariantEntry(value);
  if (!entry) return value;

  const [variant, payload] = entry;
  if (!isRecord(payload)) {
    return value;
  }

  switch (variant) {
    case "TableScan":
      return { type: "TableScan", table: payload.table, alias: payload.alias };
    case "Filter":
      return {
        type: "Filter",
        input: toAssertionRelExprForTest(payload.input),
        predicate: toAssertionRelPredicateForTest(payload.predicate),
      };
    case "Union":
      return {
        type: "Union",
        inputs: Array.isArray(payload.inputs)
          ? payload.inputs.map((input) => toAssertionRelExprForTest(input))
          : [],
      };
    case "Join":
      return {
        type: "Join",
        left: toAssertionRelExprForTest(payload.left),
        right: toAssertionRelExprForTest(payload.right),
        on: payload.on,
        joinKind: payload.join_kind,
      };
    case "Project":
      return {
        type: "Project",
        input: toAssertionRelExprForTest(payload.input),
        columns: Array.isArray(payload.columns)
          ? payload.columns.map((column) => ({
              alias: isRecord(column) ? column.alias : undefined,
              expr: toAssertionRelProjectExprForTest(isRecord(column) ? column.expr : undefined),
            }))
          : [],
      };
    case "Gather":
      return {
        type: "Gather",
        seed: toAssertionRelExprForTest(payload.seed),
        step: toAssertionRelExprForTest(payload.step),
        frontierKey: toAssertionRelKeyRefForTest(payload.frontier_key),
        maxDepth: readMaxDepthForTest(payload),
        dedupeKey: Array.isArray(payload.dedupe_key)
          ? payload.dedupe_key.map((key) => toAssertionRelKeyRefForTest(key))
          : [],
      };
    case "Distinct":
      return {
        type: "Distinct",
        input: toAssertionRelExprForTest(payload.input),
        key: Array.isArray(payload.key)
          ? payload.key.map((key) => toAssertionRelKeyRefForTest(key))
          : [],
      };
    case "OrderBy":
      return {
        type: "OrderBy",
        input: toAssertionRelExprForTest(payload.input),
        terms: payload.terms,
      };
    case "Offset":
      return {
        type: "Offset",
        input: toAssertionRelExprForTest(payload.input),
        offset: payload.offset,
      };
    case "Limit":
      return {
        type: "Limit",
        input: toAssertionRelExprForTest(payload.input),
        limit: payload.limit,
      };
    default:
      return value;
  }
}

function readMaxDepthForTest(payload: Record<string, unknown>): unknown {
  const bound = payload.bound;
  if (isRecord(bound) && typeof bound.MaxDepth === "number") return bound.MaxDepth;
  return undefined;
}

export function toAssertionPolicyExprWithRelForTest(value: unknown): any {
  if (!isRecord(value) || typeof value.type !== "string") {
    return value;
  }

  if (value.type === "ExistsRel") {
    return {
      ...value,
      rel: toAssertionRelExprForTest(value.rel),
    };
  }

  if (value.type === "And" || value.type === "Or") {
    return {
      ...value,
      exprs: Array.isArray(value.exprs)
        ? value.exprs.map((expr) => toAssertionPolicyExprWithRelForTest(expr))
        : [],
    };
  }

  if (value.type === "Not") {
    return {
      ...value,
      expr: toAssertionPolicyExprWithRelForTest(value.expr),
    };
  }

  if (value.type === "Exists") {
    return {
      ...value,
      condition: toAssertionPolicyExprWithRelForTest(value.condition),
    };
  }

  return value;
}
