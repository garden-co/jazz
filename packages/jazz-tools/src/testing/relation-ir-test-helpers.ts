function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getVariantEntry(value: unknown): [string, unknown] | null {
  if (!isRecord(value)) return null;
  const entries = Object.entries(value);
  if (entries.length !== 1) return null;
  return entries[0] ?? null;
}

function toLegacyValueForTest(value: unknown): unknown {
  if (!isRecord(value) || typeof value.type !== "string") {
    return value;
  }

  const variant = value.type;
  if (variant === "Null") {
    return { Null: null };
  }

  const payload = value.value;
  if (variant === "Array" && Array.isArray(payload)) {
    return { Array: payload.map(toLegacyValueForTest) };
  }
  if (variant === "Row" && Array.isArray(payload)) {
    return { Row: payload.map(toLegacyValueForTest) };
  }
  if (variant === "Bytea" && payload instanceof Uint8Array) {
    return { Bytea: [...payload] };
  }
  return { [variant]: payload };
}

function toLegacyRelValueRefForTest(value: unknown): unknown {
  const entry = getVariantEntry(value);
  if (!entry) return value;

  const [variant, payload] = entry;
  switch (variant) {
    case "Literal":
      return { type: "Literal", value: toLegacyValueForTest(payload) };
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

function toLegacyRelPredicateForTest(value: unknown): unknown {
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
          ? payload.map((item) => toLegacyRelPredicateForTest(item))
          : [],
      };
    }
    if (variant === "Not") {
      return { type: "Not", expr: toLegacyRelPredicateForTest(payload) };
    }
    return value;
  }

  switch (variant) {
    case "Cmp":
      return {
        type: "Cmp",
        left: payload.left,
        op: payload.op,
        right: toLegacyRelValueRefForTest(payload.right),
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
          ? payload.values.map((item) => toLegacyRelValueRefForTest(item))
          : [],
      };
    case "Contains":
      return {
        type: "Contains",
        left: payload.left,
        value: toLegacyRelValueRefForTest(payload.right),
      };
    case "And":
      return {
        type: "And",
        exprs: Array.isArray(payload)
          ? payload.map((item) => toLegacyRelPredicateForTest(item))
          : [],
      };
    case "Or":
      return {
        type: "Or",
        exprs: Array.isArray(payload)
          ? payload.map((item) => toLegacyRelPredicateForTest(item))
          : [],
      };
    case "Not":
      return {
        type: "Not",
        expr: toLegacyRelPredicateForTest(payload),
      };
    default:
      return value;
  }
}

function toLegacyRelKeyRefForTest(value: unknown): unknown {
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

function toLegacyRelProjectExprForTest(value: unknown): unknown {
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

export function toLegacyRelExprForTest(value: unknown): any {
  const entry = getVariantEntry(value);
  if (!entry) return value;

  const [variant, payload] = entry;
  if (!isRecord(payload)) {
    return value;
  }

  switch (variant) {
    case "TableScan":
      return { type: "TableScan", table: payload.table };
    case "Filter":
      return {
        type: "Filter",
        input: toLegacyRelExprForTest(payload.input),
        predicate: toLegacyRelPredicateForTest(payload.predicate),
      };
    case "Join":
      return {
        type: "Join",
        left: toLegacyRelExprForTest(payload.left),
        right: toLegacyRelExprForTest(payload.right),
        on: payload.on,
        joinKind: payload.join_kind,
      };
    case "Project":
      return {
        type: "Project",
        input: toLegacyRelExprForTest(payload.input),
        columns: Array.isArray(payload.columns)
          ? payload.columns.map((column) => ({
              alias: isRecord(column) ? column.alias : undefined,
              expr: toLegacyRelProjectExprForTest(isRecord(column) ? column.expr : undefined),
            }))
          : [],
      };
    case "Gather":
      return {
        type: "Gather",
        seed: toLegacyRelExprForTest(payload.seed),
        step: toLegacyRelExprForTest(payload.step),
        frontierKey: toLegacyRelKeyRefForTest(payload.frontier_key),
        maxDepth: payload.max_depth,
        dedupeKey: Array.isArray(payload.dedupe_key)
          ? payload.dedupe_key.map((key) => toLegacyRelKeyRefForTest(key))
          : [],
      };
    case "Distinct":
      return {
        type: "Distinct",
        input: toLegacyRelExprForTest(payload.input),
        key: Array.isArray(payload.key)
          ? payload.key.map((key) => toLegacyRelKeyRefForTest(key))
          : [],
      };
    case "OrderBy":
      return {
        type: "OrderBy",
        input: toLegacyRelExprForTest(payload.input),
        terms: payload.terms,
      };
    case "Offset":
      return {
        type: "Offset",
        input: toLegacyRelExprForTest(payload.input),
        offset: payload.offset,
      };
    case "Limit":
      return {
        type: "Limit",
        input: toLegacyRelExprForTest(payload.input),
        limit: payload.limit,
      };
    default:
      return value;
  }
}

export function toLegacyPolicyExprWithRelForTest(value: unknown): any {
  if (!isRecord(value) || typeof value.type !== "string") {
    return value;
  }

  if (value.type === "ExistsRel") {
    return {
      ...value,
      rel: toLegacyRelExprForTest(value.rel),
    };
  }

  if (value.type === "And" || value.type === "Or") {
    return {
      ...value,
      exprs: Array.isArray(value.exprs)
        ? value.exprs.map((expr) => toLegacyPolicyExprWithRelForTest(expr))
        : [],
    };
  }

  if (value.type === "Not") {
    return {
      ...value,
      expr: toLegacyPolicyExprWithRelForTest(value.expr),
    };
  }

  if (value.type === "Exists") {
    return {
      ...value,
      condition: toLegacyPolicyExprWithRelForTest(value.condition),
    };
  }

  return value;
}
