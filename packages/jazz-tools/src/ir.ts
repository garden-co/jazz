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

/** A numeric lexeme retained only while adapting raw native query JSON to Postcard. */
export class RawJsonNumber {
  constructor(readonly text: string) {}
}

function assertNoUnpairedSurrogates(value: string): void {
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      if (
        ++index >= value.length ||
        value.charCodeAt(index) < 0xdc00 ||
        value.charCodeAt(index) > 0xdfff
      )
        throw new Error("invalid relation query: unpaired surrogate");
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new Error("invalid relation query: unpaired surrogate");
    }
  }
}

/** Parse raw query JSON without losing numeric token spelling. */
export function parseRelationQueryJsonLossless(queryJson: string): unknown {
  const validated = JSON.parse(queryJson) as unknown;
  const containsMarker = (value: unknown, marker: string): boolean => {
    if (typeof value === "string") {
      assertNoUnpairedSurrogates(value);
      return value.includes(marker);
    }
    if (Array.isArray(value)) return value.some((child) => containsMarker(child, marker));
    if (value && typeof value === "object")
      return Object.entries(value).some(([key, child]) => {
        assertNoUnpairedSurrogates(key);
        return key.includes(marker) || containsMarker(child, marker);
      });
    return false;
  };
  let marker = "__jrq_raw_number_";
  while (queryJson.includes(marker) || containsMarker(validated, marker)) marker = `_${marker}`;
  const numbers: string[] = [];
  let rewritten = "";
  let string = false;
  let escaped = false;
  for (let index = 0; index < queryJson.length; index++) {
    const char = queryJson[index]!;
    if (string) {
      rewritten += char;
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') string = false;
      continue;
    }
    if (char === '"') {
      string = true;
      rewritten += char;
      continue;
    }
    if (char === "-" || (char >= "0" && char <= "9")) {
      const match = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(queryJson.slice(index));
      if (match) {
        numbers.push(match[0]);
        rewritten += JSON.stringify(`${marker}${numbers.length - 1}__`);
        index += match[0].length - 1;
        continue;
      }
    }
    rewritten += char;
  }
  const revive = (value: unknown): unknown => {
    if (typeof value === "string") {
      const match = new RegExp(`^${marker}(\\d+)__$`).exec(value);
      return match ? new RawJsonNumber(numbers[Number(match[1])]!) : value;
    }
    if (Array.isArray(value)) return value.map(revive);
    if (value && typeof value === "object")
      return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, revive(child)]));
    return value;
  };
  return revive(JSON.parse(rewritten));
}

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

/** Encode the typed Postcard relation tree used by native and peer query envelopes. */
export function encodeRelationQueryPostcard(relation: RelExpr): Uint8Array {
  const MAX_BYTES = 1 << 20;
  const MAX_ITEMS = 4096;
  const MAX_STRING_BYTES = 1 << 16;
  const MAX_DEPTH = 128;
  const bytes: number[] = [];
  let depth = 0;
  let nodes = 0;
  const text = new TextEncoder();
  const fail = (message: string): never => {
    throw new Error(`invalid relation Postcard: ${message}`);
  };
  const push = (...values: number[]) => {
    if (values.some((value) => !Number.isInteger(value) || value < 0 || value > 0xff)) fail("byte");
    if (bytes.length > MAX_BYTES - values.length) fail("byte limit");
    bytes.push(...values);
  };
  const collection = (length: number) => {
    if (!Number.isSafeInteger(length) || length < 0 || length > MAX_ITEMS) fail("collection");
    u64(length);
  };
  const node = () => {
    if (++nodes > MAX_ITEMS) fail("node limit");
  };
  const nested = (write: () => void) => {
    if (++depth > MAX_DEPTH) fail("depth limit");
    try {
      write();
    } finally {
      depth--;
    }
  };
  const u64 = (value: bigint | number) => {
    let remaining = typeof value === "bigint" ? value : BigInt(value);
    if (remaining < 0n || remaining > 0xffff_ffff_ffff_ffffn) fail("integer range");
    do {
      let byte = Number(remaining & 0x7fn);
      remaining >>= 7n;
      if (remaining) byte |= 0x80;
      push(byte);
    } while (remaining);
  };
  const i64 = (value: bigint) => u64(value < 0n ? (-value << 1n) - 1n : value << 1n);
  const string = (value: string) => {
    if (typeof value !== "string") fail("string");
    assertNoUnpairedSurrogates(value);
    const encoded = text.encode(value);
    if (encoded.length > MAX_STRING_BYTES) fail("string limit");
    u64(encoded.length);
    push(...encoded);
  };
  const option = (value: unknown, write: () => void) => {
    if (value === undefined || value === null) push(0);
    else {
      push(1);
      write();
    }
  };
  const tagged = (value: object, tags: string[], kind: string) => {
    const keys = Object.keys(value);
    if (keys.length !== 1 || !tags.includes(keys[0]!)) fail(kind);
  };
  const dimension = (value: unknown) => {
    if (value instanceof RawJsonNumber) {
      if (!/^(?:0|[1-9]\d*)$/.test(value.text)) fail("dimension");
      value = Number(value.text);
    }
    const dimensionValue = typeof value === "number" ? value : fail("dimension");
    if (!Number.isSafeInteger(dimensionValue) || dimensionValue < 0 || dimensionValue > 0xffff_ffff)
      fail("dimension");
    u64(dimensionValue);
  };
  const column = (value: RelColumnRef) => {
    if (
      !value ||
      typeof value !== "object" ||
      typeof value.column !== "string" ||
      (value.scope !== undefined && typeof value.scope !== "string")
    )
      fail("column");
    option(value.scope, () => string(value.scope!));
    string(value.column);
  };
  const rowId = (value: RelRowIdRef) =>
    u64(
      value === "Current" ? 0 : value === "Outer" ? 1 : value === "Frontier" ? 2 : fail("row id"),
    );
  const json = (value: unknown): void => {
    node();
    if (value instanceof RawJsonNumber) {
      if (/^(?:0|[1-9]\d*|-[1-9]\d*)$/.test(value.text)) {
        const integer = BigInt(value.text);
        if (integer >= -0x8000_0000_0000_0000n && integer <= 0x7fff_ffff_ffff_ffffn) {
          u64(2);
          i64(integer);
          return;
        }
        if (integer >= 0n && integer <= 0xffff_ffff_ffff_ffffn) {
          u64(3);
          u64(integer);
          return;
        }
      }
      const number = Number(value.text);
      if (!Number.isFinite(number)) fail("number");
      // A decimal/exponent spelling is a JSON floating-point number even when
      // its JavaScript value happens to be integral. This is required for the
      // raw subscription API, where Rust serde_json preserves `1.0`, `1e0`,
      // and `-0` as floating-point values.
      u64(4);
      const raw = new DataView(new ArrayBuffer(8));
      raw.setFloat64(0, number, true);
      u64(raw.getBigUint64(0, true));
      return;
    }
    if (value === null) {
      u64(0);
      return;
    }
    if (typeof value === "boolean") {
      u64(1);
      push(value ? 1 : 0);
      return;
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value)) fail("number");
      if (Number.isInteger(value) && !Object.is(value, -0)) {
        const decimal = JSON.stringify(value);
        if (/^(?:0|[1-9]\d*|-[1-9]\d*)$/.test(decimal)) {
          const integer = BigInt(decimal);
          if (integer >= -0x8000_0000_0000_0000n && integer <= 0x7fff_ffff_ffff_ffffn) {
            u64(2);
            i64(integer);
            return;
          }
          if (integer >= 0n && integer <= 0xffff_ffff_ffff_ffffn) {
            u64(3);
            u64(integer);
            return;
          }
        }
      }
      u64(4);
      const raw = new DataView(new ArrayBuffer(8));
      raw.setFloat64(0, value, true);
      u64(raw.getBigUint64(0, true));
      return;
    }
    if (typeof value === "string") {
      u64(5);
      string(value);
      return;
    }
    if (Array.isArray(value)) {
      u64(6);
      collection(value.length);
      value.forEach((child) => nested(() => json(child)));
      return;
    }
    if (!value || typeof value !== "object") fail("literal");
    u64(7);
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) => {
      const a = text.encode(left);
      const b = text.encode(right);
      for (let i = 0; i < Math.min(a.length, b.length); i++)
        if (a[i] !== b[i]) return a[i]! - b[i]!;
      return a.length - b.length;
    });
    collection(entries.length);
    for (const [key, child] of entries) {
      string(key);
      nested(() => json(child));
    }
  };
  const key = (value: RelKeyRef) => {
    node();
    tagged(value, ["Column", "RowId"], "key");
    if ("Column" in value) {
      u64(0);
      column(value.Column);
    } else {
      u64(1);
      rowId(value.RowId);
    }
  };
  const project = (value: RelProjectExpr) => {
    node();
    tagged(value, ["Column", "RowId"], "project");
    if ("Column" in value) {
      u64(0);
      column(value.Column);
    } else {
      u64(1);
      rowId(value.RowId);
    }
  };
  const valueRef = (value: RelValueRef) => {
    node();
    tagged(
      value,
      ["Literal", "Param", "SessionRef", "OuterColumn", "FrontierColumn", "RowId"],
      "value",
    );
    if ("Literal" in value) {
      u64(0);
      json(value.Literal);
    } else if ("Param" in value) {
      u64(1);
      string(value.Param);
    } else if ("SessionRef" in value) {
      u64(2);
      collection(value.SessionRef.length);
      value.SessionRef.forEach(string);
    } else if ("OuterColumn" in value) {
      u64(3);
      column(value.OuterColumn);
    } else if ("FrontierColumn" in value) {
      u64(4);
      column(value.FrontierColumn);
    } else {
      u64(5);
      rowId(value.RowId);
    }
  };
  const predicate = (value: RelPredicateExpr): void => {
    node();
    if (value === "True") {
      u64(9);
      return;
    }
    if (value === "False") {
      u64(10);
      return;
    }
    tagged(
      value,
      ["Cmp", "IsNull", "IsNotNull", "In", "Contains", "EnumMatch", "And", "Or", "Not"],
      "predicate",
    );
    if ("Cmp" in value) {
      u64(0);
      column(value.Cmp.left);
      const op = ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"].indexOf(value.Cmp.op);
      if (op < 0) fail("comparison operator");
      u64(op);
      nested(() => valueRef(value.Cmp.right));
    } else if ("IsNull" in value) {
      u64(1);
      column(value.IsNull.column);
    } else if ("IsNotNull" in value) {
      u64(2);
      column(value.IsNotNull.column);
    } else if ("In" in value) {
      u64(3);
      column(value.In.left);
      collection(value.In.values.length);
      value.In.values.forEach((child) => nested(() => valueRef(child)));
    } else if ("Contains" in value) {
      u64(4);
      column(value.Contains.left);
      nested(() => valueRef(value.Contains.right));
    } else if ("EnumMatch" in value) {
      u64(5);
      column(value.EnumMatch.column);
      string(value.EnumMatch.case);
      nested(() => predicate(value.EnumMatch.payload));
    } else if ("And" in value) {
      u64(6);
      collection(value.And.length);
      value.And.forEach((child) => nested(() => predicate(child)));
    } else if ("Or" in value) {
      u64(7);
      collection(value.Or.length);
      value.Or.forEach((child) => nested(() => predicate(child)));
    } else {
      u64(8);
      nested(() => predicate(value.Not));
    }
  };
  const expr = (value: RelExpr): void => {
    node();
    tagged(
      value,
      [
        "TableScan",
        "Filter",
        "Union",
        "Join",
        "Project",
        "Gather",
        "Distinct",
        "OrderBy",
        "Offset",
        "Limit",
      ],
      "expression",
    );
    if ("TableScan" in value) {
      u64(0);
      string(value.TableScan.table);
      option(value.TableScan.alias, () => string(value.TableScan.alias!));
    } else if ("Filter" in value) {
      u64(1);
      nested(() => expr(value.Filter.input));
      nested(() => predicate(value.Filter.predicate));
    } else if ("Union" in value) {
      u64(2);
      collection(value.Union.inputs.length);
      value.Union.inputs.forEach((arm) => {
        string(arm.label);
        nested(() => expr(arm.input));
      });
    } else if ("Join" in value) {
      u64(3);
      nested(() => expr(value.Join.left));
      nested(() => expr(value.Join.right));
      collection(value.Join.on.length);
      value.Join.on.forEach((on) => {
        node();
        column(on.left);
        column(on.right);
      });
      u64(value.Join.join_kind === "Inner" ? 0 : 1);
    } else if ("Project" in value) {
      u64(4);
      nested(() => expr(value.Project.input));
      collection(value.Project.columns.length);
      value.Project.columns.forEach((columnValue) => {
        string(columnValue.alias);
        project(columnValue.expr);
      });
    } else if ("Gather" in value) {
      u64(5);
      nested(() => expr(value.Gather.seed));
      nested(() => expr(value.Gather.step));
      key(value.Gather.frontier_key);
      if (value.Gather.bound === "Fixpoint") {
        u64(0);
      } else {
        u64(1);
        dimension(value.Gather.bound.MaxDepth);
      }
      collection(value.Gather.dedupe_key.length);
      value.Gather.dedupe_key.forEach(key);
    } else if ("Distinct" in value) {
      u64(6);
      nested(() => expr(value.Distinct.input));
      collection(value.Distinct.key.length);
      value.Distinct.key.forEach(key);
    } else if ("OrderBy" in value) {
      u64(7);
      nested(() => expr(value.OrderBy.input));
      collection(value.OrderBy.terms.length);
      value.OrderBy.terms.forEach((term) => {
        node();
        column(term.column);
        u64(term.direction === "Asc" ? 0 : 1);
      });
    } else if ("Offset" in value) {
      u64(8);
      nested(() => expr(value.Offset.input));
      dimension(value.Offset.offset);
    } else {
      u64(9);
      nested(() => expr(value.Limit.input));
      dimension(value.Limit.limit);
    }
  };
  expr(relation);
  return Uint8Array.from(bytes);
}
