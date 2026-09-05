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

/** A numeric lexeme retained only while adapting raw native query JSON to JRQ. */
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
        throw new Error("invalid JRQ: unpaired surrogate");
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new Error("invalid JRQ: unpaired surrogate");
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

/** Encode the Rust-owned closed JRQ v1 relation grammar. */
export function encodeRelationQueryV1(relation: RelExpr): Uint8Array {
  const maxBytes = 1 << 20;
  class JBytes extends Array<number> {
    override push(...items: number[]): number {
      if (this.length + items.length > maxBytes) throw new Error("invalid JRQ: byte limit");
      return super.push(...items);
    }
  }
  const bytes = new JBytes();
  bytes.push(0x4a, 0x52, 0x51, 0x01);
  const text = new TextEncoder();
  const maxDepth = 128;
  const maxItems = 4096;
  const maxString = 1 << 16;
  let nodes = 0;
  let stringBytes = 0;
  const fail = (message: string): never => {
    throw new Error(`invalid JRQ: ${message}`);
  };
  const room = (additional: number) => {
    if (!Number.isSafeInteger(additional) || additional < 0 || bytes.length + additional > maxBytes)
      fail("byte limit");
  };
  const node = (depth: number) => {
    if (depth >= maxDepth || ++nodes > maxItems) fail("tree limit");
  };
  const length = (value: number) => {
    if (!Number.isSafeInteger(value) || value < 0) fail("length");
    do {
      let byte = value % 128;
      value = Math.floor(value / 128);
      if (value) byte += 128;
      bytes.push(byte);
    } while (value);
  };
  const count = (value: number) => {
    if (!Number.isSafeInteger(value) || value < 0 || value > maxItems) fail("collection limit");
    length(value);
  };
  const dimension = (value: unknown) => {
    if (value instanceof RawJsonNumber) {
      if (!/^(?:0|[1-9]\d*|-[1-9]\d*)$/.test(value.text)) fail("dimension");
      value = Number(value.text);
    }
    const dimensionValue = typeof value === "number" ? value : fail("dimension");
    if (!Number.isSafeInteger(dimensionValue) || dimensionValue < 0 || dimensionValue > 0xffff_ffff)
      fail("dimension");
    length(dimensionValue);
  };
  const string = (value: string) => {
    if (typeof value !== "string") fail("string");
    assertNoUnpairedSurrogates(value);
    const encoded = text.encode(value);
    if (encoded.length > maxString || (stringBytes += encoded.length) > maxBytes)
      fail("string limit");
    length(encoded.length);
    bytes.push(...encoded);
  };
  const label = (value: string) => {
    const encoded = text.encode(value);
    if (!encoded.length || encoded.length > 4096 || encoded.includes(0)) fail("union label");
    string(value);
  };
  const unsigned = (value: bigint) => {
    if (value < 0n || value > 0xffff_ffff_ffff_ffffn) fail("integer range");
    do {
      let byte = Number(value & 0x7fn);
      value >>= 7n;
      if (value) byte |= 0x80;
      bytes.push(byte);
    } while (value);
  };
  const signed = (value: bigint) => unsigned((value << 1n) ^ (value >> 63n));
  const tagged = (value: object, tags: string[], kind: string) => {
    const keys = Object.keys(value);
    if (keys.length !== 1 || !tags.includes(keys[0]!)) fail(kind);
  };
  const column = (value: RelColumnRef) => {
    if (
      !value ||
      typeof value !== "object" ||
      Object.keys(value).some((key) => key !== "scope" && key !== "column") ||
      typeof value.column !== "string" ||
      (value.scope !== undefined && typeof value.scope !== "string")
    )
      fail("column");
    if (value.scope === undefined) bytes.push(0);
    else {
      bytes.push(1);
      string(value.scope);
    }
    string(value.column);
  };
  const rowId = (value: RelRowIdRef) =>
    bytes.push(
      value === "Current" ? 0 : value === "Outer" ? 1 : value === "Frontier" ? 2 : fail("row id"),
    );
  const key = (value: RelKeyRef) => {
    tagged(value, ["Column", "RowId"], "key");
    if ("Column" in value) {
      bytes.push(0);
      column(value.Column);
    } else {
      bytes.push(1);
      rowId(value.RowId);
    }
  };
  const project = (value: RelProjectExpr) => {
    tagged(value, ["Column", "RowId"], "project expression");
    if ("Column" in value) {
      bytes.push(0);
      column(value.Column);
    } else {
      bytes.push(1);
      rowId(value.RowId);
    }
  };
  const json = (value: unknown, depth: number): void => {
    node(depth);
    if (value instanceof RawJsonNumber) {
      if (/^(?:0|[1-9]\d*|-[1-9]\d*)$/.test(value.text)) {
        const integer = BigInt(value.text);
        if (integer >= -0x8000_0000_0000_0000n && integer <= 0x7fff_ffff_ffff_ffffn) {
          bytes.push(3);
          signed(integer);
          return;
        }
        if (integer >= 0n && integer <= 0xffff_ffff_ffff_ffffn) {
          bytes.push(4);
          unsigned(integer);
          return;
        }
      }
      const number = Number(value.text);
      if (!Number.isFinite(number)) fail("number");
      bytes.push(5);
      const raw = new DataView(new ArrayBuffer(8));
      raw.setFloat64(0, number, true);
      for (let index = 0; index < 8; index++) bytes.push(raw.getUint8(index));
      return;
    }
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
      if (!Number.isFinite(value)) fail("number");
      // Preserve the public JSON decimal normalization for unsafe integers.
      // `BigInt(value)` uses the binary approximation, which can differ from
      // JSON.stringify (for example, 2**63), so parse that decimal instead.
      if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
        const decimal = JSON.stringify(value);
        if (/^-?(?:0|[1-9]\d*)$/.test(decimal)) {
          const normalized = BigInt(decimal);
          if (normalized >= -0x8000_0000_0000_0000n && normalized <= 0x7fff_ffff_ffff_ffffn) {
            bytes.push(3);
            signed(normalized);
            return;
          }
          if (normalized >= 0n && normalized <= 0xffff_ffff_ffff_ffffn) {
            bytes.push(4);
            unsigned(normalized);
            return;
          }
        }
      }
      if (Number.isInteger(value) && !Object.is(value, -0)) {
        const integer = BigInt(value);
        if (integer >= -0x8000_0000_0000_0000n && integer <= 0x7fff_ffff_ffff_ffffn) {
          bytes.push(3);
          signed(integer);
          return;
        }
        if (integer >= 0n && integer <= 0xffff_ffff_ffff_ffffn) {
          bytes.push(4);
          unsigned(integer);
          return;
        }
      }
      bytes.push(5);
      const raw = new DataView(new ArrayBuffer(8));
      raw.setFloat64(0, value, true);
      for (let index = 0; index < 8; index++) bytes.push(raw.getUint8(index));
      return;
    }
    if (typeof value === "string") {
      bytes.push(6);
      string(value);
      return;
    }
    if (Array.isArray(value)) {
      bytes.push(7);
      count(value.length);
      value.forEach((child) => json(child, depth + 1));
      return;
    }
    if (value && typeof value === "object") {
      bytes.push(8);
      const entries = Object.entries(value as Record<string, unknown>).map(
        ([key, child]) => [text.encode(key), key, child] as const,
      );
      entries.sort(([a], [b]) => {
        for (let index = 0; index < Math.min(a.length, b.length); index++)
          if (a[index] !== b[index]) return a[index]! - b[index]!;
        return a.length - b.length;
      });
      count(entries.length);
      for (const [, key, child] of entries) {
        string(key);
        json(child, depth + 1);
      }
      return;
    }
    fail("literal type");
  };
  const value = (input: RelValueRef, depth: number): void => {
    node(depth);
    tagged(
      input,
      ["Literal", "Param", "SessionRef", "OuterColumn", "FrontierColumn", "RowId"],
      "value",
    );
    if ("Literal" in input) {
      bytes.push(0);
      json(input.Literal, depth + 1);
    } else if ("Param" in input) {
      bytes.push(1);
      string(input.Param);
    } else if ("SessionRef" in input) {
      bytes.push(2);
      count(input.SessionRef.length);
      input.SessionRef.forEach(string);
    } else if ("OuterColumn" in input) {
      bytes.push(3);
      column(input.OuterColumn);
    } else if ("FrontierColumn" in input) {
      bytes.push(4);
      column(input.FrontierColumn);
    } else {
      bytes.push(5);
      rowId(input.RowId);
    }
  };
  const predicate = (input: RelPredicateExpr, depth: number): void => {
    node(depth);
    if (input === "True") {
      bytes.push(9);
      return;
    }
    if (input === "False") {
      bytes.push(10);
      return;
    }
    tagged(
      input,
      ["Cmp", "IsNull", "IsNotNull", "In", "Contains", "EnumMatch", "And", "Or", "Not"],
      "predicate",
    );
    if ("Cmp" in input) {
      bytes.push(0);
      column(input.Cmp.left);
      const op = ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"].indexOf(input.Cmp.op);
      if (op < 0) fail("comparison");
      bytes.push(op);
      value(input.Cmp.right, depth + 1);
    } else if ("IsNull" in input) {
      bytes.push(1);
      column(input.IsNull.column);
    } else if ("IsNotNull" in input) {
      bytes.push(2);
      column(input.IsNotNull.column);
    } else if ("In" in input) {
      bytes.push(3);
      column(input.In.left);
      count(input.In.values.length);
      input.In.values.forEach((item) => value(item, depth + 1));
    } else if ("Contains" in input) {
      bytes.push(4);
      column(input.Contains.left);
      value(input.Contains.right, depth + 1);
    } else if ("EnumMatch" in input) {
      bytes.push(5);
      column(input.EnumMatch.column);
      string(input.EnumMatch.case);
      predicate(input.EnumMatch.payload, depth + 1);
    } else if ("And" in input) {
      bytes.push(6);
      count(input.And.length);
      input.And.forEach((item) => predicate(item, depth + 1));
    } else if ("Or" in input) {
      bytes.push(7);
      count(input.Or.length);
      input.Or.forEach((item) => predicate(item, depth + 1));
    } else {
      bytes.push(8);
      predicate(input.Not, depth + 1);
    }
  };
  const expr = (input: RelExpr, depth: number): void => {
    node(depth);
    tagged(
      input,
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
    if ("TableScan" in input) {
      bytes.push(0);
      string(input.TableScan.table);
      if (input.TableScan.alias === undefined) bytes.push(0);
      else {
        bytes.push(1);
        string(input.TableScan.alias);
      }
    } else if ("Filter" in input) {
      bytes.push(1);
      expr(input.Filter.input, depth + 1);
      predicate(input.Filter.predicate, depth + 1);
    } else if ("Union" in input) {
      bytes.push(2);
      count(input.Union.inputs.length);
      const labels = new Set<string>();
      input.Union.inputs.forEach((arm) => {
        label(arm.label);
        if (labels.has(arm.label)) fail("duplicate union label");
        labels.add(arm.label);
        expr(arm.input, depth + 1);
      });
    } else if ("Join" in input) {
      bytes.push(3);
      expr(input.Join.left, depth + 1);
      expr(input.Join.right, depth + 1);
      bytes.push(
        input.Join.join_kind === "Inner"
          ? 0
          : input.Join.join_kind === "Left"
            ? 1
            : fail("join kind"),
      );
      count(input.Join.on.length);
      input.Join.on.forEach((condition) => {
        column(condition.left);
        column(condition.right);
      });
    } else if ("Project" in input) {
      bytes.push(4);
      expr(input.Project.input, depth + 1);
      count(input.Project.columns.length);
      room(input.Project.columns.length * 3);
      input.Project.columns.forEach((item) => {
        string(item.alias);
        project(item.expr);
      });
    } else if ("Gather" in input) {
      bytes.push(5);
      expr(input.Gather.seed, depth + 1);
      expr(input.Gather.step, depth + 1);
      key(input.Gather.frontier_key);
      if (input.Gather.bound === "Fixpoint") bytes.push(0);
      else {
        bytes.push(1);
        dimension(input.Gather.bound.MaxDepth);
      }
      count(input.Gather.dedupe_key.length);
      input.Gather.dedupe_key.forEach(key);
    } else if ("Distinct" in input) {
      bytes.push(6);
      expr(input.Distinct.input, depth + 1);
      count(input.Distinct.key.length);
      input.Distinct.key.forEach(key);
    } else if ("OrderBy" in input) {
      bytes.push(7);
      expr(input.OrderBy.input, depth + 1);
      count(input.OrderBy.terms.length);
      input.OrderBy.terms.forEach((term) => {
        column(term.column);
        bytes.push(
          term.direction === "Asc" ? 0 : term.direction === "Desc" ? 1 : fail("order direction"),
        );
      });
    } else if ("Offset" in input) {
      bytes.push(8);
      expr(input.Offset.input, depth + 1);
      dimension(input.Offset.offset);
    } else {
      bytes.push(9);
      expr(input.Limit.input, depth + 1);
      dimension(input.Limit.limit);
    }
  };
  expr(relation, 0);
  if (bytes.length > maxBytes) fail("byte limit");
  return Uint8Array.from(bytes);
}

/** Encode the typed Postcard relation tree used by native and peer query envelopes. */
export function encodeRelationQueryPostcard(relation: RelExpr): Uint8Array {
  const bytes: number[] = [];
  const text = new TextEncoder();
  const fail = (message: string): never => {
    throw new Error(`invalid relation Postcard: ${message}`);
  };
  const u64 = (value: bigint | number) => {
    let remaining = typeof value === "bigint" ? value : BigInt(value);
    if (remaining < 0n || remaining > 0xffff_ffff_ffff_ffffn) fail("integer range");
    do {
      let byte = Number(remaining & 0x7fn);
      remaining >>= 7n;
      if (remaining) byte |= 0x80;
      bytes.push(byte);
    } while (remaining);
  };
  const i64 = (value: bigint) => u64(value < 0n ? (-value << 1n) - 1n : value << 1n);
  const string = (value: string) => {
    if (typeof value !== "string") fail("string");
    assertNoUnpairedSurrogates(value);
    const encoded = text.encode(value);
    u64(encoded.length);
    bytes.push(...encoded);
  };
  const option = (value: unknown, write: () => void) => {
    if (value === undefined || value === null) bytes.push(0);
    else {
      bytes.push(1);
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
      value = Number(value.text);
    }
    if (value === null) {
      u64(0);
      return;
    }
    if (typeof value === "boolean") {
      u64(1);
      bytes.push(value ? 1 : 0);
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
      for (let index = 0; index < 8; index++) bytes.push(raw.getUint8(index));
      return;
    }
    if (typeof value === "string") {
      u64(5);
      string(value);
      return;
    }
    if (Array.isArray(value)) {
      u64(6);
      u64(value.length);
      value.forEach(json);
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
    u64(entries.length);
    for (const [key, child] of entries) {
      string(key);
      json(child);
    }
  };
  const key = (value: RelKeyRef) => {
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
      u64(value.SessionRef.length);
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
      u64(["Eq", "Ne", "Lt", "Le", "Gt", "Ge"].indexOf(value.Cmp.op));
      valueRef(value.Cmp.right);
    } else if ("IsNull" in value) {
      u64(1);
      column(value.IsNull.column);
    } else if ("IsNotNull" in value) {
      u64(2);
      column(value.IsNotNull.column);
    } else if ("In" in value) {
      u64(3);
      column(value.In.left);
      u64(value.In.values.length);
      value.In.values.forEach(valueRef);
    } else if ("Contains" in value) {
      u64(4);
      column(value.Contains.left);
      valueRef(value.Contains.right);
    } else if ("EnumMatch" in value) {
      u64(5);
      column(value.EnumMatch.column);
      string(value.EnumMatch.case);
      predicate(value.EnumMatch.payload);
    } else if ("And" in value) {
      u64(6);
      u64(value.And.length);
      value.And.forEach(predicate);
    } else if ("Or" in value) {
      u64(7);
      u64(value.Or.length);
      value.Or.forEach(predicate);
    } else {
      u64(8);
      predicate(value.Not);
    }
  };
  const expr = (value: RelExpr): void => {
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
      expr(value.Filter.input);
      predicate(value.Filter.predicate);
    } else if ("Union" in value) {
      u64(2);
      u64(value.Union.inputs.length);
      value.Union.inputs.forEach((arm) => {
        string(arm.label);
        expr(arm.input);
      });
    } else if ("Join" in value) {
      u64(3);
      expr(value.Join.left);
      expr(value.Join.right);
      u64(value.Join.on.length);
      value.Join.on.forEach((on) => {
        column(on.left);
        column(on.right);
      });
      u64(value.Join.join_kind === "Inner" ? 0 : 1);
    } else if ("Project" in value) {
      u64(4);
      expr(value.Project.input);
      u64(value.Project.columns.length);
      value.Project.columns.forEach((columnValue) => {
        string(columnValue.alias);
        project(columnValue.expr);
      });
    } else if ("Gather" in value) {
      u64(5);
      expr(value.Gather.seed);
      expr(value.Gather.step);
      key(value.Gather.frontier_key);
      if (value.Gather.bound === "Fixpoint") {
        u64(0);
      } else {
        u64(1);
        dimension(value.Gather.bound.MaxDepth);
      }
      u64(value.Gather.dedupe_key.length);
      value.Gather.dedupe_key.forEach(key);
    } else if ("Distinct" in value) {
      u64(6);
      expr(value.Distinct.input);
      u64(value.Distinct.key.length);
      value.Distinct.key.forEach(key);
    } else if ("OrderBy" in value) {
      u64(7);
      expr(value.OrderBy.input);
      u64(value.OrderBy.terms.length);
      value.OrderBy.terms.forEach((term) => {
        column(term.column);
        u64(term.direction === "Asc" ? 0 : 1);
      });
    } else if ("Offset" in value) {
      u64(8);
      expr(value.Offset.input);
      dimension(value.Offset.offset);
    } else {
      u64(9);
      expr(value.Limit.input);
      dimension(value.Limit.limit);
    }
  };
  expr(relation);
  return Uint8Array.from(bytes);
}
