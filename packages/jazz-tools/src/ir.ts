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

/** Encode the Rust-owned closed JRQ v1 relation grammar. */
export function encodeRelationQueryV1(relation: RelExpr): Uint8Array {
  const bytes: number[] = [0x4a, 0x52, 0x51, 0x01];
  const text = new TextEncoder();
  const maxBytes = 1 << 20;
  const maxDepth = 128;
  const maxItems = 4096;
  const maxString = 1 << 16;
  let nodes = 0;
  let stringBytes = 0;
  const fail = (message: string): never => {
    throw new Error(`invalid JRQ: ${message}`);
  };
  const node = (depth: number) => {
    if (depth >= maxDepth || ++nodes > maxItems) fail("tree limit");
  };
  const length = (value: number) => {
    if (!Number.isSafeInteger(value) || value < 0) fail("length");
    do {
      let byte = value & 0x7f;
      value = Math.floor(value / 128);
      if (value) byte |= 0x80;
      bytes.push(byte);
    } while (value);
  };
  const count = (value: number) => {
    if (!Number.isSafeInteger(value) || value < 0 || value > maxItems) fail("collection limit");
    length(value);
  };
  const string = (value: string) => {
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
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) fail("integer range");
    length(Number(value));
  };
  const signed = (value: bigint) => unsigned((value << 1n) ^ (value >> 63n));
  const column = (value: RelColumnRef) => {
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
    if ("Column" in value) {
      bytes.push(0);
      column(value.Column);
    } else {
      bytes.push(1);
      rowId(value.RowId);
    }
  };
  const project = (value: RelProjectExpr) => {
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
      if (Number.isSafeInteger(value)) {
        if (value < 0) {
          bytes.push(3);
          signed(BigInt(value));
        } else {
          bytes.push(4);
          unsigned(BigInt(value));
        }
        return;
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
    if ("Cmp" in input) {
      bytes.push(0);
      column(input.Cmp.left);
      bytes.push(["Eq", "Ne", "Lt", "Le", "Gt", "Ge"].indexOf(input.Cmp.op));
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
        length(input.Gather.bound.MaxDepth);
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
      length(input.Offset.offset);
    } else {
      bytes.push(9);
      expr(input.Limit.input, depth + 1);
      length(input.Limit.limit);
    }
  };
  expr(relation, 0);
  if (bytes.length > maxBytes) fail("byte limit");
  return Uint8Array.from(bytes);
}
