/**
 * The relation IR, for `exists ( query )` (SPEC §10.10's `query`): the rare,
 * advanced form behind `exists.<t>(…).hop(…)`, for full parity with what
 * the engine holds (joins, select, distinct, order, union, recursive
 * walks). Column refs are strings (`"a.x"`), unchecked until Rust loads the
 * file.
 */
import { colref, handle, name, PermissionsDslError, text, type Ctx } from "./compile.js";
import type { ValueHandle } from "./types.js";

const QUERY: unique symbol = Symbol("jazz.permissions.query") as never;

/** A key: a column ref or `rowId("row" | "outer" | "frontier")`. */
export type RelationKey = string | RowIdRef;
export interface RowIdRef {
  readonly __jazzRowId: "row" | "outer" | "frontier";
}

type Part = (ctx: Ctx) => string;

/** A source a query reads: a table (`from.t`), a subquery, a union or a recursive walk. */
export interface RelationSource {
  readonly [QUERY]: { readonly source: Part };
}

function keyText(key: RelationKey): string {
  if (typeof key === "string") return colref(key);
  if (key && typeof key === "object" && "__jazzRowId" in key) return `id(${key.__jazzRowId})`;
  throw new PermissionsDslError(`${String(key)} is not a key: use "a.col" or rowId("row")`);
}

function sourceText(source: RelationSource | string, ctx: Ctx): string {
  if (typeof source === "string") return name(source);
  const data = (source as RelationSource)[QUERY];
  if (!data)
    throw new PermissionsDslError(
      "not a relation source: use from.<table>, a query, union(…) or recursive(…)",
    );
  return data.source(ctx);
}

/** A query: `from …` followed by clauses, each wrapping what comes before it. */
export class RelationQuery implements RelationSource {
  readonly [QUERY]: { readonly source: Part };

  constructor(private readonly parts: readonly Part[]) {
    this[QUERY] = { source: (ctx) => `(${this.render(ctx)})` };
  }

  /** @internal */
  render(ctx: Ctx): string {
    return this.parts.map((part) => part(ctx)).join(" ");
  }

  private with(part: Part): RelationQuery {
    return new RelationQuery([...this.parts, part]);
  }

  /** `join src [as a] [on l = r and …]`; `left: true` for `left join`. */
  join(
    source: RelationSource | string,
    options: {
      as?: string;
      on?: readonly (readonly [string, string])[];
      left?: boolean;
    } = {},
  ): RelationQuery {
    return this.with((ctx) => {
      let out = `${options.left ? "left " : ""}join ${sourceText(source, ctx)}`;
      if (options.as) out += ` as ${name(options.as)}`;
      if (options.on?.length)
        out += ` on ${options.on.map(([l, r]) => `${colref(l)} = ${colref(r)}`).join(" and ")}`;
      return out;
    });
  }

  /** `where predicate`: an object of `"a.col": value` (AND-ed) or anyOf/allOf/not of them. */
  where(predicate: unknown): RelationQuery {
    return this.with((ctx) => `where ${text(predicate, { ...ctx, relation: true })}`);
  }

  /** `select key as name, …` (`{}` selects nothing: `select ()`). */
  select(columns: Readonly<Record<string, RelationKey>>): RelationQuery {
    return this.with((_ctx) => {
      const entries = Object.entries(columns);
      if (entries.length === 0) return "select ()";
      return `select ${entries.map(([as, key]) => `${keyText(key)} as ${name(as)}`).join(", ")}`;
    });
  }

  /** `distinct on (key, …)`. */
  distinctOn(...keys: RelationKey[]): RelationQuery {
    return this.with(() => `distinct on (${keys.map(keyText).join(", ")})`);
  }

  /** `order by col [asc|desc], …` (no columns: `order by ()`). */
  orderBy(...columns: (string | readonly [string, "asc" | "desc"])[]): RelationQuery {
    return this.with(() => {
      if (columns.length === 0) return "order by ()";
      return `order by ${columns
        .map((column) =>
          typeof column === "string" ? colref(column) : `${colref(column[0])} ${column[1]}`,
        )
        .join(", ")}`;
    });
  }

  limit(count: number): RelationQuery {
    return this.with(() => `limit ${integer(count, "limit")}`);
  }

  offset(count: number): RelationQuery {
    return this.with(() => `offset ${integer(count, "offset")}`);
  }
}

function integer(value: number, what: string): string {
  if (!Number.isInteger(value) || value < 0)
    throw new PermissionsDslError(`${what} takes a non-negative integer, not ${value}`);
  return String(value);
}

function source(part: Part): RelationSource {
  return { [QUERY]: { source: part } };
}

/** `from.<table>(alias?)` / `from(source, alias?)`: the start of a query. */
export type From<Tables extends string> = {
  (source: RelationSource, alias?: string): RelationQuery;
} & { readonly [T in Tables]: (alias?: string) => RelationQuery };

export function makeFrom<Tables extends string>(tables: readonly Tables[]): From<Tables> {
  const from = (src: RelationSource, alias?: string) =>
    new RelationQuery([
      (ctx) => `from ${sourceText(src, ctx)}${alias ? ` as ${name(alias)}` : ""}`,
    ]);
  for (const table of tables) {
    Object.defineProperty(from, table, {
      enumerable: true,
      value: (alias?: string) =>
        new RelationQuery([() => `from ${name(table)}${alias ? ` as ${name(alias)}` : ""}`]),
    });
  }
  return from as From<Tables>;
}

/** `union(label: query, …)`. */
export function union(branches: Readonly<Record<string, RelationQuery>>): RelationSource {
  return source(
    (ctx) =>
      `union(${Object.entries(branches)
        .map(([label, query]) => `${name(label)}: ${query.render(ctx)}`)
        .join(", ")})`,
  );
}

/** `recursive(seed: …, step: …, frontier: key, dedupe: (…), maxDepth: n | fixpoint)`. */
export function recursive(options: {
  seed: RelationQuery;
  step: RelationQuery;
  frontier: RelationKey;
  dedupe: readonly RelationKey[];
  maxDepth: number | "fixpoint";
}): RelationSource {
  return source((ctx) => {
    const depth =
      options.maxDepth === "fixpoint" ? "fixpoint" : integer(options.maxDepth, "maxDepth");
    return (
      `recursive(seed: ${options.seed.render(ctx)}, step: ${options.step.render(ctx)}, ` +
      `frontier: ${keyText(options.frontier)}, dedupe: (${options.dedupe.map(keyText).join(", ")}), ` +
      `maxDepth: ${depth})`
    );
  });
}

/** `outer.col`: a column of the row outside the relation (the checked row). */
export function outer(column: string): ValueHandle<any, any> {
  return handle({ k: "text", text: `outer.${colref(column)}` }) as unknown as ValueHandle<any, any>;
}
/** `frontier.col`: a column of a recursive walk's frontier row. */
export function frontier(column: string): ValueHandle<any, any> {
  return handle({ k: "text", text: `frontier.${colref(column)}` }) as unknown as ValueHandle<
    any,
    any
  >;
}
/** `param.name`: a query parameter. */
export function param(paramName: string): ValueHandle<any, any> {
  return handle({ k: "text", text: `param.${name(paramName)}` }) as unknown as ValueHandle<
    any,
    any
  >;
}
/** `id(row)`, `id(outer)`, `id(frontier)`: a row id, as a value or a key. */
export function rowId(of: "row" | "outer" | "frontier"): RowIdRef & ValueHandle<any, any> {
  return Object.freeze({
    __jazzRowId: of,
    ...handle({ k: "text", text: `id(${of})` }),
  }) as unknown as RowIdRef & ValueHandle<any, any>;
}
/** `json('…')`: a JSON value the relation holds as is. */
export function jsonValue(value: unknown): ValueHandle<any, any> {
  return handle({
    k: "text",
    text: `json('${JSON.stringify(value).replaceAll("'", "''")}')`,
  }) as unknown as ValueHandle<any, any>;
}
