/**
 * Reading `jazz.app.json` for `jazz-tools upgrade`, keeping key order (a
 * `history` version named by an all-digit hash must stay where it is).
 */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };

/** The `tsOnly` markers of a .64 file: per table, field → kind. */
export type TsOnlyMarkers = Record<string, Record<string, "computed" | "transform">>;

/**
 * Objects with an integer-like key (a `history` version named by an
 * all-digit short hash, a JSON Schema's `"0"` property) keep their keys
 * here, in file order: a plain JS object would move integer-like keys
 * first, silently reordering history that must stay oldest-first.
 */
export const ORDERED: unique symbol = Symbol("jazz.upgrade.orderedEntries");
type Ordered = { [ORDERED]?: Array<[string, Json]> };

/** An object's entries in file order. */
export function entriesOf(value: JsonObject): Array<[string, Json]> {
  return (value as Ordered)[ORDERED] ?? Object.entries(value);
}

/** Parse JSON keeping every object's key order. */
export function parseJsonOrdered(text: string): Json {
  let i = 0;
  const fail = (what: string): never => {
    throw new SyntaxError(`invalid JSON at offset ${i}: ${what}`);
  };
  const ws = () => {
    while (i < text.length && /\s/.test(text[i]!)) i++;
  };
  const value = (): Json => {
    ws();
    const c = text[i];
    if (c === "{") {
      i++;
      const entries: Array<[string, Json]> = [];
      ws();
      if (text[i] === "}") {
        i++;
      } else {
        for (;;) {
          ws();
          if (text[i] !== '"') fail("expected a key");
          const key = string();
          ws();
          if (text[i++] !== ":") fail("expected `:`");
          entries.push([key, value()]);
          ws();
          const next = text[i++];
          if (next === "}") break;
          if (next !== ",") fail("expected `,` or `}`");
        }
      }
      const object: JsonObject = {};
      for (const [k, v] of entries) object[k] = v;
      if (entries.some(([k]) => /^(0|[1-9]\d*)$/.test(k))) {
        Object.defineProperty(object, ORDERED, { value: entries, enumerable: false });
      }
      return object;
    }
    if (c === "[") {
      i++;
      const items: Json[] = [];
      ws();
      if (text[i] === "]") {
        i++;
        return items;
      }
      for (;;) {
        items.push(value());
        ws();
        const next = text[i++];
        if (next === "]") return items;
        if (next !== ",") fail("expected `,` or `]`");
      }
    }
    if (c === '"') return string();
    const match = /^(?:-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)/.exec(
      text.slice(i, i + 400),
    );
    if (!match) return fail("unexpected character");
    i += match[0].length;
    return JSON.parse(match[0]) as Json;
  };
  const string = (): string => {
    const start = i;
    i++;
    while (i < text.length && text[i] !== '"') i += text[i] === "\\" ? 2 : 1;
    i++;
    return JSON.parse(text.slice(start, i)) as string;
  };
  const result = value();
  ws();
  if (i !== text.length) fail("trailing content");
  return result;
}

export function parseAppJson(text: string): JsonObject {
  const value = parseJsonOrdered(text);
  if (!isObject(value)) throw new Error("jazz.app.json is not a JSON object");
  return value;
}

export function isObject(value: Json | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** The current tables of the file. */
export function appTables(app: JsonObject): Record<string, JsonObject> {
  const tables = app.tables;
  if (!isObject(tables)) throw new Error("jazz.app.json has no `tables` object");
  const out: Record<string, JsonObject> = {};
  for (const [name, table] of Object.entries(tables)) {
    if (!isObject(table)) throw new Error(`jazz.app.json: tables.${name} is not an object`);
    out[name] = table;
  }
  return out;
}

/** The stored columns of a table, with their specs. */
export function tableColumns(table: JsonObject): Record<string, Json> {
  return isObject(table.columns) ? table.columns : {};
}

/** The `tsOnly` markers in the current tables. */
export function tsOnlyMarkers(app: JsonObject): TsOnlyMarkers {
  const out: TsOnlyMarkers = {};
  for (const [name, table] of Object.entries(appTables(app))) {
    const markers = table.tsOnly;
    if (!isObject(markers)) continue;
    out[name] = {};
    for (const [field, kind] of Object.entries(markers)) {
      if (kind !== "computed" && kind !== "transform") {
        throw new Error(
          `jazz.app.json: tables.${name}.tsOnly.${field} is neither "computed" nor "transform"`,
        );
      }
      out[name]![field] = kind;
    }
  }
  return out;
}
