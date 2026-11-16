/**
 * Editing `jazz.app.json` for `jazz-tools upgrade`: the .64 file's `tsOnly`
 * markers and `slices` go, the `computed` fields and `transforms` the
 * conversion found come in. Printed as Jazz's Rust writer prints the file
 * (an object or array stays on one line while it fits in 100 columns), so
 * the diff shows only what upgrade changed.
 */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };

/** A column, computed field or transform type, as the file spells it. */
export type TypeSpec = string | JsonObject;

export interface ComputedDecl {
  type: TypeSpec;
  from: string[];
}

export interface TransformDecl {
  stored: TypeSpec;
  view: TypeSpec;
}

/** The `tsOnly` markers of a .64 file: per table, field → kind. */
export type TsOnlyMarkers = Record<string, Record<string, "computed" | "transform">>;

const WIDTH = 100;

/** The key order Jazz's writer uses for a table. */
const TABLE_KEYS = ["columns", "relations", "indexes", "branchBy", "history", "computed"];

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

/** The base type and optionality of a column spec (`"string?"`, `{ "enum": … }`). */
export function columnType(spec: Json): { type: TypeSpec; nullable: boolean } {
  if (typeof spec === "string") {
    return spec.endsWith("?")
      ? { type: spec.slice(0, -1), nullable: true }
      : { type: spec, nullable: false };
  }
  if (!isObject(spec))
    throw new Error(`a column spec is a string or an object, not ${JSON.stringify(spec)}`);
  const {
    optional,
    default: _default,
    merge: _merge,
    references: _references,
    transform: _t,
    ...rest
  } = spec;
  const nullable = optional === true;
  if ("type" in rest && Object.keys(rest).length === 1 && typeof rest.type === "string") {
    const inner = columnType(rest.type);
    return { type: inner.type, nullable: nullable || inner.nullable };
  }
  return { type: rest, nullable };
}

export interface AppEdits {
  computed: Record<string, Record<string, ComputedDecl>>;
  transforms: Record<string, TransformDecl>;
  /** Per table, column → the transform it names. */
  columnTransforms: Record<string, Record<string, string>>;
}

/**
 * The file with `tsOnly` and `slices` removed and `edits` added. Returns
 * what it removed, for the report.
 */
export function editAppJson(
  app: JsonObject,
  edits: AppEdits,
): { app: JsonObject; droppedSlices: string[] } {
  const droppedSlices = isObject(app.slices) ? Object.keys(app.slices) : [];
  const out: JsonObject = {};
  for (const [key, value] of entriesOf(app)) {
    if (key === "slices") continue;
    if (key === "tables") {
      out.tables = editTables(appTables(app), edits);
      continue;
    }
    if (key === "transforms") continue; // re-added below, merged
    out[key] = value;
  }
  const transforms: JsonObject = isObject(app.transforms) ? { ...app.transforms } : {};
  for (const [name, decl] of Object.entries(edits.transforms)) {
    transforms[name] = { stored: decl.stored, view: decl.view };
  }
  if (Object.keys(transforms).length > 0) out.transforms = transforms;
  return { app: out, droppedSlices };
}

function editTables(tables: Record<string, JsonObject>, edits: AppEdits): JsonObject {
  const out: JsonObject = {};
  for (const [name, table] of Object.entries(tables)) {
    const columnTransforms = edits.columnTransforms[name] ?? {};
    const columns: JsonObject = {};
    for (const [column, spec] of Object.entries(tableColumns(table))) {
      const transform = columnTransforms[column];
      columns[column] = transform === undefined ? spec : withTransform(spec, transform);
    }
    const computed: JsonObject = isObject(table.computed) ? { ...table.computed } : {};
    for (const [field, decl] of Object.entries(edits.computed[name] ?? {})) {
      computed[field] = { type: decl.type, from: decl.from };
    }
    const fields: JsonObject = {};
    const rest = Object.entries(table).filter(
      ([key]) => key !== "tsOnly" && key !== "computed" && key !== "columns",
    );
    // Known keys in the writer's order, then anything else as it was.
    for (const key of TABLE_KEYS) {
      if (key === "columns") fields.columns = columns;
      else if (key === "computed") {
        if (Object.keys(computed).length > 0) fields.computed = computed;
      } else {
        const found = rest.find(([k]) => k === key);
        if (found) fields[key] = found[1];
      }
    }
    for (const [key, value] of rest) if (!(key in fields)) fields[key] = value;
    out[name] = fields;
  }
  return out;
}

/** A column spec naming `transform`: in object form, `transform` last. */
function withTransform(spec: Json, transform: string): JsonObject {
  if (typeof spec === "string") return { type: spec, transform };
  if (!isObject(spec))
    throw new Error(`a column spec is a string or an object, not ${JSON.stringify(spec)}`);
  const { transform: _old, ...rest } = spec;
  return { ...rest, transform };
}

/** The versioned `$schema` of jazz.app.json, which carries the file's version. */
export const APP_SCHEMA_URL = "https://jazz.tools/schemas/jazz.app.v1.json";

/**
 * The file in the versioned form: `$schema` first, naming the version, and
 * no `format` (.63/.64 wrote `"format": 1` and an unversioned `$schema`).
 */
export function versionedAppJson(value: JsonObject): JsonObject {
  const rest = entriesOf(value).filter(([key]) => key !== "format" && key !== "$schema");
  return Object.fromEntries([["$schema", APP_SCHEMA_URL], ...rest]) as JsonObject;
}

/** The versioned `$schema` of jazz.permissions.json. */
export const PERMISSIONS_SCHEMA_URL = "https://jazz.tools/schemas/jazz.permissions.v1.json";
const LEGACY_PERMISSIONS_SCHEMA_URL = "https://jazz.tools/schemas/jazz.permissions.schema.json";

/**
 * A .64 jazz.permissions.json's text in the versioned form, edited in place
 * so the rest keeps its bytes; the text unchanged when it is versioned already
 * or laid out unlike a tool-written file (`jazz-tools generate` rewrites it).
 */
export function versionedPermissionsText(text: string): string {
  let out = text.replace(/\n  "format": 1,\n/, "\n");
  if (out.includes(`"$schema": "${LEGACY_PERMISSIONS_SCHEMA_URL}"`)) {
    out = out.replace(LEGACY_PERMISSIONS_SCHEMA_URL, PERMISSIONS_SCHEMA_URL);
  } else if (!out.includes('"$schema"') && out.startsWith("{\n  ")) {
    out = `{\n  "$schema": "${PERMISSIONS_SCHEMA_URL}",\n  ${out.slice(4)}`;
  }
  try {
    JSON.parse(out);
    return out;
  } catch {
    return text;
  }
}

/** Print as Jazz's Rust writer does. */
export function printAppJson(value: Json): string {
  return `${writeValue(value, 0, 0)}\n`;
}

function quote(text: string): string {
  return JSON.stringify(text);
}

function inline(value: Json): string {
  if (value === null) return "null";
  if (typeof value === "boolean" || typeof value === "number") return String(value);
  if (typeof value === "string") return quote(value);
  if (Array.isArray(value)) return `[${value.map(inline).join(", ")}]`;
  const entries = entriesOf(value);
  if (entries.length === 0) return "{}";
  return `{ ${entries.map(([k, v]) => `${quote(k)}: ${inline(v)}`).join(", ")} }`;
}

function writeValue(value: Json, indent: number, prefix: number): string {
  const flat = inline(value);
  const fits = prefix + [...flat].length <= WIDTH && !flat.includes("\n");
  const pad = (n: number) => " ".repeat(n);
  if (Array.isArray(value) && !fits && value.length > 0) {
    const items = value.map(
      (item) => `${pad(indent + 2)}${writeValue(item, indent + 2, indent + 2)}`,
    );
    return `[\n${items.join(",\n")}\n${pad(indent)}]`;
  }
  if (isObject(value) && !fits && entriesOf(value).length > 0) {
    const items = entriesOf(value).map(([key, item]) => {
      const head = `${quote(key)}: `;
      return `${pad(indent + 2)}${head}${writeValue(item, indent + 2, indent + 2 + [...head].length)}`;
    });
    return `{\n${items.join(",\n")}\n${pad(indent)}}`;
  }
  return flat;
}
