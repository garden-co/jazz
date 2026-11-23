/** `jazz.app.json` tables → `.64` builders (`s.table(…)`) for `s.defineSchema({…})`. */
import type { AppJson, ColumnJson, ColumnObject, TableJson } from "./app-json.js";
import { jsLiteral, propKey } from "./names.js";
import type { Unsupported } from "./permissions.js";

export interface TablesPrintOptions {
  /** Print relations (current tables) or `references` (history versions, which have none). */
  relations?: boolean;
  /** The app's `transforms`, for columns naming one. */
  transforms?: AppJson["transforms"];
  path: string;
  unsupported: Unsupported[];
}

/** The object literal of tables: `{ todos: s.table(…), … }`. */
export function printTables(
  tables: Record<string, TableJson>,
  options: TablesPrintOptions,
): string {
  const entries = Object.entries(tables).map(([name, table]) => {
    const comment = table.$comment ? lineComment(table.$comment) : "";
    return `${comment}${propKey(name)}: ${printTable(table, { ...options, path: `${options.path}.${name}` })},`;
  });
  return `{\n${entries.join("\n")}\n}`;
}

export function printTable(table: TableJson, options: TablesPrintOptions): string {
  const encryption = options.relations === false ? undefined : table.encryption;
  const encryptedColumns = encryption?.columns ?? {};
  const equality = new Set(encryption?.equality);
  const tokenColumns = new Set([...equality].map((name) => `__e2ee_eq_${name}`));
  if (encryption) refuseUnprintedEncryption(encryption, options);
  const columns = Object.entries(table.columns)
    .filter(([name]) => !tokenColumns.has(name))
    .map(([name, stored]) => {
      const plaintext = Object.hasOwn(encryptedColumns, name) ? encryptedColumns[name] : undefined;
      const column = plaintext ?? stored;
      const comment =
        typeof stored === "object" && stored.$comment ? lineComment(stored.$comment) : "";
      const builder = printColumn(column, { ...options, path: `${options.path}.columns.${name}` });
      const encryptionOptions = equality.has(name) ? "{ equality: true }" : "";
      const suffix = plaintext ? `.encrypted(${encryptionOptions})` : "";
      return `${comment}${propKey(name)}: ${builder}${suffix},`;
    });
  const fields: string[] = [];
  if (options.relations !== false) {
    for (const [name, rel] of Object.entries(table.relations ?? {})) {
      const comment = rel.$comment ? lineComment(rel.$comment) : "";
      const value =
        "rel" in rel
          ? `s.rel(${JSON.stringify(rel.rel)}, ${JSON.stringify(rel.column)})`
          : `s.reverse(${JSON.stringify(rel.reverse)}, ${JSON.stringify(rel.relation)})`;
      fields.push(`${comment}${propKey(name)}: ${value},`);
    }
  }
  // Computed fields sit among the columns: the type here, the implementation in jazz.computed.ts.
  for (const [name, computed] of Object.entries(table.computed ?? {})) {
    const type = printColumn(computed.type, {
      ...options,
      path: `${options.path}.computed.${name}.type`,
    });
    columns.push(`${propKey(name)}: s.computed(${type}, ${JSON.stringify(computed.from)}),`);
  }
  // Columns stay on one line when they fit (unless a comment needs lines); relations are expanded.
  const columnsText = !columns.length
    ? "{}"
    : columns.some((c) => c.includes("//"))
      ? `{\n${columns.join("\n")}\n}`
      : `{ ${columns.join(" ").replace(/,$/, "")} }`;
  let space: string | undefined;
  if (encryption) {
    space =
      encryption.space === "id"
        ? "id"
        : Object.entries(table.relations ?? {}).find(
            ([, rel]) =>
              "rel" in rel && rel.rel === encryption.scope && rel.column === encryption.space,
          )?.[0];
    if (space === undefined) {
      options.unsupported.push({
        path: `${options.path}.encryption.space`,
        text: encryption.space,
        reason: `the space column needs a forward relation to ${encryption.scope}`,
      });
    }
  }
  const fieldsText = fields.length ? `{\n${fields.join("\n")}\n}` : "{}";
  const args = [columnsText];
  if (fields.length || space !== undefined) args.push(fieldsText);
  if (space !== undefined) args.push(`{ space: ${JSON.stringify(space)} }`);
  let text = `s.table(${args.join(", ")})`;
  const singles = (table.indexes ?? []).filter((i): i is string => typeof i === "string");
  if (singles.length) text += `.indexOnly(${JSON.stringify(singles)})`;
  for (const composite of (table.indexes ?? []).filter((i): i is string[] => Array.isArray(i))) {
    text += `.compositeIndex(${JSON.stringify(composite)})`;
  }
  if (table.branchBy?.length) {
    text += `.branchBy(${JSON.stringify(table.branchBy.length === 1 ? table.branchBy[0] : table.branchBy)})`;
  }
  if (table.history) text += `.history(${JSON.stringify(table.history)})`;
  return text;
}

/** What an encryption entry holds that `s.table` and `.encrypted()` can print. */
const PRINTED_ENTRY_KEYS = new Set(["space", "scope", "columns", "equality"]);
/** Plaintext declaration fields represented by the column builder. */
const PRINTED_COLUMN_KEYS = new Set([
  "type",
  "enum",
  "json",
  "array",
  "optional",
  "default",
  "$comment",
]);

/** Report what a table's encryption entry holds that the TS DSL can't declare yet. */
function refuseUnprintedEncryption(
  encryption: NonNullable<TableJson["encryption"]>,
  options: TablesPrintOptions,
): void {
  const at = `${options.path}.encryption`;
  for (const [key, value] of Object.entries(encryption)) {
    if (PRINTED_ENTRY_KEYS.has(key)) continue;
    options.unsupported.push({
      path: `${at}.${key}`,
      text: JSON.stringify(value),
      reason: `Unknown encryption key ${key}. Expected space, scope, columns or equality.`,
    });
  }
  for (const [name, column] of Object.entries(encryption.columns)) {
    if (typeof column !== "object") continue;
    for (const field of Object.keys(column)) {
      if (PRINTED_COLUMN_KEYS.has(field)) continue;
      options.unsupported.push({
        path: `${at}.columns.${name}.${field}`,
        text: JSON.stringify((column as Record<string, unknown>)[field]),
        reason:
          field === "sealedType"
            ? "Printing an encrypted column's field sealedType is not supported yet"
            : `Unknown encrypted column field ${field}. Expected a type declaration, sealedType or default.`,
      });
    }
  }
}

const SCALARS = new Set([
  "string",
  "int",
  "bigint",
  "float",
  "boolean",
  "timestamp",
  "uuid",
  "bytes",
]);

export function printColumn(column: ColumnJson, options: TablesPrintOptions): string {
  if (typeof column === "string") {
    const optional = column.endsWith("?");
    const type = optional ? column.slice(0, -1) : column;
    if (!SCALARS.has(type)) {
      options.unsupported.push({ path: options.path, text: column, reason: `unknown column type` });
      return `s.string()`;
    }
    return `s.${type}()${optional ? ".optional()" : ""}`;
  }
  let text = baseColumn(column, options);
  if (column.references !== undefined) text += `.references(${JSON.stringify(column.references)})`;
  if (column.optional) text += ".optional()";
  if (column.default !== undefined) text += `.default(${printDefault(column, column.default)})`;
  if (column.merge !== undefined) text += `.merge(${JSON.stringify(column.merge)})`;
  if (column.transform !== undefined) {
    const transform = options.transforms?.[column.transform];
    if (!transform) {
      options.unsupported.push({
        path: options.path,
        text: column.transform,
        reason: "an unknown transform",
      });
    } else {
      const view = printColumn(transform.view, {
        ...options,
        path: `transforms.${column.transform}.view`,
      });
      text += `.transform(${JSON.stringify(column.transform)}, ${view})`;
    }
  }
  return text;
}

function baseColumn(column: ColumnObject, options: TablesPrintOptions): string {
  if (column.type !== undefined) return printColumn(column.type, options);
  if (column.enum !== undefined)
    return `s.enum(${column.enum.map((v) => JSON.stringify(v)).join(", ")})`;
  if (column.json !== undefined)
    return column.json === true ? "s.json()" : `s.json(${jsLiteral(column.json)})`;
  if (column.array !== undefined) return `s.array(${printColumn(column.array, options)})`;
  options.unsupported.push({
    path: options.path,
    text: JSON.stringify(column),
    reason: "unknown column",
  });
  return "s.string()";
}

/** A default value: JSON as is; a `bigint` is a decimal string in JSON and a `bigint` in TS. */
export function printDefault(column: ColumnJson, value: unknown): string {
  const type = typeof column === "string" ? column.replace(/\?$/, "") : column.type;
  if (type === "bigint" && typeof value === "string") return `${value}n`;
  return jsLiteral(value);
}

function lineComment(text: string): string {
  return text
    .split("\n")
    .map((line) => `// ${line}`.trimEnd() + "\n")
    .join("");
}
