import { hasSystemTables } from "../encryption/table-names.js";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { structuralBigInt } from "../runtime/structural-values.js";
import {
  isDefinedMigration,
  witnessColumns,
  type DefinedMigration,
  type ReadableMigrationSteps,
} from "../migrations.js";
import { isDefinedTable } from "../schema-dsl/table.js";
import {
  readableColumn,
  type ReadableJson,
  type ReadableTable,
} from "../schema-dsl/readable-app.js";
import { compileApp } from "./codegen-napi.js";

/** The jazz-napi functions this module calls, on schemas in the engine's JSON form (`{ "tables": … }`). */
async function jazzNapi(): Promise<{
  schemaHash(engineSchemaJson: string): string;
  schemasAreCompatible(leftJson: string, rightJson: string): boolean;
  migrationDiff(fromJson: string, toJson: string): string;
  versionTables(engineSchemaJson: string): string;
  checkEncryptionMigration(fromJson: string, toJson: string): void;
}> {
  // A specifier bundlers can't follow: this module is Node tooling only.
  const specifier = "jazz-napi";
  return import(/* @vite-ignore */ specifier);
}

/** The engine's hash of a schema in its JSON form (`{ "tables": … }`), through jazz-napi. */
async function schemaHashOf(engineSchemaJson: string): Promise<string> {
  return (await jazzNapi()).schemaHash(engineSchemaJson);
}
import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
  Value,
} from "../drivers/types.js";
import { shortSchemaHash } from "./schema-utils.js";

function pickWitnessSchema(schema: WasmSchema, tableNames: readonly string[]): WasmSchema {
  const visited = new Set(tableNames);
  for (const tableName of visited) {
    const table = schema[tableName];
    for (const column of table?.columns ?? []) {
      if (column.references) visited.add(column.references);
    }
    for (const relation of Object.values(table?.relations ?? {})) visited.add(relation.table);
  }
  const uniqueTableNames = [...visited];
  return Object.fromEntries(
    uniqueTableNames
      .filter((tableName) => schema[tableName])
      .map((tableName) => [tableName, schema[tableName]!]),
  );
}

function indentBlock(text: string, indent: number): string {
  const prefix = " ".repeat(indent);
  return text
    .split("\n")
    .map((line) => (line.length === 0 ? line : `${prefix}${line}`))
    .join("\n");
}

// JSON.parse preserves own "__proto__" keys that object-literal syntax reinterprets.
function jsonSchemaExpression(schema: unknown): string {
  return `JSON.parse(${JSON.stringify(JSON.stringify(schema))})`;
}

function baseBuilderExpression(columnType: WasmColumnType): string {
  switch (columnType.type) {
    case "Text":
      return "s.string()";
    case "Boolean":
      return "s.boolean()";
    case "Integer":
      return "s.int()";
    case "Double":
      return "s.float()";
    case "Timestamp":
      return "s.timestamp()";
    case "Bytea":
      return "s.bytes()";
    case "Json":
      return columnType.schema ? `s.json(${jsonSchemaExpression(columnType.schema)})` : "s.json()";
    case "Enum":
      return `s.enum(${columnType.variants.map((variant) => JSON.stringify(variant)).join(", ")})`;
    case "Uuid":
      return "s.uuid()";
    case "Array":
      return `s.array(${baseBuilderExpression(columnType.element)})`;
    case "BigInt":
      return "s.bigint()";
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

// Render stored values directly: JSON is already serialized text, timestamps are
// milliseconds, and bigint/bytes need JavaScript constructors rather than JSON.
function defaultExpression(value: Value): string {
  switch (value.type) {
    case "Null":
      return "null";
    case "Text":
    case "Uuid":
      return JSON.stringify(value.value);
    case "Boolean":
      return value.value ? "true" : "false";
    case "BigInt":
      return `${structuralBigInt(value.value)}n`;
    case "Integer":
    case "Double":
      return Object.is(value.value, -0) ? "-0" : String(Number(value.value));
    case "Timestamp": {
      const milliseconds = Number(value.value);
      if (!Object.is(new Date(milliseconds).getTime(), milliseconds)) {
        throw new Error(
          "Cannot render migration timestamp default exactly as a Date; use whole milliseconds within the JavaScript Date range.",
        );
      }
      return `new Date(${milliseconds})`;
    }
    case "Bytea":
      return `new Uint8Array([${Array.from(new Uint8Array(value.value)).join(", ")}])`;
    case "Array":
      return `[${value.value.map(defaultExpression).join(", ")}]`;
    default:
      throw new Error(`Migration stub generation does not yet support ${value.type} defaults.`);
  }
}

function builderExpressionForColumn(column: ColumnDescriptor): string {
  const base = baseBuilderExpression(column.column_type);
  const optional = column.nullable ? `${base}.optional()` : base;
  const withOptional =
    column.default === undefined
      ? optional
      : `${optional}.default(${defaultExpression(column.default)})`;
  if (column.merge_strategy === "Counter") {
    return `${withOptional}.merge("counter")`;
  }
  if (column.merge_strategy === "GSet") {
    return `${withOptional}.merge("g-set")`;
  }
  if (column.merge_strategy === "Text") {
    return `${withOptional}.merge("text")`;
  }
  if (column.merge_strategy === "Markdown") {
    return `${withOptional}.merge("markdown")`;
  }
  if (column.merge_strategy === "Html") {
    return `${withOptional}.merge("html")`;
  }
  return withOptional;
}

function renderSchemaWitness(schema: WasmSchema): string {
  const tableEntries = Object.entries(schema)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([tableName, tableSchema]) => {
      const columnLines = tableSchema.columns.map(
        (column) => `${JSON.stringify(column.name)}: ${builderExpressionForColumn(column)},`,
      );
      const relations = { ...tableSchema.relations };
      for (const column of tableSchema.columns) {
        if (
          !column.references ||
          Object.values(relations).some((r) => r.kind === "forward" && r.column === column.name)
        )
          continue;
        const alias = `${column.name}Relation`;
        if (Object.hasOwn(relations, alias) || tableSchema.columns.some((c) => c.name === alias))
          throw new Error(
            `Cannot render migration witness: relationship "${tableName}.${alias}" collides with an existing name.`,
          );
        relations[alias] = { kind: "forward", table: column.references, column: column.name };
      }
      const relationLines = Object.entries(relations).map(
        ([name, relation]) =>
          `${JSON.stringify(name)}: s.${relation.kind === "forward" ? "rel" : "reverse"}(${JSON.stringify(relation.table)}, ${JSON.stringify(relation.kind === "forward" ? relation.column : relation.relation)}),`,
      );
      const index =
        tableSchema.indexed_columns === undefined
          ? ""
          : `.indexOnly(${JSON.stringify(tableSchema.indexed_columns)})`;
      const compositeIndexes = (tableSchema.composite_indexes ?? [])
        .map((columns) => `.compositeIndex(${JSON.stringify(columns)})`)
        .join("");
      const branch =
        tableSchema.branchBy === undefined
          ? ""
          : `.branchBy(${JSON.stringify(tableSchema.branchBy)})`;
      const history =
        tableSchema.history === undefined ? "" : `.history(${JSON.stringify(tableSchema.history)})`;
      return `${JSON.stringify(tableName)}: s.table({\n${indentBlock(columnLines.join("\n"), 2)}\n}, {\n${indentBlock(relationLines.join("\n"), 2)}\n})${index}${compositeIndexes}${branch}${history}`;
    });

  if (tableEntries.length === 0) {
    return "{}";
  }

  return `{\n${indentBlock(tableEntries.join(",\n"), 2)}\n}`;
}

/**
 * Render a migration column operation such as `m.add.string({ default: null })`.
 * `drop` operations carry their default under `backwardsDefault`.
 */
function renderColumnOperationExpression(
  operation: "add" | "drop",
  column: ColumnDescriptor,
  defaultExpression: string,
): string {
  const m = `m.${operation}`;
  const defaultEntry = `${operation === "add" ? "default" : "backwardsDefault"}: ${defaultExpression}`;
  const columnType = column.column_type;
  switch (columnType.type) {
    case "Text":
      return `${m}.string({ ${defaultEntry} })`;
    case "Boolean":
      return `${m}.boolean({ ${defaultEntry} })`;
    case "Integer":
      return `${m}.int({ ${defaultEntry} })`;
    case "Double":
      return `${m}.float({ ${defaultEntry} })`;
    case "Timestamp":
      return `${m}.timestamp({ ${defaultEntry} })`;
    case "Bytea":
      return `${m}.bytes({ ${defaultEntry} })`;
    case "Json":
      return columnType.schema
        ? `${m}.json({ ${defaultEntry}, schema: ${jsonSchemaExpression(columnType.schema)} })`
        : `${m}.json({ ${defaultEntry} })`;
    case "Enum":
      return `${m}.enum(${columnType.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { ${defaultEntry} })`;
    case "Uuid":
      return column.references
        ? `${m}.ref(${JSON.stringify(column.references)}, { ${defaultEntry} })`
        : `${m}.uuid({ ${defaultEntry} })`;
    case "Array":
      return `${m}.array({ of: ${baseBuilderExpression(columnType.element)}, ${defaultEntry} })`;
    case "BigInt":
      return `${m}.bigint({ ${defaultEntry} })`;
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

/**
 * The TODOs a generated stub leaves where a step needs the author's value:
 * a migration file that still holds one does not reproduce its target
 * schema, so it cannot deploy.
 */
export const UNFILLED_STUB_TODO =
  /\/\/ TODO: ((?:Added|Removed)(?: required)? column "([^"]+)" (?:needs an explicit (?:default|backwardsDefault)|may be a rename of [^:]+:[^\n]*)\.)/;

/**
 * What a migration between two snapshots does, as Rust decides it
 * (jazz-napi `migrationDiff`, the diff `jazz.app.json`'s `migrations create`
 * writes too). It refuses a change no step does (a kept column's type or
 * optionality, a reference moving to another table).
 */
export interface MigrationDiff {
  createTables: string[];
  dropTables: string[];
  renameTables: { from: string; to: string }[];
  migrate: {
    table: string;
    steps: MigrationDiffStep[];
    addedVariants: { column: string; variants: string[] }[];
    /** When there are no steps or added variants: what changed instead. */
    unchangedRows: string[];
  }[];
}

export type MigrationDiffStep =
  | { step: "add"; column: string; needsDefault: boolean }
  | { step: "drop"; column: string; needsBackwardsDefault: boolean }
  | { step: "maybeRenamed"; column: string; candidates: string[] }
  | { step: "maybeRenamedAway"; column: string };

async function migrationDiff(fromSchema: WasmSchema, toSchema: WasmSchema): Promise<MigrationDiff> {
  const napi = await jazzNapi();
  return JSON.parse(
    napi.migrationDiff(
      JSON.stringify({ tables: fromSchema }),
      JSON.stringify({ tables: toSchema }),
    ),
  ) as MigrationDiff;
}

/** A table's steps as stub lines: `// TODO:` where only the author can decide. */
function renderTableSteps(
  fromTable: WasmSchema[string],
  toTable: WasmSchema[string],
  table: MigrationDiff["migrate"][number],
  /** The `to` version's encrypted columns of this table. */
  encrypted: ReadonlySet<string> = new Set(),
): string[] {
  const column = (schema: WasmSchema[string], name: string) =>
    schema.columns.find((candidate) => candidate.name === name)!;
  const fill = (descriptor: ColumnDescriptor) =>
    descriptor.default === undefined ? "null" : defaultExpression(descriptor.default);
  const lines: string[] = [];
  for (const step of table.steps) {
    const name = JSON.stringify(step.column);
    switch (step.step) {
      case "maybeRenamed": {
        const of = step.candidates.map((candidate) => JSON.stringify(candidate)).join(" or ");
        lines.push(
          `// TODO: Added column ${name} may be a rename of ${of}: to keep its values, ` +
            `give it the step m.renameFrom(${JSON.stringify(step.candidates[0])}); to start it empty, ` +
            `add it with m.add and drop the old column with m.drop.`,
        );
        break;
      }
      case "maybeRenamedAway":
        // Decided with the added column it may have been renamed to.
        break;
      case "add": {
        const descriptor = column(toTable, step.column);
        if (encrypted.has(step.column)) {
          // Stored as required bytes. The empty ciphertext is what a cell no
          // writer sealed holds: the rows stored before the column read it
          // as never written, null or the declared default. (A required
          // column without a default is refused before a stub is written.)
          lines.push(
            `// Rows stored before ${name} read it as never written: null, or its declared default.`,
            `${name}: m.add.bytes({ default: new Uint8Array() }),`,
          );
          break;
        }
        lines.push(
          step.needsDefault
            ? `// TODO: Added required column ${name} needs an explicit default.`
            : `${name}: ${renderColumnOperationExpression("add", descriptor, fill(descriptor))},`,
        );
        break;
      }
      case "drop": {
        const descriptor = column(fromTable, step.column);
        lines.push(
          step.needsBackwardsDefault
            ? `// TODO: Removed required column ${name} needs an explicit backwardsDefault.`
            : `${name}: ${renderColumnOperationExpression("drop", descriptor, fill(descriptor))},`,
        );
        break;
      }
    }
  }
  for (const { column: name, variants } of table.addedVariants) {
    lines.push(
      `// Column ${JSON.stringify(name)} gains enum variant${variants.length === 1 ? "" : "s"} ` +
        `${variants.map((variant) => JSON.stringify(variant)).join(", ")}: existing rows are ` +
        `unchanged, so it needs no step.`,
    );
  }
  if (table.unchangedRows.length > 0) {
    lines.push(`// Rows pass unchanged: only ${table.unchangedRows.join(", ")} changed.`);
  }
  return lines;
}

function renderMigrationBody(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
  diff: MigrationDiff,
  encrypted: EncryptionEntries = {},
): {
  migrateBody?: string;
  renameTablesBody?: string;
  createTablesBody?: string;
  dropTablesBody?: string;
  witnessFrom: WasmSchema;
  witnessTo: WasmSchema;
} {
  const migrated = diff.migrate.map((table) => table.table);
  const witnessFromTables = [
    ...migrated,
    ...diff.dropTables,
    ...diff.renameTables.map((rename) => rename.from),
  ];
  const witnessToTables = [
    ...migrated,
    ...diff.createTables,
    ...diff.renameTables.map((rename) => rename.to),
  ];
  // Include dependencies on both sides so unchanged referenced tables are not
  // mistaken for newly created or removed tables by the public migration DSL.
  const dependencies = new Set([
    ...Object.keys(pickWitnessSchema(fromSchema, witnessFromTables)),
    ...Object.keys(pickWitnessSchema(toSchema, witnessToTables)),
  ]);
  let count: number;
  do {
    count = dependencies.size;
    for (const name of Object.keys(pickWitnessSchema(fromSchema, [...dependencies])))
      dependencies.add(name);
    for (const name of Object.keys(pickWitnessSchema(toSchema, [...dependencies])))
      dependencies.add(name);
  } while (dependencies.size !== count);
  const witnessFrom = pickWitnessSchema(fromSchema, [...dependencies]);
  const witnessTo = pickWitnessSchema(toSchema, [...dependencies]);

  const lines: string[] = [];
  for (const table of diff.migrate) {
    lines.push(`${JSON.stringify(table.table)}: {`);
    for (const line of renderTableSteps(
      fromSchema[table.table]!,
      toSchema[table.table]!,
      table,
      encryptedColumns(encrypted, table.table),
    ))
      lines.push(`  ${line}`);
    lines.push("},");
    lines.push("");
  }
  if (
    lines.length === 0 &&
    diff.renameTables.length === 0 &&
    diff.createTables.length === 0 &&
    diff.dropTables.length === 0
  ) {
    lines.push("// TODO: No schema differences were detected.");
  }

  const tableFlags = (tables: string[]) =>
    tables.length > 0
      ? tables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
      : undefined;
  return {
    migrateBody: lines.length > 0 ? lines.join("\n").trimEnd() : undefined,
    createTablesBody: tableFlags(diff.createTables),
    dropTablesBody: tableFlags(diff.dropTables),
    renameTablesBody:
      diff.renameTables.length > 0
        ? diff.renameTables
            .map((rename) => `${rename.to}: m.renameTableFrom(${JSON.stringify(rename.from)}),`)
            .join("\n")
        : undefined,
    witnessFrom,
    witnessTo,
  };
}

function renderMigrationStub(
  input: {
    fromHash: string;
    toHash: string;
    fromSchema: WasmSchema;
    toSchema: WasmSchema;
    /** The `to` version's encryption entries, by table. */
    toEncryption?: EncryptionEntries;
  },
  diff: MigrationDiff,
): string {
  const rendered = renderMigrationBody(input.fromSchema, input.toSchema, diff, input.toEncryption);
  const sections: string[] = [];

  if (rendered.renameTablesBody) {
    sections.push(`  renameTables: {\n${indentBlock(rendered.renameTablesBody, 4)}\n  },`);
  }

  if (rendered.createTablesBody) {
    sections.push(`  createTables: {\n${indentBlock(rendered.createTablesBody, 4)}\n  },`);
  }

  if (rendered.dropTablesBody) {
    sections.push(`  dropTables: {\n${indentBlock(rendered.dropTablesBody, 4)}\n  },`);
  }

  if (rendered.migrateBody) {
    sections.push(`  migrate: {\n${indentBlock(rendered.migrateBody, 4)}\n  },`);
  }

  sections.push(`  fromHash: ${JSON.stringify(shortSchemaHash(input.fromHash))},`);
  sections.push(`  toHash: ${JSON.stringify(shortSchemaHash(input.toHash))},`);
  sections.push(`  from: ${renderSchemaWitness(rendered.witnessFrom)},`);
  sections.push(`  to: ${renderSchemaWitness(rendered.witnessTo)},`);

  return `import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
${sections.join("\n")}
});
`;
}

// ============================================================================
// Snapshots and history (alpha.66: TS migrations → jazz.app.json)
// ============================================================================

/** A schema version in `jazz.app.json`'s readable form: `{ tables }`. */
export interface ReadableVersion {
  tables: Record<string, ReadableTable>;
}

/** A `migrations` entry of `jazz.app.json` (SPEC §10.10). */
export interface ReadableMigration {
  name?: string;
  from: string;
  to?: string;
  createTables?: string[];
  dropTables?: string[];
  renameTables?: Record<string, string>;
  migrate?: Record<string, Record<string, ReadableJson>>;
}

/** Loads a migration file (`migrations/*.ts`) and returns its exports. */
export type ModuleLoader = (file: string) => Promise<Record<string, unknown>>;

const SNAPSHOT_FILE = /^(?:(\d{8,17}T\d{6})-)?([0-9a-f]{12})\.json$/i;
const MIGRATION_FILE = /-([0-9a-f]{12})-([0-9a-f]{12})\.ts$/i;

/**
 * A version's tables without their columns' `transform`s: a snapshot holds
 * what is stored, and the top-level `transforms` that name a column's view
 * type aren't part of a version (nor of its schema hash).
 */
function withoutTransforms(tables: Record<string, ReadableTable>): Record<string, ReadableTable> {
  return Object.fromEntries(
    Object.entries(tables).map(([name, table]) => {
      const columns = (table as { columns?: Record<string, unknown> }).columns;
      if (!columns) return [name, table];
      return [
        name,
        {
          ...table,
          columns: Object.fromEntries(
            Object.entries(columns).map(([column, spec]) => {
              if (spec && typeof spec === "object" && "transform" in spec) {
                const { transform: _transform, ...stored } = spec as Record<string, unknown>;
                return [column, stored];
              }
              return [column, spec];
            }),
          ),
        } as ReadableTable,
      ];
    }),
  );
}

/**
 * The engine schema (the snapshot's contents) and its full hash of a version
 * given in readable form, through the Rust compiler (napi `compileApp`).
 * Relations a table declares are kept on the snapshot, as .64 wrote them;
 * they are not part of the hash.
 */
export async function snapshotFromVersion(
  versionTables: Record<string, ReadableTable>,
): Promise<{ hash: string; schema: WasmSchema }> {
  const compiled = (await compileApp(
    JSON.stringify({
      $schema: "https://jazz.tools/schemas/jazz.app.v1.json",
      tables: withoutTransforms(versionTables),
    }),
  )) as {
    schema: { current: string; schemas: Array<{ hash: string; schema: { tables: WasmSchema } }> };
  };
  const current = compiled.schema.schemas.find((entry) => entry.hash === compiled.schema.current);
  if (!current) throw new Error("compileApp returned no current schema");
  const schema: WasmSchema = {};
  for (const [name, table] of Object.entries(current.schema.tables)) {
    const relations = versionTables[name]?.relations as
      | Record<string, { rel?: string; column?: string; reverse?: string; relation?: string }>
      | undefined;
    schema[name] = relations
      ? {
          ...table,
          relations: Object.fromEntries(
            Object.entries(relations).map(([relation, spec]) => [
              relation,
              spec.rel !== undefined
                ? { kind: "forward" as const, table: spec.rel, column: spec.column! }
                : { kind: "reverse" as const, table: spec.reverse!, relation: spec.relation! },
            ]),
          ),
        }
      : table;
  }
  return { hash: current.hash, schema };
}

/**
 * A snapshot (engine schema) as a readable history version: columns with
 * `references`, no relations. Rust's `versionTables` (jazz-napi), the form
 * `jazz.app.json`'s writer prints a `history` version in.
 */
export async function snapshotToVersion(schema: WasmSchema): Promise<ReadableVersion> {
  const tables = (await jazzNapi()).versionTables(JSON.stringify({ tables: schema }));
  return { tables: JSON.parse(tables) as Record<string, ReadableTable> };
}

/** A migration witness table as a readable version table (references, no relations). */
function witnessVersionTable(table: unknown): ReadableTable {
  const columns = witnessColumns(table as never);
  const entry: ReadableTable = {
    columns: Object.fromEntries(
      Object.entries(columns).map(([name, column]) => [name, readableColumn(column)]),
    ),
  };
  if (isDefinedTable(table)) {
    const indexes: ReadableJson[] = [
      ...(table.indexedColumns ?? []),
      ...(table.compositeIndexes ?? []).map((index) => [...index]),
    ];
    if (indexes.length > 0) entry.indexes = indexes;
    if (table.branchColumns?.length) entry.branchBy = [...table.branchColumns];
    if (table.historyRetention) entry.history = table.historyRetention;
  }
  return entry;
}

/**
 * The version on the other side of `migration`, from the version on this
 * side (`direction` "backward": from `to` to `from`): the known tables, with
 * the witness's tables, renames, created and dropped tables applied.
 */
function deriveVersion(
  known: Record<string, ReadableTable>,
  migration: DefinedMigration,
  direction: "backward" | "forward",
): Record<string, ReadableTable> {
  const steps = migration.steps;
  const out: Record<string, ReadableTable> = { ...known };
  const witness = (direction === "backward" ? migration.from : migration.to) as Record<
    string,
    unknown
  >;
  const renames = Object.entries(steps.renameTables ?? {});
  if (direction === "backward") {
    for (const table of steps.createTables ?? []) delete out[table];
    for (const [target, source] of renames) {
      if (out[target]) out[source] = out[target]!;
      delete out[target];
    }
  } else {
    for (const table of steps.dropTables ?? []) delete out[table];
    for (const [target, source] of renames) {
      if (out[source]) out[target] = out[source]!;
      delete out[source];
    }
  }
  for (const [name, table] of Object.entries(witness)) out[name] = witnessVersionTable(table);
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
}

function unwrapDefault(value: unknown): unknown {
  let current = value;
  while (
    typeof current === "object" &&
    current !== null &&
    "default" in current &&
    !isDefinedMigration(current) &&
    Object.keys(current).length === 1
  )
    current = (current as { default: unknown }).default;
  return current;
}

interface LoadedMigrationFile {
  file: string;
  fromHash: string;
  toHash: string;
  migration: DefinedMigration;
}

async function listDir(dir: string): Promise<string[]> {
  try {
    return (await readdir(dir)).sort();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

/** `migrations/*.ts`, oldest first (their names start with a timestamp), loaded. */
async function loadMigrationFiles(
  migrationsDir: string,
  loadModule: ModuleLoader,
  skipUnresolved = false,
): Promise<LoadedMigrationFile[]> {
  const out: LoadedMigrationFile[] = [];
  for (const name of await listDir(migrationsDir)) {
    if (!name.endsWith(".ts") || name.endsWith(".d.ts") || name === "index.ts") continue;
    const file = join(migrationsDir, name);
    // A stub's TODOs are decisions `migrations create` left to the author:
    // a migration with one left would publish the wrong lens.
    const todos = (await readFile(file, "utf8"))
      .split("\n")
      .map((line, index) => ({ line: index + 1, match: UNFILLED_STUB_TODO.exec(line) }))
      .filter((entry) => entry.match);
    if (todos.length > 0 && skipUnresolved) continue;
    if (todos.length > 0)
      throw new Error(
        `migrations/${name} has ${todos.length} unresolved TODO${todos.length === 1 ? "" : "s"} left by \`jazz-tools migrations create\`: ` +
          todos.map(({ line, match }) => `line ${line}: ${match![1]}`).join("; ") +
          ". Write the step it asks for (e.g. `col: m.add.int({ default: 0 })`) and delete the TODO comment.",
      );
    const exports = await loadModule(file);
    const migration = unwrapDefault(exports.default ?? exports.migration);
    if (!isDefinedMigration(migration))
      throw new Error(
        `migrations/${name}: export default m.defineMigration({...}) (from "jazz-tools": \`import { schema as s, migration as m }\`).`,
      );
    const fromFile = MIGRATION_FILE.exec(name);
    const fromHash = (migration.fromHash ?? fromFile?.[1])?.toLowerCase();
    const toHash = (migration.toHash ?? fromFile?.[2])?.toLowerCase();
    if (!fromHash || !toHash)
      throw new Error(
        `migrations/${name}: name its versions with fromHash and toHash (or end the file name with -<from>-<to>.ts).`,
      );
    out.push({
      file: name,
      fromHash: shortSchemaHash(fromHash),
      toHash: shortSchemaHash(toHash),
      migration,
    });
  }
  return out;
}

interface SnapshotEntry {
  file: string;
  timestamp?: string;
  hash: string;
  schema: WasmSchema;
}

/** `migrations/snapshots/*.json`, each with its full hash. */
export async function listSnapshots(migrationsDir: string): Promise<SnapshotEntry[]> {
  const dir = join(migrationsDir, "snapshots");
  const out: SnapshotEntry[] = [];
  for (const file of await listDir(dir)) {
    const match = SNAPSHOT_FILE.exec(file);
    if (!match) continue;
    const schema = JSON.parse(await readFile(join(dir, file), "utf8")) as WasmSchema;
    const hash = await schemaHashOf(JSON.stringify({ tables: schema }));
    if (shortSchemaHash(hash) !== match[2]!.toLowerCase())
      throw new Error(
        `migrations/snapshots/${file}: its schema hashes to ${shortSchemaHash(hash)}, not ${match[2]}. Snapshots are written by \`jazz-tools migrations create\`; don't edit them.`,
      );
    out.push({ file, ...(match[1] ? { timestamp: match[1] } : {}), hash, schema });
  }
  return out;
}

// ============================================================================
// Encryption declarations beside snapshots
// ============================================================================

/**
 * A version's encryption entries, by table: each table's `encryption` as
 * `jazz.app.json` writes it. A snapshot holds what is stored (an encrypted
 * column is `bytes`), so the entries of a version that declares encryption
 * are kept beside its snapshot, in `<snapshot>.encryption.json`.
 */
type EncryptionEntries = Record<string, ReadableJson>;

export function encryptionEntries<T>(
  tables: Record<string, { encryption?: T }>,
): Record<string, T> {
  const entries: Record<string, T> = {};
  for (const [name, table] of Object.entries(tables))
    if (table.encryption !== undefined) entries[name] = table.encryption;
  return entries;
}

/** The encrypted columns of `table` in `entries`. */
function encryptedColumns(entries: EncryptionEntries, table: string): Set<string> {
  const entry = entries[table] as { columns?: Record<string, unknown> } | undefined;
  return new Set(Object.keys(entry?.columns ?? {}));
}

function entriesFile(snapshotFile: string): string {
  return snapshotFile.replace(/\.json$/i, ".encryption.json");
}

export function encryptionEntriesText(entries: Record<string, unknown>): string {
  return `${JSON.stringify({ tables: entries }, null, 2)}\n`;
}

/**
 * The encryption entries of `snapshot`'s version: none for a version without
 * Jazz's system tables, else its `.encryption.json`, which must be there.
 */
async function snapshotEntries(
  migrationsDir: string,
  version: SnapshotEntry | string,
  snapshots: readonly SnapshotEntry[],
  files: readonly LoadedMigrationFile[],
  before = files.length,
): Promise<EncryptionEntries | undefined> {
  const short = typeof version === "string" ? version : shortSchemaHash(version.hash);
  const snapshot =
    typeof version === "string"
      ? snapshots.filter(({ hash }) => shortSchemaHash(hash) === short).at(-1)
      : version;
  let entries: EncryptionEntries | undefined;
  if (snapshot) {
    const file = entriesFile(snapshot.file);
    try {
      entries = parseEntries(file, await readFile(join(migrationsDir, "snapshots", file), "utf8"));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      if (!hasSystemTables(snapshot.schema)) return {};
      throw new Error(
        `migrations/snapshots/${snapshot.file}: version ${short} declares encryption ` +
          `(it has Jazz's system tables), but migrations/snapshots/${file}, which holds its encryption ` +
          "declarations, is missing. Restore it from version control.",
      );
    }
  } else {
    entries = await sidecarEntries(migrationsDir, short);
  }

  if (!entries || Object.keys(entries).length === 0) return entries;
  // The snapshot is written before the author chooses whether this is a rename.
  let index = before - 1;
  while (index >= 0 && files[index]!.toHash !== short) index--;
  const migration = files[index];
  const steps = migration?.migration.steps;
  if (
    migration &&
    steps &&
    (steps.renameTables ||
      Object.values(steps.migrate ?? {}).some((columns) =>
        Object.values(columns).some((step) => "renameFrom" in step),
      ))
  ) {
    const previous = await snapshotEntries(
      migrationsDir,
      migration.fromHash,
      snapshots,
      files,
      index,
    );
    if (previous) pinEncryptedEnums(entries, previous, false, steps);
  }
  return entries;
}

function parseEntries(file: string, text: string): EncryptionEntries {
  const parsed = JSON.parse(text) as { tables?: EncryptionEntries };
  if (!parsed || typeof parsed.tables !== "object" || parsed.tables === null)
    throw new Error(
      `migrations/snapshots/${file}: expected { "tables": { <table>: <encryption> } }.`,
    );
  return parsed.tables;
}

/**
 * The encryption entries kept for version `short` in a `.encryption.json`
 * of its own, if there is one: what a version whose snapshot is missing
 * still records of its declarations.
 */
async function sidecarEntries(
  migrationsDir: string,
  short: string,
): Promise<EncryptionEntries | undefined> {
  const dir = join(migrationsDir, "snapshots");
  const file = (await listDir(dir))
    .filter((name) => name.toLowerCase().endsWith(`${short}.encryption.json`))
    .sort()
    .at(-1);
  if (!file) return undefined;
  return parseEntries(file, await readFile(join(dir, file), "utf8"));
}

/** `tables` without any table's encryption entry. */
function withoutEntries(tables: Record<string, ReadableTable>): Record<string, ReadableTable> {
  return Object.fromEntries(
    Object.entries(tables).map(([name, { encryption: _entry, ...table }]) => [name, table]),
  );
}

/** `tables` with each table's encryption entry from `entries`. */
function withEntries(
  tables: Record<string, ReadableTable>,
  entries: EncryptionEntries,
  version: string,
): Record<string, ReadableTable> {
  const out = { ...tables };
  for (const [name, entry] of Object.entries(entries)) {
    if (!out[name])
      throw new Error(
        `version ${version}: its encryption declarations name table ${name}, which the version doesn't have.`,
      );
    out[name] = { ...out[name], encryption: entry };
  }
  return out;
}

/** Keep the original sealed enum type beside the declaration made by schema.ts. */
function pinEncryptedEnums(
  entries: EncryptionEntries,
  previous: EncryptionEntries,
  sameVersion: boolean,
  steps?: ReadableMigrationSteps,
): void {
  type EnumType = { enum?: string[]; optional?: boolean };
  type EncryptedColumn = EnumType & {
    type?: EnumType;
    sealedType?: EnumType;
    default?: ReadableJson;
  };
  type Entry = { columns: Record<string, EncryptedColumn | string> };
  const appends = (before: EnumType, after: EnumType) => {
    const variants = after.enum;
    return (
      Array.isArray(before.enum) &&
      Array.isArray(variants) &&
      (before.optional ?? false) === (after.optional ?? false) &&
      before.enum.length < variants.length &&
      before.enum.every((variant, index) => variant === variants[index])
    );
  };

  const renames = steps?.renameTables;
  const migrate = steps?.migrate;
  for (const [table, entry] of Object.entries(entries)) {
    const oldTable = renames && Object.hasOwn(renames, table) ? renames[table]! : table;
    if (!Object.hasOwn(previous, oldTable)) continue;
    const before = (previous[oldTable] as unknown as Entry | undefined)?.columns;
    if (!before) continue;
    const current = entry as unknown as Entry;
    const columns = { ...current.columns };
    const columnSteps = migrate && Object.hasOwn(migrate, table) ? migrate[table] : undefined;
    for (const [name, column] of Object.entries(current.columns)) {
      const step = columnSteps && Object.hasOwn(columnSteps, name) ? columnSteps[name] : undefined;
      const renamedFrom = step && "renameFrom" in step ? step.renameFrom : undefined;
      if (steps && renamedFrom === undefined && (oldTable === table || step)) continue;
      const oldName = renamedFrom ?? name;
      const old = Object.hasOwn(before, oldName) ? before[oldName] : undefined;
      if (!old || typeof old !== "object" || typeof column !== "object") continue;
      const oldType = old.type ?? old;
      const { default: defaultValue, ...declared } = column;
      const currentType = declared.type ?? declared;
      if (sameVersion && appends(oldType, currentType)) {
        throw new Error(
          `${table}.${name}: adding variants to an encrypted enum leaves the schema version unchanged. ` +
            "Make a schema change that creates a new version, such as adding an index, " +
            "and run jazz-tools migrations create again.",
        );
      }
      const sealed = old.sealedType ?? oldType;
      if (column.sealedType || !appends(sealed, currentType)) continue;
      columns[name] = {
        type: currentType,
        sealedType: { enum: sealed.enum, ...(sealed.optional ? { optional: true } : {}) },
        ...(defaultValue !== undefined ? { default: defaultValue } : {}),
      };
    }
    entries[table] = { ...current, columns } as ReadableJson;
  }
}

async function checkSnapshotEncryption(
  previous: SnapshotEntry,
  previousEntries: EncryptionEntries,
  currentTables: Record<string, ReadableTable>,
  currentEntries: EncryptionEntries,
): Promise<void> {
  if (Object.keys(previousEntries).length === 0 && Object.keys(currentEntries).length === 0) return;
  const { tables } = await snapshotToVersion(previous.schema);
  const previousTables = withEntries(tables, previousEntries, shortSchemaHash(previous.hash));
  const app = (tables: Record<string, ReadableTable>) =>
    JSON.stringify({
      $schema: "https://jazz.tools/schemas/jazz.app.v1.json",
      tables: withoutTransforms(tables),
    });
  (await jazzNapi()).checkEncryptionMigration(app(previousTables), app(currentTables));
}

export interface MigrationHistory {
  /** Current readable tables with the pins kept in their snapshot. */
  tables: Record<string, ReadableTable>;
  /** Earlier versions, oldest first, by short hash: `jazz.app.json`'s `history`. */
  history: Record<string, ReadableVersion>;
  /** `jazz.app.json`'s `migrations`, in file order. */
  migrations: ReadableMigration[];
  /** The current schema's full hash. */
  current: string;
}

/**
 * The history `jazz.app.json` ships, from `migrations/*.ts` and
 * `migrations/snapshots/*.json`: every version a migration names (but the
 * current one) and the migrations. A version without a snapshot is derived
 * from its neighbour along the migration, through the migration's witness
 * tables, and must hash to the name the migration gives it.
 */
export async function loadMigrationHistory(options: {
  migrationsDir: string;
  /** The current schema's tables, readable (`jazz.app.json`'s `tables`). */
  currentTables: Record<string, ReadableTable>;
  loadModule: ModuleLoader;
}): Promise<MigrationHistory> {
  const current = await snapshotFromVersion(options.currentTables);
  const currentShort = shortSchemaHash(current.hash);
  const currentEntries = encryptionEntries<ReadableJson>(options.currentTables);
  const snapshots = await listSnapshots(options.migrationsDir);
  const files = await loadMigrationFiles(options.migrationsDir, options.loadModule);
  // The current version's snapshot must declare what schema.ts declares:
  // the hash doesn't cover a plaintext type.
  const currentSnapshot = snapshots.filter((snapshot) => snapshot.hash === current.hash).at(-1);
  let tables = options.currentTables;
  if (currentSnapshot) {
    const previousEntries =
      (await snapshotEntries(options.migrationsDir, currentSnapshot, snapshots, files)) ?? {};
    pinEncryptedEnums(currentEntries, previousEntries, true);
    tables = withEntries(tables, currentEntries, currentShort);
    await checkSnapshotEncryption(currentSnapshot, previousEntries, tables, currentEntries);
  }
  if (files.length === 0) return { tables, history: {}, migrations: [], current: current.hash };

  const versions = new Map<string, Record<string, ReadableTable>>();
  versions.set(
    currentShort,
    withEntries((await snapshotToVersion(current.schema)).tables, currentEntries, currentShort),
  );
  for (const snapshot of snapshots) {
    const short = shortSchemaHash(snapshot.hash);
    versions.set(
      short,
      withEntries(
        (await snapshotToVersion(snapshot.schema)).tables,
        (await snapshotEntries(options.migrationsDir, snapshot, snapshots, files)) ?? {},
        short,
      ),
    );
  }

  // Derive what no snapshot holds, walking migrations until nothing changes.
  // A derived version takes its encryption declarations from its own
  // `.encryption.json` when that is kept; in an app that declares
  // encryption, a version without one can't be known to declare none.
  const declaresEncryption =
    Object.keys(currentEntries).length > 0 ||
    snapshots.some((snapshot) => hasSystemTables(snapshot.schema));
  const failures = new Map<string, string>();
  for (let progress = true; progress;) {
    progress = false;
    for (const entry of files) {
      for (const [known, unknown, direction] of [
        [entry.toHash, entry.fromHash, "backward"],
        [entry.fromHash, entry.toHash, "forward"],
      ] as const) {
        if (versions.has(unknown) || !versions.has(known)) continue;
        // A witness table holds what is stored, not an encryption declaration:
        // the version's own `.encryption.json` gives its declarations.
        const kept = await snapshotEntries(options.migrationsDir, unknown, snapshots, files);
        if (!kept && declaresEncryption) {
          failures.set(
            unknown,
            `the app declares encryption, and neither the version's snapshot nor its ` +
              `.encryption.json is there to say which of its tables are encrypted; restore ` +
              `migrations/snapshots/*-${unknown}.json and its .encryption.json from version control`,
          );
          continue;
        }
        const rebuilt = deriveVersion(
          kept ? withoutEntries(versions.get(known)!) : versions.get(known)!,
          entry.migration,
          direction,
        );
        let derived: Record<string, ReadableTable>;
        try {
          derived = kept ? withEntries(rebuilt, kept, unknown) : rebuilt;
        } catch (error) {
          failures.set(unknown, `${entry.file}: ${(error as Error).message}`);
          continue;
        }
        let hash: string;
        try {
          hash = shortSchemaHash((await snapshotFromVersion(derived)).hash);
        } catch (error) {
          failures.set(unknown, `${entry.file}: ${(error as Error).message}`);
          continue;
        }
        if (hash !== unknown) {
          failures.set(
            unknown,
            `${entry.file}: its ${direction === "backward" ? "from" : "to"} tables give version ${hash}, not ${unknown}`,
          );
          continue;
        }
        versions.set(unknown, derived);
        progress = true;
      }
    }
  }

  const history: Record<string, ReadableVersion> = {};
  const migrations: ReadableMigration[] = [];
  for (const entry of files) {
    for (const hash of [entry.fromHash, entry.toHash]) {
      if (hash === currentShort || history[hash]) continue;
      const tables = versions.get(hash);
      if (!tables)
        throw new Error(
          `migrations/${entry.file}: no snapshot of version ${hash} (migrations/snapshots/*-${hash}.json), ` +
            `and it can't be derived from the migrations around it` +
            (failures.has(hash) ? ` (${failures.get(hash)})` : "") +
            ".",
        );
      history[hash] = { tables };
    }
    migrations.push({
      from: entry.fromHash,
      ...(entry.toHash === currentShort ? {} : { to: entry.toHash }),
      ...(entry.migration.steps as Omit<ReadableMigration, "from" | "to">),
    });
  }
  return { tables, history, migrations, current: current.hash };
}

// ============================================================================
// migrations create
// ============================================================================

function createTimestamp(now: Date = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}T${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`;
}

function timestampDate(timestamp: string): Date {
  const m = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})$/.exec(timestamp);
  if (!m) return new Date(0);
  return new Date(Date.UTC(+m[1]!, +m[2]! - 1, +m[3]!, +m[4]!, +m[5]!, +m[6]!));
}

function normalizeMigrationName(name: string): string {
  const normalized = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (normalized.length === 0)
    throw new Error("Migration name must contain at least one ASCII letter or digit.");
  return normalized;
}

/**
 * Whether `to` needs a migration from `from`: Rust's `schemasAreCompatible`
 * (jazz-napi), the rule a deployment uses to connect versions without one.
 */
async function requiresMigration(from: WasmSchema, to: WasmSchema): Promise<boolean> {
  const napi = await jazzNapi();
  return !napi.schemasAreCompatible(
    JSON.stringify({ tables: from }),
    JSON.stringify({ tables: to }),
  );
}

function snapshotText(schema: WasmSchema): string {
  return `${JSON.stringify(schema, null, 2)}\n`;
}

export interface CreateMigrationResult {
  /** Whether anything was written. */
  changed: boolean;
  /** The first snapshot of a project without one: nothing to diff against yet. */
  initial?: boolean;
  /** Rows of the previous version read as the new one unchanged: only a snapshot was written. */
  migrationRequired?: boolean;
  fromHash?: string;
  toHash?: string;
  /** The migration stub written, relative to `migrationsDir`. */
  file?: string;
  /** The snapshot written, relative to `migrationsDir`. */
  snapshot?: string;
  /** The stub's TODO lines, left for the author. */
  todos?: string[];
}

/**
 * `jazz-tools migrations create`: diff the current schema against the
 * latest snapshot and, when rows need a step, write the snapshot of the
 * current schema and a migration stub (.64's shape) to fill in.
 */
export async function createMigration(options: {
  migrationsDir: string;
  currentTables: Record<string, ReadableTable>;
  name?: string;
  loadModule: ModuleLoader;
  now?: Date;
}): Promise<CreateMigrationResult> {
  const { migrationsDir } = options;
  const current = await snapshotFromVersion(options.currentTables);
  const currentEntries = encryptionEntries<ReadableJson>(options.currentTables);
  const snapshots = await listSnapshots(migrationsDir);
  const timestamped = snapshots
    .filter((entry) => entry.timestamp)
    .sort((a, b) => (a.file < b.file ? -1 : 1));
  let baseline: SnapshotEntry | undefined = timestamped.at(-1);
  let files: LoadedMigrationFile[] | undefined;
  if (!baseline) {
    // Snapshots .64 named by hash alone: the version the last migration leads to.
    files = await loadMigrationFiles(migrationsDir, options.loadModule);
    const last = files.at(-1)?.toHash;
    baseline =
      (last && snapshots.find((entry) => shortSchemaHash(entry.hash) === last)) ||
      (snapshots.length === 1 ? snapshots[0] : undefined);
  }
  const latestTimestamp = timestamped.at(-1)?.timestamp;
  let timestamp = createTimestamp(options.now);
  if (latestTimestamp && timestamp <= latestTimestamp)
    timestamp = createTimestamp(new Date(timestampDate(latestTimestamp).getTime() + 1000));
  const toShort = shortSchemaHash(current.hash);
  const snapshotFile = `${timestamp}-${toShort}.json`;
  const writeSnapshot = async () => {
    await mkdir(join(migrationsDir, "snapshots"), { recursive: true });
    await writeFile(join(migrationsDir, "snapshots", snapshotFile), snapshotText(current.schema));
    if (Object.keys(currentEntries).length > 0)
      await writeFile(
        join(migrationsDir, "snapshots", entriesFile(snapshotFile)),
        encryptionEntriesText(currentEntries),
      );
    return `snapshots/${snapshotFile}`;
  };

  if (!baseline) {
    return { changed: true, initial: true, toHash: current.hash, snapshot: await writeSnapshot() };
  }
  if (!files && hasSystemTables(baseline.schema))
    files = await loadMigrationFiles(migrationsDir, options.loadModule, true);
  const previousEntries =
    (await snapshotEntries(migrationsDir, baseline, snapshots, files ?? [])) ?? {};
  pinEncryptedEnums(currentEntries, previousEntries, baseline.hash === current.hash);
  let diff: MigrationDiff | undefined;
  let checkedEntries = currentEntries;
  if (
    Object.keys(previousEntries).length > 0 &&
    Object.keys(currentEntries).some((table) => !Object.hasOwn(baseline.schema, table))
  ) {
    diff = await migrationDiff(baseline.schema, current.schema);
    // Preflight checks the stub's suggested renames. The snapshot keeps the
    // declarations until the author chooses which renames to keep.
    checkedEntries = { ...currentEntries };
    pinEncryptedEnums(checkedEntries, previousEntries, false, {
      renameTables: Object.fromEntries(diff.renameTables.map(({ from, to }) => [to, from])),
    });
  }
  await checkSnapshotEncryption(
    baseline,
    previousEntries,
    withEntries(options.currentTables, checkedEntries, toShort),
    checkedEntries,
  );
  if (baseline.hash === current.hash) return { changed: false };
  if (!(await requiresMigration(baseline.schema, current.schema))) {
    return {
      changed: true,
      migrationRequired: false,
      fromHash: baseline.hash,
      toHash: current.hash,
      snapshot: await writeSnapshot(),
    };
  }
  const name = options.name ? normalizeMigrationName(options.name) : "unnamed";
  const file = `${timestamp}-${name}-${shortSchemaHash(baseline.hash)}-${toShort}.ts`;
  const stub = renderMigrationStub(
    {
      fromHash: baseline.hash,
      toHash: current.hash,
      fromSchema: baseline.schema,
      toSchema: current.schema,
      toEncryption: currentEntries,
    },
    diff ?? (await migrationDiff(baseline.schema, current.schema)),
  );
  await mkdir(migrationsDir, { recursive: true });
  await writeFile(join(migrationsDir, file), stub);
  const snapshot = await writeSnapshot();
  return {
    changed: true,
    migrationRequired: true,
    fromHash: baseline.hash,
    toHash: current.hash,
    file,
    snapshot,
    todos: stub.split("\n").filter((line) => UNFILLED_STUB_TODO.test(line)),
  };
}
