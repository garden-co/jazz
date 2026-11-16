import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { structuralBigInt } from "../runtime/structural-values.js";
import { isDefinedMigration, witnessColumns, type DefinedMigration } from "../migrations.js";
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
import { columnTypeSignature, shortSchemaHash, tableSchemasEqual } from "./schema-utils.js";
import { enumVariantChange, onlyAddsEnumVariants } from "../runtime/schema-metadata.js";

function changedTableNames(fromSchema: WasmSchema, toSchema: WasmSchema): string[] {
  const names = new Set([...Object.keys(fromSchema), ...Object.keys(toSchema)]);
  return [...names].filter(
    (tableName) => !tableSchemasEqual(fromSchema[tableName], toSchema[tableName]),
  );
}

type TableRenameSuggestion = {
  oldTableName: string;
  newTableName: string;
};

function detectPossibleTableRenames(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): TableRenameSuggestion[] {
  const removedTables = Object.keys(fromSchema)
    .filter((tableName) => !toSchema[tableName])
    .sort();
  const addedTables = Object.keys(toSchema)
    .filter((tableName) => !fromSchema[tableName])
    .sort();
  const matches = removedTables
    .map((oldTableName) => {
      const candidateAddedTables = addedTables.filter((newTableName) =>
        tableSchemasEqual(fromSchema[oldTableName], toSchema[newTableName]),
      );
      return candidateAddedTables.length === 1
        ? ([oldTableName, candidateAddedTables[0]!] as const)
        : undefined;
    })
    .filter((match) => match !== undefined);

  return matches.flatMap(([oldTableName, newTableName], i) => {
    const isDuplicateNewTableMatch = matches.some(([_, otherNewTableName], j) => {
      return i !== j && newTableName === otherNewTableName;
    });
    return !isDuplicateNewTableMatch ? [{ oldTableName, newTableName }] : [];
  });
}

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

type TableSuggestion = {
  tableName: string;
  comments: string[];
  properties: string[];
};

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

// A column added in another's place starts every existing row at its
// default: say how to keep the values instead of suggesting a swap that
// silently drops them.
const KEEP_VALUES_ADVICE =
  "A migration can add, drop or rename a column but not convert its values, and a column " +
  "added in its place starts every existing row at its default. To keep the values, add " +
  "the new column under another name next to this one, copy the values into it from your " +
  "backend, and drop this column in a later release. Clients on the older schema keep " +
  "writing this column until then, so copy again before you drop it.";

/**
 * The TODOs a generated stub leaves where a step needs the author's value:
 * a migration file that still holds one does not reproduce its target
 * schema, so it cannot deploy.
 */
export const UNFILLED_STUB_TODO =
  /\/\/ TODO: ((?:Added|Removed)(?: required)? column "([^"]+)" (?:needs an explicit (?:default|backwardsDefault)|may be a rename of [^:]+:[^\n]*)\.)/;

/**
 * A column kept under the same name must keep its type and optionality: no
 * migration step changes them in place, so a stub for such a change could
 * never typecheck (#2163). That includes a JSON column's schema. An enum
 * column (or an array of enums) may gain variants at the end of its list,
 * since the rows it holds pass unchanged and sort as before, but not lose
 * one (renaming or removing a variant needs a value mapping no migration
 * step has yet) or reorder them. Refuse those with the supported
 * alternative.
 */
function refuseUnsupportedSharedColumnChanges(
  tableName: string,
  fromTable: WasmSchema[string],
  toTable: WasmSchema[string],
): void {
  for (const column of toTable.columns) {
    const source = fromTable.columns.find((candidate) => candidate.name === column.name);
    if (!source) continue;
    const name = `${tableName}.${column.name}`;
    if (source.nullable !== column.nullable) {
      throw new Error(
        `Changing column ${JSON.stringify(name)} from ${source.nullable ? "optional" : "required"} ` +
          `to ${column.nullable ? "optional" : "required"} is not supported yet. ` +
          KEEP_VALUES_ADVICE,
      );
    }
    if (columnTypeSignature(source.column_type) !== columnTypeSignature(column.column_type)) {
      if (onlyAddsEnumVariants(source.column_type, column.column_type)) continue;
      const enumChange = enumVariantChange(source.column_type, column.column_type);
      if (enumChange && enumChange.dropped.length === 0) {
        throw new Error(
          `Changing the order of the enum variants of column ${JSON.stringify(name)} is not ` +
            `supported: add new variants at the end of the list. A variant's position is its ` +
            `sort order, so moving one would change how existing rows sort and rewrite every ` +
            `stored value of the column.`,
        );
      }
      if (enumChange) {
        const dropped = enumChange.dropped.map((variant) => JSON.stringify(variant)).join(", ");
        throw new Error(
          `Changing the enum variants of column ${JSON.stringify(name)} is not supported yet: ` +
            `it no longer declares ${dropped}. A migration can add enum variants, but renaming ` +
            `or removing one is planned for a later release. ` +
            KEEP_VALUES_ADVICE,
        );
      }
      // A migration can only add, drop or rename a column, so a column's
      // JSON Schema is part of its type too.
      const change =
        source.column_type.type === "Json" && column.column_type.type === "Json"
          ? "the JSON Schema of"
          : "the type of";
      throw new Error(
        `Changing ${change} column ${JSON.stringify(name)} is not supported yet. ` +
          KEEP_VALUES_ADVICE,
      );
    }
  }
}

/**
 * The kept columns that gain enum variants, as notes for the stub: their
 * rows pass unchanged, so they need no step.
 */
function enumVariantAdditionNotes(fromTable: WasmSchema[string], toTable: WasmSchema[string]) {
  return toTable.columns.flatMap((column) => {
    const source = fromTable.columns.find((candidate) => candidate.name === column.name);
    if (!source) return [];
    const change = enumVariantChange(source.column_type, column.column_type);
    if (
      !change ||
      change.added.length === 0 ||
      !onlyAddsEnumVariants(source.column_type, column.column_type)
    )
      return [];
    const added = change.added.map((variant) => JSON.stringify(variant)).join(", ");
    return [
      `Column ${JSON.stringify(column.name)} gains enum variant${change.added.length === 1 ? "" : "s"} ` +
        `${added}: existing rows are unchanged, so it needs no step.`,
    ];
  });
}

function inferTableSuggestions(
  tableName: string,
  fromTable: WasmSchema[string],
  toTable: WasmSchema[string],
): TableSuggestion {
  const fromColumns = new Map(fromTable.columns.map((column) => [column.name, column]));
  const toColumns = new Map(toTable.columns.map((column) => [column.name, column]));
  const comments: string[] = [];
  const properties: string[] = [];

  const removedColumns = [...fromColumns.keys()].filter((name) => !toColumns.has(name));
  const addedColumns = [...toColumns.keys()].filter((name) => !fromColumns.has(name));

  // An added column with the same shape as a removed one may be that column
  // renamed. Adding one and dropping the other would start every row empty,
  // so neither gets a step: the author says which it is, and validate
  // refuses the file until they do.
  const sameShape = (removed: ColumnDescriptor, added: ColumnDescriptor) =>
    removed.nullable === added.nullable &&
    removed.references === added.references &&
    columnTypeSignature(removed.column_type) === columnTypeSignature(added.column_type);
  const undecided = new Set<string>();
  for (const addedName of addedColumns) {
    const added = toColumns.get(addedName)!;
    const candidates = removedColumns.filter((name) => sameShape(fromColumns.get(name)!, added));
    if (candidates.length === 0) continue;
    undecided.add(addedName);
    for (const name of candidates) undecided.add(name);
    const of = candidates.map((name) => JSON.stringify(name)).join(" or ");
    comments.push(
      `Added column ${JSON.stringify(addedName)} may be a rename of ${of}: to keep its values, ` +
        `give it the step m.renameFrom(${JSON.stringify(candidates[0])}); to start it empty, ` +
        `add it with m.add and drop the old column with m.drop.`,
    );
  }

  for (const columnName of addedColumns) {
    if (undecided.has(columnName)) continue;
    const column = toColumns.get(columnName)!;
    if (column.nullable || column.default !== undefined) {
      // Existing rows take the column's declared default.
      const fill = column.default === undefined ? "null" : defaultExpression(column.default);
      properties.push(
        `${JSON.stringify(columnName)}: ${renderColumnOperationExpression("add", column, fill)},`,
      );
    } else {
      comments.push(
        `Added required column ${JSON.stringify(columnName)} needs an explicit default.`,
      );
    }
  }

  for (const columnName of removedColumns) {
    if (undecided.has(columnName)) continue;
    const column = fromColumns.get(columnName)!;
    if (column.nullable || column.default !== undefined) {
      // Older clients read the dropped column back as its declared default.
      const fill = column.default === undefined ? "null" : defaultExpression(column.default);
      properties.push(
        `${JSON.stringify(columnName)}: ${renderColumnOperationExpression("drop", column, fill)},`,
      );
    } else {
      comments.push(
        `Removed required column ${JSON.stringify(columnName)} needs an explicit backwardsDefault.`,
      );
    }
  }

  return {
    tableName,
    comments,
    properties,
  };
}

function renderMigrationBody(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): {
  migrateBody?: string;
  renameTablesBody?: string;
  createTablesBody?: string;
  dropTablesBody?: string;
  witnessFrom: WasmSchema;
  witnessTo: WasmSchema;
} {
  const renameSuggestions = detectPossibleTableRenames(fromSchema, toSchema);
  const renamedOldTables = new Set(renameSuggestions.map((suggestion) => suggestion.oldTableName));
  const renamedNewTables = new Set(renameSuggestions.map((suggestion) => suggestion.newTableName));
  const addedTables = Object.keys(toSchema)
    .filter((tableName) => !fromSchema[tableName])
    .sort();
  const removedTables = Object.keys(fromSchema)
    .filter((tableName) => !toSchema[tableName])
    .sort();
  const explicitAddedTables = addedTables.filter((tableName) => !renamedNewTables.has(tableName));
  const explicitRemovedTables = removedTables.filter(
    (tableName) => !renamedOldTables.has(tableName),
  );
  const changedTables = changedTableNames(fromSchema, toSchema);
  const migratableTables = changedTables.filter(
    (tableName) => fromSchema[tableName] !== undefined && toSchema[tableName] !== undefined,
  );
  const witnessFromTables = [...migratableTables, ...explicitRemovedTables];
  const witnessToTables = [...migratableTables, ...explicitAddedTables];
  for (const renameSuggestion of renameSuggestions) {
    witnessFromTables.push(renameSuggestion.oldTableName);
    witnessToTables.push(renameSuggestion.newTableName);
  }
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

  for (const tableName of migratableTables) {
    const fromTable = fromSchema[tableName]!;
    const toTable = toSchema[tableName]!;

    refuseUnsupportedSharedColumnChanges(tableName, fromTable, toTable);
    const suggestion = inferTableSuggestions(tableName, fromTable, toTable);
    lines.push(`${JSON.stringify(tableName)}: {`);
    for (const comment of suggestion.comments) {
      lines.push(`  // TODO: ${comment}`);
    }
    for (const property of suggestion.properties) {
      lines.push(`  ${property}`);
    }
    const enumNotes = enumVariantAdditionNotes(fromTable, toTable);
    for (const note of enumNotes) {
      lines.push(`  // ${note}`);
    }
    if (
      suggestion.comments.length === 0 &&
      suggestion.properties.length === 0 &&
      enumNotes.length === 0
    ) {
      const referenceAddition = toTable.columns.some(
        (column) =>
          column.references &&
          fromTable.columns.some(
            (source) =>
              source.name === column.name &&
              !source.references &&
              source.nullable === column.nullable &&
              source.merge_strategy === column.merge_strategy &&
              columnTypeSignature(source.column_type) === columnTypeSignature(column.column_type),
          ),
      );
      lines.push(
        referenceAddition &&
          fromTable.columns.length === toTable.columns.length &&
          fromTable.columns.every((source) =>
            toTable.columns.some(
              (column) =>
                column.name === source.name &&
                (!source.references || source.references === column.references) &&
                source.nullable === column.nullable &&
                source.merge_strategy === column.merge_strategy &&
                columnTypeSignature(source.column_type) === columnTypeSignature(column.column_type),
            ),
          )
          ? "  // Add reference metadata with an identity lens; existing UUID values are preserved."
          : "  // TODO: No safe migration steps were inferred automatically.",
      );
    }
    lines.push("},");
    lines.push("");
  }

  if (lines.length === 0) {
    if (
      renameSuggestions.length === 0 &&
      explicitAddedTables.length === 0 &&
      explicitRemovedTables.length === 0
    ) {
      lines.push(
        changedTables.length === 0
          ? "// TODO: No schema differences were detected."
          : "// TODO: No column-level migration steps were required for the detected schema changes.",
      );
    }
  }

  return {
    migrateBody: lines.length > 0 ? lines.join("\n").trimEnd() : undefined,
    createTablesBody:
      explicitAddedTables.length > 0
        ? explicitAddedTables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
        : undefined,
    dropTablesBody:
      explicitRemovedTables.length > 0
        ? explicitRemovedTables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
        : undefined,
    renameTablesBody:
      renameSuggestions.length > 0
        ? renameSuggestions
            .map(
              (renameSuggestion) =>
                `${renameSuggestion.newTableName}: m.renameTableFrom(${JSON.stringify(renameSuggestion.oldTableName)}),`,
            )
            .join("\n")
        : undefined,
    witnessFrom,
    witnessTo,
  };
}

export function renderMigrationStub(input: {
  fromHash: string;
  toHash: string;
  fromSchema: WasmSchema;
  toSchema: WasmSchema;
}): string {
  const rendered = renderMigrationBody(input.fromSchema, input.toSchema);
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

const ENGINE_TYPE_NAMES: Record<string, string> = {
  Text: "string",
  Integer: "int",
  BigInt: "bigint",
  Double: "float",
  Boolean: "boolean",
  Timestamp: "timestamp",
  Uuid: "uuid",
  Bytea: "bytes",
};

function engineType(type: WasmColumnType): ReadableJson {
  const name = ENGINE_TYPE_NAMES[type.type];
  if (name) return name;
  switch (type.type) {
    case "Enum":
      return { enum: [...type.variants] };
    case "Json":
      return { json: (type.schema as ReadableJson | undefined) ?? true };
    case "Array":
      return { array: engineType(type.element) };
    default:
      throw new Error(`a ${type.type} column can't be written to jazz.app.json`);
  }
}

function base64(bytes: ArrayLike<number>): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
  return btoa(binary);
}

function engineValue(value: Value, type: WasmColumnType): ReadableJson {
  switch (value.type) {
    case "Null":
      return null;
    case "Integer":
    case "Double":
    case "Timestamp":
      return Number(value.value);
    case "BigInt":
      return String(structuralBigInt(value.value));
    case "Boolean":
      return value.value;
    case "Text":
      return type.type === "Json" ? (JSON.parse(value.value) as ReadableJson) : value.value;
    case "Uuid":
      return value.value;
    case "Bytea": {
      const bytes = value.value as unknown;
      if (typeof bytes === "string") return bytes;
      return base64(bytes as ArrayLike<number>);
    }
    case "Array": {
      const element = type.type === "Array" ? type.element : type;
      return value.value.map((item) => engineValue(item, element));
    }
    default:
      throw new Error(`a ${value.type} default can't be written to jazz.app.json`);
  }
}

const MERGE_NAMES: Record<string, string> = {
  Counter: "counter",
  GSet: "g-set",
  Text: "text",
  Markdown: "markdown",
  Html: "html",
};

function engineColumn(column: ColumnDescriptor): ReadableJson {
  const type = engineType(column.column_type);
  const plain =
    column.default === undefined && column.merge_strategy === undefined && !column.references;
  if (plain && typeof type === "string") return column.nullable ? `${type}?` : type;
  return {
    ...(typeof type === "string" ? { type } : (type as Record<string, ReadableJson>)),
    ...(column.nullable ? { optional: true } : {}),
    ...(column.default === undefined
      ? {}
      : { default: engineValue(column.default, column.column_type) }),
    ...(column.merge_strategy ? { merge: MERGE_NAMES[column.merge_strategy]! } : {}),
    ...(column.references ? { references: column.references } : {}),
  };
}

/** A snapshot (engine schema) as a readable history version: columns with `references`, no relations. */
export function snapshotToVersion(schema: WasmSchema): ReadableVersion {
  const tables: Record<string, ReadableTable> = {};
  for (const [name, table] of Object.entries(schema)) {
    const entry: ReadableTable = {
      columns: Object.fromEntries(
        table.columns.map((column) => [column.name, engineColumn(column)]),
      ),
    };
    const indexes: ReadableJson[] = [
      ...(table.indexed_columns ?? []),
      ...(table.composite_indexes ?? []).map((index) => [...index]),
    ];
    if (indexes.length > 0) entry.indexes = indexes;
    if (table.branchBy?.length) entry.branchBy = [...table.branchBy];
    if (table.history) entry.history = table.history;
    tables[name] = entry;
  }
  return { tables };
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

export interface MigrationHistory {
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
  const files = await loadMigrationFiles(options.migrationsDir, options.loadModule);
  if (files.length === 0) return { history: {}, migrations: [], current: current.hash };

  const versions = new Map<string, Record<string, ReadableTable>>();
  versions.set(currentShort, snapshotToVersion(current.schema).tables);
  for (const snapshot of await listSnapshots(options.migrationsDir))
    versions.set(shortSchemaHash(snapshot.hash), snapshotToVersion(snapshot.schema).tables);

  // Derive what no snapshot holds, walking migrations until nothing changes.
  const failures = new Map<string, string>();
  for (let progress = true; progress; ) {
    progress = false;
    for (const entry of files) {
      for (const [known, unknown, direction] of [
        [entry.toHash, entry.fromHash, "backward"],
        [entry.fromHash, entry.toHash, "forward"],
      ] as const) {
        if (versions.has(unknown) || !versions.has(known)) continue;
        const derived = deriveVersion(versions.get(known)!, entry.migration, direction);
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
  return { history, migrations, current: current.hash };
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
  const snapshots = await listSnapshots(migrationsDir);
  const timestamped = snapshots
    .filter((entry) => entry.timestamp)
    .sort((a, b) => (a.file < b.file ? -1 : 1));
  let baseline: SnapshotEntry | undefined = timestamped.at(-1);
  if (!baseline) {
    // Snapshots .64 named by hash alone: the version the last migration leads to.
    const files = await loadMigrationFiles(migrationsDir, options.loadModule);
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
    return `snapshots/${snapshotFile}`;
  };

  if (!baseline) {
    return { changed: true, initial: true, toHash: current.hash, snapshot: await writeSnapshot() };
  }
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
  const stub = renderMigrationStub({
    fromHash: baseline.hash,
    toHash: current.hash,
    fromSchema: baseline.schema,
    toSchema: current.schema,
  });
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
