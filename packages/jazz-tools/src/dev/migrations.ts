import { structuralBigInt } from "../runtime/structural-values.js";
import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
  Value,
} from "../drivers/types.js";
import { columnTypeSignature, shortSchemaHash, tableSchemasEqual } from "./schema-utils.js";

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
    case "EnumPayload":
      throw new Error("Migration stub generation does not yet support payload enums.");
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
      const branch =
        tableSchema.branchBy === undefined
          ? ""
          : `.branchBy(${JSON.stringify(tableSchema.branchBy)})`;
      return `${JSON.stringify(tableName)}: s.table({\n${indentBlock(columnLines.join("\n"), 2)}\n}, {\n${indentBlock(relationLines.join("\n"), 2)}\n})${index}${branch}`;
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

function renderArrayElementExpression(columnType: WasmColumnType): string {
  return baseBuilderExpression(columnType);
}

function renderAddOperationExpression(column: ColumnDescriptor, defaultExpression: string): string {
  switch (column.column_type.type) {
    case "Text":
      return `s.add.string({ default: ${defaultExpression} })`;
    case "Boolean":
      return `s.add.boolean({ default: ${defaultExpression} })`;
    case "Integer":
      return `s.add.int({ default: ${defaultExpression} })`;
    case "Double":
      return `s.add.float({ default: ${defaultExpression} })`;
    case "Timestamp":
      return `s.add.timestamp({ default: ${defaultExpression} })`;
    case "Bytea":
      return `s.add.bytes({ default: ${defaultExpression} })`;
    case "Json":
      return column.column_type.schema
        ? `s.add.json({ default: ${defaultExpression}, schema: ${jsonSchemaExpression(column.column_type.schema)} })`
        : `s.add.json({ default: ${defaultExpression} })`;
    case "Enum":
      return `s.add.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { default: ${defaultExpression} })`;
    case "EnumPayload":
      throw new Error("Migration stub generation does not yet support payload enums.");
    case "Uuid":
      if (column.references) {
        return `s.add.ref(${JSON.stringify(column.references)}, { default: ${defaultExpression} })`;
      }
      return `s.add.ref("TODO_TABLE", { default: ${defaultExpression} })`;
    case "Array":
      return `s.add.array({ of: ${renderArrayElementExpression(column.column_type.element)}, default: ${defaultExpression} })`;
    case "BigInt":
      return `s.add.bigint({ default: ${defaultExpression} })`;
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function renderDropOperationExpression(
  column: ColumnDescriptor,
  defaultExpression: string,
): string {
  switch (column.column_type.type) {
    case "Text":
      return `s.drop.string({ backwardsDefault: ${defaultExpression} })`;
    case "Boolean":
      return `s.drop.boolean({ backwardsDefault: ${defaultExpression} })`;
    case "Integer":
      return `s.drop.int({ backwardsDefault: ${defaultExpression} })`;
    case "Double":
      return `s.drop.float({ backwardsDefault: ${defaultExpression} })`;
    case "Timestamp":
      return `s.drop.timestamp({ backwardsDefault: ${defaultExpression} })`;
    case "Bytea":
      return `s.drop.bytes({ backwardsDefault: ${defaultExpression} })`;
    case "Json":
      return column.column_type.schema
        ? `s.drop.json({ backwardsDefault: ${defaultExpression}, schema: ${jsonSchemaExpression(column.column_type.schema)} })`
        : `s.drop.json({ backwardsDefault: ${defaultExpression} })`;
    case "Enum":
      return `s.drop.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { backwardsDefault: ${defaultExpression} })`;
    case "EnumPayload":
      throw new Error("Migration stub generation does not yet support payload enums.");
    case "Uuid":
      if (column.references) {
        return `s.drop.ref(${JSON.stringify(column.references)}, { backwardsDefault: ${defaultExpression} })`;
      }
      return `s.drop.ref("TODO_TABLE", { backwardsDefault: ${defaultExpression} })`;
    case "Array":
      return `s.drop.array({ of: ${renderArrayElementExpression(column.column_type.element)}, backwardsDefault: ${defaultExpression} })`;
    case "BigInt":
      return `s.drop.bigint({ backwardsDefault: ${defaultExpression} })`;
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
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

  if (removedColumns.length === 1 && addedColumns.length === 1) {
    const removed = fromColumns.get(removedColumns[0]!)!;
    const added = toColumns.get(addedColumns[0]!)!;
    if (
      removed.nullable === added.nullable &&
      removed.references === added.references &&
      columnTypeSignature(removed.column_type) === columnTypeSignature(added.column_type)
    ) {
      comments.push(
        `Possible rename detected: ${JSON.stringify(removed.name)} -> ${JSON.stringify(added.name)}.`,
      );
    }
  }

  for (const columnName of addedColumns) {
    const column = toColumns.get(columnName)!;
    if (column.nullable) {
      properties.push(
        `${JSON.stringify(columnName)}: ${renderAddOperationExpression(column, "null")},`,
      );
    } else {
      comments.push(
        `Added required column ${JSON.stringify(columnName)} needs an explicit default.`,
      );
    }
  }

  for (const columnName of removedColumns) {
    const column = fromColumns.get(columnName)!;
    if (column.nullable) {
      properties.push(
        `${JSON.stringify(columnName)}: ${renderDropOperationExpression(column, "null")},`,
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

    const suggestion = inferTableSuggestions(tableName, fromTable, toTable);
    lines.push(`${JSON.stringify(tableName)}: {`);
    for (const comment of suggestion.comments) {
      lines.push(`  // TODO: ${comment}`);
    }
    for (const property of suggestion.properties) {
      lines.push(`  ${property}`);
    }
    if (suggestion.comments.length === 0 && suggestion.properties.length === 0) {
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
                `${renameSuggestion.newTableName}: s.renameTableFrom(${JSON.stringify(renameSuggestion.oldTableName)}),`,
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

  return `import { schema as s } from "jazz-tools";

export default s.defineMigration({
${sections.join("\n")}
});
`;
}
