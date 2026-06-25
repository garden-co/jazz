import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
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
  const uniqueTableNames = [...new Set(tableNames)];
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

function baseBuilderExpression(columnType: WasmColumnType, references?: string): string {
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
      return columnType.schema ? `s.json(${JSON.stringify(columnType.schema)})` : "s.json()";
    case "Enum":
      return `s.enum(${columnType.variants.map((variant) => JSON.stringify(variant)).join(", ")})`;
    case "Uuid":
      if (!references) {
        throw new Error("Migration stub generation does not yet support bare UUID columns.");
      }
      return `s.ref(${JSON.stringify(references)})`;
    case "Array":
      return `s.array(${baseBuilderExpression(columnType.element, references)})`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function builderExpressionForColumn(column: ColumnDescriptor): string {
  const base = baseBuilderExpression(column.column_type, column.references);
  const withOptional = column.nullable ? `${base}.optional()` : base;
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
      return `${JSON.stringify(tableName)}: s.table({\n${indentBlock(columnLines.join("\n"), 2)}\n})`;
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

function renderArrayElementExpression(columnType: WasmColumnType, references?: string): string {
  return baseBuilderExpression(columnType, references);
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
        ? `s.add.json({ default: ${defaultExpression}, schema: ${JSON.stringify(column.column_type.schema)} })`
        : `s.add.json({ default: ${defaultExpression} })`;
    case "Enum":
      return `s.add.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { default: ${defaultExpression} })`;
    case "Uuid":
      if (column.references) {
        return `s.add.ref(${JSON.stringify(column.references)}, { default: ${defaultExpression} })`;
      }
      return `s.add.ref("TODO_TABLE", { default: ${defaultExpression} })`;
    case "Array":
      return `s.add.array({ of: ${renderArrayElementExpression(column.column_type.element, column.references)}, default: ${defaultExpression} })`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
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
        ? `s.drop.json({ backwardsDefault: ${defaultExpression}, schema: ${JSON.stringify(column.column_type.schema)} })`
        : `s.drop.json({ backwardsDefault: ${defaultExpression} })`;
    case "Enum":
      return `s.drop.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { backwardsDefault: ${defaultExpression} })`;
    case "Uuid":
      if (column.references) {
        return `s.drop.ref(${JSON.stringify(column.references)}, { backwardsDefault: ${defaultExpression} })`;
      }
      return `s.drop.ref("TODO_TABLE", { backwardsDefault: ${defaultExpression} })`;
    case "Array":
      return `s.drop.array({ of: ${renderArrayElementExpression(column.column_type.element, column.references)}, backwardsDefault: ${defaultExpression} })`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
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
  const witnessFrom = pickWitnessSchema(fromSchema, witnessFromTables);
  const witnessTo = pickWitnessSchema(toSchema, witnessToTables);
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
      lines.push("  // TODO: No safe migration steps were inferred automatically.");
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
