import { initGetFieldName, initGetModelName } from "better-auth/adapters";
import type { DBAdapterSchemaCreation } from "better-auth";
import type { BetterAuthDBSchema, DBFieldAttribute } from "better-auth/db";
import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { assertUserColumnNameAllowed } from "../magic-columns.js";
import { assertSchemaNameAllowed } from "../schema-name.js";

const DEFAULT_SCHEMA_FILE_PATH = "./schema-better-auth/schema.ts";
const JS_IDENTIFIER_PATTERN = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

interface SchemaNameResolvers {
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}

function toJazzColumnType(field: DBFieldAttribute): ColumnType {
  if (Array.isArray(field.type)) {
    return { type: "Enum", variants: [...field.type] };
  }

  switch (field.type) {
    case "string":
      return { type: "Text" };
    case "number":
      return field.bigint ? { type: "BigInt" } : { type: "Integer" };
    case "boolean":
      return { type: "Boolean" };
    case "date":
      return { type: "Timestamp" };
    case "json":
      return { type: "Json" };
    case "string[]":
      return { type: "Array", element: { type: "Text" } };
    case "number[]":
      return {
        type: "Array",
        element: field.bigint ? { type: "BigInt" } : { type: "Integer" },
      };
    default:
      throw new Error(`Unsupported Better Auth field type: ${String(field.type)}`);
  }
}

function toJazzReferenceColumn(args: {
  modelName: string;
  fieldName: string;
  storedFieldName: string;
  field: DBFieldAttribute;
  getModelName: (model: string) => string;
}): { columnType: ColumnType; references: string } {
  const { modelName, fieldName, field, getModelName } = args;
  const reference = field.references;

  if (!reference) {
    throw new Error(`Field "${modelName}.${fieldName}" is missing Better Auth reference metadata.`);
  }

  if (reference.field !== "id") {
    throw new Error(
      `Field "${modelName}.${fieldName}" references "${reference.model}.${reference.field}", but Jazz schema generation only supports references to "id".`,
    );
  }

  const references = getModelName(reference.model);

  switch (field.type) {
    case "string":
      return {
        columnType: { type: "Uuid" },
        references,
      };
    case "string[]":
      return {
        columnType: {
          type: "Array",
          element: { type: "Uuid" },
        },
        references,
      };
    case "number":
    case "number[]":
      throw new Error(
        `Field "${modelName}.${fieldName}" cannot be emitted as a Jazz reference because Better Auth references must use string or string[] fields.`,
      );
    default:
      throw new Error(
        `Field "${modelName}.${fieldName}" uses unsupported Better Auth reference type: ${String(field.type)}.`,
      );
  }
}

function assertStoredFieldNameAllowed(args: {
  modelName: string;
  fieldName: string;
  storedFieldName: string;
}): void {
  const { modelName, fieldName, storedFieldName } = args;

  if (storedFieldName === "id") {
    throw new Error(`Field "${modelName}.${fieldName}" conflicts with reserved Jazz row id.`);
  }

  assertUserColumnNameAllowed(storedFieldName);
}

function formatStringLiteral(value: string): string {
  return JSON.stringify(value);
}

function formatObjectKey(key: string): string {
  return JS_IDENTIFIER_PATTERN.test(key) ? key : JSON.stringify(key);
}

function formatPropertyAccess(objectName: string, key: string): string {
  return JS_IDENTIFIER_PATTERN.test(key)
    ? `${objectName}.${key}`
    : `${objectName}[${formatStringLiteral(key)}]`;
}

function withOptionalSuffix(expression: string, field: DBFieldAttribute): string {
  return field.required === false ? `${expression}.optional()` : expression;
}

function toJazzSchemaReferenceExpression(args: {
  modelName: string;
  fieldName: string;
  storedFieldName: string;
  field: DBFieldAttribute;
  getModelName: (model: string) => string;
}): string {
  const { modelName, fieldName, field } = args;
  const reference = field.references;

  if (!reference) {
    throw new Error(`Field "${modelName}.${fieldName}" is missing Better Auth reference metadata.`);
  }

  if (reference.field !== "id") {
    throw new Error(
      `Field "${modelName}.${fieldName}" references "${reference.model}.${reference.field}", but Jazz schema.ts only supports references to "id".`,
    );
  }

  switch (field.type) {
    case "string": {
      return withOptionalSuffix("s.uuid()", field);
    }
    case "string[]": {
      return withOptionalSuffix("s.array(s.uuid())", field);
    }
    case "number":
    case "number[]":
      throw new Error(
        `Field "${modelName}.${fieldName}" cannot be emitted as a Jazz reference because Better Auth references must use string or string[] fields.`,
      );
    default:
      throw new Error(
        `Field "${modelName}.${fieldName}" uses unsupported Better Auth reference type: ${String(field.type)}.`,
      );
  }
}

function toJazzSchemaColumnExpression(args: {
  modelName: string;
  fieldName: string;
  storedFieldName: string;
  field: DBFieldAttribute;
  getModelName: (model: string) => string;
}): string {
  const { modelName, fieldName, storedFieldName, field, getModelName } = args;

  if (field.references) {
    return toJazzSchemaReferenceExpression({
      modelName,
      fieldName,
      storedFieldName,
      field,
      getModelName,
    });
  }

  if (Array.isArray(field.type)) {
    return withOptionalSuffix(
      `s.enum(${field.type.map((variant) => formatStringLiteral(variant)).join(", ")})`,
      field,
    );
  }

  switch (field.type) {
    case "string":
      return withOptionalSuffix("s.string()", field);
    case "number":
      if (field.bigint) {
        throw new Error(
          `Field "${modelName}.${fieldName}" uses Better Auth bigint numbers, which Jazz schema.ts cannot represent.`,
        );
      }
      return withOptionalSuffix("s.int()", field);
    case "boolean":
      return withOptionalSuffix("s.boolean()", field);
    case "date":
      return withOptionalSuffix("s.timestamp()", field);
    case "json":
      return withOptionalSuffix("s.json()", field);
    case "string[]":
      return withOptionalSuffix("s.array(s.string())", field);
    case "number[]":
      if (field.bigint) {
        throw new Error(
          `Field "${modelName}.${fieldName}" uses Better Auth bigint arrays, which Jazz schema.ts cannot represent.`,
        );
      }
      return withOptionalSuffix("s.array(s.int())", field);
    default:
      throw new Error(`Unsupported Better Auth field type: ${String(field.type)}`);
  }
}

export function createSchemaNameResolvers(args: {
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): SchemaNameResolvers {
  const { tables, usePlural } = args;

  return {
    getModelName: initGetModelName({
      schema: tables,
      usePlural,
    }),
    getFieldName: initGetFieldName({
      schema: tables,
      usePlural,
    }),
  };
}

export function buildJazzSchema(args: {
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): WasmSchema {
  const { tables, getModelName, getFieldName } = args;
  const wasmSchema: WasmSchema = {};

  for (const [modelName, model] of Object.entries(tables)) {
    const tableName = getModelName(modelName);
    assertSchemaNameAllowed(tableName);
    const columns: WasmSchema[string]["columns"] = [];
    const relations: NonNullable<WasmSchema[string]["relations"]> = {};

    for (const [fieldName, field] of Object.entries(model.fields)) {
      if (fieldName === "id") {
        continue;
      }

      const storedFieldName = getFieldName({ model: modelName, field: fieldName });
      assertStoredFieldNameAllowed({ modelName, fieldName, storedFieldName });

      if (field.references) {
        const referenceColumn = toJazzReferenceColumn({
          modelName,
          fieldName,
          storedFieldName,
          field,
          getModelName,
        });

        relations[`${storedFieldName}Relation`] = {
          kind: "forward",
          table: referenceColumn.references,
          column: storedFieldName,
        };
        columns.push({
          name: storedFieldName,
          column_type: referenceColumn.columnType,
          nullable: field.required === false,
          references: referenceColumn.references,
        });
        continue;
      }

      columns.push({
        name: storedFieldName,
        column_type: toJazzColumnType(field),
        nullable: field.required === false,
      });
    }

    for (const name of Object.keys(relations)) {
      if (columns.some((c) => c.name === name))
        throw new Error(`Generated relationship "${tableName}.${name}" collides with a column.`);
    }
    wasmSchema[tableName] = { columns, relations };
  }

  return wasmSchema;
}

export function buildJazzSchemaFromTables(args: {
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): WasmSchema {
  const { tables, usePlural } = args;
  const resolvers = createSchemaNameResolvers({ tables, usePlural });

  return buildJazzSchema({
    tables,
    getModelName: resolvers.getModelName,
    getFieldName: resolvers.getFieldName,
  });
}

export function buildJazzSchemaSourceText(args: {
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): string {
  const { tables, getModelName, getFieldName } = args;
  const blocks: string[] = [];
  const permissionsBlocks: string[] = [];

  for (const [modelName, model] of Object.entries(tables)) {
    const tableName = getModelName(modelName);
    assertSchemaNameAllowed(tableName);
    const lines = [`  ${formatObjectKey(tableName)}: s.table({`];
    const relations: string[] = [];

    for (const [fieldName, field] of Object.entries(model.fields)) {
      if (fieldName === "id") {
        continue;
      }

      const storedFieldName = getFieldName({ model: modelName, field: fieldName });
      assertStoredFieldNameAllowed({ modelName, fieldName, storedFieldName });

      if (field.references) {
        const alias = `${storedFieldName}Relation`;
        if (
          Object.keys(model.fields).some(
            (f) => getFieldName({ model: modelName, field: f }) === alias,
          )
        )
          throw new Error(`Generated relationship "${tableName}.${alias}" collides with a column.`);
        relations.push(
          `    ${formatObjectKey(alias)}: s.rel(${formatStringLiteral(getModelName(field.references.model))}, ${formatStringLiteral(storedFieldName)}),`,
        );
      }
      const expression = toJazzSchemaColumnExpression({
        modelName,
        fieldName,
        storedFieldName,
        field,
        getModelName,
      });

      // Better Auth owns these fields, including its required timestamps.
      // Keep their domain meaning instead of recommending Jazz provenance columns.
      lines.push(
        `    ${formatObjectKey(storedFieldName)}: ${
          ["createdAt", "createdBy", "updatedAt", "updatedBy"].includes(storedFieldName)
            ? `s.allowExternalProvenanceName(${expression})`
            : expression
        },`,
      );
    }

    lines.push("  }, {", ...relations, "  }),");
    blocks.push(lines.join("\n"));

    const policy = formatPropertyAccess("policy", tableName);
    permissionsBlocks.push(
      [
        `  ${policy}.allowRead.never();`,
        `  ${policy}.allowInsert.never();`,
        `  ${policy}.allowUpdate.never();`,
        `  ${policy}.allowDelete.never();`,
      ].join("\n"),
    );
  }

  return [
    'import { schema as s } from "jazz-tools";',
    "",
    "export const schema = {",
    ...blocks.flatMap((block, index) => (index === 0 ? [block] : ["", block])),
    "};",
    "",
    "type AppSchema = s.Schema<typeof schema>;",
    "export const app: s.App<AppSchema> = s.defineApp(schema);",
    "export const wasmSchema = app.wasmSchema;",
    "",
    "export const permissions = s.definePermissions(app, ({ policy }) => {",
    ...permissionsBlocks.flatMap((block, index) => (index === 0 ? [block] : ["", block])),
    "});",
    "",
  ].join("\n");
}

export function buildJazzSchemaSourceTextFromTables(args: {
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): string {
  const { tables, usePlural } = args;
  const resolvers = createSchemaNameResolvers({ tables, usePlural });

  return buildJazzSchemaSourceText({
    tables,
    getModelName: resolvers.getModelName,
    getFieldName: resolvers.getFieldName,
  });
}

export function createJazzSchemaSourceFile(args: {
  file?: string;
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): DBAdapterSchemaCreation {
  const { file, tables, getModelName, getFieldName } = args;

  return {
    path: file ?? DEFAULT_SCHEMA_FILE_PATH,
    overwrite: true,
    code: buildJazzSchemaSourceText({
      tables,
      getModelName,
      getFieldName,
    }),
  };
}
