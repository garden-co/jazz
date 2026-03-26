/**
 * Generate TypeScript interfaces from WasmSchema.
 */

import pluralize from "pluralize-esm";
import type { WasmSchema, ColumnType } from "../drivers/types.js";
import {
  PERMISSION_INTROSPECTION_COLUMNS,
  PERMISSION_INTROSPECTION_TS_TYPE,
} from "../magic-columns.js";
import { analyzeRelations, type Relation } from "./relation-analyzer.js";
import {
  generateWhereInputTypesWithMapper,
  generateQueryBuilderClasses,
  generateAppExport,
} from "./query-builder-generator.js";

type JsonSchemaObject = Record<string, unknown>;

interface JsonSchemaTypeBinding {
  key: string;
  constName: string;
  typeName: string;
  schema: JsonSchemaObject;
}

/**
 * Convert a WasmColumnType to TypeScript type string.
 */
function arrayType(elementTs: string): string {
  return elementTs.includes("|") ? `(${elementTs})[]` : `${elementTs}[]`;
}

function isJsonSchemaObject(value: unknown): value is JsonSchemaObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function collectJsonSchemaBindings(schema: WasmSchema): JsonSchemaTypeBinding[] {
  const bindings = new Map<string, JsonSchemaTypeBinding>();

  const visit = (colType: ColumnType): void => {
    switch (colType.type) {
      case "Json": {
        if (!isJsonSchemaObject(colType.schema)) {
          return;
        }
        const key = JSON.stringify(colType.schema);
        if (bindings.has(key)) {
          return;
        }
        const index = bindings.size + 1;
        bindings.set(key, {
          key,
          constName: `__jsonSchema${index}`,
          typeName: `__JsonType${index}`,
          schema: colType.schema,
        });
        return;
      }
      case "Array":
        visit(colType.element);
        return;
      case "Row":
        for (const nested of colType.columns) {
          visit(nested.column_type);
        }
        return;
      default:
        return;
    }
  };

  for (const table of Object.values(schema)) {
    for (const column of table.columns) {
      visit(column.column_type);
    }
  }

  return [...bindings.values()];
}

function wasmTypeToTs(colType: ColumnType, jsonTypeBySchemaKey: Map<string, string>): string {
  switch (colType.type) {
    case "Text":
      return "string";
    case "Boolean":
      return "boolean";
    case "Integer":
    case "BigInt":
    case "Double":
      return "number";
    case "Timestamp":
      return "Date";
    case "Uuid":
      return "string";
    case "Bytea":
      return "Uint8Array";
    case "Json": {
      if (isJsonSchemaObject(colType.schema)) {
        const key = JSON.stringify(colType.schema);
        const boundType = jsonTypeBySchemaKey.get(key);
        if (boundType) {
          return boundType;
        }
      }
      return "JsonValue";
    }
    case "Enum":
      return colType.variants.map((variant: string) => JSON.stringify(variant)).join(" | ");
    case "Array":
      return arrayType(wasmTypeToTs(colType.element, jsonTypeBySchemaKey));
    case "Row":
      // Nested row - generate inline type
      const fields = colType.columns
        .map((c: { name: string; nullable: boolean; column_type: ColumnType }) => {
          const opt = c.nullable ? "?" : "";
          return `${c.name}${opt}: ${wasmTypeToTs(c.column_type, jsonTypeBySchemaKey)}`;
        })
        .join("; ");
      return `{ ${fields} }`;
    default:
      return "unknown";
  }
}

function singularize(word: string): string {
  return pluralize.singular(word);
}

function serializeTsLiteral(value: unknown, indent = 0): string {
  const currentIndent = "  ".repeat(indent);
  const nextIndent = "  ".repeat(indent + 1);

  if (value instanceof Uint8Array) {
    return `new Uint8Array([${Array.from(value).join(", ")}])`;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "[]";
    }
    return `[\n${value.map((entry) => `${nextIndent}${serializeTsLiteral(entry, indent + 1)}`).join(",\n")}\n${currentIndent}]`;
  }

  if (value === null) {
    return "null";
  }

  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "number":
    case "boolean":
      return String(value);
    case "object": {
      const entries = Object.entries(value).filter(([, entryValue]) => entryValue !== undefined);
      if (entries.length === 0) {
        return "{}";
      }
      return `{\n${entries
        .map(
          ([key, entryValue]) =>
            `${nextIndent}${JSON.stringify(key)}: ${serializeTsLiteral(entryValue, indent + 1)}`,
        )
        .join(",\n")}\n${currentIndent}}`;
    }
    default:
      throw new Error(`Unsupported literal value in generated schema: ${typeof value}`);
  }
}

/**
 * Convert a table name to a TypeScript interface name.
 *
 * Examples:
 *   todos -> Todo
 *   user_profiles -> UserProfile
 *   categories -> Category
 */
export function tableNameToInterface(name: string): string {
  // Convert snake_case to words, singularize the last word, then PascalCase
  const parts = name.split("_");
  // Singularize only the last part (table names are typically plural)
  const lastIndex = parts.length - 1;
  const lastPart = parts[lastIndex];
  if (lastPart === undefined) {
    return "";
  }
  parts[lastIndex] = singularize(lastPart);

  return parts.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join("");
}

function generateAnyQueryBuilderAliases(schema: WasmSchema): string[] {
  const lines: string[] = [];

  for (const tableName of Object.keys(schema)) {
    const baseInterface = tableNameToInterface(tableName);
    lines.push(
      `type Any${baseInterface}QueryBuilder<T = any> = { readonly _table: "${tableName}" } & QueryBuilder<T>;`,
    );
  }

  if (lines.length > 0) {
    lines.push("");
  }

  return lines;
}

function maybeUndefined(type: string, nullable: boolean): string {
  return nullable ? `${type} | undefined` : type;
}

function maybeNull(type: string, nullable: boolean): string {
  return nullable ? `${type} | null` : type;
}

function relationResultType(baseType: string, rel: Relation, requiredFlag?: string): string {
  if (rel.isArray) {
    return maybeUndefined(`${baseType}[]`, rel.nullable);
  }

  if (rel.type !== "forward") {
    return maybeUndefined(baseType, rel.nullable);
  }

  if (rel.nullable || !requiredFlag) {
    return maybeUndefined(baseType, true);
  }

  return `${requiredFlag} extends true ? ${baseType} : ${baseType} | undefined`;
}

/**
 * Generate Include types for nested relation loading.
 *
 * Example output:
 *   export interface TodoInclude {
 *     parent?: true | TodoInclude | AnyTodoQueryBuilder<any>;
 *     owner?: true | UserInclude | AnyUserQueryBuilder<any>;
 *   }
 */
function generateIncludeTypes(relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  for (const [tableName, rels] of relations) {
    if (rels.length === 0) continue;

    const interfaceName = tableNameToInterface(tableName) + "Include";
    lines.push(`export interface ${interfaceName} {`);
    for (const rel of rels) {
      const targetInterface = tableNameToInterface(rel.toTable);
      const targetInclude = targetInterface + "Include";
      lines.push(
        `  ${rel.name}?: true | ${targetInclude} | Any${targetInterface}QueryBuilder<any>;`,
      );
    }
    lines.push(`}`);
    lines.push(``);
  }

  return lines;
}

/**
 * Generate Relations types mapping relation names to their result types.
 *
 * Example output:
 *   export interface TodoRelations {
 *     parent: Todo;
 *     owner: User;
 *   }
 */
function generateRelationsTypes(relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  for (const [tableName, rels] of relations) {
    if (rels.length === 0) continue;

    const interfaceName = tableNameToInterface(tableName) + "Relations";
    lines.push(`export interface ${interfaceName} {`);
    for (const rel of rels) {
      const targetInterface = tableNameToInterface(rel.toTable);
      const type = relationResultType(targetInterface, rel);
      lines.push(`  ${rel.name}: ${type};`);
    }
    lines.push(`}`);
    lines.push(``);
  }

  return lines;
}

function generateIncludedRelationsTypes(relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  for (const [tableName, rels] of relations) {
    if (rels.length === 0) continue;

    const baseInterface = tableNameToInterface(tableName);
    const includeInterface = baseInterface + "Include";

    lines.push(
      `export type ${baseInterface}IncludedRelations<I extends ${includeInterface} = {}, R extends boolean = false> = {`,
    );
    lines.push(`  [K in keyof I]-?:`);

    rels.forEach((rel, index) => {
      const targetInterface = tableNameToInterface(rel.toTable);
      const targetInclude = targetInterface + "Include";
      const targetWithIncludes = targetInterface + "WithIncludes";
      const trueType = relationResultType(targetInterface, rel, "R");
      const queryBuilderSelectedType = relationResultType(`QueryRow`, rel, "R");
      const nestedIncludeType = relationResultType(
        `${targetWithIncludes}<RelationInclude, false>`,
        rel,
        "R",
      );
      const prefix = index === 0 ? "    " : "    : ";

      lines.push(`${prefix}K extends "${rel.name}"`);
      lines.push(`      ? NonNullable<I["${rel.name}"]> extends infer RelationInclude`);
      lines.push(`        ? RelationInclude extends true`);
      lines.push(`          ? ${trueType}`);
      lines.push(
        `          : RelationInclude extends Any${targetInterface}QueryBuilder<infer QueryRow>`,
      );
      lines.push(`            ? ${queryBuilderSelectedType}`);
      lines.push(`            : RelationInclude extends ${targetInclude}`);
      lines.push(`              ? ${nestedIncludeType}`);
      lines.push(`              : never`);
      lines.push(`        : never`);
    });

    lines.push(`    : never;`);
    lines.push(`};`);
    lines.push(``);
  }

  return lines;
}

function generateMagicColumnTypes(): string[] {
  const lines: string[] = [];
  const magicColumnUnion = PERMISSION_INTROSPECTION_COLUMNS.map((column) =>
    JSON.stringify(column),
  ).join(" | ");

  lines.push(`export type PermissionIntrospectionColumn = ${magicColumnUnion};`);
  lines.push(`export interface PermissionIntrospectionColumns {`);
  for (const column of PERMISSION_INTROSPECTION_COLUMNS) {
    lines.push(`  ${column}: ${PERMISSION_INTROSPECTION_TS_TYPE};`);
  }
  lines.push(`}`);
  lines.push("");

  return lines;
}

/**
 * Generate WithIncludes types for type-safe include results.
 */
function generateWithIncludesTypes(relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  for (const [tableName, rels] of relations) {
    if (rels.length === 0) continue;

    const baseInterface = tableNameToInterface(tableName);
    const includeInterface = baseInterface + "Include";
    const includedRelationsType = baseInterface + "IncludedRelations";

    lines.push(
      `export type ${baseInterface}WithIncludes<I extends ${includeInterface} = {}, R extends boolean = false> = ${baseInterface} & ${includedRelationsType}<I, R>;`,
    );
    lines.push(``);
  }

  return lines;
}

function generateSelectionTypes(schema: WasmSchema, relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  for (const tableName of Object.keys(schema)) {
    const baseInterface = tableNameToInterface(tableName);
    const includeInterface = baseInterface + "Include";
    const selectableColumnType = baseInterface + "SelectableColumn";
    const orderableColumnType = baseInterface + "OrderableColumn";
    const includedRelationsType = baseInterface + "IncludedRelations";
    const hasRelations = (relations.get(tableName) ?? []).length > 0;

    lines.push(
      `export type ${selectableColumnType} = keyof ${baseInterface} | PermissionIntrospectionColumn | "*";`,
    );
    lines.push(
      `export type ${orderableColumnType} = keyof ${baseInterface} | PermissionIntrospectionColumn;`,
    );
    lines.push("");

    lines.push(
      `export type ${baseInterface}Selected<S extends ${selectableColumnType} = keyof ${baseInterface}> = ("*" extends S ? ${baseInterface} : Pick<${baseInterface}, Extract<S | "id", keyof ${baseInterface}>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;`,
    );
    lines.push("");

    if (hasRelations) {
      lines.push(
        `export type ${baseInterface}SelectedWithIncludes<I extends ${includeInterface} = {}, S extends ${selectableColumnType} = keyof ${baseInterface}, R extends boolean = false> = ${baseInterface}Selected<S> & ${includedRelationsType}<I, R>;`,
      );
      lines.push("");
    }
  }

  return lines;
}

/**
 * Generate TypeScript code from a WasmSchema.
 *
 * Produces:
 * 1. Base interfaces with id field (e.g., Todo)
 * 2. Init interfaces without id (e.g., TodoInit)
 * 3. WhereInput types for filtering (e.g., TodoWhereInput)
 * 4. Include types for relation loading (e.g., TodoInclude)
 * 5. Relations types mapping relation names to types (e.g., TodoRelations)
 * 6. WithIncludes types for type-safe results (e.g., TodoWithIncludes)
 * 7. Selection helper types for type-safe projections (e.g., TodoSelected)
 * 8. QueryBuilder classes (e.g., TodoQueryBuilder)
 * 9. Exported wasmSchema constant
 * 10. App export with table proxies
 */
export function generateTypes(schema: WasmSchema): string {
  const jsonSchemaBindings = collectJsonSchemaBindings(schema);
  const jsonTypeBySchemaKey = new Map(
    jsonSchemaBindings.map((binding) => [binding.key, binding.typeName]),
  );

  const importNames = ["WasmSchema", "QueryBuilder"];
  if (jsonSchemaBindings.length > 0) {
    importNames.push("JsonSchemaToTs");
  }

  const lines: string[] = [
    "// AUTO-GENERATED FILE - DO NOT EDIT",
    `import type { ${importNames.join(", ")} } from "jazz-tools";`,
    "export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];",
    "",
  ];

  if (jsonSchemaBindings.length > 0) {
    for (const binding of jsonSchemaBindings) {
      lines.push(
        `const ${binding.constName} = ${JSON.stringify(binding.schema, null, 2)} as const;`,
      );
      lines.push(`type ${binding.typeName} = JsonSchemaToTs<typeof ${binding.constName}>;`);
      lines.push("");
    }
  }

  lines.push(...generateMagicColumnTypes());

  // Base types (with id)
  for (const [tableName, table] of Object.entries(schema)) {
    const interfaceName = tableNameToInterface(tableName);
    lines.push(`export interface ${interfaceName} {`);
    lines.push("  id: string;");
    for (const col of table.columns) {
      const opt = col.nullable ? "?" : "";
      lines.push(`  ${col.name}${opt}: ${wasmTypeToTs(col.column_type, jsonTypeBySchemaKey)};`);
    }
    lines.push("}");
    lines.push("");
  }

  // Init types (without id, for inserts)
  for (const [tableName, table] of Object.entries(schema)) {
    const interfaceName = tableNameToInterface(tableName) + "Init";
    lines.push(`export interface ${interfaceName} {`);
    for (const col of table.columns) {
      const opt = col.nullable || col.default !== undefined ? "?" : "";
      lines.push(
        `  ${col.name}${opt}: ${maybeNull(
          wasmTypeToTs(col.column_type, jsonTypeBySchemaKey),
          col.nullable,
        )};`,
      );
    }
    lines.push("}");
    lines.push("");
  }

  // WhereInput types (for type-safe filtering)
  lines.push(
    ...generateWhereInputTypesWithMapper(schema, (columnType) =>
      wasmTypeToTs(columnType, jsonTypeBySchemaKey),
    ),
  );

  // Helper aliases for any query builder specializations
  lines.push(...generateAnyQueryBuilderAliases(schema));

  // Analyze relations and generate relation types
  const relations = analyzeRelations(schema);

  // Include types (for specifying which relations to load)
  lines.push(...generateIncludeTypes(relations));

  // Helper types for explicitly included relations only
  lines.push(...generateIncludedRelationsTypes(relations));

  // Relations types (mapping relation names to their result types)
  lines.push(...generateRelationsTypes(relations));

  // WithIncludes types (type-safe results based on include spec)
  lines.push(...generateWithIncludesTypes(relations));

  // Selection helper types (type-safe results based on select columns)
  lines.push(...generateSelectionTypes(schema, relations));

  // Export WasmSchema JSON
  lines.push(`export const wasmSchema: WasmSchema = ${serializeTsLiteral(schema)};`);
  lines.push("");

  // QueryBuilder classes
  lines.push(...generateQueryBuilderClasses(schema, relations));

  // App export with table proxies
  lines.push(...generateAppExport(schema));

  return lines.join("\n");
}
