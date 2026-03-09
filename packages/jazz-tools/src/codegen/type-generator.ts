/**
 * Generate TypeScript interfaces from WasmSchema.
 */

import pluralize from "pluralize-esm";
import type { WasmSchema, ColumnType } from "../drivers/types.js";
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
  parts[parts.length - 1] = singularize(parts[parts.length - 1]);

  return parts.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join("");
}

/**
 * Generate Include types for nested relation loading.
 *
 * Example output:
 *   export interface TodoInclude {
 *     parent?: true | TodoInclude | TodoQueryBuilder;
 *     owner?: true | UserInclude | UserQueryBuilder;
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
      const targetQueryBuilder = targetInterface + "QueryBuilder";
      // Add QueryBuilder to union for type-safe filtered includes
      lines.push(`  ${rel.name}?: true | ${targetInclude} | ${targetQueryBuilder};`);
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
      const type = rel.isArray ? `${targetInterface}[]` : targetInterface;
      lines.push(`  ${rel.name}: ${type};`);
    }
    lines.push(`}`);
    lines.push(``);
  }

  return lines;
}

/**
 * Generate WithIncludes types for type-safe include results.
 *
 * Example output:
 *   type TodoIncludedRelations<I extends TodoInclude = {}> =
 *     ("project" extends keyof I
 *       ? (NonNullable<I["project"]> extends infer RelationInclude
 *           ? RelationInclude extends true
 *             ? { project?: Project }
 *             : RelationInclude extends ProjectQueryBuilder<infer QueryInclude extends ProjectInclude>
 *               ? { project?: ProjectWithIncludes<QueryInclude> }
 *               : RelationInclude extends ProjectInclude
 *                 ? { project?: ProjectWithIncludes<RelationInclude> }
 *                 : {}
 *           : {})
 *       : {}) &
 *     {};
 *
 *   export type TodoWithIncludes<I extends TodoInclude = {}> =
 *     Omit<Todo, Extract<keyof I, keyof TodoRelations>> & TodoIncludedRelations<I>;
 */
function generateWithIncludesTypes(relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  for (const [tableName, rels] of relations) {
    if (rels.length === 0) continue;

    const baseInterface = tableNameToInterface(tableName);
    const includeInterface = baseInterface + "Include";
    const relationsInterface = baseInterface + "Relations";
    const includedRelationsType = baseInterface + "IncludedRelations";

    lines.push(`type ${includedRelationsType}<I extends ${includeInterface} = {}> =`);
    for (const rel of rels) {
      const targetInterface = tableNameToInterface(rel.toTable);
      const targetInclude = targetInterface + "Include";
      const targetQueryBuilder = targetInterface + "QueryBuilder";
      const targetWithIncludes = targetInterface + "WithIncludes";
      const trueType = rel.isArray ? `${targetInterface}[]` : targetInterface;
      const queryBuilderSelectedType = rel.isArray
        ? `${targetInterface}SelectedWithIncludes<QueryInclude, QuerySelect>[]`
        : `${targetInterface}SelectedWithIncludes<QueryInclude, QuerySelect>`;
      const nestedIncludeType = rel.isArray
        ? `${targetWithIncludes}<RelationInclude>[]`
        : `${targetWithIncludes}<RelationInclude>`;

      lines.push(`  ("${rel.name}" extends keyof I`);
      lines.push(`    ? (NonNullable<I["${rel.name}"]> extends infer RelationInclude`);
      lines.push(`        ? RelationInclude extends true`);
      lines.push(`          ? { ${rel.name}?: ${trueType} }`);
      lines.push(
        `          : RelationInclude extends ${targetQueryBuilder}<infer QueryInclude extends ${targetInclude}, infer QuerySelect extends keyof ${targetInterface} | "*">`,
      );
      lines.push(`            ? { ${rel.name}?: ${queryBuilderSelectedType} }`);
      lines.push(`            : RelationInclude extends ${targetInclude}`);
      lines.push(`              ? { ${rel.name}?: ${nestedIncludeType} }`);
      lines.push(`              : {}`);
      lines.push(`        : {})`);
      lines.push(`    : {}) &`);
    }
    lines.push(`  {};`);
    lines.push(``);
    lines.push(
      `export type ${baseInterface}WithIncludes<I extends ${includeInterface} = {}> = Omit<${baseInterface}, Extract<keyof I, keyof ${relationsInterface}>> & ${includedRelationsType}<I>;`,
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
    const hasRelations = (relations.get(tableName) ?? []).length > 0;

    lines.push(
      `export type ${baseInterface}Selected<S extends keyof ${baseInterface} | "*" = keyof ${baseInterface}> = "*" extends S ? ${baseInterface} : Pick<${baseInterface}, Extract<S | "id", keyof ${baseInterface}>>;`,
    );
    lines.push("");

    if (hasRelations) {
      lines.push(
        `export type ${baseInterface}SelectedWithIncludes<I extends ${includeInterface} = {}, S extends keyof ${baseInterface} | "*" = keyof ${baseInterface}> = Omit<${baseInterface}Selected<S>, Extract<keyof I, keyof ${baseInterface}Relations>> & ${baseInterface}IncludedRelations<I>;`,
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
      const opt = col.nullable ? "?" : "";
      lines.push(`  ${col.name}${opt}: ${wasmTypeToTs(col.column_type, jsonTypeBySchemaKey)};`);
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

  // Analyze relations and generate relation types
  const relations = analyzeRelations(schema);

  // Include types (for specifying which relations to load)
  lines.push(...generateIncludeTypes(relations));

  // Relations types (mapping relation names to their result types)
  lines.push(...generateRelationsTypes(relations));

  // WithIncludes types (type-safe results based on include spec)
  lines.push(...generateWithIncludesTypes(relations));

  // Selection helper types (type-safe results based on select columns)
  lines.push(...generateSelectionTypes(schema, relations));

  // Export WasmSchema JSON
  lines.push(`export const wasmSchema: WasmSchema = ${JSON.stringify(schema, null, 2)};`);
  lines.push("");

  // QueryBuilder classes
  lines.push(...generateQueryBuilderClasses(schema, relations));

  // App export with table proxies
  lines.push(...generateAppExport(schema));

  return lines.join("\n");
}
