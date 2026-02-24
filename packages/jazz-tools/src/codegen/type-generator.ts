/**
 * Generate TypeScript interfaces from WasmSchema.
 */

import pluralize from "pluralize-esm";
import type { WasmSchema, ColumnType } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "./relation-analyzer.js";
import {
  generateWhereInputTypes,
  generateQueryBuilderClasses,
  generateAppExport,
} from "./query-builder-generator.js";

/**
 * Convert a WasmColumnType to TypeScript type string.
 */
function wasmTypeToTs(colType: ColumnType): string {
  switch (colType.type) {
    case "Text":
      return "string";
    case "Boolean":
      return "boolean";
    case "Integer":
    case "BigInt":
    case "Timestamp":
      return "number";
    case "Uuid":
      return "string";
    case "Enum":
      return colType.variants.map((variant: string) => JSON.stringify(variant)).join(" | ");
    case "Array":
      return `${wasmTypeToTs(colType.element)}[]`;
    case "Row":
      // Nested row - generate inline type
      const fields = colType.columns
        .map((c: { name: string; nullable: boolean; column_type: ColumnType }) => {
          const opt = c.nullable ? "?" : "";
          return `${c.name}${opt}: ${wasmTypeToTs(c.column_type)}`;
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
 *     parent?: boolean | TodoInclude | TodoQueryBuilder;
 *     owner?: boolean | UserInclude | UserQueryBuilder;
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
      lines.push(`  ${rel.name}?: boolean | ${targetInclude} | ${targetQueryBuilder};`);
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
 *   export type TodoWithIncludes<I extends TodoInclude = {}> = Todo & {
 *     [K in keyof I & keyof TodoRelations]?: I[K] extends true
 *       ? TodoRelations[K]
 *       : I[K] extends object
 *         ? TodoRelations[K] extends (infer E)[]
 *           ? WithIncludesArray<E, I[K]>
 *           : TodoRelations[K] & WithIncludesFor<TodoRelations[K], I[K]>
 *         : never;
 *   };
 */
function generateWithIncludesTypes(relations: Map<string, Relation[]>): string[] {
  const lines: string[] = [];

  // Check if any table has relations - only emit helper types if needed
  const hasAnyRelations = [...relations.values()].some((rels) => rels.length > 0);

  if (hasAnyRelations) {
    // Generate helper types only when there are relations that use them
    lines.push(`// Helper types for nested includes`);
    lines.push(`type WithIncludesFor<T, I> = T extends { id: string }`);
    lines.push(`  ? T & { [K in keyof I & string]?: unknown }`);
    lines.push(`  : T;`);
    lines.push(``);
    lines.push(`type WithIncludesArray<E, I> = E extends { id: string }`);
    lines.push(`  ? Array<E & { [K in keyof I & string]?: unknown }>`);
    lines.push(`  : E[];`);
    lines.push(``);
  }

  for (const [tableName, rels] of relations) {
    if (rels.length === 0) continue;

    const baseInterface = tableNameToInterface(tableName);
    const includeInterface = baseInterface + "Include";
    const relationsInterface = baseInterface + "Relations";

    lines.push(
      `export type ${baseInterface}WithIncludes<I extends ${includeInterface} = {}> = ${baseInterface} & {`,
    );
    lines.push(`  [K in keyof I & keyof ${relationsInterface}]?: I[K] extends true`);
    lines.push(`    ? ${relationsInterface}[K]`);
    lines.push(`    : I[K] extends object`);
    lines.push(`      ? ${relationsInterface}[K] extends (infer E)[]`);
    lines.push(`        ? WithIncludesArray<E, I[K]>`);
    lines.push(
      `        : ${relationsInterface}[K] & WithIncludesFor<${relationsInterface}[K], I[K]>`,
    );
    lines.push(`      : never;`);
    lines.push(`};`);
    lines.push(``);
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
 * 7. QueryBuilder classes (e.g., TodoQueryBuilder)
 * 8. Exported wasmSchema constant
 * 9. App export with table proxies
 */
export function generateTypes(schema: WasmSchema): string {
  const lines: string[] = [
    "// AUTO-GENERATED FILE - DO NOT EDIT",
    'import type { WasmSchema, QueryBuilder } from "jazz-tools";',
    "",
  ];

  // Base types (with id)
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const interfaceName = tableNameToInterface(tableName);
    lines.push(`export interface ${interfaceName} {`);
    lines.push("  id: string;");
    for (const col of table.columns) {
      const opt = col.nullable ? "?" : "";
      lines.push(`  ${col.name}${opt}: ${wasmTypeToTs(col.column_type)};`);
    }
    lines.push("}");
    lines.push("");
  }

  // Init types (without id, for inserts)
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const interfaceName = tableNameToInterface(tableName) + "Init";
    lines.push(`export interface ${interfaceName} {`);
    for (const col of table.columns) {
      const opt = col.nullable ? "?" : "";
      lines.push(`  ${col.name}${opt}: ${wasmTypeToTs(col.column_type)};`);
    }
    lines.push("}");
    lines.push("");
  }

  // WhereInput types (for type-safe filtering)
  lines.push(...generateWhereInputTypes(schema));

  // Analyze relations and generate relation types
  const relations = analyzeRelations(schema);

  // Include types (for specifying which relations to load)
  lines.push(...generateIncludeTypes(relations));

  // Relations types (mapping relation names to their result types)
  lines.push(...generateRelationsTypes(relations));

  // WithIncludes types (type-safe results based on include spec)
  lines.push(...generateWithIncludesTypes(relations));

  // Export WasmSchema JSON
  lines.push(`export const wasmSchema: WasmSchema = ${JSON.stringify(schema, null, 2)};`);
  lines.push("");

  // QueryBuilder classes
  lines.push(...generateQueryBuilderClasses(schema, relations));

  // App export with table proxies
  lines.push(...generateAppExport(schema));

  return lines.join("\n");
}
