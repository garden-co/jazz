import { writeFileSync, readFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import pc from "picocolors";

/**
 * SQL column types matching Groove's ColumnType enum
 */
type SqlColumnType =
  | { kind: "bool" }
  | { kind: "i64" }
  | { kind: "f64" }
  | { kind: "string" }
  | { kind: "bytes" }
  | { kind: "ref"; table: string };

/**
 * A parsed column definition
 */
interface ParsedColumn {
  name: string;
  sqlType: SqlColumnType;
  nullable: boolean;
}

/**
 * A parsed table definition
 */
interface ParsedTable {
  name: string;
  columns: ParsedColumn[];
}

/**
 * A reverse reference (one-to-many relationship)
 */
interface ReverseRef {
  name: string;
  sourceTable: string;
  sourceColumn: string;
  nullable: boolean;
}

/**
 * Parse a SQL type string to SqlColumnType
 */
function parseSqlType(typeStr: string): SqlColumnType {
  const upper = typeStr.toUpperCase().trim();

  if (upper === "BOOL" || upper === "BOOLEAN") {
    return { kind: "bool" };
  }
  if (upper === "I64" || upper === "BIGINT" || upper === "INTEGER") {
    return { kind: "i64" };
  }
  if (upper === "F64" || upper === "FLOAT" || upper === "DOUBLE" || upper === "REAL") {
    return { kind: "f64" };
  }
  if (upper === "STRING" || upper === "TEXT" || upper === "VARCHAR") {
    return { kind: "string" };
  }
  if (upper === "BYTES" || upper === "BLOB" || upper === "BYTEA") {
    return { kind: "bytes" };
  }

  // Check for REFERENCES
  const refMatch = typeStr.match(/^REFERENCES\s+(\w+)/i);
  if (refMatch) {
    return { kind: "ref", table: refMatch[1] };
  }

  throw new Error(`Unknown SQL type: ${typeStr}`);
}

/**
 * Parse a single CREATE TABLE statement
 */
function parseCreateTable(sql: string): ParsedTable | null {
  // Match CREATE TABLE name ( ... )
  const match = sql.match(/CREATE\s+TABLE\s+(\w+)\s*\(\s*([\s\S]*?)\s*\)/i);
  if (!match) {
    return null;
  }

  const tableName = match[1];
  const columnsStr = match[2];

  // Parse columns
  const columns: ParsedColumn[] = [];

  // Split by comma, handling potential nested parentheses
  const columnDefs = columnsStr.split(/,(?![^()]*\))/);

  for (const colDef of columnDefs) {
    const trimmed = colDef.trim();
    if (!trimmed) continue;

    // Parse: name TYPE [NOT NULL]
    // Also handle: name REFERENCES Table [NOT NULL]
    const colMatch = trimmed.match(/^(\w+)\s+((?:REFERENCES\s+\w+|\w+))(\s+NOT\s+NULL)?/i);
    if (!colMatch) {
      console.warn(`Could not parse column definition: ${trimmed}`);
      continue;
    }

    const colName = colMatch[1];
    const typeStr = colMatch[2];
    const notNull = !!colMatch[3];

    columns.push({
      name: colName,
      sqlType: parseSqlType(typeStr),
      nullable: !notNull,
    });
  }

  return { name: tableName, columns };
}

/**
 * Parse a SQL schema file containing multiple CREATE TABLE statements
 */
function parseSqlSchema(sql: string): ParsedTable[] {
  const tables: ParsedTable[] = [];

  // Find all CREATE TABLE statements
  const createTableRegex = /CREATE\s+TABLE\s+\w+\s*\([^)]+\)\s*;?/gi;
  const matches = sql.match(createTableRegex) || [];

  for (const match of matches) {
    const table = parseCreateTable(match);
    if (table) {
      tables.push(table);
    }
  }

  return tables;
}

/**
 * Build reverse references from parsed tables
 */
function buildReverseRefs(tables: ParsedTable[]): Map<string, ReverseRef[]> {
  const reverseRefs = new Map<string, ReverseRef[]>();

  // Initialize empty arrays for all tables
  for (const table of tables) {
    reverseRefs.set(table.name, []);
  }

  // Scan all tables for refs
  for (const table of tables) {
    for (const col of table.columns) {
      if (col.sqlType.kind === "ref") {
        const targetTable = col.sqlType.table;
        const refs = reverseRefs.get(targetTable);
        if (refs) {
          refs.push({
            // Table name is already plural (e.g., "Notes"), so use it directly
            name: table.name,
            sourceTable: table.name,
            sourceColumn: col.name,
            nullable: col.nullable,
          });
        }
      }
    }
  }

  return reverseRefs;
}

/**
 * Simple pluralization
 */
function pluralize(str: string): string {
  if (str.endsWith("s") || str.endsWith("x") || str.endsWith("z") ||
      str.endsWith("ch") || str.endsWith("sh")) {
    return str + "es";
  }
  if (str.endsWith("y") && !["a", "e", "i", "o", "u"].includes(str[str.length - 2])) {
    return str.slice(0, -1) + "ies";
  }
  return str + "s";
}

/**
 * Simple singularization (inverse of pluralize)
 */
function singularize(str: string): string {
  if (str.endsWith("ies")) {
    return str.slice(0, -3) + "y";
  }
  if (str.endsWith("es") && (
    str.slice(0, -2).endsWith("s") ||
    str.slice(0, -2).endsWith("x") ||
    str.slice(0, -2).endsWith("z") ||
    str.slice(0, -2).endsWith("ch") ||
    str.slice(0, -2).endsWith("sh")
  )) {
    return str.slice(0, -2);
  }
  if (str.endsWith("s") && !str.endsWith("ss")) {
    return str.slice(0, -1);
  }
  return str;
}

/**
 * Convert table name to PascalCase
 */
function toPascalCase(str: string): string {
  return str
    .replace(/[-_](\w)/g, (_, c) => c.toUpperCase())
    .replace(/^\w/, (c) => c.toUpperCase());
}

/**
 * Map SQL type to Prisma-style filter type
 */
function getFilterType(sqlType: SqlColumnType, nullable: boolean): string {
  let filterType: string;
  let valueType: string;

  switch (sqlType.kind) {
    case "bool":
      filterType = "BoolFilter";
      valueType = "boolean";
      break;
    case "i64":
      filterType = "BigIntFilter";
      valueType = "bigint";
      break;
    case "f64":
      filterType = "NumberFilter";
      valueType = "number";
      break;
    case "string":
      filterType = "StringFilter";
      valueType = "string";
      break;
    case "bytes":
      // Bytes don't have a filter type yet, use string
      filterType = "StringFilter";
      valueType = "string";
      break;
    case "ref":
      // Refs are ObjectIds (strings)
      filterType = "StringFilter";
      valueType = "string";
      break;
  }

  // Allow direct value or filter object
  if (nullable) {
    return `${valueType} | ${filterType} | null`;
  }
  return `${valueType} | ${filterType}`;
}

/**
 * Map SQL type to TypeScript type
 */
function sqlTypeToTs(sqlType: SqlColumnType, nullable: boolean): string {
  let tsType: string;
  switch (sqlType.kind) {
    case "bool":
      tsType = "boolean";
      break;
    case "i64":
      tsType = "bigint";
      break;
    case "f64":
      tsType = "number";
      break;
    case "string":
      tsType = "string";
      break;
    case "bytes":
      tsType = "Uint8Array";
      break;
    case "ref":
      tsType = "ObjectId";
      break;
  }
  return nullable ? `${tsType} | null` : tsType;
}

/**
 * Generate runtime metadata for query building
 */
function generateMeta(
  tables: ParsedTable[],
  reverseRefs: Map<string, ReverseRef[]>
): string {
  const lines: string[] = [
    "// Generated from SQL schema by @jazz/schema",
    "// DO NOT EDIT MANUALLY",
    "",
    'import type { SchemaMeta } from "@jazz/schema/runtime";',
    "",
    "export const schemaMeta: SchemaMeta = {",
    "  tables: {",
  ];

  for (const table of tables) {
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];

    lines.push(`    ${table.name}: {`);
    lines.push(`      name: "${table.name}",`);

    // Columns
    lines.push(`      columns: [`);
    for (const col of table.columns) {
      const typeJson = JSON.stringify(col.sqlType);
      lines.push(`        { name: "${col.name}", type: ${typeJson}, nullable: ${col.nullable} },`);
    }
    lines.push(`      ],`);

    // Forward refs
    const refCols = table.columns.filter((c) => c.sqlType.kind === "ref");
    lines.push(`      refs: [`);
    for (const col of refCols) {
      const targetTable = (col.sqlType as { kind: "ref"; table: string }).table;
      lines.push(`        { column: "${col.name}", targetTable: "${targetTable}", nullable: ${col.nullable} },`);
    }
    lines.push(`      ],`);

    // Reverse refs
    lines.push(`      reverseRefs: [`);
    for (const rev of tableReverseRefs) {
      lines.push(`        { name: "${rev.name}", sourceTable: "${rev.sourceTable}", sourceColumn: "${rev.sourceColumn}" },`);
    }
    lines.push(`      ],`);

    lines.push(`    },`);
  }

  lines.push("  },");
  lines.push("};");
  lines.push("");

  // Export individual table metas for convenience
  lines.push("// Individual table metadata exports");
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const varName = typeName.toLowerCase() + "Meta";
    lines.push(`export const ${varName} = schemaMeta.tables.${table.name};`);
  }
  lines.push("");

  return lines.join("\n");
}

/**
 * Generate TypeScript types from parsed SQL schema
 */
function generateTypes(
  tables: ParsedTable[],
  reverseRefs: Map<string, ReverseRef[]>
): string {
  // Build lookup sets
  const tablesWithRefs = new Set<string>();
  const tablesWithReverseRefs = new Set<string>();

  for (const table of tables) {
    if (table.columns.some((c) => c.sqlType.kind === "ref")) {
      tablesWithRefs.add(table.name);
    }
  }
  for (const [tableName, refs] of reverseRefs) {
    if (refs.length > 0) {
      tablesWithReverseRefs.add(tableName);
    }
  }

  const lines: string[] = [
    "// Generated from SQL schema by @jazz/schema",
    "// DO NOT EDIT MANUALLY",
    "",
    'import type { StringFilter, BigIntFilter, NumberFilter, BoolFilter } from "@jazz/schema/runtime";',
    "",
    "/** ObjectId is a 128-bit unique identifier (UUIDv7) represented as a Base32 string */",
    "export type ObjectId = string;",
    "",
    "/** Base interface for all Groove rows */",
    "export interface GrooveRow {",
    "  id: ObjectId;",
    "}",
    "",
  ];

  // Generate Includes types (specify which refs to load)
  lines.push("// === Includes types (specify which refs to load) ===");
  lines.push("");

  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const refColumns = table.columns.filter((c) => c.sqlType.kind === "ref");
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];

    if (refColumns.length === 0 && tableReverseRefs.length === 0) {
      lines.push(`export type ${typeName}Includes = {};`);
    } else {
      lines.push(`export type ${typeName}Includes = {`);
      // Forward refs
      for (const col of refColumns) {
        const refTypeName = singularize(toPascalCase(
          (col.sqlType as { kind: "ref"; table: string }).table
        ));
        lines.push(`  ${col.name}?: true | ${refTypeName}Includes;`);
      }
      // Reverse refs
      for (const rev of tableReverseRefs) {
        const refTypeName = singularize(toPascalCase(rev.sourceTable));
        lines.push(`  ${rev.name}?: true | ${refTypeName}Includes;`);
      }
      lines.push("};");
    }
    lines.push("");
  }

  // Generate Filter types (Prisma-style filters)
  lines.push("// === Filter types (Prisma-style filters) ===");
  lines.push("");

  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    lines.push(`export interface ${typeName}Filter {`);
    lines.push(`  AND?: ${typeName}Filter | ${typeName}Filter[];`);
    lines.push(`  OR?: ${typeName}Filter[];`);
    lines.push(`  NOT?: ${typeName}Filter | ${typeName}Filter[];`);

    // id field (always string/ObjectId)
    lines.push(`  id?: string | StringFilter;`);

    // All columns
    for (const col of table.columns) {
      const filterType = getFilterType(col.sqlType, col.nullable);
      lines.push(`  ${col.name}?: ${filterType};`);
    }
    lines.push("}");
    lines.push("");
  }

  // Generate Row types
  lines.push("// === Row types ===");
  lines.push("");

  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const hasRefs = tablesWithRefs.has(table.name);
    const hasReverseRefs = tablesWithReverseRefs.has(table.name);
    const refColumns = table.columns.filter((c) => c.sqlType.kind === "ref");
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];

    // Base interface
    lines.push(`/** ${typeName} row from the ${table.name} table */`);
    lines.push(`export interface ${typeName} extends GrooveRow {`);
    for (const col of table.columns) {
      const tsType = sqlTypeToTs(col.sqlType, col.nullable);
      lines.push(`  ${col.name}: ${tsType};`);
    }
    lines.push("}");
    lines.push("");

    // Insert type
    lines.push(`/** Data for inserting a new ${typeName} */`);
    lines.push(`export interface ${typeName}Insert {`);
    for (const col of table.columns) {
      const optional = col.nullable ? "?" : "";
      let tsType: string;

      if (col.sqlType.kind === "ref") {
        const refTypeName = singularize(toPascalCase(col.sqlType.table));
        tsType = `ObjectId | ${refTypeName}`;
        if (col.nullable) {
          tsType = `${tsType} | null`;
        }
      } else {
        tsType = sqlTypeToTs(col.sqlType, col.nullable);
      }

      lines.push(`  ${col.name}${optional}: ${tsType};`);
    }
    lines.push("}");
    lines.push("");

    // Loaded type
    if (hasRefs || hasReverseRefs) {
      lines.push(
        `/** ${typeName} with refs/reverse refs resolved based on includes parameter I */`
      );
      lines.push(
        `export type ${typeName}Loaded<I extends ${typeName}Includes = {}> = {`
      );
      lines.push("  id: ObjectId;");

      // Forward refs
      for (const col of table.columns) {
        if (col.sqlType.kind === "ref") {
          const refTypeName = singularize(toPascalCase(col.sqlType.table));
          const refHasRefs = tablesWithRefs.has(col.sqlType.table);
          const refHasReverseRefs = tablesWithReverseRefs.has(col.sqlType.table);
          const nullSuffix = col.nullable ? " | null" : "";

          lines.push(`  ${col.name}: '${col.name}' extends keyof I`);
          lines.push(`    ? I['${col.name}'] extends true`);
          lines.push(`      ? ${refTypeName}${nullSuffix}`);
          if (refHasRefs || refHasReverseRefs) {
            lines.push(`      : I['${col.name}'] extends object`);
            lines.push(
              `        ? ${refTypeName}Loaded<I['${col.name}'] & ${refTypeName}Includes>${nullSuffix}`
            );
            lines.push(`        : ObjectId${nullSuffix}`);
          } else {
            lines.push(`      : I['${col.name}'] extends object`);
            lines.push(`        ? ${refTypeName}${nullSuffix}`);
            lines.push(`        : ObjectId${nullSuffix}`);
          }
          lines.push(`    : ObjectId${nullSuffix};`);
        } else {
          const tsType = sqlTypeToTs(col.sqlType, col.nullable);
          lines.push(`  ${col.name}: ${tsType};`);
        }
      }

      // Close base type
      lines.push("}");

      // Reverse refs as intersections
      for (const rev of tableReverseRefs) {
        const refTypeName = singularize(toPascalCase(rev.sourceTable));
        const refHasRefs = tablesWithRefs.has(rev.sourceTable);
        const refHasReverseRefs = tablesWithReverseRefs.has(rev.sourceTable);

        lines.push(`  & ('${rev.name}' extends keyof I`);
        lines.push(`    ? I['${rev.name}'] extends true`);
        lines.push(`      ? { ${rev.name}: ${refTypeName}[] }`);
        if (refHasRefs || refHasReverseRefs) {
          lines.push(`      : I['${rev.name}'] extends object`);
          lines.push(
            `        ? { ${rev.name}: ${refTypeName}Loaded<I['${rev.name}'] & ${refTypeName}Includes>[] }`
          );
          lines.push(`        : {}`);
        } else {
          lines.push(`      : I['${rev.name}'] extends object`);
          lines.push(`        ? { ${rev.name}: ${refTypeName}[] }`);
          lines.push(`        : {}`);
        }
        lines.push(`    : {})`);
      }

      lines.push(";");
      lines.push("");
    } else {
      lines.push(
        `/** ${typeName} has no refs, so Loaded is the same as base type */`
      );
      lines.push(
        `export type ${typeName}Loaded<I extends ${typeName}Includes = {}> = ${typeName};`
      );
      lines.push("");
    }
  }

  return lines.join("\n");
}

/**
 * Options for generateFromSql
 */
export interface GenerateFromSqlOptions {
  /** Output directory for generated files (default: same as SQL file) */
  output?: string;
}

/**
 * Generate TypeScript types from a SQL schema file.
 *
 * @example
 * ```bash
 * npx tsx -e "import { generateFromSql } from '@jazz/schema'; generateFromSql('schema.sql')"
 * ```
 */
export function generateFromSql(
  sqlPath: string,
  options?: GenerateFromSqlOptions
): void {
  const startTime = Date.now();

  // Read SQL file
  const sql = readFileSync(sqlPath, "utf-8");

  // Parse tables
  const tables = parseSqlSchema(sql);
  if (tables.length === 0) {
    console.error(pc.red("No CREATE TABLE statements found in " + sqlPath));
    process.exit(1);
  }

  // Build reverse refs
  const reverseRefs = buildReverseRefs(tables);

  // Generate types and metadata
  const types = generateTypes(tables, reverseRefs);
  const meta = generateMeta(tables, reverseRefs);

  // Determine output path
  const outputDir = options?.output ?? dirname(sqlPath);
  mkdirSync(outputDir, { recursive: true });

  const typesPath = join(outputDir, "types.ts");
  const metaPath = join(outputDir, "meta.ts");
  writeFileSync(typesPath, types);
  writeFileSync(metaPath, meta);

  const elapsed = Date.now() - startTime;

  console.log(
    pc.green("✓") +
      ` Generated types and metadata from ${pc.bold(tables.length)} table(s) in ${elapsed}ms`
  );
  console.log(`  ${pc.dim("→")} ${typesPath}`);
  console.log(`  ${pc.dim("→")} ${metaPath}`);
}

// CLI entry point
if (process.argv[1]?.endsWith("from-sql.ts") || process.argv[1]?.endsWith("from-sql.js")) {
  const sqlPath = process.argv[2];
  if (!sqlPath) {
    console.error("Usage: npx tsx from-sql.ts <schema.sql>");
    process.exit(1);
  }
  generateFromSql(sqlPath);
}
