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
            name: pluralize(table.name),
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
 * Convert table name to PascalCase
 */
function toPascalCase(str: string): string {
  return str
    .replace(/[-_](\w)/g, (_, c) => c.toUpperCase())
    .replace(/^\w/, (c) => c.toUpperCase());
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
    "/** ObjectId is a 128-bit unique identifier (UUIDv7) represented as a Base32 string */",
    "export type ObjectId = string;",
    "",
    "/** Base interface for all Groove rows */",
    "export interface GrooveRow {",
    "  id: ObjectId;",
    "}",
    "",
  ];

  // Generate Depth types
  lines.push("// === Depth types (specify which refs to load) ===");
  lines.push("");

  for (const table of tables) {
    const interfaceName = toPascalCase(table.name);
    const refColumns = table.columns.filter((c) => c.sqlType.kind === "ref");
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];

    if (refColumns.length === 0 && tableReverseRefs.length === 0) {
      lines.push(`export type ${interfaceName}Depth = {};`);
    } else {
      lines.push(`export type ${interfaceName}Depth = {`);
      // Forward refs
      for (const col of refColumns) {
        const refInterfaceName = toPascalCase(
          (col.sqlType as { kind: "ref"; table: string }).table
        );
        lines.push(`  ${col.name}?: true | ${refInterfaceName}Depth;`);
      }
      // Reverse refs
      for (const rev of tableReverseRefs) {
        const refInterfaceName = toPascalCase(rev.sourceTable);
        lines.push(`  ${rev.name}?: true | ${refInterfaceName}Depth;`);
      }
      lines.push("};");
    }
    lines.push("");
  }

  // Generate Row types
  lines.push("// === Row types ===");
  lines.push("");

  for (const table of tables) {
    const interfaceName = toPascalCase(table.name);
    const hasRefs = tablesWithRefs.has(table.name);
    const hasReverseRefs = tablesWithReverseRefs.has(table.name);
    const refColumns = table.columns.filter((c) => c.sqlType.kind === "ref");
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];

    // Base interface
    lines.push(`/** ${interfaceName} row from the ${table.name} table */`);
    lines.push(`export interface ${interfaceName} extends GrooveRow {`);
    for (const col of table.columns) {
      const tsType = sqlTypeToTs(col.sqlType, col.nullable);
      lines.push(`  ${col.name}: ${tsType};`);
    }
    lines.push("}");
    lines.push("");

    // Insert type
    lines.push(`/** Data for inserting a new ${interfaceName} */`);
    lines.push(`export interface ${interfaceName}Insert {`);
    for (const col of table.columns) {
      const optional = col.nullable ? "?" : "";
      let tsType: string;

      if (col.sqlType.kind === "ref") {
        const refInterfaceName = toPascalCase(col.sqlType.table);
        tsType = `ObjectId | ${refInterfaceName}`;
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
        `/** ${interfaceName} with refs/reverse refs resolved based on depth parameter D */`
      );
      lines.push(
        `export type ${interfaceName}Loaded<D extends ${interfaceName}Depth = {}> = {`
      );
      lines.push("  id: ObjectId;");

      // Forward refs
      for (const col of table.columns) {
        if (col.sqlType.kind === "ref") {
          const refInterfaceName = toPascalCase(col.sqlType.table);
          const refHasRefs = tablesWithRefs.has(col.sqlType.table);
          const refHasReverseRefs = tablesWithReverseRefs.has(col.sqlType.table);
          const nullSuffix = col.nullable ? " | null" : "";

          lines.push(`  ${col.name}: '${col.name}' extends keyof D`);
          lines.push(`    ? D['${col.name}'] extends true`);
          lines.push(`      ? ${refInterfaceName}${nullSuffix}`);
          if (refHasRefs || refHasReverseRefs) {
            lines.push(`      : D['${col.name}'] extends object`);
            lines.push(
              `        ? ${refInterfaceName}Loaded<D['${col.name}'] & ${refInterfaceName}Depth>${nullSuffix}`
            );
            lines.push(`        : ObjectId${nullSuffix}`);
          } else {
            lines.push(`      : D['${col.name}'] extends object`);
            lines.push(`        ? ${refInterfaceName}${nullSuffix}`);
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
        const refInterfaceName = toPascalCase(rev.sourceTable);
        const refHasRefs = tablesWithRefs.has(rev.sourceTable);
        const refHasReverseRefs = tablesWithReverseRefs.has(rev.sourceTable);

        lines.push(`  & ('${rev.name}' extends keyof D`);
        lines.push(`    ? D['${rev.name}'] extends true`);
        lines.push(`      ? { ${rev.name}: ${refInterfaceName}[] }`);
        if (refHasRefs || refHasReverseRefs) {
          lines.push(`      : D['${rev.name}'] extends object`);
          lines.push(
            `        ? { ${rev.name}: ${refInterfaceName}Loaded<D['${rev.name}'] & ${refInterfaceName}Depth>[] }`
          );
          lines.push(`        : {}`);
        } else {
          lines.push(`      : D['${rev.name}'] extends object`);
          lines.push(`        ? { ${rev.name}: ${refInterfaceName}[] }`);
          lines.push(`        : {}`);
        }
        lines.push(`    : {})`);
      }

      lines.push(";");
      lines.push("");
    } else {
      lines.push(
        `/** ${interfaceName} has no refs, so Loaded is the same as base type */`
      );
      lines.push(
        `export type ${interfaceName}Loaded<D extends ${interfaceName}Depth = {}> = ${interfaceName};`
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

  // Generate types
  const types = generateTypes(tables, reverseRefs);

  // Determine output path
  const outputDir = options?.output ?? dirname(sqlPath);
  mkdirSync(outputDir, { recursive: true });

  const typesPath = join(outputDir, "types.ts");
  writeFileSync(typesPath, types);

  const elapsed = Date.now() - startTime;

  console.log(
    pc.green("✓") +
      ` Generated types from ${pc.bold(tables.length)} table(s) in ${elapsed}ms`
  );
  console.log(`  ${pc.dim("→")} ${typesPath}`);
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
