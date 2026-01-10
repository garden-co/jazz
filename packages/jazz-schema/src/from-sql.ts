import { writeFileSync, readFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import pc from "picocolors";

/**
 * SQL column types matching Groove's ColumnType enum
 */
type SqlColumnType =
  | { kind: "bool" }
  | { kind: "i32" }
  | { kind: "u32" }
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
  if (upper === "I32" || upper === "INT" || upper === "INTEGER") {
    return { kind: "i32" };
  }
  if (upper === "U32") {
    return { kind: "u32" };
  }
  if (upper === "I64" || upper === "BIGINT") {
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
    case "i32":
    case "u32":
    case "f64":
      filterType = "NumberFilter";
      valueType = "number";
      break;
    case "i64":
      filterType = "BigIntFilter";
      valueType = "bigint";
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
    case "i32":
    case "u32":
    case "f64":
      tsType = "number";
      break;
    case "i64":
      tsType = "bigint";
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
    'import type { StringFilter, BigIntFilter, NumberFilter, BoolFilter, RelationFilter, BaseWhereInput } from "@jazz/schema/runtime";',
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
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];

    lines.push(`export interface ${typeName}Filter extends BaseWhereInput {`);
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

    // Relation filters for reverse refs (e.g., IssueAssignees on Issue)
    for (const rr of tableReverseRefs) {
      const relatedTypeName = singularize(toPascalCase(rr.sourceTable));
      lines.push(`  /** Filter by related ${rr.sourceTable} */`);
      lines.push(`  ${rr.sourceTable}?: RelationFilter<${relatedTypeName}Filter>;`);
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

    // With type
    if (hasRefs || hasReverseRefs) {
      lines.push(
        `/** ${typeName} with refs/reverse refs resolved based on includes parameter I */`
      );
      lines.push(
        `export type ${typeName}With<I extends ${typeName}Includes = {}> = {`
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
              `        ? ${refTypeName}With<I['${col.name}'] & ${refTypeName}Includes>${nullSuffix}`
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
            `        ? { ${rev.name}: ${refTypeName}With<I['${rev.name}'] & ${refTypeName}Includes>[] }`
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
        `/** ${typeName} has no refs, so With is the same as base type */`
      );
      lines.push(
        `export type ${typeName}With<I extends ${typeName}Includes = {}> = ${typeName};`
      );
      lines.push("");
    }
  }

  return lines.join("\n");
}

/**
 * Get the fixed byte size of a column type, or null if variable-size.
 * For nullable fixed columns, includes the 1-byte presence flag.
 */
function getColumnFixedSize(sqlType: SqlColumnType, nullable: boolean): number | null {
  const baseSize = (() => {
    switch (sqlType.kind) {
      case "bool": return 1;
      case "i32": return 4;
      case "u32": return 4;
      case "i64": return 8;
      case "f64": return 8;
      case "ref": return 16; // 16-byte binary ObjectId
      case "string": return null; // variable
      case "bytes": return null; // variable
    }
  })();

  if (baseSize === null) return null;
  return nullable ? 1 + baseSize : baseSize; // presence byte for nullable
}

/**
 * Check if a column type is variable-size
 */
function isVariableColumn(sqlType: SqlColumnType): boolean {
  return sqlType.kind === "string" || sqlType.kind === "bytes";
}

/**
 * Analyze table columns to get fixed section size and variable column info
 */
interface TableLayout {
  fixedSize: number;
  fixedColumns: Array<{ col: ParsedColumn; offset: number }>;
  variableColumns: ParsedColumn[];
  offsetTableSize: number;
}

function analyzeTableLayout(table: ParsedTable): TableLayout {
  const fixedColumns: Array<{ col: ParsedColumn; offset: number }> = [];
  const variableColumns: ParsedColumn[] = [];
  let fixedOffset = 0;

  for (const col of table.columns) {
    const fixedSize = getColumnFixedSize(col.sqlType, col.nullable);
    if (fixedSize !== null) {
      fixedColumns.push({ col, offset: fixedOffset });
      fixedOffset += fixedSize;
    } else {
      variableColumns.push(col);
    }
  }

  // Offset table has N-1 entries for N variable columns
  const offsetTableSize = variableColumns.length > 1 ? (variableColumns.length - 1) * 4 : 0;

  return {
    fixedSize: fixedOffset,
    fixedColumns,
    variableColumns,
    offsetTableSize,
  };
}

/**
 * Generate the row type definition for a table
 */
function generateRowType(table: ParsedTable): string {
  const typeName = singularize(toPascalCase(table.name));
  const fields = [`id: string`];
  for (const col of table.columns) {
    const tsType = col.sqlType.kind === "ref"
      ? (col.nullable ? "string | null" : "string")
      : sqlTypeToTs(col.sqlType, col.nullable);
    fields.push(`${col.name}: ${tsType}`);
  }
  return `{ ${fields.join("; ")} }`;
}

/**
 * Generate binary decoders for all tables using the row buffer format.
 *
 * Row buffer format:
 * - Batch: [u32 count][u32 size₁][16-byte ObjectId][row buffer]...
 * - Row buffer: [fixed columns][offset table (N-1 u32s for N var cols)][variable data]
 * - ObjectId: 16 bytes binary (u128 LE), converted to Base32 string
 */
function generateDecoders(tables: ParsedTable[]): string {
  const lines: string[] = [
    "// Generated from SQL schema by @jazz/schema",
    "// DO NOT EDIT MANUALLY",
    "",
    "// Shared decoder for UTF-8 strings",
    "const decoder = new TextDecoder();",
    "",
    "// Delta type constants",
    "export const DELTA_ADDED = 1;",
    "export const DELTA_UPDATED = 2;",
    "export const DELTA_REMOVED = 3;",
    "",
    "// Crockford Base32 alphabet (matches Rust ObjectId encoding)",
    "const CROCKFORD_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';",
    "",
    "/**",
    " * Convert a 16-byte binary ObjectId to Base32 string.",
    " * Matches the Rust ObjectId encoding format.",
    " */",
    "function objectIdToString(bytes: Uint8Array, offset: number): string {",
    "  // Read as two 64-bit values (little-endian)",
    "  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, 16);",
    "  const lo = view.getBigUint64(0, true);",
    "  const hi = view.getBigUint64(8, true);",
    "",
    "  // Combine into 128-bit value",
    "  let value = (hi << 64n) | lo;",
    "",
    "  // Encode to Base32 (26 characters for 128 bits)",
    "  const chars = new Array(26);",
    "  for (let i = 25; i >= 0; i--) {",
    "    chars[i] = CROCKFORD_ALPHABET[Number(value & 0x1fn)];",
    "    value >>= 5n;",
    "  }",
    "",
    "  return chars.join('');",
    "}",
    "",
    "/** Delta type for incremental updates */",
    "export type Delta<T> =",
    "  | { type: 'added'; row: T }",
    "  | { type: 'updated'; row: T }",
    "  | { type: 'removed'; id: string };",
    "",
    "/**",
    " * Decoder state for reading from a binary buffer.",
    " * Used for composing decoders for nested/joined rows.",
    " */",
    "export class BinaryReader {",
    "  readonly bytes: Uint8Array;",
    "  readonly view: DataView;",
    "  offset: number;",
    "",
    "  constructor(buffer: ArrayBufferLike, startOffset = 0) {",
    "    this.bytes = new Uint8Array(buffer);",
    "    this.view = new DataView(buffer as ArrayBuffer);",
    "    this.offset = startOffset;",
    "  }",
    "",
    "  readObjectId(): string {",
    "    const id = objectIdToString(this.bytes, this.offset);",
    "    this.offset += 16;",
    "    return id;",
    "  }",
    "",
    "  readU32(): number {",
    "    const val = this.view.getUint32(this.offset, true);",
    "    this.offset += 4;",
    "    return val;",
    "  }",
    "",
    "  readI32(): number {",
    "    const val = this.view.getInt32(this.offset, true);",
    "    this.offset += 4;",
    "    return val;",
    "  }",
    "",
    "  readI64(): bigint {",
    "    const val = this.view.getBigInt64(this.offset, true);",
    "    this.offset += 8;",
    "    return val;",
    "  }",
    "",
    "  readF64(): number {",
    "    const val = this.view.getFloat64(this.offset, true);",
    "    this.offset += 8;",
    "    return val;",
    "  }",
    "",
    "  readBool(): boolean {",
    "    return this.bytes[this.offset++] === 1;",
    "  }",
    "",
    "  /** Read nullable value. Returns null if not present (presence byte = 0). */",
    "  readNullable<T>(readValue: () => T): T | null {",
    "    if (this.bytes[this.offset++] === 0) return null;",
    "    return readValue();",
    "  }",
    "",
    "  /**",
    "   * Read a nullable ObjectId ref.",
    "   * Nullable refs have a presence byte before the 16-byte ObjectId.",
    "   */",
    "  readNullableRef(): string | null {",
    "    if (this.bytes[this.offset++] === 0) {",
    "      this.offset += 16; // Skip the zeroed ObjectId bytes",
    "      return null;",
    "    }",
    "    return this.readObjectId();",
    "  }",
    "}",
    "",
  ];

  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const rowType = generateRowType(table);
    const layout = analyzeTableLayout(table);

    // Generate batch decoder (with row count and size headers)
    lines.push(`/**`);
    lines.push(` * Decode binary rows for ${table.name} table (batch format)`);
    lines.push(` *`);
    lines.push(` * Row buffer layout:`);
    lines.push(` * - Fixed size: ${layout.fixedSize} bytes`);
    lines.push(` * - Variable columns: ${layout.variableColumns.length}`);
    lines.push(` * - Offset table: ${layout.offsetTableSize} bytes`);
    lines.push(` */`);
    lines.push(`export function decode${typeName}Rows(buffer: ArrayBufferLike): Array<${rowType}> {`);
    lines.push(`  const bytes = new Uint8Array(buffer);`);
    lines.push(`  const view = new DataView(buffer as ArrayBuffer);`);
    lines.push(`  let offset = 0;`);
    lines.push(``);
    lines.push(`  // Read row count`);
    lines.push(`  const rowCount = view.getUint32(offset, true);`);
    lines.push(`  offset += 4;`);
    lines.push(``);
    lines.push(`  const rows = new Array(rowCount);`);
    lines.push(``);
    lines.push(`  for (let i = 0; i < rowCount; i++) {`);
    lines.push(`    // Read row size (includes 16-byte ObjectId + row buffer)`);
    lines.push(`    const rowSize = view.getUint32(offset, true);`);
    lines.push(`    offset += 4;`);
    lines.push(`    const rowStart = offset;`);
    lines.push(`    const rowEnd = rowStart + rowSize;`);
    lines.push(``);
    lines.push(`    // Read ObjectId (16 bytes binary -> Base32 string)`);
    lines.push(`    const id = objectIdToString(bytes, offset);`);
    lines.push(`    offset += 16;`);
    lines.push(`    const bufferStart = offset; // Start of row buffer (after ObjectId)`);
    lines.push(``);

    // Generate fixed column reads
    if (layout.fixedColumns.length > 0) {
      lines.push(`    // Fixed columns`);
      for (const { col, offset: colOffset } of layout.fixedColumns) {
        const absOffset = `bufferStart + ${colOffset}`;
        if (col.nullable) {
          lines.push(`    const ${col.name} = bytes[${absOffset}] === 0 ? null : ${generateFixedRead(col.sqlType, `${absOffset} + 1`, "view", "bytes")};`);
        } else {
          lines.push(`    const ${col.name} = ${generateFixedRead(col.sqlType, absOffset, "view", "bytes")};`);
        }
      }
      lines.push(``);
    }

    // Generate variable column reads
    if (layout.variableColumns.length > 0) {
      const offsetTableStart = `bufferStart + ${layout.fixedSize}`;
      const varDataStart = `${offsetTableStart} + ${layout.offsetTableSize}`;

      lines.push(`    // Variable columns (using offset table)`);
      lines.push(`    const offsetTableStart = ${offsetTableStart};`);

      // Read offset table entries (N-1 for N variable columns)
      if (layout.variableColumns.length > 1) {
        for (let i = 0; i < layout.variableColumns.length - 1; i++) {
          // Offsets are relative to row buffer start, add bufferStart for absolute position
          lines.push(`    const varOffset${i + 1} = bufferStart + view.getUint32(offsetTableStart + ${i * 4}, true);`);
        }
      }
      lines.push(`    const varDataStart = ${varDataStart};`);
      lines.push(``);

      // Generate variable column reads
      for (let i = 0; i < layout.variableColumns.length; i++) {
        const col = layout.variableColumns[i];
        const startExpr = i === 0 ? "varDataStart" : `varOffset${i}`;
        const endExpr = i === layout.variableColumns.length - 1 ? "rowEnd" : `varOffset${i + 1}`;

        if (col.nullable) {
          lines.push(`    let ${col.name}: string | null = null;`);
          lines.push(`    if (bytes[${startExpr}] === 1) {`);
          lines.push(`      ${col.name} = decoder.decode(bytes.subarray(${startExpr} + 1, ${endExpr}));`);
          lines.push(`    }`);
        } else {
          lines.push(`    const ${col.name} = decoder.decode(bytes.subarray(${startExpr}, ${endExpr}));`);
        }
      }
      lines.push(``);
    }

    // Build row object
    const fieldNames = ["id", ...table.columns.map(c => c.name)];
    lines.push(`    rows[i] = { ${fieldNames.join(", ")} };`);
    lines.push(`    offset = rowEnd;`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  return rows;`);
    lines.push(`}`);
    lines.push(``);

    // Generate delta decoder
    lines.push(`/**`);
    lines.push(` * Decode a ${typeName} delta from binary`);
    lines.push(` * Format: u8 type (1=added, 2=updated, 3=removed) + [16-byte ObjectId][row buffer] or just ObjectId`);
    lines.push(` */`);
    lines.push(`export function decode${typeName}Delta(buffer: ArrayBufferLike): Delta<${rowType}> {`);
    lines.push(`  const bytes = new Uint8Array(buffer);`);
    lines.push(`  const view = new DataView(buffer as ArrayBuffer);`);
    lines.push(`  const deltaType = bytes[0];`);
    lines.push(``);
    lines.push(`  if (deltaType === DELTA_REMOVED) {`);
    lines.push(`    const id = objectIdToString(bytes, 1);`);
    lines.push(`    return { type: 'removed', id };`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  // Added or Updated: decode the row`);
    lines.push(`  const id = objectIdToString(bytes, 1);`);
    lines.push(`  const bufferStart = 17; // 1 (delta type) + 16 (ObjectId)`);
    lines.push(`  const rowEnd = bytes.length;`);
    lines.push(``);

    // Generate fixed column reads for delta
    if (layout.fixedColumns.length > 0) {
      for (const { col, offset: colOffset } of layout.fixedColumns) {
        const absOffset = `bufferStart + ${colOffset}`;
        if (col.nullable) {
          lines.push(`  const ${col.name} = bytes[${absOffset}] === 0 ? null : ${generateFixedRead(col.sqlType, `${absOffset} + 1`, "view", "bytes")};`);
        } else {
          lines.push(`  const ${col.name} = ${generateFixedRead(col.sqlType, absOffset, "view", "bytes")};`);
        }
      }
    }

    // Generate variable column reads for delta
    if (layout.variableColumns.length > 0) {
      const offsetTableStart = `bufferStart + ${layout.fixedSize}`;
      const varDataStart = `${offsetTableStart} + ${layout.offsetTableSize}`;

      lines.push(`  const offsetTableStart = ${offsetTableStart};`);
      if (layout.variableColumns.length > 1) {
        for (let i = 0; i < layout.variableColumns.length - 1; i++) {
          lines.push(`  const varOffset${i + 1} = bufferStart + view.getUint32(offsetTableStart + ${i * 4}, true);`);
        }
      }
      lines.push(`  const varDataStart = ${varDataStart};`);

      for (let i = 0; i < layout.variableColumns.length; i++) {
        const col = layout.variableColumns[i];
        const startExpr = i === 0 ? "varDataStart" : `varOffset${i}`;
        const endExpr = i === layout.variableColumns.length - 1 ? "rowEnd" : `varOffset${i + 1}`;

        if (col.nullable) {
          lines.push(`  let ${col.name}: string | null = null;`);
          lines.push(`  if (bytes[${startExpr}] === 1) {`);
          lines.push(`    ${col.name} = decoder.decode(bytes.subarray(${startExpr} + 1, ${endExpr}));`);
          lines.push(`  }`);
        } else {
          lines.push(`  const ${col.name} = decoder.decode(bytes.subarray(${startExpr}, ${endExpr}));`);
        }
      }
    }

    lines.push(``);
    lines.push(`  return {`);
    lines.push(`    type: deltaType === DELTA_ADDED ? 'added' : 'updated',`);
    lines.push(`    row: { ${fieldNames.join(", ")} }`);
    lines.push(`  };`);
    lines.push(`}`);
    lines.push(``);

    // Generate reader function for BinaryReader (for nested rows)
    // Note: Tables with variable columns can't use BinaryReader directly
    lines.push(`/**`);
    lines.push(` * Read a ${typeName} row using a BinaryReader.`);
    if (layout.variableColumns.length > 0) {
      lines.push(` * NOTE: This table has variable columns - use decode${typeName}Rows/decode${typeName}Delta instead.`);
      lines.push(` */`);
      lines.push(`export function read${typeName}(reader: BinaryReader): ${rowType} {`);
      lines.push(`  throw new Error('read${typeName} requires row boundary context - use decode${typeName}Rows or decode${typeName}Delta instead');`);
      lines.push(`}`);
    } else {
      lines.push(` * Use this for nested/joined row decoding.`);
      lines.push(` */`);
      lines.push(`export function read${typeName}(reader: BinaryReader): ${rowType} {`);
      lines.push(`  const id = reader.readObjectId();`);

      for (const col of table.columns) {
        if (col.nullable && col.sqlType.kind === "ref") {
          lines.push(`  const ${col.name} = reader.readNullableRef();`);
        } else if (col.nullable) {
          lines.push(`  const ${col.name} = reader.readNullable(() => ${generateReaderCall(col.sqlType)});`);
        } else {
          lines.push(`  const ${col.name} = ${generateReaderCall(col.sqlType)};`);
        }
      }

      lines.push(`  return { ${fieldNames.join(", ")} };`);
      lines.push(`}`);
    }
    lines.push(``);
  }

  return lines.join("\n");
}

/**
 * Generate code to read a fixed-size value at a known offset
 */
function generateFixedRead(sqlType: SqlColumnType, offset: string, viewVar: string, bytesVar: string): string {
  switch (sqlType.kind) {
    case "bool":
      return `${bytesVar}[${offset}] === 1`;
    case "i32":
      return `${viewVar}.getInt32(${offset}, true)`;
    case "u32":
      return `${viewVar}.getUint32(${offset}, true)`;
    case "i64":
      return `${viewVar}.getBigInt64(${offset}, true)`;
    case "f64":
      return `${viewVar}.getFloat64(${offset}, true)`;
    case "ref":
      return `objectIdToString(${bytesVar}, ${offset})`;
    default:
      throw new Error(`Not a fixed-size type: ${sqlType.kind}`);
  }
}

/**
 * Generate a BinaryReader method call for a column type
 */
function generateReaderCall(sqlType: SqlColumnType): string {
  switch (sqlType.kind) {
    case "bool":
      return "reader.readBool()";
    case "i32":
      return "reader.readI32()";
    case "u32":
      return "reader.readU32()";
    case "i64":
      return "reader.readI64()";
    case "f64":
      return "reader.readF64()";
    case "string":
      throw new Error("String columns should use row buffer format, not BinaryReader");
    case "bytes":
      throw new Error("Bytes columns should use row buffer format, not BinaryReader");
    case "ref":
      return "reader.readObjectId()";
  }
}

/**
 * Generate client.ts with typed table clients and createDatabase function
 */
function generateClient(
  tables: ParsedTable[],
  reverseRefs: Map<string, ReverseRef[]>
): string {
  const lines: string[] = [
    "// Generated from SQL schema by @jazz/schema",
    "// DO NOT EDIT MANUALLY",
    "",
    'import {',
    '  TableClient,',
    '  type WasmDatabaseLike,',
    '  type Unsubscribe,',
    '  type TableDecoder,',
    '  type BaseWhereInput,',
    '  type IncludeSpec,',
    '  type SubscribableAllWithDb,',
    '  type SubscribableOneWithDb,',
    '  type MutableWithDb,',
    '} from "@jazz/client";',
    'import { schemaMeta } from "./meta.js";',
  ];

  // Import decoders
  const decoderImports: string[] = [];
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    decoderImports.push(`decode${typeName}Rows`);
    decoderImports.push(`decode${typeName}Delta`);
  }
  lines.push(`import { ${decoderImports.join(", ")} } from "./decoders.js";`);

  // Import types
  const typeImports: string[] = [];
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    typeImports.push(typeName);
    typeImports.push(`${typeName}Insert`);
    typeImports.push(`${typeName}Includes`);
    typeImports.push(`${typeName}With`);
    typeImports.push(`${typeName}Filter`);
  }
  lines.push(`import type { ObjectId, ${typeImports.join(", ")} } from "./types.js";`);
  lines.push("");

  // Generate query builder classes for all tables
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const descriptorName = `${table.name}Descriptor`;
    const builderName = `${table.name}QueryBuilder`;

    lines.push(`/**`);
    lines.push(` * Query builder for ${table.name} with chainable where/with methods`);
    lines.push(` * @generated from schema table: ${table.name}`);
    lines.push(` */`);
    lines.push(`export class ${builderName}<I extends ${typeName}Includes = {}>`);
    lines.push(`  implements SubscribableAllWithDb<${typeName}With<I>, ${typeName}Insert, Partial<${typeName}Insert>>,`);
    lines.push(`             SubscribableOneWithDb<${typeName}With<I>, Partial<${typeName}Insert>> {`);
    lines.push(`  private _descriptor: ${descriptorName};`);
    lines.push(`  private _where?: ${typeName}Filter;`);
    lines.push(`  private _include?: I;`);
    lines.push(``);
    lines.push(`  constructor(descriptor: ${descriptorName}, where?: ${typeName}Filter, include?: I) {`);
    lines.push(`    this._descriptor = descriptor;`);
    lines.push(`    this._where = where;`);
    lines.push(`    this._include = include;`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Get a stable key representing this query's options (for React hook deduplication)`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  get _queryKey(): string {`);
    lines.push(`    return JSON.stringify({ t: "${table.name}", w: this._where, i: this._include });`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Add a filter condition`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  where(filter: ${typeName}Filter): ${builderName}<I> {`);
    lines.push(`    return new ${builderName}(this._descriptor, filter, this._include);`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Specify which refs to include`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  with<NewI extends ${typeName}Includes>(include: NewI): ${builderName}<NewI> {`);
    lines.push(`    return new ${builderName}(this._descriptor, this._where, include);`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Subscribe to all matching ${table.name}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  subscribeAll(db: WasmDatabaseLike, callback: (rows: ${typeName}With<I>[]) => void): Unsubscribe {`);
    lines.push(`    return this._descriptor._subscribeAllInternal(`);
    lines.push(`      db,`);
    lines.push(`      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },`);
    lines.push(`      callback as (rows: ${typeName}[]) => void`);
    lines.push(`    );`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Subscribe to a single ${typeName} by ID`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: ${typeName}With<I> | null) => void): Unsubscribe {`);
    lines.push(`    return this._descriptor._subscribeInternal(`);
    lines.push(`      db,`);
    lines.push(`      id,`);
    lines.push(`      { include: this._include as IncludeSpec | undefined },`);
    lines.push(`      callback as (row: ${typeName} | null) => void`);
    lines.push(`    );`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Create a new ${typeName}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  create(db: WasmDatabaseLike, data: ${typeName}Insert): ObjectId {`);
    lines.push(`    return this._descriptor.create(db, data);`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Update a ${typeName}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<${typeName}Insert>): void {`);
    lines.push(`    return this._descriptor.update(db, id, data);`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  /**`);
    lines.push(`   * Delete a ${typeName}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  delete(db: WasmDatabaseLike, id: ObjectId): void {`);
    lines.push(`    return this._descriptor.delete(db, id);`);
    lines.push(`  }`);
    lines.push(`}`);
    lines.push(``);
  }

  // Generate table descriptor classes
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const descriptorName = `${table.name}Descriptor`;
    const builderName = `${table.name}QueryBuilder`;

    lines.push(`/**`);
    lines.push(` * Descriptor for the ${table.name} table (no db instance, db passed at method call time)`);
    lines.push(` * @generated from schema table: ${table.name}`);
    lines.push(` */`);
    lines.push(`export class ${descriptorName} extends TableClient<${typeName}>`);
    lines.push(`  implements SubscribableAllWithDb<${typeName}, ${typeName}Insert, Partial<${typeName}Insert>>,`);
    lines.push(`             SubscribableOneWithDb<${typeName}, Partial<${typeName}Insert>>,`);
    lines.push(`             MutableWithDb<${typeName}Insert, Partial<${typeName}Insert>> {`);
    lines.push(`  constructor() {`);
    lines.push(`    super(schemaMeta.tables.${table.name}, schemaMeta, {`);
    lines.push(`      rows: decode${typeName}Rows,`);
    lines.push(`      delta: decode${typeName}Delta,`);
    lines.push(`    });`);
    lines.push(`  }`);
    lines.push("");

    // create method
    lines.push(`  /**`);
    lines.push(`   * Create a new ${typeName}`);
    lines.push(`   * @returns The ObjectId of the created row`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  create(db: WasmDatabaseLike, data: ${typeName}Insert): ObjectId {`);
    lines.push(`    const values: Record<string, unknown> = {};`);
    for (const col of table.columns) {
      if (col.nullable) {
        lines.push(`    if (data.${col.name} !== undefined) values.${col.name} = data.${col.name};`);
      } else {
        lines.push(`    values.${col.name} = data.${col.name};`);
      }
    }
    lines.push(`    return this._create(db, values);`);
    lines.push(`  }`);
    lines.push("");

    // update method
    lines.push(`  /**`);
    lines.push(`   * Update an existing ${typeName}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<${typeName}Insert>): void {`);
    lines.push(`    this._update(db, id, data as Record<string, unknown>);`);
    lines.push(`  }`);
    lines.push("");

    // delete method
    lines.push(`  /**`);
    lines.push(`   * Delete a ${typeName}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  delete(db: WasmDatabaseLike, id: ObjectId): void {`);
    lines.push(`    this._delete(db, id);`);
    lines.push(`  }`);
    lines.push("");

    // Builder entry point: where()
    lines.push(`  /**`);
    lines.push(`   * Start a query with a filter condition`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  where(filter: ${typeName}Filter): ${builderName}<{}> {`);
    lines.push(`    return new ${builderName}(this, filter, undefined);`);
    lines.push(`  }`);
    lines.push("");

    // Builder entry point: with()
    lines.push(`  /**`);
    lines.push(`   * Start a query with includes`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  with<I extends ${typeName}Includes>(include: I): ${builderName}<I> {`);
    lines.push(`    return new ${builderName}(this, undefined, include);`);
    lines.push(`  }`);
    lines.push("");

    // subscribeAll method (direct, no filters/includes)
    lines.push(`  /**`);
    lines.push(`   * Subscribe to all ${table.name}`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  subscribeAll(db: WasmDatabaseLike, callback: (rows: ${typeName}[]) => void): Unsubscribe {`);
    lines.push(`    return this._subscribeAll(db, {}, callback);`);
    lines.push(`  }`);
    lines.push("");

    // subscribe method (direct, no includes)
    lines.push(`  /**`);
    lines.push(`   * Subscribe to a single ${typeName} by ID`);
    lines.push(`   * @generated from schema table: ${table.name}`);
    lines.push(`   */`);
    lines.push(`  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: ${typeName} | null) => void): Unsubscribe {`);
    lines.push(`    return this._subscribe(db, id, {}, callback);`);
    lines.push(`  }`);
    lines.push("");

    // Internal methods for query builder to call
    lines.push(`  /** @internal Used by query builder */`);
    lines.push(`  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: ${typeName}[]) => void): Unsubscribe {`);
    lines.push(`    return this._subscribeAll(db, options, callback);`);
    lines.push(`  }`);
    lines.push("");

    lines.push(`  /** @internal Used by query builder */`);
    lines.push(`  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: ${typeName} | null) => void): Unsubscribe {`);
    lines.push(`    return this._subscribe(db, id, options, callback);`);
    lines.push(`  }`);

    lines.push(`}`);
    lines.push("");
  }

  // Generate app object (typed schema descriptor)
  lines.push(`/**`);
  lines.push(` * Typed schema descriptor for use with React hooks.`);
  lines.push(` * Pass queries to useAll/useOne, which inject the db from context.`);
  lines.push(` *`);
  lines.push(` * @example`);
  lines.push(` * \`\`\`typescript`);
  lines.push(` * import { app } from './generated/client';`);
  lines.push(` * import { useAll, useOne, useMutate } from '@jazz/react';`);
  lines.push(` *`);
  lines.push(` * function UserList() {`);
  lines.push(` *   const [users, loading, mutate] = useAll(app.users);`);
  lines.push(` *   return users.map(u => <li key={u.id}>{u.name}</li>);`);
  lines.push(` * }`);
  lines.push(` *`);
  lines.push(` * function UserProfile({ userId }) {`);
  lines.push(` *   const [user, loading, mutate] = useOne(app.users, userId);`);
  lines.push(` *   return <div>{user?.name}</div>;`);
  lines.push(` * }`);
  lines.push(` * \`\`\``);
  lines.push(` */`);
  lines.push(`export const app = {`);
  for (const table of tables) {
    const descriptorName = `${table.name}Descriptor`;
    const propName = table.name.toLowerCase();
    lines.push(`  ${propName}: new ${descriptorName}(),`);
  }
  lines.push(`};`);
  lines.push("");

  lines.push(`export type App = typeof app;`);
  lines.push("");

  // Generate Database interface and createDatabase function
  lines.push(`/**`);
  lines.push(` * Database interface with bound WASM instance.`);
  lines.push(` * Created by calling createDatabase(wasmDb).`);
  lines.push(` */`);
  lines.push(`export interface Database {`);
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const propName = table.name.toLowerCase();
    lines.push(`  ${propName}: BoundTableClient<${typeName}, ${typeName}Insert, ${typeName}Includes>;`);
  }
  lines.push(`}`);
  lines.push("");

  // Generate BoundTableClient interface
  lines.push(`/**`);
  lines.push(` * A table client with the WASM database bound, allowing direct method calls.`);
  lines.push(` */`);
  lines.push(`export interface BoundTableClient<T, TInsert, TIncludes> {`);
  lines.push(`  create(data: TInsert): ObjectId;`);
  lines.push(`  update(id: ObjectId, data: Partial<TInsert>): void;`);
  lines.push(`  delete(id: ObjectId): void;`);
  lines.push(`  subscribeAll(callback: (rows: T[]) => void): Unsubscribe;`);
  lines.push(`  subscribe(id: ObjectId, callback: (row: T | null) => void): Unsubscribe;`);
  lines.push(`  where(filter: any): BoundQueryBuilder<T, TInsert, TIncludes>;`);
  lines.push(`  with<I extends TIncludes>(include: I): BoundQueryBuilder<any, TInsert, TIncludes>;`);
  lines.push(`}`);
  lines.push("");

  // Generate BoundQueryBuilder interface
  lines.push(`/**`);
  lines.push(` * A query builder with the WASM database bound.`);
  lines.push(` */`);
  lines.push(`export interface BoundQueryBuilder<T, TInsert, TIncludes> {`);
  lines.push(`  subscribeAll(callback: (rows: T[]) => void): Unsubscribe;`);
  lines.push(`  subscribe(id: ObjectId, callback: (row: T | null) => void): Unsubscribe;`);
  lines.push(`  where(filter: any): BoundQueryBuilder<T, TInsert, TIncludes>;`);
  lines.push(`  with<I extends TIncludes>(include: I): BoundQueryBuilder<any, TInsert, TIncludes>;`);
  lines.push(`  create(data: TInsert): ObjectId;`);
  lines.push(`  update(id: ObjectId, data: Partial<TInsert>): void;`);
  lines.push(`  delete(id: ObjectId): void;`);
  lines.push(`}`);
  lines.push("");

  // Generate createDatabase function
  lines.push(`/**`);
  lines.push(` * Create a database client with the WASM database bound.`);
  lines.push(` * This allows calling methods directly without passing the db instance.`);
  lines.push(` *`);
  lines.push(` * @example`);
  lines.push(` * \`\`\`typescript`);
  lines.push(` * const db = createDatabase(wasmDb);`);
  lines.push(` * const userId = db.users.create({ name: "Alice", ... });`);
  lines.push(` * db.users.subscribeAll((users) => console.log(users));`);
  lines.push(` * \`\`\``);
  lines.push(` */`);
  lines.push(`export function createDatabase(wasmDb: WasmDatabaseLike): Database {`);
  lines.push(`  function bindQueryBuilder<T, TInsert, TIncludes>(builder: any): BoundQueryBuilder<T, TInsert, TIncludes> {`);
  lines.push(`    return {`);
  lines.push(`      subscribeAll: (cb) => builder.subscribeAll(wasmDb, cb),`);
  lines.push(`      subscribe: (id, cb) => builder.subscribe(wasmDb, id, cb),`);
  lines.push(`      where: (filter) => bindQueryBuilder(builder.where(filter)),`);
  lines.push(`      with: (include) => bindQueryBuilder(builder.with(include)),`);
  lines.push(`      create: (data) => builder.create(wasmDb, data),`);
  lines.push(`      update: (id, data) => builder.update(wasmDb, id, data),`);
  lines.push(`      delete: (id) => builder.delete(wasmDb, id),`);
  lines.push(`    };`);
  lines.push(`  }`);
  lines.push("");
  lines.push(`  function bindTableClient<T, TInsert, TIncludes>(client: any): BoundTableClient<T, TInsert, TIncludes> {`);
  lines.push(`    return {`);
  lines.push(`      create: (data) => client.create(wasmDb, data),`);
  lines.push(`      update: (id, data) => client.update(wasmDb, id, data),`);
  lines.push(`      delete: (id) => client.delete(wasmDb, id),`);
  lines.push(`      subscribeAll: (cb) => client.subscribeAll(wasmDb, cb),`);
  lines.push(`      subscribe: (id, cb) => client.subscribe(wasmDb, id, cb),`);
  lines.push(`      where: (filter) => bindQueryBuilder(client.where(filter)),`);
  lines.push(`      with: (include) => bindQueryBuilder(client.with(include)),`);
  lines.push(`    };`);
  lines.push(`  }`);
  lines.push("");
  lines.push(`  return {`);
  for (const table of tables) {
    const propName = table.name.toLowerCase();
    lines.push(`    ${propName}: bindTableClient(app.${propName}),`);
  }
  lines.push(`  };`);
  lines.push(`}`);
  lines.push("");

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

  // Generate types, metadata, decoders, and client
  const types = generateTypes(tables, reverseRefs);
  const meta = generateMeta(tables, reverseRefs);
  const decoders = generateDecoders(tables);
  const client = generateClient(tables, reverseRefs);

  // Determine output path
  const outputDir = options?.output ?? dirname(sqlPath);
  mkdirSync(outputDir, { recursive: true });

  const typesPath = join(outputDir, "types.ts");
  const metaPath = join(outputDir, "meta.ts");
  const decodersPath = join(outputDir, "decoders.ts");
  const clientPath = join(outputDir, "client.ts");
  writeFileSync(typesPath, types);
  writeFileSync(metaPath, meta);
  writeFileSync(decodersPath, decoders);
  writeFileSync(clientPath, client);

  const elapsed = Date.now() - startTime;

  console.log(
    pc.green("✓") +
      ` Generated types, metadata, decoders, and client from ${pc.bold(tables.length)} table(s) in ${elapsed}ms`
  );
  console.log(`  ${pc.dim("→")} ${typesPath}`);
  console.log(`  ${pc.dim("→")} ${metaPath}`);
  console.log(`  ${pc.dim("→")} ${decodersPath}`);
  console.log(`  ${pc.dim("→")} ${clientPath}`);
}

// CLI entry point
if (process.argv[1]?.endsWith("from-sql.ts") || process.argv[1]?.endsWith("from-sql.js")) {
  const args = process.argv.slice(2);
  let sqlPath: string | undefined;
  let output: string | undefined;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--output" || args[i] === "-o") {
      output = args[++i];
    } else if (!args[i].startsWith("-")) {
      sqlPath = args[i];
    }
  }

  if (!sqlPath) {
    console.error("Usage: npx tsx from-sql.ts <schema.sql> [--output <dir>]");
    process.exit(1);
  }
  generateFromSql(sqlPath, { output });
}
