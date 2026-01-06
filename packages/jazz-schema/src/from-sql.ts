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
 * Generate binary decoder for a single column type
 *
 * For nullable refs, we detect null by checking if the first byte is 0.
 * Base32 ObjectIds use characters 0-9 and A-Z (ASCII 48-90), so byte 0 can't appear.
 * This matches Rust's encoding which writes byte 0 for null, or 26 bytes for a ref.
 */
function generateColumnDecoder(sqlType: SqlColumnType, varName: string, nullable: boolean): string[] {
  const lines: string[] = [];

  // Special case: nullable refs don't use a presence flag
  // Instead, we detect null by checking if the first byte is 0 (which can't appear in Base32)
  if (nullable && sqlType.kind === "ref") {
    lines.push(`    if (bytes[offset] === 0) {`);
    lines.push(`      row.${varName} = null;`);
    lines.push(`      offset++;`);
    lines.push(`    } else {`);
    lines.push(`      row.${varName} = decodeObjectId(bytes, offset);`);
    lines.push(`      offset += 26;`);
    lines.push(`    }`);
    return lines;
  }

  if (nullable) {
    lines.push(`    const ${varName}Present = view.getUint8(offset++);`);
    lines.push(`    if (${varName}Present === 0) {`);
    lines.push(`      row.${varName} = null;`);
    lines.push(`    } else {`);
  }

  const indent = nullable ? "      " : "    ";

  switch (sqlType.kind) {
    case "bool":
      lines.push(`${indent}row.${varName} = view.getUint8(offset++) === 1;`);
      break;
    case "i32":
      lines.push(`${indent}row.${varName} = view.getInt32(offset, true);`);
      lines.push(`${indent}offset += 4;`);
      break;
    case "u32":
      lines.push(`${indent}row.${varName} = view.getUint32(offset, true);`);
      lines.push(`${indent}offset += 4;`);
      break;
    case "i64":
      lines.push(`${indent}row.${varName} = view.getBigInt64(offset, true);`);
      lines.push(`${indent}offset += 8;`);
      break;
    case "f64":
      lines.push(`${indent}row.${varName} = view.getFloat64(offset, true);`);
      lines.push(`${indent}offset += 8;`);
      break;
    case "string":
      lines.push(`${indent}const ${varName}Len = view.getUint32(offset, true);`);
      lines.push(`${indent}offset += 4;`);
      lines.push(`${indent}row.${varName} = decoder.decode(new Uint8Array(buffer, offset, ${varName}Len));`);
      lines.push(`${indent}offset += ${varName}Len;`);
      break;
    case "bytes":
      lines.push(`${indent}const ${varName}Len = view.getUint32(offset, true);`);
      lines.push(`${indent}offset += 4;`);
      lines.push(`${indent}row.${varName} = new Uint8Array(buffer, offset, ${varName}Len);`);
      lines.push(`${indent}offset += ${varName}Len;`);
      break;
    case "ref":
      lines.push(`${indent}row.${varName} = decodeObjectId(bytes, offset);`);
      lines.push(`${indent}offset += 26;`);
      break;
  }

  if (nullable) {
    lines.push(`    }`);
  }

  return lines;
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
      return "reader.readString()";
    case "bytes":
      return "reader.readBytes()";
    case "ref":
      return "reader.readObjectId()";
  }
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
 * Generate binary decoders for all tables
 */
function generateDecoders(tables: ParsedTable[]): string {
  const lines: string[] = [
    "// Generated from SQL schema by @jazz/schema",
    "// DO NOT EDIT MANUALLY",
    "",
    "// Shared decoder for UTF-8 strings (used for variable-length strings)",
    "const decoder = new TextDecoder();",
    "",
    "// Delta type constants",
    "export const DELTA_ADDED = 1;",
    "export const DELTA_UPDATED = 2;",
    "export const DELTA_REMOVED = 3;",
    "",
    "/**",
    " * Fast ObjectId decoding using String.fromCharCode.",
    " * Since Base32 is ASCII-only, this is faster than TextDecoder.",
    " */",
    "function decodeObjectId(bytes: Uint8Array, offset: number): string {",
    "  return String.fromCharCode(",
    "    bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3], bytes[offset+4],",
    "    bytes[offset+5], bytes[offset+6], bytes[offset+7], bytes[offset+8], bytes[offset+9],",
    "    bytes[offset+10], bytes[offset+11], bytes[offset+12], bytes[offset+13], bytes[offset+14],",
    "    bytes[offset+15], bytes[offset+16], bytes[offset+17], bytes[offset+18], bytes[offset+19],",
    "    bytes[offset+20], bytes[offset+21], bytes[offset+22], bytes[offset+23], bytes[offset+24],",
    "    bytes[offset+25]",
    "  );",
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
    "    const id = decodeObjectId(this.bytes, this.offset);",
    "    this.offset += 26;",
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
    "  readString(): string {",
    "    const len = this.readU32();",
    "    const str = decoder.decode(new Uint8Array(this.bytes.buffer, this.offset, len));",
    "    this.offset += len;",
    "    return str;",
    "  }",
    "",
    "  readBytes(): Uint8Array {",
    "    const len = this.readU32();",
    "    const bytes = new Uint8Array(this.bytes.buffer, this.offset, len);",
    "    this.offset += len;",
    "    return bytes;",
    "  }",
    "",
    "  /** Read nullable value. Returns null if not present. */",
    "  readNullable<T>(readValue: () => T): T | null {",
    "    if (this.bytes[this.offset++] === 0) return null;",
    "    return readValue();",
    "  }",
    "",
    "  /**",
    "   * Read a nullable ObjectId ref.",
    "   * Uses byte-0 detection instead of presence flag since Base32 can't contain byte 0.",
    "   */",
    "  readNullableRef(): string | null {",
    "    if (this.bytes[this.offset] === 0) {",
    "      this.offset++;",
    "      return null;",
    "    }",
    "    return this.readObjectId();",
    "  }",
    "",
    "  /**",
    "   * Read an array of values.",
    "   * @param readElement Function to read each element",
    "   */",
    "  readArray<T>(readElement: () => T): T[] {",
    "    const count = this.readU32();",
    "    const arr = new Array(count);",
    "    for (let i = 0; i < count; i++) {",
    "      arr[i] = readElement();",
    "    }",
    "    return arr;",
    "  }",
    "}",
    "",
  ];

  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const rowType = generateRowType(table);

    // Generate batch decoder (with row count header)
    lines.push(`/**`);
    lines.push(` * Decode binary rows for ${table.name} table (batch format)`);
    lines.push(` * @param buffer ArrayBuffer from WASM`);
    lines.push(` * @returns Array of ${typeName} rows`);
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
    lines.push(`    const row: any = {};`);
    lines.push(``);
    lines.push(`    // Read ObjectId (26 bytes Base32)`);
    lines.push(`    row.id = decodeObjectId(bytes, offset);`);
    lines.push(`    offset += 26;`);
    lines.push(``);

    // Generate decoder for each column
    for (const col of table.columns) {
      lines.push(`    // ${col.name}: ${col.sqlType.kind}${col.nullable ? " (nullable)" : ""}`);
      lines.push(...generateColumnDecoder(col.sqlType, col.name, col.nullable));
      lines.push(``);
    }

    lines.push(`    rows[i] = row;`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  return rows;`);
    lines.push(`}`);
    lines.push(``);

    // Generate single row decoder (no header, for delta updates)
    lines.push(`/**`);
    lines.push(` * Decode a single ${typeName} row from binary (no header)`);
    lines.push(` * @param buffer ArrayBuffer containing a single row`);
    lines.push(` * @param startOffset Byte offset to start reading from`);
    lines.push(` * @returns Decoded row and bytes consumed`);
    lines.push(` */`);
    lines.push(`export function decode${typeName}Row(buffer: ArrayBufferLike, startOffset = 0): { row: ${rowType}; bytesRead: number } {`);
    lines.push(`  const bytes = new Uint8Array(buffer);`);
    lines.push(`  const view = new DataView(buffer as ArrayBuffer);`);
    lines.push(`  let offset = startOffset;`);
    lines.push(``);
    lines.push(`  const row: any = {};`);
    lines.push(``);
    lines.push(`  // Read ObjectId (26 bytes Base32)`);
    lines.push(`  row.id = decodeObjectId(bytes, offset);`);
    lines.push(`  offset += 26;`);
    lines.push(``);

    for (const col of table.columns) {
      lines.push(`  // ${col.name}: ${col.sqlType.kind}${col.nullable ? " (nullable)" : ""}`);
      // Adjust indentation for single-row decoder (2 spaces instead of 4)
      const colLines = generateColumnDecoder(col.sqlType, col.name, col.nullable);
      lines.push(...colLines.map(l => l.replace(/^    /, "  ")));
      lines.push(``);
    }

    lines.push(`  return { row, bytesRead: offset - startOffset };`);
    lines.push(`}`);
    lines.push(``);

    // Generate delta decoder
    lines.push(`/**`);
    lines.push(` * Decode a ${typeName} delta from binary`);
    lines.push(` * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id`);
    lines.push(` * @param buffer ArrayBuffer containing a single delta`);
    lines.push(` * @returns Decoded delta`);
    lines.push(` */`);
    lines.push(`export function decode${typeName}Delta(buffer: ArrayBufferLike): Delta<${rowType}> {`);
    lines.push(`  const bytes = new Uint8Array(buffer);`);
    lines.push(`  const deltaType = bytes[0];`);
    lines.push(``);
    lines.push(`  if (deltaType === DELTA_REMOVED) {`);
    lines.push(`    // Removed: just the ObjectId`);
    lines.push(`    const id = decodeObjectId(bytes, 1);`);
    lines.push(`    return { type: 'removed', id };`);
    lines.push(`  }`);
    lines.push(``);
    lines.push(`  // Added or Updated: decode the full row`);
    lines.push(`  const { row } = decode${typeName}Row(buffer, 1);`);
    lines.push(`  return {`);
    lines.push(`    type: deltaType === DELTA_ADDED ? 'added' : 'updated',`);
    lines.push(`    row`);
    lines.push(`  };`);
    lines.push(`}`);
    lines.push(``);

    // Generate reader function for composing with BinaryReader (for nested rows)
    lines.push(`/**`);
    lines.push(` * Read a ${typeName} row using a BinaryReader.`);
    lines.push(` * Use this for nested/joined row decoding.`);
    lines.push(` */`);
    lines.push(`export function read${typeName}(reader: BinaryReader): ${rowType} {`);
    lines.push(`  const id = reader.readObjectId();`);

    for (const col of table.columns) {
      if (col.nullable && col.sqlType.kind === "ref") {
        // Nullable refs use byte-0 detection instead of presence flag
        lines.push(`  const ${col.name} = reader.readNullableRef();`);
      } else if (col.nullable) {
        lines.push(`  const ${col.name} = reader.readNullable(() => ${generateReaderCall(col.sqlType)});`);
      } else {
        lines.push(`  const ${col.name} = ${generateReaderCall(col.sqlType)};`);
      }
    }

    const fieldNames = ["id", ...table.columns.map(c => c.name)];
    lines.push(`  return { ${fieldNames.join(", ")} };`);
    lines.push(`}`);
    lines.push(``);
  }

  return lines.join("\n");
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
    'import { TableClient, type WasmDatabaseLike, type Unsubscribe, type TableDecoder, type BaseWhereInput, type IncludeSpec } from "@jazz/client";',
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
    typeImports.push(`${typeName}Loaded`);
    typeImports.push(`${typeName}Filter`);
  }
  lines.push(`import type { ObjectId, ${typeImports.join(", ")} } from "./types.js";`);
  lines.push("");

  // Generate query builder classes for tables with refs
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const clientName = `${table.name}Client`;
    const builderName = `${table.name}QueryBuilder`;
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];
    const hasRefs = table.columns.some(c => c.sqlType.kind === "ref");
    const hasReverseRefs = tableReverseRefs.length > 0;

    if (hasRefs || hasReverseRefs) {
      lines.push(`/**`);
      lines.push(` * Query builder for ${table.name} with chainable where/with methods`);
      lines.push(` */`);
      lines.push(`export class ${builderName}<I extends ${typeName}Includes = {}> {`);
      lines.push(`  private _client: ${clientName};`);
      lines.push(`  private _where?: ${typeName}Filter;`);
      lines.push(`  private _include?: I;`);
      lines.push(``);
      lines.push(`  constructor(client: ${clientName}, where?: ${typeName}Filter, include?: I) {`);
      lines.push(`    this._client = client;`);
      lines.push(`    this._where = where;`);
      lines.push(`    this._include = include;`);
      lines.push(`  }`);
      lines.push(``);
      lines.push(`  /**`);
      lines.push(`   * Get a stable key representing this query's options (for React hook deduplication)`);
      lines.push(`   */`);
      lines.push(`  get _queryKey(): string {`);
      lines.push(`    return JSON.stringify({ t: "${table.name}", w: this._where, i: this._include });`);
      lines.push(`  }`);
      lines.push(``);
      lines.push(`  /**`);
      lines.push(`   * Add a filter condition`);
      lines.push(`   */`);
      lines.push(`  where(filter: ${typeName}Filter): ${builderName}<I> {`);
      lines.push(`    return new ${builderName}(this._client, filter, this._include);`);
      lines.push(`  }`);
      lines.push(``);
      lines.push(`  /**`);
      lines.push(`   * Specify which refs to include`);
      lines.push(`   */`);
      lines.push(`  with<NewI extends ${typeName}Includes>(include: NewI): ${builderName}<NewI> {`);
      lines.push(`    return new ${builderName}(this._client, this._where, include);`);
      lines.push(`  }`);
      lines.push(``);
      lines.push(`  /**`);
      lines.push(`   * Subscribe to all matching ${table.name}`);
      lines.push(`   */`);
      lines.push(`  subscribeAll(callback: (rows: ${typeName}Loaded<I>[]) => void): Unsubscribe {`);
      lines.push(`    return this._client._subscribeAllInternal(`);
      lines.push(`      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },`);
      lines.push(`      callback as (rows: ${typeName}[]) => void`);
      lines.push(`    );`);
      lines.push(`  }`);
      lines.push(``);
      lines.push(`  /**`);
      lines.push(`   * Subscribe to a single ${typeName} by ID`);
      lines.push(`   */`);
      lines.push(`  subscribe(id: ObjectId, callback: (row: ${typeName}Loaded<I> | null) => void): Unsubscribe {`);
      lines.push(`    return this._client._subscribeInternal(`);
      lines.push(`      id,`);
      lines.push(`      { include: this._include as IncludeSpec | undefined },`);
      lines.push(`      callback as (row: ${typeName} | null) => void`);
      lines.push(`    );`);
      lines.push(`  }`);
      lines.push(`}`);
      lines.push(``);
    }
  }

  // Generate table client classes
  for (const table of tables) {
    const typeName = singularize(toPascalCase(table.name));
    const clientName = `${table.name}Client`;
    const builderName = `${table.name}QueryBuilder`;
    const tableReverseRefs = reverseRefs.get(table.name) ?? [];
    const hasRefs = table.columns.some(c => c.sqlType.kind === "ref");
    const hasReverseRefs = tableReverseRefs.length > 0;

    lines.push(`/**`);
    lines.push(` * Client for the ${table.name} table`);
    lines.push(` */`);
    lines.push(`export class ${clientName} extends TableClient<${typeName}> {`);
    lines.push(`  constructor(db: WasmDatabaseLike) {`);
    lines.push(`    super(db, schemaMeta.tables.${table.name}, schemaMeta, {`);
    lines.push(`      rows: decode${typeName}Rows,`);
    lines.push(`      delta: decode${typeName}Delta,`);
    lines.push(`    });`);
    lines.push(`  }`);
    lines.push("");

    // create method
    lines.push(`  /**`);
    lines.push(`   * Create a new ${typeName}`);
    lines.push(`   * @returns The ObjectId of the created row`);
    lines.push(`   */`);
    lines.push(`  create(data: ${typeName}Insert): ObjectId {`);
    lines.push(`    const values: Record<string, unknown> = {};`);
    for (const col of table.columns) {
      if (col.nullable) {
        lines.push(`    if (data.${col.name} !== undefined) values.${col.name} = data.${col.name};`);
      } else {
        lines.push(`    values.${col.name} = data.${col.name};`);
      }
    }
    lines.push(`    return this._create(values);`);
    lines.push(`  }`);
    lines.push("");

    // update method
    lines.push(`  /**`);
    lines.push(`   * Update an existing ${typeName}`);
    lines.push(`   */`);
    lines.push(`  update(id: ObjectId, data: Partial<${typeName}Insert>): void {`);
    lines.push(`    this._update(id, data as Record<string, unknown>);`);
    lines.push(`  }`);
    lines.push("");

    // delete method
    lines.push(`  /**`);
    lines.push(`   * Delete a ${typeName}`);
    lines.push(`   */`);
    lines.push(`  delete(id: ObjectId): void {`);
    lines.push(`    this._delete(id);`);
    lines.push(`  }`);
    lines.push("");

    if (hasRefs || hasReverseRefs) {
      // Builder entry point: where()
      lines.push(`  /**`);
      lines.push(`   * Start a query with a filter condition`);
      lines.push(`   */`);
      lines.push(`  where(filter: ${typeName}Filter): ${builderName}<{}> {`);
      lines.push(`    return new ${builderName}(this, filter, undefined);`);
      lines.push(`  }`);
      lines.push("");

      // Builder entry point: with()
      lines.push(`  /**`);
      lines.push(`   * Start a query with includes`);
      lines.push(`   */`);
      lines.push(`  with<I extends ${typeName}Includes>(include: I): ${builderName}<I> {`);
      lines.push(`    return new ${builderName}(this, undefined, include);`);
      lines.push(`  }`);
      lines.push("");

      // subscribe method (direct, no includes)
      lines.push(`  /**`);
      lines.push(`   * Subscribe to a single ${typeName} by ID`);
      lines.push(`   */`);
      lines.push(`  subscribe(id: ObjectId, callback: (row: ${typeName} | null) => void): Unsubscribe {`);
      lines.push(`    return this._subscribe(id, {}, callback);`);
      lines.push(`  }`);
      lines.push("");

      // subscribeAll method (direct, no filters/includes)
      lines.push(`  /**`);
      lines.push(`   * Subscribe to all ${table.name}`);
      lines.push(`   */`);
      lines.push(`  subscribeAll(callback: (rows: ${typeName}[]) => void): Unsubscribe {`);
      lines.push(`    return this._subscribeAll({}, callback);`);
      lines.push(`  }`);
      lines.push("");

      // Internal methods for query builder to call
      lines.push(`  /** @internal Used by query builder */`);
      lines.push(`  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: ${typeName}[]) => void): Unsubscribe {`);
      lines.push(`    return this._subscribeAll(options, callback);`);
      lines.push(`  }`);
      lines.push("");

      lines.push(`  /** @internal Used by query builder */`);
      lines.push(`  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: ${typeName} | null) => void): Unsubscribe {`);
      lines.push(`    return this._subscribe(id, options, callback);`);
      lines.push(`  }`);
    } else {
      // No refs - simpler methods without builder
      lines.push(`  /**`);
      lines.push(`   * Subscribe to a single ${typeName} by ID`);
      lines.push(`   */`);
      lines.push(`  subscribe(id: ObjectId, callback: (row: ${typeName} | null) => void): Unsubscribe {`);
      lines.push(`    return this._subscribe(id, {}, callback);`);
      lines.push(`  }`);
      lines.push("");

      lines.push(`  /**`);
      lines.push(`   * Subscribe to all ${table.name}`);
      lines.push(`   */`);
      lines.push(`  subscribeAll(callback: (rows: ${typeName}[]) => void): Unsubscribe {`);
      lines.push(`    return this._subscribeAll({}, callback);`);
      lines.push(`  }`);
      lines.push("");

      // For simple tables, add where method that returns a simple query object
      lines.push(`  /**`);
      lines.push(`   * Subscribe to ${table.name} matching a filter`);
      lines.push(`   */`);
      lines.push(`  where(filter: ${typeName}Filter): { subscribeAll(callback: (rows: ${typeName}[]) => void): Unsubscribe } {`);
      lines.push(`    return {`);
      lines.push(`      subscribeAll: (callback) => this._subscribeAll({ where: filter as BaseWhereInput }, callback)`);
      lines.push(`    };`);
      lines.push(`  }`);
    }

    lines.push(`}`);
    lines.push("");
  }

  // Generate Database interface
  lines.push(`/**`);
  lines.push(` * Typed database interface`);
  lines.push(` */`);
  lines.push(`export interface Database {`);
  lines.push(`  /** Raw WASM database for direct SQL access */`);
  lines.push(`  raw: WasmDatabaseLike;`);
  for (const table of tables) {
    const clientName = `${table.name}Client`;
    const propName = table.name.toLowerCase();
    lines.push(`  ${propName}: ${clientName};`);
  }
  lines.push(`}`);
  lines.push("");

  // Generate createDatabase function
  lines.push(`/**`);
  lines.push(` * Create a typed database client from a WASM database instance.`);
  lines.push(` *`);
  lines.push(` * @example`);
  lines.push(` * \`\`\`typescript`);
  lines.push(` * import init, { WasmDatabase } from './pkg/groove_wasm.js';`);
  lines.push(` *`);
  lines.push(` * await init();`);
  lines.push(` * const wasmDb = new WasmDatabase();`);
  lines.push(` * const db = createDatabase(wasmDb);`);
  lines.push(` *`);
  lines.push(` * // Create a user`);
  lines.push(` * const userId = db.users.create({ name: 'Alice', email: 'alice@example.com' });`);
  lines.push(` *`);
  lines.push(` * // Subscribe to all users`);
  lines.push(` * db.users.subscribeAll({}, (users) => console.log(users));`);
  lines.push(` * \`\`\``);
  lines.push(` */`);
  lines.push(`export function createDatabase(wasmDb: WasmDatabaseLike): Database {`);
  lines.push(`  return {`);
  lines.push(`    raw: wasmDb,`);
  for (const table of tables) {
    const clientName = `${table.name}Client`;
    const propName = table.name.toLowerCase();
    lines.push(`    ${propName}: new ${clientName}(wasmDb),`);
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
