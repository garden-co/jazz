/**
 * Generate TypeScript query builder classes from WasmSchema.
 *
 * Produces:
 * 1. WhereInput types for type-safe filtering
 * 2. QueryBuilder classes with fluent API
 * 3. App export with table proxies
 */

import type { WasmSchema, ColumnType } from "../drivers/types.js";
import { tableNameToInterface } from "./type-generator.js";
import type { Relation } from "./relation-analyzer.js";

function columnTypeToTs(type: ColumnType): string {
  switch (type.type) {
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
    case "Array":
      return `${columnTypeToTs(type.element)}[]`;
    default:
      return "unknown";
  }
}

/**
 * Generate WhereInput type for a column based on its type.
 */
function columnToWhereInputType(col: {
  name: string;
  column_type: ColumnType;
  nullable: boolean;
  references?: string;
}): string {
  const baseType = col.column_type.type;

  switch (baseType) {
    case "Text":
      return "string | { eq?: string; ne?: string; contains?: string }";
    case "Boolean":
      return "boolean";
    case "Integer":
    case "BigInt":
      return "number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number }";
    case "Timestamp":
      return "number | { eq?: number; gt?: number; gte?: number; lt?: number; lte?: number }";
    case "Uuid":
      if (col.references) {
        // FK - add isNull for optional refs
        return col.nullable
          ? "string | { eq?: string; ne?: string; isNull?: boolean }"
          : "string | { eq?: string; ne?: string }";
      }
      return "string | { eq?: string; ne?: string; in?: string[] }";
    case "Array": {
      const elementTs = columnTypeToTs(col.column_type.element);
      const arrayTs = `${elementTs}[]`;
      return `${arrayTs} | { eq?: ${arrayTs}; contains?: ${elementTs} }`;
    }
    default:
      return "unknown";
  }
}

/**
 * Generate WhereInput interfaces for all tables.
 */
export function generateWhereInputTypes(schema: WasmSchema): string[] {
  const lines: string[] = [];

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const interfaceName = tableNameToInterface(tableName) + "WhereInput";
    lines.push(`export interface ${interfaceName} {`);

    // Always include id
    lines.push(`  id?: string | { eq?: string; ne?: string; in?: string[] };`);

    for (const col of table.columns) {
      const type = columnToWhereInputType(col);
      lines.push(`  ${col.name}?: ${type};`);
    }
    lines.push(`}`);
    lines.push(``);
  }

  return lines;
}

/**
 * Generate QueryBuilder class for a table.
 */
function generateQueryBuilderClass(
  tableName: string,
  relations: Map<string, Relation[]>,
): string[] {
  const lines: string[] = [];
  const interfaceName = tableNameToInterface(tableName);
  const whereInputInterface = interfaceName + "WhereInput";
  const tableRels = relations.get(tableName) || [];
  const hasRelations = tableRels.length > 0;

  // Determine Include type - use the interface if it exists, otherwise empty object
  const includeConstraint = hasRelations ? `${interfaceName}Include` : "Record<string, never>";

  lines.push(
    `export class ${interfaceName}QueryBuilder<I extends ${includeConstraint} = {}> implements QueryBuilder<${interfaceName}> {`,
  );
  lines.push(`  readonly _table = "${tableName}";`);
  lines.push(`  readonly _schema: WasmSchema = wasmSchema;`);
  // Phantom fields used only for type inference.
  lines.push(`  declare readonly _rowType: ${interfaceName};`);
  lines.push(`  declare readonly _initType: ${interfaceName}Init;`);
  lines.push(`  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];`);
  lines.push(`  private _includes: Partial<${includeConstraint}> = {};`);
  lines.push(`  private _orderBys: Array<[string, "asc" | "desc"]> = [];`);
  lines.push(`  private _limitVal?: number;`);
  lines.push(`  private _offsetVal?: number;`);
  lines.push(`  private _recursiveVal?: {`);
  lines.push(`    table: string;`);
  lines.push(`    inner_column: string;`);
  lines.push(`    outer_column: string;`);
  lines.push(`    select_columns: string[] | null;`);
  lines.push(`    max_depth: number;`);
  lines.push(`  };`);
  lines.push(``);

  // where() method
  lines.push(`  where(conditions: ${whereInputInterface}): ${interfaceName}QueryBuilder<I> {`);
  lines.push(`    const clone = this._clone();`);
  lines.push(`    for (const [key, value] of Object.entries(conditions)) {`);
  lines.push(`      if (value === undefined) continue;`);
  lines.push(`      if (typeof value === "object" && value !== null && !Array.isArray(value)) {`);
  lines.push(`        for (const [op, opValue] of Object.entries(value)) {`);
  lines.push(`          if (opValue !== undefined) {`);
  lines.push(`            clone._conditions.push({ column: key, op, value: opValue });`);
  lines.push(`          }`);
  lines.push(`        }`);
  lines.push(`      } else {`);
  lines.push(`        clone._conditions.push({ column: key, op: "eq", value });`);
  lines.push(`      }`);
  lines.push(`    }`);
  lines.push(`    return clone;`);
  lines.push(`  }`);
  lines.push(``);

  // include() method - only if table has relations
  if (hasRelations) {
    const includeInterface = interfaceName + "Include";
    lines.push(
      `  include<NewI extends ${includeInterface}>(relations: NewI): ${interfaceName}QueryBuilder<I & NewI> {`,
    );
    lines.push(
      `    const clone = this._clone() as unknown as ${interfaceName}QueryBuilder<I & NewI>;`,
    );
    lines.push(`    clone._includes = { ...this._includes, ...relations };`);
    lines.push(`    return clone;`);
    lines.push(`  }`);
    lines.push(``);
  }

  // orderBy() method
  lines.push(
    `  orderBy(column: keyof ${interfaceName}, direction: "asc" | "desc" = "asc"): ${interfaceName}QueryBuilder<I> {`,
  );
  lines.push(`    const clone = this._clone();`);
  lines.push(`    clone._orderBys.push([column as string, direction]);`);
  lines.push(`    return clone;`);
  lines.push(`  }`);
  lines.push(``);

  // limit() method
  lines.push(`  limit(n: number): ${interfaceName}QueryBuilder<I> {`);
  lines.push(`    const clone = this._clone();`);
  lines.push(`    clone._limitVal = n;`);
  lines.push(`    return clone;`);
  lines.push(`  }`);
  lines.push(``);

  // offset() method
  lines.push(`  offset(n: number): ${interfaceName}QueryBuilder<I> {`);
  lines.push(`    const clone = this._clone();`);
  lines.push(`    clone._offsetVal = n;`);
  lines.push(`    return clone;`);
  lines.push(`  }`);
  lines.push(``);

  // withRecursive() method
  lines.push(`  withRecursive(options: {`);
  lines.push(`    from: string;`);
  lines.push(`    correlate: { inner: string; outer: string };`);
  lines.push(`    select?: ReadonlyArray<string>;`);
  lines.push(`    maxDepth?: number;`);
  lines.push(`  }): ${interfaceName}QueryBuilder<I> {`);
  lines.push(`    const clone = this._clone();`);
  lines.push(``);
  lines.push(`    if (typeof options.from !== "string" || !options.from.trim()) {`);
  lines.push(
    `      throw new Error("withRecursive(...) requires from to be a non-empty table name.");`,
  );
  lines.push(`    }`);
  lines.push(
    `    if (typeof options.correlate?.inner !== "string" || !options.correlate.inner.trim()) {`,
  );
  lines.push(
    `      throw new Error("withRecursive(...) requires correlate.inner to be a non-empty column name.");`,
  );
  lines.push(`    }`);
  lines.push(
    `    if (typeof options.correlate?.outer !== "string" || !options.correlate.outer.trim()) {`,
  );
  lines.push(
    `      throw new Error("withRecursive(...) requires correlate.outer to be a non-empty column name.");`,
  );
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const maxDepth = options.maxDepth ?? 10;`);
  lines.push(`    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {`);
  lines.push(`      throw new Error("withRecursive(...) maxDepth must be a positive integer.");`);
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    if (options.select !== undefined) {`);
  lines.push(`      if (!Array.isArray(options.select) || options.select.length === 0) {`);
  lines.push(
    `        throw new Error("withRecursive(...) select must be a non-empty array when provided.");`,
  );
  lines.push(`      }`);
  lines.push(
    `      if (options.select.some((column) => typeof column !== "string" || !column.trim())) {`,
  );
  lines.push(
    `        throw new Error("withRecursive(...) select entries must be non-empty column names.");`,
  );
  lines.push(`      }`);
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    clone._recursiveVal = {`);
  lines.push(`      table: options.from,`);
  lines.push(`      inner_column: options.correlate.inner,`);
  lines.push(`      outer_column: options.correlate.outer,`);
  lines.push(`      select_columns: options.select ? [...options.select] : null,`);
  lines.push(`      max_depth: maxDepth,`);
  lines.push(`    };`);
  lines.push(``);
  lines.push(`    return clone;`);
  lines.push(`  }`);
  lines.push(``);

  // _build() method
  lines.push(`  _build(): string {`);
  lines.push(`    return JSON.stringify({`);
  lines.push(`      table: this._table,`);
  lines.push(`      conditions: this._conditions,`);
  lines.push(`      includes: this._includes,`);
  lines.push(`      orderBy: this._orderBys,`);
  lines.push(`      limit: this._limitVal,`);
  lines.push(`      offset: this._offsetVal,`);
  lines.push(`      recursive: this._recursiveVal,`);
  lines.push(`    });`);
  lines.push(`  }`);
  lines.push(``);

  // _clone() method
  lines.push(`  private _clone(): ${interfaceName}QueryBuilder<I> {`);
  lines.push(`    const clone = new ${interfaceName}QueryBuilder<I>();`);
  lines.push(`    clone._conditions = [...this._conditions];`);
  lines.push(`    clone._includes = { ...this._includes };`);
  lines.push(`    clone._orderBys = [...this._orderBys];`);
  lines.push(`    clone._limitVal = this._limitVal;`);
  lines.push(`    clone._offsetVal = this._offsetVal;`);
  lines.push(
    `    clone._recursiveVal = this._recursiveVal ? { ...this._recursiveVal, select_columns: this._recursiveVal.select_columns ? [...this._recursiveVal.select_columns] : null } : undefined;`,
  );
  lines.push(`    return clone;`);
  lines.push(`  }`);

  lines.push(`}`);
  lines.push(``);

  return lines;
}

/**
 * Generate all QueryBuilder classes.
 */
export function generateQueryBuilderClasses(
  schema: WasmSchema,
  relations: Map<string, Relation[]>,
): string[] {
  const lines: string[] = [];

  for (const tableName of Object.keys(schema.tables)) {
    lines.push(...generateQueryBuilderClass(tableName, relations));
  }

  return lines;
}

/**
 * Generate the app export object.
 */
export function generateAppExport(schema: WasmSchema): string[] {
  const lines: string[] = [];

  lines.push(`export const app = {`);
  for (const tableName of Object.keys(schema.tables)) {
    const interfaceName = tableNameToInterface(tableName);
    lines.push(`  ${tableName}: new ${interfaceName}QueryBuilder(),`);
  }
  lines.push(`  wasmSchema,`);
  lines.push(`};`);
  lines.push(``);

  return lines;
}
