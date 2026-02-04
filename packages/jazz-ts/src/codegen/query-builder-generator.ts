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
  schema: WasmSchema,
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
  lines.push(`  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];`);
  lines.push(`  private _includes: Partial<${includeConstraint}> = {};`);
  lines.push(`  private _orderBys: Array<[string, "asc" | "desc"]> = [];`);
  lines.push(`  private _limitVal?: number;`);
  lines.push(`  private _offsetVal?: number;`);
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

  // _build() method
  lines.push(`  _build(): string {`);
  lines.push(`    return JSON.stringify({`);
  lines.push(`      table: this._table,`);
  lines.push(`      conditions: this._conditions,`);
  lines.push(`      includes: this._includes,`);
  lines.push(`      orderBy: this._orderBys,`);
  lines.push(`      limit: this._limitVal,`);
  lines.push(`      offset: this._offsetVal,`);
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
    lines.push(...generateQueryBuilderClass(tableName, schema, relations));
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
