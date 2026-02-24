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

function arrayType(elementTs: string): string {
  return elementTs.includes("|") ? `(${elementTs})[]` : `${elementTs}[]`;
}

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
    case "Bytea":
      return "Uint8Array";
    case "Enum":
      return type.variants.map((variant: string) => JSON.stringify(variant)).join(" | ");
    case "Array":
      return arrayType(columnTypeToTs(type.element));
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
    case "Bytea":
      return "Uint8Array | { eq?: Uint8Array; ne?: Uint8Array }";
    case "Enum": {
      const variants = col.column_type.variants
        .map((variant: string) => JSON.stringify(variant))
        .join(" | ");
      return `${variants} | { eq?: ${variants}; ne?: ${variants}; in?: (${variants})[] }`;
    }
    case "Array": {
      const elementTs = columnTypeToTs(col.column_type.element);
      const arrayTs = arrayType(elementTs);
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
  lines.push(`  private _hops: string[] = [];`);
  lines.push(`  private _gatherVal?: {`);
  lines.push(`    max_depth: number;`);
  lines.push(`    step_table: string;`);
  lines.push(`    step_current_column: string;`);
  lines.push(`    step_conditions: Array<{ column: string; op: string; value: unknown }>;`);
  lines.push(`    step_hops: string[];`);
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

  if (hasRelations) {
    const relationUnion = tableRels.map((rel) => `"${rel.name}"`).join(" | ");
    lines.push(`  hopTo(relation: ${relationUnion}): ${interfaceName}QueryBuilder<I> {`);
    lines.push(`    const clone = this._clone();`);
    lines.push(`    clone._hops.push(relation);`);
    lines.push(`    return clone;`);
    lines.push(`  }`);
    lines.push(``);
  }

  // gather() method
  lines.push(`  gather(options: {`);
  lines.push(`    start: ${whereInputInterface};`);
  lines.push(`    step: (ctx: { current: any }) => unknown;`);
  lines.push(`    maxDepth?: number;`);
  lines.push(`  }): ${interfaceName}QueryBuilder<I> {`);
  lines.push(`    if (options.start === undefined) {`);
  lines.push(`      throw new Error("gather(...) requires start where conditions.");`);
  lines.push(`    }`);
  lines.push(`    if (typeof options.step !== "function") {`);
  lines.push(`      throw new Error("gather(...) requires step callback.");`);
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const maxDepth = options.maxDepth ?? 10;`);
  lines.push(`    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {`);
  lines.push(`      throw new Error("gather(...) maxDepth must be a positive integer.");`);
  lines.push(`    }`);
  lines.push(`    if (Object.keys(this._includes).length > 0) {`);
  lines.push(`      throw new Error("gather(...) does not support include(...) in MVP.");`);
  lines.push(`    }`);
  lines.push(`    if (this._hops.length > 0) {`);
  lines.push(`      throw new Error("gather(...) must be called before hopTo(...).");`);
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const currentToken = "__jazz_gather_current__";`);
  lines.push(`    const stepOutput = options.step({ current: currentToken });`);
  lines.push(
    `    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {`,
  );
  lines.push(
    `      throw new Error("gather(...) step must return a query expression built from app.<table>.");`,
  );
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const stepBuilt = JSON.parse(`);
  lines.push(`      (stepOutput as { _build: () => string })._build(),`);
  lines.push(`    ) as {`);
  lines.push(`      table?: unknown;`);
  lines.push(`      conditions?: Array<{ column: string; op: string; value: unknown }>;`);
  lines.push(`      hops?: unknown;`);
  lines.push(`    };`);
  lines.push(``);
  lines.push(`    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {`);
  lines.push(`      throw new Error("gather(...) step query is missing table metadata.");`);
  lines.push(`    }`);
  lines.push(`    if (!Array.isArray(stepBuilt.conditions)) {`);
  lines.push(`      throw new Error("gather(...) step query is missing condition metadata.");`);
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const stepHops = Array.isArray(stepBuilt.hops)`);
  lines.push(`      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")`);
  lines.push(`      : [];`);
  lines.push(`    if (stepHops.length !== 1) {`);
  lines.push(`      throw new Error("gather(...) step must include exactly one hopTo(...).");`);
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const currentConditions = stepBuilt.conditions.filter(`);
  lines.push(`      (condition) => condition.op === "eq" && condition.value === currentToken,`);
  lines.push(`    );`);
  lines.push(`    if (currentConditions.length !== 1) {`);
  lines.push(
    `      throw new Error("gather(...) step must include exactly one where condition bound to current.");`,
  );
  lines.push(`    }`);
  lines.push(``);
  lines.push(`    const currentCondition = currentConditions[0];`);
  lines.push(`    const stepConditions = stepBuilt.conditions.filter(`);
  lines.push(`      (condition) => !(condition.op === "eq" && condition.value === currentToken),`);
  lines.push(`    );`);
  lines.push(``);
  lines.push(`    const withStart = this.where(options.start);`);
  lines.push(`    const clone = withStart._clone();`);
  lines.push(`    clone._hops = [];`);
  lines.push(`    clone._gatherVal = {`);
  lines.push(`      max_depth: maxDepth,`);
  lines.push(`      step_table: stepBuilt.table,`);
  lines.push(`      step_current_column: currentCondition.column,`);
  lines.push(`      step_conditions: stepConditions,`);
  lines.push(`      step_hops: stepHops,`);
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
  lines.push(`      hops: this._hops,`);
  lines.push(`      gather: this._gatherVal,`);
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
  lines.push(`    clone._hops = [...this._hops];`);
  lines.push(`    clone._gatherVal = this._gatherVal`);
  lines.push(`      ? {`);
  lines.push(`          ...this._gatherVal,`);
  lines.push(
    `          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),`,
  );
  lines.push(`          step_hops: [...this._gatherVal.step_hops],`);
  lines.push(`        }`);
  lines.push(`      : undefined;`);
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
