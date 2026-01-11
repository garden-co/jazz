/**
 * Runtime utilities for @jazz/schema
 *
 * This module provides:
 * - Table metadata types for runtime query building
 * - SQL query builder for subscribe/subscribeAll
 * - Type-safe client interfaces
 */

// === Table Metadata Types ===

/**
 * Runtime column type information
 */
export type ColumnType =
  | { kind: "bool" }
  | { kind: "i64" }
  | { kind: "f64" }
  | { kind: "string" }
  | { kind: "bytes" }
  | { kind: "ref"; table: string };

/**
 * Column metadata for runtime query building
 */
export interface ColumnMeta {
  name: string;
  type: ColumnType;
  nullable: boolean;
}

/**
 * Forward reference metadata (e.g., Note.author -> User)
 */
export interface RefMeta {
  /** Column name in this table */
  column: string;
  /** Target table name */
  targetTable: string;
  /** Whether the reference is nullable */
  nullable: boolean;
}

/**
 * Reverse reference metadata (e.g., User.Notes from Note.author)
 */
export interface ReverseRefMeta {
  /** Property name for this reverse ref (e.g., "Notes") */
  name: string;
  /** Source table that has the forward ref */
  sourceTable: string;
  /** Column in source table that references this table */
  sourceColumn: string;
}

/**
 * Complete table metadata for runtime operations
 */
export interface TableMeta {
  name: string;
  columns: ColumnMeta[];
  refs: RefMeta[];
  reverseRefs: ReverseRefMeta[];
}

/**
 * Schema metadata containing all tables
 */
export interface SchemaMeta {
  tables: Record<string, TableMeta>;
}

// === Prisma-style Filter Types ===

/**
 * Filter for string columns (including ObjectId refs)
 */
export interface StringFilter {
  equals?: string;
  not?: string | StringFilter;
  contains?: string;
  startsWith?: string;
  endsWith?: string;
  in?: string[];
  notIn?: string[];
}

/**
 * Filter for bigint (i64) columns
 */
export interface BigIntFilter {
  equals?: bigint;
  not?: bigint | BigIntFilter;
  gt?: bigint;
  gte?: bigint;
  lt?: bigint;
  lte?: bigint;
  in?: bigint[];
  notIn?: bigint[];
}

/**
 * Filter for number (f64) columns
 */
export interface NumberFilter {
  equals?: number;
  not?: number | NumberFilter;
  gt?: number;
  gte?: number;
  lt?: number;
  lte?: number;
  in?: number[];
  notIn?: number[];
}

/**
 * Filter for boolean columns
 */
export interface BoolFilter {
  equals?: boolean;
  not?: boolean | BoolFilter;
}

/**
 * Nullable wrapper for filters
 */
export type Nullable<T> = T | null;

/**
 * Relation filter for "has related records matching condition"
 * Used for filtering via junction tables (many-to-many) or reverse refs (one-to-many)
 */
export interface RelationFilter<T = BaseWhereInput> {
  /** At least one related record matches */
  some?: T;
  /** All related records match (or no related records exist) */
  every?: T;
  /** No related records match */
  none?: T;
}

/**
 * Base where input interface - extended by generated per-table types.
 * The index signature allows arbitrary column filters at runtime.
 */
export interface BaseWhereInput {
  AND?: BaseWhereInput | BaseWhereInput[];
  OR?: BaseWhereInput[];
  NOT?: BaseWhereInput | BaseWhereInput[];
  [column: string]: unknown;
}

/**
 * Include specification for eager loading.
 * Can be `true` for shallow load or nested object for deep load.
 */
export interface IncludeSpec {
  [key: string]: true | IncludeSpec;
}

/**
 * Options for subscribeAll queries (generic version)
 */
export interface SubscribeAllOptions<
  W extends BaseWhereInput = BaseWhereInput,
> {
  where?: W;
  include?: IncludeSpec;
}

/**
 * Options for single-row subscribe queries
 */
export interface SubscribeOptions {
  include?: IncludeSpec;
}

// === Query Builder ===

/**
 * Build a SQL query for a table with optional where/include clauses.
 *
 * @example
 * ```typescript
 * // Simple query
 * buildQuery(noteMeta, {})
 * // => "SELECT n.* FROM Note n"
 *
 * // With where clause (Prisma-style)
 * buildQuery(noteMeta, { where: { title: { equals: "Hello" } } })
 * // => "SELECT n.* FROM Note n WHERE n.title = 'Hello'"
 *
 * // With include (forward ref)
 * buildQuery(noteMeta, { include: { author: true } })
 * // => "SELECT n.*, ROW(...) as author FROM Note n JOIN User u ON n.author = u.id"
 *
 * // With AND/OR combinators
 * buildQuery(noteMeta, { where: { OR: [{ title: "A" }, { title: "B" }] } })
 * // => "SELECT n.* FROM Note n WHERE (n.title = 'A' OR n.title = 'B')"
 * ```
 */
export function buildQuery(
  table: TableMeta,
  schema: SchemaMeta,
  options: SubscribeAllOptions = {},
): string {
  const alias = table.name.toLowerCase()[0];
  const parts: string[] = [];
  const allJoins: string[] = [];

  // Build projection - expand to explicit columns (Groove doesn't support .*)
  // NOTE: We include ALL columns including FK columns, because JoinedRow outputs
  // all columns flat. The decoder reads FK columns and uses them as the nested
  // object's id.
  const baseColumns = [
    `${alias}.id`,
    ...table.columns.map((c) => `${alias}.${c.name}`),
  ];
  const projections: string[] = baseColumns;

  if (options.include) {
    for (const [key, includeValue] of Object.entries(options.include)) {
      const projection = buildIncludeProjection(
        table,
        schema,
        alias,
        key,
        includeValue,
      );
      if (projection) {
        projections.push(projection);
      }
    }
  }

  // Build JOINs for forward refs that are included
  if (options.include) {
    const joins = buildJoins(table, schema, alias, options.include);
    allJoins.push(...joins);
  }

  // Extract relation filters and build JOINs for them
  const relationJoinInfo: Map<
    string,
    { tableName: string; conditions: string[] }
  > = new Map();

  if (options.where) {
    extractRelationFilters(
      table,
      schema,
      alias,
      options.where,
      allJoins,
      relationJoinInfo,
    );
  }

  parts.push(`SELECT ${projections.join(", ")}`);
  parts.push(`FROM ${table.name} ${alias}`);

  if (allJoins.length > 0) {
    parts.push(allJoins.join(" "));
  }

  // Build WHERE clause (including relation filter conditions)
  const whereConditions: string[] = [];

  if (options.where && Object.keys(options.where).length > 0) {
    const whereClause = buildWhereClause(alias, options.where, table, schema);
    if (whereClause) {
      whereConditions.push(whereClause);
    }
  }

  // Add relation filter conditions
  for (const [, info] of relationJoinInfo) {
    whereConditions.push(...info.conditions);
  }

  if (whereConditions.length > 0) {
    parts.push(`WHERE ${whereConditions.join(" AND ")}`);
  }

  return parts.join(" ");
}

/**
 * Extract relation filters from where clause and build JOINs
 */
function extractRelationFilters(
  table: TableMeta,
  schema: SchemaMeta,
  tableAlias: string,
  where: BaseWhereInput,
  joins: string[],
  joinInfo: Map<string, { tableName: string; conditions: string[] }>,
): void {
  for (const [key, value] of Object.entries(where)) {
    if (value === undefined) continue;
    if (key === "AND" || key === "OR" || key === "NOT") continue;

    // Check if this is a reverse ref (relation filter)
    const reverseRef = table.reverseRefs.find((r) => r.name === key);
    if (reverseRef && isRelationFilter(value)) {
      const sourceTable = schema.tables[reverseRef.sourceTable];
      if (!sourceTable) continue;

      // Groove parser doesn't support aliases on joined tables, use table name directly
      const joinTableName = reverseRef.sourceTable;

      // Only add join once per relation
      // Groove now handles detecting which side has the Ref column
      if (!joinInfo.has(key)) {
        joins.push(
          `JOIN ${joinTableName} ON ${joinTableName}.${reverseRef.sourceColumn} = ${tableAlias}.id`,
        );
        joinInfo.set(key, { tableName: joinTableName, conditions: [] });
      }

      const info = joinInfo.get(key)!;
      const relationFilter = value as RelationFilter;

      // Handle 'some' - at least one related record matches
      if (relationFilter.some) {
        const condition = buildWhereClause(
          joinTableName,
          relationFilter.some,
          sourceTable,
          schema,
        );
        if (condition) {
          info.conditions.push(condition);
        }
      }

      // TODO: Handle 'every' and 'none' (requires subqueries)
    }
  }
}

/**
 * Check if a value is a relation filter (has some/every/none)
 */
function isRelationFilter(value: unknown): value is RelationFilter {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  return "some" in v || "every" in v || "none" in v;
}

/**
 * Build a SQL query for a single row by ID
 */
export function buildQueryById(
  table: TableMeta,
  schema: SchemaMeta,
  id: string,
  options: SubscribeOptions = {},
): string {
  const baseQuery = buildQuery(table, schema, {
    include: options.include,
    where: { id: { equals: id } } as BaseWhereInput,
  });
  return baseQuery;
}

/**
 * Build projection for an included relation
 */
function buildIncludeProjection(
  table: TableMeta,
  schema: SchemaMeta,
  alias: string,
  key: string,
  includeValue: true | IncludeSpec,
): string | null {
  // Check if it's a forward ref
  const forwardRef = table.refs.find((r) => r.column === key);
  if (forwardRef) {
    const targetTable = schema.tables[forwardRef.targetTable];
    if (!targetTable) return null;

    // Use table name directly - Groove's JOIN doesn't support aliases
    // The table row will be returned as composite type, aliased to the column name
    return `${forwardRef.targetTable} as ${key}`;
  }

  // Check if it's a reverse ref
  const reverseRef = table.reverseRefs.find((r) => r.name === key);
  if (reverseRef) {
    const sourceTable = schema.tables[reverseRef.sourceTable];
    if (!sourceTable) return null;

    const innerAlias = reverseRef.sourceTable.toLowerCase()[0] + "_inner";

    // Build base columns for inner query (Groove doesn't support .*)
    const innerBaseColumns = [
      `${innerAlias}.id`,
      ...sourceTable.columns.map((c) => `${innerAlias}.${c.name}`),
    ];
    let innerProjection = innerBaseColumns.join(", ");
    const innerJoins: string[] = [];

    if (typeof includeValue === "object") {
      // Build nested projections and JOINs for forward refs in the nested include
      const nestedProjections: string[] = [];
      // Track which columns are resolved as refs (to skip from base columns)
      const nestedResolvedRefs = new Set<string>();

      for (const [nestedKey, nestedValue] of Object.entries(includeValue)) {
        const nestedForwardRef = sourceTable.refs.find(
          (r) => r.column === nestedKey,
        );
        if (nestedForwardRef) {
          const nestedTargetTable = schema.tables[nestedForwardRef.targetTable];
          if (nestedTargetTable) {
            nestedResolvedRefs.add(nestedKey);
            // Groove's JOIN doesn't support aliases, use table name directly
            nestedProjections.push(
              `${nestedForwardRef.targetTable} as ${nestedKey}`,
            );

            // TODO: Groove parser doesn't support LEFT JOIN yet, using JOIN for now
            innerJoins.push(
              `JOIN ${nestedForwardRef.targetTable} ON ${innerAlias}.${nestedKey} = ${nestedForwardRef.targetTable}.id`,
            );
          }
        }
      }

      if (nestedProjections.length > 0) {
        // Include ALL base columns - Groove's JOIN handling produces all columns from
        // the combined schema regardless of the SELECT clause, so we must include the
        // FK columns even though they're being resolved by nested includes.
        innerProjection = `${innerBaseColumns.join(", ")}, ${nestedProjections.join(", ")}`;
      }
    }

    const joinClause = innerJoins.length > 0 ? ` ${innerJoins.join(" ")}` : "";
    return `ARRAY(SELECT ${innerProjection} FROM ${reverseRef.sourceTable} ${innerAlias}${joinClause} WHERE ${innerAlias}.${reverseRef.sourceColumn} = ${alias}.id) as ${key}`;
  }

  return null;
}

/**
 * Build JOIN clauses for included forward refs
 */
function buildJoins(
  table: TableMeta,
  schema: SchemaMeta,
  alias: string,
  include: IncludeSpec,
): string[] {
  const joins: string[] = [];

  for (const key of Object.keys(include)) {
    const forwardRef = table.refs.find((r) => r.column === key);
    if (forwardRef) {
      // Groove's JOIN doesn't support aliases or LEFT JOIN, use table name directly
      const targetTable = forwardRef.targetTable;
      // TODO: Groove parser doesn't support LEFT JOIN yet, using JOIN for now
      joins.push(`JOIN ${targetTable} ON ${alias}.${key} = ${targetTable}.id`);
    }
  }

  return joins;
}

/**
 * Build a WHERE clause from a Prisma-style where input
 */
function buildWhereClause(
  alias: string,
  where: BaseWhereInput,
  table?: TableMeta,
  schema?: SchemaMeta,
): string | null {
  const conditions: string[] = [];

  for (const [key, value] of Object.entries(where)) {
    if (value === undefined) continue;

    // Skip relation filters (handled separately by extractRelationFilters)
    if (table && isRelationFilter(value)) {
      const isReverseRef = table.reverseRefs.some((r) => r.name === key);
      if (isReverseRef) continue;
    }

    // Handle combinators
    if (key === "AND") {
      const andConditions = Array.isArray(value) ? value : [value];
      const parts = andConditions
        .map((w) => buildWhereClause(alias, w as BaseWhereInput, table, schema))
        .filter((c): c is string => c !== null);
      if (parts.length > 0) {
        conditions.push(
          parts.length === 1 ? parts[0] : `(${parts.join(" AND ")})`,
        );
      }
    } else if (key === "OR") {
      const orConditions = value as BaseWhereInput[];
      const parts = orConditions
        .map((w) => buildWhereClause(alias, w, table, schema))
        .filter((c): c is string => c !== null);
      if (parts.length > 0) {
        conditions.push(`(${parts.join(" OR ")})`);
      }
    } else if (key === "NOT") {
      const notConditions = Array.isArray(value) ? value : [value];
      const parts = notConditions
        .map((w) => buildWhereClause(alias, w as BaseWhereInput, table, schema))
        .filter((c): c is string => c !== null);
      if (parts.length > 0) {
        conditions.push(`NOT (${parts.join(" AND ")})`);
      }
    } else {
      // It's a column filter
      const columnCondition = buildColumnCondition(alias, key, value);
      if (columnCondition) {
        conditions.push(columnCondition);
      }
    }
  }

  if (conditions.length === 0) return null;
  return conditions.join(" AND ");
}

/**
 * Build a condition for a single column
 */
function buildColumnCondition(
  alias: string,
  column: string,
  filter: unknown,
): string | null {
  const col = `${alias}.${column}`;

  // Direct value - treat as equality
  if (filter === null) {
    return `${col} IS NULL`;
  }
  if (typeof filter !== "object") {
    return `${col} = ${formatValue(filter)}`;
  }

  // Filter object
  const conditions: string[] = [];
  const f = filter as Record<string, unknown>;

  if ("equals" in f) {
    if (f.equals === null) {
      conditions.push(`${col} IS NULL`);
    } else {
      conditions.push(`${col} = ${formatValue(f.equals)}`);
    }
  }

  if ("not" in f) {
    if (f.not === null) {
      conditions.push(`${col} IS NOT NULL`);
    } else if (typeof f.not === "object") {
      // Nested filter
      const nested = buildColumnCondition(alias, column, f.not);
      if (nested) conditions.push(`NOT (${nested})`);
    } else {
      conditions.push(`${col} != ${formatValue(f.not)}`);
    }
  }

  if ("gt" in f) {
    conditions.push(`${col} > ${formatValue(f.gt)}`);
  }
  if ("gte" in f) {
    conditions.push(`${col} >= ${formatValue(f.gte)}`);
  }
  if ("lt" in f) {
    conditions.push(`${col} < ${formatValue(f.lt)}`);
  }
  if ("lte" in f) {
    conditions.push(`${col} <= ${formatValue(f.lte)}`);
  }

  if ("contains" in f) {
    conditions.push(
      `${col} LIKE '%${String(f.contains).replace(/'/g, "''")}%'`,
    );
  }
  if ("startsWith" in f) {
    conditions.push(
      `${col} LIKE '${String(f.startsWith).replace(/'/g, "''")}%'`,
    );
  }
  if ("endsWith" in f) {
    conditions.push(`${col} LIKE '%${String(f.endsWith).replace(/'/g, "''")}'`);
  }

  if ("in" in f && Array.isArray(f.in)) {
    const values = f.in.map(formatValue).join(", ");
    conditions.push(`${col} IN (${values})`);
  }
  if ("notIn" in f && Array.isArray(f.notIn)) {
    const values = f.notIn.map(formatValue).join(", ");
    conditions.push(`${col} NOT IN (${values})`);
  }

  if (conditions.length === 0) return null;
  return conditions.join(" AND ");
}

/**
 * Format a value for SQL
 */
function formatValue(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replace(/'/g, "''")}'`;
  if (typeof value === "number") return String(value);
  if (typeof value === "bigint") return String(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (value instanceof Date) return String(value.getTime());
  return String(value);
}

// === Subscribe Types (for client implementation) ===

/**
 * Unsubscribe function returned by subscribe calls
 */
export type Unsubscribe = () => void;

/**
 * Table client interface pattern (for documentation).
 *
 * Generated code implements this pattern for each table:
 *
 * ```typescript
 * interface NoteClient {
 *   subscribe<D extends NoteDepth>(
 *     id: string,
 *     options: { include?: D },
 *     callback: (note: NoteLoaded<D> | null) => void
 *   ): Unsubscribe;
 *
 *   subscribeAll<D extends NoteDepth>(
 *     options: { where?: WhereClause; include?: D },
 *     callback: (notes: NoteLoaded<D>[]) => void
 *   ): Unsubscribe;
 * }
 * ```
 */
