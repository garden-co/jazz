/**
 * TableClient base class for generated table clients
 */

import {
  type TableMeta,
  type SchemaMeta,
  type BaseWhereInput,
  type IncludeSpec,
  type Unsubscribe,
  type TableDecoder,
  type Delta,
  type WasmDatabaseLike,
  buildQuery,
  buildQueryById,
} from "./types.js";

/**
 * Base class for type-safe table clients.
 *
 * Generated code extends this class to provide type-safe CRUD and subscription methods.
 * The base class handles the common logic for SQL generation, binary decoding, and state management.
 */
export abstract class TableClient<T extends { id: string }> {
  constructor(
    protected readonly db: WasmDatabaseLike,
    protected readonly tableMeta: TableMeta,
    protected readonly schemaMeta: SchemaMeta,
    protected readonly decoder: TableDecoder<T>
  ) {}

  /**
   * Get the table name
   */
  get tableName(): string {
    return this.tableMeta.name;
  }

  /**
   * Subscribe to a single row by ID.
   * Called by generated typed method.
   */
  protected _subscribe(
    id: string,
    options: { include?: IncludeSpec },
    callback: (row: T | null) => void
  ): Unsubscribe {
    const sql = buildQueryById(this.tableMeta, this.schemaMeta, id, {
      include: options.include,
    });

    let currentRow: T | null = null;

    const handle = this.db.subscribe_delta(sql, (deltas: Uint8Array[]) => {
      for (const deltaBuffer of deltas) {
        const delta = this.decoder.delta(deltaBuffer.buffer) as Delta<T>;

        if (delta.type === "added" || delta.type === "updated") {
          currentRow = delta.row;
        } else if (delta.type === "removed") {
          currentRow = null;
        }
      }
      callback(currentRow);
    });

    return () => {
      handle.unsubscribe();
      handle.free();
    };
  }

  /**
   * Subscribe to all rows matching a filter.
   * Called by generated typed method.
   */
  protected _subscribeAll(
    options: { where?: BaseWhereInput; include?: IncludeSpec },
    callback: (rows: T[]) => void
  ): Unsubscribe {
    const sql = buildQuery(this.tableMeta, this.schemaMeta, {
      where: options.where,
      include: options.include,
    });

    // Debug: log the SQL query
    console.log(`[${this.tableName}] SQL:`, sql);

    const rowsById = new Map<string, T>();

    const handle = this.db.subscribe_delta(sql, (deltas: Uint8Array[]) => {
      for (const deltaBuffer of deltas) {
        const delta = this.decoder.delta(deltaBuffer.buffer) as Delta<T>;

        if (delta.type === "added" || delta.type === "updated") {
          rowsById.set(delta.row.id, delta.row);
        } else if (delta.type === "removed") {
          rowsById.delete(delta.id);
        }
      }
      callback(Array.from(rowsById.values()));
    });

    return () => {
      handle.unsubscribe();
      handle.free();
    };
  }

  /**
   * Create a new row.
   * Called by generated typed method.
   *
   * @param values - Column name to value mapping
   * @returns The generated ObjectId of the new row
   */
  protected _create(values: Record<string, unknown>): string {
    const columns = Object.keys(values);
    const sqlValues = columns.map((col) => formatSqlValue(values[col]));

    const sql = `INSERT INTO ${this.tableName} (${columns.join(", ")}) VALUES (${sqlValues.join(", ")})`;
    const result = this.db.execute(sql);

    // Result should be "inserted:<id>"
    const resultStr = String(result);
    if (resultStr.startsWith("inserted:")) {
      return resultStr.slice(9);
    }

    throw new Error(`Unexpected insert result: ${resultStr}`);
  }

  /**
   * Update an existing row.
   * Called by generated typed method.
   *
   * @param id - The row's ObjectId
   * @param values - Column name to value mapping (partial)
   */
  protected _update(id: string, values: Record<string, unknown>): void {
    for (const [column, value] of Object.entries(values)) {
      if (value === undefined) continue;

      // Use update_row for string and bigint values
      if (typeof value === "string" || typeof value === "bigint") {
        this.db.update_row(this.tableName, id, column, value);
      } else if (typeof value === "number") {
        // Convert numbers to bigint for i64 columns or string for others
        this.db.update_row(this.tableName, id, column, String(value));
      } else if (value === null) {
        // Handle null values via SQL UPDATE
        this.db.execute(
          `UPDATE ${this.tableName} SET ${column} = NULL WHERE id = '${id}'`
        );
      } else {
        // Fallback to SQL for other types
        this.db.execute(
          `UPDATE ${this.tableName} SET ${column} = ${formatSqlValue(value)} WHERE id = '${id}'`
        );
      }
    }
  }

  /**
   * Delete a row.
   * Called by generated typed method.
   *
   * @param id - The row's ObjectId
   */
  protected _delete(id: string): void {
    this.db.execute(`DELETE FROM ${this.tableName} WHERE id = '${id}'`);
  }
}

/**
 * Format a value for SQL insertion
 */
function formatSqlValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "string") return `'${value.replace(/'/g, "''")}'`;
  if (typeof value === "number") return String(value);
  if (typeof value === "bigint") return String(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (value instanceof Date) return String(value.getTime());

  // Handle objects that might be row references (extract id)
  if (typeof value === "object" && value !== null && "id" in value) {
    return `'${String((value as { id: string }).id)}'`;
  }

  return String(value);
}
