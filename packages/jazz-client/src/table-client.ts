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
import { decodeDeltaWithIncludes } from "./decoder.js";

/**
 * Base class for type-safe table descriptors.
 *
 * Generated code extends this class to provide type-safe CRUD and subscription methods.
 * The base class handles the common logic for SQL generation, binary decoding, and state management.
 *
 * Note: This class does NOT hold a database instance. The db is passed at method call time,
 * allowing the same descriptor to be used with different database instances.
 */
export abstract class TableClient<T extends { id: string }> {
  constructor(
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
   *
   * @param db - The database instance to subscribe through
   * @param id - The row ID to subscribe to
   * @param options - Subscribe options (includes)
   * @param callback - Callback invoked when row changes
   */
  protected _subscribe(
    db: WasmDatabaseLike,
    id: string,
    options: { include?: IncludeSpec },
    callback: (row: T | null) => void
  ): Unsubscribe {
    // Debug: log the query options
    console.log(`[${this.tableName}] useOne options:`, {
      id,
      include: options.include,
    });

    const sql = buildQueryById(this.tableMeta, this.schemaMeta, id, {
      include: options.include,
    });

    // Debug: log the SQL query
    console.log(`[${this.tableName}] useOne SQL:`, sql);

    let currentRow: T | null = null;

    const handle = db.subscribe_delta(sql, (deltas: Uint8Array[]) => {
      // Debug: log when callback is invoked
      console.log(`[${this.tableName}] useOne callback invoked with ${deltas.length} deltas`);
      for (const deltaBuffer of deltas) {
        // Use dynamic decoder when includes are specified, otherwise use generated decoder
        const delta = options.include
          ? decodeDeltaWithIncludes<T>(deltaBuffer.buffer, this.tableMeta, this.schemaMeta, options.include)
          : this.decoder.delta(deltaBuffer.buffer) as Delta<T>;

        if (delta.type === "added" || delta.type === "updated") {
          currentRow = delta.row;
        } else if (delta.type === "removed") {
          currentRow = null;
        }
      }
      callback(currentRow);
    });

    // Debug: log the query graph diagram
    console.log(`[${this.tableName}] useOne Query Graph:\n${handle.diagram()}`);

    return () => {
      handle.unsubscribe();
      handle.free();
    };
  }

  /**
   * Subscribe to all rows matching a filter.
   * Called by generated typed method.
   *
   * @param db - The database instance to subscribe through
   * @param options - Subscribe options (where filter, includes)
   * @param callback - Callback invoked when rows change
   */
  protected _subscribeAll(
    db: WasmDatabaseLike,
    options: { where?: BaseWhereInput; include?: IncludeSpec },
    callback: (rows: T[]) => void
  ): Unsubscribe {
    // Debug: log the JS query options
    console.log(`[${this.tableName}] useAll options:`, {
      where: options.where,
      include: options.include,
    });

    const sql = buildQuery(this.tableMeta, this.schemaMeta, {
      where: options.where,
      include: options.include,
    });

    // Debug: log the SQL query
    console.log(`[${this.tableName}] useAll SQL:`, sql);

    const rowsById = new Map<string, T>();

    const handle = db.subscribe_delta(sql, (deltas: Uint8Array[]) => {
      // Debug: log when callback is invoked
      console.log(`[${this.tableName}] useAll callback invoked with ${deltas.length} deltas`);

      for (const deltaBuffer of deltas) {
        try {
          // Use dynamic decoder when includes are specified, otherwise use generated decoder
          const delta = options.include
            ? decodeDeltaWithIncludes<T>(deltaBuffer.buffer, this.tableMeta, this.schemaMeta, options.include)
            : this.decoder.delta(deltaBuffer.buffer) as Delta<T>;

          // Debug: log decoded delta
          console.log(`[${this.tableName}] useAll delta:`, delta);

          if (delta.type === "added" || delta.type === "updated") {
            rowsById.set(delta.row.id, delta.row);
          } else if (delta.type === "removed") {
            rowsById.delete(delta.id);
          }
        } catch (error) {
          console.error(`[${this.tableName}] Error decoding delta:`, error);
          console.error(`[${this.tableName}] Delta buffer length:`, deltaBuffer.length);
          console.error(`[${this.tableName}] Delta buffer (hex):`, Array.from(deltaBuffer).map(b => b.toString(16).padStart(2, '0')).join(' '));
        }
      }
      callback(Array.from(rowsById.values()));
    });

    // Debug: log the query graph diagram (after initial subscription sets up the graph)
    console.log(`[${this.tableName}] Query Graph:\n${handle.diagram()}`);

    return () => {
      handle.unsubscribe();
      handle.free();
    };
  }

  /**
   * Create a new row.
   * Called by generated typed method.
   *
   * @param db - The database instance to execute on
   * @param values - Column name to value mapping
   * @returns The generated ObjectId of the new row
   */
  protected _create(db: WasmDatabaseLike, values: Record<string, unknown>): string {
    const columns = Object.keys(values);
    const sqlValues = columns.map((col) => formatSqlValue(values[col]));

    const sql = `INSERT INTO ${this.tableName} (${columns.join(", ")}) VALUES (${sqlValues.join(", ")})`;
    const result = db.execute(sql);

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
   * @param db - The database instance to execute on
   * @param id - The row's ObjectId
   * @param values - Column name to value mapping (partial)
   */
  protected _update(db: WasmDatabaseLike, id: string, values: Record<string, unknown>): void {
    for (const [column, value] of Object.entries(values)) {
      if (value === undefined) continue;

      // Use typed update methods based on value type
      if (typeof value === "string") {
        db.update_row(this.tableName, id, column, value);
      } else if (typeof value === "bigint") {
        db.update_row_i64(this.tableName, id, column, value);
      } else if (typeof value === "number") {
        // For integers, use i64 update. For floats, use SQL.
        if (Number.isInteger(value)) {
          db.update_row_i64(this.tableName, id, column, BigInt(value));
        } else {
          // F64 values need SQL update
          db.execute(
            `UPDATE ${this.tableName} SET ${column} = ${value} WHERE id = '${id}'`
          );
        }
      } else if (typeof value === "boolean") {
        // Boolean values need SQL update
        db.execute(
          `UPDATE ${this.tableName} SET ${column} = ${value ? 'TRUE' : 'FALSE'} WHERE id = '${id}'`
        );
      } else if (value === null) {
        // Handle null values via SQL UPDATE
        db.execute(
          `UPDATE ${this.tableName} SET ${column} = NULL WHERE id = '${id}'`
        );
      } else {
        // Fallback to SQL for other types
        db.execute(
          `UPDATE ${this.tableName} SET ${column} = ${formatSqlValue(value)} WHERE id = '${id}'`
        );
      }
    }
  }

  /**
   * Delete a row.
   * Called by generated typed method.
   *
   * @param db - The database instance to execute on
   * @param id - The row's ObjectId
   */
  protected _delete(db: WasmDatabaseLike, id: string): void {
    db.execute(`DELETE FROM ${this.tableName} WHERE id = '${id}'`);
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
