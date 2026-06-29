import type { QueryBuilder } from "./db.js";

/**
 * Wrap a query so it executes with `limit 1`, returning at most one row. Used by
 * `Db.one` and by the framework `useOne`-style bindings so both share the exact
 * same single-row semantics.
 *
 * This lives in its own module (rather than alongside `Db`) so framework
 * bindings can reuse it without pulling in the wasm runtime.
 */
export function limitQueryToOne<T>(query: QueryBuilder<T>): QueryBuilder<T> {
  return {
    get _table() {
      return query._table;
    },
    get _schema() {
      return query._schema;
    },
    get _columnTransforms() {
      return query._columnTransforms;
    },
    get _rowType() {
      return query._rowType;
    },
    _build() {
      const builtQuery = JSON.parse(query._build()) as Record<string, unknown>;
      builtQuery.limit = 1;
      return JSON.stringify(builtQuery);
    },
  };
}
