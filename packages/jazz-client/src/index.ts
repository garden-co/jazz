/**
 * @jazz/client - Core runtime client for Jazz databases
 *
 * This package provides the base infrastructure for generated Jazz database clients:
 * - TableClient base class for type-safe CRUD and subscriptions
 * - Types for decoders, deltas, and WASM integration
 *
 * @example
 * ```typescript
 * // Generated code uses this package:
 * import { TableClient } from "@jazz/client";
 *
 * class UsersClient extends TableClient<User> {
 *   create(data: UserInsert): string {
 *     return this._create(data);
 *   }
 *   // ...
 * }
 * ```
 */

export { TableClient } from "./table-client.js";

export {
  // Types
  type TableMeta,
  type SchemaMeta,
  type ColumnMeta,
  type ColumnType,
  type RefMeta,
  type ReverseRefMeta,
  type BaseWhereInput,
  type IncludeSpec,
  type SubscribeAllOptions,
  type SubscribeOptions,
  type Unsubscribe,
  type Delta,
  type DeltaType,
  type RowDecoder,
  type DeltaDecoder,
  type TableDecoder,
  type WasmDatabaseLike,
  type WasmQueryHandleLike,
  // Constants
  DELTA_ADDED,
  DELTA_UPDATED,
  DELTA_REMOVED,
  // Query builders (re-exported for convenience)
  buildQuery,
  buildQueryById,
} from "./types.js";
