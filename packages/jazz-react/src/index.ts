/**
 * @jazz/react - React hooks for Jazz databases
 *
 * This package provides React integration for Jazz databases:
 * - JazzProvider context for database access
 * - useJazz hook to get raw database from context
 * - useOne hook for single-row subscriptions with update helper
 * - useAll hook for multi-row subscriptions with update/create helpers
 * - useCreate hook for creating rows without subscribing
 *
 * @example
 * ```tsx
 * import { JazzProvider, useAll, useOne, useCreate } from "@jazz/react";
 * import { app } from "./generated/client.js";
 * import * as wasm from "groove-wasm";
 *
 * // Initialize WASM database
 * const wasmDb = new wasm.WasmDatabase();
 * wasmDb.init_schema(schema);
 *
 * function App() {
 *   return (
 *     <JazzProvider database={wasmDb}>
 *       <UserList />
 *     </JazzProvider>
 *   );
 * }
 *
 * function UserList() {
 *   // app provides typed schema, hooks get db from context
 *   const [users, loading, updateUser, createUser] = useAll(app.users);
 *
 *   if (loading) return <div>Loading...</div>;
 *
 *   return (
 *     <div>
 *       <button onClick={() => createUser({ name: "New User" })}>
 *         Add User
 *       </button>
 *       <ul>
 *         {users.map(user => (
 *           <li key={user.id}>
 *             {user.name}
 *             <button onClick={() => updateUser(user.id, { name: "Updated" })}>
 *               Edit
 *             </button>
 *           </li>
 *         ))}
 *       </ul>
 *     </div>
 *   );
 * }
 * ```
 */

export { JazzProvider, useJazz, type JazzProviderProps } from "./context.js";

export {
  useOne,
  useAll,
  useMutate,
  useCreate, // deprecated alias for useMutate
  // Deprecated types - prefer SubscribableAllWithDb/SubscribableOneWithDb from @jazz/client
  type SubscribableOne,
  type SubscribableAll,
} from "./hooks.js";

// Re-export commonly needed types from @jazz/client for convenience
export type {
  WasmDatabaseLike,
  SubscribableAllWithDb,
  SubscribableOneWithDb,
  MutableWithDb,
  MutateAll,
  MutateOne,
} from "@jazz/client";
