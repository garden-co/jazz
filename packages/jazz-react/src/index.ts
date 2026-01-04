/**
 * @jazz/react - React hooks for Jazz databases
 *
 * This package provides React integration for Jazz databases:
 * - JazzProvider context for database access
 * - useJazz hook to get database from context
 * - useOne hook for single-row subscriptions
 * - useAll hook for multi-row subscriptions
 *
 * @example
 * ```tsx
 * import { JazzProvider, useJazz, useOne, useAll } from "@jazz/react";
 * import { createDatabase } from "./generated/client.js";
 *
 * // Create database (typically done at app initialization)
 * const db = createDatabase(wasmDb);
 *
 * function App() {
 *   return (
 *     <JazzProvider database={db}>
 *       <UserList />
 *     </JazzProvider>
 *   );
 * }
 *
 * function UserList() {
 *   const db = useJazz();
 *   const { data: users, loading } = useAll(db.users, {});
 *
 *   if (loading) return <div>Loading...</div>;
 *
 *   return (
 *     <ul>
 *       {users.map(user => (
 *         <li key={user.id}>{user.name}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 * ```
 */

export { JazzProvider, useJazz, type JazzProviderProps } from "./context.js";

export {
  useOne,
  useAll,
  type UseOneResult,
  type UseAllResult,
  type SubscribableOne,
  type SubscribableAll,
} from "./hooks.js";
