/**
 * React context for Jazz database
 */

import type { WasmDatabaseLike } from "@jazz/client";
import { type ReactNode, createContext, useContext } from "react";

const JazzContext = createContext<WasmDatabaseLike | null>(null);

/**
 * Props for JazzProvider
 */
export interface JazzProviderProps {
  /** The raw WASM database instance */
  database: WasmDatabaseLike;
  /** Child components */
  children: ReactNode;
}

/**
 * Provider component that makes the Jazz database available to child components.
 *
 * The provider stores the raw WASM database instance. Type-safe table access
 * is provided through the `app` schema descriptor (imported from generated code).
 *
 * @example
 * ```tsx
 * import { app } from './generated/client';
 *
 * const wasmDb = new WasmDatabase();
 * wasmDb.init_schema(schema);
 *
 * function Root() {
 *   return (
 *     <JazzProvider database={wasmDb}>
 *       <App />
 *     </JazzProvider>
 *   );
 * }
 *
 * function MyComponent() {
 *   // useAll gets db from context, app provides typed schema
 *   const [issues, loading, updateIssue, createIssue] = useAll(app.issues);
 * }
 * ```
 */
export function JazzProvider({ database, children }: JazzProviderProps) {
  return (
    <JazzContext.Provider value={database}>{children}</JazzContext.Provider>
  );
}

/**
 * Hook to get the raw WASM database from context.
 *
 * This returns the untyped WASM database instance. For type-safe table access,
 * use the `app` schema descriptor with hooks like `useAll` and `useOne`.
 *
 * @example
 * ```tsx
 * function MyComponent() {
 *   // For direct db access (rare - prefer useAll/useOne with app)
 *   const db = useJazz();
 *
 *   // Type-safe access via app + hooks
 *   const [users] = useAll(app.users);
 * }
 * ```
 */
export function useJazz(): WasmDatabaseLike {
  const db = useContext(JazzContext);
  if (!db) {
    throw new Error("useJazz must be used within a JazzProvider");
  }
  return db;
}
