/**
 * React context for Jazz database
 */

import { createContext, useContext, type ReactNode } from "react";

/**
 * Generic database type - the actual type is defined by generated code
 */
export interface JazzDatabaseLike {
  raw: unknown;
  [tableName: string]: unknown;
}

const JazzContext = createContext<JazzDatabaseLike | null>(null);

/**
 * Props for JazzProvider
 */
export interface JazzProviderProps {
  /** The Jazz database instance */
  database: JazzDatabaseLike;
  /** Child components */
  children: ReactNode;
}

/**
 * Provider component that makes the Jazz database available to child components.
 *
 * @example
 * ```tsx
 * const db = createDatabase(wasmDb);
 *
 * function Root() {
 *   return (
 *     <JazzProvider database={db}>
 *       <App />
 *     </JazzProvider>
 *   );
 * }
 * ```
 */
export function JazzProvider({ database, children }: JazzProviderProps) {
  return (
    <JazzContext.Provider value={database}>{children}</JazzContext.Provider>
  );
}

/**
 * Hook to get the Jazz database from context.
 *
 * @example
 * ```tsx
 * function MyComponent() {
 *   const db = useJazz();
 *   // Use db.users, db.notes, etc.
 * }
 * ```
 */
export function useJazz<T extends JazzDatabaseLike = JazzDatabaseLike>(): T {
  const db = useContext(JazzContext);
  if (!db) {
    throw new Error("useJazz must be used within a JazzProvider");
  }
  return db as T;
}
