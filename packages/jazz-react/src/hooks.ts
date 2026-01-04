/**
 * React hooks for subscribing to Jazz database tables
 */

import { useState, useEffect, useRef } from "react";
import type { Unsubscribe, BaseWhereInput, IncludeSpec } from "@jazz/client";

/**
 * Interface that table clients must implement for useOne
 */
export interface SubscribableOne<T, I> {
  subscribe(
    id: string,
    options: { include?: I },
    callback: (row: T | null) => void
  ): Unsubscribe;
}

/**
 * Interface that table clients must implement for useAll
 */
export interface SubscribableAll<T, W, I> {
  subscribeAll(
    options: { where?: W; include?: I },
    callback: (rows: T[]) => void
  ): Unsubscribe;
}

/**
 * Result of useOne hook
 */
export interface UseOneResult<T> {
  /** The row data, or null if not found */
  data: T | null;
  /** True while waiting for initial data */
  loading: boolean;
}

/**
 * Result of useAll hook
 */
export interface UseAllResult<T> {
  /** Array of matching rows */
  data: T[];
  /** True while waiting for initial data */
  loading: boolean;
}

/**
 * Hook to subscribe to a single row by ID.
 *
 * @param tableClient - The table client from the database (e.g., db.users)
 * @param id - The row's ObjectId
 * @param options - Optional include spec for eager loading
 * @returns Object with data and loading state
 *
 * @example
 * ```tsx
 * function UserProfile({ userId }: { userId: string }) {
 *   const db = useJazz();
 *   const { data: user, loading } = useOne(db.users, userId, {
 *     include: { notes: true }
 *   });
 *
 *   if (loading) return <div>Loading...</div>;
 *   if (!user) return <div>User not found</div>;
 *
 *   return <div>{user.name}</div>;
 * }
 * ```
 */
export function useOne<T, I extends IncludeSpec = Record<string, never>>(
  tableClient: SubscribableOne<T, I>,
  id: string | null | undefined,
  options: { include?: I } = {}
): UseOneResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);

  // Track if this is the first callback
  const isFirstCallback = useRef(true);

  // Stable reference for options to avoid re-subscribing on every render
  const optionsRef = useRef(options);
  optionsRef.current = options;

  useEffect(() => {
    // Reset state on id change
    setLoading(true);
    setData(null);
    isFirstCallback.current = true;

    // Don't subscribe if id is null/undefined
    if (!id) {
      setLoading(false);
      return;
    }

    const unsubscribe = tableClient.subscribe(
      id,
      { include: optionsRef.current.include },
      (row) => {
        setData(row);
        if (isFirstCallback.current) {
          setLoading(false);
          isFirstCallback.current = false;
        }
      }
    );

    return unsubscribe;
  }, [tableClient, id]);

  return { data, loading };
}

/**
 * Hook to subscribe to all rows matching a filter.
 *
 * @param tableClient - The table client from the database (e.g., db.notes)
 * @param options - Optional where filter and include spec
 * @returns Object with data array and loading state
 *
 * @example
 * ```tsx
 * function NotesList({ authorId }: { authorId: string }) {
 *   const db = useJazz();
 *   const { data: notes, loading } = useAll(db.notes, {
 *     where: { author: authorId },
 *     include: { folder: true }
 *   });
 *
 *   if (loading) return <div>Loading...</div>;
 *
 *   return (
 *     <ul>
 *       {notes.map(note => (
 *         <li key={note.id}>{note.title}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 * ```
 */
export function useAll<
  T,
  W extends BaseWhereInput = BaseWhereInput,
  I extends IncludeSpec = Record<string, never>,
>(
  tableClient: SubscribableAll<T, W, I>,
  options: { where?: W; include?: I } = {}
): UseAllResult<T> {
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);

  // Track if this is the first callback
  const isFirstCallback = useRef(true);

  // Stable reference for options
  const optionsRef = useRef(options);
  optionsRef.current = options;

  // Create a stable key for the options to detect changes
  const optionsKey = JSON.stringify({
    where: options.where,
    include: options.include,
  });

  useEffect(() => {
    // Reset state on options change
    setLoading(true);
    setData([]);
    isFirstCallback.current = true;

    const unsubscribe = tableClient.subscribeAll(
      {
        where: optionsRef.current.where,
        include: optionsRef.current.include,
      },
      (rows) => {
        setData(rows);
        if (isFirstCallback.current) {
          setLoading(false);
          isFirstCallback.current = false;
        }
      }
    );

    return unsubscribe;
  }, [tableClient, optionsKey]);

  return { data, loading };
}
