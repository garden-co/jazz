/**
 * React hooks for subscribing to Jazz database tables
 */

import { useState, useEffect, useRef } from "react";
import type { Unsubscribe } from "@jazz/client";

/**
 * Interface for objects that can subscribe to a single row by ID.
 * Both table clients and query builders implement this.
 */
export interface SubscribableOne<T> {
  subscribe(id: string, callback: (row: T | null) => void): Unsubscribe;
}

/**
 * Interface for objects that can subscribe to all matching rows.
 * Both table clients and query builders implement this.
 */
export interface SubscribableAll<T> {
  subscribeAll(callback: (rows: T[]) => void): Unsubscribe;
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
 * @param subscribable - A table client or query builder (e.g., db.users or db.users.with({ notes: true }))
 * @param id - The row's ObjectId
 * @returns Object with data and loading state
 *
 * @example
 * ```tsx
 * function UserProfile({ userId }: { userId: string }) {
 *   const db = useJazz();
 *
 *   // Without includes - returns plain User
 *   const { data: user, loading } = useOne(db.users, userId);
 *
 *   // With includes - returns UserLoaded<{ notes: true }>
 *   const { data: userWithNotes } = useOne(
 *     db.users.with({ notes: true }),
 *     userId
 *   );
 *
 *   if (loading) return <div>Loading...</div>;
 *   if (!user) return <div>User not found</div>;
 *
 *   return <div>{user.name}</div>;
 * }
 * ```
 */
export function useOne<T>(
  subscribable: SubscribableOne<T>,
  id: string | null | undefined
): UseOneResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);

  // Track if this is the first callback
  const isFirstCallback = useRef(true);

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

    const unsubscribe = subscribable.subscribe(id, (row) => {
      setData(row);
      if (isFirstCallback.current) {
        setLoading(false);
        isFirstCallback.current = false;
      }
    });

    return unsubscribe;
  }, [subscribable, id]);

  return { data, loading };
}

/**
 * Hook to subscribe to all rows matching a query.
 *
 * @param subscribable - A table client or query builder (e.g., db.notes or db.notes.where({ author: id }).with({ folder: true }))
 * @returns Object with data array and loading state
 *
 * @example
 * ```tsx
 * function NotesList({ authorId }: { authorId: string }) {
 *   const db = useJazz();
 *
 *   // Without filter/includes - returns plain Note[]
 *   const { data: allNotes, loading } = useAll(db.notes);
 *
 *   // With filter - returns Note[]
 *   const { data: authorNotes } = useAll(
 *     db.notes.where({ author: authorId })
 *   );
 *
 *   // With filter and includes - returns NoteLoaded<{ folder: true }>[]
 *   const { data: notesWithFolders } = useAll(
 *     db.notes.where({ author: authorId }).with({ folder: true })
 *   );
 *
 *   if (loading) return <div>Loading...</div>;
 *
 *   return (
 *     <ul>
 *       {notesWithFolders.map(note => (
 *         <li key={note.id}>{note.title} - {note.folder.name}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 * ```
 */
export function useAll<T>(subscribable: SubscribableAll<T>): UseAllResult<T> {
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);

  // Track if this is the first callback
  const isFirstCallback = useRef(true);

  useEffect(() => {
    // Reset state on subscribable change
    setLoading(true);
    setData([]);
    isFirstCallback.current = true;

    const unsubscribe = subscribable.subscribeAll((rows) => {
      setData(rows);
      if (isFirstCallback.current) {
        setLoading(false);
        isFirstCallback.current = false;
      }
    });

    return unsubscribe;
  }, [subscribable]);

  return { data, loading };
}
