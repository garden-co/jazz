/**
 * React hooks for subscribing to Jazz database tables
 */

import type {
  MutableWithDb,
  MutateAll,
  MutateOne,
  SubscribableAllWithDb,
  SubscribableOneWithDb,
} from "@jazz/client";
import { useEffect, useMemo, useRef, useState } from "react";
import { useJazz } from "./context.js";

/**
 * Get a stable key for a subscribable object.
 * QueryBuilders have _queryKey, table clients use object identity.
 */
function getSubscribableKey(subscribable: { _queryKey?: string }):
  | string
  | object {
  return subscribable._queryKey ?? subscribable;
}

/**
 * Hook to subscribe to a single row by ID.
 *
 * Returns [data, loading, mutate] where:
 * - data: The row or null if not found/loading
 * - loading: Boolean flag (true while fetching initial data)
 * - mutate: Object with update() and delete() methods (id is captured)
 *
 * @param subscribable - A table descriptor or query builder (e.g., app.users or app.users.with({ notes: true }))
 * @param id - The row's ObjectId
 * @returns Tuple of [data, loading, mutate]
 *
 * @example
 * ```tsx
 * import { app } from './generated/client';
 *
 * function UserProfile({ userId }: { userId: string }) {
 *   const [user, loading, mutate] = useOne(app.users, userId);
 *
 *   if (loading) return <div>Loading...</div>;
 *   if (!user) return <div>User not found</div>;
 *
 *   return (
 *     <div>
 *       <h1>{user.name}</h1>
 *       <button onClick={() => mutate.update({ name: "New Name" })}>
 *         Rename
 *       </button>
 *       <button onClick={() => mutate.delete()}>
 *         Delete
 *       </button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useOne<T, U>(
  subscribable: SubscribableOneWithDb<T, U>,
  id: string | null | undefined,
): [T | null, boolean, MutateOne<U>] {
  const db = useJazz();
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);

  // Track if this is the first callback
  const isFirstCallback = useRef(true);

  // Track the previous subscribable to avoid unnecessary re-subscriptions
  const prevSubscribableRef = useRef<SubscribableOneWithDb<T, U> | null>(null);
  const prevKeyRef = useRef<string | object | null>(null);

  // Get stable key for structural comparison
  const currentKey = getSubscribableKey(subscribable);

  // Use the previous subscribable if the key matches (structural equality)
  const stableSubscribable =
    prevKeyRef.current === currentKey && prevSubscribableRef.current
      ? prevSubscribableRef.current
      : subscribable;

  // Update refs for next render
  if (prevKeyRef.current !== currentKey) {
    prevKeyRef.current = currentKey;
    prevSubscribableRef.current = subscribable;
  }

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

    const unsubscribe = stableSubscribable.subscribe(db, id, (row) => {
      setData(row);
      if (isFirstCallback.current) {
        setLoading(false);
        isFirstCallback.current = false;
      }
    });

    return unsubscribe;
  }, [db, stableSubscribable, id]);

  // Create mutate object with captured id
  const mutate = useMemo<MutateOne<U>>(
    () => ({
      update: (values: U) => {
        if (id) {
          stableSubscribable.update(db, id, values);
        }
      },
      delete: () => {
        if (id) {
          stableSubscribable.delete(db, id);
        }
      },
    }),
    [db, stableSubscribable, id],
  );

  return [data, loading, mutate];
}

/**
 * Hook to subscribe to all rows matching a query.
 *
 * Returns [data, loading, mutate] where:
 * - data: Array of rows (empty array while loading)
 * - loading: Boolean flag (true while fetching initial data)
 * - mutate: Object with create(), update(), and delete() methods
 *
 * @param subscribable - A table descriptor or query builder (e.g., app.notes or app.notes.where({ author: id }))
 * @returns Tuple of [data, loading, mutate]
 *
 * @example
 * ```tsx
 * import { app } from './generated/client';
 *
 * function NotesList({ authorId }: { authorId: string }) {
 *   const [notes, loading, mutate] = useAll(
 *     app.notes.where({ author: authorId }).with({ folder: true })
 *   );
 *
 *   if (loading) return <div>Loading...</div>;
 *
 *   return (
 *     <div>
 *       <button onClick={() => mutate.create({ title: "New Note", author: authorId })}>
 *         Add Note
 *       </button>
 *       <ul>
 *         {notes.map(note => (
 *           <li key={note.id}>
 *             {note.title}
 *             <button onClick={() => mutate.update(note.id, { title: "Updated" })}>
 *               Edit
 *             </button>
 *             <button onClick={() => mutate.delete(note.id)}>
 *               Delete
 *             </button>
 *           </li>
 *         ))}
 *       </ul>
 *     </div>
 *   );
 * }
 * ```
 */
export function useAll<T, C, U>(
  subscribable: SubscribableAllWithDb<T, C, U>,
): [T[], boolean, MutateAll<C, U>] {
  const db = useJazz();
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);

  // Track if this is the first callback
  const isFirstCallback = useRef(true);

  // Track the previous subscribable to avoid unnecessary re-subscriptions
  const prevSubscribableRef = useRef<SubscribableAllWithDb<T, C, U> | null>(
    null,
  );
  const prevKeyRef = useRef<string | object | null>(null);

  // Get stable key for structural comparison
  const currentKey = getSubscribableKey(subscribable);

  // Use the previous subscribable if the key matches (structural equality)
  const stableSubscribable =
    prevKeyRef.current === currentKey && prevSubscribableRef.current
      ? prevSubscribableRef.current
      : subscribable;

  // Update refs for next render
  if (prevKeyRef.current !== currentKey) {
    prevKeyRef.current = currentKey;
    prevSubscribableRef.current = subscribable;
  }

  useEffect(() => {
    // Reset state on subscribable change
    setLoading(true);
    setData([]);
    isFirstCallback.current = true;

    const unsubscribe = stableSubscribable.subscribeAll(db, (rows) => {
      setData(rows);
      if (isFirstCallback.current) {
        setLoading(false);
        isFirstCallback.current = false;
      }
    });

    return unsubscribe;
  }, [db, stableSubscribable]);

  // Create mutate object
  const mutate = useMemo<MutateAll<C, U>>(
    () => ({
      create: (values: C) => stableSubscribable.create(db, values),
      update: (id: string, values: U) =>
        stableSubscribable.update(db, id, values),
      delete: (id: string) => stableSubscribable.delete(db, id),
    }),
    [db, stableSubscribable],
  );

  return [data, loading, mutate];
}

/**
 * Hook to get mutation helpers for a table without subscribing to data.
 *
 * Useful when you need to create/update/delete rows but don't need to display them.
 *
 * @param table - A table descriptor (e.g., app.users)
 * @returns A mutate object with create(), update(), and delete() methods
 *
 * @example
 * ```tsx
 * import { app } from './generated/client';
 *
 * function CreateUserButton() {
 *   const mutate = useMutate(app.users);
 *
 *   return (
 *     <button onClick={() => {
 *       const id = mutate.create({ name: "New User", email: "user@example.com" });
 *       console.log("Created user with id:", id);
 *     }}>
 *       Create User
 *     </button>
 *   );
 * }
 * ```
 */
export function useMutate<C, U>(table: MutableWithDb<C, U>): MutateAll<C, U> {
  const db = useJazz();

  return useMemo<MutateAll<C, U>>(
    () => ({
      create: (values: C) => table.create(db, values),
      update: (id: string, values: U) => table.update(db, id, values),
      delete: (id: string) => table.delete(db, id),
    }),
    [db, table],
  );
}
