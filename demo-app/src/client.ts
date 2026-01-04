/**
 * Groove Database Client
 *
 * Type-safe client for subscribing to database rows with eager loading support.
 */

import {
  buildQuery,
  buildQueryById,
  type BaseWhereInput,
  type IncludeSpec,
  type Unsubscribe,
} from "@jazz/schema";

import { schemaMeta } from "./meta.js";
import type {
  ObjectId,
  User,
  UserDepth,
  UserLoaded,
  UserWhereInput,
  Folder,
  FolderDepth,
  FolderLoaded,
  FolderWhereInput,
  Note,
  NoteDepth,
  NoteLoaded,
  NoteWhereInput,
  Tag,
  TagDepth,
  TagLoaded,
  TagWhereInput,
} from "./types.js";

// === Client Interfaces ===

interface UserClient {
  /**
   * Subscribe to a single user by ID
   */
  subscribe<D extends UserDepth = {}>(
    id: ObjectId,
    options: { include?: D },
    callback: (user: UserLoaded<D> | null) => void
  ): Unsubscribe;

  /**
   * Subscribe to multiple users matching criteria
   */
  subscribeAll<D extends UserDepth = {}>(
    options: { where?: UserWhereInput; include?: D },
    callback: (users: UserLoaded<D>[]) => void
  ): Unsubscribe;
}

interface FolderClient {
  /**
   * Subscribe to a single folder by ID
   */
  subscribe<D extends FolderDepth = {}>(
    id: ObjectId,
    options: { include?: D },
    callback: (folder: FolderLoaded<D> | null) => void
  ): Unsubscribe;

  /**
   * Subscribe to multiple folders matching criteria
   */
  subscribeAll<D extends FolderDepth = {}>(
    options: { where?: FolderWhereInput; include?: D },
    callback: (folders: FolderLoaded<D>[]) => void
  ): Unsubscribe;
}

interface NoteClient {
  /**
   * Subscribe to a single note by ID
   */
  subscribe<D extends NoteDepth = {}>(
    id: ObjectId,
    options: { include?: D },
    callback: (note: NoteLoaded<D> | null) => void
  ): Unsubscribe;

  /**
   * Subscribe to multiple notes matching criteria
   */
  subscribeAll<D extends NoteDepth = {}>(
    options: { where?: NoteWhereInput; include?: D },
    callback: (notes: NoteLoaded<D>[]) => void
  ): Unsubscribe;
}

interface TagClient {
  /**
   * Subscribe to a single tag by ID
   */
  subscribe<D extends TagDepth = {}>(
    id: ObjectId,
    options: { include?: D },
    callback: (tag: TagLoaded<D> | null) => void
  ): Unsubscribe;

  /**
   * Subscribe to multiple tags matching criteria
   */
  subscribeAll<D extends TagDepth = {}>(
    options: { where?: TagWhereInput; include?: D },
    callback: (tags: TagLoaded<D>[]) => void
  ): Unsubscribe;
}

// === Database Interface ===

export interface GrooveDatabase {
  user: UserClient;
  folder: FolderClient;
  note: NoteClient;
  tag: TagClient;
}

// === Implementation ===

/**
 * Create a table client for a given table.
 * This is a stub implementation - the actual WASM integration is TODO.
 */
function createTableClient(tableName: string) {
  const tableMeta = schemaMeta.tables[tableName];

  return {
    subscribe(
      id: ObjectId,
      options: { include?: IncludeSpec },
      callback: (row: unknown) => void
    ): Unsubscribe {
      // Build the SQL query
      const sql = buildQueryById(tableMeta, schemaMeta, id, {
        include: options.include,
      });

      console.log(`[${tableName}.subscribe] SQL:`, sql);

      // TODO: Execute query via WASM and set up reactive subscription
      // For now, just log and return a no-op unsubscribe

      return () => {
        console.log(`[${tableName}.subscribe] Unsubscribed from ${id}`);
      };
    },

    subscribeAll(
      options: { where?: BaseWhereInput; include?: IncludeSpec },
      callback: (rows: unknown[]) => void
    ): Unsubscribe {
      // Build the SQL query
      const sql = buildQuery(tableMeta, schemaMeta, {
        where: options.where,
        include: options.include,
      });

      console.log(`[${tableName}.subscribeAll] SQL:`, sql);

      // TODO: Execute query via WASM and set up reactive subscription
      // For now, just log and return a no-op unsubscribe

      return () => {
        console.log(`[${tableName}.subscribeAll] Unsubscribed`);
      };
    },
  };
}

/**
 * Create a Groove database client.
 *
 * @example
 * ```typescript
 * const db = createDatabase();
 *
 * // Subscribe to a single note with author loaded
 * db.note.subscribe(noteId, { include: { author: true } }, (note) => {
 *   console.log(note?.title, note?.author.name);
 * });
 *
 * // Subscribe to all notes created today
 * db.note.subscribeAll(
 *   { where: { createdAt: { gte: todayTimestamp } }, include: { author: true } },
 *   (notes) => {
 *     notes.forEach(n => console.log(n.title));
 *   }
 * );
 * ```
 */
export function createDatabase(): GrooveDatabase {
  // Type assertions needed because TypeScript can't infer generic type parameters
  // through the generic createTableClient function. The runtime behavior is correct.
  return {
    user: createTableClient("User") as unknown as UserClient,
    folder: createTableClient("Folder") as unknown as FolderClient,
    note: createTableClient("Note") as unknown as NoteClient,
    tag: createTableClient("Tag") as unknown as TagClient,
  };
}

// === Query Preview (for debugging) ===

/**
 * Preview the SQL query that would be generated for a subscription.
 * Useful for debugging and understanding what queries are being built.
 */
export function previewQuery(
  tableName: keyof typeof schemaMeta.tables,
  options: { id?: ObjectId; where?: BaseWhereInput; include?: IncludeSpec } = {}
): string {
  const tableMeta = schemaMeta.tables[tableName];

  if (options.id) {
    return buildQueryById(tableMeta, schemaMeta, options.id, {
      include: options.include,
    });
  }

  return buildQuery(tableMeta, schemaMeta, {
    where: options.where,
    include: options.include,
  });
}
