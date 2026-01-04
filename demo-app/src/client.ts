/**
 * Groove Database Client
 *
 * Type-safe client for subscribing to database rows with eager loading support.
 * Uses binary encoding for efficient data transfer from WASM.
 */

import {
  buildQuery,
  buildQueryById,
  type BaseWhereInput,
  type IncludeSpec,
  type Unsubscribe,
} from "@jazz/schema/runtime";

import { schemaMeta } from "./meta.js";
import {
  decodeUserRows,
  decodeUserDelta,
  decodeFolderRows,
  decodeFolderDelta,
  decodeNoteRows,
  decodeNoteDelta,
  decodeTagRows,
  decodeTagDelta,
  type Delta,
  DELTA_ADDED,
  DELTA_UPDATED,
  DELTA_REMOVED,
} from "./decoders.js";
import type {
  ObjectId,
  User,
  UserIncludes,
  UserLoaded,
  UserFilter,
  Folder,
  FolderIncludes,
  FolderLoaded,
  FolderFilter,
  Note,
  NoteIncludes,
  NoteLoaded,
  NoteFilter,
  Tag,
  TagIncludes,
  TagLoaded,
  TagFilter,
} from "./types.js";

// Import WASM types
import type { WasmDatabase, WasmQueryHandleDelta } from "../pkg/groove_wasm.js";

// === Client Interfaces ===

interface UserClient {
  subscribe<I extends UserIncludes = {}>(
    id: ObjectId,
    options: { include?: I },
    callback: (user: UserLoaded<I> | null) => void
  ): Unsubscribe;

  subscribeAll<I extends UserIncludes = {}>(
    options: { where?: UserFilter; include?: I },
    callback: (users: UserLoaded<I>[]) => void
  ): Unsubscribe;
}

interface FolderClient {
  subscribe<I extends FolderIncludes = {}>(
    id: ObjectId,
    options: { include?: I },
    callback: (folder: FolderLoaded<I> | null) => void
  ): Unsubscribe;

  subscribeAll<I extends FolderIncludes = {}>(
    options: { where?: FolderFilter; include?: I },
    callback: (folders: FolderLoaded<I>[]) => void
  ): Unsubscribe;
}

interface NoteClient {
  subscribe<I extends NoteIncludes = {}>(
    id: ObjectId,
    options: { include?: I },
    callback: (note: NoteLoaded<I> | null) => void
  ): Unsubscribe;

  subscribeAll<I extends NoteIncludes = {}>(
    options: { where?: NoteFilter; include?: I },
    callback: (notes: NoteLoaded<I>[]) => void
  ): Unsubscribe;
}

interface TagClient {
  subscribe<I extends TagIncludes = {}>(
    id: ObjectId,
    options: { include?: I },
    callback: (tag: TagLoaded<I> | null) => void
  ): Unsubscribe;

  subscribeAll<I extends TagIncludes = {}>(
    options: { where?: TagFilter; include?: I },
    callback: (tags: TagLoaded<I>[]) => void
  ): Unsubscribe;
}

// === Database Interface ===

export interface GrooveDatabase {
  /** Raw WASM database for direct SQL execution */
  raw: WasmDatabase;

  user: UserClient;
  folder: FolderClient;
  note: NoteClient;
  tag: TagClient;
}

// === Decoder Registry ===

type RowDecoder<T> = (buffer: ArrayBuffer) => T[];
type DeltaDecoder<T> = (buffer: ArrayBuffer) => Delta<T>;

const decoders: Record<string, { rows: RowDecoder<any>; delta: DeltaDecoder<any> }> = {
  User: { rows: decodeUserRows, delta: decodeUserDelta },
  Folder: { rows: decodeFolderRows, delta: decodeFolderDelta },
  Note: { rows: decodeNoteRows, delta: decodeNoteDelta },
  Tag: { rows: decodeTagRows, delta: decodeTagDelta },
};

// === Implementation ===

/**
 * Create a table client for a given table.
 */
function createTableClient<T, I extends object, F extends BaseWhereInput>(
  db: WasmDatabase,
  tableName: string
) {
  const tableMeta = schemaMeta.tables[tableName];
  const decoder = decoders[tableName];

  return {
    subscribe(
      id: ObjectId,
      options: { include?: I },
      callback: (row: T | null) => void
    ): Unsubscribe {
      const sql = buildQueryById(tableMeta, schemaMeta, id, {
        include: options.include as IncludeSpec,
      });

      // Maintain current state
      let currentRow: T | null = null;

      // Use delta subscription for efficiency
      const handle = db.subscribe_delta(sql, (deltas: Uint8Array[]) => {
        for (const deltaBuffer of deltas) {
          const delta = decoder.delta(deltaBuffer.buffer) as Delta<T>;

          if (delta.type === 'added' || delta.type === 'updated') {
            currentRow = delta.row;
          } else if (delta.type === 'removed') {
            currentRow = null;
          }
        }
        callback(currentRow);
      });

      return () => {
        handle.unsubscribe();
        handle.free();
      };
    },

    subscribeAll(
      options: { where?: F; include?: I },
      callback: (rows: T[]) => void
    ): Unsubscribe {
      const sql = buildQuery(tableMeta, schemaMeta, {
        where: options.where,
        include: options.include as IncludeSpec,
      });

      // Maintain current state as a Map for efficient updates
      const rowsById = new Map<string, T>();

      // Use delta subscription for efficiency
      const handle = db.subscribe_delta(sql, (deltas: Uint8Array[]) => {
        for (const deltaBuffer of deltas) {
          const delta = decoder.delta(deltaBuffer.buffer) as Delta<T>;

          if (delta.type === 'added' || delta.type === 'updated') {
            rowsById.set((delta.row as any).id, delta.row);
          } else if (delta.type === 'removed') {
            rowsById.delete(delta.id);
          }
        }
        callback(Array.from(rowsById.values()));
      });

      return () => {
        handle.unsubscribe();
        handle.free();
      };
    },
  };
}

/**
 * Create a Groove database client connected to a WASM database instance.
 *
 * @example
 * ```typescript
 * import init, { WasmDatabase } from './pkg/groove_wasm.js';
 *
 * await init();
 * const wasmDb = new WasmDatabase();
 * const db = createDatabase(wasmDb);
 *
 * // Create schema
 * db.raw.execute(`CREATE TABLE Note (title STRING NOT NULL, content STRING NOT NULL)`);
 *
 * // Subscribe to all notes
 * db.note.subscribeAll({}, (notes) => {
 *   console.log('Notes:', notes);
 * });
 *
 * // Insert a note - subscription will be called
 * db.raw.execute(`INSERT INTO Note (title, content) VALUES ('Hello', 'World')`);
 * ```
 */
export function createDatabase(wasmDb: WasmDatabase): GrooveDatabase {
  return {
    raw: wasmDb,
    user: createTableClient<User, UserIncludes, UserFilter>(wasmDb, "User") as UserClient,
    folder: createTableClient<Folder, FolderIncludes, FolderFilter>(wasmDb, "Folder") as FolderClient,
    note: createTableClient<Note, NoteIncludes, NoteFilter>(wasmDb, "Note") as NoteClient,
    tag: createTableClient<Tag, TagIncludes, TagFilter>(wasmDb, "Tag") as TagClient,
  };
}

// === Query Preview (for debugging) ===

/**
 * Preview the SQL query that would be generated for a subscription.
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
