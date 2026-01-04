// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import { TableClient, type WasmDatabaseLike, type Unsubscribe, type TableDecoder } from "@jazz/client";
import { schemaMeta } from "./meta.js";
import { decodeUserRows, decodeUserDelta, decodeFolderRows, decodeFolderDelta, decodeNoteRows, decodeNoteDelta, decodeTagRows, decodeTagDelta } from "./decoders.js";
import type { ObjectId, User, UserInsert, UserIncludes, UserLoaded, UserFilter, Folder, FolderInsert, FolderIncludes, FolderLoaded, FolderFilter, Note, NoteInsert, NoteIncludes, NoteLoaded, NoteFilter, Tag, TagInsert, TagIncludes, TagLoaded, TagFilter } from "./types.js";

/**
 * Client for the Users table
 */
export class UsersClient extends TableClient<User> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Users, schemaMeta, {
      rows: decodeUserRows,
      delta: decodeUserDelta,
    });
  }

  /**
   * Create a new User
   * @returns The ObjectId of the created row
   */
  create(data: UserInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.email = data.email;
    if (data.avatar !== undefined) values.avatar = data.avatar;
    return this._create(values);
  }

  /**
   * Update an existing User
   */
  update(id: ObjectId, data: Partial<UserInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a User
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Subscribe to a single User by ID
   */
  subscribe<I extends UserIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: UserLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: User | null) => void);
  }

  /**
   * Subscribe to all Users matching a filter
   */
  subscribeAll<I extends UserIncludes = {}>(options: { where?: UserFilter; include?: I }, callback: (rows: UserLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback as (rows: User[]) => void);
  }
}

/**
 * Client for the Folders table
 */
export class FoldersClient extends TableClient<Folder> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Folders, schemaMeta, {
      rows: decodeFolderRows,
      delta: decodeFolderDelta,
    });
  }

  /**
   * Create a new Folder
   * @returns The ObjectId of the created row
   */
  create(data: FolderInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.owner = data.owner;
    if (data.parent !== undefined) values.parent = data.parent;
    return this._create(values);
  }

  /**
   * Update an existing Folder
   */
  update(id: ObjectId, data: Partial<FolderInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a Folder
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Subscribe to a single Folder by ID
   */
  subscribe<I extends FolderIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: FolderLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: Folder | null) => void);
  }

  /**
   * Subscribe to all Folders matching a filter
   */
  subscribeAll<I extends FolderIncludes = {}>(options: { where?: FolderFilter; include?: I }, callback: (rows: FolderLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback as (rows: Folder[]) => void);
  }
}

/**
 * Client for the Notes table
 */
export class NotesClient extends TableClient<Note> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Notes, schemaMeta, {
      rows: decodeNoteRows,
      delta: decodeNoteDelta,
    });
  }

  /**
   * Create a new Note
   * @returns The ObjectId of the created row
   */
  create(data: NoteInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.title = data.title;
    values.content = data.content;
    values.author = data.author;
    if (data.folder !== undefined) values.folder = data.folder;
    values.createdAt = data.createdAt;
    values.updatedAt = data.updatedAt;
    return this._create(values);
  }

  /**
   * Update an existing Note
   */
  update(id: ObjectId, data: Partial<NoteInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a Note
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Subscribe to a single Note by ID
   */
  subscribe<I extends NoteIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: NoteLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: Note | null) => void);
  }

  /**
   * Subscribe to all Notes matching a filter
   */
  subscribeAll<I extends NoteIncludes = {}>(options: { where?: NoteFilter; include?: I }, callback: (rows: NoteLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback as (rows: Note[]) => void);
  }
}

/**
 * Client for the Tags table
 */
export class TagsClient extends TableClient<Tag> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Tags, schemaMeta, {
      rows: decodeTagRows,
      delta: decodeTagDelta,
    });
  }

  /**
   * Create a new Tag
   * @returns The ObjectId of the created row
   */
  create(data: TagInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.color = data.color;
    return this._create(values);
  }

  /**
   * Update an existing Tag
   */
  update(id: ObjectId, data: Partial<TagInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a Tag
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Subscribe to a single Tag by ID
   */
  subscribe(id: ObjectId, options: { include?: TagIncludes }, callback: (row: Tag | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }

  /**
   * Subscribe to all Tags matching a filter
   */
  subscribeAll(options: { where?: TagFilter; include?: TagIncludes }, callback: (rows: Tag[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }
}

/**
 * Typed database interface
 */
export interface Database {
  /** Raw WASM database for direct SQL access */
  raw: WasmDatabaseLike;
  users: UsersClient;
  folders: FoldersClient;
  notes: NotesClient;
  tags: TagsClient;
}

/**
 * Create a typed database client from a WASM database instance.
 *
 * @example
 * ```typescript
 * import init, { WasmDatabase } from './pkg/groove_wasm.js';
 *
 * await init();
 * const wasmDb = new WasmDatabase();
 * const db = createDatabase(wasmDb);
 *
 * // Create a user
 * const userId = db.users.create({ name: 'Alice', email: 'alice@example.com' });
 *
 * // Subscribe to all users
 * db.users.subscribeAll({}, (users) => console.log(users));
 * ```
 */
export function createDatabase(wasmDb: WasmDatabaseLike): Database {
  return {
    raw: wasmDb,
    users: new UsersClient(wasmDb),
    folders: new FoldersClient(wasmDb),
    notes: new NotesClient(wasmDb),
    tags: new TagsClient(wasmDb),
  };
}
