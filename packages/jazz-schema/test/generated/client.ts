// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import {
  TableClient,
  type WasmDatabaseLike,
  type Unsubscribe,
  type TableDecoder,
  type BaseWhereInput,
  type IncludeSpec,
  type SubscribableAllWithDb,
  type SubscribableOneWithDb,
  type MutableWithDb,
} from "@jazz/client";
import { schemaMeta } from "./meta.js";
import { decodeUserRows, decodeUserDelta, decodeFolderRows, decodeFolderDelta, decodeNoteRows, decodeNoteDelta, decodeTagRows, decodeTagDelta } from "./decoders.js";
import type { ObjectId, User, UserInsert, UserIncludes, UserWith, UserFilter, Folder, FolderInsert, FolderIncludes, FolderWith, FolderFilter, Note, NoteInsert, NoteIncludes, NoteWith, NoteFilter, Tag, TagInsert, TagIncludes, TagWith, TagFilter } from "./types.js";

/**
 * Query builder for Users with chainable where/with methods
 * @generated from schema table: Users
 */
export class UsersQueryBuilder<I extends UserIncludes = {}>
  implements SubscribableAllWithDb<UserWith<I>, UserInsert, Partial<UserInsert>>,
             SubscribableOneWithDb<UserWith<I>, Partial<UserInsert>> {
  private _descriptor: UsersDescriptor;
  private _where?: UserFilter;
  private _include?: I;

  constructor(descriptor: UsersDescriptor, where?: UserFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Users
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Users", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Users
   */
  where(filter: UserFilter): UsersQueryBuilder<I> {
    return new UsersQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Users
   */
  with<NewI extends UserIncludes>(include: NewI): UsersQueryBuilder<NewI> {
    return new UsersQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Users
   * @generated from schema table: Users
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: UserWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: User[]) => void
    );
  }

  /**
   * Subscribe to a single User by ID
   * @generated from schema table: Users
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: UserWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: User | null) => void
    );
  }

  /**
   * Create a new User
   * @generated from schema table: Users
   */
  create(db: WasmDatabaseLike, data: UserInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a User
   * @generated from schema table: Users
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<UserInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a User
   * @generated from schema table: Users
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Folders with chainable where/with methods
 * @generated from schema table: Folders
 */
export class FoldersQueryBuilder<I extends FolderIncludes = {}>
  implements SubscribableAllWithDb<FolderWith<I>, FolderInsert, Partial<FolderInsert>>,
             SubscribableOneWithDb<FolderWith<I>, Partial<FolderInsert>> {
  private _descriptor: FoldersDescriptor;
  private _where?: FolderFilter;
  private _include?: I;

  constructor(descriptor: FoldersDescriptor, where?: FolderFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Folders
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Folders", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Folders
   */
  where(filter: FolderFilter): FoldersQueryBuilder<I> {
    return new FoldersQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Folders
   */
  with<NewI extends FolderIncludes>(include: NewI): FoldersQueryBuilder<NewI> {
    return new FoldersQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Folders
   * @generated from schema table: Folders
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: FolderWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Folder[]) => void
    );
  }

  /**
   * Subscribe to a single Folder by ID
   * @generated from schema table: Folders
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: FolderWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Folder | null) => void
    );
  }

  /**
   * Create a new Folder
   * @generated from schema table: Folders
   */
  create(db: WasmDatabaseLike, data: FolderInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Folder
   * @generated from schema table: Folders
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<FolderInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Folder
   * @generated from schema table: Folders
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Notes with chainable where/with methods
 * @generated from schema table: Notes
 */
export class NotesQueryBuilder<I extends NoteIncludes = {}>
  implements SubscribableAllWithDb<NoteWith<I>, NoteInsert, Partial<NoteInsert>>,
             SubscribableOneWithDb<NoteWith<I>, Partial<NoteInsert>> {
  private _descriptor: NotesDescriptor;
  private _where?: NoteFilter;
  private _include?: I;

  constructor(descriptor: NotesDescriptor, where?: NoteFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Notes
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Notes", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Notes
   */
  where(filter: NoteFilter): NotesQueryBuilder<I> {
    return new NotesQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Notes
   */
  with<NewI extends NoteIncludes>(include: NewI): NotesQueryBuilder<NewI> {
    return new NotesQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Notes
   * @generated from schema table: Notes
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: NoteWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Note[]) => void
    );
  }

  /**
   * Subscribe to a single Note by ID
   * @generated from schema table: Notes
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: NoteWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Note | null) => void
    );
  }

  /**
   * Create a new Note
   * @generated from schema table: Notes
   */
  create(db: WasmDatabaseLike, data: NoteInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Note
   * @generated from schema table: Notes
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<NoteInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Note
   * @generated from schema table: Notes
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Tags with chainable where/with methods
 * @generated from schema table: Tags
 */
export class TagsQueryBuilder<I extends TagIncludes = {}>
  implements SubscribableAllWithDb<TagWith<I>, TagInsert, Partial<TagInsert>>,
             SubscribableOneWithDb<TagWith<I>, Partial<TagInsert>> {
  private _descriptor: TagsDescriptor;
  private _where?: TagFilter;
  private _include?: I;

  constructor(descriptor: TagsDescriptor, where?: TagFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Tags
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Tags", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Tags
   */
  where(filter: TagFilter): TagsQueryBuilder<I> {
    return new TagsQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Tags
   */
  with<NewI extends TagIncludes>(include: NewI): TagsQueryBuilder<NewI> {
    return new TagsQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Tags
   * @generated from schema table: Tags
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: TagWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Tag[]) => void
    );
  }

  /**
   * Subscribe to a single Tag by ID
   * @generated from schema table: Tags
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: TagWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Tag | null) => void
    );
  }

  /**
   * Create a new Tag
   * @generated from schema table: Tags
   */
  create(db: WasmDatabaseLike, data: TagInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Tag
   * @generated from schema table: Tags
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TagInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Tag
   * @generated from schema table: Tags
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Descriptor for the Users table (no db instance, db passed at method call time)
 * @generated from schema table: Users
 */
export class UsersDescriptor extends TableClient<User>
  implements SubscribableAllWithDb<User, UserInsert, Partial<UserInsert>>,
             SubscribableOneWithDb<User, Partial<UserInsert>>,
             MutableWithDb<UserInsert, Partial<UserInsert>> {
  constructor() {
    super(schemaMeta.tables.Users, schemaMeta, {
      rows: decodeUserRows,
      delta: decodeUserDelta,
    });
  }

  /**
   * Create a new User
   * @returns The ObjectId of the created row
   * @generated from schema table: Users
   */
  create(db: WasmDatabaseLike, data: UserInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.email = data.email;
    if (data.avatar !== undefined) values.avatar = data.avatar;
    values.age = data.age;
    values.score = data.score;
    values.isAdmin = data.isAdmin;
    return this._create(db, values);
  }

  /**
   * Update an existing User
   * @generated from schema table: Users
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<UserInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a User
   * @generated from schema table: Users
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Users
   */
  where(filter: UserFilter): UsersQueryBuilder<{}> {
    return new UsersQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Users
   */
  with<I extends UserIncludes>(include: I): UsersQueryBuilder<I> {
    return new UsersQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Users
   * @generated from schema table: Users
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: User[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single User by ID
   * @generated from schema table: Users
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: User | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: User[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: User | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Folders table (no db instance, db passed at method call time)
 * @generated from schema table: Folders
 */
export class FoldersDescriptor extends TableClient<Folder>
  implements SubscribableAllWithDb<Folder, FolderInsert, Partial<FolderInsert>>,
             SubscribableOneWithDb<Folder, Partial<FolderInsert>>,
             MutableWithDb<FolderInsert, Partial<FolderInsert>> {
  constructor() {
    super(schemaMeta.tables.Folders, schemaMeta, {
      rows: decodeFolderRows,
      delta: decodeFolderDelta,
    });
  }

  /**
   * Create a new Folder
   * @returns The ObjectId of the created row
   * @generated from schema table: Folders
   */
  create(db: WasmDatabaseLike, data: FolderInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.owner = data.owner;
    if (data.parent !== undefined) values.parent = data.parent;
    return this._create(db, values);
  }

  /**
   * Update an existing Folder
   * @generated from schema table: Folders
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<FolderInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Folder
   * @generated from schema table: Folders
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Folders
   */
  where(filter: FolderFilter): FoldersQueryBuilder<{}> {
    return new FoldersQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Folders
   */
  with<I extends FolderIncludes>(include: I): FoldersQueryBuilder<I> {
    return new FoldersQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Folders
   * @generated from schema table: Folders
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Folder[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Folder by ID
   * @generated from schema table: Folders
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Folder | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Folder[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Folder | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Notes table (no db instance, db passed at method call time)
 * @generated from schema table: Notes
 */
export class NotesDescriptor extends TableClient<Note>
  implements SubscribableAllWithDb<Note, NoteInsert, Partial<NoteInsert>>,
             SubscribableOneWithDb<Note, Partial<NoteInsert>>,
             MutableWithDb<NoteInsert, Partial<NoteInsert>> {
  constructor() {
    super(schemaMeta.tables.Notes, schemaMeta, {
      rows: decodeNoteRows,
      delta: decodeNoteDelta,
    });
  }

  /**
   * Create a new Note
   * @returns The ObjectId of the created row
   * @generated from schema table: Notes
   */
  create(db: WasmDatabaseLike, data: NoteInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.title = data.title;
    values.content = data.content;
    values.author = data.author;
    if (data.folder !== undefined) values.folder = data.folder;
    values.createdAt = data.createdAt;
    values.updatedAt = data.updatedAt;
    values.isPublic = data.isPublic;
    return this._create(db, values);
  }

  /**
   * Update an existing Note
   * @generated from schema table: Notes
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<NoteInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Note
   * @generated from schema table: Notes
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Notes
   */
  where(filter: NoteFilter): NotesQueryBuilder<{}> {
    return new NotesQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Notes
   */
  with<I extends NoteIncludes>(include: I): NotesQueryBuilder<I> {
    return new NotesQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Notes
   * @generated from schema table: Notes
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Note[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Note by ID
   * @generated from schema table: Notes
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Note | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Note[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Note | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Tags table (no db instance, db passed at method call time)
 * @generated from schema table: Tags
 */
export class TagsDescriptor extends TableClient<Tag>
  implements SubscribableAllWithDb<Tag, TagInsert, Partial<TagInsert>>,
             SubscribableOneWithDb<Tag, Partial<TagInsert>>,
             MutableWithDb<TagInsert, Partial<TagInsert>> {
  constructor() {
    super(schemaMeta.tables.Tags, schemaMeta, {
      rows: decodeTagRows,
      delta: decodeTagDelta,
    });
  }

  /**
   * Create a new Tag
   * @returns The ObjectId of the created row
   * @generated from schema table: Tags
   */
  create(db: WasmDatabaseLike, data: TagInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.color = data.color;
    return this._create(db, values);
  }

  /**
   * Update an existing Tag
   * @generated from schema table: Tags
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TagInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Tag
   * @generated from schema table: Tags
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Tags
   */
  where(filter: TagFilter): TagsQueryBuilder<{}> {
    return new TagsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Tags
   */
  with<I extends TagIncludes>(include: I): TagsQueryBuilder<I> {
    return new TagsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Tags
   * @generated from schema table: Tags
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Tag[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Tag by ID
   * @generated from schema table: Tags
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Tag | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Tag[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Tag | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Typed schema descriptor for use with React hooks.
 * Pass queries to useAll/useOne, which inject the db from context.
 *
 * @example
 * ```typescript
 * import { app } from './generated/client';
 * import { useAll, useOne, useMutate } from '@jazz/react';
 *
 * function UserList() {
 *   const [users, loading, mutate] = useAll(app.users);
 *   return users.map(u => <li key={u.id}>{u.name}</li>);
 * }
 *
 * function UserProfile({ userId }) {
 *   const [user, loading, mutate] = useOne(app.users, userId);
 *   return <div>{user?.name}</div>;
 * }
 * ```
 */
export const app = {
  users: new UsersDescriptor(),
  folders: new FoldersDescriptor(),
  notes: new NotesDescriptor(),
  tags: new TagsDescriptor(),
};

export type App = typeof app;

/**
 * Database interface with bound WASM instance.
 * Created by calling createDatabase(wasmDb).
 */
export interface Database {
  users: BoundTableClient<User, UserInsert, UserIncludes>;
  folders: BoundTableClient<Folder, FolderInsert, FolderIncludes>;
  notes: BoundTableClient<Note, NoteInsert, NoteIncludes>;
  tags: BoundTableClient<Tag, TagInsert, TagIncludes>;
}

/**
 * A table client with the WASM database bound, allowing direct method calls.
 */
export interface BoundTableClient<T, TInsert, TIncludes> {
  create(data: TInsert): ObjectId;
  update(id: ObjectId, data: Partial<TInsert>): void;
  delete(id: ObjectId): void;
  subscribeAll(callback: (rows: T[]) => void): Unsubscribe;
  subscribe(id: ObjectId, callback: (row: T | null) => void): Unsubscribe;
  where(filter: any): BoundQueryBuilder<T, TInsert, TIncludes>;
  with<I extends TIncludes>(include: I): BoundQueryBuilder<any, TInsert, TIncludes>;
}

/**
 * A query builder with the WASM database bound.
 */
export interface BoundQueryBuilder<T, TInsert, TIncludes> {
  subscribeAll(callback: (rows: T[]) => void): Unsubscribe;
  subscribe(id: ObjectId, callback: (row: T | null) => void): Unsubscribe;
  where(filter: any): BoundQueryBuilder<T, TInsert, TIncludes>;
  with<I extends TIncludes>(include: I): BoundQueryBuilder<any, TInsert, TIncludes>;
  create(data: TInsert): ObjectId;
  update(id: ObjectId, data: Partial<TInsert>): void;
  delete(id: ObjectId): void;
}

/**
 * Create a database client with the WASM database bound.
 * This allows calling methods directly without passing the db instance.
 *
 * @example
 * ```typescript
 * const db = createDatabase(wasmDb);
 * const userId = db.users.create({ name: "Alice", ... });
 * db.users.subscribeAll((users) => console.log(users));
 * ```
 */
export function createDatabase(wasmDb: WasmDatabaseLike): Database {
  function bindQueryBuilder<T, TInsert, TIncludes>(builder: any): BoundQueryBuilder<T, TInsert, TIncludes> {
    return {
      subscribeAll: (cb) => builder.subscribeAll(wasmDb, cb),
      subscribe: (id, cb) => builder.subscribe(wasmDb, id, cb),
      where: (filter) => bindQueryBuilder(builder.where(filter)),
      with: (include) => bindQueryBuilder(builder.with(include)),
      create: (data) => builder.create(wasmDb, data),
      update: (id, data) => builder.update(wasmDb, id, data),
      delete: (id) => builder.delete(wasmDb, id),
    };
  }

  function bindTableClient<T, TInsert, TIncludes>(client: any): BoundTableClient<T, TInsert, TIncludes> {
    return {
      create: (data) => client.create(wasmDb, data),
      update: (id, data) => client.update(wasmDb, id, data),
      delete: (id) => client.delete(wasmDb, id),
      subscribeAll: (cb) => client.subscribeAll(wasmDb, cb),
      subscribe: (id, cb) => client.subscribe(wasmDb, id, cb),
      where: (filter) => bindQueryBuilder(client.where(filter)),
      with: (include) => bindQueryBuilder(client.with(include)),
    };
  }

  return {
    users: bindTableClient(app.users),
    folders: bindTableClient(app.folders),
    notes: bindTableClient(app.notes),
    tags: bindTableClient(app.tags),
  };
}
