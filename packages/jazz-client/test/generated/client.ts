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
import { decodeUserRows, decodeUserDelta, decodeProjectRows, decodeProjectDelta, decodeTaskRows, decodeTaskDelta, decodeTagRows, decodeTagDelta, decodeTaskTagRows, decodeTaskTagDelta, decodeCategoryRows, decodeCategoryDelta, decodeCommentRows, decodeCommentDelta } from "./decoders.js";
import type { ObjectId, User, UserInsert, UserIncludes, UserWith, UserFilter, Project, ProjectInsert, ProjectIncludes, ProjectWith, ProjectFilter, Task, TaskInsert, TaskIncludes, TaskWith, TaskFilter, Tag, TagInsert, TagIncludes, TagWith, TagFilter, TaskTag, TaskTagInsert, TaskTagIncludes, TaskTagWith, TaskTagFilter, Category, CategoryInsert, CategoryIncludes, CategoryWith, CategoryFilter, Comment, CommentInsert, CommentIncludes, CommentWith, CommentFilter } from "./types.js";

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
 * Query builder for Projects with chainable where/with methods
 * @generated from schema table: Projects
 */
export class ProjectsQueryBuilder<I extends ProjectIncludes = {}>
  implements SubscribableAllWithDb<ProjectWith<I>, ProjectInsert, Partial<ProjectInsert>>,
             SubscribableOneWithDb<ProjectWith<I>, Partial<ProjectInsert>> {
  private _descriptor: ProjectsDescriptor;
  private _where?: ProjectFilter;
  private _include?: I;

  constructor(descriptor: ProjectsDescriptor, where?: ProjectFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Projects
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Projects", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Projects
   */
  where(filter: ProjectFilter): ProjectsQueryBuilder<I> {
    return new ProjectsQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Projects
   */
  with<NewI extends ProjectIncludes>(include: NewI): ProjectsQueryBuilder<NewI> {
    return new ProjectsQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Projects
   * @generated from schema table: Projects
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: ProjectWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Project[]) => void
    );
  }

  /**
   * Subscribe to a single Project by ID
   * @generated from schema table: Projects
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: ProjectWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Project | null) => void
    );
  }

  /**
   * Create a new Project
   * @generated from schema table: Projects
   */
  create(db: WasmDatabaseLike, data: ProjectInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Project
   * @generated from schema table: Projects
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<ProjectInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Project
   * @generated from schema table: Projects
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Tasks with chainable where/with methods
 * @generated from schema table: Tasks
 */
export class TasksQueryBuilder<I extends TaskIncludes = {}>
  implements SubscribableAllWithDb<TaskWith<I>, TaskInsert, Partial<TaskInsert>>,
             SubscribableOneWithDb<TaskWith<I>, Partial<TaskInsert>> {
  private _descriptor: TasksDescriptor;
  private _where?: TaskFilter;
  private _include?: I;

  constructor(descriptor: TasksDescriptor, where?: TaskFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Tasks
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Tasks", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Tasks
   */
  where(filter: TaskFilter): TasksQueryBuilder<I> {
    return new TasksQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Tasks
   */
  with<NewI extends TaskIncludes>(include: NewI): TasksQueryBuilder<NewI> {
    return new TasksQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Tasks
   * @generated from schema table: Tasks
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: TaskWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Task[]) => void
    );
  }

  /**
   * Subscribe to a single Task by ID
   * @generated from schema table: Tasks
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: TaskWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Task | null) => void
    );
  }

  /**
   * Create a new Task
   * @generated from schema table: Tasks
   */
  create(db: WasmDatabaseLike, data: TaskInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Task
   * @generated from schema table: Tasks
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TaskInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Task
   * @generated from schema table: Tasks
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
 * Query builder for TaskTags with chainable where/with methods
 * @generated from schema table: TaskTags
 */
export class TaskTagsQueryBuilder<I extends TaskTagIncludes = {}>
  implements SubscribableAllWithDb<TaskTagWith<I>, TaskTagInsert, Partial<TaskTagInsert>>,
             SubscribableOneWithDb<TaskTagWith<I>, Partial<TaskTagInsert>> {
  private _descriptor: TaskTagsDescriptor;
  private _where?: TaskTagFilter;
  private _include?: I;

  constructor(descriptor: TaskTagsDescriptor, where?: TaskTagFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: TaskTags
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "TaskTags", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: TaskTags
   */
  where(filter: TaskTagFilter): TaskTagsQueryBuilder<I> {
    return new TaskTagsQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: TaskTags
   */
  with<NewI extends TaskTagIncludes>(include: NewI): TaskTagsQueryBuilder<NewI> {
    return new TaskTagsQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching TaskTags
   * @generated from schema table: TaskTags
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: TaskTagWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: TaskTag[]) => void
    );
  }

  /**
   * Subscribe to a single TaskTag by ID
   * @generated from schema table: TaskTags
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: TaskTagWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: TaskTag | null) => void
    );
  }

  /**
   * Create a new TaskTag
   * @generated from schema table: TaskTags
   */
  create(db: WasmDatabaseLike, data: TaskTagInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a TaskTag
   * @generated from schema table: TaskTags
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TaskTagInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a TaskTag
   * @generated from schema table: TaskTags
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Categories with chainable where/with methods
 * @generated from schema table: Categories
 */
export class CategoriesQueryBuilder<I extends CategoryIncludes = {}>
  implements SubscribableAllWithDb<CategoryWith<I>, CategoryInsert, Partial<CategoryInsert>>,
             SubscribableOneWithDb<CategoryWith<I>, Partial<CategoryInsert>> {
  private _descriptor: CategoriesDescriptor;
  private _where?: CategoryFilter;
  private _include?: I;

  constructor(descriptor: CategoriesDescriptor, where?: CategoryFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Categories
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Categories", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Categories
   */
  where(filter: CategoryFilter): CategoriesQueryBuilder<I> {
    return new CategoriesQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Categories
   */
  with<NewI extends CategoryIncludes>(include: NewI): CategoriesQueryBuilder<NewI> {
    return new CategoriesQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Categories
   * @generated from schema table: Categories
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: CategoryWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Category[]) => void
    );
  }

  /**
   * Subscribe to a single Category by ID
   * @generated from schema table: Categories
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: CategoryWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Category | null) => void
    );
  }

  /**
   * Create a new Category
   * @generated from schema table: Categories
   */
  create(db: WasmDatabaseLike, data: CategoryInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Category
   * @generated from schema table: Categories
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<CategoryInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Category
   * @generated from schema table: Categories
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Comments with chainable where/with methods
 * @generated from schema table: Comments
 */
export class CommentsQueryBuilder<I extends CommentIncludes = {}>
  implements SubscribableAllWithDb<CommentWith<I>, CommentInsert, Partial<CommentInsert>>,
             SubscribableOneWithDb<CommentWith<I>, Partial<CommentInsert>> {
  private _descriptor: CommentsDescriptor;
  private _where?: CommentFilter;
  private _include?: I;

  constructor(descriptor: CommentsDescriptor, where?: CommentFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Comments
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Comments", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Comments
   */
  where(filter: CommentFilter): CommentsQueryBuilder<I> {
    return new CommentsQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Comments
   */
  with<NewI extends CommentIncludes>(include: NewI): CommentsQueryBuilder<NewI> {
    return new CommentsQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Comments
   * @generated from schema table: Comments
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: CommentWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Comment[]) => void
    );
  }

  /**
   * Subscribe to a single Comment by ID
   * @generated from schema table: Comments
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: CommentWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Comment | null) => void
    );
  }

  /**
   * Create a new Comment
   * @generated from schema table: Comments
   */
  create(db: WasmDatabaseLike, data: CommentInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Comment
   * @generated from schema table: Comments
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<CommentInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Comment
   * @generated from schema table: Comments
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
 * Descriptor for the Projects table (no db instance, db passed at method call time)
 * @generated from schema table: Projects
 */
export class ProjectsDescriptor extends TableClient<Project>
  implements SubscribableAllWithDb<Project, ProjectInsert, Partial<ProjectInsert>>,
             SubscribableOneWithDb<Project, Partial<ProjectInsert>>,
             MutableWithDb<ProjectInsert, Partial<ProjectInsert>> {
  constructor() {
    super(schemaMeta.tables.Projects, schemaMeta, {
      rows: decodeProjectRows,
      delta: decodeProjectDelta,
    });
  }

  /**
   * Create a new Project
   * @returns The ObjectId of the created row
   * @generated from schema table: Projects
   */
  create(db: WasmDatabaseLike, data: ProjectInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    if (data.description !== undefined) values.description = data.description;
    values.owner = data.owner;
    values.color = data.color;
    return this._create(db, values);
  }

  /**
   * Update an existing Project
   * @generated from schema table: Projects
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<ProjectInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Project
   * @generated from schema table: Projects
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Projects
   */
  where(filter: ProjectFilter): ProjectsQueryBuilder<{}> {
    return new ProjectsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Projects
   */
  with<I extends ProjectIncludes>(include: I): ProjectsQueryBuilder<I> {
    return new ProjectsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Projects
   * @generated from schema table: Projects
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Project[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Project by ID
   * @generated from schema table: Projects
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Project | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Project[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Project | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Tasks table (no db instance, db passed at method call time)
 * @generated from schema table: Tasks
 */
export class TasksDescriptor extends TableClient<Task>
  implements SubscribableAllWithDb<Task, TaskInsert, Partial<TaskInsert>>,
             SubscribableOneWithDb<Task, Partial<TaskInsert>>,
             MutableWithDb<TaskInsert, Partial<TaskInsert>> {
  constructor() {
    super(schemaMeta.tables.Tasks, schemaMeta, {
      rows: decodeTaskRows,
      delta: decodeTaskDelta,
    });
  }

  /**
   * Create a new Task
   * @returns The ObjectId of the created row
   * @generated from schema table: Tasks
   */
  create(db: WasmDatabaseLike, data: TaskInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.title = data.title;
    if (data.description !== undefined) values.description = data.description;
    values.status = data.status;
    values.priority = data.priority;
    values.project = data.project;
    if (data.assignee !== undefined) values.assignee = data.assignee;
    values.createdAt = data.createdAt;
    values.updatedAt = data.updatedAt;
    values.isCompleted = data.isCompleted;
    return this._create(db, values);
  }

  /**
   * Update an existing Task
   * @generated from schema table: Tasks
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TaskInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Task
   * @generated from schema table: Tasks
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Tasks
   */
  where(filter: TaskFilter): TasksQueryBuilder<{}> {
    return new TasksQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Tasks
   */
  with<I extends TaskIncludes>(include: I): TasksQueryBuilder<I> {
    return new TasksQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Tasks
   * @generated from schema table: Tasks
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Task[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Task by ID
   * @generated from schema table: Tasks
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Task | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Task[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Task | null) => void): Unsubscribe {
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
 * Descriptor for the TaskTags table (no db instance, db passed at method call time)
 * @generated from schema table: TaskTags
 */
export class TaskTagsDescriptor extends TableClient<TaskTag>
  implements SubscribableAllWithDb<TaskTag, TaskTagInsert, Partial<TaskTagInsert>>,
             SubscribableOneWithDb<TaskTag, Partial<TaskTagInsert>>,
             MutableWithDb<TaskTagInsert, Partial<TaskTagInsert>> {
  constructor() {
    super(schemaMeta.tables.TaskTags, schemaMeta, {
      rows: decodeTaskTagRows,
      delta: decodeTaskTagDelta,
    });
  }

  /**
   * Create a new TaskTag
   * @returns The ObjectId of the created row
   * @generated from schema table: TaskTags
   */
  create(db: WasmDatabaseLike, data: TaskTagInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.task = data.task;
    values.tag = data.tag;
    return this._create(db, values);
  }

  /**
   * Update an existing TaskTag
   * @generated from schema table: TaskTags
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TaskTagInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a TaskTag
   * @generated from schema table: TaskTags
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: TaskTags
   */
  where(filter: TaskTagFilter): TaskTagsQueryBuilder<{}> {
    return new TaskTagsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: TaskTags
   */
  with<I extends TaskTagIncludes>(include: I): TaskTagsQueryBuilder<I> {
    return new TaskTagsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all TaskTags
   * @generated from schema table: TaskTags
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: TaskTag[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single TaskTag by ID
   * @generated from schema table: TaskTags
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: TaskTag | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: TaskTag[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: TaskTag | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Categories table (no db instance, db passed at method call time)
 * @generated from schema table: Categories
 */
export class CategoriesDescriptor extends TableClient<Category>
  implements SubscribableAllWithDb<Category, CategoryInsert, Partial<CategoryInsert>>,
             SubscribableOneWithDb<Category, Partial<CategoryInsert>>,
             MutableWithDb<CategoryInsert, Partial<CategoryInsert>> {
  constructor() {
    super(schemaMeta.tables.Categories, schemaMeta, {
      rows: decodeCategoryRows,
      delta: decodeCategoryDelta,
    });
  }

  /**
   * Create a new Category
   * @returns The ObjectId of the created row
   * @generated from schema table: Categories
   */
  create(db: WasmDatabaseLike, data: CategoryInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    if (data.parent !== undefined) values.parent = data.parent;
    return this._create(db, values);
  }

  /**
   * Update an existing Category
   * @generated from schema table: Categories
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<CategoryInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Category
   * @generated from schema table: Categories
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Categories
   */
  where(filter: CategoryFilter): CategoriesQueryBuilder<{}> {
    return new CategoriesQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Categories
   */
  with<I extends CategoryIncludes>(include: I): CategoriesQueryBuilder<I> {
    return new CategoriesQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Categories
   * @generated from schema table: Categories
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Category[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Category by ID
   * @generated from schema table: Categories
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Category | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Category[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Category | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Comments table (no db instance, db passed at method call time)
 * @generated from schema table: Comments
 */
export class CommentsDescriptor extends TableClient<Comment>
  implements SubscribableAllWithDb<Comment, CommentInsert, Partial<CommentInsert>>,
             SubscribableOneWithDb<Comment, Partial<CommentInsert>>,
             MutableWithDb<CommentInsert, Partial<CommentInsert>> {
  constructor() {
    super(schemaMeta.tables.Comments, schemaMeta, {
      rows: decodeCommentRows,
      delta: decodeCommentDelta,
    });
  }

  /**
   * Create a new Comment
   * @returns The ObjectId of the created row
   * @generated from schema table: Comments
   */
  create(db: WasmDatabaseLike, data: CommentInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.content = data.content;
    values.author = data.author;
    if (data.task !== undefined) values.task = data.task;
    if (data.parentComment !== undefined) values.parentComment = data.parentComment;
    values.createdAt = data.createdAt;
    return this._create(db, values);
  }

  /**
   * Update an existing Comment
   * @generated from schema table: Comments
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<CommentInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Comment
   * @generated from schema table: Comments
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Comments
   */
  where(filter: CommentFilter): CommentsQueryBuilder<{}> {
    return new CommentsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Comments
   */
  with<I extends CommentIncludes>(include: I): CommentsQueryBuilder<I> {
    return new CommentsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Comments
   * @generated from schema table: Comments
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Comment[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Comment by ID
   * @generated from schema table: Comments
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Comment | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Comment[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Comment | null) => void): Unsubscribe {
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
  projects: new ProjectsDescriptor(),
  tasks: new TasksDescriptor(),
  tags: new TagsDescriptor(),
  tasktags: new TaskTagsDescriptor(),
  categories: new CategoriesDescriptor(),
  comments: new CommentsDescriptor(),
};

export type App = typeof app;

/**
 * Database interface with bound WASM instance.
 * Created by calling createDatabase(wasmDb).
 */
export interface Database {
  users: BoundTableClient<User, UserInsert, UserIncludes>;
  projects: BoundTableClient<Project, ProjectInsert, ProjectIncludes>;
  tasks: BoundTableClient<Task, TaskInsert, TaskIncludes>;
  tags: BoundTableClient<Tag, TagInsert, TagIncludes>;
  tasktags: BoundTableClient<TaskTag, TaskTagInsert, TaskTagIncludes>;
  categories: BoundTableClient<Category, CategoryInsert, CategoryIncludes>;
  comments: BoundTableClient<Comment, CommentInsert, CommentIncludes>;
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
    projects: bindTableClient(app.projects),
    tasks: bindTableClient(app.tasks),
    tags: bindTableClient(app.tags),
    tasktags: bindTableClient(app.tasktags),
    categories: bindTableClient(app.categories),
    comments: bindTableClient(app.comments),
  };
}
