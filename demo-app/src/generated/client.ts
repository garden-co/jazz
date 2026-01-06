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
import { decodeUserRows, decodeUserDelta, decodeProjectRows, decodeProjectDelta, decodeIssueRows, decodeIssueDelta, decodeLabelRows, decodeLabelDelta, decodeIssueLabelRows, decodeIssueLabelDelta, decodeIssueAssigneeRows, decodeIssueAssigneeDelta } from "./decoders.js";
import type { ObjectId, User, UserInsert, UserIncludes, UserWith, UserFilter, Project, ProjectInsert, ProjectIncludes, ProjectWith, ProjectFilter, Issue, IssueInsert, IssueIncludes, IssueWith, IssueFilter, Label, LabelInsert, LabelIncludes, LabelWith, LabelFilter, IssueLabel, IssueLabelInsert, IssueLabelIncludes, IssueLabelWith, IssueLabelFilter, IssueAssignee, IssueAssigneeInsert, IssueAssigneeIncludes, IssueAssigneeWith, IssueAssigneeFilter } from "./types.js";

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
 * Query builder for Issues with chainable where/with methods
 * @generated from schema table: Issues
 */
export class IssuesQueryBuilder<I extends IssueIncludes = {}>
  implements SubscribableAllWithDb<IssueWith<I>, IssueInsert, Partial<IssueInsert>>,
             SubscribableOneWithDb<IssueWith<I>, Partial<IssueInsert>> {
  private _descriptor: IssuesDescriptor;
  private _where?: IssueFilter;
  private _include?: I;

  constructor(descriptor: IssuesDescriptor, where?: IssueFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Issues
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Issues", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Issues
   */
  where(filter: IssueFilter): IssuesQueryBuilder<I> {
    return new IssuesQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Issues
   */
  with<NewI extends IssueIncludes>(include: NewI): IssuesQueryBuilder<NewI> {
    return new IssuesQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Issues
   * @generated from schema table: Issues
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: IssueWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Issue[]) => void
    );
  }

  /**
   * Subscribe to a single Issue by ID
   * @generated from schema table: Issues
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: IssueWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Issue | null) => void
    );
  }

  /**
   * Create a new Issue
   * @generated from schema table: Issues
   */
  create(db: WasmDatabaseLike, data: IssueInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Issue
   * @generated from schema table: Issues
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<IssueInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Issue
   * @generated from schema table: Issues
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for Labels with chainable where/with methods
 * @generated from schema table: Labels
 */
export class LabelsQueryBuilder<I extends LabelIncludes = {}>
  implements SubscribableAllWithDb<LabelWith<I>, LabelInsert, Partial<LabelInsert>>,
             SubscribableOneWithDb<LabelWith<I>, Partial<LabelInsert>> {
  private _descriptor: LabelsDescriptor;
  private _where?: LabelFilter;
  private _include?: I;

  constructor(descriptor: LabelsDescriptor, where?: LabelFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: Labels
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Labels", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: Labels
   */
  where(filter: LabelFilter): LabelsQueryBuilder<I> {
    return new LabelsQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: Labels
   */
  with<NewI extends LabelIncludes>(include: NewI): LabelsQueryBuilder<NewI> {
    return new LabelsQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching Labels
   * @generated from schema table: Labels
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: LabelWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Label[]) => void
    );
  }

  /**
   * Subscribe to a single Label by ID
   * @generated from schema table: Labels
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: LabelWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Label | null) => void
    );
  }

  /**
   * Create a new Label
   * @generated from schema table: Labels
   */
  create(db: WasmDatabaseLike, data: LabelInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a Label
   * @generated from schema table: Labels
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<LabelInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a Label
   * @generated from schema table: Labels
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for IssueLabels with chainable where/with methods
 * @generated from schema table: IssueLabels
 */
export class IssueLabelsQueryBuilder<I extends IssueLabelIncludes = {}>
  implements SubscribableAllWithDb<IssueLabelWith<I>, IssueLabelInsert, Partial<IssueLabelInsert>>,
             SubscribableOneWithDb<IssueLabelWith<I>, Partial<IssueLabelInsert>> {
  private _descriptor: IssueLabelsDescriptor;
  private _where?: IssueLabelFilter;
  private _include?: I;

  constructor(descriptor: IssueLabelsDescriptor, where?: IssueLabelFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: IssueLabels
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "IssueLabels", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: IssueLabels
   */
  where(filter: IssueLabelFilter): IssueLabelsQueryBuilder<I> {
    return new IssueLabelsQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: IssueLabels
   */
  with<NewI extends IssueLabelIncludes>(include: NewI): IssueLabelsQueryBuilder<NewI> {
    return new IssueLabelsQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching IssueLabels
   * @generated from schema table: IssueLabels
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: IssueLabelWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: IssueLabel[]) => void
    );
  }

  /**
   * Subscribe to a single IssueLabel by ID
   * @generated from schema table: IssueLabels
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: IssueLabelWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: IssueLabel | null) => void
    );
  }

  /**
   * Create a new IssueLabel
   * @generated from schema table: IssueLabels
   */
  create(db: WasmDatabaseLike, data: IssueLabelInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a IssueLabel
   * @generated from schema table: IssueLabels
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<IssueLabelInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a IssueLabel
   * @generated from schema table: IssueLabels
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    return this._descriptor.delete(db, id);
  }
}

/**
 * Query builder for IssueAssignees with chainable where/with methods
 * @generated from schema table: IssueAssignees
 */
export class IssueAssigneesQueryBuilder<I extends IssueAssigneeIncludes = {}>
  implements SubscribableAllWithDb<IssueAssigneeWith<I>, IssueAssigneeInsert, Partial<IssueAssigneeInsert>>,
             SubscribableOneWithDb<IssueAssigneeWith<I>, Partial<IssueAssigneeInsert>> {
  private _descriptor: IssueAssigneesDescriptor;
  private _where?: IssueAssigneeFilter;
  private _include?: I;

  constructor(descriptor: IssueAssigneesDescriptor, where?: IssueAssigneeFilter, include?: I) {
    this._descriptor = descriptor;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   * @generated from schema table: IssueAssignees
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "IssueAssignees", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   * @generated from schema table: IssueAssignees
   */
  where(filter: IssueAssigneeFilter): IssueAssigneesQueryBuilder<I> {
    return new IssueAssigneesQueryBuilder(this._descriptor, filter, this._include);
  }

  /**
   * Specify which refs to include
   * @generated from schema table: IssueAssignees
   */
  with<NewI extends IssueAssigneeIncludes>(include: NewI): IssueAssigneesQueryBuilder<NewI> {
    return new IssueAssigneesQueryBuilder(this._descriptor, this._where, include);
  }

  /**
   * Subscribe to all matching IssueAssignees
   * @generated from schema table: IssueAssignees
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: IssueAssigneeWith<I>[]) => void): Unsubscribe {
    return this._descriptor._subscribeAllInternal(
      db,
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: IssueAssignee[]) => void
    );
  }

  /**
   * Subscribe to a single IssueAssignee by ID
   * @generated from schema table: IssueAssignees
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: IssueAssigneeWith<I> | null) => void): Unsubscribe {
    return this._descriptor._subscribeInternal(
      db,
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: IssueAssignee | null) => void
    );
  }

  /**
   * Create a new IssueAssignee
   * @generated from schema table: IssueAssignees
   */
  create(db: WasmDatabaseLike, data: IssueAssigneeInsert): ObjectId {
    return this._descriptor.create(db, data);
  }

  /**
   * Update a IssueAssignee
   * @generated from schema table: IssueAssignees
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<IssueAssigneeInsert>): void {
    return this._descriptor.update(db, id, data);
  }

  /**
   * Delete a IssueAssignee
   * @generated from schema table: IssueAssignees
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
    values.avatarColor = data.avatarColor;
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
    values.color = data.color;
    if (data.description !== undefined) values.description = data.description;
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
 * Descriptor for the Issues table (no db instance, db passed at method call time)
 * @generated from schema table: Issues
 */
export class IssuesDescriptor extends TableClient<Issue>
  implements SubscribableAllWithDb<Issue, IssueInsert, Partial<IssueInsert>>,
             SubscribableOneWithDb<Issue, Partial<IssueInsert>>,
             MutableWithDb<IssueInsert, Partial<IssueInsert>> {
  constructor() {
    super(schemaMeta.tables.Issues, schemaMeta, {
      rows: decodeIssueRows,
      delta: decodeIssueDelta,
    });
  }

  /**
   * Create a new Issue
   * @returns The ObjectId of the created row
   * @generated from schema table: Issues
   */
  create(db: WasmDatabaseLike, data: IssueInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.title = data.title;
    if (data.description !== undefined) values.description = data.description;
    values.status = data.status;
    values.priority = data.priority;
    values.project = data.project;
    values.createdAt = data.createdAt;
    values.updatedAt = data.updatedAt;
    return this._create(db, values);
  }

  /**
   * Update an existing Issue
   * @generated from schema table: Issues
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<IssueInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Issue
   * @generated from schema table: Issues
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Issues
   */
  where(filter: IssueFilter): IssuesQueryBuilder<{}> {
    return new IssuesQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Issues
   */
  with<I extends IssueIncludes>(include: I): IssuesQueryBuilder<I> {
    return new IssuesQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Issues
   * @generated from schema table: Issues
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Issue[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Issue by ID
   * @generated from schema table: Issues
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Issue | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Issue[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Issue | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the Labels table (no db instance, db passed at method call time)
 * @generated from schema table: Labels
 */
export class LabelsDescriptor extends TableClient<Label>
  implements SubscribableAllWithDb<Label, LabelInsert, Partial<LabelInsert>>,
             SubscribableOneWithDb<Label, Partial<LabelInsert>>,
             MutableWithDb<LabelInsert, Partial<LabelInsert>> {
  constructor() {
    super(schemaMeta.tables.Labels, schemaMeta, {
      rows: decodeLabelRows,
      delta: decodeLabelDelta,
    });
  }

  /**
   * Create a new Label
   * @returns The ObjectId of the created row
   * @generated from schema table: Labels
   */
  create(db: WasmDatabaseLike, data: LabelInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.color = data.color;
    return this._create(db, values);
  }

  /**
   * Update an existing Label
   * @generated from schema table: Labels
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<LabelInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a Label
   * @generated from schema table: Labels
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: Labels
   */
  where(filter: LabelFilter): LabelsQueryBuilder<{}> {
    return new LabelsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: Labels
   */
  with<I extends LabelIncludes>(include: I): LabelsQueryBuilder<I> {
    return new LabelsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all Labels
   * @generated from schema table: Labels
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Label[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single Label by ID
   * @generated from schema table: Labels
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Label | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Label[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Label | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the IssueLabels table (no db instance, db passed at method call time)
 * @generated from schema table: IssueLabels
 */
export class IssueLabelsDescriptor extends TableClient<IssueLabel>
  implements SubscribableAllWithDb<IssueLabel, IssueLabelInsert, Partial<IssueLabelInsert>>,
             SubscribableOneWithDb<IssueLabel, Partial<IssueLabelInsert>>,
             MutableWithDb<IssueLabelInsert, Partial<IssueLabelInsert>> {
  constructor() {
    super(schemaMeta.tables.IssueLabels, schemaMeta, {
      rows: decodeIssueLabelRows,
      delta: decodeIssueLabelDelta,
    });
  }

  /**
   * Create a new IssueLabel
   * @returns The ObjectId of the created row
   * @generated from schema table: IssueLabels
   */
  create(db: WasmDatabaseLike, data: IssueLabelInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.issue = data.issue;
    values.label = data.label;
    return this._create(db, values);
  }

  /**
   * Update an existing IssueLabel
   * @generated from schema table: IssueLabels
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<IssueLabelInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a IssueLabel
   * @generated from schema table: IssueLabels
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: IssueLabels
   */
  where(filter: IssueLabelFilter): IssueLabelsQueryBuilder<{}> {
    return new IssueLabelsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: IssueLabels
   */
  with<I extends IssueLabelIncludes>(include: I): IssueLabelsQueryBuilder<I> {
    return new IssueLabelsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all IssueLabels
   * @generated from schema table: IssueLabels
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: IssueLabel[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single IssueLabel by ID
   * @generated from schema table: IssueLabels
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: IssueLabel | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: IssueLabel[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: IssueLabel | null) => void): Unsubscribe {
    return this._subscribe(db, id, options, callback);
  }
}

/**
 * Descriptor for the IssueAssignees table (no db instance, db passed at method call time)
 * @generated from schema table: IssueAssignees
 */
export class IssueAssigneesDescriptor extends TableClient<IssueAssignee>
  implements SubscribableAllWithDb<IssueAssignee, IssueAssigneeInsert, Partial<IssueAssigneeInsert>>,
             SubscribableOneWithDb<IssueAssignee, Partial<IssueAssigneeInsert>>,
             MutableWithDb<IssueAssigneeInsert, Partial<IssueAssigneeInsert>> {
  constructor() {
    super(schemaMeta.tables.IssueAssignees, schemaMeta, {
      rows: decodeIssueAssigneeRows,
      delta: decodeIssueAssigneeDelta,
    });
  }

  /**
   * Create a new IssueAssignee
   * @returns The ObjectId of the created row
   * @generated from schema table: IssueAssignees
   */
  create(db: WasmDatabaseLike, data: IssueAssigneeInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.issue = data.issue;
    values.user = data.user;
    return this._create(db, values);
  }

  /**
   * Update an existing IssueAssignee
   * @generated from schema table: IssueAssignees
   */
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<IssueAssigneeInsert>): void {
    this._update(db, id, data as Record<string, unknown>);
  }

  /**
   * Delete a IssueAssignee
   * @generated from schema table: IssueAssignees
   */
  delete(db: WasmDatabaseLike, id: ObjectId): void {
    this._delete(db, id);
  }

  /**
   * Start a query with a filter condition
   * @generated from schema table: IssueAssignees
   */
  where(filter: IssueAssigneeFilter): IssueAssigneesQueryBuilder<{}> {
    return new IssueAssigneesQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   * @generated from schema table: IssueAssignees
   */
  with<I extends IssueAssigneeIncludes>(include: I): IssueAssigneesQueryBuilder<I> {
    return new IssueAssigneesQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to all IssueAssignees
   * @generated from schema table: IssueAssignees
   */
  subscribeAll(db: WasmDatabaseLike, callback: (rows: IssueAssignee[]) => void): Unsubscribe {
    return this._subscribeAll(db, {}, callback);
  }

  /**
   * Subscribe to a single IssueAssignee by ID
   * @generated from schema table: IssueAssignees
   */
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: IssueAssignee | null) => void): Unsubscribe {
    return this._subscribe(db, id, {}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(db: WasmDatabaseLike, options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: IssueAssignee[]) => void): Unsubscribe {
    return this._subscribeAll(db, options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(db: WasmDatabaseLike, id: ObjectId, options: { include?: IncludeSpec }, callback: (row: IssueAssignee | null) => void): Unsubscribe {
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
  issues: new IssuesDescriptor(),
  labels: new LabelsDescriptor(),
  issuelabels: new IssueLabelsDescriptor(),
  issueassignees: new IssueAssigneesDescriptor(),
};

export type App = typeof app;
