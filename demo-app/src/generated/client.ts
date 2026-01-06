// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import { TableClient, type WasmDatabaseLike, type Unsubscribe, type TableDecoder, type BaseWhereInput, type IncludeSpec } from "@jazz/client";
import { schemaMeta } from "./meta.js";
import { decodeUserRows, decodeUserDelta, decodeProjectRows, decodeProjectDelta, decodeIssueRows, decodeIssueDelta, decodeLabelRows, decodeLabelDelta, decodeIssueLabelRows, decodeIssueLabelDelta, decodeIssueAssigneeRows, decodeIssueAssigneeDelta } from "./decoders.js";
import type { ObjectId, User, UserInsert, UserIncludes, UserLoaded, UserFilter, Project, ProjectInsert, ProjectIncludes, ProjectLoaded, ProjectFilter, Issue, IssueInsert, IssueIncludes, IssueLoaded, IssueFilter, Label, LabelInsert, LabelIncludes, LabelLoaded, LabelFilter, IssueLabel, IssueLabelInsert, IssueLabelIncludes, IssueLabelLoaded, IssueLabelFilter, IssueAssignee, IssueAssigneeInsert, IssueAssigneeIncludes, IssueAssigneeLoaded, IssueAssigneeFilter } from "./types.js";

/**
 * Query builder for Users with chainable where/with methods
 */
export class UsersQueryBuilder<I extends UserIncludes = {}> {
  private _client: UsersClient;
  private _where?: UserFilter;
  private _include?: I;

  constructor(client: UsersClient, where?: UserFilter, include?: I) {
    this._client = client;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Users", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   */
  where(filter: UserFilter): UsersQueryBuilder<I> {
    return new UsersQueryBuilder(this._client, filter, this._include);
  }

  /**
   * Specify which refs to include
   */
  with<NewI extends UserIncludes>(include: NewI): UsersQueryBuilder<NewI> {
    return new UsersQueryBuilder(this._client, this._where, include);
  }

  /**
   * Subscribe to all matching Users
   */
  subscribeAll(callback: (rows: UserLoaded<I>[]) => void): Unsubscribe {
    return this._client._subscribeAllInternal(
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: User[]) => void
    );
  }

  /**
   * Subscribe to a single User by ID
   */
  subscribe(id: ObjectId, callback: (row: UserLoaded<I> | null) => void): Unsubscribe {
    return this._client._subscribeInternal(
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: User | null) => void
    );
  }
}

/**
 * Query builder for Projects with chainable where/with methods
 */
export class ProjectsQueryBuilder<I extends ProjectIncludes = {}> {
  private _client: ProjectsClient;
  private _where?: ProjectFilter;
  private _include?: I;

  constructor(client: ProjectsClient, where?: ProjectFilter, include?: I) {
    this._client = client;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Projects", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   */
  where(filter: ProjectFilter): ProjectsQueryBuilder<I> {
    return new ProjectsQueryBuilder(this._client, filter, this._include);
  }

  /**
   * Specify which refs to include
   */
  with<NewI extends ProjectIncludes>(include: NewI): ProjectsQueryBuilder<NewI> {
    return new ProjectsQueryBuilder(this._client, this._where, include);
  }

  /**
   * Subscribe to all matching Projects
   */
  subscribeAll(callback: (rows: ProjectLoaded<I>[]) => void): Unsubscribe {
    return this._client._subscribeAllInternal(
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Project[]) => void
    );
  }

  /**
   * Subscribe to a single Project by ID
   */
  subscribe(id: ObjectId, callback: (row: ProjectLoaded<I> | null) => void): Unsubscribe {
    return this._client._subscribeInternal(
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Project | null) => void
    );
  }
}

/**
 * Query builder for Issues with chainable where/with methods
 */
export class IssuesQueryBuilder<I extends IssueIncludes = {}> {
  private _client: IssuesClient;
  private _where?: IssueFilter;
  private _include?: I;

  constructor(client: IssuesClient, where?: IssueFilter, include?: I) {
    this._client = client;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Issues", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   */
  where(filter: IssueFilter): IssuesQueryBuilder<I> {
    return new IssuesQueryBuilder(this._client, filter, this._include);
  }

  /**
   * Specify which refs to include
   */
  with<NewI extends IssueIncludes>(include: NewI): IssuesQueryBuilder<NewI> {
    return new IssuesQueryBuilder(this._client, this._where, include);
  }

  /**
   * Subscribe to all matching Issues
   */
  subscribeAll(callback: (rows: IssueLoaded<I>[]) => void): Unsubscribe {
    return this._client._subscribeAllInternal(
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Issue[]) => void
    );
  }

  /**
   * Subscribe to a single Issue by ID
   */
  subscribe(id: ObjectId, callback: (row: IssueLoaded<I> | null) => void): Unsubscribe {
    return this._client._subscribeInternal(
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Issue | null) => void
    );
  }
}

/**
 * Query builder for Labels with chainable where/with methods
 */
export class LabelsQueryBuilder<I extends LabelIncludes = {}> {
  private _client: LabelsClient;
  private _where?: LabelFilter;
  private _include?: I;

  constructor(client: LabelsClient, where?: LabelFilter, include?: I) {
    this._client = client;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "Labels", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   */
  where(filter: LabelFilter): LabelsQueryBuilder<I> {
    return new LabelsQueryBuilder(this._client, filter, this._include);
  }

  /**
   * Specify which refs to include
   */
  with<NewI extends LabelIncludes>(include: NewI): LabelsQueryBuilder<NewI> {
    return new LabelsQueryBuilder(this._client, this._where, include);
  }

  /**
   * Subscribe to all matching Labels
   */
  subscribeAll(callback: (rows: LabelLoaded<I>[]) => void): Unsubscribe {
    return this._client._subscribeAllInternal(
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: Label[]) => void
    );
  }

  /**
   * Subscribe to a single Label by ID
   */
  subscribe(id: ObjectId, callback: (row: LabelLoaded<I> | null) => void): Unsubscribe {
    return this._client._subscribeInternal(
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: Label | null) => void
    );
  }
}

/**
 * Query builder for IssueLabels with chainable where/with methods
 */
export class IssueLabelsQueryBuilder<I extends IssueLabelIncludes = {}> {
  private _client: IssueLabelsClient;
  private _where?: IssueLabelFilter;
  private _include?: I;

  constructor(client: IssueLabelsClient, where?: IssueLabelFilter, include?: I) {
    this._client = client;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "IssueLabels", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   */
  where(filter: IssueLabelFilter): IssueLabelsQueryBuilder<I> {
    return new IssueLabelsQueryBuilder(this._client, filter, this._include);
  }

  /**
   * Specify which refs to include
   */
  with<NewI extends IssueLabelIncludes>(include: NewI): IssueLabelsQueryBuilder<NewI> {
    return new IssueLabelsQueryBuilder(this._client, this._where, include);
  }

  /**
   * Subscribe to all matching IssueLabels
   */
  subscribeAll(callback: (rows: IssueLabelLoaded<I>[]) => void): Unsubscribe {
    return this._client._subscribeAllInternal(
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: IssueLabel[]) => void
    );
  }

  /**
   * Subscribe to a single IssueLabel by ID
   */
  subscribe(id: ObjectId, callback: (row: IssueLabelLoaded<I> | null) => void): Unsubscribe {
    return this._client._subscribeInternal(
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: IssueLabel | null) => void
    );
  }
}

/**
 * Query builder for IssueAssignees with chainable where/with methods
 */
export class IssueAssigneesQueryBuilder<I extends IssueAssigneeIncludes = {}> {
  private _client: IssueAssigneesClient;
  private _where?: IssueAssigneeFilter;
  private _include?: I;

  constructor(client: IssueAssigneesClient, where?: IssueAssigneeFilter, include?: I) {
    this._client = client;
    this._where = where;
    this._include = include;
  }

  /**
   * Get a stable key representing this query's options (for React hook deduplication)
   */
  get _queryKey(): string {
    return JSON.stringify({ t: "IssueAssignees", w: this._where, i: this._include });
  }

  /**
   * Add a filter condition
   */
  where(filter: IssueAssigneeFilter): IssueAssigneesQueryBuilder<I> {
    return new IssueAssigneesQueryBuilder(this._client, filter, this._include);
  }

  /**
   * Specify which refs to include
   */
  with<NewI extends IssueAssigneeIncludes>(include: NewI): IssueAssigneesQueryBuilder<NewI> {
    return new IssueAssigneesQueryBuilder(this._client, this._where, include);
  }

  /**
   * Subscribe to all matching IssueAssignees
   */
  subscribeAll(callback: (rows: IssueAssigneeLoaded<I>[]) => void): Unsubscribe {
    return this._client._subscribeAllInternal(
      { where: this._where as BaseWhereInput | undefined, include: this._include as IncludeSpec | undefined },
      callback as (rows: IssueAssignee[]) => void
    );
  }

  /**
   * Subscribe to a single IssueAssignee by ID
   */
  subscribe(id: ObjectId, callback: (row: IssueAssigneeLoaded<I> | null) => void): Unsubscribe {
    return this._client._subscribeInternal(
      id,
      { include: this._include as IncludeSpec | undefined },
      callback as (row: IssueAssignee | null) => void
    );
  }
}

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
    values.avatarColor = data.avatarColor;
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
   * Start a query with a filter condition
   */
  where(filter: UserFilter): UsersQueryBuilder<{}> {
    return new UsersQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   */
  with<I extends UserIncludes>(include: I): UsersQueryBuilder<I> {
    return new UsersQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to a single User by ID
   */
  subscribe(id: ObjectId, callback: (row: User | null) => void): Unsubscribe {
    return this._subscribe(id, {}, callback);
  }

  /**
   * Subscribe to all Users
   */
  subscribeAll(callback: (rows: User[]) => void): Unsubscribe {
    return this._subscribeAll({}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: User[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: User | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }
}

/**
 * Client for the Projects table
 */
export class ProjectsClient extends TableClient<Project> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Projects, schemaMeta, {
      rows: decodeProjectRows,
      delta: decodeProjectDelta,
    });
  }

  /**
   * Create a new Project
   * @returns The ObjectId of the created row
   */
  create(data: ProjectInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.color = data.color;
    if (data.description !== undefined) values.description = data.description;
    return this._create(values);
  }

  /**
   * Update an existing Project
   */
  update(id: ObjectId, data: Partial<ProjectInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a Project
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Start a query with a filter condition
   */
  where(filter: ProjectFilter): ProjectsQueryBuilder<{}> {
    return new ProjectsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   */
  with<I extends ProjectIncludes>(include: I): ProjectsQueryBuilder<I> {
    return new ProjectsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to a single Project by ID
   */
  subscribe(id: ObjectId, callback: (row: Project | null) => void): Unsubscribe {
    return this._subscribe(id, {}, callback);
  }

  /**
   * Subscribe to all Projects
   */
  subscribeAll(callback: (rows: Project[]) => void): Unsubscribe {
    return this._subscribeAll({}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Project[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Project | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }
}

/**
 * Client for the Issues table
 */
export class IssuesClient extends TableClient<Issue> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Issues, schemaMeta, {
      rows: decodeIssueRows,
      delta: decodeIssueDelta,
    });
  }

  /**
   * Create a new Issue
   * @returns The ObjectId of the created row
   */
  create(data: IssueInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.title = data.title;
    if (data.description !== undefined) values.description = data.description;
    values.status = data.status;
    values.priority = data.priority;
    values.project = data.project;
    values.createdAt = data.createdAt;
    values.updatedAt = data.updatedAt;
    return this._create(values);
  }

  /**
   * Update an existing Issue
   */
  update(id: ObjectId, data: Partial<IssueInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a Issue
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Start a query with a filter condition
   */
  where(filter: IssueFilter): IssuesQueryBuilder<{}> {
    return new IssuesQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   */
  with<I extends IssueIncludes>(include: I): IssuesQueryBuilder<I> {
    return new IssuesQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to a single Issue by ID
   */
  subscribe(id: ObjectId, callback: (row: Issue | null) => void): Unsubscribe {
    return this._subscribe(id, {}, callback);
  }

  /**
   * Subscribe to all Issues
   */
  subscribeAll(callback: (rows: Issue[]) => void): Unsubscribe {
    return this._subscribeAll({}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Issue[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Issue | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }
}

/**
 * Client for the Labels table
 */
export class LabelsClient extends TableClient<Label> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.Labels, schemaMeta, {
      rows: decodeLabelRows,
      delta: decodeLabelDelta,
    });
  }

  /**
   * Create a new Label
   * @returns The ObjectId of the created row
   */
  create(data: LabelInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.name = data.name;
    values.color = data.color;
    return this._create(values);
  }

  /**
   * Update an existing Label
   */
  update(id: ObjectId, data: Partial<LabelInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a Label
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Start a query with a filter condition
   */
  where(filter: LabelFilter): LabelsQueryBuilder<{}> {
    return new LabelsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   */
  with<I extends LabelIncludes>(include: I): LabelsQueryBuilder<I> {
    return new LabelsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to a single Label by ID
   */
  subscribe(id: ObjectId, callback: (row: Label | null) => void): Unsubscribe {
    return this._subscribe(id, {}, callback);
  }

  /**
   * Subscribe to all Labels
   */
  subscribeAll(callback: (rows: Label[]) => void): Unsubscribe {
    return this._subscribeAll({}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: Label[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: Label | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }
}

/**
 * Client for the IssueLabels table
 */
export class IssueLabelsClient extends TableClient<IssueLabel> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.IssueLabels, schemaMeta, {
      rows: decodeIssueLabelRows,
      delta: decodeIssueLabelDelta,
    });
  }

  /**
   * Create a new IssueLabel
   * @returns The ObjectId of the created row
   */
  create(data: IssueLabelInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.issue = data.issue;
    values.label = data.label;
    return this._create(values);
  }

  /**
   * Update an existing IssueLabel
   */
  update(id: ObjectId, data: Partial<IssueLabelInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a IssueLabel
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Start a query with a filter condition
   */
  where(filter: IssueLabelFilter): IssueLabelsQueryBuilder<{}> {
    return new IssueLabelsQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   */
  with<I extends IssueLabelIncludes>(include: I): IssueLabelsQueryBuilder<I> {
    return new IssueLabelsQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to a single IssueLabel by ID
   */
  subscribe(id: ObjectId, callback: (row: IssueLabel | null) => void): Unsubscribe {
    return this._subscribe(id, {}, callback);
  }

  /**
   * Subscribe to all IssueLabels
   */
  subscribeAll(callback: (rows: IssueLabel[]) => void): Unsubscribe {
    return this._subscribeAll({}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: IssueLabel[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: IssueLabel | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }
}

/**
 * Client for the IssueAssignees table
 */
export class IssueAssigneesClient extends TableClient<IssueAssignee> {
  constructor(db: WasmDatabaseLike) {
    super(db, schemaMeta.tables.IssueAssignees, schemaMeta, {
      rows: decodeIssueAssigneeRows,
      delta: decodeIssueAssigneeDelta,
    });
  }

  /**
   * Create a new IssueAssignee
   * @returns The ObjectId of the created row
   */
  create(data: IssueAssigneeInsert): ObjectId {
    const values: Record<string, unknown> = {};
    values.issue = data.issue;
    values.user = data.user;
    return this._create(values);
  }

  /**
   * Update an existing IssueAssignee
   */
  update(id: ObjectId, data: Partial<IssueAssigneeInsert>): void {
    this._update(id, data as Record<string, unknown>);
  }

  /**
   * Delete a IssueAssignee
   */
  delete(id: ObjectId): void {
    this._delete(id);
  }

  /**
   * Start a query with a filter condition
   */
  where(filter: IssueAssigneeFilter): IssueAssigneesQueryBuilder<{}> {
    return new IssueAssigneesQueryBuilder(this, filter, undefined);
  }

  /**
   * Start a query with includes
   */
  with<I extends IssueAssigneeIncludes>(include: I): IssueAssigneesQueryBuilder<I> {
    return new IssueAssigneesQueryBuilder(this, undefined, include);
  }

  /**
   * Subscribe to a single IssueAssignee by ID
   */
  subscribe(id: ObjectId, callback: (row: IssueAssignee | null) => void): Unsubscribe {
    return this._subscribe(id, {}, callback);
  }

  /**
   * Subscribe to all IssueAssignees
   */
  subscribeAll(callback: (rows: IssueAssignee[]) => void): Unsubscribe {
    return this._subscribeAll({}, callback);
  }

  /** @internal Used by query builder */
  _subscribeAllInternal(options: { where?: BaseWhereInput; include?: IncludeSpec }, callback: (rows: IssueAssignee[]) => void): Unsubscribe {
    return this._subscribeAll(options, callback);
  }

  /** @internal Used by query builder */
  _subscribeInternal(id: ObjectId, options: { include?: IncludeSpec }, callback: (row: IssueAssignee | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback);
  }
}

/**
 * Typed database interface
 */
export interface Database {
  /** Raw WASM database for direct SQL access */
  raw: WasmDatabaseLike;
  users: UsersClient;
  projects: ProjectsClient;
  issues: IssuesClient;
  labels: LabelsClient;
  issuelabels: IssueLabelsClient;
  issueassignees: IssueAssigneesClient;
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
    projects: new ProjectsClient(wasmDb),
    issues: new IssuesClient(wasmDb),
    labels: new LabelsClient(wasmDb),
    issuelabels: new IssueLabelsClient(wasmDb),
    issueassignees: new IssueAssigneesClient(wasmDb),
  };
}
