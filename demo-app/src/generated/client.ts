// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import { TableClient, type WasmDatabaseLike, type Unsubscribe, type TableDecoder, type BaseWhereInput, type IncludeSpec } from "@jazz/client";
import { schemaMeta } from "./meta.js";
import { decodeUserRows, decodeUserDelta, decodeProjectRows, decodeProjectDelta, decodeIssueRows, decodeIssueDelta, decodeLabelRows, decodeLabelDelta, decodeIssueLabelRows, decodeIssueLabelDelta, decodeIssueAssigneeRows, decodeIssueAssigneeDelta } from "./decoders.js";
import type { ObjectId, User, UserInsert, UserIncludes, UserLoaded, UserFilter, Project, ProjectInsert, ProjectIncludes, ProjectLoaded, ProjectFilter, Issue, IssueInsert, IssueIncludes, IssueLoaded, IssueFilter, Label, LabelInsert, LabelIncludes, LabelLoaded, LabelFilter, IssueLabel, IssueLabelInsert, IssueLabelIncludes, IssueLabelLoaded, IssueLabelFilter, IssueAssignee, IssueAssigneeInsert, IssueAssigneeIncludes, IssueAssigneeLoaded, IssueAssigneeFilter } from "./types.js";

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
   * Subscribe to a single User by ID
   */
  subscribe<I extends UserIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: UserLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: User | null) => void);
  }

  /**
   * Subscribe to all Users matching a filter
   */
  subscribeAll<I extends UserIncludes = {}>(options: { where?: UserFilter; include?: I }, callback: (rows: UserLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options as { where?: BaseWhereInput; include?: IncludeSpec }, callback as (rows: User[]) => void);
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
   * Subscribe to a single Project by ID
   */
  subscribe<I extends ProjectIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: ProjectLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: Project | null) => void);
  }

  /**
   * Subscribe to all Projects matching a filter
   */
  subscribeAll<I extends ProjectIncludes = {}>(options: { where?: ProjectFilter; include?: I }, callback: (rows: ProjectLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options as { where?: BaseWhereInput; include?: IncludeSpec }, callback as (rows: Project[]) => void);
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
   * Subscribe to a single Issue by ID
   */
  subscribe<I extends IssueIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: IssueLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: Issue | null) => void);
  }

  /**
   * Subscribe to all Issues matching a filter
   */
  subscribeAll<I extends IssueIncludes = {}>(options: { where?: IssueFilter; include?: I }, callback: (rows: IssueLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options as { where?: BaseWhereInput; include?: IncludeSpec }, callback as (rows: Issue[]) => void);
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
   * Subscribe to a single Label by ID
   */
  subscribe<I extends LabelIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: LabelLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: Label | null) => void);
  }

  /**
   * Subscribe to all Labels matching a filter
   */
  subscribeAll<I extends LabelIncludes = {}>(options: { where?: LabelFilter; include?: I }, callback: (rows: LabelLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options as { where?: BaseWhereInput; include?: IncludeSpec }, callback as (rows: Label[]) => void);
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
   * Subscribe to a single IssueLabel by ID
   */
  subscribe<I extends IssueLabelIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: IssueLabelLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: IssueLabel | null) => void);
  }

  /**
   * Subscribe to all IssueLabels matching a filter
   */
  subscribeAll<I extends IssueLabelIncludes = {}>(options: { where?: IssueLabelFilter; include?: I }, callback: (rows: IssueLabelLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options as { where?: BaseWhereInput; include?: IncludeSpec }, callback as (rows: IssueLabel[]) => void);
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
   * Subscribe to a single IssueAssignee by ID
   */
  subscribe<I extends IssueAssigneeIncludes = {}>(id: ObjectId, options: { include?: I }, callback: (row: IssueAssigneeLoaded<I> | null) => void): Unsubscribe {
    return this._subscribe(id, options, callback as (row: IssueAssignee | null) => void);
  }

  /**
   * Subscribe to all IssueAssignees matching a filter
   */
  subscribeAll<I extends IssueAssigneeIncludes = {}>(options: { where?: IssueAssigneeFilter; include?: I }, callback: (rows: IssueAssigneeLoaded<I>[]) => void): Unsubscribe {
    return this._subscribeAll(options as { where?: BaseWhereInput; include?: IncludeSpec }, callback as (rows: IssueAssignee[]) => void);
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
