// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import type { StringFilter, BigIntFilter, NumberFilter, BoolFilter, RelationFilter, BaseWhereInput } from "@jazz/schema/runtime";

/** ObjectId is a 128-bit unique identifier (UUIDv7) represented as a Base32 string */
export type ObjectId = string;

/** Base interface for all Groove rows */
export interface GrooveRow {
  id: ObjectId;
}

// === Includes types (specify which refs to load) ===

export type UserIncludes = {
  IssueAssignees?: true | IssueAssigneeIncludes;
};

export type ProjectIncludes = {
  Issues?: true | IssueIncludes;
};

export type IssueIncludes = {
  project?: true | ProjectIncludes;
  IssueLabels?: true | IssueLabelIncludes;
  IssueAssignees?: true | IssueAssigneeIncludes;
};

export type LabelIncludes = {
  IssueLabels?: true | IssueLabelIncludes;
};

export type IssueLabelIncludes = {
  issue?: true | IssueIncludes;
  label?: true | LabelIncludes;
};

export type IssueAssigneeIncludes = {
  issue?: true | IssueIncludes;
  user?: true | UserIncludes;
};

// === Filter types (Prisma-style filters) ===

export interface UserFilter extends BaseWhereInput {
  AND?: UserFilter | UserFilter[];
  OR?: UserFilter[];
  NOT?: UserFilter | UserFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  email?: string | StringFilter;
  avatarColor?: string | StringFilter;
  /** Filter by related IssueAssignees */
  IssueAssignees?: RelationFilter<IssueAssigneeFilter>;
}

export interface ProjectFilter extends BaseWhereInput {
  AND?: ProjectFilter | ProjectFilter[];
  OR?: ProjectFilter[];
  NOT?: ProjectFilter | ProjectFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  color?: string | StringFilter;
  description?: string | StringFilter | null;
  /** Filter by related Issues */
  Issues?: RelationFilter<IssueFilter>;
}

export interface IssueFilter extends BaseWhereInput {
  AND?: IssueFilter | IssueFilter[];
  OR?: IssueFilter[];
  NOT?: IssueFilter | IssueFilter[];
  id?: string | StringFilter;
  title?: string | StringFilter;
  description?: string | StringFilter | null;
  status?: string | StringFilter;
  priority?: string | StringFilter;
  project?: string | StringFilter;
  createdAt?: bigint | BigIntFilter;
  updatedAt?: bigint | BigIntFilter;
  /** Filter by related IssueLabels */
  IssueLabels?: RelationFilter<IssueLabelFilter>;
  /** Filter by related IssueAssignees */
  IssueAssignees?: RelationFilter<IssueAssigneeFilter>;
}

export interface LabelFilter extends BaseWhereInput {
  AND?: LabelFilter | LabelFilter[];
  OR?: LabelFilter[];
  NOT?: LabelFilter | LabelFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  color?: string | StringFilter;
  /** Filter by related IssueLabels */
  IssueLabels?: RelationFilter<IssueLabelFilter>;
}

export interface IssueLabelFilter extends BaseWhereInput {
  AND?: IssueLabelFilter | IssueLabelFilter[];
  OR?: IssueLabelFilter[];
  NOT?: IssueLabelFilter | IssueLabelFilter[];
  id?: string | StringFilter;
  issue?: string | StringFilter;
  label?: string | StringFilter;
}

export interface IssueAssigneeFilter extends BaseWhereInput {
  AND?: IssueAssigneeFilter | IssueAssigneeFilter[];
  OR?: IssueAssigneeFilter[];
  NOT?: IssueAssigneeFilter | IssueAssigneeFilter[];
  id?: string | StringFilter;
  issue?: string | StringFilter;
  user?: string | StringFilter;
}

// === Row types ===

/** User row from the Users table */
export interface User extends GrooveRow {
  name: string;
  email: string;
  avatarColor: string;
}

/** Data for inserting a new User */
export interface UserInsert {
  name: string;
  email: string;
  avatarColor: string;
}

/** User with refs/reverse refs resolved based on includes parameter I */
export type UserWith<I extends UserIncludes = {}> = {
  id: ObjectId;
  name: string;
  email: string;
  avatarColor: string;
}
  & ('IssueAssignees' extends keyof I
    ? I['IssueAssignees'] extends true
      ? { IssueAssignees: IssueAssignee[] }
      : I['IssueAssignees'] extends object
        ? { IssueAssignees: IssueAssigneeWith<I['IssueAssignees'] & IssueAssigneeIncludes>[] }
        : {}
    : {})
;

/** Project row from the Projects table */
export interface Project extends GrooveRow {
  name: string;
  color: string;
  description: string | null;
}

/** Data for inserting a new Project */
export interface ProjectInsert {
  name: string;
  color: string;
  description?: string | null;
}

/** Project with refs/reverse refs resolved based on includes parameter I */
export type ProjectWith<I extends ProjectIncludes = {}> = {
  id: ObjectId;
  name: string;
  color: string;
  description: string | null;
}
  & ('Issues' extends keyof I
    ? I['Issues'] extends true
      ? { Issues: Issue[] }
      : I['Issues'] extends object
        ? { Issues: IssueWith<I['Issues'] & IssueIncludes>[] }
        : {}
    : {})
;

/** Issue row from the Issues table */
export interface Issue extends GrooveRow {
  title: string;
  description: string | null;
  status: string;
  priority: string;
  project: ObjectId;
  createdAt: bigint;
  updatedAt: bigint;
}

/** Data for inserting a new Issue */
export interface IssueInsert {
  title: string;
  description?: string | null;
  status: string;
  priority: string;
  project: ObjectId | Project;
  createdAt: bigint;
  updatedAt: bigint;
}

/** Issue with refs/reverse refs resolved based on includes parameter I */
export type IssueWith<I extends IssueIncludes = {}> = {
  id: ObjectId;
  title: string;
  description: string | null;
  status: string;
  priority: string;
  project: 'project' extends keyof I
    ? I['project'] extends true
      ? Project
      : I['project'] extends object
        ? ProjectWith<I['project'] & ProjectIncludes>
        : ObjectId
    : ObjectId;
  createdAt: bigint;
  updatedAt: bigint;
}
  & ('IssueLabels' extends keyof I
    ? I['IssueLabels'] extends true
      ? { IssueLabels: IssueLabel[] }
      : I['IssueLabels'] extends object
        ? { IssueLabels: IssueLabelWith<I['IssueLabels'] & IssueLabelIncludes>[] }
        : {}
    : {})
  & ('IssueAssignees' extends keyof I
    ? I['IssueAssignees'] extends true
      ? { IssueAssignees: IssueAssignee[] }
      : I['IssueAssignees'] extends object
        ? { IssueAssignees: IssueAssigneeWith<I['IssueAssignees'] & IssueAssigneeIncludes>[] }
        : {}
    : {})
;

/** Label row from the Labels table */
export interface Label extends GrooveRow {
  name: string;
  color: string;
}

/** Data for inserting a new Label */
export interface LabelInsert {
  name: string;
  color: string;
}

/** Label with refs/reverse refs resolved based on includes parameter I */
export type LabelWith<I extends LabelIncludes = {}> = {
  id: ObjectId;
  name: string;
  color: string;
}
  & ('IssueLabels' extends keyof I
    ? I['IssueLabels'] extends true
      ? { IssueLabels: IssueLabel[] }
      : I['IssueLabels'] extends object
        ? { IssueLabels: IssueLabelWith<I['IssueLabels'] & IssueLabelIncludes>[] }
        : {}
    : {})
;

/** IssueLabel row from the IssueLabels table */
export interface IssueLabel extends GrooveRow {
  issue: ObjectId;
  label: ObjectId;
}

/** Data for inserting a new IssueLabel */
export interface IssueLabelInsert {
  issue: ObjectId | Issue;
  label: ObjectId | Label;
}

/** IssueLabel with refs/reverse refs resolved based on includes parameter I */
export type IssueLabelWith<I extends IssueLabelIncludes = {}> = {
  id: ObjectId;
  issue: 'issue' extends keyof I
    ? I['issue'] extends true
      ? Issue
      : I['issue'] extends object
        ? IssueWith<I['issue'] & IssueIncludes>
        : ObjectId
    : ObjectId;
  label: 'label' extends keyof I
    ? I['label'] extends true
      ? Label
      : I['label'] extends object
        ? LabelWith<I['label'] & LabelIncludes>
        : ObjectId
    : ObjectId;
}
;

/** IssueAssignee row from the IssueAssignees table */
export interface IssueAssignee extends GrooveRow {
  issue: ObjectId;
  user: ObjectId;
}

/** Data for inserting a new IssueAssignee */
export interface IssueAssigneeInsert {
  issue: ObjectId | Issue;
  user: ObjectId | User;
}

/** IssueAssignee with refs/reverse refs resolved based on includes parameter I */
export type IssueAssigneeWith<I extends IssueAssigneeIncludes = {}> = {
  id: ObjectId;
  issue: 'issue' extends keyof I
    ? I['issue'] extends true
      ? Issue
      : I['issue'] extends object
        ? IssueWith<I['issue'] & IssueIncludes>
        : ObjectId
    : ObjectId;
  user: 'user' extends keyof I
    ? I['user'] extends true
      ? User
      : I['user'] extends object
        ? UserWith<I['user'] & UserIncludes>
        : ObjectId
    : ObjectId;
}
;
