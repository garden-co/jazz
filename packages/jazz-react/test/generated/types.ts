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
  Projects?: true | ProjectIncludes;
  Tasks?: true | TaskIncludes;
};

export type ProjectIncludes = {
  owner?: true | UserIncludes;
  Tasks?: true | TaskIncludes;
};

export type TaskIncludes = {
  project?: true | ProjectIncludes;
  assignee?: true | UserIncludes;
  TaskTags?: true | TaskTagIncludes;
};

export type TagIncludes = {
  TaskTags?: true | TaskTagIncludes;
};

export type TaskTagIncludes = {
  task?: true | TaskIncludes;
  tag?: true | TagIncludes;
};

// === Filter types (Prisma-style filters) ===

export interface UserFilter extends BaseWhereInput {
  AND?: UserFilter | UserFilter[];
  OR?: UserFilter[];
  NOT?: UserFilter | UserFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  email?: string | StringFilter;
  avatar?: string | StringFilter | null;
  age?: bigint | BigIntFilter;
  score?: number | NumberFilter;
  isAdmin?: boolean | BoolFilter;
  /** Filter by related Projects */
  Projects?: RelationFilter<ProjectFilter>;
  /** Filter by related Tasks */
  Tasks?: RelationFilter<TaskFilter>;
}

export interface ProjectFilter extends BaseWhereInput {
  AND?: ProjectFilter | ProjectFilter[];
  OR?: ProjectFilter[];
  NOT?: ProjectFilter | ProjectFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  description?: string | StringFilter | null;
  owner?: string | StringFilter;
  color?: string | StringFilter;
  /** Filter by related Tasks */
  Tasks?: RelationFilter<TaskFilter>;
}

export interface TaskFilter extends BaseWhereInput {
  AND?: TaskFilter | TaskFilter[];
  OR?: TaskFilter[];
  NOT?: TaskFilter | TaskFilter[];
  id?: string | StringFilter;
  title?: string | StringFilter;
  description?: string | StringFilter | null;
  status?: string | StringFilter;
  priority?: string | StringFilter;
  project?: string | StringFilter;
  assignee?: string | StringFilter | null;
  createdAt?: bigint | BigIntFilter;
  updatedAt?: bigint | BigIntFilter;
  isCompleted?: boolean | BoolFilter;
  /** Filter by related TaskTags */
  TaskTags?: RelationFilter<TaskTagFilter>;
}

export interface TagFilter extends BaseWhereInput {
  AND?: TagFilter | TagFilter[];
  OR?: TagFilter[];
  NOT?: TagFilter | TagFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  color?: string | StringFilter;
  /** Filter by related TaskTags */
  TaskTags?: RelationFilter<TaskTagFilter>;
}

export interface TaskTagFilter extends BaseWhereInput {
  AND?: TaskTagFilter | TaskTagFilter[];
  OR?: TaskTagFilter[];
  NOT?: TaskTagFilter | TaskTagFilter[];
  id?: string | StringFilter;
  task?: string | StringFilter;
  tag?: string | StringFilter;
}

// === Row types ===

/** User row from the Users table */
export interface User extends GrooveRow {
  name: string;
  email: string;
  avatar: string | null;
  age: bigint;
  score: number;
  isAdmin: boolean;
}

/** Data for inserting a new User */
export interface UserInsert {
  name: string;
  email: string;
  avatar?: string | null;
  age: bigint;
  score: number;
  isAdmin: boolean;
}

/** User with refs/reverse refs resolved based on includes parameter I */
export type UserWith<I extends UserIncludes = {}> = {
  id: ObjectId;
  name: string;
  email: string;
  avatar: string | null;
  age: bigint;
  score: number;
  isAdmin: boolean;
}
  & ('Projects' extends keyof I
    ? I['Projects'] extends true
      ? { Projects: Project[] }
      : I['Projects'] extends object
        ? { Projects: ProjectWith<I['Projects'] & ProjectIncludes>[] }
        : {}
    : {})
  & ('Tasks' extends keyof I
    ? I['Tasks'] extends true
      ? { Tasks: Task[] }
      : I['Tasks'] extends object
        ? { Tasks: TaskWith<I['Tasks'] & TaskIncludes>[] }
        : {}
    : {})
;

/** Project row from the Projects table */
export interface Project extends GrooveRow {
  name: string;
  description: string | null;
  owner: ObjectId;
  color: string;
}

/** Data for inserting a new Project */
export interface ProjectInsert {
  name: string;
  description?: string | null;
  owner: ObjectId | User;
  color: string;
}

/** Project with refs/reverse refs resolved based on includes parameter I */
export type ProjectWith<I extends ProjectIncludes = {}> = {
  id: ObjectId;
  name: string;
  description: string | null;
  owner: 'owner' extends keyof I
    ? I['owner'] extends true
      ? User
      : I['owner'] extends object
        ? UserWith<I['owner'] & UserIncludes>
        : ObjectId
    : ObjectId;
  color: string;
}
  & ('Tasks' extends keyof I
    ? I['Tasks'] extends true
      ? { Tasks: Task[] }
      : I['Tasks'] extends object
        ? { Tasks: TaskWith<I['Tasks'] & TaskIncludes>[] }
        : {}
    : {})
;

/** Task row from the Tasks table */
export interface Task extends GrooveRow {
  title: string;
  description: string | null;
  status: string;
  priority: string;
  project: ObjectId;
  assignee: ObjectId | null;
  createdAt: bigint;
  updatedAt: bigint;
  isCompleted: boolean;
}

/** Data for inserting a new Task */
export interface TaskInsert {
  title: string;
  description?: string | null;
  status: string;
  priority: string;
  project: ObjectId | Project;
  assignee?: ObjectId | User | null;
  createdAt: bigint;
  updatedAt: bigint;
  isCompleted: boolean;
}

/** Task with refs/reverse refs resolved based on includes parameter I */
export type TaskWith<I extends TaskIncludes = {}> = {
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
  assignee: 'assignee' extends keyof I
    ? I['assignee'] extends true
      ? User | null
      : I['assignee'] extends object
        ? UserWith<I['assignee'] & UserIncludes> | null
        : ObjectId | null
    : ObjectId | null;
  createdAt: bigint;
  updatedAt: bigint;
  isCompleted: boolean;
}
  & ('TaskTags' extends keyof I
    ? I['TaskTags'] extends true
      ? { TaskTags: TaskTag[] }
      : I['TaskTags'] extends object
        ? { TaskTags: TaskTagWith<I['TaskTags'] & TaskTagIncludes>[] }
        : {}
    : {})
;

/** Tag row from the Tags table */
export interface Tag extends GrooveRow {
  name: string;
  color: string;
}

/** Data for inserting a new Tag */
export interface TagInsert {
  name: string;
  color: string;
}

/** Tag with refs/reverse refs resolved based on includes parameter I */
export type TagWith<I extends TagIncludes = {}> = {
  id: ObjectId;
  name: string;
  color: string;
}
  & ('TaskTags' extends keyof I
    ? I['TaskTags'] extends true
      ? { TaskTags: TaskTag[] }
      : I['TaskTags'] extends object
        ? { TaskTags: TaskTagWith<I['TaskTags'] & TaskTagIncludes>[] }
        : {}
    : {})
;

/** TaskTag row from the TaskTags table */
export interface TaskTag extends GrooveRow {
  task: ObjectId;
  tag: ObjectId;
}

/** Data for inserting a new TaskTag */
export interface TaskTagInsert {
  task: ObjectId | Task;
  tag: ObjectId | Tag;
}

/** TaskTag with refs/reverse refs resolved based on includes parameter I */
export type TaskTagWith<I extends TaskTagIncludes = {}> = {
  id: ObjectId;
  task: 'task' extends keyof I
    ? I['task'] extends true
      ? Task
      : I['task'] extends object
        ? TaskWith<I['task'] & TaskIncludes>
        : ObjectId
    : ObjectId;
  tag: 'tag' extends keyof I
    ? I['tag'] extends true
      ? Tag
      : I['tag'] extends object
        ? TagWith<I['tag'] & TagIncludes>
        : ObjectId
    : ObjectId;
}
;
