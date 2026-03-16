// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface User {
  id: string;
  name: string;
  friends: string[];
}

export interface Project {
  id: string;
  name: string;
}

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  tags: string[];
  project: string;
  owner?: string;
  assignees: string[];
}

export interface UserInit {
  name: string;
  friends: string[];
}

export interface ProjectInit {
  name: string;
}

export interface TodoInit {
  title: string;
  done: boolean;
  tags: string[];
  project: string;
  owner?: string;
  assignees: string[];
}

export interface UserWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  friends?: string[] | { eq?: string[]; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ProjectWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  tags?: string[] | { eq?: string[]; contains?: string };
  project?: string | { eq?: string; ne?: string };
  owner?: string | { eq?: string; ne?: string; isNull?: boolean };
  assignees?: string[] | { eq?: string[]; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyUserQueryBuilder<T = any> = { readonly _table: "users" } & QueryBuilder<T>;
type AnyProjectQueryBuilder<T = any> = { readonly _table: "projects" } & QueryBuilder<T>;
type AnyTodoQueryBuilder<T = any> = { readonly _table: "todos" } & QueryBuilder<T>;

export interface UserInclude {
  friends?: true | UserInclude | AnyUserQueryBuilder<any>;
  usersViaFriends?: true | UserInclude | AnyUserQueryBuilder<any>;
  todosViaOwner?: true | TodoInclude | AnyTodoQueryBuilder<any>;
  todosViaAssignees?: true | TodoInclude | AnyTodoQueryBuilder<any>;
}

export interface ProjectInclude {
  todosViaProject?: true | TodoInclude | AnyTodoQueryBuilder<any>;
}

export interface TodoInclude {
  project?: true | ProjectInclude | AnyProjectQueryBuilder<any>;
  owner?: true | UserInclude | AnyUserQueryBuilder<any>;
  assignees?: true | UserInclude | AnyUserQueryBuilder<any>;
}

export type UserIncludedRelations<I extends UserInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?: K extends "friends"
    ? NonNullable<I["friends"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? User[]
        : RelationInclude extends AnyUserQueryBuilder<infer QueryRow>
          ? QueryRow[]
          : RelationInclude extends UserInclude
            ? UserWithIncludes<RelationInclude, false>[]
            : never
      : never
    : K extends "usersViaFriends"
      ? NonNullable<I["usersViaFriends"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? User[]
          : RelationInclude extends AnyUserQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends UserInclude
              ? UserWithIncludes<RelationInclude, false>[]
              : never
        : never
      : K extends "todosViaOwner"
        ? NonNullable<I["todosViaOwner"]> extends infer RelationInclude
          ? RelationInclude extends true
            ? Todo[]
            : RelationInclude extends AnyTodoQueryBuilder<infer QueryRow>
              ? QueryRow[]
              : RelationInclude extends TodoInclude
                ? TodoWithIncludes<RelationInclude, false>[]
                : never
          : never
        : K extends "todosViaAssignees"
          ? NonNullable<I["todosViaAssignees"]> extends infer RelationInclude
            ? RelationInclude extends true
              ? Todo[]
              : RelationInclude extends AnyTodoQueryBuilder<infer QueryRow>
                ? QueryRow[]
                : RelationInclude extends TodoInclude
                  ? TodoWithIncludes<RelationInclude, false>[]
                  : never
            : never
          : never;
};

export type ProjectIncludedRelations<I extends ProjectInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?: K extends "todosViaProject"
    ? NonNullable<I["todosViaProject"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? Todo[]
        : RelationInclude extends AnyTodoQueryBuilder<infer QueryRow>
          ? QueryRow[]
          : RelationInclude extends TodoInclude
            ? TodoWithIncludes<RelationInclude, false>[]
            : never
      : never
    : never;
};

export type TodoIncludedRelations<I extends TodoInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?: K extends "project"
    ? NonNullable<I["project"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? R extends true
          ? Project
          : Project | undefined
        : RelationInclude extends AnyProjectQueryBuilder<infer QueryRow>
          ? R extends true
            ? QueryRow
            : QueryRow | undefined
          : RelationInclude extends ProjectInclude
            ? R extends true
              ? ProjectWithIncludes<RelationInclude, false>
              : ProjectWithIncludes<RelationInclude, false> | undefined
            : never
      : never
    : K extends "owner"
      ? NonNullable<I["owner"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? User | undefined
          : RelationInclude extends AnyUserQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends UserInclude
              ? UserWithIncludes<RelationInclude, false> | undefined
              : never
        : never
      : K extends "assignees"
        ? NonNullable<I["assignees"]> extends infer RelationInclude
          ? RelationInclude extends true
            ? User[]
            : RelationInclude extends AnyUserQueryBuilder<infer QueryRow>
              ? QueryRow[]
              : RelationInclude extends UserInclude
                ? UserWithIncludes<RelationInclude, false>[]
                : never
          : never
        : never;
};

export interface UserRelations {
  friends: User[];
  usersViaFriends: User[];
  todosViaOwner: Todo[];
  todosViaAssignees: Todo[];
}

export interface ProjectRelations {
  todosViaProject: Todo[];
}

export interface TodoRelations {
  project: Project | undefined;
  owner: User | undefined;
  assignees: User[];
}

export type UserWithIncludes<I extends UserInclude = {}, R extends boolean = false> = Omit<
  User,
  Extract<keyof I, keyof User>
> &
  UserIncludedRelations<I, R>;

export type ProjectWithIncludes<I extends ProjectInclude = {}, R extends boolean = false> = Omit<
  Project,
  Extract<keyof I, keyof Project>
> &
  ProjectIncludedRelations<I, R>;

export type TodoWithIncludes<I extends TodoInclude = {}, R extends boolean = false> = Omit<
  Todo,
  Extract<keyof I, keyof Todo>
> &
  TodoIncludedRelations<I, R>;

export type UserSelectableColumn = keyof User | PermissionIntrospectionColumn | "*";
export type UserOrderableColumn = keyof User | PermissionIntrospectionColumn;

export type UserSelected<S extends UserSelectableColumn = keyof User> = "*" extends S
  ? User
  : Pick<User, Extract<S | "id", keyof User>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type UserSelectedWithIncludes<
  I extends UserInclude = {},
  S extends UserSelectableColumn = keyof User,
  R extends boolean = false,
> = Omit<UserSelected<S>, Extract<keyof I, keyof UserSelected<S>>> & UserIncludedRelations<I, R>;

export type ProjectSelectableColumn = keyof Project | PermissionIntrospectionColumn | "*";
export type ProjectOrderableColumn = keyof Project | PermissionIntrospectionColumn;

export type ProjectSelected<S extends ProjectSelectableColumn = keyof Project> = "*" extends S
  ? Project
  : Pick<Project, Extract<S | "id", keyof Project>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ProjectSelectedWithIncludes<
  I extends ProjectInclude = {},
  S extends ProjectSelectableColumn = keyof Project,
  R extends boolean = false,
> = Omit<ProjectSelected<S>, Extract<keyof I, keyof ProjectSelected<S>>> &
  ProjectIncludedRelations<I, R>;

export type TodoSelectableColumn = keyof Todo | PermissionIntrospectionColumn | "*";
export type TodoOrderableColumn = keyof Todo | PermissionIntrospectionColumn;

export type TodoSelected<S extends TodoSelectableColumn = keyof Todo> = "*" extends S
  ? Todo
  : Pick<Todo, Extract<S | "id", keyof Todo>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type TodoSelectedWithIncludes<
  I extends TodoInclude = {},
  S extends TodoSelectableColumn = keyof Todo,
  R extends boolean = false,
> = Omit<TodoSelected<S>, Extract<keyof I, keyof TodoSelected<S>>> & TodoIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  users: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "friends",
        column_type: {
          type: "Array",
          element: {
            type: "Uuid",
          },
        },
        nullable: false,
        references: "users",
      },
    ],
  },
  projects: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
    ],
  },
  todos: {
    columns: [
      {
        name: "title",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "done",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
      {
        name: "tags",
        column_type: {
          type: "Array",
          element: {
            type: "Text",
          },
        },
        nullable: false,
      },
      {
        name: "project",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "projects",
      },
      {
        name: "owner",
        column_type: {
          type: "Uuid",
        },
        nullable: true,
        references: "users",
      },
      {
        name: "assignees",
        column_type: {
          type: "Array",
          element: {
            type: "Uuid",
          },
        },
        nullable: false,
        references: "users",
      },
    ],
    policies: {
      select: {
        using: {
          type: "True",
        },
      },
      insert: {
        with_check: {
          type: "True",
        },
      },
      update: {
        using: {
          type: "Cmp",
          column: "done",
          op: "Eq",
          value: {
            type: "Literal",
            value: {
              type: "Boolean",
              value: false,
            },
          },
        },
        with_check: {
          type: "True",
        },
      },
      delete: {
        using: {
          type: "Cmp",
          column: "done",
          op: "Eq",
          value: {
            type: "Literal",
            value: {
              type: "Boolean",
              value: false,
            },
          },
        },
      },
    },
  },
};

export class UserQueryBuilder<
  I extends UserInclude = {},
  S extends UserSelectableColumn = keyof User,
  R extends boolean = false,
> implements QueryBuilder<UserSelectedWithIncludes<I, S, R>> {
  readonly _table = "users";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: UserSelectedWithIncludes<I, S, R>;
  readonly _initType!: UserInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<UserInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: UserWhereInput): UserQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends UserSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): UserQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends UserInclude>(relations: NewI): UserQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): UserQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: UserOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): UserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): UserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): UserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(
    relation: "friends" | "usersViaFriends" | "todosViaOwner" | "todosViaAssignees",
  ): UserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: UserWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): UserQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends UserInclude = I,
    CloneS extends UserSelectableColumn = S,
    CloneR extends boolean = R,
  >(): UserQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new UserQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class ProjectQueryBuilder<
  I extends ProjectInclude = {},
  S extends ProjectSelectableColumn = keyof Project,
  R extends boolean = false,
> implements QueryBuilder<ProjectSelectedWithIncludes<I, S, R>> {
  readonly _table = "projects";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ProjectSelectedWithIncludes<I, S, R>;
  readonly _initType!: ProjectInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProjectInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ProjectWhereInput): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends ProjectSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): ProjectQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProjectInclude>(relations: NewI): ProjectQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ProjectQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: ProjectOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "todosViaProject"): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProjectWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ProjectQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ProjectInclude = I,
    CloneS extends ProjectSelectableColumn = S,
    CloneR extends boolean = R,
  >(): ProjectQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ProjectQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class TodoQueryBuilder<
  I extends TodoInclude = {},
  S extends TodoSelectableColumn = keyof Todo,
  R extends boolean = false,
> implements QueryBuilder<TodoSelectedWithIncludes<I, S, R>> {
  readonly _table = "todos";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: TodoSelectedWithIncludes<I, S, R>;
  readonly _initType!: TodoInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<TodoInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: TodoWhereInput): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends TodoSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): TodoQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends TodoInclude>(relations: NewI): TodoQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): TodoQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: TodoOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "project" | "owner" | "assignees"): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: TodoWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): TodoQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends TodoInclude = I,
    CloneS extends TodoSelectableColumn = S,
    CloneR extends boolean = R,
  >(): TodoQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new TodoQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  users: UserQueryBuilder;
  projects: ProjectQueryBuilder;
  todos: TodoQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  users: new UserQueryBuilder(),
  projects: new ProjectQueryBuilder(),
  todos: new TodoQueryBuilder(),
  wasmSchema,
};
