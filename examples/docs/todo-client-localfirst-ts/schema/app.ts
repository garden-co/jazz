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

export interface Project {
  id: string;
  name: string;
}

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  parent?: string;
  project?: string;
}

export interface ProjectInit {
  name: string;
}

export interface TodoInit {
  title: string;
  done: boolean;
  description?: string;
  parent?: string;
  project?: string;
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
  description?: string | { eq?: string; ne?: string; contains?: string };
  parent?: string | { eq?: string; ne?: string; isNull?: boolean };
  project?: string | { eq?: string; ne?: string; isNull?: boolean };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ProjectInclude {
  todosViaProject?: true | TodoInclude | TodoQueryBuilder;
}

export interface TodoInclude {
  parent?: true | TodoInclude | TodoQueryBuilder;
  todosViaParent?: true | TodoInclude | TodoQueryBuilder;
  project?: true | ProjectInclude | ProjectQueryBuilder;
}

export interface ProjectRelations {
  todosViaProject: Todo[];
}

export interface TodoRelations {
  parent: Todo;
  todosViaParent: Todo[];
  project: Project;
}

export type ProjectWithIncludes<I extends ProjectInclude = {}> = Project & {
  todosViaProject?: NonNullable<I["todosViaProject"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Todo[]
      : RelationInclude extends TodoQueryBuilder<
            infer QueryInclude extends TodoInclude,
            infer QuerySelect extends TodoSelectableColumn
          >
        ? TodoSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends TodoInclude
          ? TodoWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type TodoWithIncludes<I extends TodoInclude = {}> = Todo & {
  parent?: NonNullable<I["parent"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Todo
      : RelationInclude extends TodoQueryBuilder<
            infer QueryInclude extends TodoInclude,
            infer QuerySelect extends TodoSelectableColumn
          >
        ? TodoSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends TodoInclude
          ? TodoWithIncludes<RelationInclude>
          : never
    : never;
  todosViaParent?: NonNullable<I["todosViaParent"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Todo[]
      : RelationInclude extends TodoQueryBuilder<
            infer QueryInclude extends TodoInclude,
            infer QuerySelect extends TodoSelectableColumn
          >
        ? TodoSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends TodoInclude
          ? TodoWithIncludes<RelationInclude>[]
          : never
    : never;
  project?: NonNullable<I["project"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Project
      : RelationInclude extends ProjectQueryBuilder<
            infer QueryInclude extends ProjectInclude,
            infer QuerySelect extends ProjectSelectableColumn
          >
        ? ProjectSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ProjectInclude
          ? ProjectWithIncludes<RelationInclude>
          : never
    : never;
};

export type ProjectSelectableColumn = keyof Project | PermissionIntrospectionColumn | "*";
export type ProjectOrderableColumn = keyof Project | PermissionIntrospectionColumn;

export type ProjectSelected<S extends ProjectSelectableColumn = keyof Project> = "*" extends S
  ? Project
  : Pick<Project, Extract<S | "id", keyof Project>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ProjectSelectedWithIncludes<
  I extends ProjectInclude = {},
  S extends ProjectSelectableColumn = keyof Project,
> = ProjectSelected<S> & Omit<ProjectWithIncludes<I>, keyof Project>;

export type TodoSelectableColumn = keyof Todo | PermissionIntrospectionColumn | "*";
export type TodoOrderableColumn = keyof Todo | PermissionIntrospectionColumn;

export type TodoSelected<S extends TodoSelectableColumn = keyof Todo> = "*" extends S
  ? Todo
  : Pick<Todo, Extract<S | "id", keyof Todo>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type TodoSelectedWithIncludes<
  I extends TodoInclude = {},
  S extends TodoSelectableColumn = keyof Todo,
> = TodoSelected<S> & Omit<TodoWithIncludes<I>, keyof Todo>;

export const wasmSchema: WasmSchema = {
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
        name: "description",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "parent",
        column_type: {
          type: "Uuid",
        },
        nullable: true,
        references: "todos",
      },
      {
        name: "project",
        column_type: {
          type: "Uuid",
        },
        nullable: true,
        references: "projects",
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
          type: "True",
        },
        with_check: {
          type: "True",
        },
      },
      delete: {
        using: {
          type: "True",
        },
      },
    },
  },
};

export class ProjectQueryBuilder<
  I extends ProjectInclude = {},
  S extends ProjectSelectableColumn = keyof Project,
> implements QueryBuilder<ProjectSelectedWithIncludes<I, S>> {
  readonly _table = "projects";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ProjectSelectedWithIncludes<I, S>;
  declare readonly _initType: ProjectInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProjectInclude> = {};
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

  where(conditions: ProjectWhereInput): ProjectQueryBuilder<I, S> {
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
  ): ProjectQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProjectInclude>(relations: NewI): ProjectQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(
    column: ProjectOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): ProjectQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProjectQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProjectQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "todosViaProject"): ProjectQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProjectWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ProjectQueryBuilder<I, S> {
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
  >(): ProjectQueryBuilder<CloneI, CloneS> {
    const clone = new ProjectQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
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
> implements QueryBuilder<TodoSelectedWithIncludes<I, S>> {
  readonly _table = "todos";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: TodoSelectedWithIncludes<I, S>;
  declare readonly _initType: TodoInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<TodoInclude> = {};
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

  where(conditions: TodoWhereInput): TodoQueryBuilder<I, S> {
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
  ): TodoQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends TodoInclude>(relations: NewI): TodoQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: TodoOrderableColumn, direction: "asc" | "desc" = "asc"): TodoQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): TodoQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): TodoQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "parent" | "todosViaParent" | "project"): TodoQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: TodoWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): TodoQueryBuilder<I, S> {
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
  >(): TodoQueryBuilder<CloneI, CloneS> {
    const clone = new TodoQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
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
  projects: ProjectQueryBuilder;
  todos: TodoQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  projects: new ProjectQueryBuilder(),
  todos: new TodoQueryBuilder(),
  wasmSchema,
};
