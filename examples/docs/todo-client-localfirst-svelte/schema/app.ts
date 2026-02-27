// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

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
}

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  description?: string | { eq?: string; ne?: string; contains?: string };
  parent?: string | { eq?: string; ne?: string; isNull?: boolean };
  project?: string | { eq?: string; ne?: string; isNull?: boolean };
}

export interface ProjectInclude {
  todosViaProject?: boolean | TodoInclude | TodoQueryBuilder;
}

export interface TodoInclude {
  parent?: boolean | TodoInclude | TodoQueryBuilder;
  todosViaParent?: boolean | TodoInclude | TodoQueryBuilder;
  project?: boolean | ProjectInclude | ProjectQueryBuilder;
}

export interface ProjectRelations {
  todosViaProject: Todo[];
}

export interface TodoRelations {
  parent: Todo;
  todosViaParent: Todo[];
  project: Project;
}

// Helper types for nested includes
type WithIncludesFor<T, I> = T extends { id: string }
  ? T & { [K in keyof I & string]?: unknown }
  : T;

type WithIncludesArray<E, I> = E extends { id: string }
  ? Array<E & { [K in keyof I & string]?: unknown }>
  : E[];

export type ProjectWithIncludes<I extends ProjectInclude = {}> = Project & {
  [K in keyof I & keyof ProjectRelations]?: I[K] extends true
    ? ProjectRelations[K]
    : I[K] extends object
      ? ProjectRelations[K] extends (infer E)[]
        ? WithIncludesArray<E, I[K]>
        : ProjectRelations[K] & WithIncludesFor<ProjectRelations[K], I[K]>
      : never;
};

export type TodoWithIncludes<I extends TodoInclude = {}> = Todo & {
  [K in keyof I & keyof TodoRelations]?: I[K] extends true
    ? TodoRelations[K]
    : I[K] extends object
      ? TodoRelations[K] extends (infer E)[]
        ? WithIncludesArray<E, I[K]>
        : TodoRelations[K] & WithIncludesFor<TodoRelations[K], I[K]>
      : never;
};

export const wasmSchema: WasmSchema = {
  tables: {
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
  },
};

export class ProjectQueryBuilder<I extends ProjectInclude = {}> implements QueryBuilder<Project> {
  readonly _table = "projects";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Project;
  declare readonly _initType: ProjectInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProjectInclude> = {};
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

  where(conditions: ProjectWhereInput): ProjectQueryBuilder<I> {
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

  include<NewI extends ProjectInclude>(relations: NewI): ProjectQueryBuilder<I & NewI> {
    const clone = this._clone() as unknown as ProjectQueryBuilder<I & NewI>;
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Project, direction: "asc" | "desc" = "asc"): ProjectQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProjectQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProjectQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "todosViaProject"): ProjectQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProjectWhereInput;
    step: (ctx: { current: any }) => unknown;
    maxDepth?: number;
  }): ProjectQueryBuilder<I> {
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

    const stepBuilt = JSON.parse((stepOutput as { _build: () => string })._build()) as {
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone(): ProjectQueryBuilder<I> {
    const clone = new ProjectQueryBuilder<I>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
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

export class TodoQueryBuilder<I extends TodoInclude = {}> implements QueryBuilder<Todo> {
  readonly _table = "todos";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Todo;
  declare readonly _initType: TodoInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<TodoInclude> = {};
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

  where(conditions: TodoWhereInput): TodoQueryBuilder<I> {
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

  include<NewI extends TodoInclude>(relations: NewI): TodoQueryBuilder<I & NewI> {
    const clone = this._clone() as unknown as TodoQueryBuilder<I & NewI>;
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Todo, direction: "asc" | "desc" = "asc"): TodoQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): TodoQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): TodoQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "parent" | "todosViaParent" | "project"): TodoQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: TodoWhereInput;
    step: (ctx: { current: any }) => unknown;
    maxDepth?: number;
  }): TodoQueryBuilder<I> {
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

    const stepBuilt = JSON.parse((stepOutput as { _build: () => string })._build()) as {
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone(): TodoQueryBuilder<I> {
    const clone = new TodoQueryBuilder<I>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
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

export const app = {
  projects: new ProjectQueryBuilder(),
  todos: new TodoQueryBuilder(),
  wasmSchema,
};
