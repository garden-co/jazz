// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";

export interface Project {
  id: string;
  name: string;
}

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  owner_id: string;
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
  owner_id: string;
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
  owner_id?: string | { eq?: string; ne?: string; contains?: string };
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
          name: "owner_id",
          column_type: {
            type: "Text",
          },
          nullable: false,
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
            column: "owner_id",
            op: "Eq",
            value: {
              type: "SessionRef",
              path: ["user_id"],
            },
          },
        },
        update: {
          using: {
            type: "Cmp",
            column: "owner_id",
            op: "Eq",
            value: {
              type: "SessionRef",
              path: ["user_id"],
            },
          },
          with_check: {
            type: "Cmp",
            column: "owner_id",
            op: "Eq",
            value: {
              type: "SessionRef",
              path: ["user_id"],
            },
          },
        },
        delete: {
          using: {
            type: "Cmp",
            column: "owner_id",
            op: "Eq",
            value: {
              type: "SessionRef",
              path: ["user_id"],
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

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
    });
  }

  private _clone(): ProjectQueryBuilder<I> {
    const clone = new ProjectQueryBuilder<I>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
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

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
    });
  }

  private _clone(): TodoQueryBuilder<I> {
    const clone = new TodoQueryBuilder<I>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    return clone;
  }
}

export const app = {
  projects: new ProjectQueryBuilder(),
  todos: new TodoQueryBuilder(),
  wasmSchema,
};
