// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-ts";

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  parent?: string;
}

export interface TodoInit {
  title: string;
  done: boolean;
  description?: string;
  parent?: string;
}

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  description?: string | { eq?: string; ne?: string; contains?: string };
  parent?: string | { eq?: string; ne?: string; isNull?: boolean };
}

export interface TodoInclude {
  parent?: boolean | TodoInclude | TodoQueryBuilder;
  todosViaParent?: boolean | TodoInclude | TodoQueryBuilder;
}

export interface TodoRelations {
  parent: Todo;
  todosViaParent: Todo[];
}

// Helper types for nested includes
type WithIncludesFor<T, I> = T extends { id: string }
  ? T & { [K in keyof I & string]?: unknown }
  : T;

type WithIncludesArray<E, I> = E extends { id: string }
  ? Array<E & { [K in keyof I & string]?: unknown }>
  : E[];

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
      ],
    },
  },
};

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
  todos: new TodoQueryBuilder(),
  wasmSchema,
};
