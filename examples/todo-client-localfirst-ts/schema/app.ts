// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-ts";

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
}

export interface TodoInit {
  title: string;
  done: boolean;
  description?: string;
}

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  description?: string | { eq?: string; ne?: string; contains?: string };
}

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
      ],
    },
  },
};

export class TodoQueryBuilder<I extends Record<string, never> = {}> implements QueryBuilder<Todo> {
  readonly _table = "todos";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Todo;
  declare readonly _initType: TodoInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
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
