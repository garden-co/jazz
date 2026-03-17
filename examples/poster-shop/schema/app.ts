// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder, JsonSchemaToTs } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

const __jsonSchema1 = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "x": {
        "type": "number"
      },
      "y": {
        "type": "number"
      }
    },
    "required": [
      "x",
      "y"
    ]
  }
} as const;
type __JsonType1 = JsonSchemaToTs<typeof __jsonSchema1>;

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface User {
  id: string;
  user_id: string;
  name: string;
  created_at: string;
}

export interface Canvas {
  id: string;
  name: string;
  created_at: string;
}

export interface Stroke {
  id: string;
  canvas_id: string;
  user_id: string;
  points: __JsonType1;
  created_at: string;
}

export interface UserInit {
  user_id: string;
  name: string;
  created_at: string;
}

export interface CanvasInit {
  name: string;
  created_at: string;
}

export interface StrokeInit {
  canvas_id: string;
  user_id: string;
  points: __JsonType1;
  created_at: string;
}

export interface UserWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface CanvasWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface StrokeWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  canvas_id?: string | { eq?: string; ne?: string };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  points?: __JsonType1 | { eq?: __JsonType1; ne?: __JsonType1; in?: __JsonType1[] };
  created_at?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyUserQueryBuilder<T = any> = { readonly _table: "users" } & QueryBuilder<T>;
type AnyCanvasQueryBuilder<T = any> = { readonly _table: "canvases" } & QueryBuilder<T>;
type AnyStrokeQueryBuilder<T = any> = { readonly _table: "strokes" } & QueryBuilder<T>;

export interface CanvasInclude {
  strokesViaCanvas?: true | StrokeInclude | AnyStrokeQueryBuilder<any>;
}

export interface StrokeInclude {
  canvas?: true | CanvasInclude | AnyCanvasQueryBuilder<any>;
}

export type CanvasIncludedRelations<I extends CanvasInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "strokesViaCanvas"
      ? NonNullable<I["strokesViaCanvas"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Stroke[]
          : RelationInclude extends AnyStrokeQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends StrokeInclude
              ? StrokeWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type StrokeIncludedRelations<I extends StrokeInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "canvas"
      ? NonNullable<I["canvas"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Canvas : Canvas | undefined
          : RelationInclude extends AnyCanvasQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends CanvasInclude
              ? R extends true ? CanvasWithIncludes<RelationInclude, false> : CanvasWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface CanvasRelations {
  strokesViaCanvas: Stroke[];
}

export interface StrokeRelations {
  canvas: Canvas | undefined;
}

export type CanvasWithIncludes<I extends CanvasInclude = {}, R extends boolean = false> = Canvas & CanvasIncludedRelations<I, R>;

export type StrokeWithIncludes<I extends StrokeInclude = {}, R extends boolean = false> = Stroke & StrokeIncludedRelations<I, R>;

export type UserSelectableColumn = keyof User | PermissionIntrospectionColumn | "*";
export type UserOrderableColumn = keyof User | PermissionIntrospectionColumn;

export type UserSelected<S extends UserSelectableColumn = keyof User> = "*" extends S ? User : Pick<User, Extract<S | "id", keyof User>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CanvasSelectableColumn = keyof Canvas | PermissionIntrospectionColumn | "*";
export type CanvasOrderableColumn = keyof Canvas | PermissionIntrospectionColumn;

export type CanvasSelected<S extends CanvasSelectableColumn = keyof Canvas> = "*" extends S ? Canvas : Pick<Canvas, Extract<S | "id", keyof Canvas>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type CanvasSelectedWithIncludes<I extends CanvasInclude = {}, S extends CanvasSelectableColumn = keyof Canvas, R extends boolean = false> = CanvasSelected<S> & CanvasIncludedRelations<I, R>;

export type StrokeSelectableColumn = keyof Stroke | PermissionIntrospectionColumn | "*";
export type StrokeOrderableColumn = keyof Stroke | PermissionIntrospectionColumn;

export type StrokeSelected<S extends StrokeSelectableColumn = keyof Stroke> = "*" extends S ? Stroke : Pick<Stroke, Extract<S | "id", keyof Stroke>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type StrokeSelectedWithIncludes<I extends StrokeInclude = {}, S extends StrokeSelectableColumn = keyof Stroke, R extends boolean = false> = StrokeSelected<S> & StrokeIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "users": {
    "columns": [
      {
        "name": "user_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "True"
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "canvases": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "True"
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "strokes": {
    "columns": [
      {
        "name": "canvas_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "canvases"
      },
      {
        "name": "user_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "points",
        "column_type": {
          "type": "Json",
          "schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "x": {
                  "type": "number"
                },
                "y": {
                  "type": "number"
                }
              },
              "required": [
                "x",
                "y"
              ]
            }
          }
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "True"
        }
      },
      "update": {},
      "delete": {}
    }
  }
};

export class UserQueryBuilder<I extends Record<string, never> = {}, S extends UserSelectableColumn = keyof User, R extends boolean = false> implements QueryBuilder<UserSelected<S>> {
  readonly _table = "users";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: UserSelected<S>;
  readonly _initType!: UserInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
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

  select<NewS extends UserSelectableColumn>(...columns: [NewS, ...NewS[]]): UserQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(column: UserOrderableColumn, direction: "asc" | "desc" = "asc"): UserQueryBuilder<I, S, R> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends Record<string, never> = I, CloneS extends UserSelectableColumn = S, CloneR extends boolean = R>(): UserQueryBuilder<CloneI, CloneS, CloneR> {
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

export class CanvasQueryBuilder<I extends CanvasInclude = {}, S extends CanvasSelectableColumn = keyof Canvas, R extends boolean = false> implements QueryBuilder<CanvasSelectedWithIncludes<I, S, R>> {
  readonly _table = "canvases";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: CanvasSelectedWithIncludes<I, S, R>;
  readonly _initType!: CanvasInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CanvasInclude> = {};
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

  where(conditions: CanvasWhereInput): CanvasQueryBuilder<I, S, R> {
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

  select<NewS extends CanvasSelectableColumn>(...columns: [NewS, ...NewS[]]): CanvasQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CanvasInclude>(relations: NewI): CanvasQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): CanvasQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: CanvasOrderableColumn, direction: "asc" | "desc" = "asc"): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "strokesViaCanvas"): CanvasQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CanvasWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CanvasQueryBuilder<I, S, R> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends CanvasInclude = I, CloneS extends CanvasSelectableColumn = S, CloneR extends boolean = R>(): CanvasQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new CanvasQueryBuilder<CloneI, CloneS, CloneR>();
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

export class StrokeQueryBuilder<I extends StrokeInclude = {}, S extends StrokeSelectableColumn = keyof Stroke, R extends boolean = false> implements QueryBuilder<StrokeSelectedWithIncludes<I, S, R>> {
  readonly _table = "strokes";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: StrokeSelectedWithIncludes<I, S, R>;
  readonly _initType!: StrokeInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<StrokeInclude> = {};
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

  where(conditions: StrokeWhereInput): StrokeQueryBuilder<I, S, R> {
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

  select<NewS extends StrokeSelectableColumn>(...columns: [NewS, ...NewS[]]): StrokeQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends StrokeInclude>(relations: NewI): StrokeQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): StrokeQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: StrokeOrderableColumn, direction: "asc" | "desc" = "asc"): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "canvas"): StrokeQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: StrokeWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): StrokeQueryBuilder<I, S, R> {
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
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
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
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
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

  private _clone<CloneI extends StrokeInclude = I, CloneS extends StrokeSelectableColumn = S, CloneR extends boolean = R>(): StrokeQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new StrokeQueryBuilder<CloneI, CloneS, CloneR>();
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
  canvases: CanvasQueryBuilder;
  strokes: StrokeQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  users: new UserQueryBuilder(),
  canvases: new CanvasQueryBuilder(),
  strokes: new StrokeQueryBuilder(),
  wasmSchema,
};
