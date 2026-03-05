// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

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
  points: JsonValue;
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
  points: JsonValue;
  created_at: string;
}

export interface UserWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: string | { eq?: string; ne?: string; contains?: string };
}

export interface CanvasWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  created_at?: string | { eq?: string; ne?: string; contains?: string };
}

export interface StrokeWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  canvas_id?: string | { eq?: string; ne?: string };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  points?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  created_at?: string | { eq?: string; ne?: string; contains?: string };
}

export interface CanvasInclude {
  strokesViaCanvas?: true | StrokeInclude | StrokeQueryBuilder;
}

export interface StrokeInclude {
  canvas?: true | CanvasInclude | CanvasQueryBuilder;
}

export interface CanvasRelations {
  strokesViaCanvas: Stroke[];
}

export interface StrokeRelations {
  canvas: Canvas;
}

export type CanvasWithIncludes<I extends CanvasInclude = {}> = Canvas & {
  strokesViaCanvas?: NonNullable<I["strokesViaCanvas"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Stroke[]
      : RelationInclude extends StrokeQueryBuilder<infer QueryInclude extends StrokeInclude>
        ? StrokeWithIncludes<QueryInclude>[]
        : RelationInclude extends StrokeInclude
          ? StrokeWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type StrokeWithIncludes<I extends StrokeInclude = {}> = Stroke & {
  canvas?: NonNullable<I["canvas"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Canvas
      : RelationInclude extends CanvasQueryBuilder<infer QueryInclude extends CanvasInclude>
        ? CanvasWithIncludes<QueryInclude>
        : RelationInclude extends CanvasInclude
          ? CanvasWithIncludes<RelationInclude>
          : never
    : never;
};

export const wasmSchema: WasmSchema = {
  users: {
    columns: [
      {
        name: "user_id",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "created_at",
        column_type: {
          type: "Text",
        },
        nullable: false,
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
      update: {},
      delete: {},
    },
  },
  canvases: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "created_at",
        column_type: {
          type: "Text",
        },
        nullable: false,
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
      update: {},
      delete: {},
    },
  },
  strokes: {
    columns: [
      {
        name: "canvas_id",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "canvases",
      },
      {
        name: "user_id",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "points",
        column_type: {
          type: "Json",
        },
        nullable: false,
      },
      {
        name: "created_at",
        column_type: {
          type: "Text",
        },
        nullable: false,
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
      update: {},
      delete: {},
    },
  },
};

export class UserQueryBuilder<I extends Record<string, never> = {}> implements QueryBuilder<User> {
  readonly _table = "users";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: User;
  declare readonly _initType: UserInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
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

  where(conditions: UserWhereInput): UserQueryBuilder<I> {
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

  orderBy(column: keyof User, direction: "asc" | "desc" = "asc"): UserQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): UserQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): UserQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: UserWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): UserQueryBuilder<I> {
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone<CloneI extends Record<string, never> = I>(): UserQueryBuilder<CloneI> {
    const clone = new UserQueryBuilder<CloneI>();
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

export class CanvasQueryBuilder<I extends CanvasInclude = {}> implements QueryBuilder<
  CanvasWithIncludes<I>
> {
  readonly _table = "canvases";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: CanvasWithIncludes<I>;
  declare readonly _initType: CanvasInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CanvasInclude> = {};
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

  where(conditions: CanvasWhereInput): CanvasQueryBuilder<I> {
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

  include<NewI extends CanvasInclude>(relations: NewI): CanvasQueryBuilder<I & NewI> {
    const clone = this._clone<I & NewI>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Canvas, direction: "asc" | "desc" = "asc"): CanvasQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CanvasQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CanvasQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "strokesViaCanvas"): CanvasQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CanvasWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CanvasQueryBuilder<I> {
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone<CloneI extends CanvasInclude = I>(): CanvasQueryBuilder<CloneI> {
    const clone = new CanvasQueryBuilder<CloneI>();
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

export class StrokeQueryBuilder<I extends StrokeInclude = {}> implements QueryBuilder<
  StrokeWithIncludes<I>
> {
  readonly _table = "strokes";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: StrokeWithIncludes<I>;
  declare readonly _initType: StrokeInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<StrokeInclude> = {};
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

  where(conditions: StrokeWhereInput): StrokeQueryBuilder<I> {
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

  include<NewI extends StrokeInclude>(relations: NewI): StrokeQueryBuilder<I & NewI> {
    const clone = this._clone<I & NewI>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Stroke, direction: "asc" | "desc" = "asc"): StrokeQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): StrokeQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): StrokeQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "canvas"): StrokeQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: StrokeWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): StrokeQueryBuilder<I> {
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone<CloneI extends StrokeInclude = I>(): StrokeQueryBuilder<CloneI> {
    const clone = new StrokeQueryBuilder<CloneI>();
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
