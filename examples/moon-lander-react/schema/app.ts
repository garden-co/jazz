// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export interface Player {
  id: string;
  playerId: string;
  name: string;
  color: string;
  mode: string;
  online: boolean;
  lastSeen: number;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  requiredFuelType: string;
  landerFuelLevel: number;
  landerSpawnX: number;
  thrusting: boolean;
}

export interface FuelDeposit {
  id: string;
  fuelType: string;
  positionX: number;
  createdAt: number;
  collected: boolean;
  collectedBy: string;
}

export interface ChatMessage {
  id: string;
  playerId: string;
  message: string;
  createdAt: number;
}

export interface PlayerInit {
  playerId: string;
  name: string;
  color: string;
  mode: string;
  online: boolean;
  lastSeen: number;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  requiredFuelType: string;
  landerFuelLevel: number;
  landerSpawnX: number;
  thrusting: boolean;
}

export interface FuelDepositInit {
  fuelType: string;
  positionX: number;
  createdAt: number;
  collected: boolean;
  collectedBy: string;
}

export interface ChatMessageInit {
  playerId: string;
  message: string;
  createdAt: number;
}

export interface PlayerWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  playerId?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  color?: string | { eq?: string; ne?: string; contains?: string };
  mode?: string | { eq?: string; ne?: string; contains?: string };
  online?: boolean;
  lastSeen?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  positionX?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  positionY?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  velocityX?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  velocityY?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  requiredFuelType?: string | { eq?: string; ne?: string; contains?: string };
  landerFuelLevel?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  landerSpawnX?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  thrusting?: boolean;
}

export interface FuelDepositWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  fuelType?: string | { eq?: string; ne?: string; contains?: string };
  positionX?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  createdAt?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  collected?: boolean;
  collectedBy?: string | { eq?: string; ne?: string; contains?: string };
}

export interface ChatMessageWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  playerId?: string | { eq?: string; ne?: string; contains?: string };
  message?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export const wasmSchema: WasmSchema = {
  players: {
    columns: [
      {
        name: "playerId",
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
        name: "color",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "mode",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "online",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
      {
        name: "lastSeen",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "positionX",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "positionY",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "velocityX",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "velocityY",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "requiredFuelType",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "landerFuelLevel",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "landerSpawnX",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "thrusting",
        column_type: {
          type: "Boolean",
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
  fuel_deposits: {
    columns: [
      {
        name: "fuelType",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "positionX",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "collected",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
      {
        name: "collectedBy",
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
  chat_messages: {
    columns: [
      {
        name: "playerId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "message",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Integer",
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

export class PlayerQueryBuilder<
  I extends Record<string, never> = {},
> implements QueryBuilder<Player> {
  readonly _table = "players";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Player;
  declare readonly _initType: PlayerInit;
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

  where(conditions: PlayerWhereInput): PlayerQueryBuilder<I> {
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

  orderBy(column: keyof Player, direction: "asc" | "desc" = "asc"): PlayerQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): PlayerQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): PlayerQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: PlayerWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): PlayerQueryBuilder<I> {
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

  private _clone<CloneI extends Record<string, never> = I>(): PlayerQueryBuilder<CloneI> {
    const clone = new PlayerQueryBuilder<CloneI>();
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

export class FuelDepositQueryBuilder<
  I extends Record<string, never> = {},
> implements QueryBuilder<FuelDeposit> {
  readonly _table = "fuel_deposits";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: FuelDeposit;
  declare readonly _initType: FuelDepositInit;
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

  where(conditions: FuelDepositWhereInput): FuelDepositQueryBuilder<I> {
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

  orderBy(
    column: keyof FuelDeposit,
    direction: "asc" | "desc" = "asc",
  ): FuelDepositQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FuelDepositQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FuelDepositQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: FuelDepositWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FuelDepositQueryBuilder<I> {
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

  private _clone<CloneI extends Record<string, never> = I>(): FuelDepositQueryBuilder<CloneI> {
    const clone = new FuelDepositQueryBuilder<CloneI>();
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

export class ChatMessageQueryBuilder<
  I extends Record<string, never> = {},
> implements QueryBuilder<ChatMessage> {
  readonly _table = "chat_messages";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ChatMessage;
  declare readonly _initType: ChatMessageInit;
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

  where(conditions: ChatMessageWhereInput): ChatMessageQueryBuilder<I> {
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

  orderBy(
    column: keyof ChatMessage,
    direction: "asc" | "desc" = "asc",
  ): ChatMessageQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ChatMessageQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ChatMessageQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: ChatMessageWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ChatMessageQueryBuilder<I> {
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

  private _clone<CloneI extends Record<string, never> = I>(): ChatMessageQueryBuilder<CloneI> {
    const clone = new ChatMessageQueryBuilder<CloneI>();
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
  players: PlayerQueryBuilder;
  fuel_deposits: FuelDepositQueryBuilder;
  chat_messages: ChatMessageQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  players: new PlayerQueryBuilder(),
  fuel_deposits: new FuelDepositQueryBuilder(),
  chat_messages: new ChatMessageQueryBuilder(),
  wasmSchema,
};
