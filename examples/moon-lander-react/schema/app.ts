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
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
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
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ChatMessageWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  playerId?: string | { eq?: string; ne?: string; contains?: string };
  message?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyPlayerQueryBuilder<T = any> = { readonly _table: "players" } & QueryBuilder<T>;
type AnyFuelDepositQueryBuilder<T = any> = { readonly _table: "fuel_deposits" } & QueryBuilder<T>;
type AnyChatMessageQueryBuilder<T = any> = { readonly _table: "chat_messages" } & QueryBuilder<T>;

export type PlayerSelectableColumn = keyof Player | PermissionIntrospectionColumn | "*";
export type PlayerOrderableColumn = keyof Player | PermissionIntrospectionColumn;

export type PlayerSelected<S extends PlayerSelectableColumn = keyof Player> = "*" extends S
  ? Player
  : Pick<Player, Extract<S | "id", keyof Player>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FuelDepositSelectableColumn = keyof FuelDeposit | PermissionIntrospectionColumn | "*";
export type FuelDepositOrderableColumn = keyof FuelDeposit | PermissionIntrospectionColumn;

export type FuelDepositSelected<S extends FuelDepositSelectableColumn = keyof FuelDeposit> =
  "*" extends S
    ? FuelDeposit
    : Pick<FuelDeposit, Extract<S | "id", keyof FuelDeposit>> &
        Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ChatMessageSelectableColumn = keyof ChatMessage | PermissionIntrospectionColumn | "*";
export type ChatMessageOrderableColumn = keyof ChatMessage | PermissionIntrospectionColumn;

export type ChatMessageSelected<S extends ChatMessageSelectableColumn = keyof ChatMessage> =
  "*" extends S
    ? ChatMessage
    : Pick<ChatMessage, Extract<S | "id", keyof ChatMessage>> &
        Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

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
  },
};

export class PlayerQueryBuilder<
  I extends Record<string, never> = {},
  S extends PlayerSelectableColumn = keyof Player,
> implements QueryBuilder<PlayerSelected<S>> {
  readonly _table = "players";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: PlayerSelected<S>;
  declare readonly _initType: PlayerInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
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

  where(conditions: PlayerWhereInput): PlayerQueryBuilder<I, S> {
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

  select<NewS extends PlayerSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): PlayerQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(
    column: PlayerOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): PlayerQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): PlayerQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): PlayerQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: PlayerWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): PlayerQueryBuilder<I, S> {
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
    CloneI extends Record<string, never> = I,
    CloneS extends PlayerSelectableColumn = S,
  >(): PlayerQueryBuilder<CloneI, CloneS> {
    const clone = new PlayerQueryBuilder<CloneI, CloneS>();
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

export class FuelDepositQueryBuilder<
  I extends Record<string, never> = {},
  S extends FuelDepositSelectableColumn = keyof FuelDeposit,
> implements QueryBuilder<FuelDepositSelected<S>> {
  readonly _table = "fuel_deposits";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: FuelDepositSelected<S>;
  declare readonly _initType: FuelDepositInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
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

  where(conditions: FuelDepositWhereInput): FuelDepositQueryBuilder<I, S> {
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

  select<NewS extends FuelDepositSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): FuelDepositQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(
    column: FuelDepositOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): FuelDepositQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FuelDepositQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FuelDepositQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: FuelDepositWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FuelDepositQueryBuilder<I, S> {
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
    CloneI extends Record<string, never> = I,
    CloneS extends FuelDepositSelectableColumn = S,
  >(): FuelDepositQueryBuilder<CloneI, CloneS> {
    const clone = new FuelDepositQueryBuilder<CloneI, CloneS>();
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

export class ChatMessageQueryBuilder<
  I extends Record<string, never> = {},
  S extends ChatMessageSelectableColumn = keyof ChatMessage,
> implements QueryBuilder<ChatMessageSelected<S>> {
  readonly _table = "chat_messages";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ChatMessageSelected<S>;
  declare readonly _initType: ChatMessageInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
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

  where(conditions: ChatMessageWhereInput): ChatMessageQueryBuilder<I, S> {
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

  select<NewS extends ChatMessageSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): ChatMessageQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(
    column: ChatMessageOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): ChatMessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ChatMessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ChatMessageQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: ChatMessageWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ChatMessageQueryBuilder<I, S> {
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
    CloneI extends Record<string, never> = I,
    CloneS extends ChatMessageSelectableColumn = S,
  >(): ChatMessageQueryBuilder<CloneI, CloneS> {
    const clone = new ChatMessageQueryBuilder<CloneI, CloneS>();
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
