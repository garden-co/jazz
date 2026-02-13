// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-ts";

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
}

export interface PlayerWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  playerId?: string | { eq?: string; ne?: string; contains?: string };
  name?: string | { eq?: string; ne?: string; contains?: string };
  color?: string | { eq?: string; ne?: string; contains?: string };
  mode?: string | { eq?: string; ne?: string; contains?: string };
  online?: boolean;
  lastSeen?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  positionX?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  positionY?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  velocityX?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  velocityY?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  requiredFuelType?: string | { eq?: string; ne?: string; contains?: string };
  landerFuelLevel?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  landerSpawnX?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export const wasmSchema: WasmSchema = {
  "tables": {
    "players": {
      "columns": [
        {
          "name": "playerId",
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
          "name": "color",
          "column_type": {
            "type": "Text"
          },
          "nullable": false
        },
        {
          "name": "mode",
          "column_type": {
            "type": "Text"
          },
          "nullable": false
        },
        {
          "name": "online",
          "column_type": {
            "type": "Boolean"
          },
          "nullable": false
        },
        {
          "name": "lastSeen",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "positionX",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "positionY",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "velocityX",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "velocityY",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "requiredFuelType",
          "column_type": {
            "type": "Text"
          },
          "nullable": false
        },
        {
          "name": "landerFuelLevel",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "landerSpawnX",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        }
      ]
    }
  }
};

export class PlayerQueryBuilder<I extends Record<string, never> = {}> implements QueryBuilder<Player> {
  readonly _table = "players";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Player;
  declare readonly _initType: PlayerInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;

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

  private _clone(): PlayerQueryBuilder<I> {
    const clone = new PlayerQueryBuilder<I>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    return clone;
  }
}

export const app = {
  players: new PlayerQueryBuilder(),
  wasmSchema,
};
