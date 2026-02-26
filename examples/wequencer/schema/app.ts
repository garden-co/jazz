// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";

export interface Instrument {
  id: string;
  name: string;
  sound: Uint8Array;
  display_order: number;
}

export interface Jam {
  id: string;
  created_at: Date;
  transport_start?: Date;
  bpm: number;
  beat_count: number;
}

export interface Beat {
  id: string;
  jam: string;
  instrument: string;
  beat_index: number;
  placed_by: string;
}

export interface Participant {
  id: string;
  jam: string;
  user_id: string;
  display_name: string;
}

export interface InstrumentInit {
  name: string;
  sound: Uint8Array;
  display_order: number;
}

export interface JamInit {
  created_at: Date;
  transport_start?: Date;
  bpm: number;
  beat_count: number;
}

export interface BeatInit {
  jam: string;
  instrument: string;
  beat_index: number;
  placed_by: string;
}

export interface ParticipantInit {
  jam: string;
  user_id: string;
  display_name: string;
}

export interface InstrumentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  sound?: Uint8Array | { eq?: Uint8Array; ne?: Uint8Array };
  display_order?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface JamWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  transport_start?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  bpm?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  beat_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface BeatWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jam?: string | { eq?: string; ne?: string };
  instrument?: string | { eq?: string; ne?: string };
  beat_index?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  placed_by?: string | { eq?: string; ne?: string; contains?: string };
}

export interface ParticipantWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jam?: string | { eq?: string; ne?: string };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  display_name?: string | { eq?: string; ne?: string; contains?: string };
}

export interface InstrumentInclude {
  beatsViaInstrument?: boolean | BeatInclude | BeatQueryBuilder;
}

export interface JamInclude {
  beatsViaJam?: boolean | BeatInclude | BeatQueryBuilder;
  participantsViaJam?: boolean | ParticipantInclude | ParticipantQueryBuilder;
}

export interface BeatInclude {
  jam?: boolean | JamInclude | JamQueryBuilder;
  instrument?: boolean | InstrumentInclude | InstrumentQueryBuilder;
}

export interface ParticipantInclude {
  jam?: boolean | JamInclude | JamQueryBuilder;
}

export interface InstrumentRelations {
  beatsViaInstrument: Beat[];
}

export interface JamRelations {
  beatsViaJam: Beat[];
  participantsViaJam: Participant[];
}

export interface BeatRelations {
  jam: Jam;
  instrument: Instrument;
}

export interface ParticipantRelations {
  jam: Jam;
}

// Helper types for nested includes
type WithIncludesFor<T, I> = T extends { id: string }
  ? T & { [K in keyof I & string]?: unknown }
  : T;

type WithIncludesArray<E, I> = E extends { id: string }
  ? Array<E & { [K in keyof I & string]?: unknown }>
  : E[];

export type InstrumentWithIncludes<I extends InstrumentInclude = {}> = Instrument & {
  [K in keyof I & keyof InstrumentRelations]?: I[K] extends true
    ? InstrumentRelations[K]
    : I[K] extends object
      ? InstrumentRelations[K] extends (infer E)[]
        ? WithIncludesArray<E, I[K]>
        : InstrumentRelations[K] & WithIncludesFor<InstrumentRelations[K], I[K]>
      : never;
};

export type JamWithIncludes<I extends JamInclude = {}> = Jam & {
  [K in keyof I & keyof JamRelations]?: I[K] extends true
    ? JamRelations[K]
    : I[K] extends object
      ? JamRelations[K] extends (infer E)[]
        ? WithIncludesArray<E, I[K]>
        : JamRelations[K] & WithIncludesFor<JamRelations[K], I[K]>
      : never;
};

export type BeatWithIncludes<I extends BeatInclude = {}> = Beat & {
  [K in keyof I & keyof BeatRelations]?: I[K] extends true
    ? BeatRelations[K]
    : I[K] extends object
      ? BeatRelations[K] extends (infer E)[]
        ? WithIncludesArray<E, I[K]>
        : BeatRelations[K] & WithIncludesFor<BeatRelations[K], I[K]>
      : never;
};

export type ParticipantWithIncludes<I extends ParticipantInclude = {}> = Participant & {
  [K in keyof I & keyof ParticipantRelations]?: I[K] extends true
    ? ParticipantRelations[K]
    : I[K] extends object
      ? ParticipantRelations[K] extends (infer E)[]
        ? WithIncludesArray<E, I[K]>
        : ParticipantRelations[K] & WithIncludesFor<ParticipantRelations[K], I[K]>
      : never;
};

export const wasmSchema: WasmSchema = {
  "tables": {
    "instruments": {
      "columns": [
        {
          "name": "name",
          "column_type": {
            "type": "Text"
          },
          "nullable": false
        },
        {
          "name": "sound",
          "column_type": {
            "type": "Bytea"
          },
          "nullable": false
        },
        {
          "name": "display_order",
          "column_type": {
            "type": "Integer"
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
        }
      }
    },
    "jams": {
      "columns": [
        {
          "name": "created_at",
          "column_type": {
            "type": "Timestamp"
          },
          "nullable": false
        },
        {
          "name": "transport_start",
          "column_type": {
            "type": "Timestamp"
          },
          "nullable": true
        },
        {
          "name": "bpm",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "beat_count",
          "column_type": {
            "type": "Integer"
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
        "update": {
          "using": {
            "type": "True"
          },
          "with_check": {
            "type": "True"
          }
        }
      }
    },
    "beats": {
      "columns": [
        {
          "name": "jam",
          "column_type": {
            "type": "Uuid"
          },
          "nullable": false,
          "references": "jams"
        },
        {
          "name": "instrument",
          "column_type": {
            "type": "Uuid"
          },
          "nullable": false,
          "references": "instruments"
        },
        {
          "name": "beat_index",
          "column_type": {
            "type": "Integer"
          },
          "nullable": false
        },
        {
          "name": "placed_by",
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
        "delete": {
          "using": {
            "type": "True"
          }
        }
      }
    },
    "participants": {
      "columns": [
        {
          "name": "jam",
          "column_type": {
            "type": "Uuid"
          },
          "nullable": false,
          "references": "jams"
        },
        {
          "name": "user_id",
          "column_type": {
            "type": "Text"
          },
          "nullable": false
        },
        {
          "name": "display_name",
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
        "delete": {
          "using": {
            "type": "Cmp",
            "column": "user_id",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      }
    }
  }
};

export class InstrumentQueryBuilder<I extends InstrumentInclude = {}> implements QueryBuilder<Instrument> {
  readonly _table = "instruments";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Instrument;
  declare readonly _initType: InstrumentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<InstrumentInclude> = {};
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

  where(conditions: InstrumentWhereInput): InstrumentQueryBuilder<I> {
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

  include<NewI extends InstrumentInclude>(relations: NewI): InstrumentQueryBuilder<I & NewI> {
    const clone = this._clone() as unknown as InstrumentQueryBuilder<I & NewI>;
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Instrument, direction: "asc" | "desc" = "asc"): InstrumentQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): InstrumentQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): InstrumentQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "beatsViaInstrument"): InstrumentQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: InstrumentWhereInput;
    step: (ctx: { current: any }) => unknown;
    maxDepth?: number;
  }): InstrumentQueryBuilder<I> {
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
      (stepOutput as { _build: () => string })._build(),
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone(): InstrumentQueryBuilder<I> {
    const clone = new InstrumentQueryBuilder<I>();
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

export class JamQueryBuilder<I extends JamInclude = {}> implements QueryBuilder<Jam> {
  readonly _table = "jams";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Jam;
  declare readonly _initType: JamInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JamInclude> = {};
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

  where(conditions: JamWhereInput): JamQueryBuilder<I> {
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

  include<NewI extends JamInclude>(relations: NewI): JamQueryBuilder<I & NewI> {
    const clone = this._clone() as unknown as JamQueryBuilder<I & NewI>;
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Jam, direction: "asc" | "desc" = "asc"): JamQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JamQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JamQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "beatsViaJam" | "participantsViaJam"): JamQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JamWhereInput;
    step: (ctx: { current: any }) => unknown;
    maxDepth?: number;
  }): JamQueryBuilder<I> {
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
      (stepOutput as { _build: () => string })._build(),
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone(): JamQueryBuilder<I> {
    const clone = new JamQueryBuilder<I>();
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

export class BeatQueryBuilder<I extends BeatInclude = {}> implements QueryBuilder<Beat> {
  readonly _table = "beats";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Beat;
  declare readonly _initType: BeatInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BeatInclude> = {};
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

  where(conditions: BeatWhereInput): BeatQueryBuilder<I> {
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

  include<NewI extends BeatInclude>(relations: NewI): BeatQueryBuilder<I & NewI> {
    const clone = this._clone() as unknown as BeatQueryBuilder<I & NewI>;
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Beat, direction: "asc" | "desc" = "asc"): BeatQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BeatQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BeatQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "jam" | "instrument"): BeatQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BeatWhereInput;
    step: (ctx: { current: any }) => unknown;
    maxDepth?: number;
  }): BeatQueryBuilder<I> {
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
      (stepOutput as { _build: () => string })._build(),
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone(): BeatQueryBuilder<I> {
    const clone = new BeatQueryBuilder<I>();
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

export class ParticipantQueryBuilder<I extends ParticipantInclude = {}> implements QueryBuilder<Participant> {
  readonly _table = "participants";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: Participant;
  declare readonly _initType: ParticipantInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ParticipantInclude> = {};
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

  where(conditions: ParticipantWhereInput): ParticipantQueryBuilder<I> {
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

  include<NewI extends ParticipantInclude>(relations: NewI): ParticipantQueryBuilder<I & NewI> {
    const clone = this._clone() as unknown as ParticipantQueryBuilder<I & NewI>;
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Participant, direction: "asc" | "desc" = "asc"): ParticipantQueryBuilder<I> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ParticipantQueryBuilder<I> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ParticipantQueryBuilder<I> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "jam"): ParticipantQueryBuilder<I> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ParticipantWhereInput;
    step: (ctx: { current: any }) => unknown;
    maxDepth?: number;
  }): ParticipantQueryBuilder<I> {
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
      (stepOutput as { _build: () => string })._build(),
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
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  private _clone(): ParticipantQueryBuilder<I> {
    const clone = new ParticipantQueryBuilder<I>();
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
  instruments: new InstrumentQueryBuilder(),
  jams: new JamQueryBuilder(),
  beats: new BeatQueryBuilder(),
  participants: new ParticipantQueryBuilder(),
  wasmSchema,
};
