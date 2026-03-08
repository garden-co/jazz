// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

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
  display_order?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface JamWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  created_at?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  transport_start?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  bpm?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  beat_count?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface BeatWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jam?: string | { eq?: string; ne?: string };
  instrument?: string | { eq?: string; ne?: string };
  beat_index?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  placed_by?: string | { eq?: string; ne?: string; contains?: string };
}

export interface ParticipantWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jam?: string | { eq?: string; ne?: string };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  display_name?: string | { eq?: string; ne?: string; contains?: string };
}

export interface InstrumentInclude {
  beatsViaInstrument?: true | BeatInclude | BeatQueryBuilder;
}

export interface JamInclude {
  beatsViaJam?: true | BeatInclude | BeatQueryBuilder;
  participantsViaJam?: true | ParticipantInclude | ParticipantQueryBuilder;
}

export interface BeatInclude {
  jam?: true | JamInclude | JamQueryBuilder;
  instrument?: true | InstrumentInclude | InstrumentQueryBuilder;
}

export interface ParticipantInclude {
  jam?: true | JamInclude | JamQueryBuilder;
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

export type InstrumentWithIncludes<I extends InstrumentInclude = {}> = Instrument & {
  beatsViaInstrument?: NonNullable<I["beatsViaInstrument"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Beat[]
      : RelationInclude extends BeatQueryBuilder<
            infer QueryInclude extends BeatInclude,
            infer QuerySelect extends keyof Beat
          >
        ? BeatSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends BeatInclude
          ? BeatWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type JamWithIncludes<I extends JamInclude = {}> = Jam & {
  beatsViaJam?: NonNullable<I["beatsViaJam"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Beat[]
      : RelationInclude extends BeatQueryBuilder<
            infer QueryInclude extends BeatInclude,
            infer QuerySelect extends keyof Beat
          >
        ? BeatSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends BeatInclude
          ? BeatWithIncludes<RelationInclude>[]
          : never
    : never;
  participantsViaJam?: NonNullable<I["participantsViaJam"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Participant[]
      : RelationInclude extends ParticipantQueryBuilder<
            infer QueryInclude extends ParticipantInclude,
            infer QuerySelect extends keyof Participant
          >
        ? ParticipantSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends ParticipantInclude
          ? ParticipantWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type BeatWithIncludes<I extends BeatInclude = {}> = Beat & {
  jam?: NonNullable<I["jam"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Jam
      : RelationInclude extends JamQueryBuilder<
            infer QueryInclude extends JamInclude,
            infer QuerySelect extends keyof Jam
          >
        ? JamSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends JamInclude
          ? JamWithIncludes<RelationInclude>
          : never
    : never;
  instrument?: NonNullable<I["instrument"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Instrument
      : RelationInclude extends InstrumentQueryBuilder<
            infer QueryInclude extends InstrumentInclude,
            infer QuerySelect extends keyof Instrument
          >
        ? InstrumentSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends InstrumentInclude
          ? InstrumentWithIncludes<RelationInclude>
          : never
    : never;
};

export type ParticipantWithIncludes<I extends ParticipantInclude = {}> = Participant & {
  jam?: NonNullable<I["jam"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Jam
      : RelationInclude extends JamQueryBuilder<
            infer QueryInclude extends JamInclude,
            infer QuerySelect extends keyof Jam
          >
        ? JamSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends JamInclude
          ? JamWithIncludes<RelationInclude>
          : never
    : never;
};

export type InstrumentSelected<S extends keyof Instrument = keyof Instrument> = Pick<
  Instrument,
  Extract<S | "id", keyof Instrument>
>;

export type InstrumentSelectedWithIncludes<
  I extends InstrumentInclude = {},
  S extends keyof Instrument = keyof Instrument,
> = InstrumentSelected<S> & Omit<InstrumentWithIncludes<I>, keyof Instrument>;

export type JamSelected<S extends keyof Jam = keyof Jam> = Pick<Jam, Extract<S | "id", keyof Jam>>;

export type JamSelectedWithIncludes<
  I extends JamInclude = {},
  S extends keyof Jam = keyof Jam,
> = JamSelected<S> & Omit<JamWithIncludes<I>, keyof Jam>;

export type BeatSelected<S extends keyof Beat = keyof Beat> = Pick<
  Beat,
  Extract<S | "id", keyof Beat>
>;

export type BeatSelectedWithIncludes<
  I extends BeatInclude = {},
  S extends keyof Beat = keyof Beat,
> = BeatSelected<S> & Omit<BeatWithIncludes<I>, keyof Beat>;

export type ParticipantSelected<S extends keyof Participant = keyof Participant> = Pick<
  Participant,
  Extract<S | "id", keyof Participant>
>;

export type ParticipantSelectedWithIncludes<
  I extends ParticipantInclude = {},
  S extends keyof Participant = keyof Participant,
> = ParticipantSelected<S> & Omit<ParticipantWithIncludes<I>, keyof Participant>;

export const wasmSchema: WasmSchema = {
  instruments: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "sound",
        column_type: {
          type: "Bytea",
        },
        nullable: false,
      },
      {
        name: "display_order",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
  },
  jams: {
    columns: [
      {
        name: "created_at",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "transport_start",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
      {
        name: "bpm",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "beat_count",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
  },
  beats: {
    columns: [
      {
        name: "jam",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "jams",
      },
      {
        name: "instrument",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "instruments",
      },
      {
        name: "beat_index",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "placed_by",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
    ],
  },
  participants: {
    columns: [
      {
        name: "jam",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "jams",
      },
      {
        name: "user_id",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "display_name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
    ],
  },
};

export class InstrumentQueryBuilder<
  I extends InstrumentInclude = {},
  S extends keyof Instrument = keyof Instrument,
> implements QueryBuilder<InstrumentSelectedWithIncludes<I, S>> {
  readonly _table = "instruments";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: InstrumentSelectedWithIncludes<I, S>;
  declare readonly _initType: InstrumentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<InstrumentInclude> = {};
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

  where(conditions: InstrumentWhereInput): InstrumentQueryBuilder<I, S> {
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

  select<NewS extends keyof Instrument>(
    ...columns: [NewS, ...NewS[]]
  ): InstrumentQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends InstrumentInclude>(relations: NewI): InstrumentQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(
    column: keyof Instrument,
    direction: "asc" | "desc" = "asc",
  ): InstrumentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): InstrumentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): InstrumentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "beatsViaInstrument"): InstrumentQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: InstrumentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): InstrumentQueryBuilder<I, S> {
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

  private _clone<
    CloneI extends InstrumentInclude = I,
    CloneS extends keyof Instrument = S,
  >(): InstrumentQueryBuilder<CloneI, CloneS> {
    const clone = new InstrumentQueryBuilder<CloneI, CloneS>();
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

export class JamQueryBuilder<
  I extends JamInclude = {},
  S extends keyof Jam = keyof Jam,
> implements QueryBuilder<JamSelectedWithIncludes<I, S>> {
  readonly _table = "jams";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: JamSelectedWithIncludes<I, S>;
  declare readonly _initType: JamInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JamInclude> = {};
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

  where(conditions: JamWhereInput): JamQueryBuilder<I, S> {
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

  select<NewS extends keyof Jam>(...columns: [NewS, ...NewS[]]): JamQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JamInclude>(relations: NewI): JamQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Jam, direction: "asc" | "desc" = "asc"): JamQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JamQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JamQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "beatsViaJam" | "participantsViaJam"): JamQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JamWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JamQueryBuilder<I, S> {
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

  private _clone<CloneI extends JamInclude = I, CloneS extends keyof Jam = S>(): JamQueryBuilder<
    CloneI,
    CloneS
  > {
    const clone = new JamQueryBuilder<CloneI, CloneS>();
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

export class BeatQueryBuilder<
  I extends BeatInclude = {},
  S extends keyof Beat = keyof Beat,
> implements QueryBuilder<BeatSelectedWithIncludes<I, S>> {
  readonly _table = "beats";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: BeatSelectedWithIncludes<I, S>;
  declare readonly _initType: BeatInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BeatInclude> = {};
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

  where(conditions: BeatWhereInput): BeatQueryBuilder<I, S> {
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

  select<NewS extends keyof Beat>(...columns: [NewS, ...NewS[]]): BeatQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BeatInclude>(relations: NewI): BeatQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Beat, direction: "asc" | "desc" = "asc"): BeatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BeatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BeatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "jam" | "instrument"): BeatQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BeatWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BeatQueryBuilder<I, S> {
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

  private _clone<CloneI extends BeatInclude = I, CloneS extends keyof Beat = S>(): BeatQueryBuilder<
    CloneI,
    CloneS
  > {
    const clone = new BeatQueryBuilder<CloneI, CloneS>();
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

export class ParticipantQueryBuilder<
  I extends ParticipantInclude = {},
  S extends keyof Participant = keyof Participant,
> implements QueryBuilder<ParticipantSelectedWithIncludes<I, S>> {
  readonly _table = "participants";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ParticipantSelectedWithIncludes<I, S>;
  declare readonly _initType: ParticipantInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ParticipantInclude> = {};
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

  where(conditions: ParticipantWhereInput): ParticipantQueryBuilder<I, S> {
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

  select<NewS extends keyof Participant>(
    ...columns: [NewS, ...NewS[]]
  ): ParticipantQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ParticipantInclude>(relations: NewI): ParticipantQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(
    column: keyof Participant,
    direction: "asc" | "desc" = "asc",
  ): ParticipantQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ParticipantQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ParticipantQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "jam"): ParticipantQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ParticipantWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ParticipantQueryBuilder<I, S> {
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

  private _clone<
    CloneI extends ParticipantInclude = I,
    CloneS extends keyof Participant = S,
  >(): ParticipantQueryBuilder<CloneI, CloneS> {
    const clone = new ParticipantQueryBuilder<CloneI, CloneS>();
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
  instruments: InstrumentQueryBuilder;
  jams: JamQueryBuilder;
  beats: BeatQueryBuilder;
  participants: ParticipantQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  instruments: new InstrumentQueryBuilder(),
  jams: new JamQueryBuilder(),
  beats: new BeatQueryBuilder(),
  participants: new ParticipantQueryBuilder(),
  wasmSchema,
};
