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
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
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
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BeatWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jam?: string | { eq?: string; ne?: string };
  instrument?: string | { eq?: string; ne?: string };
  beat_index?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  placed_by?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ParticipantWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jam?: string | { eq?: string; ne?: string };
  user_id?: string | { eq?: string; ne?: string; contains?: string };
  display_name?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyInstrumentQueryBuilder<T = any> = { readonly _table: "instruments" } & QueryBuilder<T>;
type AnyJamQueryBuilder<T = any> = { readonly _table: "jams" } & QueryBuilder<T>;
type AnyBeatQueryBuilder<T = any> = { readonly _table: "beats" } & QueryBuilder<T>;
type AnyParticipantQueryBuilder<T = any> = { readonly _table: "participants" } & QueryBuilder<T>;

export interface InstrumentInclude {
  beatsViaInstrument?: true | BeatInclude | AnyBeatQueryBuilder<any>;
}

export interface JamInclude {
  beatsViaJam?: true | BeatInclude | AnyBeatQueryBuilder<any>;
  participantsViaJam?: true | ParticipantInclude | AnyParticipantQueryBuilder<any>;
}

export interface BeatInclude {
  jam?: true | JamInclude | AnyJamQueryBuilder<any>;
  instrument?: true | InstrumentInclude | AnyInstrumentQueryBuilder<any>;
}

export interface ParticipantInclude {
  jam?: true | JamInclude | AnyJamQueryBuilder<any>;
}

export type InstrumentIncludedRelations<I extends InstrumentInclude = {}> = {
  [K in keyof I]-?: K extends "beatsViaInstrument"
    ? NonNullable<I["beatsViaInstrument"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? Beat[]
        : RelationInclude extends AnyBeatQueryBuilder<infer QueryRow>
          ? QueryRow[]
          : RelationInclude extends BeatInclude
            ? BeatWithIncludes<RelationInclude>[]
            : never
      : never
    : never;
};

export type JamIncludedRelations<I extends JamInclude = {}> = {
  [K in keyof I]-?: K extends "beatsViaJam"
    ? NonNullable<I["beatsViaJam"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? Beat[]
        : RelationInclude extends AnyBeatQueryBuilder<infer QueryRow>
          ? QueryRow[]
          : RelationInclude extends BeatInclude
            ? BeatWithIncludes<RelationInclude>[]
            : never
      : never
    : K extends "participantsViaJam"
      ? NonNullable<I["participantsViaJam"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Participant[]
          : RelationInclude extends AnyParticipantQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ParticipantInclude
              ? ParticipantWithIncludes<RelationInclude>[]
              : never
        : never
      : never;
};

export type BeatIncludedRelations<I extends BeatInclude = {}> = {
  [K in keyof I]-?: K extends "jam"
    ? NonNullable<I["jam"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? Jam
        : RelationInclude extends AnyJamQueryBuilder<infer QueryRow>
          ? QueryRow
          : RelationInclude extends JamInclude
            ? JamWithIncludes<RelationInclude>
            : never
      : never
    : K extends "instrument"
      ? NonNullable<I["instrument"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Instrument
          : RelationInclude extends AnyInstrumentQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends InstrumentInclude
              ? InstrumentWithIncludes<RelationInclude>
              : never
        : never
      : never;
};

export type ParticipantIncludedRelations<I extends ParticipantInclude = {}> = {
  [K in keyof I]-?: K extends "jam"
    ? NonNullable<I["jam"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? Jam
        : RelationInclude extends AnyJamQueryBuilder<infer QueryRow>
          ? QueryRow
          : RelationInclude extends JamInclude
            ? JamWithIncludes<RelationInclude>
            : never
      : never
    : never;
};

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

export type InstrumentWithIncludes<I extends InstrumentInclude = {}> = Omit<
  Instrument,
  Extract<keyof I, keyof Instrument>
> &
  InstrumentIncludedRelations<I>;

export type JamWithIncludes<I extends JamInclude = {}> = Omit<Jam, Extract<keyof I, keyof Jam>> &
  JamIncludedRelations<I>;

export type BeatWithIncludes<I extends BeatInclude = {}> = Omit<
  Beat,
  Extract<keyof I, keyof Beat>
> &
  BeatIncludedRelations<I>;

export type ParticipantWithIncludes<I extends ParticipantInclude = {}> = Omit<
  Participant,
  Extract<keyof I, keyof Participant>
> &
  ParticipantIncludedRelations<I>;

export type InstrumentSelectableColumn = keyof Instrument | PermissionIntrospectionColumn | "*";
export type InstrumentOrderableColumn = keyof Instrument | PermissionIntrospectionColumn;

export type InstrumentSelected<S extends InstrumentSelectableColumn = keyof Instrument> =
  "*" extends S
    ? Instrument
    : Pick<Instrument, Extract<S | "id", keyof Instrument>> &
        Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type InstrumentSelectedWithIncludes<
  I extends InstrumentInclude = {},
  S extends InstrumentSelectableColumn = keyof Instrument,
> = Omit<InstrumentSelected<S>, Extract<keyof I, keyof InstrumentSelected<S>>> &
  InstrumentIncludedRelations<I>;

export type JamSelectableColumn = keyof Jam | PermissionIntrospectionColumn | "*";
export type JamOrderableColumn = keyof Jam | PermissionIntrospectionColumn;

export type JamSelected<S extends JamSelectableColumn = keyof Jam> = "*" extends S
  ? Jam
  : Pick<Jam, Extract<S | "id", keyof Jam>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JamSelectedWithIncludes<
  I extends JamInclude = {},
  S extends JamSelectableColumn = keyof Jam,
> = Omit<JamSelected<S>, Extract<keyof I, keyof JamSelected<S>>> & JamIncludedRelations<I>;

export type BeatSelectableColumn = keyof Beat | PermissionIntrospectionColumn | "*";
export type BeatOrderableColumn = keyof Beat | PermissionIntrospectionColumn;

export type BeatSelected<S extends BeatSelectableColumn = keyof Beat> = "*" extends S
  ? Beat
  : Pick<Beat, Extract<S | "id", keyof Beat>> &
      Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BeatSelectedWithIncludes<
  I extends BeatInclude = {},
  S extends BeatSelectableColumn = keyof Beat,
> = Omit<BeatSelected<S>, Extract<keyof I, keyof BeatSelected<S>>> & BeatIncludedRelations<I>;

export type ParticipantSelectableColumn = keyof Participant | PermissionIntrospectionColumn | "*";
export type ParticipantOrderableColumn = keyof Participant | PermissionIntrospectionColumn;

export type ParticipantSelected<S extends ParticipantSelectableColumn = keyof Participant> =
  "*" extends S
    ? Participant
    : Pick<Participant, Extract<S | "id", keyof Participant>> &
        Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ParticipantSelectedWithIncludes<
  I extends ParticipantInclude = {},
  S extends ParticipantSelectableColumn = keyof Participant,
> = Omit<ParticipantSelected<S>, Extract<keyof I, keyof ParticipantSelected<S>>> &
  ParticipantIncludedRelations<I>;

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
  S extends InstrumentSelectableColumn = keyof Instrument,
> implements QueryBuilder<InstrumentSelectedWithIncludes<I, S>> {
  readonly _table = "instruments";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: InstrumentSelectedWithIncludes<I, S>;
  readonly _initType!: InstrumentInit;
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

  select<NewS extends InstrumentSelectableColumn>(
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
    column: InstrumentOrderableColumn,
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

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends InstrumentInclude = I,
    CloneS extends InstrumentSelectableColumn = S,
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
  S extends JamSelectableColumn = keyof Jam,
> implements QueryBuilder<JamSelectedWithIncludes<I, S>> {
  readonly _table = "jams";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JamSelectedWithIncludes<I, S>;
  readonly _initType!: JamInit;
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

  select<NewS extends JamSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): JamQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JamInclude>(relations: NewI): JamQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: JamOrderableColumn, direction: "asc" | "desc" = "asc"): JamQueryBuilder<I, S> {
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

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends JamInclude = I,
    CloneS extends JamSelectableColumn = S,
  >(): JamQueryBuilder<CloneI, CloneS> {
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
  S extends BeatSelectableColumn = keyof Beat,
> implements QueryBuilder<BeatSelectedWithIncludes<I, S>> {
  readonly _table = "beats";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BeatSelectedWithIncludes<I, S>;
  readonly _initType!: BeatInit;
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

  select<NewS extends BeatSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): BeatQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BeatInclude>(relations: NewI): BeatQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: BeatOrderableColumn, direction: "asc" | "desc" = "asc"): BeatQueryBuilder<I, S> {
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

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends BeatInclude = I,
    CloneS extends BeatSelectableColumn = S,
  >(): BeatQueryBuilder<CloneI, CloneS> {
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
  S extends ParticipantSelectableColumn = keyof Participant,
> implements QueryBuilder<ParticipantSelectedWithIncludes<I, S>> {
  readonly _table = "participants";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ParticipantSelectedWithIncludes<I, S>;
  readonly _initType!: ParticipantInit;
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

  select<NewS extends ParticipantSelectableColumn>(
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
    column: ParticipantOrderableColumn,
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

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ParticipantInclude = I,
    CloneS extends ParticipantSelectableColumn = S,
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
