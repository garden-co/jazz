// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface FilePart {
  id: string;
  data: Uint8Array;
}

export interface File {
  id: string;
  name?: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
}

export interface Instrument {
  id: string;
  name: string;
  soundFileId: string;
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
  jamId: string;
  instrumentId: string;
  beat_index: number;
  placed_by: string;
}

export interface Participant {
  id: string;
  jamId: string;
  userId: string;
  display_name: string;
}

export interface FilePartInit {
  data: Uint8Array;
}

export interface FileInit {
  name?: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
}

export interface InstrumentInit {
  name: string;
  soundFileId: string;
  display_order: number;
}

export interface JamInit {
  created_at: Date;
  transport_start?: Date;
  bpm: number;
  beat_count: number;
}

export interface BeatInit {
  jamId: string;
  instrumentId: string;
  beat_index: number;
  placed_by: string;
}

export interface ParticipantInit {
  jamId: string;
  userId: string;
  display_name: string;
}

export interface FilePartWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  data?: Uint8Array | { eq?: Uint8Array; ne?: Uint8Array };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface FileWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  mimeType?: string | { eq?: string; ne?: string; contains?: string };
  partIds?: string[] | { eq?: string[]; contains?: string };
  partSizes?: number[] | { eq?: number[]; contains?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface InstrumentWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  soundFileId?: string | { eq?: string; ne?: string };
  display_order?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface JamWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  transport_start?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  bpm?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  beat_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BeatWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jamId?: string | { eq?: string; ne?: string };
  instrumentId?: string | { eq?: string; ne?: string };
  beat_index?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  placed_by?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ParticipantWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  jamId?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  display_name?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyFilePartQueryBuilder<T = any> = { readonly _table: "file_parts" } & QueryBuilder<T>;
type AnyFileQueryBuilder<T = any> = { readonly _table: "files" } & QueryBuilder<T>;
type AnyInstrumentQueryBuilder<T = any> = { readonly _table: "instruments" } & QueryBuilder<T>;
type AnyJamQueryBuilder<T = any> = { readonly _table: "jams" } & QueryBuilder<T>;
type AnyBeatQueryBuilder<T = any> = { readonly _table: "beats" } & QueryBuilder<T>;
type AnyParticipantQueryBuilder<T = any> = { readonly _table: "participants" } & QueryBuilder<T>;

export interface FilePartInclude {
  filesViaParts?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export interface FileInclude {
  parts?: true | FilePartInclude | AnyFilePartQueryBuilder<any>;
  instrumentsViaSoundFile?: true | InstrumentInclude | AnyInstrumentQueryBuilder<any>;
}

export interface InstrumentInclude {
  soundFile?: true | FileInclude | AnyFileQueryBuilder<any>;
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

export type FilePartIncludedRelations<I extends FilePartInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "filesViaParts"
      ? NonNullable<I["filesViaParts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? File[]
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FileInclude
              ? FileWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type FileIncludedRelations<I extends FileInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "parts"
      ? NonNullable<I["parts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? FilePart[]
          : RelationInclude extends AnyFilePartQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FilePartInclude
              ? FilePartWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "instrumentsViaSoundFile"
      ? NonNullable<I["instrumentsViaSoundFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Instrument[]
          : RelationInclude extends AnyInstrumentQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends InstrumentInclude
              ? InstrumentWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type InstrumentIncludedRelations<I extends InstrumentInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "soundFile"
      ? NonNullable<I["soundFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? File : File | undefined
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends FileInclude
              ? R extends true ? FileWithIncludes<RelationInclude, false> : FileWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "beatsViaInstrument"
      ? NonNullable<I["beatsViaInstrument"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Beat[]
          : RelationInclude extends AnyBeatQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends BeatInclude
              ? BeatWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type JamIncludedRelations<I extends JamInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "beatsViaJam"
      ? NonNullable<I["beatsViaJam"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Beat[]
          : RelationInclude extends AnyBeatQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends BeatInclude
              ? BeatWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "participantsViaJam"
      ? NonNullable<I["participantsViaJam"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Participant[]
          : RelationInclude extends AnyParticipantQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ParticipantInclude
              ? ParticipantWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type BeatIncludedRelations<I extends BeatInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "jam"
      ? NonNullable<I["jam"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Jam : Jam | undefined
          : RelationInclude extends AnyJamQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JamInclude
              ? R extends true ? JamWithIncludes<RelationInclude, false> : JamWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "instrument"
      ? NonNullable<I["instrument"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Instrument : Instrument | undefined
          : RelationInclude extends AnyInstrumentQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends InstrumentInclude
              ? R extends true ? InstrumentWithIncludes<RelationInclude, false> : InstrumentWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type ParticipantIncludedRelations<I extends ParticipantInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "jam"
      ? NonNullable<I["jam"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Jam : Jam | undefined
          : RelationInclude extends AnyJamQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends JamInclude
              ? R extends true ? JamWithIncludes<RelationInclude, false> : JamWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface FilePartRelations {
  filesViaParts: File[];
}

export interface FileRelations {
  parts: FilePart[];
  instrumentsViaSoundFile: Instrument[];
}

export interface InstrumentRelations {
  soundFile: File | undefined;
  beatsViaInstrument: Beat[];
}

export interface JamRelations {
  beatsViaJam: Beat[];
  participantsViaJam: Participant[];
}

export interface BeatRelations {
  jam: Jam | undefined;
  instrument: Instrument | undefined;
}

export interface ParticipantRelations {
  jam: Jam | undefined;
}

export type FilePartWithIncludes<I extends FilePartInclude = {}, R extends boolean = false> = FilePart & FilePartIncludedRelations<I, R>;

export type FileWithIncludes<I extends FileInclude = {}, R extends boolean = false> = File & FileIncludedRelations<I, R>;

export type InstrumentWithIncludes<I extends InstrumentInclude = {}, R extends boolean = false> = Instrument & InstrumentIncludedRelations<I, R>;

export type JamWithIncludes<I extends JamInclude = {}, R extends boolean = false> = Jam & JamIncludedRelations<I, R>;

export type BeatWithIncludes<I extends BeatInclude = {}, R extends boolean = false> = Beat & BeatIncludedRelations<I, R>;

export type ParticipantWithIncludes<I extends ParticipantInclude = {}, R extends boolean = false> = Participant & ParticipantIncludedRelations<I, R>;

export type FilePartSelectableColumn = keyof FilePart | PermissionIntrospectionColumn | "*";
export type FilePartOrderableColumn = keyof FilePart | PermissionIntrospectionColumn;

export type FilePartSelected<S extends FilePartSelectableColumn = keyof FilePart> = "*" extends S ? FilePart : Pick<FilePart, Extract<S | "id", keyof FilePart>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FilePartSelectedWithIncludes<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> = FilePartSelected<S> & FilePartIncludedRelations<I, R>;

export type FileSelectableColumn = keyof File | PermissionIntrospectionColumn | "*";
export type FileOrderableColumn = keyof File | PermissionIntrospectionColumn;

export type FileSelected<S extends FileSelectableColumn = keyof File> = "*" extends S ? File : Pick<File, Extract<S | "id", keyof File>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FileSelectedWithIncludes<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> = FileSelected<S> & FileIncludedRelations<I, R>;

export type InstrumentSelectableColumn = keyof Instrument | PermissionIntrospectionColumn | "*";
export type InstrumentOrderableColumn = keyof Instrument | PermissionIntrospectionColumn;

export type InstrumentSelected<S extends InstrumentSelectableColumn = keyof Instrument> = "*" extends S ? Instrument : Pick<Instrument, Extract<S | "id", keyof Instrument>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type InstrumentSelectedWithIncludes<I extends InstrumentInclude = {}, S extends InstrumentSelectableColumn = keyof Instrument, R extends boolean = false> = InstrumentSelected<S> & InstrumentIncludedRelations<I, R>;

export type JamSelectableColumn = keyof Jam | PermissionIntrospectionColumn | "*";
export type JamOrderableColumn = keyof Jam | PermissionIntrospectionColumn;

export type JamSelected<S extends JamSelectableColumn = keyof Jam> = "*" extends S ? Jam : Pick<Jam, Extract<S | "id", keyof Jam>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type JamSelectedWithIncludes<I extends JamInclude = {}, S extends JamSelectableColumn = keyof Jam, R extends boolean = false> = JamSelected<S> & JamIncludedRelations<I, R>;

export type BeatSelectableColumn = keyof Beat | PermissionIntrospectionColumn | "*";
export type BeatOrderableColumn = keyof Beat | PermissionIntrospectionColumn;

export type BeatSelected<S extends BeatSelectableColumn = keyof Beat> = "*" extends S ? Beat : Pick<Beat, Extract<S | "id", keyof Beat>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BeatSelectedWithIncludes<I extends BeatInclude = {}, S extends BeatSelectableColumn = keyof Beat, R extends boolean = false> = BeatSelected<S> & BeatIncludedRelations<I, R>;

export type ParticipantSelectableColumn = keyof Participant | PermissionIntrospectionColumn | "*";
export type ParticipantOrderableColumn = keyof Participant | PermissionIntrospectionColumn;

export type ParticipantSelected<S extends ParticipantSelectableColumn = keyof Participant> = "*" extends S ? Participant : Pick<Participant, Extract<S | "id", keyof Participant>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ParticipantSelectedWithIncludes<I extends ParticipantInclude = {}, S extends ParticipantSelectableColumn = keyof Participant, R extends boolean = false> = ParticipantSelected<S> & ParticipantIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "file_parts": {
    "columns": [
      {
        "name": "data",
        "column_type": {
          "type": "Bytea"
        },
        "nullable": false
      }
    ]
  },
  "files": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "mimeType",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "partIds",
        "column_type": {
          "type": "Array",
          "element": {
            "type": "Uuid"
          }
        },
        "nullable": false,
        "references": "file_parts"
      },
      {
        "name": "partSizes",
        "column_type": {
          "type": "Array",
          "element": {
            "type": "Integer"
          }
        },
        "nullable": false
      }
    ]
  },
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
        "name": "soundFileId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "files"
      },
      {
        "name": "display_order",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      }
    ]
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
    ]
  },
  "beats": {
    "columns": [
      {
        "name": "jamId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "jams"
      },
      {
        "name": "instrumentId",
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
    ]
  },
  "participants": {
    "columns": [
      {
        "name": "jamId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "jams"
      },
      {
        "name": "userId",
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
    ]
  }
};

export class FilePartQueryBuilder<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> implements QueryBuilder<FilePartSelectedWithIncludes<I, S, R>> {
  readonly _table = "file_parts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: FilePartSelectedWithIncludes<I, S, R>;
  readonly _initType!: FilePartInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FilePartInclude> = {};
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

  where(conditions: FilePartWhereInput): FilePartQueryBuilder<I, S, R> {
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

  select<NewS extends FilePartSelectableColumn>(...columns: [NewS, ...NewS[]]): FilePartQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FilePartInclude>(relations: NewI): FilePartQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): FilePartQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: FilePartOrderableColumn, direction: "asc" | "desc" = "asc"): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "filesViaParts"): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FilePartWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FilePartQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends FilePartInclude = I, CloneS extends FilePartSelectableColumn = S, CloneR extends boolean = R>(): FilePartQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new FilePartQueryBuilder<CloneI, CloneS, CloneR>();
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

export class FileQueryBuilder<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> implements QueryBuilder<FileSelectedWithIncludes<I, S, R>> {
  readonly _table = "files";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: FileSelectedWithIncludes<I, S, R>;
  readonly _initType!: FileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FileInclude> = {};
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

  where(conditions: FileWhereInput): FileQueryBuilder<I, S, R> {
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

  select<NewS extends FileSelectableColumn>(...columns: [NewS, ...NewS[]]): FileQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FileInclude>(relations: NewI): FileQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): FileQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: FileOrderableColumn, direction: "asc" | "desc" = "asc"): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "parts" | "instrumentsViaSoundFile"): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FileQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends FileInclude = I, CloneS extends FileSelectableColumn = S, CloneR extends boolean = R>(): FileQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new FileQueryBuilder<CloneI, CloneS, CloneR>();
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

export class InstrumentQueryBuilder<I extends InstrumentInclude = {}, S extends InstrumentSelectableColumn = keyof Instrument, R extends boolean = false> implements QueryBuilder<InstrumentSelectedWithIncludes<I, S, R>> {
  readonly _table = "instruments";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: InstrumentSelectedWithIncludes<I, S, R>;
  readonly _initType!: InstrumentInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<InstrumentInclude> = {};
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

  where(conditions: InstrumentWhereInput): InstrumentQueryBuilder<I, S, R> {
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

  select<NewS extends InstrumentSelectableColumn>(...columns: [NewS, ...NewS[]]): InstrumentQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends InstrumentInclude>(relations: NewI): InstrumentQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): InstrumentQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: InstrumentOrderableColumn, direction: "asc" | "desc" = "asc"): InstrumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): InstrumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): InstrumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "soundFile" | "beatsViaInstrument"): InstrumentQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: InstrumentWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): InstrumentQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends InstrumentInclude = I, CloneS extends InstrumentSelectableColumn = S, CloneR extends boolean = R>(): InstrumentQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new InstrumentQueryBuilder<CloneI, CloneS, CloneR>();
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

export class JamQueryBuilder<I extends JamInclude = {}, S extends JamSelectableColumn = keyof Jam, R extends boolean = false> implements QueryBuilder<JamSelectedWithIncludes<I, S, R>> {
  readonly _table = "jams";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: JamSelectedWithIncludes<I, S, R>;
  readonly _initType!: JamInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<JamInclude> = {};
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

  where(conditions: JamWhereInput): JamQueryBuilder<I, S, R> {
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

  select<NewS extends JamSelectableColumn>(...columns: [NewS, ...NewS[]]): JamQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends JamInclude>(relations: NewI): JamQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): JamQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: JamOrderableColumn, direction: "asc" | "desc" = "asc"): JamQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): JamQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): JamQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "beatsViaJam" | "participantsViaJam"): JamQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: JamWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): JamQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends JamInclude = I, CloneS extends JamSelectableColumn = S, CloneR extends boolean = R>(): JamQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new JamQueryBuilder<CloneI, CloneS, CloneR>();
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

export class BeatQueryBuilder<I extends BeatInclude = {}, S extends BeatSelectableColumn = keyof Beat, R extends boolean = false> implements QueryBuilder<BeatSelectedWithIncludes<I, S, R>> {
  readonly _table = "beats";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BeatSelectedWithIncludes<I, S, R>;
  readonly _initType!: BeatInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BeatInclude> = {};
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

  where(conditions: BeatWhereInput): BeatQueryBuilder<I, S, R> {
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

  select<NewS extends BeatSelectableColumn>(...columns: [NewS, ...NewS[]]): BeatQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BeatInclude>(relations: NewI): BeatQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): BeatQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: BeatOrderableColumn, direction: "asc" | "desc" = "asc"): BeatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BeatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BeatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "jam" | "instrument"): BeatQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BeatWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BeatQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends BeatInclude = I, CloneS extends BeatSelectableColumn = S, CloneR extends boolean = R>(): BeatQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BeatQueryBuilder<CloneI, CloneS, CloneR>();
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

export class ParticipantQueryBuilder<I extends ParticipantInclude = {}, S extends ParticipantSelectableColumn = keyof Participant, R extends boolean = false> implements QueryBuilder<ParticipantSelectedWithIncludes<I, S, R>> {
  readonly _table = "participants";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ParticipantSelectedWithIncludes<I, S, R>;
  readonly _initType!: ParticipantInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ParticipantInclude> = {};
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

  where(conditions: ParticipantWhereInput): ParticipantQueryBuilder<I, S, R> {
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

  select<NewS extends ParticipantSelectableColumn>(...columns: [NewS, ...NewS[]]): ParticipantQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ParticipantInclude>(relations: NewI): ParticipantQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ParticipantQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ParticipantOrderableColumn, direction: "asc" | "desc" = "asc"): ParticipantQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ParticipantQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ParticipantQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "jam"): ParticipantQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ParticipantWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ParticipantQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends ParticipantInclude = I, CloneS extends ParticipantSelectableColumn = S, CloneR extends boolean = R>(): ParticipantQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ParticipantQueryBuilder<CloneI, CloneS, CloneR>();
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
  file_parts: FilePartQueryBuilder;
  files: FileQueryBuilder;
  instruments: InstrumentQueryBuilder;
  jams: JamQueryBuilder;
  beats: BeatQueryBuilder;
  participants: ParticipantQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  file_parts: new FilePartQueryBuilder(),
  files: new FileQueryBuilder(),
  instruments: new InstrumentQueryBuilder(),
  jams: new JamQueryBuilder(),
  beats: new BeatQueryBuilder(),
  participants: new ParticipantQueryBuilder(),
  wasmSchema,
};
