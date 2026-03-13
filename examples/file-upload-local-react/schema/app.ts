// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface File {
  id: string;
  name: string;
  mimeType: string;
  parts: string[];
  partSizes: number[];
}

export interface FilePart {
  id: string;
  data: Uint8Array;
}

export interface Upload {
  id: string;
  size: number;
  last_modified: Date;
  file_id: string;
  owner_id: string;
}

export interface FileInit {
  name: string;
  mimeType: string;
  parts: string[];
  partSizes: number[];
}

export interface FilePartInit {
  data: Uint8Array;
}

export interface UploadInit {
  size: number;
  last_modified: Date;
  file_id: string;
  owner_id: string;
}

export interface FileWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  mimeType?: string | { eq?: string; ne?: string; contains?: string };
  parts?: string[] | { eq?: string[]; contains?: string };
  partSizes?: number[] | { eq?: number[]; contains?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface FilePartWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  data?: Uint8Array | { eq?: Uint8Array; ne?: Uint8Array };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface UploadWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  size?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  last_modified?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  file_id?: string | { eq?: string; ne?: string };
  owner_id?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyFileQueryBuilder<T = any> = { readonly _table: "files" } & QueryBuilder<T>;
type AnyFilePartQueryBuilder<T = any> = { readonly _table: "file_parts" } & QueryBuilder<T>;
type AnyUploadQueryBuilder<T = any> = { readonly _table: "uploads" } & QueryBuilder<T>;

export interface FileInclude {
  parts?: true | FilePartInclude | AnyFilePartQueryBuilder<any>;
  uploadsViaFile?: true | UploadInclude | AnyUploadQueryBuilder<any>;
}

export interface FilePartInclude {
  filesViaParts?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export interface UploadInclude {
  file?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export type FileIncludedRelations<I extends FileInclude = {}> = {
  [K in keyof I]-?:
    K extends "parts"
      ? NonNullable<I["parts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? FilePart[]
          : RelationInclude extends AnyFilePartQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FilePartInclude
              ? FilePartWithIncludes<RelationInclude>[]
              : never
        : never
    : K extends "uploadsViaFile"
      ? NonNullable<I["uploadsViaFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Upload[]
          : RelationInclude extends AnyUploadQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends UploadInclude
              ? UploadWithIncludes<RelationInclude>[]
              : never
        : never
    : never;
};

export type FilePartIncludedRelations<I extends FilePartInclude = {}> = {
  [K in keyof I]-?:
    K extends "filesViaParts"
      ? NonNullable<I["filesViaParts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? File[]
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FileInclude
              ? FileWithIncludes<RelationInclude>[]
              : never
        : never
    : never;
};

export type UploadIncludedRelations<I extends UploadInclude = {}> = {
  [K in keyof I]-?:
    K extends "file"
      ? NonNullable<I["file"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? File
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? QueryRow
            : RelationInclude extends FileInclude
              ? FileWithIncludes<RelationInclude>
              : never
        : never
    : never;
};

export interface FileRelations {
  parts: FilePart[];
  uploadsViaFile: Upload[];
}

export interface FilePartRelations {
  filesViaParts: File[];
}

export interface UploadRelations {
  file: File;
}

export type FileWithIncludes<I extends FileInclude = {}> = Omit<File, Extract<keyof I, keyof File>> & FileIncludedRelations<I>;

export type FilePartWithIncludes<I extends FilePartInclude = {}> = Omit<FilePart, Extract<keyof I, keyof FilePart>> & FilePartIncludedRelations<I>;

export type UploadWithIncludes<I extends UploadInclude = {}> = Omit<Upload, Extract<keyof I, keyof Upload>> & UploadIncludedRelations<I>;

export type FileSelectableColumn = keyof File | PermissionIntrospectionColumn | "*";
export type FileOrderableColumn = keyof File | PermissionIntrospectionColumn;

export type FileSelected<S extends FileSelectableColumn = keyof File> = "*" extends S ? File : Pick<File, Extract<S | "id", keyof File>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FileSelectedWithIncludes<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File> = Omit<FileSelected<S>, Extract<keyof I, keyof FileSelected<S>>> & FileIncludedRelations<I>;

export type FilePartSelectableColumn = keyof FilePart | PermissionIntrospectionColumn | "*";
export type FilePartOrderableColumn = keyof FilePart | PermissionIntrospectionColumn;

export type FilePartSelected<S extends FilePartSelectableColumn = keyof FilePart> = "*" extends S ? FilePart : Pick<FilePart, Extract<S | "id", keyof FilePart>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FilePartSelectedWithIncludes<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart> = Omit<FilePartSelected<S>, Extract<keyof I, keyof FilePartSelected<S>>> & FilePartIncludedRelations<I>;

export type UploadSelectableColumn = keyof Upload | PermissionIntrospectionColumn | "*";
export type UploadOrderableColumn = keyof Upload | PermissionIntrospectionColumn;

export type UploadSelected<S extends UploadSelectableColumn = keyof Upload> = "*" extends S ? Upload : Pick<Upload, Extract<S | "id", keyof Upload>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type UploadSelectedWithIncludes<I extends UploadInclude = {}, S extends UploadSelectableColumn = keyof Upload> = Omit<UploadSelected<S>, Extract<keyof I, keyof UploadSelected<S>>> & UploadIncludedRelations<I>;

export const wasmSchema: WasmSchema = {
  "files": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "mimeType",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "parts",
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
  "uploads": {
    "columns": [
      {
        "name": "size",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "last_modified",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "file_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "files"
      },
      {
        "name": "owner_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ]
  }
};

export class FileQueryBuilder<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File> implements QueryBuilder<FileSelectedWithIncludes<I, S>> {
  readonly _table = "files";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: FileSelectedWithIncludes<I, S>;
  declare readonly _initType: FileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FileInclude> = {};
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

  where(conditions: FileWhereInput): FileQueryBuilder<I, S> {
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

  select<NewS extends FileSelectableColumn>(...columns: [NewS, ...NewS[]]): FileQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FileInclude>(relations: NewI): FileQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: FileOrderableColumn, direction: "asc" | "desc" = "asc"): FileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "parts" | "uploadsViaFile"): FileQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FileQueryBuilder<I, S> {
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

  private _clone<CloneI extends FileInclude = I, CloneS extends FileSelectableColumn = S>(): FileQueryBuilder<CloneI, CloneS> {
    const clone = new FileQueryBuilder<CloneI, CloneS>();
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

export class FilePartQueryBuilder<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart> implements QueryBuilder<FilePartSelectedWithIncludes<I, S>> {
  readonly _table = "file_parts";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: FilePartSelectedWithIncludes<I, S>;
  declare readonly _initType: FilePartInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FilePartInclude> = {};
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

  where(conditions: FilePartWhereInput): FilePartQueryBuilder<I, S> {
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

  select<NewS extends FilePartSelectableColumn>(...columns: [NewS, ...NewS[]]): FilePartQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FilePartInclude>(relations: NewI): FilePartQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: FilePartOrderableColumn, direction: "asc" | "desc" = "asc"): FilePartQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FilePartQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FilePartQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "filesViaParts"): FilePartQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FilePartWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FilePartQueryBuilder<I, S> {
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

  private _clone<CloneI extends FilePartInclude = I, CloneS extends FilePartSelectableColumn = S>(): FilePartQueryBuilder<CloneI, CloneS> {
    const clone = new FilePartQueryBuilder<CloneI, CloneS>();
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

export class UploadQueryBuilder<I extends UploadInclude = {}, S extends UploadSelectableColumn = keyof Upload> implements QueryBuilder<UploadSelectedWithIncludes<I, S>> {
  readonly _table = "uploads";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: UploadSelectedWithIncludes<I, S>;
  declare readonly _initType: UploadInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<UploadInclude> = {};
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

  where(conditions: UploadWhereInput): UploadQueryBuilder<I, S> {
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

  select<NewS extends UploadSelectableColumn>(...columns: [NewS, ...NewS[]]): UploadQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends UploadInclude>(relations: NewI): UploadQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: UploadOrderableColumn, direction: "asc" | "desc" = "asc"): UploadQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): UploadQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): UploadQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "file"): UploadQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: UploadWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): UploadQueryBuilder<I, S> {
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

  private _clone<CloneI extends UploadInclude = I, CloneS extends UploadSelectableColumn = S>(): UploadQueryBuilder<CloneI, CloneS> {
    const clone = new UploadQueryBuilder<CloneI, CloneS>();
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
  files: FileQueryBuilder;
  file_parts: FilePartQueryBuilder;
  uploads: UploadQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  files: new FileQueryBuilder(),
  file_parts: new FilePartQueryBuilder(),
  uploads: new UploadQueryBuilder(),
  wasmSchema,
};
