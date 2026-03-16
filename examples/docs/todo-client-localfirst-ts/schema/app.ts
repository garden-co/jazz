// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface Project {
  id: string;
  name: string;
}

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  ownerId?: string;
  parentId?: string;
  projectId?: string;
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

export interface Upload {
  id: string;
  ownerId: string;
  label: string;
  fileId: string;
}

export interface ProjectInit {
  name: string;
}

export interface TodoInit {
  title: string;
  done: boolean;
  description?: string;
  ownerId?: string;
  parentId?: string;
  projectId?: string;
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

export interface UploadInit {
  ownerId: string;
  label: string;
  fileId: string;
}

export interface ProjectWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  description?: string | { eq?: string; ne?: string; contains?: string };
  ownerId?: string | { eq?: string; ne?: string; contains?: string };
  parentId?: string | { eq?: string; ne?: string; isNull?: boolean };
  projectId?: string | { eq?: string; ne?: string; isNull?: boolean };
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

export interface UploadWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  ownerId?: string | { eq?: string; ne?: string; contains?: string };
  label?: string | { eq?: string; ne?: string; contains?: string };
  fileId?: string | { eq?: string; ne?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyProjectQueryBuilder<T = any> = { readonly _table: "projects" } & QueryBuilder<T>;
type AnyTodoQueryBuilder<T = any> = { readonly _table: "todos" } & QueryBuilder<T>;
type AnyFilePartQueryBuilder<T = any> = { readonly _table: "file_parts" } & QueryBuilder<T>;
type AnyFileQueryBuilder<T = any> = { readonly _table: "files" } & QueryBuilder<T>;
type AnyUploadQueryBuilder<T = any> = { readonly _table: "uploads" } & QueryBuilder<T>;

export interface ProjectInclude {
  todosViaProject?: true | TodoInclude | AnyTodoQueryBuilder<any>;
}

export interface TodoInclude {
  parent?: true | TodoInclude | AnyTodoQueryBuilder<any>;
  todosViaParent?: true | TodoInclude | AnyTodoQueryBuilder<any>;
  project?: true | ProjectInclude | AnyProjectQueryBuilder<any>;
}

export interface FilePartInclude {
  filesViaPart?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export interface FileInclude {
  part?: true | FilePartInclude | AnyFilePartQueryBuilder<any>;
  uploadsViaFile?: true | UploadInclude | AnyUploadQueryBuilder<any>;
}

export interface UploadInclude {
  file?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export type ProjectIncludedRelations<I extends ProjectInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "todosViaProject"
      ? NonNullable<I["todosViaProject"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Todo[]
          : RelationInclude extends AnyTodoQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends TodoInclude
              ? TodoWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type TodoIncludedRelations<I extends TodoInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "parent"
      ? NonNullable<I["parent"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Todo | undefined
          : RelationInclude extends AnyTodoQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends TodoInclude
              ? TodoWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "todosViaParent"
      ? NonNullable<I["todosViaParent"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Todo[]
          : RelationInclude extends AnyTodoQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends TodoInclude
              ? TodoWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "project"
      ? NonNullable<I["project"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Project | undefined
          : RelationInclude extends AnyProjectQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends ProjectInclude
              ? ProjectWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type FilePartIncludedRelations<I extends FilePartInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "filesViaPart"
      ? NonNullable<I["filesViaPart"]> extends infer RelationInclude
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
    K extends "part"
      ? NonNullable<I["part"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? FilePart[]
          : RelationInclude extends AnyFilePartQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FilePartInclude
              ? FilePartWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "uploadsViaFile"
      ? NonNullable<I["uploadsViaFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Upload[]
          : RelationInclude extends AnyUploadQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends UploadInclude
              ? UploadWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type UploadIncludedRelations<I extends UploadInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "file"
      ? NonNullable<I["file"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? File : File | undefined
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends FileInclude
              ? R extends true ? FileWithIncludes<RelationInclude, false> : FileWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface ProjectRelations {
  todosViaProject: Todo[];
}

export interface TodoRelations {
  parent: Todo | undefined;
  todosViaParent: Todo[];
  project: Project | undefined;
}

export interface FilePartRelations {
  filesViaPart: File[];
}

export interface FileRelations {
  part: FilePart[];
  uploadsViaFile: Upload[];
}

export interface UploadRelations {
  file: File | undefined;
}

export type ProjectWithIncludes<I extends ProjectInclude = {}, R extends boolean = false> = Omit<Project, Extract<keyof I, keyof Project>> & ProjectIncludedRelations<I, R>;

export type TodoWithIncludes<I extends TodoInclude = {}, R extends boolean = false> = Omit<Todo, Extract<keyof I, keyof Todo>> & TodoIncludedRelations<I, R>;

export type FilePartWithIncludes<I extends FilePartInclude = {}, R extends boolean = false> = Omit<FilePart, Extract<keyof I, keyof FilePart>> & FilePartIncludedRelations<I, R>;

export type FileWithIncludes<I extends FileInclude = {}, R extends boolean = false> = Omit<File, Extract<keyof I, keyof File>> & FileIncludedRelations<I, R>;

export type UploadWithIncludes<I extends UploadInclude = {}, R extends boolean = false> = Omit<Upload, Extract<keyof I, keyof Upload>> & UploadIncludedRelations<I, R>;

export type ProjectSelectableColumn = keyof Project | PermissionIntrospectionColumn | "*";
export type ProjectOrderableColumn = keyof Project | PermissionIntrospectionColumn;

export type ProjectSelected<S extends ProjectSelectableColumn = keyof Project> = "*" extends S ? Project : Pick<Project, Extract<S | "id", keyof Project>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ProjectSelectedWithIncludes<I extends ProjectInclude = {}, S extends ProjectSelectableColumn = keyof Project, R extends boolean = false> = Omit<ProjectSelected<S>, Extract<keyof I, keyof ProjectSelected<S>>> & ProjectIncludedRelations<I, R>;

export type TodoSelectableColumn = keyof Todo | PermissionIntrospectionColumn | "*";
export type TodoOrderableColumn = keyof Todo | PermissionIntrospectionColumn;

export type TodoSelected<S extends TodoSelectableColumn = keyof Todo> = "*" extends S ? Todo : Pick<Todo, Extract<S | "id", keyof Todo>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type TodoSelectedWithIncludes<I extends TodoInclude = {}, S extends TodoSelectableColumn = keyof Todo, R extends boolean = false> = Omit<TodoSelected<S>, Extract<keyof I, keyof TodoSelected<S>>> & TodoIncludedRelations<I, R>;

export type FilePartSelectableColumn = keyof FilePart | PermissionIntrospectionColumn | "*";
export type FilePartOrderableColumn = keyof FilePart | PermissionIntrospectionColumn;

export type FilePartSelected<S extends FilePartSelectableColumn = keyof FilePart> = "*" extends S ? FilePart : Pick<FilePart, Extract<S | "id", keyof FilePart>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FilePartSelectedWithIncludes<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> = Omit<FilePartSelected<S>, Extract<keyof I, keyof FilePartSelected<S>>> & FilePartIncludedRelations<I, R>;

export type FileSelectableColumn = keyof File | PermissionIntrospectionColumn | "*";
export type FileOrderableColumn = keyof File | PermissionIntrospectionColumn;

export type FileSelected<S extends FileSelectableColumn = keyof File> = "*" extends S ? File : Pick<File, Extract<S | "id", keyof File>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FileSelectedWithIncludes<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> = Omit<FileSelected<S>, Extract<keyof I, keyof FileSelected<S>>> & FileIncludedRelations<I, R>;

export type UploadSelectableColumn = keyof Upload | PermissionIntrospectionColumn | "*";
export type UploadOrderableColumn = keyof Upload | PermissionIntrospectionColumn;

export type UploadSelected<S extends UploadSelectableColumn = keyof Upload> = "*" extends S ? Upload : Pick<Upload, Extract<S | "id", keyof Upload>> & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type UploadSelectedWithIncludes<I extends UploadInclude = {}, S extends UploadSelectableColumn = keyof Upload, R extends boolean = false> = Omit<UploadSelected<S>, Extract<keyof I, keyof UploadSelected<S>>> & UploadIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "projects": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ]
  },
  "todos": {
    "columns": [
      {
        "name": "title",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "done",
        "column_type": {
          "type": "Boolean"
        },
        "nullable": false
      },
      {
        "name": "description",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "ownerId",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "parentId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "todos"
      },
      {
        "name": "projectId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "projects"
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
      },
      "delete": {
        "using": {
          "type": "True"
        }
      }
    }
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
  "uploads": {
    "columns": [
      {
        "name": "ownerId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "label",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "fileId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "files"
      }
    ]
  }
};

export class ProjectQueryBuilder<I extends ProjectInclude = {}, S extends ProjectSelectableColumn = keyof Project, R extends boolean = false> implements QueryBuilder<ProjectSelectedWithIncludes<I, S, R>> {
  readonly _table = "projects";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ProjectSelectedWithIncludes<I, S, R>;
  readonly _initType!: ProjectInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProjectInclude> = {};
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

  where(conditions: ProjectWhereInput): ProjectQueryBuilder<I, S, R> {
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

  select<NewS extends ProjectSelectableColumn>(...columns: [NewS, ...NewS[]]): ProjectQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProjectInclude>(relations: NewI): ProjectQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ProjectQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ProjectOrderableColumn, direction: "asc" | "desc" = "asc"): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "todosViaProject"): ProjectQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProjectWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ProjectQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends ProjectInclude = I, CloneS extends ProjectSelectableColumn = S, CloneR extends boolean = R>(): ProjectQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ProjectQueryBuilder<CloneI, CloneS, CloneR>();
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

export class TodoQueryBuilder<I extends TodoInclude = {}, S extends TodoSelectableColumn = keyof Todo, R extends boolean = false> implements QueryBuilder<TodoSelectedWithIncludes<I, S, R>> {
  readonly _table = "todos";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: TodoSelectedWithIncludes<I, S, R>;
  readonly _initType!: TodoInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<TodoInclude> = {};
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

  where(conditions: TodoWhereInput): TodoQueryBuilder<I, S, R> {
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

  select<NewS extends TodoSelectableColumn>(...columns: [NewS, ...NewS[]]): TodoQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends TodoInclude>(relations: NewI): TodoQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): TodoQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: TodoOrderableColumn, direction: "asc" | "desc" = "asc"): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "parent" | "todosViaParent" | "project"): TodoQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: TodoWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): TodoQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends TodoInclude = I, CloneS extends TodoSelectableColumn = S, CloneR extends boolean = R>(): TodoQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new TodoQueryBuilder<CloneI, CloneS, CloneR>();
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

  hopTo(relation: "filesViaPart"): FilePartQueryBuilder<I, S, R> {
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

  hopTo(relation: "part" | "uploadsViaFile"): FileQueryBuilder<I, S, R> {
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

export class UploadQueryBuilder<I extends UploadInclude = {}, S extends UploadSelectableColumn = keyof Upload, R extends boolean = false> implements QueryBuilder<UploadSelectedWithIncludes<I, S, R>> {
  readonly _table = "uploads";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: UploadSelectedWithIncludes<I, S, R>;
  readonly _initType!: UploadInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<UploadInclude> = {};
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

  where(conditions: UploadWhereInput): UploadQueryBuilder<I, S, R> {
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

  select<NewS extends UploadSelectableColumn>(...columns: [NewS, ...NewS[]]): UploadQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends UploadInclude>(relations: NewI): UploadQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): UploadQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: UploadOrderableColumn, direction: "asc" | "desc" = "asc"): UploadQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): UploadQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): UploadQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "file"): UploadQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: UploadWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): UploadQueryBuilder<I, S, R> {
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

  private _clone<CloneI extends UploadInclude = I, CloneS extends UploadSelectableColumn = S, CloneR extends boolean = R>(): UploadQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new UploadQueryBuilder<CloneI, CloneS, CloneR>();
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
  projects: ProjectQueryBuilder;
  todos: TodoQueryBuilder;
  file_parts: FilePartQueryBuilder;
  files: FileQueryBuilder;
  uploads: UploadQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  projects: new ProjectQueryBuilder(),
  todos: new TodoQueryBuilder(),
  file_parts: new FilePartQueryBuilder(),
  files: new FileQueryBuilder(),
  uploads: new UploadQueryBuilder(),
  wasmSchema,
};
