// AUTO-GENERATED FILE - DO NOT EDIT

// Regenerate via: node scripts/generate-app.mjs

// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean;
  $canEdit: boolean;
  $canDelete: boolean;
}

export interface ClaudeSession {
  id: string;
  session_id: string;
  transcript_path: string;
  cwd: string;
  project_root: string;
  repo_root?: string;
  git_branch?: string;
  entrypoint?: string;
  user_type?: string;
  cli_version?: string;
  first_user_message?: string;
  latest_user_message?: string;
  latest_assistant_message?: string;
  latest_preview?: string;
  status: string;
  user_turn_count: number;
  assistant_turn_count: number;
  total_entries: number;
  created_at: Date;
  updated_at: Date;
  latest_activity_at: Date;
  last_user_at?: Date;
  last_assistant_at?: Date;
  metadata_json?: JsonValue;
}

export interface ClaudeTurn {
  id: string;
  turn_uuid: string;
  session_id: string;
  session_row_id: string;
  sequence: number;
  parent_uuid?: string;
  role: string;
  text?: string;
  timestamp?: Date;
  updated_at: Date;
}

export interface ClaudeSessionPresence {
  id: string;
  session_id: string;
  session_row_id: string;
  project_root: string;
  repo_root?: string;
  cwd: string;
  state: string;
  current_turn_uuid?: string;
  current_turn_row_id?: string;
  started_at: Date;
  latest_activity_at: Date;
  last_event_at: Date;
  last_user_at?: Date;
  last_assistant_at?: Date;
  last_synced_at: Date;
  runtime_pid?: number;
}

export interface ClaudeSessionInit {
  session_id: string;
  transcript_path: string;
  cwd: string;
  project_root: string;
  repo_root?: string | null;
  git_branch?: string | null;
  entrypoint?: string | null;
  user_type?: string | null;
  cli_version?: string | null;
  first_user_message?: string | null;
  latest_user_message?: string | null;
  latest_assistant_message?: string | null;
  latest_preview?: string | null;
  status: string;
  user_turn_count: number;
  assistant_turn_count: number;
  total_entries: number;
  created_at: Date;
  updated_at: Date;
  latest_activity_at: Date;
  last_user_at?: Date | null;
  last_assistant_at?: Date | null;
  metadata_json?: JsonValue | null;
}

export interface ClaudeTurnInit {
  turn_uuid: string;
  session_id: string;
  session_row_id: string;
  sequence: number;
  parent_uuid?: string | null;
  role: string;
  text?: string | null;
  timestamp?: Date | null;
  updated_at: Date;
}

export interface ClaudeSessionPresenceInit {
  session_id: string;
  session_row_id: string;
  project_root: string;
  repo_root?: string | null;
  cwd: string;
  state: string;
  current_turn_uuid?: string | null;
  current_turn_row_id?: string | null;
  started_at: Date;
  latest_activity_at: Date;
  last_event_at: Date;
  last_user_at?: Date | null;
  last_assistant_at?: Date | null;
  last_synced_at: Date;
  runtime_pid?: number | null;
}

export interface ClaudeSessionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  transcript_path?: string | { eq?: string; ne?: string; contains?: string };
  cwd?: string | { eq?: string; ne?: string; contains?: string };
  project_root?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  git_branch?: string | { eq?: string; ne?: string; contains?: string };
  entrypoint?: string | { eq?: string; ne?: string; contains?: string };
  user_type?: string | { eq?: string; ne?: string; contains?: string };
  cli_version?: string | { eq?: string; ne?: string; contains?: string };
  first_user_message?: string | { eq?: string; ne?: string; contains?: string };
  latest_user_message?: string | { eq?: string; ne?: string; contains?: string };
  latest_assistant_message?: string | { eq?: string; ne?: string; contains?: string };
  latest_preview?: string | { eq?: string; ne?: string; contains?: string };
  status?: string | { eq?: string; ne?: string; contains?: string };
  user_turn_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  assistant_turn_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  total_entries?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  latest_activity_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_user_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_assistant_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ClaudeTurnWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  turn_uuid?: string | { eq?: string; ne?: string; contains?: string };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  session_row_id?: string | { eq?: string; ne?: string };
  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  parent_uuid?: string | { eq?: string; ne?: string; contains?: string };
  role?: string | { eq?: string; ne?: string; contains?: string };
  text?: string | { eq?: string; ne?: string; contains?: string };
  timestamp?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface ClaudeSessionPresenceWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  session_id?: string | { eq?: string; ne?: string; contains?: string };
  session_row_id?: string | { eq?: string; ne?: string };
  project_root?: string | { eq?: string; ne?: string; contains?: string };
  repo_root?: string | { eq?: string; ne?: string; contains?: string };
  cwd?: string | { eq?: string; ne?: string; contains?: string };
  state?: string | { eq?: string; ne?: string; contains?: string };
  current_turn_uuid?: string | { eq?: string; ne?: string; contains?: string };
  current_turn_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };
  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  latest_activity_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_event_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_user_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_assistant_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  last_synced_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  runtime_pid?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyClaudeSessionQueryBuilder<T = any> = { readonly _table: "claude_sessions" } & QueryBuilder<T>;
type AnyClaudeTurnQueryBuilder<T = any> = { readonly _table: "claude_turns" } & QueryBuilder<T>;
type AnyClaudeSessionPresenceQueryBuilder<T = any> = { readonly _table: "claude_session_presence" } & QueryBuilder<T>;

export interface ClaudeSessionInclude {
  claude_turnsViaSession_row?: true | ClaudeTurnInclude | AnyClaudeTurnQueryBuilder<any>;
  claude_session_presenceViaSession_row?: true | ClaudeSessionPresenceInclude | AnyClaudeSessionPresenceQueryBuilder<any>;
}

export interface ClaudeTurnInclude {
  session_row?: true | ClaudeSessionInclude | AnyClaudeSessionQueryBuilder<any>;
  claude_session_presenceViaCurrent_turn_row?: true | ClaudeSessionPresenceInclude | AnyClaudeSessionPresenceQueryBuilder<any>;
}

export interface ClaudeSessionPresenceInclude {
  session_row?: true | ClaudeSessionInclude | AnyClaudeSessionQueryBuilder<any>;
  current_turn_row?: true | ClaudeTurnInclude | AnyClaudeTurnQueryBuilder<any>;
}

export type ClaudeSessionIncludedRelations<I extends ClaudeSessionInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "claude_turnsViaSession_row"
      ? NonNullable<I["claude_turnsViaSession_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? ClaudeTurn[]
          : RelationInclude extends AnyClaudeTurnQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ClaudeTurnInclude
              ? ClaudeTurnWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "claude_session_presenceViaSession_row"
      ? NonNullable<I["claude_session_presenceViaSession_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? ClaudeSessionPresence[]
          : RelationInclude extends AnyClaudeSessionPresenceQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ClaudeSessionPresenceInclude
              ? ClaudeSessionPresenceWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type ClaudeTurnIncludedRelations<I extends ClaudeTurnInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "session_row"
      ? NonNullable<I["session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? ClaudeSession : ClaudeSession | undefined
          : RelationInclude extends AnyClaudeSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends ClaudeSessionInclude
              ? R extends true ? ClaudeSessionWithIncludes<RelationInclude, false> : ClaudeSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "claude_session_presenceViaCurrent_turn_row"
      ? NonNullable<I["claude_session_presenceViaCurrent_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? ClaudeSessionPresence[]
          : RelationInclude extends AnyClaudeSessionPresenceQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends ClaudeSessionPresenceInclude
              ? ClaudeSessionPresenceWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type ClaudeSessionPresenceIncludedRelations<I extends ClaudeSessionPresenceInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "session_row"
      ? NonNullable<I["session_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? ClaudeSession : ClaudeSession | undefined
          : RelationInclude extends AnyClaudeSessionQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends ClaudeSessionInclude
              ? R extends true ? ClaudeSessionWithIncludes<RelationInclude, false> : ClaudeSessionWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "current_turn_row"
      ? NonNullable<I["current_turn_row"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? ClaudeTurn | undefined
          : RelationInclude extends AnyClaudeTurnQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends ClaudeTurnInclude
              ? ClaudeTurnWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface ClaudeSessionRelations {
  claude_turnsViaSession_row: ClaudeTurn[];
  claude_session_presenceViaSession_row: ClaudeSessionPresence[];
}

export interface ClaudeTurnRelations {
  session_row: ClaudeSession | undefined;
  claude_session_presenceViaCurrent_turn_row: ClaudeSessionPresence[];
}

export interface ClaudeSessionPresenceRelations {
  session_row: ClaudeSession | undefined;
  current_turn_row: ClaudeTurn | undefined;
}

export type ClaudeSessionWithIncludes<I extends ClaudeSessionInclude = {}, R extends boolean = false> = ClaudeSession & ClaudeSessionIncludedRelations<I, R>;

export type ClaudeTurnWithIncludes<I extends ClaudeTurnInclude = {}, R extends boolean = false> = ClaudeTurn & ClaudeTurnIncludedRelations<I, R>;

export type ClaudeSessionPresenceWithIncludes<I extends ClaudeSessionPresenceInclude = {}, R extends boolean = false> = ClaudeSessionPresence & ClaudeSessionPresenceIncludedRelations<I, R>;

export type ClaudeSessionSelectableColumn = keyof ClaudeSession | PermissionIntrospectionColumn | "*";
export type ClaudeSessionOrderableColumn = keyof ClaudeSession | PermissionIntrospectionColumn;

export type ClaudeSessionSelected<S extends ClaudeSessionSelectableColumn = keyof ClaudeSession> = ("*" extends S ? ClaudeSession : Pick<ClaudeSession, Extract<S | "id", keyof ClaudeSession>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ClaudeSessionSelectedWithIncludes<I extends ClaudeSessionInclude = {}, S extends ClaudeSessionSelectableColumn = keyof ClaudeSession, R extends boolean = false> = ClaudeSessionSelected<S> & ClaudeSessionIncludedRelations<I, R>;

export type ClaudeTurnSelectableColumn = keyof ClaudeTurn | PermissionIntrospectionColumn | "*";
export type ClaudeTurnOrderableColumn = keyof ClaudeTurn | PermissionIntrospectionColumn;

export type ClaudeTurnSelected<S extends ClaudeTurnSelectableColumn = keyof ClaudeTurn> = ("*" extends S ? ClaudeTurn : Pick<ClaudeTurn, Extract<S | "id", keyof ClaudeTurn>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ClaudeTurnSelectedWithIncludes<I extends ClaudeTurnInclude = {}, S extends ClaudeTurnSelectableColumn = keyof ClaudeTurn, R extends boolean = false> = ClaudeTurnSelected<S> & ClaudeTurnIncludedRelations<I, R>;

export type ClaudeSessionPresenceSelectableColumn = keyof ClaudeSessionPresence | PermissionIntrospectionColumn | "*";
export type ClaudeSessionPresenceOrderableColumn = keyof ClaudeSessionPresence | PermissionIntrospectionColumn;

export type ClaudeSessionPresenceSelected<S extends ClaudeSessionPresenceSelectableColumn = keyof ClaudeSessionPresence> = ("*" extends S ? ClaudeSessionPresence : Pick<ClaudeSessionPresence, Extract<S | "id", keyof ClaudeSessionPresence>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type ClaudeSessionPresenceSelectedWithIncludes<I extends ClaudeSessionPresenceInclude = {}, S extends ClaudeSessionPresenceSelectableColumn = keyof ClaudeSessionPresence, R extends boolean = false> = ClaudeSessionPresenceSelected<S> & ClaudeSessionPresenceIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "claude_sessions": {
    "columns": [
      {
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "transcript_path",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "cwd",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "project_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "git_branch",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "entrypoint",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "user_type",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "cli_version",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "first_user_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_user_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_assistant_message",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "latest_preview",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "status",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "user_turn_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "assistant_turn_count",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "total_entries",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "created_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "latest_activity_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "last_user_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_assistant_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "metadata_json",
        "column_type": {
          "type": "Json"
        },
        "nullable": true
      }
    ]
  },
  "claude_turns": {
    "columns": [
      {
        "name": "turn_uuid",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "claude_sessions"
      },
      {
        "name": "sequence",
        "column_type": {
          "type": "Integer"
        },
        "nullable": false
      },
      {
        "name": "parent_uuid",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "role",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "text",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "timestamp",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "updated_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ]
  },
  "claude_session_presence": {
    "columns": [
      {
        "name": "session_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "session_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "claude_sessions"
      },
      {
        "name": "project_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "repo_root",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "cwd",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "state",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "current_turn_uuid",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      },
      {
        "name": "current_turn_row_id",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "claude_turns"
      },
      {
        "name": "started_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "latest_activity_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "last_event_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "last_user_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_assistant_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": true
      },
      {
        "name": "last_synced_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "runtime_pid",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      }
    ]
  }
};

export class ClaudeSessionQueryBuilder<I extends ClaudeSessionInclude = {}, S extends ClaudeSessionSelectableColumn = keyof ClaudeSession, R extends boolean = false> implements QueryBuilder<ClaudeSessionSelectedWithIncludes<I, S, R>> {
  readonly _table = "claude_sessions";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ClaudeSessionSelectedWithIncludes<I, S, R>;
  readonly _initType!: ClaudeSessionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ClaudeSessionInclude> = {};
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

  where(conditions: ClaudeSessionWhereInput): ClaudeSessionQueryBuilder<I, S, R> {
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

  select<NewS extends ClaudeSessionSelectableColumn>(...columns: [NewS, ...NewS[]]): ClaudeSessionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ClaudeSessionInclude>(relations: NewI): ClaudeSessionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ClaudeSessionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ClaudeSessionOrderableColumn, direction: "asc" | "desc" = "asc"): ClaudeSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ClaudeSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ClaudeSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "claude_turnsViaSession_row" | "claude_session_presenceViaSession_row"): ClaudeSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ClaudeSessionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ClaudeSessionQueryBuilder<I, S, R> {
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
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
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

  private _clone<CloneI extends ClaudeSessionInclude = I, CloneS extends ClaudeSessionSelectableColumn = S, CloneR extends boolean = R>(): ClaudeSessionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ClaudeSessionQueryBuilder<CloneI, CloneS, CloneR>();
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

export class ClaudeTurnQueryBuilder<I extends ClaudeTurnInclude = {}, S extends ClaudeTurnSelectableColumn = keyof ClaudeTurn, R extends boolean = false> implements QueryBuilder<ClaudeTurnSelectedWithIncludes<I, S, R>> {
  readonly _table = "claude_turns";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ClaudeTurnSelectedWithIncludes<I, S, R>;
  readonly _initType!: ClaudeTurnInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ClaudeTurnInclude> = {};
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

  where(conditions: ClaudeTurnWhereInput): ClaudeTurnQueryBuilder<I, S, R> {
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

  select<NewS extends ClaudeTurnSelectableColumn>(...columns: [NewS, ...NewS[]]): ClaudeTurnQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ClaudeTurnInclude>(relations: NewI): ClaudeTurnQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ClaudeTurnQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ClaudeTurnOrderableColumn, direction: "asc" | "desc" = "asc"): ClaudeTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ClaudeTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ClaudeTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "session_row" | "claude_session_presenceViaCurrent_turn_row"): ClaudeTurnQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ClaudeTurnWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ClaudeTurnQueryBuilder<I, S, R> {
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
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
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

  private _clone<CloneI extends ClaudeTurnInclude = I, CloneS extends ClaudeTurnSelectableColumn = S, CloneR extends boolean = R>(): ClaudeTurnQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ClaudeTurnQueryBuilder<CloneI, CloneS, CloneR>();
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

export class ClaudeSessionPresenceQueryBuilder<I extends ClaudeSessionPresenceInclude = {}, S extends ClaudeSessionPresenceSelectableColumn = keyof ClaudeSessionPresence, R extends boolean = false> implements QueryBuilder<ClaudeSessionPresenceSelectedWithIncludes<I, S, R>> {
  readonly _table = "claude_session_presence";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: ClaudeSessionPresenceSelectedWithIncludes<I, S, R>;
  readonly _initType!: ClaudeSessionPresenceInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ClaudeSessionPresenceInclude> = {};
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

  where(conditions: ClaudeSessionPresenceWhereInput): ClaudeSessionPresenceQueryBuilder<I, S, R> {
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

  select<NewS extends ClaudeSessionPresenceSelectableColumn>(...columns: [NewS, ...NewS[]]): ClaudeSessionPresenceQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ClaudeSessionPresenceInclude>(relations: NewI): ClaudeSessionPresenceQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): ClaudeSessionPresenceQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: ClaudeSessionPresenceOrderableColumn, direction: "asc" | "desc" = "asc"): ClaudeSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ClaudeSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ClaudeSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "session_row" | "current_turn_row"): ClaudeSessionPresenceQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ClaudeSessionPresenceWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ClaudeSessionPresenceQueryBuilder<I, S, R> {
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
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
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

  private _clone<CloneI extends ClaudeSessionPresenceInclude = I, CloneS extends ClaudeSessionPresenceSelectableColumn = S, CloneR extends boolean = R>(): ClaudeSessionPresenceQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new ClaudeSessionPresenceQueryBuilder<CloneI, CloneS, CloneR>();
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
  claude_sessions: ClaudeSessionQueryBuilder;
  claude_turns: ClaudeTurnQueryBuilder;
  claude_session_presence: ClaudeSessionPresenceQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  claude_sessions: new ClaudeSessionQueryBuilder(),
  claude_turns: new ClaudeTurnQueryBuilder(),
  claude_session_presence: new ClaudeSessionPresenceQueryBuilder(),
  wasmSchema,
};


export interface ClaudeSessionWhereInput {,  id?: string | { eq?: string; ne?: string; in?: string[] };,  session_id?: string | { eq?: string; ne?: string; contains?: string };,  transcript_path?: string | { eq?: string; ne?: string; contains?: string };,  cwd?: string | { eq?: string; ne?: string; contains?: string };,  project_root?: string | { eq?: string; ne?: string; contains?: string };,  repo_root?: string | { eq?: string; ne?: string; contains?: string };,  git_branch?: string | { eq?: string; ne?: string; contains?: string };,  entrypoint?: string | { eq?: string; ne?: string; contains?: string };,  user_type?: string | { eq?: string; ne?: string; contains?: string };,  cli_version?: string | { eq?: string; ne?: string; contains?: string };,  first_user_message?: string | { eq?: string; ne?: string; contains?: string };,  latest_user_message?: string | { eq?: string; ne?: string; contains?: string };,  latest_assistant_message?: string | { eq?: string; ne?: string; contains?: string };,  latest_preview?: string | { eq?: string; ne?: string; contains?: string };,  status?: string | { eq?: string; ne?: string; contains?: string };,  user_turn_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };,  assistant_turn_count?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };,  total_entries?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };,  created_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  latest_activity_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  last_user_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  last_assistant_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  metadata_json?: JsonValue | { eq?: JsonValue; ne?: JsonValue; in?: JsonValue[] };,  $canRead?: boolean;,  $canEdit?: boolean;,  $canDelete?: boolean;,},,export interface ClaudeTurnWhereInput {,  id?: string | { eq?: string; ne?: string; in?: string[] };,  turn_uuid?: string | { eq?: string; ne?: string; contains?: string };,  session_id?: string | { eq?: string; ne?: string; contains?: string };,  session_row_id?: string | { eq?: string; ne?: string };,  sequence?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };,  parent_uuid?: string | { eq?: string; ne?: string; contains?: string };,  role?: string | { eq?: string; ne?: string; contains?: string };,  text?: string | { eq?: string; ne?: string; contains?: string };,  timestamp?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  updated_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  $canRead?: boolean;,  $canEdit?: boolean;,  $canDelete?: boolean;,},,export interface ClaudeSessionPresenceWhereInput {,  id?: string | { eq?: string; ne?: string; in?: string[] };,  session_id?: string | { eq?: string; ne?: string; contains?: string };,  session_row_id?: string | { eq?: string; ne?: string };,  project_root?: string | { eq?: string; ne?: string; contains?: string };,  repo_root?: string | { eq?: string; ne?: string; contains?: string };,  cwd?: string | { eq?: string; ne?: string; contains?: string };,  state?: string | { eq?: string; ne?: string; contains?: string };,  current_turn_uuid?: string | { eq?: string; ne?: string; contains?: string };,  current_turn_row_id?: string | { eq?: string; ne?: string; isNull?: boolean };,  started_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  latest_activity_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  last_event_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  last_user_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  last_assistant_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  last_synced_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };,  runtime_pid?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };,  $canRead?: boolean;,  $canEdit?: boolean;,  $canDelete?: boolean;,},

export class ClaudeSessionQueryBuilder<I extends ClaudeSessionInclude = {}, S extends ClaudeSessionSelectableColumn = keyof ClaudeSession, R extends boolean = false> implements QueryBuilder<ClaudeSessionSelectedWithIncludes<I, S, R>> {,  readonly _table = "claude_sessions";,  readonly _schema: WasmSchema = wasmSchema;,  readonly _rowType!: ClaudeSessionSelectedWithIncludes<I, S, R>;,  readonly _initType!: ClaudeSessionInit;,  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];,  private _includes: Partial<ClaudeSessionInclude> = {};,  private _requireIncludes = false;,  private _selectColumns?: string[];,  private _orderBys: Array<[string, "asc" | "desc"]> = [];,  private _limitVal?: number;,  private _offsetVal?: number;,  private _hops: string[] = [];,  private _gatherVal?: {,    max_depth: number;,    step_table: string;,    step_current_column: string;,    step_conditions: Array<{ column: string; op: string; value: unknown }>;,    step_hops: string[];,  };,,  where(conditions: ClaudeSessionWhereInput): ClaudeSessionQueryBuilder<I, S, R> {,    const clone = this._clone();,    for (const [key, value] of Object.entries(conditions)) {,      if (value === undefined) continue;,      if (typeof value === "object" && value !== null && !Array.isArray(value)) {,        for (const [op, opValue] of Object.entries(value)) {,          if (opValue !== undefined) {,            clone._conditions.push({ column: key, op, value: opValue });,          },        },      } else {,        clone._conditions.push({ column: key, op: "eq", value });,      },    },    return clone;,  },,  select<NewS extends ClaudeSessionSelectableColumn>(...columns: [NewS, ...NewS[]]): ClaudeSessionQueryBuilder<I, NewS, R> {,    const clone = this._clone<I, NewS, R>();,    clone._selectColumns = [...columns] as string[];,    return clone;,  },,  include<NewI extends ClaudeSessionInclude>(relations: NewI): ClaudeSessionQueryBuilder<I & NewI, S, R> {,    const clone = this._clone<I & NewI, S, R>();,    clone._includes = { ...this._includes, ...relations };,    return clone;,  },,  requireIncludes(): ClaudeSessionQueryBuilder<I, S, true> {,    const clone = this._clone<I, S, true>();,    clone._requireIncludes = true;,    return clone;,  },,  orderBy(column: ClaudeSessionOrderableColumn, direction: "asc" | "desc" = "asc"): ClaudeSessionQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._orderBys.push([column as string, direction]);,    return clone;,  },,  limit(n: number): ClaudeSessionQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._limitVal = n;,    return clone;,  },,  offset(n: number): ClaudeSessionQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._offsetVal = n;,    return clone;,  },,  hopTo(relation: "claude_turnsViaSession_row" | "claude_session_presenceViaSession_row"): ClaudeSessionQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._hops.push(relation);,    return clone;,  },,  gather(options: {,    start: ClaudeSessionWhereInput;,    step: (ctx: { current: string }) => QueryBuilder<unknown>;,    maxDepth?: number;,  }): ClaudeSessionQueryBuilder<I, S, R> {,    if (options.start === undefined) {,      throw new Error("gather(...) requires start where conditions.");,    },    if (typeof options.step !== "function") {,      throw new Error("gather(...) requires step callback.");,    },,    const maxDepth = options.maxDepth ?? 10;,    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {,      throw new Error("gather(...) maxDepth must be a positive integer.");,    },    if (Object.keys(this._includes).length > 0) {,      throw new Error("gather(...) does not support include(...) in MVP.");,    },    if (this._hops.length > 0) {,      throw new Error("gather(...) must be called before hopTo(...).");,    },,    const currentToken = "__jazz_gather_current__";,    const stepOutput = options.step({ current: currentToken });,    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {,      throw new Error("gather(...) step must return a query expression built from app.<table>.");,    },,    const stepBuilt = JSON.parse(,      stepOutput._build(),,    ) as {,      table?: unknown;,      conditions?: Array<{ column: string; op: string; value: unknown }>;,      hops?: unknown;,    };,,    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {,      throw new Error("gather(...) step query is missing table metadata.");,    },    if (!Array.isArray(stepBuilt.conditions)) {,      throw new Error("gather(...) step query is missing condition metadata.");,    },,    const stepHops = Array.isArray(stepBuilt.hops),      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string"),      : [];,    if (stepHops.length !== 1) {,      throw new Error("gather(...) step must include exactly one hopTo(...).");,    },,    const currentConditions = stepBuilt.conditions.filter(,      (condition) => condition.op === "eq" && condition.value === currentToken,,    );,    if (currentConditions.length !== 1) {,      throw new Error("gather(...) step must include exactly one where condition bound to current.");,    },,    const currentCondition = currentConditions[0];,    if (currentCondition === undefined) {,      throw new Error("gather(...) step must include exactly one where condition bound to current.");,    },    const stepConditions = stepBuilt.conditions.filter(,      (condition) => !(condition.op === "eq" && condition.value === currentToken),,    );,,    const withStart = this.where(options.start);,    const clone = withStart._clone();,    clone._hops = [];,    clone._gatherVal = {,      max_depth: maxDepth,,      step_table: stepBuilt.table,,      step_current_column: currentCondition.column,,      step_conditions: stepConditions,,      step_hops: stepHops,,    };,,    return clone;,  },,  _build(): string {,    return JSON.stringify({,      table: this._table,,      conditions: this._conditions,,      includes: this._includes,,      __jazz_requireIncludes: this._requireIncludes || undefined,,      select: this._selectColumns,,      orderBy: this._orderBys,,      limit: this._limitVal,,      offset: this._offsetVal,,      hops: this._hops,,      gather: this._gatherVal,,    });,  },,  toJSON(): unknown {,    return JSON.parse(this._build());,  },,  private _clone<CloneI extends ClaudeSessionInclude = I, CloneS extends ClaudeSessionSelectableColumn = S, CloneR extends boolean = R>(): ClaudeSessionQueryBuilder<CloneI, CloneS, CloneR> {,    const clone = new ClaudeSessionQueryBuilder<CloneI, CloneS, CloneR>();,    clone._conditions = [...this._conditions];,    clone._includes = { ...this._includes };,    clone._requireIncludes = this._requireIncludes;,    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;,    clone._orderBys = [...this._orderBys];,    clone._limitVal = this._limitVal;,    clone._offsetVal = this._offsetVal;,    clone._hops = [...this._hops];,    clone._gatherVal = this._gatherVal,      ? {,          ...this._gatherVal,,          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),,          step_hops: [...this._gatherVal.step_hops],,        },      : undefined;,    return clone;,  },},,export class ClaudeTurnQueryBuilder<I extends ClaudeTurnInclude = {}, S extends ClaudeTurnSelectableColumn = keyof ClaudeTurn, R extends boolean = false> implements QueryBuilder<ClaudeTurnSelectedWithIncludes<I, S, R>> {,  readonly _table = "claude_turns";,  readonly _schema: WasmSchema = wasmSchema;,  readonly _rowType!: ClaudeTurnSelectedWithIncludes<I, S, R>;,  readonly _initType!: ClaudeTurnInit;,  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];,  private _includes: Partial<ClaudeTurnInclude> = {};,  private _requireIncludes = false;,  private _selectColumns?: string[];,  private _orderBys: Array<[string, "asc" | "desc"]> = [];,  private _limitVal?: number;,  private _offsetVal?: number;,  private _hops: string[] = [];,  private _gatherVal?: {,    max_depth: number;,    step_table: string;,    step_current_column: string;,    step_conditions: Array<{ column: string; op: string; value: unknown }>;,    step_hops: string[];,  };,,  where(conditions: ClaudeTurnWhereInput): ClaudeTurnQueryBuilder<I, S, R> {,    const clone = this._clone();,    for (const [key, value] of Object.entries(conditions)) {,      if (value === undefined) continue;,      if (typeof value === "object" && value !== null && !Array.isArray(value)) {,        for (const [op, opValue] of Object.entries(value)) {,          if (opValue !== undefined) {,            clone._conditions.push({ column: key, op, value: opValue });,          },        },      } else {,        clone._conditions.push({ column: key, op: "eq", value });,      },    },    return clone;,  },,  select<NewS extends ClaudeTurnSelectableColumn>(...columns: [NewS, ...NewS[]]): ClaudeTurnQueryBuilder<I, NewS, R> {,    const clone = this._clone<I, NewS, R>();,    clone._selectColumns = [...columns] as string[];,    return clone;,  },,  include<NewI extends ClaudeTurnInclude>(relations: NewI): ClaudeTurnQueryBuilder<I & NewI, S, R> {,    const clone = this._clone<I & NewI, S, R>();,    clone._includes = { ...this._includes, ...relations };,    return clone;,  },,  requireIncludes(): ClaudeTurnQueryBuilder<I, S, true> {,    const clone = this._clone<I, S, true>();,    clone._requireIncludes = true;,    return clone;,  },,  orderBy(column: ClaudeTurnOrderableColumn, direction: "asc" | "desc" = "asc"): ClaudeTurnQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._orderBys.push([column as string, direction]);,    return clone;,  },,  limit(n: number): ClaudeTurnQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._limitVal = n;,    return clone;,  },,  offset(n: number): ClaudeTurnQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._offsetVal = n;,    return clone;,  },,  hopTo(relation: "session_row" | "claude_session_presenceViaCurrent_turn_row"): ClaudeTurnQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._hops.push(relation);,    return clone;,  },,  gather(options: {,    start: ClaudeTurnWhereInput;,    step: (ctx: { current: string }) => QueryBuilder<unknown>;,    maxDepth?: number;,  }): ClaudeTurnQueryBuilder<I, S, R> {,    if (options.start === undefined) {,      throw new Error("gather(...) requires start where conditions.");,    },    if (typeof options.step !== "function") {,      throw new Error("gather(...) requires step callback.");,    },,    const maxDepth = options.maxDepth ?? 10;,    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {,      throw new Error("gather(...) maxDepth must be a positive integer.");,    },    if (Object.keys(this._includes).length > 0) {,      throw new Error("gather(...) does not support include(...) in MVP.");,    },    if (this._hops.length > 0) {,      throw new Error("gather(...) must be called before hopTo(...).");,    },,    const currentToken = "__jazz_gather_current__";,    const stepOutput = options.step({ current: currentToken });,    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {,      throw new Error("gather(...) step must return a query expression built from app.<table>.");,    },,    const stepBuilt = JSON.parse(,      stepOutput._build(),,    ) as {,      table?: unknown;,      conditions?: Array<{ column: string; op: string; value: unknown }>;,      hops?: unknown;,    };,,    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {,      throw new Error("gather(...) step query is missing table metadata.");,    },    if (!Array.isArray(stepBuilt.conditions)) {,      throw new Error("gather(...) step query is missing condition metadata.");,    },,    const stepHops = Array.isArray(stepBuilt.hops),      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string"),      : [];,    if (stepHops.length !== 1) {,      throw new Error("gather(...) step must include exactly one hopTo(...).");,    },,    const currentConditions = stepBuilt.conditions.filter(,      (condition) => condition.op === "eq" && condition.value === currentToken,,    );,    if (currentConditions.length !== 1) {,      throw new Error("gather(...) step must include exactly one where condition bound to current.");,    },,    const currentCondition = currentConditions[0];,    if (currentCondition === undefined) {,      throw new Error("gather(...) step must include exactly one where condition bound to current.");,    },    const stepConditions = stepBuilt.conditions.filter(,      (condition) => !(condition.op === "eq" && condition.value === currentToken),,    );,,    const withStart = this.where(options.start);,    const clone = withStart._clone();,    clone._hops = [];,    clone._gatherVal = {,      max_depth: maxDepth,,      step_table: stepBuilt.table,,      step_current_column: currentCondition.column,,      step_conditions: stepConditions,,      step_hops: stepHops,,    };,,    return clone;,  },,  _build(): string {,    return JSON.stringify({,      table: this._table,,      conditions: this._conditions,,      includes: this._includes,,      __jazz_requireIncludes: this._requireIncludes || undefined,,      select: this._selectColumns,,      orderBy: this._orderBys,,      limit: this._limitVal,,      offset: this._offsetVal,,      hops: this._hops,,      gather: this._gatherVal,,    });,  },,  toJSON(): unknown {,    return JSON.parse(this._build());,  },,  private _clone<CloneI extends ClaudeTurnInclude = I, CloneS extends ClaudeTurnSelectableColumn = S, CloneR extends boolean = R>(): ClaudeTurnQueryBuilder<CloneI, CloneS, CloneR> {,    const clone = new ClaudeTurnQueryBuilder<CloneI, CloneS, CloneR>();,    clone._conditions = [...this._conditions];,    clone._includes = { ...this._includes };,    clone._requireIncludes = this._requireIncludes;,    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;,    clone._orderBys = [...this._orderBys];,    clone._limitVal = this._limitVal;,    clone._offsetVal = this._offsetVal;,    clone._hops = [...this._hops];,    clone._gatherVal = this._gatherVal,      ? {,          ...this._gatherVal,,          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),,          step_hops: [...this._gatherVal.step_hops],,        },      : undefined;,    return clone;,  },},,export class ClaudeSessionPresenceQueryBuilder<I extends ClaudeSessionPresenceInclude = {}, S extends ClaudeSessionPresenceSelectableColumn = keyof ClaudeSessionPresence, R extends boolean = false> implements QueryBuilder<ClaudeSessionPresenceSelectedWithIncludes<I, S, R>> {,  readonly _table = "claude_session_presence";,  readonly _schema: WasmSchema = wasmSchema;,  readonly _rowType!: ClaudeSessionPresenceSelectedWithIncludes<I, S, R>;,  readonly _initType!: ClaudeSessionPresenceInit;,  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];,  private _includes: Partial<ClaudeSessionPresenceInclude> = {};,  private _requireIncludes = false;,  private _selectColumns?: string[];,  private _orderBys: Array<[string, "asc" | "desc"]> = [];,  private _limitVal?: number;,  private _offsetVal?: number;,  private _hops: string[] = [];,  private _gatherVal?: {,    max_depth: number;,    step_table: string;,    step_current_column: string;,    step_conditions: Array<{ column: string; op: string; value: unknown }>;,    step_hops: string[];,  };,,  where(conditions: ClaudeSessionPresenceWhereInput): ClaudeSessionPresenceQueryBuilder<I, S, R> {,    const clone = this._clone();,    for (const [key, value] of Object.entries(conditions)) {,      if (value === undefined) continue;,      if (typeof value === "object" && value !== null && !Array.isArray(value)) {,        for (const [op, opValue] of Object.entries(value)) {,          if (opValue !== undefined) {,            clone._conditions.push({ column: key, op, value: opValue });,          },        },      } else {,        clone._conditions.push({ column: key, op: "eq", value });,      },    },    return clone;,  },,  select<NewS extends ClaudeSessionPresenceSelectableColumn>(...columns: [NewS, ...NewS[]]): ClaudeSessionPresenceQueryBuilder<I, NewS, R> {,    const clone = this._clone<I, NewS, R>();,    clone._selectColumns = [...columns] as string[];,    return clone;,  },,  include<NewI extends ClaudeSessionPresenceInclude>(relations: NewI): ClaudeSessionPresenceQueryBuilder<I & NewI, S, R> {,    const clone = this._clone<I & NewI, S, R>();,    clone._includes = { ...this._includes, ...relations };,    return clone;,  },,  requireIncludes(): ClaudeSessionPresenceQueryBuilder<I, S, true> {,    const clone = this._clone<I, S, true>();,    clone._requireIncludes = true;,    return clone;,  },,  orderBy(column: ClaudeSessionPresenceOrderableColumn, direction: "asc" | "desc" = "asc"): ClaudeSessionPresenceQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._orderBys.push([column as string, direction]);,    return clone;,  },,  limit(n: number): ClaudeSessionPresenceQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._limitVal = n;,    return clone;,  },,  offset(n: number): ClaudeSessionPresenceQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._offsetVal = n;,    return clone;,  },,  hopTo(relation: "session_row" | "current_turn_row"): ClaudeSessionPresenceQueryBuilder<I, S, R> {,    const clone = this._clone();,    clone._hops.push(relation);,    return clone;,  },,  gather(options: {,    start: ClaudeSessionPresenceWhereInput;,    step: (ctx: { current: string }) => QueryBuilder<unknown>;,    maxDepth?: number;,  }): ClaudeSessionPresenceQueryBuilder<I, S, R> {,    if (options.start === undefined) {,      throw new Error("gather(...) requires start where conditions.");,    },    if (typeof options.step !== "function") {,      throw new Error("gather(...) requires step callback.");,    },,    const maxDepth = options.maxDepth ?? 10;,    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {,      throw new Error("gather(...) maxDepth must be a positive integer.");,    },    if (Object.keys(this._includes).length > 0) {,      throw new Error("gather(...) does not support include(...) in MVP.");,    },    if (this._hops.length > 0) {,      throw new Error("gather(...) must be called before hopTo(...).");,    },,    const currentToken = "__jazz_gather_current__";,    const stepOutput = options.step({ current: currentToken });,    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {,      throw new Error("gather(...) step must return a query expression built from app.<table>.");,    },,    const stepBuilt = JSON.parse(,      stepOutput._build(),,    ) as {,      table?: unknown;,      conditions?: Array<{ column: string; op: string; value: unknown }>;,      hops?: unknown;,    };,,    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {,      throw new Error("gather(...) step query is missing table metadata.");,    },    if (!Array.isArray(stepBuilt.conditions)) {,      throw new Error("gather(...) step query is missing condition metadata.");,    },,    const stepHops = Array.isArray(stepBuilt.hops),      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string"),      : [];,    if (stepHops.length !== 1) {,      throw new Error("gather(...) step must include exactly one hopTo(...).");,    },,    const currentConditions = stepBuilt.conditions.filter(,      (condition) => condition.op === "eq" && condition.value === currentToken,,    );,    if (currentConditions.length !== 1) {,      throw new Error("gather(...) step must include exactly one where condition bound to current.");,    },,    const currentCondition = currentConditions[0];,    if (currentCondition === undefined) {,      throw new Error("gather(...) step must include exactly one where condition bound to current.");,    },    const stepConditions = stepBuilt.conditions.filter(,      (condition) => !(condition.op === "eq" && condition.value === currentToken),,    );,,    const withStart = this.where(options.start);,    const clone = withStart._clone();,    clone._hops = [];,    clone._gatherVal = {,      max_depth: maxDepth,,      step_table: stepBuilt.table,,      step_current_column: currentCondition.column,,      step_conditions: stepConditions,,      step_hops: stepHops,,    };,,    return clone;,  },,  _build(): string {,    return JSON.stringify({,      table: this._table,,      conditions: this._conditions,,      includes: this._includes,,      __jazz_requireIncludes: this._requireIncludes || undefined,,      select: this._selectColumns,,      orderBy: this._orderBys,,      limit: this._limitVal,,      offset: this._offsetVal,,      hops: this._hops,,      gather: this._gatherVal,,    });,  },,  toJSON(): unknown {,    return JSON.parse(this._build());,  },,  private _clone<CloneI extends ClaudeSessionPresenceInclude = I, CloneS extends ClaudeSessionPresenceSelectableColumn = S, CloneR extends boolean = R>(): ClaudeSessionPresenceQueryBuilder<CloneI, CloneS, CloneR> {,    const clone = new ClaudeSessionPresenceQueryBuilder<CloneI, CloneS, CloneR>();,    clone._conditions = [...this._conditions];,    clone._includes = { ...this._includes };,    clone._requireIncludes = this._requireIncludes;,    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;,    clone._orderBys = [...this._orderBys];,    clone._limitVal = this._limitVal;,    clone._offsetVal = this._offsetVal;,    clone._hops = [...this._hops];,    clone._gatherVal = this._gatherVal,      ? {,          ...this._gatherVal,,          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),,          step_hops: [...this._gatherVal.step_hops],,        },      : undefined;,    return clone;,  },},

export interface GeneratedApp {,  claude_sessions: ClaudeSessionQueryBuilder;,  claude_turns: ClaudeTurnQueryBuilder;,  claude_session_presence: ClaudeSessionPresenceQueryBuilder;,  wasmSchema: WasmSchema;,},,export const app: GeneratedApp = {,  claude_sessions: new ClaudeSessionQueryBuilder(),,  claude_turns: new ClaudeTurnQueryBuilder(),,  claude_session_presence: new ClaudeSessionPresenceQueryBuilder(),,  wasmSchema,,};,
