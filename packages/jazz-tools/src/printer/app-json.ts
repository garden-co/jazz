/** The parts of `jazz.app.json` / `jazz.permissions.json` the printer reads (SPEC §10.10). */

export type ColumnJson = string | ColumnObject;

export interface ColumnObject {
  type?: string;
  enum?: string[];
  json?: unknown;
  array?: ColumnJson;
  vector?: number;
  optional?: boolean;
  default?: unknown;
  merge?: string;
  references?: string;
  transform?: string;
  $comment?: string;
}

export type RelationJson =
  | { rel: string; column: string; $comment?: string }
  | { reverse: string; relation: string; $comment?: string };

/** A text, vector or geo index (SPEC 20.1). */
export type IndexJson =
  | {
      name: string;
      text: Record<string, number | { weight: number; length?: number }>;
      analyzer: string;
      prefix?: boolean;
      serverOnly?: boolean;
      $comment?: string;
    }
  | {
      name: string;
      vector: string;
      metric: string;
      ann?: string;
      serverOnly?: boolean;
      $comment?: string;
    }
  | { name: string; geo: string; serverOnly?: boolean; $comment?: string };

export interface TableJson {
  columns: Record<string, ColumnJson>;
  relations?: Record<string, RelationJson>;
  indexes?: (string | string[] | IndexJson)[];
  branchBy?: string[];
  history?: "latest";
  computed?: Record<string, { type: string; from: string[] }>;
  $comment?: string;
}

export interface MigrationJson {
  name?: string;
  from: string;
  to?: string;
  createTables?: string[];
  dropTables?: string[];
  renameTables?: Record<string, string>;
  migrate?: Record<string, Record<string, MigrateOpJson>>;
  $comment?: string;
}

export type MigrateOpJson =
  | { add: ColumnJson; default?: unknown }
  | { drop: ColumnJson; backwardsDefault?: unknown }
  | { renameFrom: string };

export interface AppJson {
  format: 1;
  tables: Record<string, TableJson>;
  history?: Record<string, { tables: Record<string, TableJson>; $comment?: string }>;
  migrations?: MigrationJson[];
  transforms?: Record<string, { stored: ColumnJson; view: ColumnJson }>;
}

export type PolicyJson =
  | string
  | { anyOf: PolicyJson[] }
  | { allOf: PolicyJson[] }
  | { $comment: string; where: PolicyJson }
  | { using: PolicyJson; withCheck: PolicyJson };

export interface TablePoliciesJson {
  read?: PolicyJson;
  insert?: PolicyJson;
  update?: PolicyJson | { old: PolicyJson; new: PolicyJson };
  delete?: PolicyJson;
  $comment?: string;
}

export interface PermissionsJson {
  format: 1;
  defs?: Record<string, PolicyJson>;
  tables: Record<string, TablePoliciesJson>;
}

/** A column's scalar type without `?`, or the kind of object it is. */
export function columnKind(column: ColumnJson | undefined): string | undefined {
  if (column === undefined) return undefined;
  if (typeof column === "string") return column.replace(/\?$/, "");
  if (column.type) return column.type;
  if (column.enum) return "enum";
  if (column.json !== undefined) return "json";
  if (column.array !== undefined) return "array";
  if (column.vector !== undefined) return "vector";
  return undefined;
}
