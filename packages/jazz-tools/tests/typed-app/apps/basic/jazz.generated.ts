// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type TableWithConstraintData = { datetime?: string; integer?: number; string?: string };
export type TableWithDefaultEnum = "a" | "b" | "c";
export type TableWithDefaultJson = { age?: number; name: string };

export type Project = {
  readonly id: string;
  name: string;
};

export type ProjectInsert = {
  name: string;
};

export type TableWithConstraint = {
  readonly id: string;
  data: TableWithConstraintData;
};

export type TableWithConstraintInsert = {
  data: TableWithConstraintData;
};

export type TableWithDefault = {
  readonly id: string;
  integer: number;
  float: number;
  bytes: Uint8Array;
  enum: TableWithDefaultEnum;
  json: TableWithDefaultJson;
  timestampDate: Date;
  timestampNumber: Date;
  string: string;
  array: string[];
  boolean: boolean;
  nullable: string | null;
  nullableInteger: number | null;
  refId: string | null;
};

export type TableWithDefaultInsert = {
  integer?: number;
  float?: number;
  bytes?: Uint8Array;
  enum?: TableWithDefaultEnum;
  json?: TableWithDefaultJson;
  timestampDate?: Date;
  timestampNumber?: Date;
  string?: string;
  array?: string[];
  boolean?: boolean;
  nullable?: string | null;
  nullableInteger?: number | null;
  refId?: string | null;
};

export type Todo = {
  readonly id: string;
  title: string;
  done: boolean;
  tags: string[];
  projectId: string;
  ownerId: string | null;
  assigneesIds: string[];
};

export type TodoInsert = {
  title: string;
  done?: boolean;
  tags?: string[];
  projectId: string;
  ownerId?: string | null;
  assigneesIds?: string[];
};

export type User = {
  readonly id: string;
  name: string;
  friendsIds: string[];
};

export type UserInsert = {
  name: string;
  friendsIds: string[];
};

interface Types extends AppTypes {
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      todosViaProject: { many: "todos" };
    };
  };
  table_with_constraints: {
    row: TableWithConstraint;
    insert: TableWithConstraintInsert;
    columns: {
      data: { kind: "json"; nullable: false; value: TableWithConstraintData };
    };
    relations: {};
  };
  table_with_defaults: {
    row: TableWithDefault;
    insert: TableWithDefaultInsert;
    columns: {
      integer: { kind: "int"; nullable: false; value: number };
      float: { kind: "float"; nullable: false; value: number };
      bytes: { kind: "bytes"; nullable: false; value: Uint8Array };
      enum: { kind: "enum"; nullable: false; value: TableWithDefaultEnum };
      json: { kind: "json"; nullable: false; value: TableWithDefaultJson };
      timestampDate: { kind: "timestamp"; nullable: false; value: Date };
      timestampNumber: { kind: "timestamp"; nullable: false; value: Date };
      string: { kind: "string"; nullable: false; value: string };
      array: { kind: "array"; nullable: false; value: string[] };
      boolean: { kind: "boolean"; nullable: false; value: boolean };
      nullable: { kind: "string"; nullable: true; value: string };
      nullableInteger: { kind: "int"; nullable: true; value: number };
      refId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      ref: { one: "todos"; nullable: true };
    };
  };
  todos: {
    row: Todo;
    insert: TodoInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      tags: { kind: "array"; nullable: false; value: string[] };
      projectId: { kind: "uuid"; nullable: false; value: string };
      ownerId: { kind: "uuid"; nullable: true; value: string };
      assigneesIds: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      assignees: { many: "users"; nullable: false };
      owner: { one: "users"; nullable: true };
      project: { one: "projects"; nullable: false };
      table_with_defaultsViaRef: { many: "table_with_defaults" };
    };
  };
  users: {
    row: User;
    insert: UserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      friendsIds: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      friends: { many: "users"; nullable: false };
      todosViaAssignees: { many: "todos" };
      todosViaOwner: { many: "todos" };
      usersViaFriends: { many: "users" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:2534fbe81910a8915b5de564b5a2be9a91cb3a0cbadd477a9140caeb169dd6da",
  "schema": {
    "current": "0df370750528d1ee8fd0af76d69725650040bc3eabac5c0930bf0304f85bc1b5",
    "schemas": [
      {
        "hash": "0df370750528d1ee8fd0af76d69725650040bc3eabac5c0930bf0304f85bc1b5",
        "schema": {
          "tables": {
            "projects": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "table_with_constraints": {
              "columns": [
                {
                  "column_type": {
                    "schema": {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "additionalProperties": false,
                      "properties": {
                        "datetime": {
                          "format": "date-time",
                          "pattern": "^(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))T(?:(?:[01]\\d|2[0-3]):[0-5]\\d(?::[0-5]\\d(?:\\.\\d+)?)?(?:Z))$",
                          "type": "string",
                        },
                        "integer": { "maximum": 10, "minimum": 0, "type": "integer" },
                        "string": { "maxLength": 10, "minLength": 1, "type": "string" },
                      },
                      "type": "object",
                    },
                    "type": "Json",
                  },
                  "name": "data",
                  "nullable": false,
                },
              ],
            },
            "table_with_defaults": {
              "columns": [
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 1 },
                  "name": "integer",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Double" },
                  "default": { "type": "Double", "value": 1.0 },
                  "name": "float",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Bytea" },
                  "default": { "type": "Bytea", "value": [0, 1, 255] },
                  "name": "bytes",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Enum", "variants": ["a", "b", "c"] },
                  "default": { "type": "Text", "value": "a" },
                  "name": "enum",
                  "nullable": false,
                },
                {
                  "column_type": {
                    "schema": {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "additionalProperties": false,
                      "properties": { "age": { "type": "number" }, "name": { "type": "string" } },
                      "required": ["name"],
                      "type": "object",
                    },
                    "type": "Json",
                  },
                  "default": { "type": "Text", "value": "{\"name\":\"default name\"}" },
                  "name": "json",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Timestamp" },
                  "default": { "type": "Timestamp", "value": 1767225600000 },
                  "name": "timestampDate",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Timestamp" },
                  "default": { "type": "Timestamp", "value": 0 },
                  "name": "timestampNumber",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Text" },
                  "default": { "type": "Text", "value": "default value" },
                  "name": "string",
                  "nullable": false,
                },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "default": {
                    "type": "Array",
                    "value": [
                      { "type": "Text", "value": "a" },
                      { "type": "Text", "value": "b" },
                      { "type": "Text", "value": "c" },
                    ],
                  },
                  "name": "array",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": true },
                  "name": "boolean",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Text" },
                  "default": { "type": "Null" },
                  "name": "nullable",
                  "nullable": true,
                },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Null" },
                  "name": "nullableInteger",
                  "nullable": true,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "default": { "type": "Uuid", "value": "00000000-0000-0000-0000-000000000000" },
                  "name": "refId",
                  "nullable": true,
                  "references": "todos",
                },
              ],
            },
            "todos": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "done",
                  "nullable": false,
                },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "default": { "type": "Array", "value": [] },
                  "name": "tags",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "projectId",
                  "nullable": false,
                  "references": "projects",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "ownerId",
                  "nullable": true,
                  "references": "users",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "default": { "type": "Array", "value": [] },
                  "name": "assigneesIds",
                  "nullable": false,
                  "references": "users",
                },
              ],
              "indexed_columns": ["done"],
            },
            "users": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "friendsIds",
                  "nullable": false,
                  "references": "users",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "projects": { "todosViaProject": { "many": "todos", "relation": "project" } },
    "table_with_constraints": {},
    "table_with_defaults": { "ref": { "one": "todos", "column": "refId" } },
    "todos": {
      "assignees": { "one": "users", "column": "assigneesIds" },
      "owner": { "one": "users", "column": "ownerId" },
      "project": { "one": "projects", "column": "projectId" },
      "table_with_defaultsViaRef": { "many": "table_with_defaults", "relation": "ref" },
    },
    "users": {
      "friends": { "one": "users", "column": "friendsIds" },
      "todosViaAssignees": { "many": "todos", "relation": "assignees" },
      "todosViaOwner": { "many": "todos", "relation": "owner" },
      "usersViaFriends": { "many": "users", "relation": "friends" },
    },
  },
});
