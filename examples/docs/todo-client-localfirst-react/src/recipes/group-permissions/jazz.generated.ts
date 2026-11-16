// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output d8cdf2e1dc39d675. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type WorkspaceMemberRole = "reader" | "writer" | "contributor" | "admin";

export type Document = {
  readonly id: string;
  title: string;
  content: string;
  workspaceId: string;
};

export type DocumentInsert = {
  title: string;
  content: string;
  workspaceId: string;
};

export type WorkspaceMember = {
  readonly id: string;
  workspaceId: string;
  user_id: string;
  role: WorkspaceMemberRole;
};

export type WorkspaceMemberInsert = {
  workspaceId: string;
  user_id: string;
  role: WorkspaceMemberRole;
};

export type Workspace = {
  readonly id: string;
  name: string;
};

export type WorkspaceInsert = {
  name: string;
};

interface Types extends AppTypes {
  documents: {
    row: Document;
    insert: DocumentInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      content: { kind: "string"; nullable: false; value: string };
      workspaceId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  workspaceMembers: {
    row: WorkspaceMember;
    insert: WorkspaceMemberInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: WorkspaceMemberRole };
    };
    relations: {
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  workspaces: {
    row: Workspace;
    insert: WorkspaceInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      documents: { many: "documents" };
      members: { many: "workspaceMembers" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:dd9ccd85120c8b532c8e29876dc556cb98d6e3ae98f3a3d4551901cc99b8f2ea",
  "schema": {
    "current": "5e446851017d4c1a292f497ce169124a5d05d2b84e5c4c832dc053faaa2c6e6a",
    "schemas": [
      {
        "hash": "5e446851017d4c1a292f497ce169124a5d05d2b84e5c4c832dc053faaa2c6e6a",
        "schema": {
          "tables": {
            "documents": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "content", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
              ],
            },
            "workspaceMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                { "column_type": { "type": "Uuid" }, "name": "user_id", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["reader", "writer", "contributor", "admin"],
                  },
                  "name": "role",
                  "nullable": false,
                },
              ],
            },
            "workspaces": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "documents": { "workspace": { "one": "workspaces", "column": "workspaceId" } },
    "workspaceMembers": { "workspace": { "one": "workspaces", "column": "workspaceId" } },
    "workspaces": {
      "documents": { "many": "documents", "relation": "workspace" },
      "members": { "many": "workspaceMembers", "relation": "workspace" },
    },
  },
});
