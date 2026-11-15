// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type FolderInviteRole = "viewer" | "editor";
export type FolderMemberRole = "viewer" | "editor";

export type File = {
  readonly id: string;
  folder_id: string;
  name: string;
  content_type: string;
  size_bytes: number;
  owner_id: string;
  contents: Uint8Array;
};

export type FileInsert = {
  folder_id: string;
  name: string;
  content_type: string;
  size_bytes: number;
  owner_id: string;
  contents: Uint8Array;
};

export type FolderInvite = {
  readonly id: string;
  folder_id: string;
  code: string;
  role: FolderInviteRole;
};

export type FolderInviteInsert = {
  folder_id: string;
  code: string;
  role: FolderInviteRole;
};

export type FolderMember = {
  readonly id: string;
  folder_id: string;
  user_id: string;
  role: FolderMemberRole;
  invite_code: string;
  folder_owner_id: string;
};

export type FolderMemberInsert = {
  folder_id: string;
  user_id: string;
  role: FolderMemberRole;
  invite_code: string;
  folder_owner_id: string;
};

export type Folder = {
  readonly id: string;
  name: string;
  owner_id: string;
  parent_id: string | null;
};

export type FolderInsert = {
  name: string;
  owner_id: string;
  parent_id?: string | null;
};

export type Profile = {
  readonly id: string;
  name: string;
};

export type ProfileInsert = {
  name: string;
};

interface Types extends AppTypes {
  files: {
    row: File;
    insert: FileInsert;
    columns: {
      folder_id: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      content_type: { kind: "string"; nullable: false; value: string };
      size_bytes: { kind: "int"; nullable: false; value: number };
      owner_id: { kind: "uuid"; nullable: false; value: string };
      contents: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {
      folder: { one: "folders"; nullable: false };
    };
  };
  folderInvites: {
    row: FolderInvite;
    insert: FolderInviteInsert;
    columns: {
      folder_id: { kind: "uuid"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: FolderInviteRole };
    };
    relations: {
      folder: { one: "folders"; nullable: false };
    };
  };
  folderMembers: {
    row: FolderMember;
    insert: FolderMemberInsert;
    columns: {
      folder_id: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: FolderMemberRole };
      invite_code: { kind: "string"; nullable: false; value: string };
      folder_owner_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      folder: { one: "folders"; nullable: false };
      folderOwner: { one: "profiles"; nullable: false };
      user: { one: "profiles"; nullable: false };
    };
  };
  folders: {
    row: Folder;
    insert: FolderInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      owner_id: { kind: "uuid"; nullable: false; value: string };
      parent_id: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      filesViaFolder: { many: "files" };
      parent: { one: "folders"; nullable: true };
    };
  };
  profiles: {
    row: Profile;
    insert: ProfileInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      hostedMemberships: { many: "folderMembers" };
      memberships: { many: "folderMembers" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:4f6b3bd87967ce2182fa1a61673af1cbd2132bdd78fc3ac54fc8a13d3551d1a1",
  "schema": {
    "current": "63f6bc222535168c0d2103c511fd1ad6c93fa24cf36df4aff0735964b3712314",
    "schemas": [
      {
        "hash": "63f6bc222535168c0d2103c511fd1ad6c93fa24cf36df4aff0735964b3712314",
        "schema": {
          "tables": {
            "files": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "folder_id",
                  "nullable": false,
                  "references": "folders",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "content_type", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "size_bytes", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "contents", "nullable": false },
              ],
              "indexed_columns": ["folder_id"],
            },
            "folderInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "folder_id",
                  "nullable": false,
                  "references": "folders",
                },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["viewer", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
            },
            "folderMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "folder_id",
                  "nullable": false,
                  "references": "folders",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "user_id",
                  "nullable": false,
                  "references": "profiles",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["viewer", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "invite_code", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "folder_owner_id",
                  "nullable": false,
                  "references": "profiles",
                },
              ],
              "indexed_columns": ["folder_id", "user_id"],
            },
            "folders": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parent_id",
                  "nullable": true,
                  "references": "folders",
                },
              ],
            },
            "profiles": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "files": { "folder": { "one": "folders", "column": "folder_id" } },
    "folderInvites": { "folder": { "one": "folders", "column": "folder_id" } },
    "folderMembers": {
      "folder": { "one": "folders", "column": "folder_id" },
      "folderOwner": { "one": "profiles", "column": "folder_owner_id" },
      "user": { "one": "profiles", "column": "user_id" },
    },
    "folders": {
      "filesViaFolder": { "many": "files", "relation": "folder" },
      "parent": { "one": "folders", "column": "parent_id" },
    },
    "profiles": {
      "hostedMemberships": { "many": "folderMembers", "relation": "folderOwner" },
      "memberships": { "many": "folderMembers", "relation": "user" },
    },
  },
});
