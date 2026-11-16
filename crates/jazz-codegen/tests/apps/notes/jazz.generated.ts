// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 93dcf6a2b45fb655. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Folder = {
  readonly id: string;
  name: string;
};

export type FolderInsert = {
  name: string;
};

export type Note = {
  readonly id: string;
  body: string;
  pinned: boolean;
  folderId: string | null;
};

export type NoteInsert = {
  body: string;
  pinned?: boolean;
  folderId?: string | null;
};

interface Types extends AppTypes {
  folders: {
    row: Folder;
    insert: FolderInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      notesViaFolder: { many: "notes" };
    };
  };
  notes: {
    row: Note;
    insert: NoteInsert;
    columns: {
      body: { kind: "string"; nullable: false; value: string };
      pinned: { kind: "boolean"; nullable: false; value: boolean };
      folderId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      folder: { one: "folders"; nullable: true };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:5ccc0713e28b92a571e092aa1dc450c53bb6bc14d130cb468dd60f878a052b22",
  "schema": {
    "current": "800c8786b97eba357eee432aca7d46092e99e590225846e8b2a6c1fd6c6a20cd",
    "schemas": [
      {
        "hash": "e0ccddc46dd79c94beedaed98cf426b5e3ce0273dc92a2f03cc32ec2a706fb53",
        "schema": {
          "tables": {
            "notes": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "text", "nullable": false }],
            },
          },
        },
      },
      {
        "hash": "800c8786b97eba357eee432aca7d46092e99e590225846e8b2a6c1fd6c6a20cd",
        "schema": {
          "tables": {
            "folders": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "pinned",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "folderId",
                  "nullable": true,
                  "references": "folders",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": true,
            "operations": [],
            "removed": false,
            "renamedFrom": null,
            "table": "folders",
          },
          {
            "added": false,
            "operations": [
              { "column": "text", "type": "rename", "value": "body" },
              {
                "column": "pinned",
                "column_type": { "type": "Boolean" },
                "type": "introduce",
                "value": { "type": "Boolean", "value": false },
              },
              {
                "column": "folderId",
                "column_type": { "type": "Uuid" },
                "type": "introduce",
                "value": { "type": "Null" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "notes",
          },
        ],
        "fromHash": "e0ccddc46dd79c94beedaed98cf426b5e3ce0273dc92a2f03cc32ec2a706fb53",
        "toHash": "800c8786b97eba357eee432aca7d46092e99e590225846e8b2a6c1fd6c6a20cd",
      },
    ],
  },
  "relations": {
    "folders": { "notesViaFolder": { "many": "notes", "relation": "folder" } },
    "notes": { "folder": { "one": "folders", "column": "folderId" } },
  },
});
