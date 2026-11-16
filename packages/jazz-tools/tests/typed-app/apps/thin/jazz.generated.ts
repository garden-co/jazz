// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 6b79c3335c9ce925. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Document = {
  readonly id: string;
  branch: string;
  title: string;
};

export type DocumentInsert = {
  branch: string;
  title: string;
};

export type File = {
  readonly id: string;
  name: string;
  data: Uint8Array;
};

export type FileInsert = {
  name: string;
  data: Uint8Array;
};

export type Note = {
  readonly id: string;
  text: string;
  done: boolean;
};

export type NoteInsert = {
  text: string;
  done: boolean;
};

interface Types extends AppTypes {
  documents: {
    row: Document;
    insert: DocumentInsert;
    columns: {
      branch: { kind: "string"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
  files: {
    row: File;
    insert: FileInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      data: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {};
  };
  notes: {
    row: Note;
    insert: NoteInsert;
    columns: {
      text: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:60fe072531c46d578baf68e0f4eb7b9da9393ca7bee4e5934a535c5c660006cc",
  "schema": {
    "current": "64f0f6ee75a82ca210e570dfcb7e501a5abdb0489dfe026f7ec1bfa71bd1c677",
    "schemas": [
      {
        "hash": "64f0f6ee75a82ca210e570dfcb7e501a5abdb0489dfe026f7ec1bfa71bd1c677",
        "schema": {
          "tables": {
            "documents": {
              "branchBy": ["branch"],
              "columns": [
                { "column_type": { "type": "Text" }, "name": "branch", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
              ],
            },
            "files": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "data", "nullable": false },
              ],
            },
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "documents": {}, "files": {}, "notes": {} },
});
