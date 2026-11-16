// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type File = {
  readonly id: string;
  name: string;
  contents: Uint8Array;
};

export type FileInsert = {
  name: string;
  contents: Uint8Array;
};

interface Types extends AppTypes {
  files: {
    row: File;
    insert: FileInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      contents: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:6edb49aed9bb8b37bdfd43b84728cc159b1a7161fdc174f03e75b25801de9a51",
  "schema": {
    "current": "3e82ba837c50230eb1eba01fa8d47367cc002e0deaec734b622ad2ce6fab4519",
    "schemas": [
      {
        "hash": "3e82ba837c50230eb1eba01fa8d47367cc002e0deaec734b622ad2ce6fab4519",
        "schema": {
          "tables": {
            "files": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "contents", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "files": {} },
});
