// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
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
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:84abd1b960b3141499f4ac23e349ae1380d6c2ff8c1383448e578514f2c943c4",
  "schema": {
    "current": "493195f58e2ffc5a005abeecb7a70ea5a9a8ae4ff1ac32b9de5baaefdac90adf",
    "schemas": [
      {
        "hash": "493195f58e2ffc5a005abeecb7a70ea5a9a8ae4ff1ac32b9de5baaefdac90adf",
        "schema": {
          "tables": {
            "documents": {
              "branchBy": ["branch"],
              "columns": [
                { "column_type": { "type": "Text" }, "name": "branch", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "documents": {} },
});
