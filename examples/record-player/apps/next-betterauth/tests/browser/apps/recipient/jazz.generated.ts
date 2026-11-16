// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 08f5ba802a99624e. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Invitation = {
  readonly id: string;
  subject: string;
  label: string;
};

export type InvitationInsert = {
  subject: string;
  label: string;
};

interface Types extends AppTypes {
  invitations: {
    row: Invitation;
    insert: InvitationInsert;
    columns: {
      subject: { kind: "uuid"; nullable: false; value: string };
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:b35ace8f6294810ddbe05379f415a5c4550f96d1145e8dfac8e6bc32904e215d",
  "schema": {
    "current": "4fb8b8c4294b1802cbb8bc5a0bf4601c70a56410ef0364739690df00dd383936",
    "schemas": [
      {
        "hash": "4fb8b8c4294b1802cbb8bc5a0bf4601c70a56410ef0364739690df00dd383936",
        "schema": {
          "tables": {
            "invitations": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "subject", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "invitations": {} },
});
