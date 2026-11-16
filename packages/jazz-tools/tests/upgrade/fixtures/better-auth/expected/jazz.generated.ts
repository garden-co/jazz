// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type BetterAuthSession = {
  readonly id: string;
  token: string;
  userId: string;
};

export type BetterAuthSessionInsert = {
  token: string;
  userId: string;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
};

export type Message = {
  readonly id: string;
  text: string;
};

export type MessageInsert = {
  text: string;
};

interface Types extends AppTypes {
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      token: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {};
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      text: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:927d3e1748e449cc914967fd8a836a74db9a9e1f39ec35864f75334b79e9dbed",
  "schema": {
    "current": "9775a30ebe7f837041c627c049c76a4867fb91e903a949e70b6f1af57ad8168e",
    "schemas": [
      {
        "hash": "9775a30ebe7f837041c627c049c76a4867fb91e903a949e70b6f1af57ad8168e",
        "schema": {
          "tables": {
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "userId", "nullable": false },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
              ],
            },
            "messages": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "text", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "better_auth_session": {}, "better_auth_user": {}, "messages": {} },
});
