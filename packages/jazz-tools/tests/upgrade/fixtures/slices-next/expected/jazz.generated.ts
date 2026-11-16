// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 04dfee5aef29703e. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type CardStatus = "todo" | "doing" | "done";

export type Board = {
  readonly id: string;
  name: string;
};

export type BoardInsert = {
  name: string;
};

export type Card = {
  readonly id: string;
  boardId: string;
  title: string;
  status: CardStatus;
};

export type CardInsert = {
  boardId: string;
  title: string;
  status: CardStatus;
};

interface Types extends AppTypes {
  boards: {
    row: Board;
    insert: BoardInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      cardsViaBoard: { many: "cards" };
    };
  };
  cards: {
    row: Card;
    insert: CardInsert;
    columns: {
      boardId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: CardStatus };
    };
    relations: {
      board: { one: "boards"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:c47ebae32f629f54be90c2a780d199ffff375b573be9165d98c54d8f20f1b484",
  "schema": {
    "current": "36d511c69a4462437f6b5e424515588c0a03d2a31a77c3436baa8d74318f9c6d",
    "schemas": [
      {
        "hash": "36d511c69a4462437f6b5e424515588c0a03d2a31a77c3436baa8d74318f9c6d",
        "schema": {
          "tables": {
            "boards": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "cards": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "boardId",
                  "nullable": false,
                  "references": "boards",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["todo", "doing", "done"] },
                  "name": "status",
                  "nullable": false,
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
    "boards": { "cardsViaBoard": { "many": "cards", "relation": "board" } },
    "cards": { "board": { "one": "boards", "column": "boardId" } },
  },
});
