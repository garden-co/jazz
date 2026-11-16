// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 6353f1ea5aed923b. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Author = {
  readonly id: string;
  name: string;
};

export type AuthorInsert = {
  name: string;
};

export type Item = {
  readonly id: string;
  title: string;
  parent_id: string;
  author_id: string;
  label_id: string;
};

export type ItemInsert = {
  title: string;
  parent_id: string;
  author_id: string;
  label_id: string;
};

export type Label = {
  readonly id: string;
  name: string;
};

export type LabelInsert = {
  name: string;
};

export type Parent = {
  readonly id: string;
  name: string;
};

export type ParentInsert = {
  name: string;
};

export type Unrelated = {
  readonly id: string;
  value: string;
};

export type UnrelatedInsert = {
  value: string;
};

interface Types extends AppTypes {
  authors: {
    row: Author;
    insert: AuthorInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      itemsViaAuthor: { many: "items" };
    };
  };
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      parent_id: { kind: "uuid"; nullable: false; value: string };
      author_id: { kind: "uuid"; nullable: false; value: string };
      label_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      author: { one: "authors"; nullable: false };
      label: { one: "labels"; nullable: false };
      parent: { one: "parents"; nullable: false };
    };
  };
  labels: {
    row: Label;
    insert: LabelInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      itemsViaLabel: { many: "items" };
    };
  };
  parents: {
    row: Parent;
    insert: ParentInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      itemsViaParent: { many: "items" };
    };
  };
  unrelated: {
    row: Unrelated;
    insert: UnrelatedInsert;
    columns: {
      value: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:80121119f9a4f47ee1736466b8740ef878b8e2f9b54cffb22b545a3cc53923db",
  "schema": {
    "current": "a7db12e8d0fc9d3c9ee7e632cee353b07910030b7fb9e150eded43135bab6f62",
    "schemas": [
      {
        "hash": "a7db12e8d0fc9d3c9ee7e632cee353b07910030b7fb9e150eded43135bab6f62",
        "schema": {
          "tables": {
            "authors": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "items": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parent_id",
                  "nullable": false,
                  "references": "parents",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "author_id",
                  "nullable": false,
                  "references": "authors",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "label_id",
                  "nullable": false,
                  "references": "labels",
                },
              ],
            },
            "labels": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "parents": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "unrelated": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "value", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "authors": { "itemsViaAuthor": { "many": "items", "relation": "author" } },
    "items": {
      "author": { "one": "authors", "column": "author_id" },
      "label": { "one": "labels", "column": "label_id" },
      "parent": { "one": "parents", "column": "parent_id" },
    },
    "labels": { "itemsViaLabel": { "many": "items", "relation": "label" } },
    "parents": { "itemsViaParent": { "many": "items", "relation": "parent" } },
    "unrelated": {},
  },
});
