// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type EventEvent = {
  type: "message";
  requiredText: string;
  nullableText: string | null;
  defaultedText: string;
};
export type EventEventInit = {
  type: "message";
  requiredText: string;
  nullableText?: string | null;
  defaultedText?: string;
};
export type EventOptionalEvent = {
  type: "message";
  requiredText: string;
  nullableText: string | null;
  defaultedText: string;
};
export type EventOptionalEventInit = {
  type: "message";
  requiredText: string;
  nullableText?: string | null;
  defaultedText?: string;
};

export type Event = {
  readonly id: string;
  event: EventEvent;
  optionalEvent: EventOptionalEvent | null;
};

export type EventInsert = {
  event: EventEventInit;
  optionalEvent?: EventOptionalEventInit | null;
};

interface Types extends AppTypes {
  events: {
    row: Event;
    insert: EventInsert;
    columns: {
      event: { kind: "enum"; nullable: false; value: EventEvent };
      optionalEvent: { kind: "enum"; nullable: true; value: EventOptionalEvent };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:35f09f8792ced1396d8d3843fba991aebeb834df25384fb3af7520ca96e62e07",
  "schema": {
    "current": "950f62751c5b0c1e12f7e7320e9c17c5257edd7938f2c636a7b82a3dc17ad8d5",
    "schemas": [
      {
        "hash": "950f62751c5b0c1e12f7e7320e9c17c5257edd7938f2c636a7b82a3dc17ad8d5",
        "schema": {
          "tables": {
            "events": {
              "columns": [
                {
                  "column_type": {
                    "cases": [
                      {
                        "fields": [
                          {
                            "column_type": { "type": "Text" },
                            "name": "requiredText",
                            "nullable": false,
                          },
                          {
                            "column_type": { "type": "Text" },
                            "name": "nullableText",
                            "nullable": true,
                          },
                          {
                            "column_type": { "type": "Text" },
                            "default": { "type": "Text", "value": "default" },
                            "name": "defaultedText",
                            "nullable": false,
                          },
                        ],
                        "name": "message",
                      },
                    ],
                    "type": "EnumPayload",
                  },
                  "name": "event",
                  "nullable": false,
                },
                {
                  "column_type": {
                    "cases": [
                      {
                        "fields": [
                          {
                            "column_type": { "type": "Text" },
                            "name": "requiredText",
                            "nullable": false,
                          },
                          {
                            "column_type": { "type": "Text" },
                            "name": "nullableText",
                            "nullable": true,
                          },
                          {
                            "column_type": { "type": "Text" },
                            "default": { "type": "Text", "value": "default" },
                            "name": "defaultedText",
                            "nullable": false,
                          },
                        ],
                        "name": "message",
                      },
                    ],
                    "type": "EnumPayload",
                  },
                  "name": "optionalEvent",
                  "nullable": true,
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "events": {} },
});
