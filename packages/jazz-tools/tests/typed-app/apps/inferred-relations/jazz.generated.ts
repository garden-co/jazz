// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Analyse = {
  readonly id: string;
  summary: string;
};

export type AnalyseInsert = {
  summary: string;
};

export type Category = {
  readonly id: string;
  label: string;
};

export type CategoryInsert = {
  label: string;
};

export type Entry = {
  readonly id: string;
  category_id: string;
  personId: string;
  team_id: string;
  teamIds: string[];
  address: string | null;
  category_ids: string[];
  person_ids: string[];
  analysis_ids: string[];
  status_ids: string[];
};

export type EntryInsert = {
  category_id: string;
  personId: string;
  team_id: string;
  teamIds: string[];
  address?: string | null;
  category_ids: string[];
  person_ids: string[];
  analysis_ids: string[];
  status_ids: string[];
};

export type Person = {
  readonly id: string;
  name: string;
};

export type PersonInsert = {
  name: string;
};

export type Status = {
  readonly id: string;
  label: string;
};

export type StatusInsert = {
  label: string;
};

export type Team = {
  readonly id: string;
  category_id: string;
};

export type TeamInsert = {
  category_id: string;
};

interface Types extends AppTypes {
  analyses: {
    row: Analyse;
    insert: AnalyseInsert;
    columns: {
      summary: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      entriesViaAnalyses: { many: "entries" };
    };
  };
  categories: {
    row: Category;
    insert: CategoryInsert;
    columns: {
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      entriesViaCategories: { many: "entries" };
      entriesViaCategory: { many: "entries" };
      teamsViaCategory: { many: "teams" };
    };
  };
  entries: {
    row: Entry;
    insert: EntryInsert;
    columns: {
      category_id: { kind: "uuid"; nullable: false; value: string };
      personId: { kind: "uuid"; nullable: false; value: string };
      team_id: { kind: "uuid"; nullable: false; value: string };
      teamIds: { kind: "array"; nullable: false; value: string[] };
      address: { kind: "uuid"; nullable: true; value: string };
      category_ids: { kind: "array"; nullable: false; value: string[] };
      person_ids: { kind: "array"; nullable: false; value: string[] };
      analysis_ids: { kind: "array"; nullable: false; value: string[] };
      status_ids: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      addressRelation: { one: "people"; nullable: true };
      analyses: { many: "analyses"; nullable: false };
      categories: { many: "categories"; nullable: false };
      category: { one: "categories"; nullable: false };
      people: { many: "people"; nullable: false };
      person: { one: "people"; nullable: false };
      statuses: { many: "statuses"; nullable: false };
      team: { one: "teams"; nullable: false };
      teams: { many: "teams"; nullable: false };
    };
  };
  people: {
    row: Person;
    insert: PersonInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      entriesViaAddress: { many: "entries" };
      entriesViaPeople: { many: "entries" };
      entriesViaPerson: { many: "entries" };
    };
  };
  statuses: {
    row: Status;
    insert: StatusInsert;
    columns: {
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      entriesViaStatuses: { many: "entries" };
    };
  };
  teams: {
    row: Team;
    insert: TeamInsert;
    columns: {
      category_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      category: { one: "categories"; nullable: false };
      entriesViaTeam: { many: "entries" };
      entriesViaTeams: { many: "entries" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:879d54108dd02c2fe8adc5f0c45207ac5eba90e8b435f9350db1d166d3c518a2",
  "schema": {
    "current": "3d22769ff0c16f90b7dba81eaa87c097a360642b017b1325f52085454b15f99a",
    "schemas": [
      {
        "hash": "3d22769ff0c16f90b7dba81eaa87c097a360642b017b1325f52085454b15f99a",
        "schema": {
          "tables": {
            "analyses": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "summary", "nullable": false },
              ],
            },
            "categories": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "label", "nullable": false }],
            },
            "entries": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "category_id",
                  "nullable": false,
                  "references": "categories",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "personId",
                  "nullable": false,
                  "references": "people",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "team_id",
                  "nullable": false,
                  "references": "teams",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "teamIds",
                  "nullable": false,
                  "references": "teams",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "address",
                  "nullable": true,
                  "references": "people",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "category_ids",
                  "nullable": false,
                  "references": "categories",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "person_ids",
                  "nullable": false,
                  "references": "people",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "analysis_ids",
                  "nullable": false,
                  "references": "analyses",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "status_ids",
                  "nullable": false,
                  "references": "statuses",
                },
              ],
            },
            "people": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "statuses": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "label", "nullable": false }],
            },
            "teams": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "category_id",
                  "nullable": false,
                  "references": "categories",
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
    "analyses": { "entriesViaAnalyses": { "many": "entries", "relation": "analyses" } },
    "categories": {
      "entriesViaCategories": { "many": "entries", "relation": "categories" },
      "entriesViaCategory": { "many": "entries", "relation": "category" },
      "teamsViaCategory": { "many": "teams", "relation": "category" },
    },
    "entries": {
      "addressRelation": { "one": "people", "column": "address" },
      "analyses": { "one": "analyses", "column": "analysis_ids" },
      "categories": { "one": "categories", "column": "category_ids" },
      "category": { "one": "categories", "column": "category_id" },
      "people": { "one": "people", "column": "person_ids" },
      "person": { "one": "people", "column": "personId" },
      "statuses": { "one": "statuses", "column": "status_ids" },
      "team": { "one": "teams", "column": "team_id" },
      "teams": { "one": "teams", "column": "teamIds" },
    },
    "people": {
      "entriesViaAddress": { "many": "entries", "relation": "addressRelation" },
      "entriesViaPeople": { "many": "entries", "relation": "people" },
      "entriesViaPerson": { "many": "entries", "relation": "person" },
    },
    "statuses": { "entriesViaStatuses": { "many": "entries", "relation": "statuses" } },
    "teams": {
      "category": { "one": "categories", "column": "category_id" },
      "entriesViaTeam": { "many": "entries", "relation": "team" },
      "entriesViaTeams": { "many": "entries", "relation": "teams" },
    },
  },
});
