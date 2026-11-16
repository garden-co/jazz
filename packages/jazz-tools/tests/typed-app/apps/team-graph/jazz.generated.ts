// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type TeamEdge = {
  readonly id: string;
  child_team: string;
  parent_team: string;
};

export type TeamEdgeInsert = {
  child_team: string;
  parent_team: string;
};

export type Team = {
  readonly id: string;
  name: string;
};

export type TeamInsert = {
  name: string;
};

interface Types extends AppTypes {
  team_edges: {
    row: TeamEdge;
    insert: TeamEdgeInsert;
    columns: {
      child_team: { kind: "uuid"; nullable: false; value: string };
      parent_team: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      child_teamRelation: { one: "teams"; nullable: false };
      parent_teamRelation: { one: "teams"; nullable: false };
    };
  };
  teams: {
    row: Team;
    insert: TeamInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      team_edgesViaChild_team: { many: "team_edges" };
      team_edgesViaParent_team: { many: "team_edges" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:7ef2465e311a0e144164b7da5980aed168c9c13c0b92db001884eae134ed2df9",
  "schema": {
    "current": "3c5e4bf8eca2cafcd4b91f76c8709f5c40ec04b7a721d66d0b1a1eaf0fb1ca83",
    "schemas": [
      {
        "hash": "3c5e4bf8eca2cafcd4b91f76c8709f5c40ec04b7a721d66d0b1a1eaf0fb1ca83",
        "schema": {
          "tables": {
            "team_edges": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "child_team",
                  "nullable": false,
                  "references": "teams",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parent_team",
                  "nullable": false,
                  "references": "teams",
                },
              ],
            },
            "teams": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "team_edges": {
      "child_teamRelation": { "one": "teams", "column": "child_team" },
      "parent_teamRelation": { "one": "teams", "column": "parent_team" },
    },
    "teams": {
      "team_edgesViaChild_team": { "many": "team_edges", "relation": "child_teamRelation" },
      "team_edgesViaParent_team": { "many": "team_edges", "relation": "parent_teamRelation" },
    },
  },
});
