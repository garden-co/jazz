// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 4e2bb427679489d1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Scenario = {
  readonly id: string;
  name: string;
  base_scenario_id: string | null;
};

export type ScenarioInsert = {
  name: string;
  base_scenario_id?: string | null;
};

export type Task = {
  readonly id: string;
  scenario_id: string;
  title: string;
};

export type TaskInsert = {
  scenario_id: string;
  title: string;
};

interface Types extends AppTypes {
  scenarios: {
    row: Scenario;
    insert: ScenarioInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      base_scenario_id: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      base_scenario: { one: "scenarios"; nullable: true };
      scenariosViaBase_scenario: { many: "scenarios" };
      tasksViaScenario: { many: "tasks" };
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      scenario_id: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      scenario: { one: "scenarios"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:7e15719be369a4cee36a2a5cf201d8730135dded37656a561d6b54a5326bcbb6",
  "schema": {
    "current": "895e8712f691e1e2cbb629526e4573880cfd36322a7f5b7b6cf33578646cdc67",
    "schemas": [
      {
        "hash": "895e8712f691e1e2cbb629526e4573880cfd36322a7f5b7b6cf33578646cdc67",
        "schema": {
          "tables": {
            "scenarios": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "base_scenario_id",
                  "nullable": true,
                  "references": "scenarios",
                },
              ],
            },
            "tasks": {
              "branchBy": ["scenario_id"],
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "scenario_id",
                  "nullable": false,
                  "references": "scenarios",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "scenarios": {
      "base_scenario": { "one": "scenarios", "column": "base_scenario_id" },
      "scenariosViaBase_scenario": { "many": "scenarios", "relation": "base_scenario" },
      "tasksViaScenario": { "many": "tasks", "relation": "scenario" },
    },
    "tasks": { "scenario": { "one": "scenarios", "column": "scenario_id" } },
  },
});
