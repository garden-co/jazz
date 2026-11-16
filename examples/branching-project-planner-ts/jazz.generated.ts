// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 5cef049e02d10900. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ScenarioStatus = "open" | "approved" | "archived";

export type Scenario = {
  readonly id: string;
  name: string;
  base_scenario_id: string | null;
  base_snapshot_ref: string | null;
  status: ScenarioStatus;
};

export type ScenarioInsert = {
  name: string;
  base_scenario_id?: string | null;
  base_snapshot_ref?: string | null;
  status: ScenarioStatus;
};

export type Task = {
  readonly id: string;
  scenario_id: string;
  title: string;
  estimate: number;
};

export type TaskInsert = {
  scenario_id: string;
  title: string;
  estimate: number;
};

interface Types extends AppTypes {
  scenarios: {
    row: Scenario;
    insert: ScenarioInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      base_scenario_id: { kind: "uuid"; nullable: true; value: string };
      base_snapshot_ref: { kind: "string"; nullable: true; value: string };
      status: { kind: "enum"; nullable: false; value: ScenarioStatus };
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
      estimate: { kind: "int"; nullable: false; value: number };
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
  "source": "sha256:e9ad30a5ac2e1ec05c67ef792d4174ade5f66dac278d9074f04c603fbd556db4",
  "schema": {
    "current": "abdfc2124f3969743e0af7adecced972d005a1de444bb94f37592ff4b5740f6d",
    "schemas": [
      {
        "hash": "abdfc2124f3969743e0af7adecced972d005a1de444bb94f37592ff4b5740f6d",
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
                { "column_type": { "type": "Text" }, "name": "base_snapshot_ref", "nullable": true },
                {
                  "column_type": { "type": "Enum", "variants": ["open", "approved", "archived"] },
                  "name": "status",
                  "nullable": false,
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
                { "column_type": { "type": "Integer" }, "name": "estimate", "nullable": false },
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
