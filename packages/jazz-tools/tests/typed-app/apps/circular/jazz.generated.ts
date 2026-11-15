// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Employee = {
  readonly id: string;
  name: string;
  manager: string | null;
  mentors: string[];
  homeTeam: string | null;
};

export type EmployeeInsert = {
  name: string;
  manager?: string | null;
  mentors: string[];
  homeTeam?: string | null;
};

export type Project = {
  readonly id: string;
  name: string;
  team: string;
  approver: string | null;
};

export type ProjectInsert = {
  name: string;
  team: string;
  approver?: string | null;
};

export type Team = {
  readonly id: string;
  name: string;
  lead: string;
  parentTeam: string | null;
  flagshipProject: string | null;
};

export type TeamInsert = {
  name: string;
  lead: string;
  parentTeam?: string | null;
  flagshipProject?: string | null;
};

interface Types extends AppTypes {
  employees: {
    row: Employee;
    insert: EmployeeInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      manager: { kind: "uuid"; nullable: true; value: string };
      mentors: { kind: "array"; nullable: false; value: string[] };
      homeTeam: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      employeesViaManager: { many: "employees" };
      employeesViaMentors: { many: "employees" };
      homeTeamRelation: { one: "teams"; nullable: true };
      managerRelation: { one: "employees"; nullable: true };
      mentorsRelation: { many: "employees"; nullable: false };
      projectsViaApprover: { many: "projects" };
      teamsViaLead: { many: "teams" };
    };
  };
  projects: {
    row: Project;
    insert: ProjectInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      team: { kind: "uuid"; nullable: false; value: string };
      approver: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      approverRelation: { one: "employees"; nullable: true };
      teamRelation: { one: "teams"; nullable: false };
      teamsViaFlagshipProject: { many: "teams" };
    };
  };
  teams: {
    row: Team;
    insert: TeamInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      lead: { kind: "uuid"; nullable: false; value: string };
      parentTeam: { kind: "uuid"; nullable: true; value: string };
      flagshipProject: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      employeesViaHomeTeam: { many: "employees" };
      flagshipProjectRelation: { one: "projects"; nullable: true };
      leadRelation: { one: "employees"; nullable: false };
      parentTeamRelation: { one: "teams"; nullable: true };
      projectsViaTeam: { many: "projects" };
      teamsViaParentTeam: { many: "teams" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:c978cb767531e2c6d5282c7f7713ffa75dcd46dc175f7d06de71a6fa5e3bddfa",
  "schema": {
    "current": "18ec260244a76aad182afc74a3a7fb2e3202c4fcc56ec22a4d4fd04f386ec580",
    "schemas": [
      {
        "hash": "18ec260244a76aad182afc74a3a7fb2e3202c4fcc56ec22a4d4fd04f386ec580",
        "schema": {
          "tables": {
            "employees": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "manager",
                  "nullable": true,
                  "references": "employees",
                },
                {
                  "column_type": { "element": { "type": "Uuid" }, "type": "Array" },
                  "name": "mentors",
                  "nullable": false,
                  "references": "employees",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "homeTeam",
                  "nullable": true,
                  "references": "teams",
                },
              ],
            },
            "projects": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "team",
                  "nullable": false,
                  "references": "teams",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "approver",
                  "nullable": true,
                  "references": "employees",
                },
              ],
            },
            "teams": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "lead",
                  "nullable": false,
                  "references": "employees",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentTeam",
                  "nullable": true,
                  "references": "teams",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "flagshipProject",
                  "nullable": true,
                  "references": "projects",
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
    "employees": {
      "employeesViaManager": { "many": "employees", "relation": "managerRelation" },
      "employeesViaMentors": { "many": "employees", "relation": "mentorsRelation" },
      "homeTeamRelation": { "one": "teams", "column": "homeTeam" },
      "managerRelation": { "one": "employees", "column": "manager" },
      "mentorsRelation": { "one": "employees", "column": "mentors" },
      "projectsViaApprover": { "many": "projects", "relation": "approverRelation" },
      "teamsViaLead": { "many": "teams", "relation": "leadRelation" },
    },
    "projects": {
      "approverRelation": { "one": "employees", "column": "approver" },
      "teamRelation": { "one": "teams", "column": "team" },
      "teamsViaFlagshipProject": { "many": "teams", "relation": "flagshipProjectRelation" },
    },
    "teams": {
      "employeesViaHomeTeam": { "many": "employees", "relation": "homeTeamRelation" },
      "flagshipProjectRelation": { "one": "projects", "column": "flagshipProject" },
      "leadRelation": { "one": "employees", "column": "lead" },
      "parentTeamRelation": { "one": "teams", "column": "parentTeam" },
      "projectsViaTeam": { "many": "projects", "relation": "teamRelation" },
      "teamsViaParentTeam": { "many": "teams", "relation": "parentTeamRelation" },
    },
  },
});
