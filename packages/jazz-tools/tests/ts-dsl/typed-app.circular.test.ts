import { describe, expect, expectTypeOf, it } from "vitest";
import { col } from "../../src/dsl.js";
import {
  defineApp,
  type DefinedSchema,
  type Query,
  type RowOf,
  type Table,
  type TypedApp,
} from "../../src/typed-app.js";

const schemaDef = {
  employees: {
    name: col.string(),
    manager: col.ref("employees").optional(),
    mentors: col.array(col.ref("employees")),
    homeTeam: col.ref("teams").optional(),
  },
  teams: {
    name: col.string(),
    lead: col.ref("employees"),
    parentTeam: col.ref("teams").optional(),
    flagshipProject: col.ref("projects").optional(),
  },
  projects: {
    name: col.string(),
    team: col.ref("teams"),
    approver: col.ref("employees").optional(),
  },
};

type CircularAppSchema = DefinedSchema<typeof schemaDef>;
const app: TypedApp<CircularAppSchema> = defineApp(schemaDef);

describe("typed app circular schemas", () => {
  it("serializes self and circular include trees", () => {
    expect(
      JSON.parse(
        app.employees
          .include({
            manager: {
              manager: true,
            },
            mentors: app.employees.select("name"),
            employeesViaManager: app.employees.select("name"),
            employeesViaMentors: app.employees.select("name"),
            homeTeam: {
              lead: {
                manager: true,
              },
              parentTeam: {
                lead: true,
              },
              projectsViaTeam: app.projects.select("name"),
            },
          })
          ._build(),
      ),
    ).toEqual({
      table: "employees",
      conditions: [],
      includes: {
        manager: {
          manager: true,
        },
        mentors: {
          table: "employees",
          conditions: [],
          includes: {},
          select: ["name"],
          orderBy: [],
          hops: [],
        },
        employeesViaManager: {
          table: "employees",
          conditions: [],
          includes: {},
          select: ["name"],
          orderBy: [],
          hops: [],
        },
        employeesViaMentors: {
          table: "employees",
          conditions: [],
          includes: {},
          select: ["name"],
          orderBy: [],
          hops: [],
        },
        homeTeam: {
          lead: {
            manager: true,
          },
          parentTeam: {
            lead: true,
          },
          projectsViaTeam: {
            table: "projects",
            conditions: [],
            includes: {},
            select: ["name"],
            orderBy: [],
            hops: [],
          },
        },
      },
      orderBy: [],
      hops: [],
    });
  });

  it("infers self-references and cyclic reverse relations without collapsing", () => {
    const employeeGraphQuery = app.employees.include({
      manager: {
        homeTeam: {
          lead: true,
        },
      },
      mentors: app.employees.select("name"),
      employeesViaManager: app.employees.select("name"),
      employeesViaMentors: app.employees.select("name"),
      homeTeam: {
        lead: {
          manager: true,
        },
        parentTeam: {
          lead: true,
        },
        projectsViaTeam: app.projects.select("name"),
        teamsViaParentTeam: app.teams.select("name"),
      },
    });

    type EmployeeGraph = RowOf<typeof employeeGraphQuery>;
    const employeeGraph = {} as EmployeeGraph;

    expectTypeOf(employeeGraph.id).toEqualTypeOf<string>();
    expectTypeOf(employeeGraph.name).toEqualTypeOf<string>();
    expectTypeOf(employeeGraph.manager?.homeTeam?.lead?.name).toEqualTypeOf<string | undefined>();
    expectTypeOf(employeeGraph.homeTeam?.lead?.manager?.id).toEqualTypeOf<string | undefined>();
    expectTypeOf(employeeGraph.homeTeam?.parentTeam?.lead?.name).toEqualTypeOf<
      string | undefined
    >();
    expectTypeOf(employeeGraph.mentors).toEqualTypeOf<Array<{ id: string; name: string }>>();
    expectTypeOf(employeeGraph.employeesViaMentors).toEqualTypeOf<
      Array<{ id: string; name: string }>
    >();
    expectTypeOf(employeeGraph.homeTeam?.projectsViaTeam).toEqualTypeOf<
      Array<{ id: string; name: string }> | undefined
    >();
    expectTypeOf(employeeGraph.homeTeam?.teamsViaParentTeam).toEqualTypeOf<
      Array<{ id: string; name: string }> | undefined
    >();
    expectTypeOf(employeeGraph.employeesViaManager).toEqualTypeOf<
      Array<{ id: string; name: string }>
    >();

    const employeeTableContract: Table<"employees", CircularAppSchema> = app.employees;
    const employeeQueryContract: Query<
      "employees",
      {
        manager: { homeTeam: { lead: true } };
        mentors: ReturnType<typeof app.employees.select<"name">>;
        employeesViaManager: ReturnType<typeof app.employees.select<"name">>;
        employeesViaMentors: ReturnType<typeof app.employees.select<"name">>;
        homeTeam: {
          lead: { manager: true };
          parentTeam: { lead: true };
          projectsViaTeam: ReturnType<typeof app.projects.select<"name">>;
          teamsViaParentTeam: ReturnType<typeof app.teams.select<"name">>;
        };
      },
      any,
      CircularAppSchema
    > = employeeGraphQuery;

    void employeeTableContract;
    void employeeQueryContract;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error invalid reverse relation name on employees
      app.employees.include({ projectsViaLead: true });

      // @ts-expect-error invalid self relation name on teams
      app.teams.include({ parent: true });
    }
  });
});
