import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
import type { Query, Table } from "../../src/typed-app.js";

const schema = {
  employees: s.table({
    name: s.string(),
    manager: s.ref("employees").optional(),
    mentors: s.array(s.ref("employees")),
    homeTeam: s.ref("teams").optional(),
  }),
  teams: s.table({
    name: s.string(),
    lead: s.ref("employees"),
    parentTeam: s.ref("teams").optional(),
    flagshipProject: s.ref("projects").optional(),
  }),
  projects: s.table({
    name: s.string(),
    team: s.ref("teams"),
    approver: s.ref("employees").optional(),
  }),
};

type CircularAppSchema = s.Schema<typeof schema>;
const app: s.App<CircularAppSchema> = s.defineApp(schema);

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

    type EmployeeGraph = s.RowOf<typeof employeeGraphQuery>;
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
