import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
import type { Query, Table } from "../../src/typed-app.js";

const schema = {
  employees: s.table(
    {
      name: s.string(),
      manager: s.uuid().optional(),
      mentors: s.array(s.uuid()),
      homeTeam: s.uuid().optional(),
    },
    {
      managerRelation: s.rel("employees", "manager"),
      employeesViaManager: s.reverse("employees", "managerRelation"),
      mentorsRelation: s.rel("employees", "mentors"),
      employeesViaMentors: s.reverse("employees", "mentorsRelation"),
      homeTeamRelation: s.rel("teams", "homeTeam"),
      teamsViaLead: s.reverse("teams", "leadRelation"),
      projectsViaApprover: s.reverse("projects", "approverRelation"),
    },
  ),
  teams: s.table(
    {
      name: s.string(),
      lead: s.uuid(),
      parentTeam: s.uuid().optional(),
      flagshipProject: s.uuid().optional(),
    },
    {
      employeesViaHomeTeam: s.reverse("employees", "homeTeamRelation"),
      leadRelation: s.rel("employees", "lead"),
      parentTeamRelation: s.rel("teams", "parentTeam"),
      teamsViaParentTeam: s.reverse("teams", "parentTeamRelation"),
      flagshipProjectRelation: s.rel("projects", "flagshipProject"),
      projectsViaTeam: s.reverse("projects", "teamRelation"),
    },
  ),
  projects: s.table(
    {
      name: s.string(),
      team: s.uuid(),
      approver: s.uuid().optional(),
    },
    {
      teamsViaFlagshipProject: s.reverse("teams", "flagshipProjectRelation"),
      teamRelation: s.rel("teams", "team"),
      approverRelation: s.rel("employees", "approver"),
    },
  ),
};

type CircularAppSchema = s.Schema<typeof schema>;
const app: s.App<CircularAppSchema> = s.defineApp(schema);

describe("typed app circular schemas", () => {
  it("serializes self and circular include trees", () => {
    expect(
      JSON.parse(
        app.employees
          .include({
            managerRelation: {
              managerRelation: true,
            },
            mentorsRelation: app.employees.select("name"),
            employeesViaManager: app.employees.select("name"),
            employeesViaMentors: app.employees.select("name"),
            homeTeamRelation: {
              leadRelation: {
                managerRelation: true,
              },
              parentTeamRelation: {
                leadRelation: true,
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
        managerRelation: {
          managerRelation: true,
        },
        mentorsRelation: {
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
        homeTeamRelation: {
          leadRelation: {
            managerRelation: true,
          },
          parentTeamRelation: {
            leadRelation: true,
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
      managerRelation: {
        homeTeamRelation: {
          leadRelation: true,
        },
      },
      mentorsRelation: app.employees.select("name"),
      employeesViaManager: app.employees.select("name"),
      employeesViaMentors: app.employees.select("name"),
      homeTeamRelation: {
        leadRelation: {
          managerRelation: true,
        },
        parentTeamRelation: {
          leadRelation: true,
        },
        projectsViaTeam: app.projects.select("name"),
        teamsViaParentTeam: app.teams.select("name"),
      },
    });

    type EmployeeGraph = s.RowOf<typeof employeeGraphQuery>;
    const employeeGraph = {} as EmployeeGraph;

    expectTypeOf(employeeGraph.id).toEqualTypeOf<string>();
    expectTypeOf(employeeGraph.name).toEqualTypeOf<string>();
    expectTypeOf(employeeGraph.managerRelation?.homeTeamRelation?.leadRelation?.name).toEqualTypeOf<
      string | undefined
    >();
    expectTypeOf(employeeGraph.homeTeamRelation?.leadRelation?.managerRelation?.id).toEqualTypeOf<
      string | undefined
    >();
    expectTypeOf(
      employeeGraph.homeTeamRelation?.parentTeamRelation?.leadRelation?.name,
    ).toEqualTypeOf<string | undefined>();
    expectTypeOf(employeeGraph.mentorsRelation).toEqualTypeOf<
      Array<{ id: string; name: string }>
    >();
    expectTypeOf(employeeGraph.employeesViaMentors).toEqualTypeOf<
      Array<{ id: string; name: string }>
    >();
    expectTypeOf(employeeGraph.homeTeamRelation?.projectsViaTeam).toEqualTypeOf<
      Array<{ id: string; name: string }> | undefined
    >();
    expectTypeOf(employeeGraph.homeTeamRelation?.teamsViaParentTeam).toEqualTypeOf<
      Array<{ id: string; name: string }> | undefined
    >();
    expectTypeOf(employeeGraph.employeesViaManager).toEqualTypeOf<
      Array<{ id: string; name: string }>
    >();

    const employeeTableContract: Table<"employees", CircularAppSchema> = app.employees;
    const employeeQueryContract: Query<
      "employees",
      {
        managerRelation: { homeTeamRelation: { leadRelation: true } };
        mentorsRelation: ReturnType<typeof app.employees.select<"name">>;
        employeesViaManager: ReturnType<typeof app.employees.select<"name">>;
        employeesViaMentors: ReturnType<typeof app.employees.select<"name">>;
        homeTeamRelation: {
          leadRelation: { managerRelation: true };
          parentTeamRelation: { leadRelation: true };
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
