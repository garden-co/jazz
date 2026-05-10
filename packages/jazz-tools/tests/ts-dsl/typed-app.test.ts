import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
import type { QueryBuilder, TableProxy } from "../../src/runtime/db.js";
import type { Query, Table } from "../../src/typed-app.js";

interface ProjectRecord {
  id: string;
  name: string;
}

interface TodoTitleRecord {
  id: string;
  title: string;
}

const schema = {
  users: s.table({
    name: s.string(),
  }),
  projects: s.table({
    name: s.string(),
  }),
  todos: s
    .table({
      title: s.string(),
      done: s.boolean(),
      tags: s.array(s.string()),
      project: s.ref("projects"),
      owner: s.ref("users").optional(),
    })
    .indexOnly(["done"]),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

const defaultedSchema = {
  users: s.table({
    name: s.string(),
  }),
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean().default(false),
    tags: s.array(s.string()).default([]),
    projectId: s.ref("projects"),
    ownerId: s.ref("users").optional().default(null),
    assigneesIds: s.array(s.ref("users")).default([]),
  }),
};
type DefaultedAppSchema = s.Schema<typeof defaultedSchema>;
const defaultedApp: s.App<DefaultedAppSchema> = s.defineApp(defaultedSchema);

type Urgency = "low" | "high";

const transformedColumnSchema = {
  tasks: s.table({
    title: s.string(),
    urgency: s.int().transform<Urgency>({
      from: (value) => (value > 5 ? "high" : "low"),
      to: (value) => (value === "high" ? 10 : 1),
    }),
  }),
};
type TransformedColumnAppSchema = s.Schema<typeof transformedColumnSchema>;
const transformedColumnApp: s.App<TransformedColumnAppSchema> =
  s.defineApp(transformedColumnSchema);

const graphSchema = {
  teams: s.table({
    name: s.string(),
  }),
  team_edges: s.table({
    child_team: s.ref("teams"),
    parent_team: s.ref("teams"),
  }),
};
type GraphAppSchema = s.Schema<typeof graphSchema>;
const graphApp: s.App<GraphAppSchema> = s.defineApp(graphSchema);

const largeSchema = {
  accounts: s.table({
    name: s.string(),
  }),
  workspaces: s.table({
    name: s.string(),
    accountId: s.ref("accounts"),
  }),
  catalog_items: s.table({
    title: s.string(),
    workspaceId: s.ref("workspaces"),
  }),
  orders: s.table({
    number: s.string(),
    catalogItemId: s.ref("catalog_items"),
    buyerId: s.ref("users"),
  }),
  shipments: s.table({
    trackingCode: s.string(),
    orderId: s.ref("orders"),
  }),
  users: s.table({
    name: s.string(),
  }),
  support_tickets: s.table({
    workspaceId: s.ref("workspaces"),
    requesterId: s.ref("users"),
  }),
};

describe("typed app prototype", () => {
  it("serializes select/include metadata without codegen", () => {
    expect(JSON.parse(app.todos.select("title").include({ project: true })._build())).toEqual({
      table: "todos",
      conditions: [],
      includes: { project: true },
      select: ["title"],
      orderBy: [],
      hops: [],
    });
  });

  it("serializes nested include builders as query objects", () => {
    expect(
      JSON.parse(app.projects.include({ todosViaProject: app.todos.select("title") })._build()),
    ).toEqual({
      table: "projects",
      conditions: [],
      includes: {
        todosViaProject: {
          table: "todos",
          conditions: [],
          includes: {},
          select: ["title"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
      hops: [],
    });
  });

  it("serializes provenance magic columns and infers their projected types", () => {
    const provenanceQuery = app.todos
      .where({ $createdBy: "alice" })
      .select("title", "$createdBy", "$updatedAt");

    expect(JSON.parse(provenanceQuery._build())).toEqual({
      table: "todos",
      conditions: [{ column: "$createdBy", op: "eq", value: "alice" }],
      includes: {},
      select: ["title", "$createdBy", "$updatedAt"],
      orderBy: [],
      hops: [],
    });

    type ProvenanceRow = s.RowOf<typeof provenanceQuery>;
    const row = {} as ProvenanceRow;

    expectTypeOf(row.title).toEqualTypeOf<string>();
    expectTypeOf(row.$createdBy).toEqualTypeOf<string>();
    expectTypeOf(row.$updatedAt).toEqualTypeOf<Date>();
  });

  it("emits indexOnly metadata into the runtime schema", () => {
    expect(app.wasmSchema.todos?.indexed_columns).toEqual(["done"]);
    expect(app.wasmSchema.users?.indexed_columns).toBeUndefined();
  });

  it("serializes gather seeded from the current relation", () => {
    const directParents = graphApp.team_edges.where({ child_team: "team-a" }).hopTo("parent_team");
    const reachableTeams = directParents.gather({
      step: ({ current }) =>
        graphApp.team_edges.where({ child_team: current }).hopTo("parent_team"),
      maxDepth: 3,
    });

    expect(JSON.parse(reachableTeams._build())).toEqual({
      table: "team_edges",
      conditions: [],
      includes: {},
      orderBy: [],
      hops: [],
      gather: {
        seed: {
          table: "team_edges",
          conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
          hops: ["parent_team"],
        },
        max_depth: 3,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_team"],
      },
    });
  });

  it("serializes union gather seeds", () => {
    const directParents = graphApp.team_edges.where({ child_team: "team-a" }).hopTo("parent_team");
    const adminReachableTeams = graphApp.teams.gather({
      start: { name: "admins" },
      step: ({ current }) =>
        graphApp.team_edges.where({ child_team: current }).hopTo("parent_team"),
      maxDepth: 2,
    });
    const reachableTeams = graphApp.union([directParents, adminReachableTeams]).gather({
      step: ({ current }) =>
        graphApp.team_edges.where({ child_team: current }).hopTo("parent_team"),
      maxDepth: 4,
    });

    expect(JSON.parse(reachableTeams._build())).toEqual({
      table: "team_edges",
      conditions: [],
      includes: {},
      orderBy: [],
      hops: [],
      gather: {
        seed: {
          union: {
            inputs: [
              {
                table: "team_edges",
                conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
                hops: ["parent_team"],
              },
              {
                table: "teams",
                conditions: [],
                hops: [],
                gather: {
                  max_depth: 2,
                  step_table: "team_edges",
                  step_current_column: "child_team",
                  step_conditions: [],
                  step_hops: ["parent_team"],
                },
              },
            ],
          },
        },
        max_depth: 4,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_team"],
      },
    });
  });

  it("infers rows, init payloads, where inputs, and include names from schema literals", () => {
    const todoWithProjectQuery = app.todos.include({ project: true });
    const projectWithTitlesQuery = app.projects.include({
      todosViaProject: app.todos.select("title"),
    });

    type TodoRow = s.RowOf<typeof app.todos>;
    type TodoInsert = s.InsertOf<typeof app.todos>;
    type TodoWhere = s.WhereOf<typeof app.todos>;
    type TodoWithProject = s.RowOf<typeof todoWithProjectQuery>;
    type ProjectWithTitles = s.RowOf<typeof projectWithTitlesQuery>;
    const todoRow = {} as TodoRow;
    const todoInsert = {} as TodoInsert;
    const todoWithProject = {} as TodoWithProject;
    const projectWithTitles = {} as ProjectWithTitles;

    expectTypeOf(todoRow.id).toEqualTypeOf<string>();
    expectTypeOf(todoRow.title).toEqualTypeOf<string>();
    expectTypeOf(todoRow.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoRow.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoRow.project).toEqualTypeOf<string>();
    expectTypeOf(todoRow.owner).toEqualTypeOf<string | null>();

    expectTypeOf(todoInsert.title).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoInsert.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoInsert.project).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.owner).toEqualTypeOf<string | null | undefined>();

    expectTypeOf<TodoWhere["project"]>().toEqualTypeOf<
      string | { eq?: string; ne?: string } | undefined
    >();
    expectTypeOf<TodoWhere["owner"]>().branded.toEqualTypeOf<
      string | null | { eq?: string | null; ne?: string | null; isNull?: boolean } | undefined
    >();
    expectTypeOf<TodoWhere["tags"]>().branded.toEqualTypeOf<
      string[] | { eq?: string[]; ne?: string[]; contains?: string } | undefined
    >();

    const projectRecord: ProjectRecord | null = todoWithProject.project;
    expectTypeOf(todoWithProject.owner).toEqualTypeOf<string | null>();
    const todoTitleRecords: TodoTitleRecord[] = projectWithTitles.todosViaProject;
    const queryContract: QueryBuilder<TodoWithProject> = todoWithProjectQuery;
    const typedQueryContract: Query<"todos", { project: true }, any, AppSchema> =
      todoWithProjectQuery;
    const tableProxyContract: TableProxy<TodoRow, TodoInsert> = app.todos;
    const tableContract: Table<"todos", AppSchema> = app.todos;

    void projectRecord;
    void todoTitleRecords;
    void queryContract;
    void typedQueryContract;
    void tableProxyContract;
    void tableContract;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error invalid root key
      void app.unknown;

      // @ts-expect-error invalid where column
      app.todos.where({ missing: true });

      // @ts-expect-error invalid select column
      app.todos.select("missing");

      // @ts-expect-error invalid include relation
      app.todos.include({ todosViaProject: true });

      // @ts-expect-error invalid reverse include on wrong table
      app.users.include({ todosViaProject: true });

      const invalidScalarRefSchema = {
        users: s.table({
          name: s.string(),
        }),
        todos: s.table({
          owner: s.ref("accounts"),
        }),
      };

      // @ts-expect-error invalid ref target table name
      s.defineApp(invalidScalarRefSchema);

      const invalidArrayRefSchema = {
        users: s.table({
          name: s.string(),
        }),
        groups: s.table({
          members: s.array(s.ref("accounts")),
        }),
      };

      // @ts-expect-error invalid ref target table name inside array ref
      s.defineApp(invalidArrayRefSchema);
    }
  });

  it("infers fields with defaults as optional for init payloads", () => {
    type TodoInsert = s.InsertOf<typeof defaultedApp.todos>;
    const minimalInsert: TodoInsert = {
      title: "Ship defaults",
      projectId: "00000000-0000-0000-0000-000000000001",
    };
    const explicitOptionalValues: TodoInsert = {
      title: "Ship defaults",
      projectId: "00000000-0000-0000-0000-000000000001",
      ownerId: null,
      assigneesIds: ["00000000-0000-0000-0000-000000000002"],
    };

    expectTypeOf(minimalInsert.title).toEqualTypeOf<string>();
    expectTypeOf(minimalInsert.projectId).toEqualTypeOf<string>();
    expectTypeOf(minimalInsert.done).toEqualTypeOf<boolean | undefined>();
    expectTypeOf(minimalInsert.tags).toEqualTypeOf<string[] | undefined>();
    expectTypeOf(explicitOptionalValues.ownerId).toEqualTypeOf<string | null | undefined>();
    expectTypeOf(explicitOptionalValues.assigneesIds).toEqualTypeOf<string[] | undefined>();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      const invalidDefaultedNull: TodoInsert = {
        title: "Broken",
        projectId: "00000000-0000-0000-0000-000000000001",
        // @ts-expect-error non-nullable defaulted columns still reject null
        done: null,
      };
      void invalidDefaultedNull;
    }
  });

  it("infers transformed column row and write types while keeping where raw", () => {
    expectTypeOf<s.RowOf<typeof transformedColumnApp.tasks>>().toEqualTypeOf<{
      id: string;
      title: string;
      urgency: Urgency;
    }>();
    expectTypeOf<s.InsertOf<typeof transformedColumnApp.tasks>>().toEqualTypeOf<{
      title: string;
      urgency: Urgency;
    }>();
    expectTypeOf<s.WhereOf<typeof transformedColumnApp.tasks>["urgency"]>().branded.toEqualTypeOf<
      | number
      | {
          eq?: number;
          ne?: number;
          gt?: number;
          gte?: number;
          lt?: number;
          lte?: number;
        }
      | undefined
    >();
  });

  it("creates typed app slices over one full runtime schema", () => {
    const sliceableApp = s.defineSliceableApp(largeSchema);
    const commerceApp = sliceableApp.slice(
      "accounts",
      "workspaces",
      "catalog_items",
      "orders",
      "shipments",
    );
    const supportApp = sliceableApp.slice("accounts", "workspaces", "support_tickets");

    expect(commerceApp.wasmSchema).toBe(sliceableApp.wasmSchema);
    expect(supportApp.wasmSchema).toBe(sliceableApp.wasmSchema);
    expect(() => (sliceableApp.slice as (...tables: string[]) => unknown)()).toThrow(
      "slice(...) requires at least one table name.",
    );
    expect(() => (sliceableApp.slice as (...tables: string[]) => unknown)("missing")).toThrow(
      'slice(...) references unknown table "missing".',
    );
    expect(Object.keys(commerceApp.wasmSchema).sort()).toEqual([
      "accounts",
      "catalog_items",
      "orders",
      "shipments",
      "support_tickets",
      "users",
      "workspaces",
    ]);
    expect(JSON.parse(commerceApp.orders.include({ catalogItem: true })._build())).toEqual({
      table: "orders",
      conditions: [],
      includes: { catalogItem: true },
      orderBy: [],
      hops: [],
    });

    type OrderRow = s.RowOf<typeof commerceApp.orders>;
    type OrderWithCatalogItem = s.RowOf<
      ReturnType<typeof commerceApp.orders.include<{ catalogItem: true }>>
    >;
    type CatalogItemWithOrders = s.RowOf<
      ReturnType<
        typeof commerceApp.catalog_items.include<{
          ordersViaCatalogItem: typeof commerceApp.orders;
        }>
      >
    >;
    type WorkspaceWithSupportTickets = s.RowOf<
      ReturnType<typeof supportApp.workspaces.include<{ support_ticketsViaWorkspace: true }>>
    >;

    const orderRow = {} as OrderRow;
    const orderWithCatalogItem = {} as OrderWithCatalogItem;
    const catalogItemWithOrders = {} as CatalogItemWithOrders;
    const workspaceWithSupportTickets = {} as WorkspaceWithSupportTickets;

    expectTypeOf(orderRow.buyerId).toEqualTypeOf<string>();
    expectTypeOf(orderWithCatalogItem.catalogItem).toEqualTypeOf<{
      id: string;
      title: string;
      workspaceId: string;
    } | null>();
    expectTypeOf(catalogItemWithOrders.ordersViaCatalogItem).toEqualTypeOf<OrderRow[]>();
    expectTypeOf(workspaceWithSupportTickets.support_ticketsViaWorkspace).toEqualTypeOf<
      Array<{
        id: string;
        workspaceId: string;
        requesterId: string;
      }>
    >();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error the full app does not expose a typed global table graph
      void sliceableApp.orders;

      // @ts-expect-error only selected tables are exposed on this slice
      void commerceApp.support_tickets;

      // @ts-expect-error refs outside the slice stay scalar ids, not relations
      commerceApp.orders.include({ buyer: true });

      // @ts-expect-error reverse relations are derived only from the selected slice tables
      commerceApp.workspaces.include({ support_ticketsViaWorkspace: true });

      // @ts-expect-error unknown slice table
      sliceableApp.slice("accounts", "missing");
    }
  });
});
