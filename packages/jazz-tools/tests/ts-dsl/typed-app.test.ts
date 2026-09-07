import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
import type { Db, QueryBuilder, TableProxy } from "../../src/runtime/db.js";
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
      attachment: s.bytes(),
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

const largeValueUpdateSchema = {
  documents: s.table({
    title: s.string(),
    payload: s.bytes(),
    metadata: s.json(),
    done: s.boolean(),
  }),
};
type LargeValueUpdateAppSchema = s.Schema<typeof largeValueUpdateSchema>;
const largeValueUpdateApp: s.App<LargeValueUpdateAppSchema> = s.defineApp(largeValueUpdateSchema);

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

  it("serializes partial large-value select descriptors", () => {
    const query = app.todos.select({
      attachment: { from: 1_000_000, to: 2_000_000 },
      title: { fromUtf8: 4, toUtf8: 67 },
    });

    expect(JSON.parse(query._build())).toMatchObject({
      table: "todos",
      select: {
        attachment: { from: 1_000_000, to: 2_000_000 },
        title: { fromUtf8: 4, toUtf8: 67 },
      },
    });
    expectTypeOf(query).toMatchTypeOf<
      QueryBuilder<{ id: string; attachment: Uint8Array; title: string }>
    >();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // The object form is a schema-derived partial projection, rather than a
      // generic descriptor bag. In particular, JSON pointers cannot leak onto
      // an arbitrary scalar just because its JS representation is primitive.
      // @ts-expect-error BOOLEAN columns do not support partial projections.
      app.todos.select({ done: { at: "/" } });
      // @ts-expect-error bytes use byte ranges, not text UTF-8 coordinates.
      app.todos.select({ attachment: { fromUtf8: 0, toUtf8: 1 } });
      // @ts-expect-error TEXT cannot use the JSON-pointer projection form.
      app.todos.select({ title: { at: "/" } });
    }
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

  it("does not expose permission introspection columns through typed queries", () => {
    // @ts-expect-error Permission introspection columns are not selectable query columns.
    app.todos.select("$canRead");
    // @ts-expect-error Permission introspection columns are not filterable query columns.
    app.todos.where({ $canRead: true });
    // @ts-expect-error Permission introspection columns are not orderable query columns.
    app.todos.orderBy("$canRead");

    app.todos
      .select("$createdAt")
      .where({ $updatedAt: { lte: new Date() } })
      .orderBy("$createdBy");
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
      maxDepth: 0,
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
        max_depth: 0,
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
                label: "derived:1305159ebbe387ef62c9b24b7ee2823fe2181cb4e1c6be16282ff393c7fe2fbf",
                input: {
                  table: "team_edges",
                  conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
                  hops: ["parent_team"],
                },
              },
              {
                label: "derived:1b79e5056c01cd288d096b23cbc4e17bf07f6484e03cfc14ded25553810026fe",
                input: {
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
    type TodoStreamingInsert = s.StreamingInsertOf<typeof app.todos>;
    type TodoStreamingUpdate = s.StreamingUpdateOf<typeof app.todos>;
    type TodoWhere = s.WhereOf<typeof app.todos>;
    type TodoWithProject = s.RowOf<typeof todoWithProjectQuery>;
    type ProjectWithTitles = s.RowOf<typeof projectWithTitlesQuery>;
    const todoRow = {} as TodoRow;
    const todoInsert = {} as TodoInsert;
    const streamedTitle = {
      title: new ReadableStream<string>(),
      done: false,
      tags: [],
      attachment: new Uint8Array(),
      project: "project-id",
    } satisfies TodoStreamingInsert;
    const streamedAttachment = {
      title: "todo",
      done: false,
      tags: [],
      attachment: new ReadableStream<Uint8Array>(),
      project: "project-id",
    } satisfies TodoStreamingInsert;
    const streamedTitleUpdate = {
      title: new ReadableStream<string>(),
    } satisfies TodoStreamingUpdate;
    const todoWithProject = {} as TodoWithProject;
    const projectWithTitles = {} as ProjectWithTitles;

    expectTypeOf(todoRow.id).toEqualTypeOf<string>();
    expectTypeOf(todoRow.title).toEqualTypeOf<string>();
    expectTypeOf(todoRow.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoRow.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoRow.attachment).toEqualTypeOf<Uint8Array>();
    expectTypeOf(todoRow.project).toEqualTypeOf<string>();
    expectTypeOf(todoRow.owner).toEqualTypeOf<string | null>();

    expectTypeOf(todoInsert.title).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoInsert.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoInsert.attachment).toEqualTypeOf<Uint8Array>();
    expectTypeOf(todoInsert.project).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.owner).toEqualTypeOf<string | null | undefined>();
    expectTypeOf(streamedTitle.title).toEqualTypeOf<ReadableStream<string>>();
    expectTypeOf(streamedAttachment.attachment).toEqualTypeOf<ReadableStream<Uint8Array>>();
    expectTypeOf(streamedTitleUpdate.title).toEqualTypeOf<ReadableStream<string>>();

    expectTypeOf<TodoWhere["project"]>().branded.toEqualTypeOf<
      string | { eq?: string; ne?: string; in?: string[]; notIn?: string[] } | undefined
    >();
    expectTypeOf<TodoWhere["owner"]>().branded.toEqualTypeOf<
      | string
      | null
      | {
          eq?: string | null;
          ne?: string | null;
          in?: string[];
          notIn?: string[];
          isNull?: boolean;
        }
      | undefined
    >();
    expectTypeOf<TodoWhere["tags"]>().branded.toEqualTypeOf<
      | string[]
      | { eq?: string[]; ne?: string[]; contains?: string; in?: string[][]; notIn?: string[][] }
      | undefined
    >();
    expectTypeOf<TodoWhere["attachment"]>().branded.toEqualTypeOf<
      | Uint8Array
      | {
          eq?: Uint8Array;
          ne?: Uint8Array;
          in?: (Uint8Array | number[])[];
          notIn?: (Uint8Array | number[])[];
        }
      | undefined
    >();

    // Membership is deliberately non-nullable. Express null handling with
    // isNull/isNotNull rather than SQL-style null membership semantics.
    // @ts-expect-error null is not a valid membership value
    app.todos.where({ owner: { notIn: [null] } });

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
    void streamedTitle;
    void streamedAttachment;
    void streamedTitleUpdate;
    void queryContract;
    void typedQueryContract;
    void tableProxyContract;
    void tableContract;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error invalid root key
      void app.unknown;
      const invalidStreamedReference: TodoStreamingInsert = {
        title: "todo",
        done: false,
        tags: [],
        attachment: new Uint8Array(),
        // @ts-expect-error UUID references are not streamable despite being strings in TypeScript.
        project: new ReadableStream<string>(),
      };
      void invalidStreamedReference;

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

  it("infers update payloads with column-specific large-value descriptors", () => {
    type DocumentUpdate = s.LargeValueUpdateOf<typeof largeValueUpdateApp.documents>;
    const update = {
      title: {
        within: { from: 0, to: 4 },
        splices: [{ at: 1, delete: 2, insert: "ee" }],
      },
      payload: {
        within: { from: 0, to: 3 },
        splices: [{ at: 1, delete: 1, insert: new Uint8Array([9]) }],
      },
      metadata: {
        edits: [{ op: "set", at: "/selected/answer", value: 43 }],
      },
    } satisfies DocumentUpdate;

    const utf8TextUpdate = {
      title: {
        within: { fromUtf8: 0, toUtf8: 4 },
        splices: [{ atUtf8: 0, deleteUtf8: 4, insert: "text" }],
      },
    } satisfies DocumentUpdate;

    void update;
    void utf8TextUpdate;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // Whole-column replacements and column-specific diffs share Db.update.
      const db = null as unknown as Db;
      db.update(
        largeValueUpdateApp.documents,
        "00000000-0000-0000-0000-000000000001",
        {
          done: true,
        },
        {
          applyDiffs: {
            title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
          },
        },
      );
      // @ts-expect-error Db no longer exposes a separate applyDiffs method
      db.applyDiffs(largeValueUpdateApp.documents, "00000000-0000-0000-0000-000000000001", {});
      // @ts-expect-error a column cannot be both replaced and diffed
      db.update(
        largeValueUpdateApp.documents,
        "00000000-0000-0000-0000-000000000001",
        { title: "replacement" },
        {
          applyDiffs: {
            title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
          },
        },
      );
      db.upsert(largeValueUpdateApp.documents, "00000000-0000-0000-0000-000000000001", {
        // @ts-expect-error partial descriptors belong exclusively to update's applyDiffs option
        title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
      });

      const byteUpdateWithText = {
        payload: {
          within: { from: 0, to: 1 },
          splices: [
            {
              at: 0,
              delete: 0,
              // @ts-expect-error byte splice inserts must be Uint8Array
              insert: "x",
            },
          ],
        },
      } satisfies DocumentUpdate;
      void byteUpdateWithText;

      const textUpdateWithBytes = {
        title: {
          within: { fromUtf8: 0, toUtf8: 1 },
          splices: [
            {
              atUtf8: 0,
              deleteUtf8: 0,
              // @ts-expect-error text splice inserts must be strings
              insert: new Uint8Array([1]),
            },
          ],
        },
      } satisfies DocumentUpdate;
      void textUpdateWithBytes;
    }
  });

  it("infers in filters for boolean, bytes, and array columns", () => {
    expectTypeOf<s.WhereOf<typeof app.todos>["done"]>().branded.toEqualTypeOf<
      | boolean
      | {
          eq?: boolean;
          ne?: boolean;
          in?: boolean[];
          notIn?: boolean[];
        }
      | undefined
    >();
    expectTypeOf<s.WhereOf<typeof app.todos>["tags"]>().branded.toEqualTypeOf<
      | string[]
      | {
          eq?: string[];
          ne?: string[];
          contains?: string;
          in?: string[][];
          notIn?: string[][];
        }
      | undefined
    >();
    expectTypeOf<s.WhereOf<typeof app.todos>["attachment"]>().branded.toEqualTypeOf<
      | Uint8Array
      | {
          eq?: Uint8Array;
          ne?: Uint8Array;
          in?: (Uint8Array | number[])[];
          notIn?: (Uint8Array | number[])[];
        }
      | undefined
    >();
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
          in?: number[];
          notIn?: number[];
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
