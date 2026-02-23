import * as React from "react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { userEvent } from "vitest/browser";
import { createRoot, type Root } from "react-dom/client";
import type { WasmSchema } from "../../src/drivers/types.js";
import type { QueryBuilder, TableProxy } from "../../src/runtime/db.js";
import { createJazzClient, type JazzClient } from "../../src/react/create-jazz-client.js";
import { JazzProvider } from "../../src/react-core/provider.js";
import { useAllSuspense } from "../../src/react-core/use-all.js";

const schema: WasmSchema = {
  tables: {
    orgs: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    teams: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "org_id", column_type: { type: "Uuid" }, nullable: true, references: "orgs" },
        {
          name: "parent_id",
          column_type: { type: "Uuid" },
          nullable: true,
          references: "teams",
        },
      ],
    },
    users: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "team_id", column_type: { type: "Uuid" }, nullable: true, references: "teams" },
      ],
    },
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "priority", column_type: { type: "Integer" }, nullable: true },
        { name: "owner_id", column_type: { type: "Uuid" }, nullable: true, references: "users" },
        {
          name: "tags",
          column_type: { type: "Array", element: { type: "Text" } },
          nullable: false,
        },
      ],
    },
  },
};

type Org = { id: string; name: string };
type Team = { id: string; name: string; org_id?: string; parent_id?: string };
type User = { id: string; name: string; team_id?: string; todosViaOwner?: Todo[] };
type Todo = {
  id: string;
  title: string;
  done: boolean;
  priority?: number;
  owner_id?: string;
  tags: string[];
};

const orgs: TableProxy<Org, Omit<Org, "id">> = {
  _table: "orgs",
  _schema: schema,
  _rowType: {} as Org,
  _initType: {} as Omit<Org, "id">,
};

const teams: TableProxy<Team, Omit<Team, "id">> = {
  _table: "teams",
  _schema: schema,
  _rowType: {} as Team,
  _initType: {} as Omit<Team, "id">,
};

const users: TableProxy<User, Omit<User, "id" | "todosViaOwner">> = {
  _table: "users",
  _schema: schema,
  _rowType: {} as User,
  _initType: {} as Omit<User, "id" | "todosViaOwner">,
};

const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

function uniqueId(label: string): string {
  return `use-all-suspense-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function makeQuery<T>(
  table: string,
  body: {
    conditions?: Array<{ column: string; op: string; value?: unknown }>;
    includes?: Record<string, boolean | object>;
    orderBy?: Array<[string, "asc" | "desc"]>;
    limit?: number;
    offset?: number;
    hops?: string[];
    gather?: {
      max_depth: number;
      step_table: string;
      step_current_column: string;
      step_conditions: Array<{ column: string; op: string; value: unknown }>;
      step_hops: string[];
    };
  },
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: schema,
    _rowType: {} as T,
    _build() {
      return JSON.stringify({
        table,
        conditions: body.conditions ?? [],
        includes: body.includes ?? {},
        orderBy: body.orderBy ?? [],
        limit: body.limit,
        offset: body.offset,
        hops: body.hops,
        gather: body.gather,
      });
    },
  };
}

let root: Root | null = null;
let container: HTMLDivElement | null = null;

beforeEach(() => {
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  if (root) {
    root.unmount();
    root = null;
  }
  if (container) {
    container.remove();
    container = null;
  }
});

function render(node: React.ReactNode): void {
  if (!root) throw new Error("render called before root initialization");
  root.render(node);
}

function renderSuspense(node: React.ReactNode): void {
  render(
    <React.Suspense fallback={<div data-testid="rows-fallback">pending</div>}>
      {node}
    </React.Suspense>,
  );
}

function getText(testId: string): string {
  if (!container) return "";
  const node = container.querySelector(`[data-testid="${testId}"]`);
  return node?.textContent ?? "";
}

function hasTestId(testId: string): boolean {
  if (!container) return false;
  return container.querySelector(`[data-testid="${testId}"]`) !== null;
}

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  errorMessage: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(errorMessage);
}

function UseAllProbe<T extends { id: string }>({
  query,
  pick,
}: {
  query: QueryBuilder<T>;
  pick: (row: T) => string;
}) {
  const rows = useAllSuspense(query);
  const text = rows.map(pick).join("|");
  return <div data-testid="rows">{text}</div>;
}

describe("useAllSuspense browser integration", () => {
  const clients: JazzClient[] = [];
  const conditionCases: Array<{
    name: string;
    query: QueryBuilder<Todo>;
    insert: Omit<Todo, "id">;
    pick: string;
  }> = [
    {
      name: "eq",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "eq", value: "eq-hit" }],
      }),
      insert: { title: "eq-hit", done: false, priority: 1, owner_id: undefined, tags: ["x"] },
      pick: "eq-hit",
    },
    {
      name: "ne",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "ne", value: "blocked" }],
      }),
      insert: { title: "ne-hit", done: false, priority: 2, owner_id: undefined, tags: ["x"] },
      pick: "ne-hit",
    },
    {
      name: "gt",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "gt", value: 10 }],
      }),
      insert: { title: "gt-hit", done: false, priority: 11, owner_id: undefined, tags: ["x"] },
      pick: "gt-hit",
    },
    {
      name: "gte",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "gte", value: 10 }],
      }),
      insert: { title: "gte-hit", done: false, priority: 10, owner_id: undefined, tags: ["x"] },
      pick: "gte-hit",
    },
    {
      name: "lt",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "lt", value: 0 }],
      }),
      insert: { title: "lt-hit", done: false, priority: -1, owner_id: undefined, tags: ["x"] },
      pick: "lt-hit",
    },
    {
      name: "lte",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "lte", value: 0 }],
      }),
      insert: { title: "lte-hit", done: false, priority: 0, owner_id: undefined, tags: ["x"] },
      pick: "lte-hit",
    },
    {
      name: "isNull",
      query: makeQuery<Todo>("todos", { conditions: [{ column: "priority", op: "isNull" }] }),
      insert: {
        title: "null-hit",
        done: false,
        priority: undefined,
        owner_id: undefined,
        tags: ["x"],
      },
      pick: "null-hit",
    },
    {
      name: "contains-array",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "tags", op: "contains", value: "needle" }],
      }),
      insert: {
        title: "contains-array-hit",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["needle", "hay"],
      },
      pick: "contains-array-hit",
    },
    {
      name: "contains-text",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "contains", value: "needle" }],
      }),
      insert: {
        title: "hay-needle-title",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["x"],
      },
      pick: "hay-needle-title",
    },
    {
      name: "contains-text-empty",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "contains", value: "" }],
      }),
      insert: {
        title: "any-title",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["x"],
      },
      pick: "any-title",
    },
  ];

  function track(client: JazzClient): JazzClient {
    clients.push(client);
    return client;
  }

  afterEach(async () => {
    for (const client of clients.splice(0).reverse()) {
      await client.shutdown();
    }
  });

  for (const testCase of conditionCases) {
    it(`supports condition operator ${testCase.name}`, async () => {
      const client = track(
        await createJazzClient({
          appId: uniqueId(`operators-${testCase.name}`),
          dbName: uniqueId(`operators-${testCase.name}`),
        }),
      );
      const preloadBeforeRender =
        testCase.name === "contains-text" || testCase.name === "contains-text-empty";
      if (preloadBeforeRender) {
        client.db.insert(todos, testCase.insert);
      }

      renderSuspense(
        <JazzProvider client={client} key={testCase.name}>
          <UseAllProbe query={testCase.query} pick={(row) => row.title} />
        </JazzProvider>,
      );

      await waitForCondition(
        () => hasTestId("rows"),
        5000,
        `expected suspense rows mount for ${testCase.name}`,
      );

      if (!preloadBeforeRender) {
        client.db.insert(todos, testCase.insert);
      }

      await waitForCondition(
        () => getText("rows").split("|").includes(testCase.pick),
        5000,
        `expected useAll rows to include ${testCase.name}`,
      );
    });
  }

  it("supports orderBy + limit + offset", async () => {
    const client = track(
      await createJazzClient({
        appId: uniqueId("order"),
        dbName: uniqueId("order"),
      }),
    );

    const query = makeQuery<Todo>("todos", {
      orderBy: [["priority", "desc"]],
      offset: 1,
      limit: 1,
    });

    renderSuspense(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.title} />
      </JazzProvider>,
    );

    client.db.insert(todos, {
      title: "p1",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });
    client.db.insert(todos, {
      title: "p2",
      done: false,
      priority: 2,
      owner_id: undefined,
      tags: ["x"],
    });
    client.db.insert(todos, {
      title: "p3",
      done: false,
      priority: 3,
      owner_id: undefined,
      tags: ["x"],
    });

    await waitForCondition(
      () => getText("rows") === "p2",
      5000,
      "expected p2 in paginated useAllSuspense",
    );
  });

  it("does not include rows for non-matching text contains", async () => {
    const client = track(
      await createJazzClient({
        appId: uniqueId("contains-text-miss"),
        dbName: uniqueId("contains-text-miss"),
      }),
    );

    renderSuspense(
      <JazzProvider client={client}>
        <UseAllProbe
          query={makeQuery<Todo>("todos", {
            conditions: [{ column: "title", op: "contains", value: "needle" }],
          })}
          pick={(row) => row.title}
        />
      </JazzProvider>,
    );

    client.db.insert(todos, {
      title: "completely unrelated",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });

    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(getText("rows").includes("completely unrelated")).toBe(false);
  });

  it("supports include query execution path", async () => {
    const client = track(
      await createJazzClient({
        appId: uniqueId("include"),
        dbName: uniqueId("include"),
      }),
    );

    const query = makeQuery<User>("users", {
      includes: { todosViaOwner: true },
    });

    renderSuspense(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.name} />
      </JazzProvider>,
    );

    const userId = client.db.insert(users, { name: "Owner", team_id: undefined });
    client.db.insert(todos, {
      title: "owned-todo",
      done: false,
      priority: 1,
      owner_id: userId,
      tags: ["x"],
    });

    await waitForCondition(
      () => getText("rows").includes("Owner"),
      5000,
      "expected include useAllSuspense row",
    );
  });

  it("supports hop queries", async () => {
    const client = track(
      await createJazzClient({
        appId: uniqueId("hops"),
        dbName: uniqueId("hops"),
      }),
    );

    const query = makeQuery<Org>("users", {
      hops: ["team", "org"],
    });

    renderSuspense(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.name} />
      </JazzProvider>,
    );

    const orgId = client.db.insert(orgs, { name: "Hop Org" });
    const teamId = client.db.insert(teams, {
      name: "Hop Team",
      org_id: orgId,
      parent_id: undefined,
    });
    client.db.insert(users, { name: "Hop User", team_id: teamId });

    await waitForCondition(
      () => getText("rows").includes("Hop Org"),
      5000,
      "expected hop useAllSuspense row",
    );
  });

  it("supports gather queries", async () => {
    const client = track(
      await createJazzClient({
        appId: uniqueId("gather"),
        dbName: uniqueId("gather"),
      }),
    );

    const query = makeQuery<Team>("teams", {
      conditions: [{ column: "name", op: "eq", value: "leaf" }],
      gather: {
        max_depth: 10,
        step_table: "teams",
        step_current_column: "id",
        step_conditions: [],
        step_hops: ["parent"],
      },
    });

    renderSuspense(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.name} />
      </JazzProvider>,
    );

    const rootId = client.db.insert(teams, {
      name: "root",
      org_id: undefined,
      parent_id: undefined,
    });
    const midId = client.db.insert(teams, { name: "mid", org_id: undefined, parent_id: rootId });
    client.db.insert(teams, { name: "leaf", org_id: undefined, parent_id: midId });

    await waitForCondition(
      () => {
        const values = getText("rows").split("|");
        return values.includes("root") && values.includes("mid") && values.includes("leaf");
      },
      5000,
      "expected gather useAllSuspense rows",
    );
  });

  it("reacts to query changes", async () => {
    const client = track(
      await createJazzClient({
        appId: uniqueId("query-change"),
        dbName: uniqueId("query-change"),
      }),
    );

    client.db.insert(todos, {
      title: "open-task",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });
    client.db.insert(todos, {
      title: "done-task",
      done: true,
      priority: 2,
      owner_id: undefined,
      tags: ["x"],
    });

    function QuerySwitchProbe() {
      const [showDone, setShowDone] = React.useState(false);
      const query = makeQuery<Todo>("todos", {
        conditions: [{ column: "done", op: "eq", value: showDone }],
      });
      const rows = useAllSuspense(query);
      return (
        <>
          <button data-testid="toggle-query" onClick={() => setShowDone((value) => !value)}>
            toggle
          </button>
          <div data-testid="rows">{rows.map((row) => row.title).join("|")}</div>
        </>
      );
    }

    renderSuspense(
      <JazzProvider client={client}>
        <QuerySwitchProbe />
      </JazzProvider>,
    );

    await waitForCondition(
      () => getText("rows").includes("open-task") && !getText("rows").includes("done-task"),
      5000,
      "expected initial query to show only open task",
    );

    const toggleQuery = container?.querySelector('[data-testid="toggle-query"]');
    expect(toggleQuery).toBeTruthy();
    await userEvent.click(toggleQuery as HTMLElement);

    await waitForCondition(
      () => getText("rows").includes("done-task") && !getText("rows").includes("open-task"),
      5000,
      "expected updated query to show only done task",
    );
  });
});
