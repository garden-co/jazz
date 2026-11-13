// The Inspector a backend serves, end to end against a real createJazz
// backend: the page's API calls go to `jazz.handle`, as they would over HTTP.

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { schema as s } from "jazz-tools";
import { createJazz, memory, type InspectGrant, type Jazz } from "jazz-tools/backend";
import { inspectorLink } from "jazz-tools/inspect";
import { afterEach, describe, expect, it, vi } from "vitest";
import { GenericQueryBuilder } from "../utility/generic-query-builder.js";
import { createInspectorCall, InspectorApiError } from "./api.js";
import { decodeRow, toInspectQuery, toWriteOps } from "./api-data.js";
import { ApprovalPage } from "./ApprovalPage.js";
import { BackendApp, initialRouteFor } from "./BackendApp.js";

const app = s.defineApp(
  {
    todos: s.table(
      {
        title: s.string(),
        done: s.boolean(),
        owner: s.string(),
        rank: s.bigint().optional(),
        due: s.timestamp().optional(),
      },
      {},
    ),
  },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.where({ owner: "alice" });
  policy.todos.allowInsert.always();
  policy.todos.allowUpdate.always();
  policy.todos.allowDelete.always();
});

const PAGE = "http://localhost:3000/jazz/inspect/";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  cleanup();
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

function staff(request: Request): InspectGrant | null {
  const by = request.headers.get("x-staff");
  return by ? { by, write: request.headers.get("x-staff-write") === "1" } : null;
}

async function backend() {
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate: (request) => {
      const userId = request.headers.get("x-test-user");
      return userId ? { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600 } : null;
    },
    inspect: staff,
    inspector: { audit: () => {} },
  });
  cleanups.push(() => jazz.close());
  await jazz.db
    .insert(app.todos, { title: "alice's milk", done: false, owner: "alice", rank: 7n })
    .wait({ tier: "global" });
  await jazz.db
    .insert(app.todos, { title: "bob's bread", done: true, owner: "bob" })
    .wait({ tier: "global" });
  return jazz;
}

/** The page's API, as the browser of `headers` reaches it. */
function callAs(jazz: Jazz, headers: Record<string, string>) {
  const calls: unknown[] = [];
  const call = createInspectorCall("http://localhost:3000/jazz/inspect/api", async (url, init) => {
    calls.push(JSON.parse(String(init?.body)));
    return jazz.handle(
      new Request(String(url), {
        ...init,
        headers: { ...(init?.headers as Record<string, string>), ...headers },
      }),
    );
  });
  return { call, calls };
}

describe("the data explorer's queries over the API", () => {
  const schema = app.wasmSchema;

  it("writes the grid's query as the API's", () => {
    const query = new GenericQueryBuilder("todos", schema)
      .select("*", "$createdAt")
      .where({ done: false, title: { contains: "milk" }, rank: { gt: 3n } })
      .orderBy("title", "desc")
      .limit(26)
      .offset(25);
    expect(toInspectQuery(query)).toEqual({
      table: "todos",
      where: { done: { eq: false }, title: { contains: "milk" }, rank: { gt: "3" } },
      select: ["*", "$createdAt"],
      orderBy: [["title", "desc"]],
      limit: 26,
      offset: 25,
    });
  });

  it("names a filter the API can't take", () => {
    const query = new GenericQueryBuilder("todos", schema)
      .where({ rank: { gt: 1n } })
      .where({ rank: { gt: 2n } });
    expect(() => toInspectQuery(query)).toThrow(/Two "gt" filters on rank are not supported yet/);
  });

  it("decodes rows into what the grid holds, and encodes writes back", () => {
    const columns = new Map(schema.todos!.columns.map((column) => [column.name, column]));
    const row = decodeRow(columns, {
      id: "t1",
      rank: "9007199254740993",
      due: "2026-11-03T10:00:00.000Z",
      $createdAt: "2026-11-01T00:00:00.000Z",
      title: "x",
    });
    expect(row.rank).toBe(9007199254740993n);
    expect(row.due).toEqual(new Date("2026-11-03T10:00:00.000Z"));
    expect(row.$createdAt).toEqual(new Date("2026-11-01T00:00:00.000Z"));
    expect(
      toWriteOps("todos", {
        updates: [{ rowId: "t1", updates: { rank: 5n, due: new Date(0) } }],
        deletes: ["t2"],
        inserts: [{ id: "t3", values: { title: "new" } }],
      }),
    ).toEqual([
      {
        kind: "update",
        table: "todos",
        id: "t1",
        values: { rank: "5", due: "1970-01-01T00:00:00.000Z" },
      },
      { kind: "delete", table: "todos", id: "t2" },
      { kind: "insert", table: "todos", id: "t3", values: { title: "new" } },
    ]);
  });

  it("opens a deep link's table, row or query", () => {
    expect(initialRouteFor({}, schema)).toBe("/data-explorer");
    expect(initialRouteFor({ table: "todos" }, schema)).toBe("/data-explorer/todos/data");
    expect(initialRouteFor({ table: "nope" }, schema)).toBe("/data-explorer");
    expect(initialRouteFor({ table: "todos", row: "r1" }, schema)).toMatch(
      /^\/data-explorer\/todos\/data\?filters=/,
    );
    expect(initialRouteFor({ query: { table: "todos" } }, schema)).toBe("/linked");
  });
});

describe("the Inspector a backend serves", () => {
  it("shows every row with the backend's authority, and who is signed in", async () => {
    const jazz = await backend();
    const { call } = callAs(jazz, { "x-staff": "ops@example.com" });
    render(<BackendApp pageUrl={`${PAGE}#table=todos`} call={call} />);

    expect(await screen.findByText("alice's milk")).toBeTruthy();
    expect(await screen.findByText("bob's bread")).toBeTruthy();
    expect(screen.getByText("ops@example.com")).toBeTruthy();
    expect(screen.getByText("read-only")).toBeTruthy();
  });

  it("views as a user, under their permissions", async () => {
    const jazz = await backend();
    // Only a user with an account can be viewed as.
    await jazz.createAccountFor("alice");
    const { call, calls } = callAs(jazz, { "x-staff": "ops@example.com" });
    render(<BackendApp pageUrl={`${PAGE}#table=todos&as=alice`} call={call} />);

    expect(await screen.findByText("alice's milk")).toBeTruthy();
    expect(screen.queryByText("bob's bread")).toBeNull();
    expect(calls).toContainEqual(expect.objectContaining({ op: "query", as: "alice" }));

    // Back to everything.
    const input = screen.getByLabelText("View as user") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "" } });
    fireEvent.blur(input);
    expect(await screen.findByText("bob's bread")).toBeTruthy();
  });

  it("views as a user with the claims given for them, and flags when none are", async () => {
    // Only managers (a claim) see todos. (This schema's owner is a plain
    // string, so the policy reads the claim alone.)
    const jazz = await createJazz({
      app,
      permissions: s.definePermissions(app, ({ policy, session }) => {
        policy.todos.allowRead.where(session.where({ "claims.role": "manager" }));
      }),
      store: memory(),
      authenticate: () => null,
      inspect: staff,
      inspector: { audit: () => {} },
    });
    cleanups.push(() => jazz.close());
    await jazz.createAccountFor("manager");
    await jazz.db
      .insert(app.todos, { title: "bob's bread", done: true, owner: "bob" })
      .wait({ tier: "global" });
    const { call, calls } = callAs(jazz, { "x-staff": "ops@example.com" });
    render(<BackendApp pageUrl={`${PAGE}#table=todos&as=manager`} call={call} />);

    // No claims: a session with none, and the Inspector says so.
    expect(await screen.findByText("no claims")).toBeTruthy();
    expect(screen.queryByText("bob's bread")).toBeNull();

    const claims = screen.getByLabelText("with claims") as HTMLInputElement;
    fireEvent.change(claims, { target: { value: '{"role":"manager"}' } });
    fireEvent.blur(claims);
    expect(await screen.findByText("bob's bread")).toBeTruthy();
    expect(screen.queryByText("no claims")).toBeNull();
    expect(calls).toContainEqual(
      expect.objectContaining({
        op: "query",
        as: { userId: "manager", claims: { role: "manager" } },
      }),
    );
  });

  it("opens on exactly the rows a deep link's query names", async () => {
    const jazz = await backend();
    const { call } = callAs(jazz, { "x-staff": "ops@example.com" });
    const link = inspectorLink(PAGE, {
      query: { table: "todos", where: { done: true }, select: ["title"] },
    });
    render(<BackendApp pageUrl={link} call={call} />);

    expect(await screen.findByRole("heading", { name: "Linked query" })).toBeTruthy();
    expect(await screen.findByText("bob's bread")).toBeTruthy();
    expect(screen.queryByText("alice's milk")).toBeNull();
    expect(screen.getByText("1 row")).toBeTruthy();
    expect(screen.getByRole("link", { name: "Open todos in the data explorer" })).toBeTruthy();
  });

  it("explains which clause lets the user viewed as read each row of a linked query", async () => {
    const jazz = await backend();
    await jazz.createAccountFor("alice");
    const { call, calls } = callAs(jazz, { "x-staff": "ops@example.com" });
    const link = inspectorLink(PAGE, { query: { table: "todos" }, as: "alice" });
    render(<BackendApp pageUrl={link} call={call} />);

    fireEvent.click(await screen.findByRole("button", { name: "Explain why" }));
    const explain = await screen.findByRole("region", { name: "Explain" });
    expect(await within(explain).findByText('owner = "alice"')).toBeTruthy();
    const rows = within(explain).getAllByRole("row").slice(1);
    expect(rows.map((row) => within(row).getAllByRole("cell")[1]!.textContent).sort()).toEqual([
      "no",
      "yes",
    ]);
    expect(within(explain).getByText('0: owner = "alice"')).toBeTruthy();
    expect(within(explain).getByText("no clause holds")).toBeTruthy();
    expect(calls).toContainEqual(expect.objectContaining({ op: "explain", as: "alice" }));
  });

  it("says who decides when the hook refuses", async () => {
    const jazz = await backend();
    const { call } = callAs(jazz, {});
    render(<BackendApp pageUrl={PAGE} call={call} />);

    expect(await screen.findByRole("heading", { name: "Not allowed in" })).toBeTruthy();
    expect(screen.getByText(/the `inspect` option of createJazz/)).toBeTruthy();
  });

  it("writes through the API and shows the result", async () => {
    const jazz = await backend();
    const { call, calls } = callAs(jazz, { "x-staff": "ops@example.com", "x-staff-write": "1" });
    render(<BackendApp pageUrl={`${PAGE}#table=todos`} call={call} />);
    expect(await screen.findByText("can write")).toBeTruthy();
    await screen.findByText("bob's bread");

    // The grid's own save path, through the data source the app provided.
    const source = (await import("./api-data.js")).createApiDataSource({
      call,
      schema: app.wasmSchema,
      as: () => undefined,
    });
    const { renderHook } = await import("@testing-library/react");
    const { result } = renderHook(() => source.useWrite());
    const [bob] = await jazz.db.all(app.todos.where({ owner: "bob" }));
    const write = await result.current({ _table: "todos" } as never, {
      updates: [{ rowId: bob!.id, updates: { title: "bob's rye" } }],
      deletes: [],
      inserts: [],
    });
    await write.wait({ tier: "global" });
    expect(calls).toContainEqual(expect.objectContaining({ op: "write" }));
    expect((await jazz.db.all(app.todos.where({ owner: "bob" })))[0]?.title).toBe("bob's rye");
    expect(await screen.findByText("bob's rye", {}, { timeout: 5_000 })).toBeTruthy();
  });

  it("surfaces a refused write", async () => {
    const jazz = await backend();
    const { call } = callAs(jazz, { "x-staff": "ops@example.com" });
    const source = (await import("./api-data.js")).createApiDataSource({
      call,
      schema: app.wasmSchema,
      as: () => undefined,
    });
    const { renderHook } = await import("@testing-library/react");
    const { result } = renderHook(() => source.useWrite());
    const write = await result.current({ _table: "todos" } as never, {
      updates: [],
      deletes: ["00000000-0000-4000-8000-000000000009"],
      inserts: [],
    });
    await expect(write.wait({ tier: "global" })).rejects.toBeInstanceOf(InspectorApiError);
  });
});

describe("approving an agent's token", () => {
  const grant = {
    state: "st4te",
    label: "debugging #123",
    callback: "http://127.0.0.1:5123/callback",
  };

  it("hands the token to the asking terminal on this computer", async () => {
    const jazz = await backend();
    const { call } = callAs(jazz, { "x-staff": "ops@example.com" });
    const whoami = await call<never>({ op: "whoami" });
    const navigate = vi.fn();
    render(
      <ApprovalPage
        grant={grant}
        whoami={whoami}
        call={call}
        endpoint="http://localhost:3000/jazz"
        navigate={navigate}
      />,
    );
    expect(screen.getByText("debugging #123")).toBeTruthy();
    expect(screen.getByText(/Approve only a request you started yourself/)).toBeTruthy();
    expect(screen.getByText(/port 5123/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));

    await waitFor(() => expect(navigate).toHaveBeenCalledTimes(1));
    const handed = new URL(navigate.mock.calls[0]![0] as string);
    expect(`${handed.origin}${handed.pathname}`).toBe("http://127.0.0.1:5123/callback");
    expect(handed.searchParams.get("state")).toBe("st4te");
    const token = handed.searchParams.get("token")!;
    const asAgent = createInspectorCall("http://localhost:3000/jazz/inspect/api", (url, init) =>
      jazz.handle(
        new Request(String(url), {
          ...init,
          headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
        }),
      ),
    );
    expect(await asAgent({ op: "whoami" })).toMatchObject({
      by: "ops@example.com",
      via: "token",
      write: false,
    });
  });

  it("shows the token to paste when there is no terminal on this computer", async () => {
    const jazz = await backend();
    const { call } = callAs(jazz, { "x-staff": "ops@example.com" });
    const whoami = await call<never>({ op: "whoami" });
    const navigate = vi.fn();
    render(
      <ApprovalPage
        grant={{ ...grant, callback: "https://evil.example.com/callback" }}
        whoami={whoami}
        call={call}
        endpoint="http://localhost:3000/jazz"
        navigate={navigate}
      />,
    );
    expect(screen.getByText(/no terminal on this computer/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    const command = await screen.findByText(/npx jazz-tools inspect login --app/);
    expect(command.textContent).toMatch(/--token jzi1_/);
    expect(navigate).not.toHaveBeenCalled();
  });

  it("won't let a read-only person approve writes, and denies", async () => {
    const jazz = await backend();
    const { call, calls } = callAs(jazz, { "x-staff": "ops@example.com" });
    const whoami = await call<never>({ op: "whoami" });
    render(
      <ApprovalPage
        grant={{ ...grant, write: true }}
        whoami={whoami}
        call={call}
        endpoint="http://localhost:3000/jazz"
      />,
    );
    const card = screen.getByRole("region", { name: "Approve agent access" });
    expect(within(card).getByText(/can't approve write access/)).toBeTruthy();
    expect((screen.getByRole("button", { name: "Approve" }) as HTMLButtonElement).disabled).toBe(
      true,
    );
    fireEvent.click(screen.getByRole("button", { name: "Deny" }));
    expect(screen.getByText("Denied. Nothing was granted.")).toBeTruthy();
    expect(calls).not.toContainEqual(expect.objectContaining({ op: "mint_token" }));
  });
});
