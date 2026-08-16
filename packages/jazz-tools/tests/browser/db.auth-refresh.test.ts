import { afterEach, describe, expect, it } from "vitest";
import { createDb, type QueryBuilder, type TableProxy } from "../../src/runtime/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { TestCleanup, uniqueDbName, waitForCondition, waitForQuery } from "./support.js";
import { getJazzServerInfo, getJazzServerJwtForUser } from "./testing-server.js";

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type TodoInit = {
  title: string;
  done: boolean;
};

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
};

const allTodos: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

describe("Db auth refresh browser integration", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  it.each([
    { mode: "main-thread memory", driver: { type: "memory" } as const },
    {
      mode: "persistent worker leader",
      driver: { type: "persistent", dbName: uniqueDbName("auth-refresh-worker") } as const,
    },
  ])(
    "recovers from auth loss in $mode after updateAuthToken and flushes queued local writes",
    async ({ mode, driver }) => {
      const { appId, serverUrl, adminSecret } = await getJazzServerInfo();

      const dbNameB = uniqueDbName("auth-refresh-b");
      const userId = "00000000-0000-0000-0000-00000000a111";
      const invalidJwt = makeFakeJwt({
        sub: userId,
        claims: { role: "member" },
        exp: Math.floor(Date.now() / 1000) + 3600,
      });
      const validJwt = await getJazzServerJwtForUser(userId, { role: "member" });

      const writer = ctx.track(
        await createDb({
          appId,
          serverUrl,
          jwtToken: invalidJwt,
          driver,
        }),
      );
      const reader = ctx.track(
        await createDb({
          appId,
          serverUrl,
          jwtToken: validJwt,
          driver: { type: "persistent", dbName: dbNameB },
        }),
      );

      await deploy({
        appId,
        serverUrl,
        adminSecret,
        schema,
        permissions: {
          todos: {
            select: { using: { type: "True" } },
            insert: { with_check: { type: "True" } },
            update: {
              using: { type: "True" },
              with_check: { type: "True" },
            },
            delete: { using: { type: "True" } },
          },
        },
      });

      const marker = `queued-after-auth-loss-${mode}-${Date.now()}`;
      writer.insert(todos, {
        title: marker,
        done: false,
      });

      await waitForCondition(
        async () => writer.getAuthState().error === "invalid",
        20_000,
        "writer should transition to unauthenticated after invalid JWT auth failure",
      );

      expect(writer.getAuthState()).toMatchObject({
        error: "invalid",
        session: {
          user_id: userId,
        },
      });

      writer.updateAuthToken(validJwt);

      await waitForCondition(
        async () => writer.getAuthState().error === undefined,
        20_000,
        "writer should return to authenticated after updateAuthToken",
      );

      await waitForQuery(
        reader,
        allTodos,
        (rows) => rows.some((row) => row.title === marker),
        "queued write should flush after auth refresh",
        20_000,
        "edge",
      );
    },
  );

  it("reapplies a follower-requested token after the old leader dies", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo();
    const userId = "00000000-0000-0000-0000-00000000a112";
    const invalidJwt = makeFakeJwt({
      sub: userId,
      claims: { role: "member" },
      exp: Math.floor(Date.now() / 1000) + 3600,
    });
    const validJwt = await getJazzServerJwtForUser(userId, { role: "member" });
    const sharedDbName = uniqueDbName("auth-refresh-failover");

    await deploy({
      appId,
      serverUrl,
      adminSecret,
      schema,
      permissions: {
        todos: {
          select: { using: { type: "True" } },
          insert: { with_check: { type: "True" } },
          update: { using: { type: "True" }, with_check: { type: "True" } },
          delete: { using: { type: "True" } },
        },
      },
    });

    const makeSharedTab = () =>
      createDb({
        appId,
        serverUrl,
        jwtToken: invalidJwt,
        driver: { type: "persistent", dbName: sharedDbName },
      });
    const oldLeader = ctx.track(await makeSharedTab());
    const requester = ctx.track(await makeSharedTab());
    const replacement = ctx.track(await makeSharedTab());
    const reader = ctx.track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: validJwt,
        driver: { type: "persistent", dbName: uniqueDbName("auth-refresh-reader") },
      }),
    );

    await Promise.all([
      oldLeader.all(allTodos, { tier: "local" }),
      requester.all(allTodos, { tier: "local" }),
      replacement.all(allTodos, { tier: "local" }),
    ]);
    await waitForCondition(
      async () => browserRole(oldLeader) === "leader" && browserRole(requester) === "follower",
      10_000,
      "the first tab should lead before auth failover",
    );
    browserBroker(requester)?.reportVisibility("hidden");
    browserBroker(replacement)?.reportVisibility("visible");

    requester.updateAuthToken(validJwt);
    await waitForCondition(
      async () => requester.getAuthState().error === undefined,
      20_000,
      "the follower-requested refresh should authenticate the namespace",
    );

    await browserRoleBridge(oldLeader).simulateCrash();
    await waitForCondition(
      async () => browserRole(replacement) === "leader",
      20_000,
      "the designated replacement should become leader",
    );

    const marker = `retained-refresh-after-failover-${Date.now()}`;
    replacement.insert(todos, { title: marker, done: false });
    await waitForQuery(
      reader,
      allTodos,
      (rows) => rows.some((row) => row.title === marker),
      "replacement leader should sync using the retained refreshed token",
      20_000,
      "edge",
    );
  });

  it("cancels a pending worker auth confirmation on disconnect and replays it on reconnect", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo();
    const userId = "00000000-0000-0000-0000-00000000a113";
    const invalidJwt = makeFakeJwt({
      sub: userId,
      claims: { role: "member" },
      exp: Math.floor(Date.now() / 1000) + 3600,
    });
    const validJwt = await getJazzServerJwtForUser(userId, { role: "member" });

    await deploy({
      appId,
      serverUrl,
      adminSecret,
      schema,
      permissions: {
        todos: {
          select: { using: { type: "True" } },
          insert: { with_check: { type: "True" } },
          update: { using: { type: "True" }, with_check: { type: "True" } },
          delete: { using: { type: "True" } },
        },
      },
    });

    const db = ctx.track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: invalidJwt,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("auth-refresh-disconnect"),
        },
      }),
    );
    await db.all(allTodos, { tier: "local" });
    await waitForCondition(
      async () => browserRole(db) === "leader",
      10_000,
      "the persistent tab should own the worker before auth refresh",
    );
    await waitForCondition(
      async () => db.getAuthState().error === "invalid",
      20_000,
      "the initial invalid credential should be rejected",
    );

    await browserRoleBridge(db).simulatePendingAuthConfirmation();
    db.updateAuthToken(validJwt);
    await waitForCondition(
      async () => browserRoleBridge(db).activeAuthRefresh !== null,
      10_000,
      "the worker auth refresh should be waiting for server confirmation",
    );

    await db.disconnect();
    await waitForCondition(
      async () => browserRoleBridge(db).activeAuthRefresh === null,
      10_000,
      "disconnect should settle the pending worker auth confirmation as deferred",
    );

    await db.reconnect();
    await waitForCondition(
      async () => db.getAuthState().error === undefined,
      20_000,
      "reconnect should replay and authenticate the deferred generation",
    );
  });
});

function browserConnection(db: unknown): any {
  return (db as any).connection;
}

function browserRole(db: unknown): string | undefined {
  return browserConnection(db)?.tabRole;
}

function browserBroker(db: unknown): any {
  return browserConnection(db)?.brokerClient;
}

function browserRoleBridge(db: unknown): any {
  const bridge = browserConnection(db)?.activeRoleBridge;
  if (!bridge?.simulateCrash) throw new Error("persistent browser leader bridge is unavailable");
  return bridge;
}

function toBase64Url(value: unknown): string {
  return btoa(JSON.stringify(value)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeFakeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.bad-signature`;
}
