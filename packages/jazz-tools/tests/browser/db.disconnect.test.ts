import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/";
import { createDb, Db, type QueryBuilder } from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { deploy } from "../../src/dev/catalogue.js";
import { TestCleanup, sleep, uniqueDbName, waitForQuery, withTimeout } from "./support.js";
import { getJazzServerInfo, type JazzServerInfo } from "./testing-server.js";

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
const { todos } = app;
type Todo = s.RowOf<typeof todos>;

const allowAllPermissions = s.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const PENDING_ASSERTION_MS = 750;
const LOCAL_OPERATION_TIMEOUT_MS = 2_000;
const SYNC_OPERATION_TIMEOUT_MS = 10_000;

type DbFactory = (
  ctx: TestCleanup,
  label: string,
  secret: string,
  server: JazzServerInfo,
) => Promise<Db>;

interface ConnectedPair {
  readonly db: Db;
  readonly peer: Db;
}

describe("Db disconnect/reconnect", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  describe("direct server connection", () => {
    it("syncs writes made while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createDirectDb);

      await db.disconnect();

      const offlineTitle = "offline write";
      db.insert(todos, { title: offlineTitle, done: true });

      const localRead = db.all(todoByTitle(offlineTitle), {
        tier: "local",
        localUpdates: "immediate",
        propagation: "local-only",
      });
      await expectStillPending(
        localRead,
        PENDING_ASSERTION_MS,
        "direct server connection: local read while disconnected",
      );

      const peerRowsBeforeReconnect = await withTimeout(
        peer.all(todoByTitle(offlineTitle), {
          tier: "local",
          localUpdates: "immediate",
          propagation: "local-only",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "direct server connection: peer local read before reconnect did not resolve",
      );
      expect(peerRowsBeforeReconnect).toEqual([]);

      await db.reconnect();

      const localRows = await withTimeout(
        localRead,
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: local read did not resolve after reconnect",
      );
      expect(localRows.some((row) => row.title === offlineTitle)).toBe(true);

      await waitForTodos(
        peer,
        (rows) => rows.some((row) => row.title === offlineTitle),
        "direct server connection: peer sees disconnected write after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("receives server updates missed while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createDirectDb);

      await db.disconnect();

      const serverOnlyTitle = "server only";
      await withTimeout(
        peer.insert(todos, { title: serverOnlyTitle, done: true }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: peer write did not reach edge while db was disconnected",
      );

      await expectStillPending(
        db.all(todoByTitle(serverOnlyTitle), {
          tier: "local",
          localUpdates: "immediate",
          propagation: "local-only",
        }),
        PENDING_ASSERTION_MS,
        "direct server connection: local-only read while disconnected",
      );

      await db.reconnect();

      await waitForTodos(
        db,
        (rows) => rows.some((row) => row.title === serverOnlyTitle),
        "direct server connection: disconnected client receives server update after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("keeps all wait tiers pending while disconnected", async () => {
      const { db } = await createDbPair(ctx, createDirectDb);

      await db.disconnect();

      const localWait = db
        .insert(todos, { title: "local wait", done: false })
        .wait({ tier: "local" });
      await expectStillPending(
        localWait,
        PENDING_ASSERTION_MS,
        "direct server connection: local wait while disconnected",
      );

      const edgeWait = db.insert(todos, { title: "edge wait", done: false }).wait({ tier: "edge" });
      await expectStillPending(
        edgeWait,
        PENDING_ASSERTION_MS,
        "direct server connection: edge wait while disconnected",
      );

      const globalWait = db
        .insert(todos, { title: "global wait", done: false })
        .wait({ tier: "global" });
      await expectStillPending(
        globalWait,
        PENDING_ASSERTION_MS,
        "direct server connection: global wait while disconnected",
      );

      await db.reconnect();

      await withTimeout(
        localWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: local wait did not settle after reconnect",
      );
      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: edge wait did not settle after reconnect",
      );
      await withTimeout(
        globalWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: global wait did not settle after reconnect",
      );
    }, 60_000);

    it("keeps disconnected reads pending until reconnect", async () => {
      const { db } = await createDbPair(ctx, createDirectDb);

      await db.disconnect();

      const localRead = db.all(app.todos, {
        tier: "local",
        localUpdates: "immediate",
        propagation: "local-only",
      });
      await expectStillPending(
        localRead,
        PENDING_ASSERTION_MS,
        "direct server connection: immediate local read while disconnected",
      );

      const deferredRead = db.all(app.todos, {
        tier: "edge",
        localUpdates: "deferred",
      });
      await expectStillPending(
        deferredRead,
        PENDING_ASSERTION_MS,
        "direct server connection: deferred read while disconnected",
      );

      await db.reconnect();

      const localRows = await withTimeout(
        localRead,
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: local read did not resolve after reconnect",
      );
      expect(localRows).toEqual([]);

      const deferredRows = await withTimeout(
        deferredRead,
        SYNC_OPERATION_TIMEOUT_MS,
        "direct server connection: deferred read did not resolve after reconnect",
      );
      expect(deferredRows).toEqual([]);
    }, 60_000);
  });

  describe("worker mode", () => {
    it("syncs writes made while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const offlineTitle = "offline write";
      db.insert(todos, { title: offlineTitle, done: true });

      const localRows = await withTimeout(
        db.all(todoByTitle(offlineTitle), {
          tier: "local",
          localUpdates: "immediate",
          propagation: "local-only",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "worker mode: local read for disconnected write did not resolve",
      );
      expect(localRows.some((row) => row.title === offlineTitle)).toBe(true);

      const peerRowsBeforeReconnect = await withTimeout(
        peer.all(todoByTitle(offlineTitle), {
          tier: "local",
          localUpdates: "immediate",
          propagation: "local-only",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "worker mode: peer local read before reconnect did not resolve",
      );
      expect(peerRowsBeforeReconnect).toEqual([]);

      await db.reconnect();

      await waitForTodos(
        peer,
        (rows) => rows.some((row) => row.title === offlineTitle),
        "worker mode: peer sees disconnected write after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("receives server updates missed while disconnected after reconnect", async () => {
      const { db, peer } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const serverOnlyTitle = "server only";
      await withTimeout(
        peer.insert(todos, { title: serverOnlyTitle, done: true }).wait({ tier: "edge" }),
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: peer write did not reach edge while db was disconnected",
      );

      const disconnectedLocalRows = await withTimeout(
        db.all(todoByTitle(serverOnlyTitle), {
          tier: "local",
          localUpdates: "immediate",
          propagation: "local-only",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "worker mode: local-only read while disconnected did not resolve",
      );
      expect(disconnectedLocalRows).toEqual([]);

      await db.reconnect();

      await waitForTodos(
        db,
        (rows) => rows.some((row) => row.title === serverOnlyTitle),
        "worker mode: disconnected client receives server update after reconnect",
        SYNC_OPERATION_TIMEOUT_MS,
        "edge",
      );
    }, 60_000);

    it("resolves local waits and keeps edge/global waits pending while disconnected", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const localWait = db
        .insert(todos, { title: "local wait", done: false })
        .wait({ tier: "local" });
      await withTimeout(
        localWait,
        LOCAL_OPERATION_TIMEOUT_MS,
        "worker mode: local wait should resolve while disconnected",
      );

      const edgeWait = db.insert(todos, { title: "edge wait", done: false }).wait({ tier: "edge" });
      await expectStillPending(
        edgeWait,
        PENDING_ASSERTION_MS,
        "worker mode: edge wait while disconnected",
      );

      const globalWait = db
        .insert(todos, { title: "global wait", done: false })
        .wait({ tier: "global" });
      await expectStillPending(
        globalWait,
        PENDING_ASSERTION_MS,
        "worker mode: global wait while disconnected",
      );

      await db.reconnect();

      await withTimeout(
        edgeWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: edge wait did not settle after reconnect",
      );
      await withTimeout(
        globalWait,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: global wait did not settle after reconnect",
      );
    }, 60_000);

    it("resolves local reads and defers edge reads while disconnected", async () => {
      const { db } = await createDbPair(ctx, createWorkerDb);

      await db.disconnect();

      const title = "read mode";
      db.insert(todos, { title, done: true });

      const localRows = await withTimeout(
        db.all(todoByTitle(title), {
          tier: "local",
          localUpdates: "immediate",
          propagation: "local-only",
        }),
        LOCAL_OPERATION_TIMEOUT_MS,
        "worker mode: immediate local read while disconnected did not resolve",
      );
      expect(localRows.some((row) => row.title === title)).toBe(true);

      const deferredRead = db.all(todoByTitle(title), {
        tier: "edge",
        localUpdates: "deferred",
      });
      await expectStillPending(
        deferredRead,
        PENDING_ASSERTION_MS,
        "worker mode: deferred read while disconnected",
      );

      await db.reconnect();

      await withTimeout(
        deferredRead,
        SYNC_OPERATION_TIMEOUT_MS,
        "worker mode: deferred read did not resolve after reconnect",
      );
    }, 60_000);
  });
});

async function createDirectDb(
  ctx: TestCleanup,
  _label: string,
  secret: string,
  server: JazzServerInfo,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      driver: { type: "memory" },
      serverUrl: server.serverUrl,
      secret,
    }),
  );
}

async function createWorkerDb(
  ctx: TestCleanup,
  label: string,
  secret: string,
  server: JazzServerInfo,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
      serverUrl: server.serverUrl,
      secret,
    }),
  );
}

async function createDbPair(ctx: TestCleanup, createDbForMode: DbFactory): Promise<ConnectedPair> {
  const label = uniqueDbName("db-disconnect-pair");
  const server = await publishSyncServerSchemaAndPermissions(label);
  const sharedSecret = generateAuthSecret();
  const db = await createDbForMode(ctx, `${label}-a`, sharedSecret, server);
  const peer = await createDbForMode(ctx, `${label}-peer`, sharedSecret, server);

  return { db, peer };
}

function todoByTitle(title: string): QueryBuilder<Todo> {
  return app.todos.where({ title: { eq: title } });
}

async function waitForTodos(
  db: Db,
  predicate: (rows: Todo[]) => boolean,
  label: string,
  timeoutMs = SYNC_OPERATION_TIMEOUT_MS,
  tier?: "local" | "edge",
): Promise<Todo[]> {
  return waitForQuery(db, app.todos, predicate, label, timeoutMs, tier);
}

async function expectStillPending<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const result = await Promise.race([
    promise.then(
      () => ({ state: "fulfilled" as const }),
      (error) => ({ state: "rejected" as const, error }),
    ),
    sleep(timeoutMs).then(() => ({ state: "pending" as const })),
  ]);

  if (result.state === "pending") return;

  const reason =
    result.state === "rejected" && result.error instanceof Error ? `: ${result.error.message}` : "";
  throw new Error(`${label} ${result.state}${reason}`);
}

async function publishSyncServerSchemaAndPermissions(_appId: string): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(_appId);
  const { appId, serverUrl, adminSecret } = testingServer;
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: app,
    permissions: allowAllPermissions,
  });
  return testingServer;
}
