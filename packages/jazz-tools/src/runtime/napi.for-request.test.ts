import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeAll, describe, expect, it, onTestFinished, vi } from "vitest";
import { schema as s } from "jazz-tools";
import type { Db } from "./db.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "./schema-fetch.js";
import { startLocalJazzServer } from "../testing/local-jazz-server.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

// ---------------------------------------------------------------------------
// Inline schema + permissions
// ---------------------------------------------------------------------------
const todoApp = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    owner_id: s.string(),
  }),
});

const todoAppPermissions = s.definePermissions(todoApp, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user_id });
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate.where({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type TempRuntimeData = {
  dataRoot: string;
  dataPath: string;
};

type LocalFirstIdentity = {
  token: string;
  userId: string;
};

type TestContext = {
  context: {
    asBackend(): Db;
    forRequest(request: { headers: Record<string, string> }): Promise<Db>;
    shutdown(): Promise<void>;
  };
  runtimeData: TempRuntimeData;
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Deterministic base64url-encoded 32-byte seeds — different values produce
// different local-first identities, which map to different owner_id values.
const ALICE_SECRET = "YWxpY2Utc2VjcmV0LWZvci10ZXN0LXB1cnBvc2VzISE";
const BOB_SECRET = "Ym9iLS0tc2VjcmV0LWZvci10ZXN0LXB1cnBvc2VzISE";
const CAROL_SECRET = "Y2Fyb2wtc2VjcmV0LWZvci10ZXN0LXB1cnBvc2VzISE";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function createTempRuntimeData(prefix: string): Promise<TempRuntimeData> {
  const dataRoot = await mkdtemp(join(tmpdir(), prefix));
  return { dataRoot, dataPath: join(dataRoot, "runtime.db") };
}

async function cleanupTempRuntimeData(data: TempRuntimeData | null): Promise<void> {
  if (!data) return;
  await rm(data.dataRoot, { recursive: true, force: true });
}

async function settleAsyncSyncWork(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

/**
 * Mints a local-first bearer token for the given seed and resolves the
 * canonical user ID derived from it.
 */
async function mintLocalFirstIdentity(secret: string, appId: string): Promise<LocalFirstIdentity> {
  const { mintLocalFirstToken, verifyLocalFirstIdentityProof } = await loadNapiModule();
  const token = mintLocalFirstToken(secret, appId, 60);
  const userId = verifyLocalFirstIdentityProof(token, appId).id;
  return { token, userId };
}

/**
 * Publishes the todo app schema + permissions to the server, creates a
 * persistent `JazzContext`, and returns both the context and the temp
 * runtime data directory for later cleanup.
 */
async function createTestContext(
  server: { url: string },
  appId: string,
  backendSecret: string,
  adminSecret: string,
): Promise<TestContext> {
  const { hash: schemaHash } = await publishStoredSchema(server.url, {
    adminSecret,
    schema: todoApp.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(server.url, { adminSecret });
  await publishStoredPermissions(server.url, {
    adminSecret,
    schemaHash,
    permissions: todoAppPermissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });

  const runtimeData = await createTempRuntimeData("jazz-napi-concurrent-request-");
  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId,
    app: todoApp,
    permissions: todoAppPermissions,
    driver: { type: "persistent", dataPath: runtimeData.dataPath },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    userBranch: "main",
    tier: "worker",
  });

  return { context, runtimeData };
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await loadNapiModule();
});

// ---------------------------------------------------------------------------
// Standalone (non-concurrent) forRequest scenarios
// ---------------------------------------------------------------------------

describe("forRequest auth and policy", () => {
  it("insert respects ownership policy — own-row insert succeeds, foreign owner_id is rejected", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-request-backend-secret";
    const adminSecret = "napi-request-admin-secret";
    const scopeTag = `request-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const testCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const { context } = testCtx;

    onTestFinished(async () => {
      await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(testCtx.runtimeData);
      await server.stop();
    });

    const alice = await mintLocalFirstIdentity(ALICE_SECRET, appId);
    const aliceDb = await context.forRequest({
      headers: { authorization: `Bearer ${alice.token}` },
    });
    const backendDb = context.asBackend();

    const row = await withTimeout(
      aliceDb.insertDurable(
        todoApp.todos,
        { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      5_000,
      "insert timed out",
    );

    // forRequest session surfaces its own row.
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          aliceDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          5_000,
          "request-scoped read timed out",
        );
        expect(rows).toEqual([
          expect.objectContaining({ id: row.id, title: "alice-todo", owner_id: alice.userId }),
        ]);
      },
      { timeout: 10_000 },
    );

    // Insert with a foreign owner_id is rejected by the policy.
    await expect(
      aliceDb.insertDurable(
        todoApp.todos,
        { title: "imposter", done: false, description: scopeTag, owner_id: "someone-else" },
        { tier: "edge" },
      ),
    ).rejects.toThrow();

    // Backend can see the row regardless of ownership.
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          backendDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          5_000,
          "backend read timed out",
        );
        expect(rows).toContainEqual(expect.objectContaining({ id: row.id }));
      },
      { timeout: 10_000 },
    );
  }, 30_000);

  it("rejects local-first token when allowLocalFirstAuth is false", async () => {
    const appId = randomUUID();
    const runtimeData = await createTempRuntimeData("jazz-napi-no-local-first-");
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const context = createJazzContext({
      appId,
      app: todoApp,
      permissions: todoAppPermissions,
      driver: { type: "persistent", dataPath: runtimeData.dataPath },
      allowLocalFirstAuth: false,
    });

    onTestFinished(async () => {
      await context.shutdown();
      await cleanupTempRuntimeData(runtimeData);
    });

    const alice = await mintLocalFirstIdentity(ALICE_SECRET, appId);
    await expect(
      context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
    ).rejects.toThrow(/allowLocalFirstAuth/i);
  });

  it("backend sees all rows; forSession and forRequest Db filter to the authenticated user", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-query-backend-secret";
    const adminSecret = "napi-query-admin-secret";
    const scopeTag = `session-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });

    // Publish schema once via the writer context; the reader shares the same published schema.
    const writerCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const readerRuntimeData = await createTempRuntimeData("jazz-napi-query-reader-");
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const readerContext = createJazzContext({
      appId,
      app: todoApp,
      permissions: todoAppPermissions,
      driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
      serverUrl: server.url,
      backendSecret,
      env: "test",
      userBranch: "main",
    });

    onTestFinished(async () => {
      await writerCtx.context.shutdown();
      await readerContext.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(writerCtx.runtimeData);
      await cleanupTempRuntimeData(readerRuntimeData);
      await server.stop();
    });

    // Derive user IDs so backend writes use the same owner_id values that local-first sessions expect.
    const [alice, bob, carol] = await Promise.all([
      mintLocalFirstIdentity(ALICE_SECRET, appId),
      mintLocalFirstIdentity(BOB_SECRET, appId),
      mintLocalFirstIdentity(CAROL_SECRET, appId),
    ]);

    const writerBackend = writerCtx.context.asBackend();
    const readerBackend = readerContext.asBackend();

    // Seed rows for all three users through the backend writer context.
    await Promise.all([
      withTimeout(
        writerBackend.insertDurable(
          todoApp.todos,
          { title: "alice-item", done: false, description: scopeTag, owner_id: alice.userId },
          { tier: "edge" },
        ),
        5_000,
        "alice writer insert timed out",
      ),
      withTimeout(
        writerBackend.insertDurable(
          todoApp.todos,
          { title: "bob-item", done: false, description: scopeTag, owner_id: bob.userId },
          { tier: "edge" },
        ),
        5_000,
        "bob writer insert timed out",
      ),
      withTimeout(
        writerBackend.insertDurable(
          todoApp.todos,
          { title: "carol-item", done: false, description: scopeTag, owner_id: carol.userId },
          { tier: "edge" },
        ),
        5_000,
        "carol writer insert timed out",
      ),
    ]);

    const aliceSessionDb = readerContext.forSession({ user_id: alice.userId, claims: {} });
    const aliceRequestDb = await readerContext.forRequest({
      headers: { authorization: `Bearer ${alice.token}` },
    });

    // Backend reader sees all three rows.
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          readerBackend.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          5_000,
          "backend reader timed out",
        );
        expect(rows.map((r) => r.title).sort()).toEqual(["alice-item", "bob-item", "carol-item"]);
      },
      { timeout: 10_000 },
    );

    // Both alice handles surface only alice's row.
    await vi.waitFor(
      async () => {
        const [sessionRows, requestRows] = await Promise.all([
          withTimeout(
            aliceSessionDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "alice session read timed out",
          ),
          withTimeout(
            aliceRequestDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "alice request read timed out",
          ),
        ]);
        expect(sessionRows.map((r) => r.title)).toEqual(["alice-item"]);
        expect(requestRows.map((r) => r.title)).toEqual(["alice-item"]);
      },
      { timeout: 10_000 },
    );
  }, 30_000);
});

describe("forRequest concurrent session isolation", () => {
  it("isolates concurrent forRequest sessions on the same context — alice and bob see only their own rows", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });

    const testCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const { context } = testCtx;

    onTestFinished(async () => {
      await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(testCtx.runtimeData);
      await server.stop();
    });

    // Mint local-first tokens for alice and bob. No JWKS server needed —
    // forRequest verifies these directly via the NAPI module.
    const [alice, bob] = await Promise.all([
      mintLocalFirstIdentity(ALICE_SECRET, appId),
      mintLocalFirstIdentity(BOB_SECRET, appId),
    ]);

    // Obtain session-scoped Db handles for alice and bob concurrently from
    // the same shared context — this is the pattern a real server would use.
    const [aliceDb, bobDb] = await Promise.all([
      context.forRequest({
        headers: { authorization: `Bearer ${alice.token}` },
      }),
      context.forRequest({
        headers: { authorization: `Bearer ${bob.token}` },
      }),
    ]);

    // Fire writes for both users in parallel.
    await Promise.all([
      withTimeout(
        aliceDb.insertDurable(
          todoApp.todos,
          {
            title: "alice-todo",
            done: false,
            description: scopeTag,
            owner_id: alice.userId,
          },
          { tier: "edge" },
        ),
        5_000,
        "alice insert timed out",
      ),
      withTimeout(
        bobDb.insertDurable(
          todoApp.todos,
          {
            title: "bob-todo",
            done: false,
            description: scopeTag,
            owner_id: bob.userId,
          },
          { tier: "edge" },
        ),
        5_000,
        "bob insert timed out",
      ),
    ]);

    // Alice's scoped Db should only surface her own row.
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          aliceDb.all(todoApp.todos.where({ description: scopeTag }), {
            tier: "edge",
          }),
          5_000,
          "alice read timed out",
        );
        expect(rows.map((r) => r.title).sort()).toEqual(["alice-todo"]);
      },
      { timeout: 10_000 },
    );

    // Bob's scoped Db should only surface his own row.
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          bobDb.all(todoApp.todos.where({ description: scopeTag }), {
            tier: "edge",
          }),
          5_000,
          "bob read timed out",
        );
        expect(rows.map((r) => r.title).sort()).toEqual(["bob-todo"]);
      },
      { timeout: 10_000 },
    );

    // Cross-user write rejection: alice and bob must not be able to insert
    // rows owned by each other, even when their requests are in flight concurrently.
    await Promise.all([
      expect(
        aliceDb.insertDurable(
          todoApp.todos,
          {
            title: "alice-as-bob",
            done: false,
            description: scopeTag,
            owner_id: bob.userId,
          },
          { tier: "edge" },
        ),
      ).rejects.toThrow(),
      expect(
        bobDb.insertDurable(
          todoApp.todos,
          {
            title: "bob-as-alice",
            done: false,
            description: scopeTag,
            owner_id: alice.userId,
          },
          { tier: "edge" },
        ),
      ).rejects.toThrow(),
    ]);

    // A fresh forRequest call for alice (simulating a later HTTP request)
    // must still be isolated from bob's data.
    const freshAlice = await mintLocalFirstIdentity(ALICE_SECRET, appId);
    const aliceDb2 = await context.forRequest({
      headers: { authorization: `Bearer ${freshAlice.token}` },
    });
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          aliceDb2.all(todoApp.todos.where({ description: scopeTag }), {
            tier: "edge",
          }),
          5_000,
          "alice2 read timed out",
        );
        expect(rows.map((r) => r.title).sort()).toEqual(["alice-todo"]);
      },
      { timeout: 10_000 },
    );
  }, 30_000);

  it("concurrent updateDurable respects per-user ownership — cross-user update is rejected", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const testCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const { context } = testCtx;

    onTestFinished(async () => {
      await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(testCtx.runtimeData);
      await server.stop();
    });

    const [alice, bob] = await Promise.all([
      mintLocalFirstIdentity(ALICE_SECRET, appId),
      mintLocalFirstIdentity(BOB_SECRET, appId),
    ]);
    const [aliceDb, bobDb] = await Promise.all([
      context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
      context.forRequest({ headers: { authorization: `Bearer ${bob.token}` } }),
    ]);

    const [aliceRow, bobRow] = await Promise.all([
      withTimeout(
        aliceDb.insertDurable(
          todoApp.todos,
          { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
          { tier: "edge" },
        ),
        5_000,
        "alice insert timed out",
      ),
      withTimeout(
        bobDb.insertDurable(
          todoApp.todos,
          { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
          { tier: "edge" },
        ),
        5_000,
        "bob insert timed out",
      ),
    ]);

    // Each user can update their own row concurrently.
    await Promise.all([
      withTimeout(
        aliceDb.updateDurable(
          todoApp.todos,
          aliceRow.id,
          { title: "alice-updated" },
          { tier: "edge" },
        ),
        5_000,
        "alice update timed out",
      ),
      withTimeout(
        bobDb.updateDurable(todoApp.todos, bobRow.id, { title: "bob-updated" }, { tier: "edge" }),
        5_000,
        "bob update timed out",
      ),
    ]);

    await vi.waitFor(
      async () => {
        const [aliceRows, bobRows] = await Promise.all([
          withTimeout(
            aliceDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "alice read timed out",
          ),
          withTimeout(
            bobDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "bob read timed out",
          ),
        ]);
        expect(aliceRows.map((r) => r.title)).toEqual(["alice-updated"]);
        expect(bobRows.map((r) => r.title)).toEqual(["bob-updated"]);
      },
      { timeout: 10_000 },
    );

    // Cross-user update must be rejected.
    await Promise.all([
      expect(
        aliceDb.updateDurable(
          todoApp.todos,
          bobRow.id,
          { title: "alice-as-bob" },
          { tier: "edge" },
        ),
      ).rejects.toThrow(),
      expect(
        bobDb.updateDurable(
          todoApp.todos,
          aliceRow.id,
          { title: "bob-as-alice" },
          { tier: "edge" },
        ),
      ).rejects.toThrow(),
    ]);
  }, 30_000);

  it("concurrent deleteDurable respects per-user ownership — cross-user delete is rejected", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const testCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const { context } = testCtx;

    onTestFinished(async () => {
      await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(testCtx.runtimeData);
      await server.stop();
    });

    const [alice, bob] = await Promise.all([
      mintLocalFirstIdentity(ALICE_SECRET, appId),
      mintLocalFirstIdentity(BOB_SECRET, appId),
    ]);
    const [aliceDb, bobDb] = await Promise.all([
      context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
      context.forRequest({ headers: { authorization: `Bearer ${bob.token}` } }),
    ]);

    const [aliceRow, bobRow] = await Promise.all([
      withTimeout(
        aliceDb.insertDurable(
          todoApp.todos,
          { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
          { tier: "edge" },
        ),
        5_000,
        "alice insert timed out",
      ),
      withTimeout(
        bobDb.insertDurable(
          todoApp.todos,
          { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
          { tier: "edge" },
        ),
        5_000,
        "bob insert timed out",
      ),
    ]);

    // Cross-user delete must be rejected while rows still exist.
    await Promise.all([
      expect(aliceDb.deleteDurable(todoApp.todos, bobRow.id, { tier: "edge" })).rejects.toThrow(),
      expect(bobDb.deleteDurable(todoApp.todos, aliceRow.id, { tier: "edge" })).rejects.toThrow(),
    ]);

    // Each user can delete their own row concurrently.
    await Promise.all([
      withTimeout(
        aliceDb.deleteDurable(todoApp.todos, aliceRow.id, { tier: "edge" }),
        5_000,
        "alice delete timed out",
      ),
      withTimeout(
        bobDb.deleteDurable(todoApp.todos, bobRow.id, { tier: "edge" }),
        5_000,
        "bob delete timed out",
      ),
    ]);

    await vi.waitFor(
      async () => {
        const [aliceRows, bobRows] = await Promise.all([
          withTimeout(
            aliceDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "alice read timed out",
          ),
          withTimeout(
            bobDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "bob read timed out",
          ),
        ]);
        expect(aliceRows).toEqual([]);
        expect(bobRows).toEqual([]);
      },
      { timeout: 10_000 },
    );
  }, 30_000);

  it("two concurrent forRequest sessions for the same user both see only that user's rows", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const testCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const { context } = testCtx;

    onTestFinished(async () => {
      await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(testCtx.runtimeData);
      await server.stop();
    });

    const alice = await mintLocalFirstIdentity(ALICE_SECRET, appId);
    const alice2 = await mintLocalFirstIdentity(ALICE_SECRET, appId);
    const bob = await mintLocalFirstIdentity(BOB_SECRET, appId);

    // Two independent forRequest handles for alice (simulating two concurrent HTTP requests).
    const [aliceDb1, aliceDb2, bobDb] = await Promise.all([
      context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
      context.forRequest({ headers: { authorization: `Bearer ${alice2.token}` } }),
      context.forRequest({ headers: { authorization: `Bearer ${bob.token}` } }),
    ]);

    await withTimeout(
      aliceDb1.insertDurable(
        todoApp.todos,
        { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      5_000,
      "alice insert timed out",
    );

    // Both alice sessions surface the row; neither should see bob's (not yet inserted).
    await vi.waitFor(
      async () => {
        const [rows1, rows2] = await Promise.all([
          withTimeout(
            aliceDb1.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "aliceDb1 read timed out",
          ),
          withTimeout(
            aliceDb2.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
            5_000,
            "aliceDb2 read timed out",
          ),
        ]);
        expect(rows1.map((r) => r.title)).toEqual(["alice-todo"]);
        expect(rows2.map((r) => r.title)).toEqual(["alice-todo"]);
      },
      { timeout: 10_000 },
    );

    await withTimeout(
      bobDb.insertDurable(
        todoApp.todos,
        { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
        { tier: "edge" },
      ),
      5_000,
      "bob insert timed out",
    );

    // After bob's insert lands, neither alice session should see bob's row.
    await vi.waitFor(
      async () => {
        const bobRows = await withTimeout(
          bobDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          5_000,
          "bob read timed out",
        );
        expect(bobRows).toHaveLength(1);
      },
      { timeout: 10_000 },
    );

    const [rows1, rows2] = await Promise.all([
      withTimeout(
        aliceDb1.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
        5_000,
        "aliceDb1 read timed out",
      ),
      withTimeout(
        aliceDb2.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
        5_000,
        "aliceDb2 read timed out",
      ),
    ]);
    expect(rows1.map((r) => r.title)).toEqual(["alice-todo"]);
    expect(rows2.map((r) => r.title)).toEqual(["alice-todo"]);
  }, 30_000);

  it("forRequest user with no rows gets empty results, not another user's rows", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const testCtx = await createTestContext(server, appId, backendSecret, adminSecret);
    const { context } = testCtx;

    onTestFinished(async () => {
      await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(testCtx.runtimeData);
      await server.stop();
    });

    const alice = await mintLocalFirstIdentity(ALICE_SECRET, appId);
    const carol = await mintLocalFirstIdentity(CAROL_SECRET, appId);
    const [aliceDb, carolDb] = await Promise.all([
      context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
      context.forRequest({ headers: { authorization: `Bearer ${carol.token}` } }),
    ]);

    await withTimeout(
      aliceDb.insertDurable(
        todoApp.todos,
        { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      5_000,
      "alice insert timed out",
    );

    // Wait for alice's row to be visible to alice, then verify carol sees nothing.
    await vi.waitFor(
      async () => {
        const rows = await withTimeout(
          aliceDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          5_000,
          "alice read timed out",
        );
        expect(rows).toHaveLength(1);
      },
      { timeout: 10_000 },
    );

    const carolRows = await withTimeout(
      carolDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
      5_000,
      "carol read timed out",
    );
    expect(carolRows).toEqual([]);
  }, 30_000);
});
