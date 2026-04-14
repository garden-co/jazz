import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import { schema as s } from "jazz-tools";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "./schema-fetch.js";
import { startLocalJazzServer } from "../testing/local-jazz-server.js";

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

type LocalFirstIdentity = {
  token: string;
  userId: string;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Mints a local-first bearer token for a named test actor and resolves the
 * canonical user ID derived from it. The seed is derived deterministically
 * from `actorName` so repeated calls with the same name and appId return the
 * same identity.
 */
async function createLocalFirstIdentity(
  actorName: string,
  appId: string,
): Promise<LocalFirstIdentity> {
  const { mintLocalFirstToken, verifyLocalFirstIdentityProof } = await import("jazz-napi");
  // Derive a deterministic 32-byte seed: pad / truncate the name to 32 bytes.
  const seed = Buffer.from(actorName.padEnd(32, "-").slice(0, 32)).toString("base64url");
  const token = mintLocalFirstToken(seed, appId, 60);
  const userId = verifyLocalFirstIdentityProof(token, appId).id;
  return { token, userId };
}

/**
 * Publishes the todo app schema + permissions to the server, creates a
 * persistent `JazzContext`, registers `onTestFinished` cleanup, and returns
 * the context.
 */
async function createTestContext(
  server: { url: string },
  appId: string,
  backendSecret: string,
  adminSecret: string,
) {
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

  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-napi-concurrent-request-"));
  const dataPath = join(dataRoot, "runtime.db");
  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId,
    app: todoApp,
    permissions: todoAppPermissions,
    driver: { type: "persistent", dataPath },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    userBranch: "main",
    tier: "worker",
  });

  onTestFinished(async () => {
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(dataRoot, { recursive: true, force: true });
  });

  return context;
}

/**
 * Full concurrent-test environment: server, context, alice+bob identities,
 * and pre-opened Db handles for both users.  Registers all cleanup via
 * `onTestFinished`; callers need no teardown boilerplate.
 */
async function createConcurrentTestEnv() {
  const appId = randomUUID();
  const backendSecret = "napi-concurrent-backend-secret";
  const adminSecret = "napi-concurrent-admin-secret";
  const scopeTag = `concurrent-scope-${randomUUID()}`;

  const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
  const context = await createTestContext(server, appId, backendSecret, adminSecret);

  onTestFinished(async () => {
    await server.stop();
  });

  const [alice, bob] = await Promise.all([
    createLocalFirstIdentity("alice", appId),
    createLocalFirstIdentity("bob", appId),
  ]);

  const [aliceDb, bobDb] = await Promise.all([
    context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
    context.forRequest({ headers: { authorization: `Bearer ${bob.token}` } }),
  ]);

  return { context, appId, alice, bob, aliceDb, bobDb, scopeTag };
}

// ---------------------------------------------------------------------------
// Standalone (non-concurrent) forRequest scenarios
// ---------------------------------------------------------------------------

describe("forRequest auth and policy", () => {
  /**
   * Alice inserts her own row; then tries to insert with a foreign owner_id.
   * Backend Db sees the row regardless of ownership.
   *
   *   alice ──forRequest──► context
   *             │
   *             ├─ insertDurable({ owner_id: alice })  ──► OK
   *             ├─ insertDurable({ owner_id: "other" }) ──► REJECT (policy)
   *             └─ all(...)                            ──► [alice-todo]
   *
   *   backend ──asBackend──► context
   *             └─ all(...)  ──► [alice-todo]  (no filter)
   */
  it("insert respects ownership policy — own-row insert succeeds, foreign owner_id is rejected", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-request-backend-secret";
    const adminSecret = "napi-request-admin-secret";
    const scopeTag = `request-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const context = await createTestContext(server, appId, backendSecret, adminSecret);

    onTestFinished(async () => {
      await server.stop();
    });

    const alice = await createLocalFirstIdentity("alice", appId);
    const aliceDb = await context.forRequest({
      headers: { authorization: `Bearer ${alice.token}` },
    });
    const backendDb = context.asBackend();

    const row = await aliceDb.insertDurable(
      todoApp.todos,
      { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
      { tier: "edge" },
    );

    // forRequest session surfaces its own row.
    await vi.waitFor(
      async () => {
        const rows = await aliceDb.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
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
        const rows = await backendDb.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(rows).toContainEqual(expect.objectContaining({ id: row.id }));
      },
      { timeout: 10_000 },
    );
  }, 30_000);

  /**
   * Context configured with allowLocalFirstAuth=false rejects local-first JWTs
   * before any DB interaction occurs.
   *
   *   alice ──forRequest──► context (allowLocalFirstAuth=false)
   *                                  └─ REJECT (token type not allowed)
   */
  it("rejects local-first token when allowLocalFirstAuth is false", async () => {
    const appId = randomUUID();
    const dataRoot = await mkdtemp(join(tmpdir(), "jazz-napi-no-local-first-"));
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const context = createJazzContext({
      appId,
      app: todoApp,
      permissions: todoAppPermissions,
      driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
      allowLocalFirstAuth: false,
    });

    onTestFinished(async () => {
      await context.shutdown();
      await rm(dataRoot, { recursive: true, force: true });
    });

    const alice = await createLocalFirstIdentity("alice", appId);
    await expect(
      context.forRequest({ headers: { authorization: `Bearer ${alice.token}` } }),
    ).rejects.toThrow(/allowLocalFirstAuth/i);
  });

  /**
   * A garbage bearer token is rejected before any DB interaction occurs.
   *
   *   forRequest({ authorization: "Bearer not-a-valid-jwt" }) ──► REJECT
   */
  it("rejects a malformed bearer token", async () => {
    const appId = randomUUID();
    const dataRoot = await mkdtemp(join(tmpdir(), "jazz-napi-bad-token-"));
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const context = createJazzContext({
      appId,
      app: todoApp,
      permissions: todoAppPermissions,
      driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
    });

    onTestFinished(async () => {
      await context.shutdown();
      await rm(dataRoot, { recursive: true, force: true });
    });

    await expect(
      context.forRequest({ headers: { authorization: "Bearer not-a-valid-jwt" } }),
    ).rejects.toThrow();
  });

  /**
   * Two contexts share the same server. One writes rows for alice, bob, and
   * carol as backend. The other reads back through backend, forSession, and
   * forRequest — verifying that backend sees everything while each user-scoped
   * handle sees only that user's rows.
   *
   *   writerContext ──asBackend──► insert alice-item, bob-item, carol-item
   *
   *   readerContext ──asBackend──────► all() ──► [alice-item, bob-item, carol-item]
   *                ──forSession(alice)──► all() ──► [alice-item]
   *                ──forRequest(alice)──► all() ──► [alice-item]
   *                ──forSession(bob)───► all() ──► [bob-item]
   */
  it("backend sees all rows; forSession and forRequest Db filter to the authenticated user", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-query-backend-secret";
    const adminSecret = "napi-query-admin-secret";
    const scopeTag = `session-scope-${randomUUID()}`;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });

    // Publish schema once via the writer context; the reader shares the same published schema.
    const writerContext = await createTestContext(server, appId, backendSecret, adminSecret);
    const readerDataRoot = await mkdtemp(join(tmpdir(), "jazz-napi-query-reader-"));
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const readerContext = createJazzContext({
      appId,
      app: todoApp,
      permissions: todoAppPermissions,
      driver: { type: "persistent", dataPath: join(readerDataRoot, "runtime.db") },
      serverUrl: server.url,
      backendSecret,
      env: "test",
      userBranch: "main",
    });

    onTestFinished(async () => {
      await readerContext.shutdown();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await rm(readerDataRoot, { recursive: true, force: true });
      await server.stop();
    });

    // Derive user IDs so backend writes use the same owner_id values that local-first sessions expect.
    const [alice, bob, carol] = await Promise.all([
      createLocalFirstIdentity("alice", appId),
      createLocalFirstIdentity("bob", appId),
      createLocalFirstIdentity("carol", appId),
    ]);

    const writerBackend = writerContext.asBackend();
    const readerBackend = readerContext.asBackend();

    // Seed rows for all three users through the backend writer context.
    await Promise.all([
      writerBackend.insertDurable(
        todoApp.todos,
        { title: "alice-item", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      writerBackend.insertDurable(
        todoApp.todos,
        { title: "bob-item", done: false, description: scopeTag, owner_id: bob.userId },
        { tier: "edge" },
      ),
      writerBackend.insertDurable(
        todoApp.todos,
        { title: "carol-item", done: false, description: scopeTag, owner_id: carol.userId },
        { tier: "edge" },
      ),
    ]);

    const aliceSessionDb = readerContext.forSession({ user_id: alice.userId, claims: {} });
    const aliceRequestDb = await readerContext.forRequest({
      headers: { authorization: `Bearer ${alice.token}` },
    });
    const bobSessionDb = readerContext.forSession({ user_id: bob.userId, claims: {} });

    // Backend reader sees all three rows.
    await vi.waitFor(
      async () => {
        const rows = await readerBackend.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(rows.map((r) => r.title).sort()).toEqual(["alice-item", "bob-item", "carol-item"]);
      },
      { timeout: 10_000 },
    );

    // Each user-scoped handle surfaces only that user's rows.
    await vi.waitFor(
      async () => {
        const [aliceSession, aliceRequest, bobSession] = await Promise.all([
          aliceSessionDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          aliceRequestDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          bobSessionDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
        ]);
        expect(aliceSession.map((r) => r.title)).toEqual(["alice-item"]);
        expect(aliceRequest.map((r) => r.title)).toEqual(["alice-item"]);
        expect(bobSession.map((r) => r.title)).toEqual(["bob-item"]);
      },
      { timeout: 10_000 },
    );
  }, 30_000);
});

describe("forRequest concurrent session isolation", () => {
  /**
   * Two forRequest sessions run concurrently on the same context. Each user
   * can insert their own row; each query sees only their own row; cross-user
   * inserts are rejected. A later additional Db handle for alice (simulating a
   * subsequent HTTP request from the same user) also stays isolated.
   *
   *   alice ──forRequest──┐
   *                       ├──► context ──► server
   *   bob   ──forRequest──┘
   *
   *   alice: insertDurable({ owner_id: alice })  ──► OK
   *   bob:   insertDurable({ owner_id: bob })    ──► OK
   *   alice: insertDurable({ owner_id: bob })    ──► REJECT
   *   bob:   insertDurable({ owner_id: alice })  ──► REJECT
   *   alice: all()  ──► [alice-todo]
   *   bob:   all()  ──► [bob-todo]
   *   aliceAgain (new Db handle, same user): all() ──► [alice-todo]
   */
  it("isolates concurrent forRequest sessions on the same context — alice and bob see only their own rows", async () => {
    const { context, alice, bob, aliceDb, bobDb, scopeTag } = await createConcurrentTestEnv();

    // Fire writes for both users in parallel.
    await Promise.all([
      aliceDb.insertDurable(
        todoApp.todos,
        { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      bobDb.insertDurable(
        todoApp.todos,
        { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
        { tier: "edge" },
      ),
    ]);

    // Alice's scoped Db should only surface her own row.
    await vi.waitFor(
      async () => {
        const rows = await aliceDb.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(rows.map((r) => r.title)).toEqual(["alice-todo"]);
      },
      { timeout: 10_000 },
    );

    // Bob's scoped Db should only surface his own row.
    await vi.waitFor(
      async () => {
        const rows = await bobDb.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(rows.map((r) => r.title)).toEqual(["bob-todo"]);
      },
      { timeout: 10_000 },
    );

    // Cross-user write rejection: alice and bob must not be able to insert
    // rows owned by each other, even when their requests are in flight concurrently.
    await Promise.all([
      expect(
        aliceDb.insertDurable(
          todoApp.todos,
          { title: "alice-as-bob", done: false, description: scopeTag, owner_id: bob.userId },
          { tier: "edge" },
        ),
      ).rejects.toThrow(),
      expect(
        bobDb.insertDurable(
          todoApp.todos,
          { title: "bob-as-alice", done: false, description: scopeTag, owner_id: alice.userId },
          { tier: "edge" },
        ),
      ).rejects.toThrow(),
    ]);

    // A new Db handle for alice (same identity, new forRequest call — simulating
    // a subsequent HTTP request from the same user) must stay isolated from bob's data.
    const aliceAgain = await context.forRequest({
      headers: { authorization: `Bearer ${alice.token}` },
    });
    await vi.waitFor(
      async () => {
        const rows = await aliceAgain.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(rows.map((r) => r.title)).toEqual(["alice-todo"]);
      },
      { timeout: 10_000 },
    );
  }, 30_000);

  /**
   * Each user inserts their own row, then both update their own row
   * concurrently. Cross-user updates are rejected.
   *
   *   alice: insert alice-todo  ──► aliceRow
   *   bob:   insert bob-todo    ──► bobRow
   *
   *   alice: updateDurable(aliceRow) ──► OK    alice: all() ──► [alice-updated]
   *   bob:   updateDurable(bobRow)   ──► OK    bob:   all() ──► [bob-updated]
   *
   *   alice: updateDurable(bobRow)   ──► REJECT
   *   bob:   updateDurable(aliceRow) ──► REJECT
   */
  it("concurrent updateDurable respects per-user ownership — cross-user update is rejected", async () => {
    const { alice, bob, aliceDb, bobDb, scopeTag } = await createConcurrentTestEnv();

    const [aliceRow, bobRow] = await Promise.all([
      aliceDb.insertDurable(
        todoApp.todos,
        { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      bobDb.insertDurable(
        todoApp.todos,
        { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
        { tier: "edge" },
      ),
    ]);

    // Each user can update their own row concurrently.
    await Promise.all([
      aliceDb.updateDurable(
        todoApp.todos,
        aliceRow.id,
        { title: "alice-updated" },
        { tier: "edge" },
      ),
      bobDb.updateDurable(todoApp.todos, bobRow.id, { title: "bob-updated" }, { tier: "edge" }),
    ]);

    await vi.waitFor(
      async () => {
        const [aliceRows, bobRows] = await Promise.all([
          aliceDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          bobDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
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

  /**
   * Each user inserts their own row. Cross-user deletes are rejected while
   * both rows still exist; then each user deletes their own row concurrently,
   * leaving both lists empty.
   *
   *   alice: insert alice-todo  ──► aliceRow
   *   bob:   insert bob-todo    ──► bobRow
   *
   *   alice: deleteDurable(bobRow)   ──► REJECT
   *   bob:   deleteDurable(aliceRow) ──► REJECT
   *
   *   alice: deleteDurable(aliceRow) ──► OK    alice: all() ──► []
   *   bob:   deleteDurable(bobRow)   ──► OK    bob:   all() ──► []
   */
  it("concurrent deleteDurable respects per-user ownership — cross-user delete is rejected", async () => {
    const { alice, bob, aliceDb, bobDb, scopeTag } = await createConcurrentTestEnv();

    const [aliceRow, bobRow] = await Promise.all([
      aliceDb.insertDurable(
        todoApp.todos,
        { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
        { tier: "edge" },
      ),
      bobDb.insertDurable(
        todoApp.todos,
        { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
        { tier: "edge" },
      ),
    ]);

    // Cross-user delete must be rejected while rows still exist.
    await Promise.all([
      expect(aliceDb.deleteDurable(todoApp.todos, bobRow.id, { tier: "edge" })).rejects.toThrow(),
      expect(bobDb.deleteDurable(todoApp.todos, aliceRow.id, { tier: "edge" })).rejects.toThrow(),
    ]);

    // Each user can delete their own row concurrently.
    await Promise.all([
      aliceDb.deleteDurable(todoApp.todos, aliceRow.id, { tier: "edge" }),
      bobDb.deleteDurable(todoApp.todos, bobRow.id, { tier: "edge" }),
    ]);

    await vi.waitFor(
      async () => {
        const [aliceRows, bobRows] = await Promise.all([
          aliceDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          bobDb.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
        ]);
        expect(aliceRows).toEqual([]);
        expect(bobRows).toEqual([]);
      },
      { timeout: 10_000 },
    );
  }, 30_000);

  /**
   * Two independent forRequest Db handles for the same user (alice) are
   * opened concurrently alongside a handle for bob. Both alice handles see
   * alice's row; bob's row, inserted after, is invisible to both.
   *
   *   alice ──forRequest──► aliceDb1 ─┐
   *   alice ──forRequest──► aliceDb2 ─┼──► context
   *   bob   ──forRequest──► bobDb    ─┘
   *
   *   aliceDb1: insert alice-todo
   *   aliceDb1: all() ──► [alice-todo]
   *   aliceDb2: all() ──► [alice-todo]
   *
   *   bobDb: insert bob-todo
   *   bobDb: all() ──► [bob-todo]  (confirms bob's write landed)
   *   aliceDb1: all() ──► [alice-todo]  (bob's row invisible)
   *   aliceDb2: all() ──► [alice-todo]  (bob's row invisible)
   */
  it("two concurrent forRequest sessions for the same user both see only that user's rows", async () => {
    const {
      context,
      alice,
      bob,
      aliceDb: aliceDb1,
      bobDb,
      scopeTag,
    } = await createConcurrentTestEnv();

    // Second Db handle for alice — same identity, independent forRequest call.
    const aliceDb2 = await context.forRequest({
      headers: { authorization: `Bearer ${alice.token}` },
    });

    await aliceDb1.insertDurable(
      todoApp.todos,
      { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
      { tier: "edge" },
    );

    // Both alice handles surface the row; neither should see bob's (not yet inserted).
    await vi.waitFor(
      async () => {
        const [rows1, rows2] = await Promise.all([
          aliceDb1.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
          aliceDb2.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
        ]);
        expect(rows1.map((r) => r.title)).toEqual(["alice-todo"]);
        expect(rows2.map((r) => r.title)).toEqual(["alice-todo"]);
      },
      { timeout: 10_000 },
    );

    await bobDb.insertDurable(
      todoApp.todos,
      { title: "bob-todo", done: false, description: scopeTag, owner_id: bob.userId },
      { tier: "edge" },
    );

    // After bob's insert lands, neither alice handle should see bob's row.
    await vi.waitFor(
      async () => {
        const bobRows = await bobDb.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(bobRows).toHaveLength(1);
      },
      { timeout: 10_000 },
    );

    const [rows1, rows2] = await Promise.all([
      aliceDb1.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
      aliceDb2.all(todoApp.todos.where({ description: scopeTag }), { tier: "edge" }),
    ]);
    expect(rows1.map((r) => r.title)).toEqual(["alice-todo"]);
    expect(rows2.map((r) => r.title)).toEqual(["alice-todo"]);
  }, 30_000);

  /**
   * Carol has no rows. Alice inserts one. After alice's row is confirmed
   * visible to alice, carol's query still returns empty — not alice's data.
   *
   *   alice ──forRequest──► aliceDb ──► insert alice-todo ──► all() ──► [alice-todo]
   *   carol ──forRequest──► carolDb ──► all() ──► []
   */
  it("forRequest user with no rows gets empty results, not another user's rows", async () => {
    const { context, alice, aliceDb, scopeTag, appId } = await createConcurrentTestEnv();

    const carol = await createLocalFirstIdentity("carol", appId);
    const carolDb = await context.forRequest({
      headers: { authorization: `Bearer ${carol.token}` },
    });

    await aliceDb.insertDurable(
      todoApp.todos,
      { title: "alice-todo", done: false, description: scopeTag, owner_id: alice.userId },
      { tier: "edge" },
    );

    // Wait for alice's row to be visible to alice, then verify carol sees nothing.
    await vi.waitFor(
      async () => {
        const rows = await aliceDb.all(todoApp.todos.where({ description: scopeTag }), {
          tier: "edge",
        });
        expect(rows).toHaveLength(1);
      },
      { timeout: 10_000 },
    );

    const carolRows = await carolDb.all(todoApp.todos.where({ description: scopeTag }), {
      tier: "edge",
    });
    expect(carolRows).toEqual([]);
  }, 30_000);
});
