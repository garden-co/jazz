import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { schema as s } from "jazz-tools";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
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

const inlineSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    owner_id: s.string(),
  }),
};

const inlineApp = s.defineApp(inlineSchema);

const inlinePermissions = s.definePermissions(inlineApp, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user_id });
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate.where({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type PolicyTodo = {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  owner_id: string;
};

type PolicyTodoInit = {
  title: string;
  done: boolean;
  description?: string;
  owner_id: string;
};

type TempRuntimeData = {
  dataRoot: string;
  dataPath: string;
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Deterministic base64url-encoded 32-byte seeds — different values produce
// different local-first identities, which map to different owner_id values.
const ALICE_SECRET = "YWxpY2Utc2VjcmV0LWZvci10ZXN0LXB1cnBvc2VzISE";
const BOB_SECRET = "Ym9iLS0tc2VjcmV0LWZvci10ZXN0LXB1cnBvc2VzISE";

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

function makePolicyTodosTable(): TableProxy<PolicyTodo, PolicyTodoInit> {
  return {
    _table: "todos",
    _schema: inlineApp.wasmSchema,
    _rowType: undefined as unknown as PolicyTodo,
    _initType: undefined as unknown as PolicyTodoInit,
  };
}

function makePolicyTodosByDescriptionQuery(description: string): QueryBuilder<PolicyTodo> {
  return {
    _table: "todos",
    _schema: inlineApp.wasmSchema,
    _rowType: undefined as unknown as PolicyTodo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "description", op: "eq", value: description }],
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await loadNapiModule();
});

describe("forRequest concurrent session isolation", () => {
  it("isolates concurrent forRequest sessions on the same context — alice and bob see only their own rows", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;
    let runtimeData: TempRuntimeData | null = null;

    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });

    let context: {
      asBackend(): Db;
      forRequest(request: { headers: Record<string, string> }): Promise<Db>;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      const { mintLocalFirstToken, verifyLocalFirstIdentityProof } = await loadNapiModule();

      const { hash: schemaHash } = await publishStoredSchema(server.url, {
        adminSecret,
        schema: inlineApp.wasmSchema,
      });
      const { head } = await fetchPermissionsHead(server.url, { adminSecret });
      await publishStoredPermissions(server.url, {
        adminSecret,
        schemaHash,
        permissions: inlinePermissions,
        expectedParentBundleObjectId: head?.bundleObjectId ?? null,
      });

      runtimeData = await createTempRuntimeData("jazz-napi-concurrent-request-");
      context = createJazzContext({
        appId,
        app: inlineApp,
        permissions: inlinePermissions,
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });

      // Mint local-first tokens for alice and bob. No JWKS server needed —
      // forRequest verifies these directly via the NAPI module.
      const aliceToken = mintLocalFirstToken(ALICE_SECRET, appId, 60);
      const bobToken = mintLocalFirstToken(BOB_SECRET, appId, 60);

      // Resolve the canonical user IDs derived from each secret so we can
      // use them as owner_id when inserting rows.
      const aliceId = verifyLocalFirstIdentityProof(aliceToken, appId).id;
      const bobId = verifyLocalFirstIdentityProof(bobToken, appId).id;

      const policyTodosTable = makePolicyTodosTable();
      const scopedQuery = makePolicyTodosByDescriptionQuery(scopeTag);

      // Obtain session-scoped Db handles for alice and bob concurrently from
      // the same shared context — this is the pattern a real server would use.
      const [aliceDb, bobDb] = await Promise.all([
        context.forRequest({
          headers: { authorization: `Bearer ${aliceToken}` },
        }),
        context.forRequest({
          headers: { authorization: `Bearer ${bobToken}` },
        }),
      ]);

      // Fire writes for both users in parallel.
      await Promise.all([
        withTimeout(
          aliceDb.insertDurable(
            policyTodosTable,
            { title: "alice-todo", done: false, description: scopeTag, owner_id: aliceId },
            { tier: "edge" },
          ),
          10_000,
          "alice insert timed out",
        ),
        withTimeout(
          bobDb.insertDurable(
            policyTodosTable,
            { title: "bob-todo", done: false, description: scopeTag, owner_id: bobId },
            { tier: "edge" },
          ),
          10_000,
          "bob insert timed out",
        ),
      ]);

      // Alice's scoped Db should only surface her own row.
      await vi.waitFor(
        async () => {
          const rows = await withTimeout(
            aliceDb.all(scopedQuery, { tier: "edge" }),
            10_000,
            "alice read timed out",
          );
          expect(rows.map((r) => r.title).sort()).toEqual(["alice-todo"]);
        },
        { timeout: 20_000 },
      );

      // Bob's scoped Db should only surface his own row.
      await vi.waitFor(
        async () => {
          const rows = await withTimeout(
            bobDb.all(scopedQuery, { tier: "edge" }),
            10_000,
            "bob read timed out",
          );
          expect(rows.map((r) => r.title).sort()).toEqual(["bob-todo"]);
        },
        { timeout: 20_000 },
      );

      // Cross-user write rejection: alice and bob must not be able to insert
      // rows owned by each other, even when their requests are in flight concurrently.
      await Promise.all([
        expect(
          aliceDb.insertDurable(
            policyTodosTable,
            { title: "alice-as-bob", done: false, description: scopeTag, owner_id: bobId },
            { tier: "edge" },
          ),
        ).rejects.toThrow(),
        expect(
          bobDb.insertDurable(
            policyTodosTable,
            { title: "bob-as-alice", done: false, description: scopeTag, owner_id: aliceId },
            { tier: "edge" },
          ),
        ).rejects.toThrow(),
      ]);

      // A fresh forRequest call for alice (simulating a later HTTP request)
      // must still be isolated from bob's data.
      const aliceDb2 = await context.forRequest({
        headers: { authorization: `Bearer ${mintLocalFirstToken(ALICE_SECRET, appId, 60)}` },
      });
      await vi.waitFor(
        async () => {
          const rows = await withTimeout(
            aliceDb2.all(scopedQuery, { tier: "edge" }),
            10_000,
            "alice2 read timed out",
          );
          expect(rows.map((r) => r.title).sort()).toEqual(["alice-todo"]);
        },
        { timeout: 20_000 },
      );
    } finally {
      if (context) await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
    }
  }, 60_000);
});
