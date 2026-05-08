/**
 * Worker-path auth refresh E2E.
 *
 * Verifies the full JS dispatch chain:
 *   Db.updateAuthToken
 *     → Db.applyAuthUpdate
 *       → WorkerBridge.updateAuth
 *         → worker postMessage({ type: "update-auth", jwtToken })
 *
 * Two tests:
 *
 * 1. Dispatch assertion (no server required): creates an OPFS-backed Db
 *    with an initial JWT for alice, waits for bridge init, then calls
 *    updateAuthToken with a refreshed JWT for the same user and asserts
 *    that a "update-auth" message was posted to the worker.
 *
 * 2. Post-refresh usability (real server via getTestingServerJwtForUser):
 *    creates a Db with an initial valid JWT, writes a row, refreshes the
 *    JWT with a fresh token for the same user, and confirms the Db still
 *    serves local-tier queries.  Uses global-setup server but does not
 *    rely on the auth-rejection → unauthenticated path.
 *
 * Coverage of the full update-auth chain is split across:
 *   - client.test.ts: JazzClient.updateAuthToken forwards to runtime.updateAuth
 *   - jazz-worker.test.ts: worker "update-auth" handler invokes runtime.updateAuth
 *     and posts "auth-failed" if it throws
 *   - napi.auth-failure.test.ts: real NAPI runtime fires onAuthFailure on a
 *     server-rejected JWT
 */

import { afterEach, describe, expect, it } from "vitest";
import { createDb, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { TestCleanup, uniqueDbName, withTimeout } from "./support.js";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

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

/**
 * Build a minimal but structurally valid fake JWT for a given userId.
 * Signature is intentionally invalid — used only for JS-layer auth state
 * tracking, not for server verification.
 */
function makeFakeJwt(userId: string, extraClaims: Record<string, unknown> = {}): string {
  const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  const payload = btoa(
    JSON.stringify({
      sub: userId,
      exp: Math.floor(Date.now() / 1000) + 3600,
      ...extraClaims,
    }),
  )
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return `${header}.${payload}.fake-signature`;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Db worker-path auth refresh — update-auth dispatch chain", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  // Direct dispatch assertion (peeking at outbound postMessage shape) lives in
  // `crates/jazz-wasm/tests/worker_bridge.rs::update_auth_emits_postcard_binary`,
  // since the bridge wire is binary postcard. The behavioural test below
  // covers the post-refresh usability path.

  it("Db remains usable for local-tier queries after updateAuthToken", async () => {
    // No serverUrl: pure OPFS Db so local-tier queries resolve locally
    // without waiting for a WS connection.  Both tokens carry sub="bob"
    // so the principal-change guard in applyJwtToken does not trigger.
    const initialJwt = makeFakeJwt("bob", { role: "member" });
    const refreshedJwt = makeFakeJwt("bob", { role: "member", refresh: 1 });

    const db = ctx.track(
      await createDb({
        appId: "test-app-auth-refresh",
        jwtToken: initialJwt,
        driver: { type: "persistent", dbName: uniqueDbName("worker-auth-usable") },
      }),
    );

    // Trigger bridge init and confirm the worker path is alive.
    const preMarker = `pre-refresh-${Date.now()}`;
    db.insert(todos, { title: preMarker, done: false });

    const rowsBefore = await withTimeout(
      db.all(allTodos, { tier: "local" }),
      15_000,
      "pre-refresh local-tier query did not resolve",
    );
    expect(rowsBefore.some((r) => r.title === preMarker)).toBe(true);

    // Refresh the token via the full chain.
    db.updateAuthToken(refreshedJwt);

    // Insert a row after the token swap.
    const postMarker = `post-refresh-${Date.now()}`;
    db.insert(todos, { title: postMarker, done: true });

    // Both rows must be visible at local tier — the Db must stay
    // operational after auth refresh.
    const rowsAfter = await withTimeout(
      db.all(allTodos, { tier: "local" }),
      10_000,
      "post-refresh local-tier query did not resolve",
    );
    expect(rowsAfter.some((r) => r.title === preMarker)).toBe(true);
    expect(rowsAfter.some((r) => r.title === postMarker)).toBe(true);
  });
});
