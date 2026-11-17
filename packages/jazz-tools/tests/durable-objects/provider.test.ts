// `durableObjects()` against a Jazz Durable Object Worker in local workerd:
// the deployment's routing and admission, and createJazz reaching the core
// through it. Until the jazz-wasm host lands this runs the mock core
// (`mock-core.mjs`), which admits like the real one but runs no Jazz.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createJazz, durableObjects } from "../../src/backend/index.js";
import { testApp } from "../../src/testing/index.js";
import { startDurableObjectCore, type DurableObjectCore } from "./harness.js";

const NodeWebSocket = globalThis.WebSocket as unknown as new (
  url: string,
  options?: { headers?: Record<string, string> },
) => WebSocket;

let core: DurableObjectCore;
beforeAll(async () => {
  core = await startDurableObjectCore();
});
afterAll(async () => {
  await core?.dispose();
});

const seen = async () =>
  (await (await fetch(`${core.url}/__mock/seen`)).json()) as Array<{
    method: string;
    path: string;
    upgrade: boolean;
    credential: boolean;
    access: string | null;
  }>;

/** Opens a socket; resolves with how it ended or that it opened. */
function openSocket(headers: Record<string, string>): Promise<"open" | "refused"> {
  return new Promise((resolve) => {
    const ws = new NodeWebSocket(`${core.url.replace(/^http/, "ws")}/apps/${core.appId}/ws`, {
      headers,
    });
    ws.addEventListener("open", () => {
      ws.close();
      resolve("open");
    });
    ws.addEventListener("error", () => resolve("refused"));
  });
}

describe("a Jazz Durable Object Worker", () => {
  it("answers /health once its object is up", async () => {
    const response = await fetch(`${core.url}/health`);
    expect(response.status).toBe(200);
  });

  it("admits the backend's socket by the core secret", async () => {
    expect(
      await openSocket({
        "x-jazz-backend-credential": core.secret,
        "x-jazz-backend-access": "backend",
      }),
    ).toBe("open");
  });

  it("refuses a socket without the core secret", async () => {
    expect(await openSocket({})).toBe("refused");
    expect(await openSocket({ "x-jazz-backend-credential": "x".repeat(40) })).toBe("refused");
  });

  it("answers 426 to the socket route without an upgrade", async () => {
    const response = await fetch(`${core.url}/apps/${core.appId}/ws`, {
      headers: { "x-jazz-backend-credential": core.secret },
    });
    expect(response.status).toBe(426);
  });

  it("refuses a request without the core secret with the native core's message", async () => {
    const response = await fetch(`${core.url}/apps/${core.appId}/admin/schemas`, {
      method: "POST",
    });
    expect(response.status).toBe(401);
    expect(await response.json()).toEqual({
      error:
        "this core admits only its app's backend: connect through the app's createJazz handler",
    });
  });
});

describe("createJazz with durableObjects()", () => {
  it("reaches the Worker's core as admin, holding the core secret", async () => {
    const before = (await seen()).length;
    const jazz = createJazz({
      app: testApp({ tables: { todos: { columns: { title: "string" } } } }),
      permissions: { format: 1, tables: { todos: { read: "true", insert: "true" } } },
      store: durableObjects({ url: core.url, appId: core.appId, secret: core.secret }),
    });
    // The mock core runs no Jazz: publishing fails with its 501.
    await expect(jazz.ready()).rejects.toThrow(/501|not implemented/);
    await jazz.close().catch(() => undefined);
    const requests = (await seen()).slice(before);
    // Publishing starts by reading the core's migration graph, as admin; the
    // mock's 501 ends it there.
    expect(requests).toEqual([
      {
        method: "GET",
        path: `/apps/${core.appId}/admin/migrations/graph`,
        upgrade: false,
        credential: true,
        access: "admin",
      },
    ]);
  });
});
