// `durableObjects()` against a Jazz Durable Object Worker in local workerd:
// the deployment's routing and admission, and createJazz reaching the core
// through it. These run the mock core (`mock-core.mjs`), which admits like
// the jazz-wasm host but runs no Jazz; `real-core.test.ts` runs the host.
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createJazz, durableObjects } from "../../src/backend/index.js";
import { testApp } from "../../src/testing/index.js";
import { startDurableObjectCore, type DurableObjectCore } from "./harness.js";

const NodeWebSocket = globalThis.WebSocket as unknown as new (
  url: string,
  options?: { headers?: Record<string, string> },
) => WebSocket;

const app = testApp({ tables: { todos: { columns: { title: "string" } } } });
const permissions = { format: 1, tables: { todos: { read: "true", insert: "true" } } };

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

async function core(options: { persist?: string } = {}): Promise<DurableObjectCore> {
  const started = await startDurableObjectCore(options);
  cleanups.push(() => started.dispose());
  return started;
}

type Seen = {
  method: string;
  path: string;
  upgrade: boolean;
  credential: boolean;
  access: string | null;
};
const seen = async (url: string) => (await (await fetch(`${url}/__mock/seen`)).json()) as Seen[];

/** createJazz on `store` until its open fails (the mock core runs no Jazz): the error. */
async function openFailure(store: ReturnType<typeof durableObjects>): Promise<Error> {
  const jazz = createJazz({ app, permissions, store });
  try {
    await jazz.ready();
    throw new Error("opened");
  } catch (error) {
    return error as Error;
  } finally {
    await jazz.close().catch(() => undefined);
  }
}

/** Opens a socket to the core; resolves with whether it opened. */
function openSocket(url: string, headers: Record<string, string>): Promise<"open" | "refused"> {
  return new Promise((resolve) => {
    const ws = new NodeWebSocket(`${url.replace(/^http/, "ws")}/apps/any/ws`, { headers });
    ws.addEventListener("open", () => {
      ws.close();
      resolve("open");
    });
    ws.addEventListener("error", () => resolve("refused"));
  });
}

async function listening(server: Server): Promise<string> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise((resolve) => server.close(resolve)));
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

describe("a Jazz Durable Object Worker", () => {
  it("answers /health, and tells only its backend the app id it keeps", async () => {
    const { url, appId } = await core();
    expect((await fetch(`${url}/health`)).status).toBe(200);
    expect((await fetch(`${url}/app`)).status).toBe(401);
    expect(await appId()).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("admits a socket carrying the core secret and refuses one without", async () => {
    const { url, secret } = await core();
    const backend = { "x-jazz-backend-credential": secret, "x-jazz-backend-access": "backend" };
    expect(await openSocket(url, backend)).toBe("open");
    expect(await openSocket(url, {})).toBe("refused");
    expect(await openSocket(url, { "x-jazz-backend-credential": "x".repeat(40) })).toBe("refused");
  });

  it("answers 426 to the socket route without an upgrade", async () => {
    const { url, secret } = await core();
    const response = await fetch(`${url}/apps/any/ws`, {
      headers: { "x-jazz-backend-credential": secret },
    });
    expect(response.status).toBe(426);
  });

  it("refuses a request without the core secret with the native core's message", async () => {
    const { url } = await core();
    const response = await fetch(`${url}/apps/any/admin/bundles`, { method: "POST" });
    expect(response.status).toBe(401);
    expect(await response.json()).toEqual({
      error:
        "this core admits only its app's backend: connect through the app's createJazz handler",
    });
  });
});

describe("createJazz with durableObjects()", () => {
  it("asks the core for its app id, then reaches its admin routes as admin with the secret", async () => {
    const { url, secret, appId } = await core();
    const id = await appId();
    const error = await openFailure(durableObjects({ url, secret }));
    // The mock core runs no Jazz: publishing ends at its 501.
    expect(error.message).toMatch(/501|not implemented/);
    const asked = (path: string, access: string | null = null) => ({
      method: "GET",
      path,
      upgrade: false,
      credential: true,
      access,
    });
    expect(await seen(url)).toEqual([
      asked("/app"), // the test's own
      asked("/app"),
      asked(`/apps/${id}/admin/migrations/graph`, "admin"),
    ]);
  });

  it("keeps the object's app id across a restart", async () => {
    const persist = await mkdtemp(join(tmpdir(), "jazz-do-provider-"));
    cleanups.push(() => rm(persist, { recursive: true, force: true }));
    const first = await core({ persist });
    const id = await first.appId();
    await first.dispose();
    const again = await core({ persist });
    expect(await again.appId()).toBe(id);
    await openFailure(durableObjects({ url: again.url, secret: again.secret }));
    expect((await seen(again.url)).map((r) => r.path)).toContain(
      `/apps/${id}/admin/migrations/graph`,
    );
  });

  it("names a wrong secret", async () => {
    const { url } = await core();
    const error = await openFailure(durableObjects({ url, secret: "w".repeat(40) }));
    expect(error.message).toMatch(
      /refused the secret: durableObjects\(\)'s secret must be the Worker's JAZZ_CORE_SECRET/,
    );
  });

  it("follows no redirect, so the secret goes nowhere else", async () => {
    const reached: IncomingHttpHeaders[] = [];
    const elsewhere = await listening(
      createServer((request, response) => {
        reached.push(request.headers);
        response.end(JSON.stringify({ appId: "00000000-0000-4000-8000-000000000000" }));
      }),
    );
    const redirecting = await listening(
      createServer((request, response) => {
        response.writeHead(307, { location: `${elsewhere}${request.url}` });
        response.end();
      }),
    );
    const error = await openFailure(durableObjects({ url: redirecting, secret: "s".repeat(40) }));
    expect(error.message).toMatch(
      /did not answer as a Jazz Durable Object core \(GET \/app: 307\)/,
    );
    expect(reached).toEqual([]);
  });
});
