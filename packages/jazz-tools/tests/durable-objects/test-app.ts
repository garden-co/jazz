import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { afterEach, beforeAll, vi } from "vitest";
import {
  createJazz,
  createJazzClient,
  remote,
  type CreateJazzOptions,
} from "../../src/backend/index.js";
import { testApp, testPermissions } from "../../src/testing/index.js";
import { networkTransport } from "../../src/backend/core-transport.js";
import { bundleWorker } from "./bundle.js";
import { startDurableObjectCore } from "./harness.js";

export const tables = {
  todos: { columns: { title: "string", owner: "uuid" } },
  memberships: { columns: { account: "uuid" } },
} as const;
export const app = testApp({ tables });
export const permissionsFile = {
  format: 1,
  tables: { todos: { read: "owner = session.user", insert: "owner = session.user" } },
};
const permissions = testPermissions(app, permissionsFile);

async function listening(server: Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

export function inAnHour(): number {
  return Math.floor(Date.now() / 1000) + 3600;
}

export function setupTestApp(lifecycle: "hibernate" | "while-connected") {
  let script: string;
  beforeAll(async () => {
    const entry = lifecycle === "hibernate" ? "observed-core-worker.mjs" : "real-core-worker.ts";
    script = await bundleWorker(join(__dirname, entry));
  });

  const cleanups: Array<() => Promise<unknown>> = [];
  afterEach(async () => {
    while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
    vi.restoreAllMocks();
  });

  /** The app's Jazz on a fresh Durable Object core, served on its own origin. */
  async function appOn(authenticate: CreateJazzOptions["authenticate"]) {
    const core = await startDurableObjectCore({
      script,
    });
    cleanups.push(() => core.dispose());
    const transport = networkTransport();
    const jazz = await createJazz({
      app,
      permissions: permissionsFile,
      store: remote({
        appId: await core.appId(),
        url: core.url,
        secret: core.secret,
        ...(lifecycle === "hibernate" && {
          transport: {
            fetch: transport.fetch,
            openWebSocket: (url, headers) =>
              transport.openWebSocket(url, { ...headers, "x-jazz-socket-lifecycle": lifecycle }),
          },
        }),
      }),
      authenticate,
    }).ready();
    const server = createServer((req, res) =>
      jazz.nodeHandler(req, res, () => {
        res.statusCode = 404;
        res.end();
      }),
    );
    await jazz.attach(server);
    const port = await listening(server);
    cleanups.push(async () => {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      await jazz.close();
    });
    const client = async (initial?: "device") => {
      const session = await createJazzClient({
        appId: jazz.appId,
        app,
        permissions,
        serverUrl: `http://127.0.0.1:${port}${jazz.basePath}`,
        driver: { type: "memory" },
        ...(initial ? { initial } : {}),
      });
      cleanups.push(() => session.close());
      if (!initial) await session.refresh();
      const { client, status, error } = session.getSnapshot();
      if (status !== "ready") throw error ?? new Error(status);
      return client!;
    };
    return { jazz, client, core, server, origin: `http://127.0.0.1:${port}` };
  }

  /** The test app's own auth: requests to the app's origin carry `x-test-user`. */
  function signInAs(origin: string, userId: string) {
    const realFetch = globalThis.fetch;
    const RealWebSocket = globalThis.WebSocket;
    globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      if (new URL(request.url).origin === origin) request.headers.set("x-test-user", userId);
      return realFetch(request);
    }) as typeof fetch;
    globalThis.WebSocket = class extends RealWebSocket {
      constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
        // The backend's own sockets to its core keep their headers.
        const headers = new URL(url).host === new URL(origin).host ? { "x-test-user": userId } : {};
        super(url, { ...options, headers: { ...options?.headers, ...headers } } as never);
      }
    } as typeof WebSocket;
    cleanups.push(async () => {
      globalThis.fetch = realFetch;
      globalThis.WebSocket = RealWebSocket;
    });
  }

  return { appOn, signInAs, cleanups };
}
