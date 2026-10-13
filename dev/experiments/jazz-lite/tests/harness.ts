import { fork } from "node:child_process";
import { readFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { WebSocketServer } from "ws";
import { createAccountManager } from "jazz-tools";
import { createJazzSession, type JazzClient } from "jazz-tools/backend";
import {
  deploy,
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "jazz-tools/testing";
import { app } from "../app/schema.js";
import permissions from "../app/permissions.js";
import { createLiteDb, type LiteDb } from "../src/client.js";
import { createLiteProxy } from "../src/proxy.js";

const ISSUER = "https://jazz-lite.example.test";

type Upstream = Omit<LocalJazzServerHandle, "dataDir"> & { pid: number };

export interface HarnessOptions {
  /** Run the upstream Jazz server in a child process (for memory measurements). */
  upstreamInChild?: boolean;
}

/** Resident set size of a process in KiB, from /proc. */
export function rssKiB(pid: number | "self" = "self"): number {
  const status = readFileSync(`/proc/${pid}/status`, "utf8");
  return Number(/VmRSS:\s+(\d+)/.exec(status)![1]);
}

async function startUpstream(
  options: Parameters<typeof startLocalJazzServer>[0],
  inChild: boolean,
): Promise<Upstream> {
  if (!inChild) return { ...(await startLocalJazzServer(options)), pid: process.pid };
  const child = fork(new URL("./upstream-child.mjs", import.meta.url), [JSON.stringify(options)]);
  const info = await new Promise<Omit<Upstream, "stop">>((resolve, reject) => {
    child.once("message", (m) => resolve(m as Omit<Upstream, "stop">));
    child.once("exit", (code) => reject(new Error(`upstream exited ${code}`)));
  });
  return {
    ...info,
    stop: () =>
      new Promise<void>((resolve) => {
        child.once("exit", () => resolve());
        child.send("stop");
      }),
  };
}

export interface Harness {
  upstream: Upstream;
  backend: JazzClient;
  proxy: ReturnType<typeof createLiteProxy>;
  proxyUrl: string;
  /** Register a user with the upstream server and return their bearer token. */
  identity(userId: string): Promise<string>;
  lite(token: string): LiteDb;
  stop(): Promise<void>;
}

/** Local Jazz server + JWT issuer + one backend client + the lite proxy on a random port. */
export async function startHarness(options: HarnessOptions = {}): Promise<Harness> {
  const jwtIssuer: TestJwtIssuerHandle = await startTestJwtIssuer();
  const upstream = await startUpstream(
    { jwksUrl: jwtIssuer.jwksUrl, jwtIssuer: ISSUER, jwtAudience: jwtIssuer.audience },
    options.upstreamInChild ?? false,
  );
  await deploy({
    serverUrl: upstream.url,
    appId: upstream.appId,
    adminSecret: upstream.adminSecret,
    schema: app,
    permissions,
  });

  const session = await createJazzSession({
    appId: upstream.appId,
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl: upstream.url,
    initial: { backendSecret: upstream.backendSecret },
    env: "dev",
    jwksUrl: jwtIssuer.jwksUrl,
  });
  const snapshot = session.getSnapshot();
  if (snapshot.status !== "ready" || !snapshot.client)
    throw snapshot.error ?? new Error("not ready");
  const backend = snapshot.client as JazzClient;

  const proxy = createLiteProxy({ backend, app: app as unknown as Record<string, unknown> });
  const httpServer: Server = createServer();
  const wss = new WebSocketServer({ server: httpServer, path: "/lite" });
  wss.on("connection", (socket) => proxy.handle(socket as never));
  await new Promise<void>((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const proxyUrl = `ws://127.0.0.1:${(httpServer.address() as AddressInfo).port}/lite`;
  const clients: LiteDb[] = [];

  return {
    upstream,
    backend,
    proxy,
    proxyUrl,
    async identity(userId) {
      const token = jwtIssuer.jwtForUser(userId, {}, { issuer: ISSUER });
      let stored: string | null = null;
      const accounts = await createAccountManager({
        appId: upstream.appId,
        serverUrl: upstream.url,
        env: `jazz-lite-${crypto.randomUUID()}`,
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      });
      await accounts.registerJWT(token);
      return token;
    },
    lite(token) {
      const db = createLiteDb({ url: proxyUrl, getToken: () => token });
      clients.push(db);
      return db;
    },
    async stop() {
      for (const db of clients) db.close();
      wss.close();
      await new Promise((resolve) => httpServer.close(resolve));
      await session.close();
      await upstream.stop();
      await jwtIssuer.stop();
    },
  };
}
