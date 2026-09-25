import { connect, createServer, type Socket } from "node:net";
import type { BrowserContext, Route, WebSocketRoute } from "playwright";
import {
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "../../src/testing/index.js";

interface StartedJazzServer {
  server: LocalJazzServerHandle;
  jwtIssuer: TestJwtIssuerHandle;
  appId: string;
  serverUrl: string;
  adminSecret: string;
  transportGate?: TransportGate;
}

const DEFAULT_JAZZ_SERVER_KEY = "__default__";
const jazzServerPromises = new Map<string, Promise<StartedJazzServer>>();
const transportGates = new Map<string, TransportGate>();

interface TransportGate {
  url: string;
  block(): void;
  unblock(): void;
  close(): Promise<void>;
}

// Keep one advertised authority while interrupting both existing and future
// connections, including worker connections that browser routing cannot reach.
async function startTransportGate(targetUrl: string): Promise<TransportGate> {
  const target = new URL(targetUrl);
  const sockets = new Set<Socket>();
  let blocked = false;
  const server = createServer((socket) => {
    if (blocked) {
      socket.destroy();
      return;
    }
    const upstream = connect({ host: target.hostname, port: Number(target.port) });
    const destroyPair = () => {
      socket.destroy();
      upstream.destroy();
    };
    for (const stream of [socket, upstream]) {
      sockets.add(stream);
      stream.on("error", destroyPair);
      stream.on("close", () => {
        sockets.delete(stream);
        destroyPair();
      });
    }
    socket.pipe(upstream).pipe(socket);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Missing transport gate address");
  }
  const block = () => {
    blocked = true;
    for (const socket of sockets) socket.destroy();
  };
  return {
    url: `http://127.0.0.1:${address.port}`,
    block,
    unblock() {
      blocked = false;
    },
    async close() {
      block();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

interface JazzServerRouteBlock {
  blocked: boolean;
  httpHandler: (route: Route) => void;
  webSocketHandler: (route: WebSocketRoute) => void | Promise<void>;
  webSocketPattern: string;
  webSocketRouted: boolean;
}

const blockedServerRoutes = new WeakMap<BrowserContext, Map<string, JazzServerRouteBlock>>();
const browserContextIds = new WeakMap<BrowserContext, number>();
let nextBrowserContextId = 1;

async function startJazzServer(
  appId?: string,
  schema?: ArrayLike<number>,
  gated = false,
): Promise<StartedJazzServer> {
  const jwtIssuer = await startTestJwtIssuer();
  const adminSecret = "jazz-browser-test-admin";
  const backendSecret = "jazz-browser-test-backend";
  let server: LocalJazzServerHandle | undefined;
  try {
    server = await startLocalJazzServer({
      appId: appId ?? "00000000-0000-0000-0000-000000000001",
      jwksUrl: jwtIssuer.jwksUrl,
      jwtIssuer: jwtIssuer.issuer,
      jwtAudience: jwtIssuer.audience,
      inMemory: true,
      adminSecret,
      backendSecret,
      schema: schema ? Uint8Array.from(schema) : undefined,
    });
    const transportGate = gated ? await startTransportGate(server.url) : undefined;
    const serverUrl = transportGate?.url ?? server.url;
    if (transportGate) transportGates.set(serverUrl, transportGate);
    return {
      server,
      jwtIssuer,
      appId: server.appId,
      serverUrl,
      adminSecret: server.adminSecret,
      transportGate,
    };
  } catch (error) {
    await Promise.allSettled([server?.stop(), jwtIssuer.stop()]);
    throw error;
  }
}

async function getOrStartJazzServer(
  appId?: string,
  schema?: ArrayLike<number>,
  gated = false,
): Promise<StartedJazzServer> {
  const key = JSON.stringify([
    appId ?? DEFAULT_JAZZ_SERVER_KEY,
    schema ? schemaCacheKey(schema) : null,
    gated,
  ]);
  const existing = jazzServerPromises.get(key);

  if (!existing) {
    const startedServer = startJazzServer(appId, schema, gated).catch((error) => {
      jazzServerPromises.delete(key);
      throw error;
    });
    jazzServerPromises.set(key, startedServer);
    return startedServer;
  }

  return existing;
}

function schemaCacheKey(schema: ArrayLike<number>): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < schema.length; index += 1) {
    hash ^= schema[index] ?? 0;
    hash = Math.imul(hash, 0x01000193);
  }
  return `${schema.length}:${(hash >>> 0).toString(16)}`;
}

export async function jazzServerInfo(
  appId?: string,
  schema?: ArrayLike<number>,
  gated = false,
): Promise<{
  appId: string;
  serverUrl: string;
  adminSecret: string;
}> {
  const started = await getOrStartJazzServer(appId, schema, gated);
  return {
    appId: started.appId,
    serverUrl: started.serverUrl,
    adminSecret: started.adminSecret,
  };
}

export async function jazzServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
  appId?: string,
): Promise<string> {
  const { jwtIssuer } = await getOrStartJazzServer(appId);
  return jwtIssuer.jwtForUser(userId, claims);
}

async function stopStartedJazzServer(started: StartedJazzServer): Promise<void> {
  transportGates.delete(started.serverUrl);
  await Promise.all([
    started.transportGate?.close(),
    started.server.stop(),
    started.jwtIssuer.stop(),
  ]);
}

export async function stopJazzServerByUrl(serverUrl: string): Promise<void> {
  for (const [key, runningServer] of jazzServerPromises) {
    const started = await runningServer;
    if (started.serverUrl !== serverUrl) continue;
    jazzServerPromises.delete(key);
    await stopStartedJazzServer(started);
    return;
  }
  throw new Error(`No Jazz test server is running at ${serverUrl}`);
}

export async function stopJazzServer(): Promise<void> {
  const runningServers = [...jazzServerPromises.values()];
  jazzServerPromises.clear();

  if (runningServers.length === 0) {
    return;
  }

  for (const runningServer of runningServers) {
    try {
      await stopStartedJazzServer(await runningServer);
    } catch {
      // Swallow all errors: either startup never produced a server (nothing to stop),
      // or stop() itself failed (nothing recoverable during teardown).
    }
  }
}

function jazzServerUrlPattern(serverUrl: string): string {
  return `${serverUrl.replace(/\/+$/, "")}/**`;
}

function jazzServerWebSocketUrlPattern(serverUrl: string): string {
  const url = new URL(serverUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return `${url.toString().replace(/\/+$/, "")}/**`;
}

function getBrowserContextId(context: BrowserContext): number {
  let id = browserContextIds.get(context);
  if (!id) {
    id = nextBrowserContextId++;
    browserContextIds.set(context, id);
  }
  return id;
}

function activeBlockedPatterns(
  contextRoutes: Map<string, JazzServerRouteBlock> | undefined,
): string[] {
  if (!contextRoutes) return [];
  return [...contextRoutes.entries()]
    .filter(([, routeBlock]) => routeBlock.blocked)
    .map(([pattern]) => pattern);
}

export interface JazzServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

export async function blockJazzServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<void> {
  transportGates.get(serverUrl)?.block();
  const pattern = jazzServerUrlPattern(serverUrl);
  const contextId = getBrowserContextId(context);
  let contextRoutes = blockedServerRoutes.get(context);
  if (!contextRoutes) {
    contextRoutes = new Map();
    blockedServerRoutes.set(context, contextRoutes);
  }
  let routeBlock = contextRoutes.get(pattern);
  if (routeBlock?.blocked) {
    console.info("[jazz-server-network]", {
      action: "block-skip",
      contextId,
      pattern,
      activePatterns: activeBlockedPatterns(contextRoutes),
    });
    return;
  }

  if (!routeBlock) {
    const webSocketPattern = jazzServerWebSocketUrlPattern(serverUrl);
    routeBlock = {
      blocked: false,
      httpHandler: (route) => {
        void route.abort("internetdisconnected");
      },
      webSocketHandler: async (webSocketRoute) => {
        const currentRouteBlock = contextRoutes?.get(pattern);
        if (!currentRouteBlock?.blocked) {
          webSocketRoute.connectToServer();
          return;
        }
        await webSocketRoute.close().catch(() => undefined);
      },
      webSocketPattern,
      webSocketRouted: false,
    };
    contextRoutes.set(pattern, routeBlock);
  }

  routeBlock.blocked = true;
  if (!routeBlock.webSocketRouted) {
    await context.routeWebSocket(routeBlock.webSocketPattern, routeBlock.webSocketHandler);
    routeBlock.webSocketRouted = true;
  }
  await context.route(pattern, routeBlock.httpHandler);
  console.info("[jazz-server-network]", {
    action: "block",
    contextId,
    pattern,
    webSocketPattern: routeBlock.webSocketPattern,
    activePatterns: activeBlockedPatterns(contextRoutes),
  });
}

export async function unblockJazzServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<void> {
  const pattern = jazzServerUrlPattern(serverUrl);
  const contextId = getBrowserContextId(context);
  const contextRoutes = blockedServerRoutes.get(context);
  const routeBlock = contextRoutes?.get(pattern);
  if (!routeBlock?.blocked) {
    transportGates.get(serverUrl)?.unblock();
    console.info("[jazz-server-network]", {
      action: "unblock-skip",
      contextId,
      pattern,
      activePatterns: activeBlockedPatterns(contextRoutes),
    });
    return;
  }

  await context.unroute(pattern, routeBlock.httpHandler);
  routeBlock.blocked = false;
  transportGates.get(serverUrl)?.unblock();
  console.info("[jazz-server-network]", {
    action: "unblock",
    contextId,
    pattern,
    webSocketPattern: routeBlock.webSocketPattern,
    activePatterns: activeBlockedPatterns(contextRoutes),
  });
}
