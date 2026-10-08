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
}

const DEFAULT_JAZZ_SERVER_KEY = "__default__";
const jazzServerPromises = new Map<string, Promise<StartedJazzServer>>();
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

async function startJazzServer(appId?: string): Promise<StartedJazzServer> {
  const jwtIssuer = await startTestJwtIssuer();
  const adminSecret = "jazz-browser-test-admin";
  const backendSecret = "jazz-browser-test-backend";
  const server = await startLocalJazzServer({
    appId: appId ?? "00000000-0000-0000-0000-000000000001",
    jwksUrl: jwtIssuer.jwksUrl,
    jwtIssuer: jwtIssuer.issuer,
    jwtAudience: jwtIssuer.audience,
    inMemory: true,
    adminSecret,
    backendSecret,
  });
  return {
    server,
    jwtIssuer,
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
  };
}

async function getOrStartJazzServer(appId?: string): Promise<StartedJazzServer> {
  const key = appId ?? DEFAULT_JAZZ_SERVER_KEY;
  const existing = jazzServerPromises.get(key);

  if (!existing) {
    const startedServer = startJazzServer(appId).catch((error) => {
      jazzServerPromises.delete(key);
      throw error;
    });
    jazzServerPromises.set(key, startedServer);
    return startedServer;
  }

  return existing;
}

export async function jazzServerInfo(appId?: string): Promise<{
  appId: string;
  serverUrl: string;
  adminSecret: string;
}> {
  const started = await getOrStartJazzServer(appId);
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

export async function stopJazzServerByUrl(serverUrl: string): Promise<void> {
  for (const [key, runningServer] of jazzServerPromises) {
    const started = await runningServer;
    if (started.serverUrl !== serverUrl) continue;
    jazzServerPromises.delete(key);
    await Promise.all([started.server.stop(), started.jwtIssuer.stop()]);
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

  let nextServer = 0;
  await Promise.all(
    Array.from({ length: Math.min(4, runningServers.length) }, async () => {
      while (nextServer < runningServers.length) {
        const runningServer = runningServers[nextServer++];
        try {
          const { server, jwtIssuer } = await runningServer;
          await server.stop();
          await jwtIssuer.stop();
        } catch {
          // Startup or shutdown failed; retain the global best-effort sweep.
        }
      }
    }),
  );
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
  console.info("[jazz-server-network]", {
    action: "unblock",
    contextId,
    pattern,
    webSocketPattern: routeBlock.webSocketPattern,
    activePatterns: activeBlockedPatterns(contextRoutes),
  });
}
