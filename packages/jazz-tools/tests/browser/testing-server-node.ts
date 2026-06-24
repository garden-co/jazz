import type { BrowserContext, Route, WebSocketRoute } from "playwright";
type JazzNapiServer = import("jazz-napi").JazzServer;
type JazzNapiTestJwtIssuer = import("jazz-napi").TestJwtIssuer;

interface StartedJazzServer {
  server: JazzNapiServer;
  jwtIssuer: JazzNapiTestJwtIssuer;
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

async function loadJazzNapi(): Promise<{
  JazzServer: typeof import("jazz-napi").JazzServer;
  TestJwtIssuer: typeof import("jazz-napi").TestJwtIssuer;
}> {
  try {
    const module = await import("jazz-napi");
    return {
      JazzServer: module.JazzServer,
      TestJwtIssuer: module.TestJwtIssuer,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      "Browser tests require the jazz-napi JazzServer host binding. Run `pnpm --filter jazz-napi build` first.\n\n" +
        `Original error: ${message}`,
    );
  }
}

async function startJazzServer(appId?: string): Promise<StartedJazzServer> {
  const { JazzServer, TestJwtIssuer } = await loadJazzNapi();
  const jwtIssuer = await TestJwtIssuer.start();
  const adminSecret = "jazz-browser-test-admin";
  const backendSecret = "jazz-browser-test-backend";
  const server = await JazzServer.start({
    appId: appId ?? "00000000-0000-0000-0000-000000000001",
    jwksUrl: jwtIssuer.jwksUrl,
    inMemory: true,
    adminSecret,
    backendSecret,
  });
  return {
    server,
    jwtIssuer,
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret ?? adminSecret,
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
  const serverInfo = await getOrStartJazzServer(appId);
  return {
    appId: serverInfo.appId,
    serverUrl: serverInfo.serverUrl,
    adminSecret: serverInfo.adminSecret,
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

export async function stopJazzServer(): Promise<void> {
  const runningServers = [...jazzServerPromises.values()];
  jazzServerPromises.clear();

  if (runningServers.length === 0) {
    return;
  }

  for (const runningServer of runningServers) {
    try {
      const { server, jwtIssuer } = await runningServer;
      await server.stop();
      await jwtIssuer.stop();
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

export async function debugJazzServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<JazzServerNetworkDebugState> {
  const pattern = jazzServerUrlPattern(serverUrl);
  const contextRoutes = blockedServerRoutes.get(context);
  return {
    contextId: getBrowserContextId(context),
    pattern,
    blocked: contextRoutes?.get(pattern)?.blocked ?? false,
    activePatterns: activeBlockedPatterns(contextRoutes),
  };
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
