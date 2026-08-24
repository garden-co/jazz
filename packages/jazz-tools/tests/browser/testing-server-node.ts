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

/**
 * A real server chain for browser adopter scenarios. The two edges are peers:
 * both keep their own local state and connect upstream to the same core.
 */
interface StartedJazzTopology {
  appId: string;
  adminSecret: string;
  backendSecret: string;
  jwtIssuer: TestJwtIssuerHandle;
  schema?: Uint8Array;
  core: LocalJazzServerHandle;
  edge: LocalJazzServerHandle;
  peerEdge: LocalJazzServerHandle;
}

const DEFAULT_JAZZ_SERVER_KEY = "__default__";
const SERVER_LIFECYCLE_TIMEOUT_MS = 10_000;
const jazzServerPromises = new Map<string, Promise<StartedJazzServer>>();
const jazzTopologyPromises = new Map<string, Promise<StartedJazzTopology>>();
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
): Promise<StartedJazzServer> {
  const jwtIssuer = await withServerLifecycleTimeout(startTestJwtIssuer(), "start test JWT issuer");
  const adminSecret = "jazz-browser-test-admin";
  const backendSecret = "jazz-browser-test-backend";
  let server: LocalJazzServerHandle;
  try {
    server = await withServerLifecycleTimeout(
      startLocalJazzServer({
        appId: appId ?? "00000000-0000-0000-0000-000000000001",
        jwksUrl: jwtIssuer.jwksUrl,
        inMemory: true,
        adminSecret,
        backendSecret,
        schema: schema ? Uint8Array.from(schema) : undefined,
      }),
      "start local Jazz server",
    );
  } catch (error) {
    await withServerLifecycleTimeout(
      jwtIssuer.stop(),
      "stop test JWT issuer after failed server start",
    ).catch(() => undefined);
    throw error;
  }
  return {
    server,
    jwtIssuer,
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
  };
}

async function getOrStartJazzServer(
  appId?: string,
  schema?: ArrayLike<number>,
): Promise<StartedJazzServer> {
  const key = schema
    ? `schema:${appId ?? DEFAULT_JAZZ_SERVER_KEY}:${schemaCacheKey(schema)}`
    : (appId ?? DEFAULT_JAZZ_SERVER_KEY);
  const existing = jazzServerPromises.get(key);

  if (!existing) {
    const startedServer = startJazzServer(appId, schema).catch((error) => {
      jazzServerPromises.delete(key);
      throw error;
    });
    jazzServerPromises.set(key, startedServer);
    return startedServer;
  }

  return existing;
}

function topologyKey(appId?: string, schema?: ArrayLike<number>): string {
  return `topology:${schema ? `schema:${appId ?? DEFAULT_JAZZ_SERVER_KEY}:${schemaCacheKey(schema)}` : (appId ?? DEFAULT_JAZZ_SERVER_KEY)}`;
}

async function startJazzTopology(
  appId: string | undefined,
  schema: ArrayLike<number> | undefined,
): Promise<StartedJazzTopology> {
  const jwtIssuer = await withServerLifecycleTimeout(
    startTestJwtIssuer(),
    "start topology JWT issuer",
  );
  const adminSecret = "jazz-browser-test-admin";
  const backendSecret = "jazz-browser-test-backend";
  const topologySchema = schema ? Uint8Array.from(schema) : undefined;
  const options = {
    appId: appId ?? "00000000-0000-0000-0000-000000000001",
    jwksUrl: jwtIssuer.jwksUrl,
    inMemory: true,
    adminSecret,
    backendSecret,
    schema: topologySchema,
  };
  let core: LocalJazzServerHandle | undefined;
  let edge: LocalJazzServerHandle | undefined;
  let peerEdge: LocalJazzServerHandle | undefined;
  try {
    core = await withServerLifecycleTimeout(startLocalJazzServer(options), "start topology core");
    edge = await withServerLifecycleTimeout(
      startLocalJazzServer({ ...options, appId: core.appId, upstreamUrl: core.url }),
      "start topology edge",
    );
    peerEdge = await withServerLifecycleTimeout(
      startLocalJazzServer({ ...options, appId: core.appId, upstreamUrl: core.url }),
      "start topology peer edge",
    );
    return {
      appId: core.appId,
      adminSecret,
      backendSecret,
      jwtIssuer,
      schema: topologySchema,
      core,
      edge,
      peerEdge,
    };
  } catch (error) {
    await Promise.allSettled(
      [peerEdge, edge, core]
        .filter((server): server is LocalJazzServerHandle => server !== undefined)
        .map((server) => withServerLifecycleTimeout(server.stop(), "stop failed topology server")),
    );
    await withServerLifecycleTimeout(jwtIssuer.stop(), "stop failed topology JWT issuer").catch(
      () => undefined,
    );
    throw error;
  }
}

async function getOrStartJazzTopology(
  appId?: string,
  schema?: ArrayLike<number>,
): Promise<[string, StartedJazzTopology]> {
  const key = topologyKey(appId, schema);
  let topology = jazzTopologyPromises.get(key);
  if (!topology) {
    topology = startJazzTopology(appId, schema).catch((error) => {
      jazzTopologyPromises.delete(key);
      throw error;
    });
    jazzTopologyPromises.set(key, topology);
  }
  return [key, await topology];
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
): Promise<{
  appId: string;
  serverUrl: string;
  adminSecret: string;
}> {
  const started = await getOrStartJazzServer(appId, schema);
  return {
    appId: started.appId,
    serverUrl: started.serverUrl,
    adminSecret: started.adminSecret,
  };
}

export interface JazzServerTopologyInfo {
  topologyId: string;
  appId: string;
  adminSecret: string;
  coreUrl: string;
  edgeUrl: string;
  peerEdgeUrl: string;
}

function topologyInfo(topologyId: string, topology: StartedJazzTopology): JazzServerTopologyInfo {
  return {
    topologyId,
    appId: topology.appId,
    adminSecret: topology.adminSecret,
    coreUrl: topology.core.url,
    edgeUrl: topology.edge.url,
    peerEdgeUrl: topology.peerEdge.url,
  };
}

/** Start a core plus two independently stateful peer edges for a browser scenario. */
export async function jazzServerTopologyInfo(
  appId?: string,
  schema?: ArrayLike<number>,
): Promise<JazzServerTopologyInfo> {
  const [topologyId, topology] = await getOrStartJazzTopology(appId, schema);
  return topologyInfo(topologyId, topology);
}

/**
 * Restart one edge in place, preserving its URL so connected browser clients
 * exercise actual reconnect behavior rather than being pointed at a new host.
 */
export async function restartJazzServerTopologyEdge(
  topologyId: string,
  edgeName: "edge" | "peerEdge",
): Promise<JazzServerTopologyInfo> {
  const promise = jazzTopologyPromises.get(topologyId);
  if (!promise) throw new Error(`unknown Jazz browser topology: ${topologyId}`);
  const topology = await promise;
  const previous = topology[edgeName];
  await withServerLifecycleTimeout(previous.stop(), `stop topology ${edgeName}`);
  topology[edgeName] = await withServerLifecycleTimeout(
    startLocalJazzServer({
      appId: topology.appId,
      port: previous.port,
      jwksUrl: topology.jwtIssuer.jwksUrl,
      inMemory: true,
      adminSecret: topology.adminSecret,
      backendSecret: topology.backendSecret,
      upstreamUrl: topology.core.url,
      schema: topology.schema,
    }),
    `restart topology ${edgeName}`,
  );
  return topologyInfo(topologyId, topology);
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
  const runningTopologies = [...jazzTopologyPromises.values()];
  jazzServerPromises.clear();
  jazzTopologyPromises.clear();

  if (runningServers.length === 0 && runningTopologies.length === 0) {
    return;
  }

  for (const runningTopology of runningTopologies) {
    try {
      const topology = await runningTopology;
      await Promise.allSettled(
        [topology.peerEdge, topology.edge, topology.core].map((server) =>
          withServerLifecycleTimeout(server.stop(), "stop local topology server"),
        ),
      );
      await withServerLifecycleTimeout(topology.jwtIssuer.stop(), "stop topology JWT issuer");
    } catch {
      // Best effort during browser-suite teardown.
    }
  }

  for (const runningServer of runningServers) {
    try {
      const { server, jwtIssuer } = await runningServer;
      await withServerLifecycleTimeout(server.stop(), "stop local Jazz server");
      await withServerLifecycleTimeout(jwtIssuer.stop(), "stop test JWT issuer");
    } catch {
      // Swallow all errors: either startup never produced a server (nothing to stop),
      // or stop() itself failed (nothing recoverable during teardown).
    }
  }
}

function withServerLifecycleTimeout<T>(operation: Promise<T>, label: string): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    operation,
    new Promise<T>((_, reject) => {
      timeout = setTimeout(() => {
        reject(
          new Error(
            `Jazz browser test server lifecycle timed out after ${SERVER_LIFECYCLE_TIMEOUT_MS}ms: ${label}`,
          ),
        );
      }, SERVER_LIFECYCLE_TIMEOUT_MS);
    }),
  ]).finally(() => {
    if (timeout) clearTimeout(timeout);
  });
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
