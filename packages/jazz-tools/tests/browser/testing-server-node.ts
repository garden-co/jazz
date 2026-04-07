import { TestingServer } from "jazz-napi";
import type { BrowserContext, Route } from "playwright";

interface StartedTestingServer {
  server: TestingServer;
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

let testingServerPromise: Promise<StartedTestingServer> | null = null;
const blockedServerRoutes = new WeakMap<BrowserContext, Map<string, (route: Route) => void>>();
const browserContextIds = new WeakMap<BrowserContext, number>();
let nextBrowserContextId = 1;

async function startTestingServer(): Promise<StartedTestingServer> {
  const server = await TestingServer.start();
  return {
    server,
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
  };
}

async function getOrStartTestingServer(): Promise<StartedTestingServer> {
  if (!testingServerPromise) {
    testingServerPromise = startTestingServer().catch((error) => {
      testingServerPromise = null;
      throw error;
    });
  }

  return testingServerPromise;
}

export async function testingServerInfo(): Promise<{
  appId: string;
  serverUrl: string;
  adminSecret: string;
}> {
  const { appId, serverUrl, adminSecret } = await getOrStartTestingServer();
  return { appId, serverUrl, adminSecret };
}

export async function testingServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
): Promise<string> {
  const { server } = await getOrStartTestingServer();
  return server.jwtForUser(userId, claims);
}

export async function stopTestingServer(): Promise<void> {
  const runningServer = testingServerPromise;
  testingServerPromise = null;

  if (!runningServer) {
    return;
  }

  try {
    const { server } = await runningServer;
    await server.stop();
  } catch {
    // Swallow all errors: either startup never produced a server (nothing to stop),
    // or stop() itself failed (nothing recoverable during teardown).
  }
}

function testingServerUrlPattern(serverUrl: string): string {
  return `${serverUrl.replace(/\/+$/, "")}/**`;
}

function getBrowserContextId(context: BrowserContext): number {
  let id = browserContextIds.get(context);
  if (!id) {
    id = nextBrowserContextId++;
    browserContextIds.set(context, id);
  }
  return id;
}

export interface TestingServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

export async function debugTestingServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<TestingServerNetworkDebugState> {
  const pattern = testingServerUrlPattern(serverUrl);
  const contextRoutes = blockedServerRoutes.get(context);
  return {
    contextId: getBrowserContextId(context),
    pattern,
    blocked: contextRoutes?.has(pattern) ?? false,
    activePatterns: contextRoutes ? [...contextRoutes.keys()] : [],
  };
}

export async function blockTestingServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<void> {
  const pattern = testingServerUrlPattern(serverUrl);
  const contextId = getBrowserContextId(context);
  let contextRoutes = blockedServerRoutes.get(context);
  if (!contextRoutes) {
    contextRoutes = new Map();
    blockedServerRoutes.set(context, contextRoutes);
  }
  if (contextRoutes.has(pattern)) {
    console.info("[testing-server-network]", {
      action: "block-skip",
      contextId,
      pattern,
      activePatterns: [...contextRoutes.keys()],
    });
    return;
  }

  const handler = (route: Route) => {
    void route.abort("internetdisconnected");
  };
  contextRoutes.set(pattern, handler);
  await context.route(pattern, handler);
  console.info("[testing-server-network]", {
    action: "block",
    contextId,
    pattern,
    activePatterns: [...contextRoutes.keys()],
  });
}

export async function unblockTestingServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<void> {
  const pattern = testingServerUrlPattern(serverUrl);
  const contextId = getBrowserContextId(context);
  const contextRoutes = blockedServerRoutes.get(context);
  const handler = contextRoutes?.get(pattern);
  if (!handler) {
    console.info("[testing-server-network]", {
      action: "unblock-skip",
      contextId,
      pattern,
      activePatterns: contextRoutes ? [...contextRoutes.keys()] : [],
    });
    return;
  }

  await context.unroute(pattern, handler);
  contextRoutes?.delete(pattern);
  console.info("[testing-server-network]", {
    action: "unblock",
    contextId,
    pattern,
    activePatterns: contextRoutes ? [...contextRoutes.keys()] : [],
  });
}
