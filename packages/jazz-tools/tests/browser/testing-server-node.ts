import {
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "../../src/testing/index.js";
import {
  createTransportControl,
  type TransportControl,
} from "../../src/runtime/testing/transport-control.js";

interface StartedJazzServer {
  server: LocalJazzServerHandle;
  jwtIssuer: TestJwtIssuerHandle;
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

const DEFAULT_JAZZ_SERVER_KEY = "__default__";
const jazzServerPromises = new Map<string, Promise<StartedJazzServer>>();
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
  for (const url of transportControls.keys()) await stopJazzServerTransportControl(url);
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

const transportControls = new Map<string, TransportControl>();

function transportControl(url: string): TransportControl {
  const control = transportControls.get(url);
  if (!control) throw new Error(`No transport control is running at ${url}`);
  return control;
}

export async function createJazzServerTransportControl(serverUrl: string): Promise<string> {
  const control = await createTransportControl(serverUrl);
  transportControls.set(control.url, control);
  return control.url;
}

export function blockJazzServerTransport(url: string, direction: "both" | "inbound"): void {
  const control = transportControl(url);
  if (direction === "inbound") control.blockInbound();
  else control.block();
}

export function unblockJazzServerTransport(url: string): void {
  transportControl(url).unblock();
}

export async function stopJazzServerTransportControl(url: string): Promise<void> {
  const control = transportControl(url);
  transportControls.delete(url);
  await control.stop();
}
