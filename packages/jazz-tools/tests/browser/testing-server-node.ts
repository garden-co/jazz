import {
  startLocalJazzServer,
  startTestJazz,
  type LocalJazzServerHandle,
  type TestJazz,
} from "../../src/testing/index.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import type { CompiledPermissionsMap } from "../../src/schema-permissions.js";
import {
  createTransportControl,
  type TransportControl,
} from "../../src/runtime/testing/transport-control.js";

interface StartedJazzServer {
  server: LocalJazzServerHandle;
  /** The app's backend (createJazz) in front of the core, once a test deploys. */
  front?: TestJazz;
  /** The schema the app backend runs (from its latest `jazzAppFront`). */
  frontSchema?: WasmSchema;
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

const DEFAULT_JAZZ_SERVER_KEY = "__default__";
const jazzServerPromises = new Map<string, Promise<StartedJazzServer>>();
async function startJazzServer(appId?: string): Promise<StartedJazzServer> {
  const adminSecret = "jazz-browser-test-admin";
  const backendSecret = "jazz-browser-test-backend";
  const server = await startLocalJazzServer({
    appId: appId ?? "00000000-0000-0000-0000-000000000001",
    inMemory: true,
    adminSecret,
    backendSecret,
  });
  return {
    server,
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

async function startedFor(url: string): Promise<[string, StartedJazzServer] | undefined> {
  for (const [key, runningServer] of jazzServerPromises) {
    const started = await runningServer.catch(() => undefined);
    if (started && (started.serverUrl === url || started.front?.url === url)) return [key, started];
  }
  return undefined;
}

async function stopStarted(started: StartedJazzServer): Promise<void> {
  await started.front?.stop();
  await started.server.stop();
}

/** Stop a core (named by its own URL or its app backend's) and its app backend. */
export async function stopJazzServerByUrl(serverUrl: string): Promise<void> {
  const found = await startedFor(serverUrl);
  if (!found) throw new Error(`No Jazz test server is running at ${serverUrl}`);
  jazzServerPromises.delete(found[0]);
  await stopStarted(found[1]);
}

/**
 * Run the app's backend for what a test just deployed to the core at
 * `coreUrl`: createJazz with that schema and permissions, as the app's
 * deployment does. A later call (a redeploy) restarts it on the same origin.
 * Returns the backend's `/jazz` URL, which browser clients sync through.
 */
export async function jazzAppFront(
  coreUrl: string,
  app: { schema: WasmSchema; permissions: CompiledPermissionsMap; pageOrigin: string },
): Promise<string> {
  const found = await startedFor(coreUrl);
  if (!found) throw new Error(`No Jazz test server is running at ${coreUrl}`);
  const started = found[1];
  const port = started.front ? Number(new URL(started.front.url).port) : undefined;
  await started.front?.stop();
  started.front = await startTestJazz({
    app: app.schema,
    permissions: app.permissions,
    core: started.server,
    port,
    // The test page is served from the browser runner's own origin.
    jazz: { allowedOrigins: [app.pageOrigin] },
  });
  started.frontSchema = app.schema;
  return started.front.url;
}

/**
 * Insert `row` into `table` with the app backend's own authority, as the
 * app's server code does (e.g. a bootstrap route writing what no client may),
 * and return the row once the core has it.
 */
export async function jazzAppBackendInsert(
  frontUrl: string,
  table: string,
  row: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const started = (await startedFor(frontUrl))?.[1];
  if (!started?.front || started.front.url !== frontUrl || !started.frontSchema)
    throw new Error(`No Jazz app backend is running at ${frontUrl}`);
  const proxy = {
    _table: table,
    _schema: started.frontSchema,
    _rowType: {} as Record<string, unknown>,
    _initType: {} as Record<string, unknown>,
  };
  return started.front.jazz.db.insert(proxy, row).wait({ tier: "global" });
}

/** Sign `userId` in to the app backend at `frontUrl`: their own origin's `/jazz` URL. */
export async function jazzAppUser(
  frontUrl: string,
  userId: string,
  claims?: Record<string, unknown>,
): Promise<string> {
  const front = (await startedFor(frontUrl))?.[1].front;
  if (!front || front.url !== frontUrl)
    throw new Error(`No Jazz app backend is running at ${frontUrl}`);
  return (await front.as(userId, claims)).serverUrl;
}

/**
 * Send an HTTP request to a test app backend from Node. A page's origin is not
 * the backend's (its user origins are other ports), and a backend answers no
 * CORS: an app serves its backend on its own origin. Loopback only.
 */
export async function jazzAppRequest(
  url: string,
  init: { method: string; headers: [string, string][]; body: string | null },
): Promise<{ status: number; headers: [string, string][]; body: string }> {
  if (new URL(url).hostname !== "127.0.0.1")
    throw new Error(`jazzAppRequest only reaches loopback test backends, not ${url}`);
  const response = await fetch(url, {
    method: init.method,
    headers: init.headers,
    body: init.body,
    redirect: "error",
  });
  return { status: response.status, headers: [...response.headers], body: await response.text() };
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
      await stopStarted(await runningServer);
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

/**
 * A transport control in front of `serverUrl`. In front of a user's origin of
 * an app backend (from `jazzAppUser`), it carries that user's sign-in, as the
 * browser behind it would.
 */
export async function createJazzServerTransportControl(serverUrl: string): Promise<string> {
  const control = await createTransportControl(serverUrl);
  transportControls.set(control.url, control);
  for (const runningServer of jazzServerPromises.values()) {
    const front = (await runningServer.catch(() => undefined))?.front;
    if (front?.proxyOrigin(serverUrl, new URL(control.url).host)) break;
  }
  return control.url;
}

export function blockJazzServerTransport(
  url: string,
  direction: "both" | "inbound" | "sever",
): void {
  const control = transportControl(url);
  if (direction === "inbound") control.blockInbound();
  else if (direction === "sever") control.sever();
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
