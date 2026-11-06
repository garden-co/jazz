import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import type { AccountStore } from "../../src/accounts/persistence.js";
import type { AccountHandle } from "../../src/accounts/state.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../../src/schema-source.js";
import type { CompiledPermissionsMap } from "../../src/schema-permissions.js";
import {
  jazzServerBrowserCommands,
  jazzServerTransportControlBrowserCommands,
} from "./browser-commands.js";

export interface JazzServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  /** Set by {@link startJazzApp}: the core behind the app backend at `serverUrl`. */
  coreUrl?: string;
}

/** The core's own info, for deploying to it, whether `server` is a core or an app backend. */
export function jazzCore(server: JazzServerInfo): JazzServerInfo {
  return {
    appId: server.appId,
    serverUrl: server.coreUrl ?? server.serverUrl,
    adminSecret: server.adminSecret,
  };
}

export function getJazzServerInfo(appId?: string): Promise<JazzServerInfo> {
  return jazzServerBrowserCommands().jazzServerInfo(appId);
}

/**
 * Run the app's backend (createJazz) for what the test just deployed, and
 * return the server info with its `/jazz` URL as `serverUrl`: browser
 * clients sync through it, since a core admits only its app's backend.
 * Pass the core's info (from {@link getJazzServerInfo}) or an earlier result;
 * call again after a redeploy, and the backend restarts on the same origin.
 */
export async function startJazzApp(
  core: JazzServerInfo,
  app: { schema: SchemaSourceInput; permissions: CompiledPermissionsMap },
): Promise<JazzServerInfo> {
  const coreUrl = jazzCore(core).serverUrl;
  const serverUrl = await jazzServerBrowserCommands().jazzAppFront(coreUrl, {
    schema: resolveSchemaSource(app.schema),
    permissions: app.permissions,
    pageOrigin: location.origin,
  });
  return { ...core, serverUrl, coreUrl };
}

/**
 * Sign `userId` in to the app backend at `app.serverUrl` (from
 * {@link startJazzApp}): returns the URL of that user's own origin, whose
 * requests the backend's `authenticate` names as them. Sign in there with
 * `loginWithBackend()`.
 */
export function jazzAppUserUrl(
  app: JazzServerInfo,
  userId: string,
  claims?: Record<string, unknown>,
): Promise<string> {
  return jazzServerBrowserCommands().jazzAppUser(app.serverUrl, userId, claims ?? {});
}

/**
 * Sign the user at `userUrl` (from {@link jazzAppUserUrl}) out of the app
 * backend `app`, as its logout clears their cookie.
 */
export function signOutOfJazzApp(app: JazzServerInfo, userUrl: string): Promise<void> {
  const signOut = jazzServerBrowserCommands().jazzAppSignOut;
  if (!signOut)
    throw new Error("This browser test config does not register the jazzAppSignOut command");
  return signOut(app.serverUrl, userUrl);
}

/**
 * A `fetch` for an account manager's sign-in requests to a test app backend.
 * The page's origin is not the backend's and the backend answers no CORS (an
 * app serves its backend on its own origin), so Node sends them.
 */
/**
 * Insert a row with the app backend's own authority (from
 * {@link startJazzApp}), as the app's server code writes what no client may.
 * Resolves once the core has it.
 */
export async function insertAsJazzAppBackend<T>(
  app: JazzServerInfo,
  table: { _table: string; _rowType: T },
  row: Record<string, unknown>,
): Promise<T> {
  const insert = jazzServerBrowserCommands().jazzAppBackendInsert;
  if (!insert)
    throw new Error("This browser test config does not register the jazzAppBackendInsert command");
  return (await insert(app.serverUrl, table._table, row)) as T;
}

export const jazzAppFetch: typeof fetch = async (input, init) => {
  const request = new Request(input, init);
  const response = await jazzServerBrowserCommands().jazzAppRequest(request.url, {
    method: request.method,
    headers: [...request.headers],
    body: request.method === "GET" || request.method === "HEAD" ? null : await request.text(),
  });
  return new Response(response.body, { status: response.status, headers: response.headers });
};

/**
 * Sign `userId` in to the app backend `app` (from {@link startJazzApp}) and
 * return the Db config fields for syncing as them: their own origin's URL
 * and a `loginWithBackend()` account from it.
 */
export function jazzAppUser(
  app: JazzServerInfo,
  userId: string,
  claims?: Record<string, unknown>,
): Promise<{ appId: string; serverUrl: string; account: AccountHandle }> {
  return signInWithJazzApp(createAccountManager, app, userId, claims);
}

/** An account manager factory: this package's or an example's own `createAccountManager`. */
export type AccountManagerFactory<Account> = (config: {
  appId: string;
  serverUrl: string;
  fetch: typeof fetch;
  store: AccountStore;
}) => Promise<{ loginWithBackend(): Promise<Account> }>;

/**
 * {@link jazzAppUser} with the caller's own `createAccountManager`, so an
 * example's account handles come from (and stay in) its own copy of Jazz.
 */
export async function signInWithJazzApp<Account>(
  createManager: AccountManagerFactory<Account>,
  app: JazzServerInfo,
  userId: string,
  claims?: Record<string, unknown>,
): Promise<{ appId: string; serverUrl: string; account: Account }> {
  const serverUrl = await jazzAppUserUrl(app, userId, claims);
  return {
    appId: app.appId,
    serverUrl,
    account: await loginAtJazzAppUser(app.appId, serverUrl, createManager),
  };
}

/**
 * Sign in again at a user's own origin (from {@link jazzAppUserUrl}): the
 * same account each time, e.g. to reopen that user's persistent client.
 * Defaults to this package's own account manager.
 */
export async function loginAtJazzAppUser<Account = AccountHandle>(
  appId: string,
  userUrl: string,
  createManager: AccountManagerFactory<Account> = createAccountManager as never,
): Promise<Account> {
  const accounts = await createManager({
    appId,
    serverUrl: userUrl,
    fetch: jazzAppFetch,
    store: memoryAccountStore(),
  });
  return accounts.loginWithBackend();
}

function memoryAccountStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

export function stopJazzServer(serverUrl: string): Promise<void> {
  return jazzServerBrowserCommands().jazzServerStop(serverUrl);
}

/**
 * Browser bridge to the shared TransportControl: buffer delivery without
 * disconnecting, using the same block/blockInbound/unblock operations as Rust.
 */
export interface JazzServerTransportControl {
  url: string;
  block(): Promise<void>;
  blockInbound(): Promise<void>;
  unblock(): Promise<void>;
  /** The server goes away: connections close and new ones are refused. */
  down(): Promise<void>;
  /** The server is back, at `serverUrl` if given (a restarted front). */
  up(serverUrl?: string): Promise<void>;
  stop(): Promise<void>;
}

export async function createJazzServerTransportControl(
  serverUrl: string,
): Promise<JazzServerTransportControl> {
  const commands = jazzServerTransportControlBrowserCommands();
  const url = await commands.jazzServerTransportControlCreate(serverUrl);
  return {
    url,
    block: () => commands.jazzServerTransportControlBlock(url, "both"),
    blockInbound: () => commands.jazzServerTransportControlBlock(url, "inbound"),
    unblock: () => commands.jazzServerTransportControlUnblock(url),
    down: () => commands.jazzServerTransportControlDown(url),
    up: (serverUrl?: string) => commands.jazzServerTransportControlUp(url, serverUrl),
    stop: () => commands.jazzServerTransportControlStop(url),
  };
}
