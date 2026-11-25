import { backendNative } from "./backend-native.js";
import { cloudflareWebSocketUpgrade } from "./cloudflare-websocket.js";
import {
  BINDING_ORIGIN,
  bindingCredential,
  bindingTransport,
  networkTransport,
  refuseRedirect,
  type CoreBinding,
} from "./core-transport.js";
import { remote, unreachableCore } from "./remote-provider.js";
import { logStoreOpened } from "./store-log.js";
import type { JazzCoreOpenContext, JazzProvider } from "./core-provider.js";

type DurableObjectLifecycle = "while-connected" | "hibernate";
const CREDENTIAL_HEADER = "x-jazz-backend-credential";
class DurableObjectsProvider implements JazzProvider {
  assertSupportedEnvironment() {
    if (backendNative().host !== "workerd") {
      throw new Error(
        "durableObjects() requires a Cloudflare Worker. To connect to its core from this host, use remote({ url, appId, secret }).",
      );
    }
  }
  getWebSocketUpgrade() {
    return cloudflareWebSocketUpgrade;
  }
  readonly kind = "durableObjects";
  declare readonly secret?: string;
  constructor(
    readonly url: string,
    secret: string | undefined,
    readonly binding?: CoreBinding,
    readonly lifecycle?: DurableObjectLifecycle,
  ) {
    if (secret !== undefined) this.secret = secret;
  }
  open(context: JazzCoreOpenContext) {
    return durableObjectCore(this, context);
  }
}

/** The shortest core secret a Durable Object core accepts. */
const DURABLE_OBJECT_SECRET_MIN_LENGTH = 32;
/** The hosts a `durableObjects()` url may reach over plain http. */
const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
/** How long the store waits for the core to name its app. */
const DURABLE_OBJECT_APP_TIMEOUT_MS = 30_000;
/** An app id the core may name: no path separators, dots or line breaks. */
const DURABLE_OBJECT_APP_ID = /^[A-Za-z0-9_-]{1,128}$/;

/**
 * For a backend running in Cloudflare Workers. From Node, connect with
 * `remote({ url, appId, secret })`.
 *
 * A core running in a Cloudflare Durable Object: one SQLite-backed object per
 * app, in a Worker you deploy that exports `JazzCore` and forwards requests to
 * it with `routeJazzRequest`. Create that class with `createJazzCore` from
 * `jazz-tools/durable-object`. `url` is
 * that Worker's origin (e.g. `https://my-app-jazz.example.workers.dev`) and
 * `secret` its `JAZZ_CORE_SECRET`: a random string of at least 32 characters
 * that stays the same across deployments. The object keeps its own app id,
 * which this backend asks it for when the store opens.
 *
 * With a binding, the core can supply its persisted credential over RPC.
 * URL connections require an explicit secret.
 *
 * Large values' bytes go where the core's Worker puts them, so there is no
 * `blobs` option here: select SQLite, R2, S3 or Vercel Blob on the core Worker with `createJazzCore()`.
 *
 * The core admits only this backend, which holds the secret; browsers keep
 * talking to your backend. A Durable Object core doesn't run thin-client
 * transactions (your backend runs them, as with any core) and has no
 * HTTP-stream fallback for the backend's own sockets.
 *
 * @experimental Durable Object cores are new; the options and the Worker's
 * bindings may still change.
 */
export function durableObjects(
  options: { lifecycle?: DurableObjectLifecycle } & (
    | { url: string; secret: string; appId?: never }
    | { binding: CoreBinding; secret?: string; appId?: never }
  ),
): DurableObjectsProvider {
  if (typeof options === "object" && options !== null && "appId" in options) {
    throw new Error(
      "durableObjects(): appId is not an option: the store keeps its own app id, in the Worker's Durable Object",
    );
  }
  const lifecycle = options?.lifecycle;
  if (lifecycle !== undefined && lifecycle !== "while-connected" && lifecycle !== "hibernate") {
    throw new TypeError('durableObjects(): lifecycle must be "while-connected" or "hibernate"');
  }
  if (typeof options === "object" && options !== null && "binding" in options) {
    const { binding, secret } = options;
    if (
      typeof binding !== "object" ||
      binding === null ||
      (typeof (binding as { fetch?: unknown }).fetch !== "function" &&
        typeof (binding as { idFromName?: unknown }).idFromName !== "function")
    ) {
      throw new Error(
        "durableObjects(): binding must be the Worker's JazzCore Durable Object binding, e.g. env.JAZZ",
      );
    }
    if (secret !== undefined) checkCoreSecret(secret);
    return new DurableObjectsProvider(BINDING_ORIGIN, secret, binding, lifecycle);
  }
  const { url, secret } = (options ?? {}) as { url: string; secret: string };
  let origin: URL | undefined;
  try {
    origin = new URL(url);
  } catch {
    // Refused below.
  }
  if (
    !origin ||
    (origin.protocol !== "https:" && origin.protocol !== "http:") ||
    origin.pathname.replace(/\/+$/, "") !== "" ||
    origin.search ||
    origin.hash
  ) {
    throw new Error(
      `durableObjects(): url must be the Worker's origin, like "https://my-app-jazz.example.workers.dev"; got ${JSON.stringify(url)}`,
    );
  }
  if (origin.username || origin.password) {
    throw new Error(
      "durableObjects(): url must not carry a user name or password; the core secret is the only credential",
    );
  }
  // The secret goes with every request: in the clear only to this machine
  // (`wrangler dev`, tests).
  if (origin.protocol === "http:" && !LOOPBACK_HOSTS.has(origin.hostname)) {
    throw new Error(
      `durableObjects(): url must be https:// (http:// only for localhost, 127.0.0.1 or [::1]); got ${JSON.stringify(url)}`,
    );
  }
  checkCoreSecret(secret);
  return new DurableObjectsProvider(origin.origin, secret, undefined, lifecycle);
}

function checkCoreSecret(secret: unknown): void {
  if (typeof secret !== "string" || secret.length < DURABLE_OBJECT_SECRET_MIN_LENGTH) {
    throw new Error(
      `durableObjects(): secret must be the Worker's JAZZ_CORE_SECRET, a random string of at least ${DURABLE_OBJECT_SECRET_MIN_LENGTH} characters`,
    );
  }
}

/**
 * The `remote()` store a `durableObjects()` store opens as: the Worker's
 * object tells this backend its app id (`GET /app`, with the core secret).
 */
async function durableObjectCore(store: DurableObjectsProvider, context: JazzCoreOpenContext) {
  const transport = store.binding ? bindingTransport(store.binding) : networkTransport();
  const secret = store.secret ?? (await bindingCredential(store.binding!));
  let response: Response;
  let body: { appId?: unknown } | undefined;
  try {
    response = await transport.fetch(`${store.url}/app`, {
      headers: { [CREDENTIAL_HEADER]: secret },
      redirect: "manual",
      signal: AbortSignal.timeout(DURABLE_OBJECT_APP_TIMEOUT_MS),
    });
    await refuseRedirect(store.url, `${store.url}/app`, response, "durableObjects()");
    body = (await response.json().catch(() => undefined)) as typeof body;
  } catch (error) {
    if ((error as { name?: unknown } | null)?.name === "TimeoutError") {
      throw new Error(
        `createJazz: the Durable Object core at ${store.url} did not answer within ${DURABLE_OBJECT_APP_TIMEOUT_MS / 1000}s; this Jazz tries again on its next use`,
        { cause: error },
      );
    }
    throw unreachableCore(store.url, error);
  }
  if (response.status === 401 || response.status === 403) {
    throw new Error(
      `createJazz: the Durable Object core at ${store.url} refused the secret: durableObjects()'s secret must be the Worker's JAZZ_CORE_SECRET`,
    );
  }
  // The id goes into request paths and log lines: only a plain one.
  if (!response.ok || typeof body?.appId !== "string" || !DURABLE_OBJECT_APP_ID.test(body.appId)) {
    throw new Error(
      `createJazz: ${store.url} did not answer as a Jazz Durable Object core (GET /app: ${response.status}); is url the Worker that exports JazzCore, on this jazz-tools version?`,
    );
  }
  const core = await remote({
    appId: body.appId,
    url: store.url,
    secret,
    transport:
      store.lifecycle === "hibernate"
        ? {
            fetch: transport.fetch,
            openWebSocket: (url, headers) =>
              transport.openWebSocket(url, { ...headers, "x-jazz-socket-lifecycle": "hibernate" }),
          }
        : transport,
  }).open(context);
  logStoreOpened(`jazz: store durableObjects at ${store.url} (app ${body.appId})`);
  return core;
}
