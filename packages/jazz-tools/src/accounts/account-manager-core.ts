import { accountRegistryUrl } from "./context.js";
import { accountAppId } from "./local-first.js";
import { clientScope, DEFAULT_SERVER_URL } from "../runtime/client-scope.js";
import { prepareAccountManager, readableSelection, type AccountStore } from "./persistence.js";
import type { AccountManagerConfig } from "./create-account-manager.js";

/** Signs a local-first account's token for `audience` (see `prepareAccountManager`). */
export type LocalFirstTokenMinter = (secret: string, audience: string) => string;

/**
 * @internal The browser account manager, with token signing supplied by the
 * caller: the WASM runtime for `jazz-tools`, plain JS for the thin client.
 */
export async function createAccountManagerWithMinter(
  config: AccountManagerConfig,
  mintToken: LocalFirstTokenMinter,
  options: { rememberBackendUser?: boolean } = {},
) {
  if (config.bearer !== undefined) {
    if (typeof config.bearer !== "function")
      throw new Error("bearer must be a function that returns the app's token (or null)");
    if (sendsCookies())
      throw new Error(
        "bearer is not supported yet in a browser page or worker: its sync connection can't carry the token. There the app's backend names the user by its cookie; leave bearer out.",
      );
    // React Native's WebSocket takes headers in another argument: its own
    // entry sends the bearer on the native sync connection.
    if ((globalThis as { navigator?: { product?: string } }).navigator?.product === "ReactNative")
      throw new Error(
        "bearer from jazz-tools/client is not supported on React Native: use createAccountManager from jazz-tools/react-native or jazz-tools/expo, whose bearer the native sync connection sends.",
      );
    if (config.serverUrl === null)
      throw new Error("bearer signs in through the app's backend: a device-only client has none");
  }
  const registry = clientRegistryUrl(config);
  const scope = clientScope({
    ...config,
    serverUrl: resolvedServerUrl(config),
  });
  const store =
    config.store ??
    browserAccountStore(
      registry,
      config.env ?? "dev",
      config.scope?.trim() ? accountAppId(config.scope.trim()) : undefined,
    );
  return prepareAccountManager({
    appId: scope,
    registry,
    store,
    mintToken,
    fetch:
      config.serverUrl === null
        ? deviceOnlyFetch
        : sendsCookies() || config.bearer
          ? config.fetch
          : cookielessFetch,
    bearer: config.bearer,
    rememberBackendUser: options.rememberBackendUser,
  });
}

/**
 * Where a device-only client (`serverUrl: null`) says its accounts are
 * registered. Nothing is ever sent there: such a client only creates and
 * opens local-first accounts, which need no registry request.
 */
const DEVICE_ONLY_ENDPOINT = "https://device-only.invalid/jazz";

/** `serverUrl` as a browser client resolves it: `"/jazz"` when left out, `null` for device-only. */
export function resolvedServerUrl(config: { serverUrl?: string | null }): string | null {
  return config.serverUrl === null ? null : (config.serverUrl ?? DEFAULT_SERVER_URL);
}

/** @internal The account registry a browser client's accounts belong to. */
export function clientRegistryUrl(config: {
  appId?: string;
  scope?: string;
  serverUrl?: string | null;
}): string {
  const serverUrl = resolvedServerUrl(config);
  return accountRegistryUrl(
    serverUrl ?? DEVICE_ONLY_ENDPOINT,
    clientScope({ ...config, serverUrl }),
  );
}

/** @internal What sign-in requests of a device-only client get: a clear refusal. */
export const deviceOnlyFetch: typeof fetch = () =>
  Promise.reject(
    new Error(
      "Jazz runs on this device only (serverUrl: null), so it can't sign in through a backend. Set serverUrl to use loginWithBackend or linkWithBackend.",
    ),
  );

/**
 * Whether this is a browser page or worker, the hosts that send the app's
 * cookies on Jazz's sync connection. A user the backend signs in holds no
 * Jazz credential of their own: the backend's cookie is what names them on
 * every connection, so on a host without one the session would report
 * signed in and never sync. It checks for the host, not for the cookie: a
 * DOM emulation (jsdom, happy-dom) passes, and a Node process that adds a
 * session to its own fetch and WebSocket does not.
 */
function sendsCookies(): boolean {
  return (
    typeof document !== "undefined" ||
    typeof (globalThis as { WorkerGlobalScope?: unknown }).WorkerGlobalScope !== "undefined"
  );
}

/**
 * @internal What sign-in requests of a host without cookies get (Node, Deno,
 * Bun, a CLI) without a bearer: a clear refusal, naming `bearer`.
 */
export const cookielessFetch: typeof fetch = () =>
  Promise.reject(
    new Error(
      "loginWithBackend and linkWithBackend need a browser: the app's backend names the signed-in user by the cookie a browser sends on Jazz's sync connection, and this host sends none. Pass the app's token as bearer instead (on createJazzClient or createAccountManager; from jazz-tools/react-native or jazz-tools/expo on React Native). A server acts for its users through jazz-tools/backend (jazz.forReqUser).",
    ),
  );

const SELECTION_PREFIX = "jazz-account-selection-v1:";

/**
 * The key of the selection an earlier client of the app `scope` names kept on
 * this origin: alpha.59's, whose browsers reached the Jazz server directly,
 * so its registry is `<server>/apps/<scope>/accounts` with nothing before
 * `/apps`. With no such key, or several (one app reached through more than
 * one server), there is nothing to take over.
 */
function earlierSelectionKey(scope: string, env: string): string | null {
  const suffix = `:${encodeURIComponent(env)}`;
  const found: string[] = [];
  for (let index = 0; index < localStorage.length; index++) {
    const key = localStorage.key(index);
    if (!key?.startsWith(SELECTION_PREFIX) || !key.endsWith(suffix)) continue;
    try {
      const registry = decodeURIComponent(key.slice(SELECTION_PREFIX.length, -suffix.length));
      if (new URL(registry).pathname === `/apps/${scope}/accounts`) found.push(key);
    } catch {
      // Not a registry URL.
    }
  }
  return found.length === 1 ? found[0]! : null;
}

/**
 * The earlier selection's value, when this client can read it. One it can't
 * is left alone: it would refuse every start of this client, where without
 * it the client starts afresh.
 */
function earlierSelection(key: string | null): string | null {
  const value = key === null ? null : localStorage.getItem(key);
  return value !== null && readableSelection(value) ? value : null;
}

/**
 * Take a root this client let go of (a guest forgotten once linked, so the
 * device can't reopen it) out of the earlier selection too, so neither a
 * restore from it nor a rollback brings the guest back.
 */
function scrubEarlierSelection(key: string, kept: string): void {
  const earlier = earlierSelection(key);
  if (earlier === null) return;
  const roots = (JSON.parse(kept) as { roots: string[] }).roots;
  const value = JSON.parse(earlier) as { roots: string[]; selected: number | null };
  const selected = value.selected === null ? undefined : value.roots[value.selected];
  const remaining = value.roots.filter((root) => roots.includes(root));
  if (remaining.length === value.roots.length) return;
  value.roots = remaining;
  value.selected =
    selected === undefined || !remaining.includes(selected) ? null : remaining.indexOf(selected);
  localStorage.setItem(key, JSON.stringify(value));
}

/**
 * `scope`: the app id a client keeping earlier local-first users names. When
 * this registry holds no selection yet, it starts from the one those users'
 * earlier client kept, copied: the earlier key stays as it was, so that
 * client still finds it after a rollback.
 */
function browserAccountStore(registry: string, env: string, scope?: string): AccountStore {
  if (typeof localStorage === "undefined") {
    throw new Error("createAccountManager requires an AccountStore outside the browser");
  }
  if (typeof navigator === "undefined" || !navigator.locks) {
    throw new Error("The browser account store requires Web Locks; supply an atomic AccountStore");
  }
  const key = `${SELECTION_PREFIX}${encodeURIComponent(registry)}:${encodeURIComponent(env)}`;
  const earlierKey = () => (scope === undefined ? null : earlierSelectionKey(scope, env));
  const current = () => localStorage.getItem(key) ?? earlierSelection(earlierKey());
  return {
    async read() {
      return current();
    },
    async update(transform) {
      await navigator.locks.request(key, () => {
        const next = transform(current());
        localStorage.setItem(key, next);
        const earlier = earlierKey();
        if (earlier !== null && earlier !== key) scrubEarlierSelection(earlier, next);
      });
    },
  };
}
