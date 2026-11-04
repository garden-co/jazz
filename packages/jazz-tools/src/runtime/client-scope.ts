import { PlatformURL } from "./platform-url.js";
import { resolveServerUrl } from "./url.js";

/**
 * What a browser client's local data, local-first secret, identity proofs and
 * founder accounts are scoped by: the endpoint it syncs with.
 *
 * A browser client names no app. Its data belongs to whichever app serves its
 * `serverUrl` (by default the app's own backend at `/jazz`), so the resolved
 * endpoint URL is its scope:
 *
 * - `appId`, where given (backend and test code), is the scope, as before.
 * - `scope` carries the app id an existing local-first app's users created
 *   their secrets and accounts under, so they keep them. New apps leave it out.
 * - Otherwise the endpoint URL, resolved against the page, with `ws(s)` read
 *   as `http(s)` and no trailing slash: `https://app.example.com/jazz`.
 * - A client with no server at all is scoped `"local"`.
 *
 * The endpoint's backend checks a guest's proof against its own endpoint URL
 * and vouches for it to the core, which never needs to know the endpoint.
 */
export function clientScope(config: {
  appId?: string;
  scope?: string;
  serverUrl?: string | null;
}): string {
  const explicit = config.appId?.trim() || config.scope?.trim();
  if (explicit) return explicit;
  if (!config.serverUrl) return LOCAL_CLIENT_SCOPE;
  return endpointScope(config.serverUrl);
}

/** Where a browser client syncs when it names no `serverUrl`: its own backend. */
export const DEFAULT_SERVER_URL = "/jazz";

/** The scope of a client that syncs with no server. */
export const LOCAL_CLIENT_SCOPE = "local";

/** The endpoint URL `serverUrl` names, in the form `clientScope` uses. */
export function endpointScope(serverUrl: string): string {
  let url: URL;
  try {
    url = new PlatformURL(resolveServerUrl(serverUrl.trim()));
  } catch {
    throw new Error(
      `Invalid server URL "${serverUrl}": expected http://, https://, ws://, or wss://`,
    );
  }
  if (url.protocol === "ws:") url.protocol = "http:";
  if (url.protocol === "wss:") url.protocol = "https:";
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error(
      `Invalid server URL "${serverUrl}": expected http://, https://, ws://, or wss://`,
    );
  }
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
}
