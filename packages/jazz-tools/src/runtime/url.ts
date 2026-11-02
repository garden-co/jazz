import { fetchFor } from "./in-process-transport.js";
import { PlatformURL } from "./platform-url.js";
/**
 * Build an app-scoped URL under `/apps/<appId>`.
 *
 * Preserves any base path already present in `serverUrl`, trims surrounding
 * whitespace, rejects query/hash fragments, and accepts path inputs with or
 * without a leading slash.
 */
export function appScopedUrl(serverUrl: string, appId: string, path: string): string {
  const base = normalizeServerUrlBase(serverUrl);
  const normalizedPath = path.trim().replace(/^\/+/, "");
  const appBase = `${base}/apps/${encodeURIComponent(appId)}`;

  return normalizedPath ? `${appBase}/${normalizedPath}` : appBase;
}

/**
 * GET `url` with the admin secret and return its JSON body. A non-OK response
 * throws `<label> fetch failed: <status> <statusText>[ - <body>]<guidance>`.
 */
export async function fetchAdminJson(
  url: string,
  adminSecret: string,
  label: string,
  guidance?: (status: number) => string,
): Promise<unknown> {
  const response = await fetchFor(url)(url, {
    method: "GET",
    headers: { "X-Jazz-Admin-Secret": adminSecret },
  });
  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `${label} fetch failed: ${response.status} ${response.statusText}${detail}${guidance?.(response.status) ?? ""}`,
    );
  }
  return response.json();
}

/**
 * Convert an HTTP(S) server URL to the WebSocket `/ws` endpoint URL.
 *
 * Mirrors the Rust `http_url_to_ws` helper in `crates/jazz-tools/src/client.rs`.
 *
 * - `http://host`, `xyz` → `ws://host/apps/xyz/ws`
 * - `https://host`, `xyz` → `wss://host/apps/xyz/ws`
 * - `ws://host`, `xyz` → `ws://host/apps/xyz/ws`
 * - `ws://host/ws`, `xyz` → `ws://host/apps/xyz/ws`
 */
export function httpUrlToWs(serverUrl: string, appId: string): string {
  const parsed = parseServerUrl(serverUrl);

  if (parsed.protocol === "http:") {
    parsed.protocol = "ws:";
    return appScopedUrl(parsed.toString(), appId, "ws");
  }

  if (parsed.protocol === "https:") {
    parsed.protocol = "wss:";
    return appScopedUrl(parsed.toString(), appId, "ws");
  }

  parsed.pathname = parsed.pathname.replace(/\/ws\/?$/, "");
  return appScopedUrl(parsed.toString(), appId, "ws");
}

const ALLOWED_SERVER_URL_PROTOCOLS = new Set(["http:", "https:", "ws:", "wss:"]);

function normalizeServerUrlBase(serverUrl: string): string {
  const parsed = parseServerUrl(serverUrl);
  parsed.pathname = parsed.pathname.replace(/\/+$/, "");
  return parsed.toString().replace(/\/+$/, "");
}

/**
 * Resolve a path-only server URL such as `/jazz` against the page's origin.
 * A backend that mounts Jazz serves it on the app's own origin, so browser
 * clients name it by path. Outside a page, a path stays as given (and is
 * rejected as an invalid URL).
 */
export function resolveServerUrl(serverUrl: string): string {
  if (!serverUrl.startsWith("/") || serverUrl.startsWith("//")) return serverUrl;
  const location = (globalThis as { location?: { href?: string } }).location;
  return location?.href ? new PlatformURL(serverUrl, location.href).href : serverUrl;
}

function parseServerUrl(serverUrl: string): URL {
  let parsed: URL;

  try {
    parsed = new PlatformURL(resolveServerUrl(serverUrl.trim()));
  } catch {
    throw invalidServerUrl(serverUrl);
  }

  if (!ALLOWED_SERVER_URL_PROTOCOLS.has(parsed.protocol)) {
    throw invalidServerUrl(serverUrl);
  }

  if (parsed.search || parsed.hash) {
    throw new Error(
      `Invalid server URL "${serverUrl}": must not include query parameters or a hash fragment`,
    );
  }

  return parsed;
}

function invalidServerUrl(serverUrl: string): Error {
  return new Error(
    `Invalid server URL "${serverUrl}": expected http://, https://, ws://, or wss://`,
  );
}
