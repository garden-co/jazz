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

function parseServerUrl(serverUrl: string): URL {
  let parsed: URL;

  try {
    parsed = new URL(serverUrl.trim());
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
