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
  const base = serverUrl.replace(/\/+$/, "");
  const tail = `/apps/${encodeURIComponent(appId)}/ws`;

  if (base.startsWith("http://")) {
    return `ws://${base.slice("http://".length)}${tail}`;
  }
  if (base.startsWith("https://")) {
    return `wss://${base.slice("https://".length)}${tail}`;
  }
  if (base.startsWith("ws://") || base.startsWith("wss://")) {
    const noWsSuffix = base.endsWith("/ws") ? base.slice(0, -"/ws".length) : base;
    return `${noWsSuffix}${tail}`;
  }
  throw new Error(
    `Invalid server URL "${serverUrl}": expected http://, https://, ws://, or wss://`,
  );
}
