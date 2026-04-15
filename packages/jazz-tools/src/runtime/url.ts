/**
 * Convert an HTTP(S) server URL to the WebSocket `/ws` endpoint URL.
 *
 * Mirrors the Rust `http_url_to_ws` helper in `crates/jazz-tools/src/client.rs`.
 *
 * - `http://host`              → `ws://host/ws`
 * - `https://host`             → `wss://host/ws`
 * - `http://host`, `/apps/xyz` → `ws://host/apps/xyz/ws`
 * - `ws://host`                → `ws://host/ws`
 * - `ws://host/ws`             → unchanged
 */
export function httpUrlToWs(serverUrl: string, pathPrefix?: string): string {
  const base = serverUrl.replace(/\/+$/, "");
  const prefix = (pathPrefix ?? "").replace(/^\/+|\/+$/g, "");
  const tail = prefix.length > 0 ? `/${prefix}/ws` : "/ws";

  if (base.startsWith("http://")) {
    return `ws://${base.slice("http://".length)}${tail}`;
  }
  if (base.startsWith("https://")) {
    return `wss://${base.slice("https://".length)}${tail}`;
  }
  if (base.startsWith("ws://") || base.startsWith("wss://")) {
    // If prefix is given, append it; otherwise preserve the existing behavior:
    // idempotent /ws suffix.
    if (prefix.length > 0) {
      const noWsSuffix = base.endsWith("/ws") ? base.slice(0, -"/ws".length) : base;
      return `${noWsSuffix}${tail}`;
    }
    return base.endsWith("/ws") ? base : `${base}/ws`;
  }
  throw new Error(
    `Invalid server URL "${serverUrl}": expected http://, https://, ws://, or wss://`,
  );
}
