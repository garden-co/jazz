// Fix 2 (prototype): open the WebSocket at once on a page.
// StreamFallbackSocket first asks IndexedDB whether this backend is known to
// need the HTTP stream (open DB + read) and only then creates the socket:
// ~7-10 ms on every load, serial before the WebSocket handshake. Where Web
// Storage exists (a page, not a worker), keep a synchronous mirror of that
// memory in localStorage and open the native socket at once when it has no
// entry. (A worker keeps using IndexedDB.)
export function apply({ edit }) {
  edit(
    "runtime/native-runtime/http-stream-socket.js",
    `            // Another page or worker may have learned that this backend can't
            // upgrade sockets: then a socket here would only fail, loudly.
            void recallStreamEndpoint(url)`,
    `            // thin-startup fix 2: a page answers from its localStorage mirror.
            let mirror;
            try { mirror = globalThis.localStorage?.getItem("jazz-stream-endpoint:" + url); } catch {}
            if (mirror === null) {
                this.inner = this.openNative();
                return;
            }
            // Another page or worker may have learned that this backend can't
            // upgrade sockets: then a socket here would only fail, loudly.
            void recallStreamEndpoint(url)`,
  );
  edit(
    "runtime/native-runtime/stream-endpoint-memory.js",
    `export function rememberStreamEndpoint(url, now = Date.now()) {`,
    `export function rememberStreamEndpoint(url, now = Date.now()) {
    try { globalThis.localStorage?.setItem("jazz-stream-endpoint:" + url, String(now + STREAM_ENDPOINT_MEMORY_MS)); } catch {}`,
  );
  edit(
    "runtime/native-runtime/stream-endpoint-memory.js",
    `export function forgetStreamEndpoint(url) {`,
    `export function forgetStreamEndpoint(url) {
    try { globalThis.localStorage?.removeItem("jazz-stream-endpoint:" + url); } catch {}`,
  );
}
