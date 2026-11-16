import { randomBytes } from "node:crypto";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";

/** How long the status request waits for an answer. */
const UPGRADE_STATUS_TIMEOUT_MS = 10_000;

/**
 * Why a sync socket to `url` failed before opening, as its upgrade's HTTP
 * status: Node's WebSocket reports a refused upgrade with no detail, so a
 * backend that answers it with a 500 (on Vercel without the `ws` package,
 * say) looks like a network error. Asks with one more upgrade request,
 * carrying `headers`, only after a socket's upgrade was answered (see
 * `HttpStreamFallbackOptions.describeFailedUpgrade`).
 *
 * If this upgrade goes through (the failure was transient), it is a real
 * socket the backend accepts: it is destroyed at once, before any prelude,
 * and the result is undefined. That costs the backend one short-lived
 * connection (on Vercel, an invocation), once per failed socket.
 *
 * Uses Node's default agent, not the WebSocket's own: a backend client's
 * WebSocket is Node's global one, which uses the default network settings
 * too. Undefined as well when there's no answer, or `signal` aborts.
 */
export function describeFailedUpgrade(
  url: string,
  headers: Record<string, string>,
  signal: AbortSignal,
): Promise<string | undefined> {
  const target = new URL(url.replace(/^ws/, "http"));
  const request = target.protocol === "https:" ? httpsRequest : httpRequest;
  return new Promise((resolve) => {
    if (signal.aborted) return resolve(undefined);
    const req = request(target, {
      method: "GET",
      headers: {
        ...headers,
        connection: "Upgrade",
        upgrade: "websocket",
        "sec-websocket-version": "13",
        "sec-websocket-key": randomBytes(16).toString("base64"),
      },
      timeout: UPGRADE_STATUS_TIMEOUT_MS,
      signal,
    });
    req.on("upgrade", (_response, socket) => {
      socket.destroy();
      resolve(undefined);
    });
    req.on("response", (response) => {
      response.resume();
      const status = `${response.statusCode} ${response.statusMessage ?? ""}`.trim();
      resolve(`the WebSocket upgrade to ${url} was answered with HTTP ${status}`);
    });
    req.on("timeout", () => req.destroy());
    req.on("error", () => resolve(undefined));
    req.end();
  });
}
