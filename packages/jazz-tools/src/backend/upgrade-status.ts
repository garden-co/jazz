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
 * carrying `headers`; undefined if that one goes through (the failure
 * was transient) or gets no answer.
 */
export function describeFailedUpgrade(
  url: string,
  headers: Record<string, string> = {},
): Promise<string | undefined> {
  const target = new URL(url.replace(/^ws/, "http"));
  const request = target.protocol === "https:" ? httpsRequest : httpRequest;
  return new Promise((resolve) => {
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
