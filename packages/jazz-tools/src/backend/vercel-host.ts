import type { JazzWebSocketUpgrade } from "./core-provider.js";
import type { WsLikeSocket } from "./create-jazz.js";
import { backendNative } from "./backend-native.js";
import { closingBeforeDeadline } from "./socket-deadline.js";
import { optionalPeerImportError } from "./optional-peer.js";
import { importWs } from "./ws-peer.js";

let check: Promise<boolean> | undefined;

/**
 * Whether `next()` on Vercel can upgrade sync sockets. Vercel's
 * `upgradeWebSocket()` needs `ws`: without it every upgrade would fail.
 * Checked once per process, and a missing `ws` is logged once, naming the
 * fix. Until it is installed, `next()` answers sockets with the HTTP stream
 * marker (426 + `x-jazz-stream`), so clients sync over the HTTP stream
 * instead.
 *
 * `ws` is resolved from jazz-tools, while `upgradeWebSocket()` resolves it
 * from `@vercel/functions`. Both declare it as an optional peer, so an app
 * that installs it gives it to both; a layout where only one sees it (a
 * strict pnpm one, with `ws` installed for one package alone) gets past
 * this check, and its upgrades fail in `upgradeWebSocket()` as before.
 */
function vercelUpgradesSockets(): Promise<boolean> {
  check ??= importWs("next() on Vercel").then(
    () => true,
    (error: Error) => {
      console.error(
        `jazz: ${error.message}. Without it, sync sockets can't open: clients sync over the HTTP stream instead.`,
      );
      return false;
    },
  );
  return check;
}

/**
 * Whether this process runs on Vercel's serverless hosts: no lasting disk,
 * sockets only through `upgradeWebSocket`. Only `vercel dev` (VERCEL_ENV
 * development) is a local machine; a VERCEL flag without VERCEL_ENV counts
 * as a host, the safer default for refusing local disk.
 */
export function onVercelHost(env: Record<string, string | undefined> = process.env): boolean {
  return !!env.VERCEL && env.VERCEL_ENV !== "development";
}

export async function vercelWebSocketUpgrade(handler: "fetch" | "next") {
  if (handler !== "next" || !onVercelHost()) return undefined;
  return (await vercelUpgradesSockets()) ? upgradeVercel : undefined;
}

const upgradeVercel: JazzWebSocketUpgrade = async (_request, accept, { maxPayload }) => {
  let vercelFunctions: {
    upgradeWebSocket(
      handler: (ws: WsLikeSocket) => void,
      options?: { maxPayload?: number },
    ): Response;
    getDeadline?(): Date | undefined;
  };
  try {
    vercelFunctions = (await backendNative().importPeer(
      "@vercel/functions",
    )) as unknown as typeof vercelFunctions;
  } catch (error) {
    throw optionalPeerImportError("Jazz on Vercel", "@vercel/functions", error);
  }
  const { upgradeWebSocket } = vercelFunctions;
  // The socket ends with this invocation, at the route's maxDuration:
  // close it just before, so the client reconnects without a stall.
  const deadline = vercelFunctions.getDeadline?.()?.getTime();
  return upgradeWebSocket(
    (ws) => accept(deadline === undefined ? ws : closingBeforeDeadline(ws, deadline)),
    { maxPayload },
  );
};
