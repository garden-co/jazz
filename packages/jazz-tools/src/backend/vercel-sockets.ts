import { optionalPeerImportError } from "./optional-peer.js";

let check: Promise<boolean> | undefined;

/**
 * Whether `next()` on Vercel can upgrade sync sockets. Vercel's
 * `upgradeWebSocket()` needs the `ws` package, an optional peer of
 * jazz-tools: without it every upgrade would fail. Checked once per
 * process, and a missing `ws` is logged once, naming the fix. Until it is
 * installed, `next()` answers sockets with the HTTP stream marker (426 +
 * `x-jazz-stream`), so clients sync over the HTTP stream instead.
 */
export function vercelUpgradesSockets(): Promise<boolean> {
  check ??= import(/* @vite-ignore */ "ws").then(
    () => true,
    (error: unknown) => {
      console.error(
        `jazz: ${optionalPeerImportError("next() on Vercel", "ws", error).message}. Without it, sync sockets can't open: clients sync over the HTTP stream instead.`,
      );
      return false;
    },
  );
  return check;
}
