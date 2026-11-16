import { optionalPeerImportError } from "./optional-peer.js";

/** What jazz-tools uses of the `ws` package. */
export interface WsPackage {
  WebSocketServer: new (options: { noServer: true; maxPayload: number }) => unknown;
}

/**
 * The `ws` package, an optional peer of jazz-tools, for `feature`; every
 * use of `ws` loads it here. The import is a literal inside `try`, like
 * `importSandboxSdk`'s: bundlers (Turbopack, webpack) then bundle `ws` when
 * it's installed, and leave a missing one to fail here, at runtime, instead
 * of every route that imports jazz-tools (`.then()` or `.catch()` on the
 * import is not enough for Turbopack: it fails the build, #5055's E5).
 */
export async function importWs(feature: string): Promise<WsPackage> {
  try {
    return (await import(/* @vite-ignore */ "ws")) as unknown as WsPackage;
  } catch (error) {
    throw optionalPeerImportError(feature, "ws", error);
  }
}

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
export function vercelUpgradesSockets(): Promise<boolean> {
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
