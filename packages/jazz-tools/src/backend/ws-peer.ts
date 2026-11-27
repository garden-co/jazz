import { optionalPeerImportError } from "./optional-peer.js";
import { backendNative } from "./backend-native.js";

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
    return (await backendNative().importPeer("ws")) as unknown as WsPackage;
  } catch (error) {
    throw optionalPeerImportError(feature, "ws", error);
  }
}
