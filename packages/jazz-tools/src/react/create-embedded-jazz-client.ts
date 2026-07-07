import { setDevtoolsBridgeConnector, createParentWindowBridgePort } from "../dev-tools/index.js";
import { createExtensionJazzClient, type JazzClient } from "./create-jazz-client.js";

// Same bridge protocol as the extension client, but the transport talks to the
// top window via postMessage (the overlay iframe case) instead of a chrome port.
export function createEmbeddedJazzClient(): Promise<JazzClient> {
  setDevtoolsBridgeConnector(createParentWindowBridgePort);
  return createExtensionJazzClient();
}
