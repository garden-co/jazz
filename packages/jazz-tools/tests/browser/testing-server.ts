import {
  jazzServerBrowserCommands,
  jazzServerTransportControlBrowserCommands,
} from "./browser-commands.js";

export interface JazzServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export function getJazzServerInfo(appId?: string): Promise<JazzServerInfo> {
  return jazzServerBrowserCommands().jazzServerInfo(appId);
}

export function stopJazzServer(serverUrl: string): Promise<void> {
  return jazzServerBrowserCommands().jazzServerStop(serverUrl);
}

export async function getJazzServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
  appId?: string,
): Promise<string> {
  // Browser-command argument serialization elides `undefined` array entries.
  // Keep the optional `appId` in its third position and preserve the test
  // issuer's documented default claims rather than accidentally signing the
  // app ID as a scalar `claims` value.
  return jazzServerBrowserCommands().jazzServerJwtForUser(
    userId,
    claims ?? { role: "user" },
    appId,
  );
}

/**
 * Browser bridge to the shared TransportControl: buffer delivery without
 * disconnecting, using the same block/blockInbound/unblock operations as Rust.
 */
export interface JazzServerTransportControl {
  url: string;
  block(): Promise<void>;
  blockInbound(): Promise<void>;
  unblock(): Promise<void>;
  stop(): Promise<void>;
}

export async function createJazzServerTransportControl(
  serverUrl: string,
): Promise<JazzServerTransportControl> {
  const commands = jazzServerTransportControlBrowserCommands();
  const url = await commands.jazzServerTransportControlCreate(serverUrl);
  return {
    url,
    block: () => commands.jazzServerTransportControlBlock(url, "both"),
    blockInbound: () => commands.jazzServerTransportControlBlock(url, "inbound"),
    unblock: () => commands.jazzServerTransportControlUnblock(url),
    stop: () => commands.jazzServerTransportControlStop(url),
  };
}
