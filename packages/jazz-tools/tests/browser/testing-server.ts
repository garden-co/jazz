import {
  jazzServerBrowserCommands,
  jazzServerNetworkBrowserCommands,
  jazzServerTransportControlBrowserCommands,
} from "./browser-commands.js";

export interface JazzServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface JazzServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

/**
 * Start an empty authority; fixtures deploy their schema and permissions explicitly.
 * Opt into a TCP gate when the test must interrupt existing worker connections.
 * The advertised URL stays fixed across block/unblock and database reopen.
 */
export function getJazzServerInfo(appId?: string, gated = false): Promise<JazzServerInfo> {
  // Preserve the optional app ID's position during command serialization,
  // which otherwise elides undefined array entries.
  return jazzServerBrowserCommands().jazzServerInfo(appId ?? null, gated);
}

export function stopJazzServer(serverUrl: string): Promise<void> {
  return jazzServerBrowserCommands().jazzServerStop(serverUrl);
}

export function blockJazzServerNetwork(serverUrl: string): Promise<void> {
  return jazzServerNetworkBrowserCommands().jazzServerBlockNetwork(serverUrl);
}

export function unblockJazzServerNetwork(serverUrl: string): Promise<void> {
  return jazzServerNetworkBrowserCommands().jazzServerUnblockNetwork(serverUrl);
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
