/**
 * Shared sync transport utilities.
 *
 * Used by both `client.ts` (main thread) and `jazz-worker.ts` (worker)
 * to avoid duplicating binary frame parsing, sync POST logic, and
 * catalogue payload detection.
 */
/** Auth and identity context for sync operations. */
export interface SyncAuth {
  jwtToken?: string;
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
  adminSecret?: string;
  clientId?: string;
  pathPrefix?: string;
}
export interface LinkExternalAuth {
  jwtToken: string;
  localAuthMode: "anonymous" | "demo";
  localAuthToken: string;
  pathPrefix?: string;
}
export interface LinkExternalResponse {
  app_id?: string;
  principal_id: string;
  issuer: string;
  subject: string;
  created: boolean;
}
/** Callbacks for stream events. */
export interface StreamCallbacks {
  onSyncMessage(payloadJson: string): void;
  onConnected?(clientId: string): void;
}
export interface SyncStreamControllerOptions {
  logPrefix?: string;
  getAuth(): Pick<SyncAuth, "jwtToken" | "localAuthMode" | "localAuthToken">;
  getClientId(): string;
  setClientId(clientId: string): void;
  onConnected(): void;
  onDisconnected(): void;
  onSyncMessage(payloadJson: string): void;
}
/**
 * Minimal runtime surface required for sync stream lifecycle wiring.
 */
export interface RuntimeSyncTarget {
  addServer(): void;
  removeServer(): void;
  onSyncMessageReceived(messageJson: string): void;
}
export interface RuntimeSyncStreamControllerOptions {
  logPrefix?: string;
  getRuntime(): RuntimeSyncTarget | null | undefined;
  getAuth(): Pick<SyncAuth, "jwtToken" | "localAuthMode" | "localAuthToken">;
  getClientId(): string;
  setClientId(clientId: string): void;
}
export declare function isExpectedFetchAbortError(error: unknown, signal?: AbortSignal): boolean;
/**
 * Shared binary-stream lifecycle (connect/reconnect/auth-refresh/teardown).
 *
 * Keeps stream state and backoff policy in one place so both main-thread and
 * worker runtimes follow the same behavior.
 */
export declare class SyncStreamController {
  private readonly options;
  private readonly logPrefix;
  private streamAbortController;
  private reconnectTimer;
  private reconnectAttempt;
  private streamConnecting;
  private streamAttached;
  private activeServerUrl;
  private activeServerPathPrefix;
  private stopped;
  constructor(options: SyncStreamControllerOptions);
  start(serverUrl: string, pathPrefix?: string): void;
  stop(): void;
  updateAuth(): void;
  notifyTransportFailure(): void;
  getServerUrl(): string | null;
  getPathPrefix(): string | undefined;
  private attachServer;
  private detachServer;
  private clearReconnectTimer;
  private abortStream;
  private scheduleReconnect;
  private connectStream;
}
/**
 * Build a stream controller bound to a runtime's server/sync hooks.
 */
export declare function createRuntimeSyncStreamController(
  options: RuntimeSyncStreamControllerOptions,
): SyncStreamController;
export interface SyncOutboxRouterOptions {
  logPrefix?: string;
  onServerPayload(payload: unknown): void | Promise<void>;
  onClientPayload?(payloadJson: string): void;
  onServerPayloadError?(error: unknown): void;
  retryServerPayloads?: boolean;
}
/**
 * Create a shared runtime outbox router for server/client destinations.
 */
export declare function createSyncOutboxRouter(
  options: SyncOutboxRouterOptions,
): (envelope: string) => void;
/**
 * Generate a UUIDv4 client ID.
 *
 * Uses `crypto.randomUUID()` when available and falls back to a
 * standards-compatible template in older environments.
 */
export declare function generateClientId(): string;
/**
 * Normalize an optional route prefix into a leading-slash path with no trailing slash.
 */
export declare function normalizePathPrefix(pathPrefix?: string): string;
/**
 * Build a server endpoint URL with optional route prefix.
 */
export declare function buildEndpointUrl(
  serverUrl: string,
  endpoint: string,
  pathPrefix?: string,
): string;
/**
 * Build the stream URL for binary events.
 */
export declare function buildEventsUrl(
  serverUrl: string,
  clientId: string,
  pathPrefix?: string,
): string;
/**
 * Apply end-user auth headers with stable precedence.
 *
 * Precedence:
 * 1. Authorization bearer token
 * 2. Local anonymous/demo token headers
 */
export declare function applyUserAuthHeaders(headers: Record<string, string>, auth: SyncAuth): void;
/**
 * Check if a sync payload is for a catalogue object (schema or lens).
 * Catalogue payloads use admin-secret auth instead of JWT.
 */
export declare function isCataloguePayload(payload: any): boolean;
/**
 * POST a sync payload to the server.
 *
 * User auth headers are always applied first (JWT or local auth).
 * Catalogue payloads additionally include the admin-secret header when available.
 */
export declare function sendSyncPayload(
  serverUrl: string,
  payload: any,
  auth: SyncAuth,
  logPrefix?: string,
): Promise<void>;
/**
 * Link a local anonymous/demo identity to an external JWT identity.
 *
 * This endpoint requires both auth forms on the same request:
 * - `Authorization: Bearer <jwt>`
 * - `X-Jazz-Local-Mode` + `X-Jazz-Local-Token`
 */
export declare function linkExternalIdentity(
  serverUrl: string,
  auth: LinkExternalAuth,
  logPrefix?: string,
): Promise<LinkExternalResponse>;
/**
 * Read length-prefixed binary frames from a ReadableStreamDefaultReader.
 *
 * Each frame is: 4-byte big-endian length + UTF-8 JSON payload.
 * Calls `callbacks.onSyncMessage` for SyncUpdate events and
 * `callbacks.onConnected` for Connected events.
 *
 * Returns when the stream ends or is aborted.
 */
export declare function readBinaryFrames(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  callbacks: StreamCallbacks,
  logPrefix?: string,
): Promise<void>;
//# sourceMappingURL=sync-transport.d.ts.map
