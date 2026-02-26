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

function errorMessage(error: unknown): string {
  if (error instanceof Error && typeof error.message === "string") {
    return error.message;
  }
  if (typeof error === "string") return error;
  return String(error);
}

function isExpectedStreamAbortError(error: unknown, signal: AbortSignal): boolean {
  if (signal.aborted) return true;

  if (error && typeof error === "object") {
    const maybeName = (error as { name?: unknown }).name;
    if (maybeName === "AbortError") return true;
  }

  const message = errorMessage(error).toLowerCase();
  if (message.includes("fetch request has been canceled")) return true;
  if (message.includes("fetch request has been cancelled")) return true;
  if (message.includes("the operation was aborted")) return true;

  const cause = (error as { cause?: unknown } | null)?.cause;
  if (cause !== undefined) {
    const causeMessage = errorMessage(cause).toLowerCase();
    if (causeMessage.includes("fetch request has been canceled")) return true;
    if (causeMessage.includes("fetch request has been cancelled")) return true;
    if (causeMessage.includes("the operation was aborted")) return true;
  }

  return false;
}

/**
 * Shared binary-stream lifecycle (connect/reconnect/auth-refresh/teardown).
 *
 * Keeps stream state and backoff policy in one place so both main-thread and
 * worker runtimes follow the same behavior.
 */
export class SyncStreamController {
  private readonly logPrefix: string;
  private streamAbortController: AbortController | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  private streamConnecting = false;
  private streamAttached = false;
  private activeServerUrl: string | null = null;
  private activeServerPathPrefix: string | undefined;
  private stopped = true;

  constructor(private readonly options: SyncStreamControllerOptions) {
    this.logPrefix = options.logPrefix ?? "";
  }

  start(serverUrl: string, pathPrefix?: string): void {
    this.stop();
    this.stopped = false;
    this.activeServerUrl = serverUrl;
    this.activeServerPathPrefix = pathPrefix;
    this.connectStream();
  }

  stop(): void {
    this.stopped = true;
    this.activeServerUrl = null;
    this.activeServerPathPrefix = undefined;
    this.clearReconnectTimer();
    this.abortStream();
    this.detachServer();
  }

  updateAuth(): void {
    this.abortStream();
    this.detachServer();
    if (this.activeServerUrl && !this.stopped) {
      this.scheduleReconnect();
    }
  }

  notifyTransportFailure(): void {
    this.detachServer();
    this.scheduleReconnect();
  }

  getServerUrl(): string | null {
    return this.activeServerUrl;
  }

  getPathPrefix(): string | undefined {
    return this.activeServerPathPrefix;
  }

  private attachServer(): void {
    if (this.streamAttached) {
      this.options.onDisconnected();
    }
    this.options.onConnected();
    this.streamAttached = true;
    this.reconnectAttempt = 0;
  }

  private detachServer(): void {
    if (!this.streamAttached) return;
    this.options.onDisconnected();
    this.streamAttached = false;
  }

  private clearReconnectTimer(): void {
    if (!this.reconnectTimer) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private abortStream(): void {
    if (!this.streamAbortController) return;
    this.streamAbortController.abort();
    this.streamAbortController = null;
  }

  private scheduleReconnect(): void {
    if (this.stopped || !this.activeServerUrl) return;
    if (this.reconnectTimer) return;

    const baseMs = 300;
    const maxMs = 10_000;
    const jitterMs = Math.floor(Math.random() * 200);
    const delayMs = Math.min(maxMs, baseMs * 2 ** this.reconnectAttempt) + jitterMs;
    this.reconnectAttempt += 1;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectStream();
    }, delayMs);
  }

  private async connectStream(): Promise<void> {
    if (this.streamConnecting || this.stopped || !this.activeServerUrl) return;
    this.streamConnecting = true;

    const serverUrl = this.activeServerUrl;
    const serverPathPrefix = this.activeServerPathPrefix;
    const headers: Record<string, string> = {
      Accept: "application/octet-stream",
    };
    applyUserAuthHeaders(headers, this.options.getAuth());

    const abortController = new AbortController();
    this.streamAbortController = abortController;

    try {
      const eventsUrl = buildEventsUrl(serverUrl, this.options.getClientId(), serverPathPrefix);

      const response = await fetch(eventsUrl, {
        headers,
        signal: abortController.signal,
      });

      if (!response.ok) {
        console.error(`${this.logPrefix}Stream connect failed: ${response.status}`);
        this.detachServer();
        this.streamConnecting = false;
        this.scheduleReconnect();
        return;
      }

      if (!response.body) {
        throw new Error("Stream response did not include a body");
      }

      const reader = response.body.getReader();
      let connected = false;
      await readBinaryFrames(
        reader,
        {
          onSyncMessage: this.options.onSyncMessage,
          onConnected: (clientId) => {
            this.options.setClientId(clientId);
            if (!connected) {
              connected = true;
              this.attachServer();
            }
          },
        },
        this.logPrefix,
      );
    } catch (e: any) {
      if (isExpectedStreamAbortError(e, abortController.signal)) return;
      console.error(`${this.logPrefix}Stream connect error:`, e);
    } finally {
      if (this.streamAbortController === abortController) {
        this.streamAbortController = null;
      }
      this.streamConnecting = false;
    }

    if (!abortController.signal.aborted && !this.stopped) {
      this.detachServer();
      this.scheduleReconnect();
    }
  }
}

/**
 * Build a stream controller bound to a runtime's server/sync hooks.
 */
export function createRuntimeSyncStreamController(
  options: RuntimeSyncStreamControllerOptions,
): SyncStreamController {
  return new SyncStreamController({
    logPrefix: options.logPrefix,
    getAuth: options.getAuth,
    getClientId: options.getClientId,
    setClientId: options.setClientId,
    onConnected: () => options.getRuntime()?.addServer(),
    onDisconnected: () => options.getRuntime()?.removeServer(),
    onSyncMessage: (json) => options.getRuntime()?.onSyncMessageReceived(json),
  });
}

export interface SyncOutboxRouterOptions {
  logPrefix?: string;
  onServerPayload(payload: unknown): void | Promise<void>;
  onClientPayload?(payloadJson: string): void;
  onServerPayloadError?(error: unknown): void;
}

/**
 * Create a shared runtime outbox router for server/client destinations.
 */
export function createSyncOutboxRouter(
  options: SyncOutboxRouterOptions,
): (envelope: string) => void {
  const logPrefix = options.logPrefix ?? "";

  return (envelope: string) => {
    let parsed: { destination?: unknown; payload?: unknown };
    try {
      parsed = JSON.parse(envelope) as { destination?: unknown; payload?: unknown };
    } catch (error) {
      console.error(`${logPrefix}Sync envelope parse error:`, error);
      return;
    }

    const destination = parsed.destination;
    const payload = parsed.payload;
    const isObjectDestination = destination !== null && typeof destination === "object";

    if (isObjectDestination && "Client" in destination) {
      const payloadJson = JSON.stringify(payload);
      if (payloadJson !== undefined) {
        options.onClientPayload?.(payloadJson);
      }
      return;
    }

    if (isObjectDestination && "Server" in destination) {
      Promise.resolve(options.onServerPayload(payload)).catch((error) => {
        if (options.onServerPayloadError) {
          options.onServerPayloadError(error);
          return;
        }
        console.error(`${logPrefix}Sync POST error:`, error);
      });
    }
  };
}

/**
 * Generate a UUIDv4 client ID.
 *
 * Uses `crypto.randomUUID()` when available and falls back to a
 * standards-compatible template in older environments.
 */
export function generateClientId(): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }

  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = Math.floor(Math.random() * 16);
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

const fallbackClientId = generateClientId();
const SYNC_FETCH_TIMEOUT_MS = 10_000;

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number,
): Promise<Response> {
  if (typeof AbortController !== "function") {
    return fetch(url, init);
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

function trimTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

/**
 * Normalize an optional route prefix into a leading-slash path with no trailing slash.
 */
export function normalizePathPrefix(pathPrefix?: string): string {
  if (!pathPrefix) return "";
  const trimmed = pathPrefix.trim();
  if (!trimmed) return "";
  const withoutTrailing = trimmed.replace(/\/+$/, "");
  return withoutTrailing.startsWith("/") ? withoutTrailing : `/${withoutTrailing}`;
}

/**
 * Build a server endpoint URL with optional route prefix.
 */
export function buildEndpointUrl(serverUrl: string, endpoint: string, pathPrefix?: string): string {
  const normalizedEndpoint = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
  return `${trimTrailingSlash(serverUrl)}${normalizePathPrefix(pathPrefix)}${normalizedEndpoint}`;
}

/**
 * Build the stream URL for binary events.
 */
export function buildEventsUrl(serverUrl: string, clientId: string, pathPrefix?: string): string {
  return `${buildEndpointUrl(serverUrl, "/events", pathPrefix)}?client_id=${encodeURIComponent(clientId)}`;
}

/**
 * Apply end-user auth headers with stable precedence.
 *
 * Precedence:
 * 1. Authorization bearer token
 * 2. Local anonymous/demo token headers
 */
export function applyUserAuthHeaders(headers: Record<string, string>, auth: SyncAuth): void {
  if (auth.jwtToken) {
    headers["Authorization"] = `Bearer ${auth.jwtToken}`;
    return;
  }

  if (auth.localAuthMode && auth.localAuthToken) {
    headers["X-Jazz-Local-Mode"] = auth.localAuthMode;
    headers["X-Jazz-Local-Token"] = auth.localAuthToken;
  }
}

/**
 * Check if a sync payload is for a catalogue object (schema or lens).
 * Catalogue payloads use admin-secret auth instead of JWT.
 */
export function isCataloguePayload(payload: any): boolean {
  const metadata = payload?.ObjectUpdated?.metadata?.metadata;
  if (metadata) {
    const t = metadata["type"];
    return t === "catalogue_schema" || t === "catalogue_lens";
  }
  return false;
}

/**
 * POST a sync payload to the server.
 *
 * User auth headers are always applied first (JWT or local auth).
 * Catalogue payloads additionally include the admin-secret header when available.
 */
export async function sendSyncPayload(
  serverUrl: string,
  payload: any,
  auth: SyncAuth,
  logPrefix = "",
): Promise<void> {
  const cataloguePayload = isCataloguePayload(payload);
  if (cataloguePayload && !auth.adminSecret) {
    return;
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (cataloguePayload) {
    if (auth.adminSecret) {
      headers["X-Jazz-Admin-Secret"] = auth.adminSecret;
    }
  } else {
    applyUserAuthHeaders(headers, auth);
  }

  const body = JSON.stringify({
    payload,
    client_id: auth.clientId ?? fallbackClientId,
  });

  let response: Response;
  try {
    response = await fetchWithTimeout(
      buildEndpointUrl(serverUrl, "/sync", auth.pathPrefix),
      {
        method: "POST",
        headers,
        body,
      },
      SYNC_FETCH_TIMEOUT_MS,
    );
  } catch (e) {
    if ((e as { name?: string })?.name === "AbortError") {
      console.error(`${logPrefix}Sync POST timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
      throw new Error(`${logPrefix}Sync POST failed: timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
    }
    console.error(`${logPrefix}Sync POST fetch error:`, e);
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`${logPrefix}Sync POST failed: ${msg}`);
  }

  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : "";
    throw new Error(`${logPrefix}Sync POST failed: ${response.status}${statusText}`);
  }
}

/**
 * Link a local anonymous/demo identity to an external JWT identity.
 *
 * This endpoint requires both auth forms on the same request:
 * - `Authorization: Bearer <jwt>`
 * - `X-Jazz-Local-Mode` + `X-Jazz-Local-Token`
 */
export async function linkExternalIdentity(
  serverUrl: string,
  auth: LinkExternalAuth,
  logPrefix = "",
): Promise<LinkExternalResponse> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${auth.jwtToken}`,
    "X-Jazz-Local-Mode": auth.localAuthMode,
    "X-Jazz-Local-Token": auth.localAuthToken,
  };

  let response: Response;
  try {
    response = await fetchWithTimeout(
      buildEndpointUrl(serverUrl, "/auth/link-external", auth.pathPrefix),
      {
        method: "POST",
        headers,
      },
      SYNC_FETCH_TIMEOUT_MS,
    );
  } catch (e) {
    if ((e as { name?: string })?.name === "AbortError") {
      console.error(`${logPrefix}Link external timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
      throw new Error(`${logPrefix}Link external failed: timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
    }
    console.error(`${logPrefix}Link external fetch error:`, e);
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`${logPrefix}Link external failed: ${msg}`);
  }

  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : "";
    const body = await response.text().catch(() => "");
    const bodySuffix = body ? `: ${body}` : "";
    throw new Error(
      `${logPrefix}Link external failed: ${response.status}${statusText}${bodySuffix}`,
    );
  }

  return (await response.json()) as LinkExternalResponse;
}

/**
 * Read length-prefixed binary frames from a ReadableStreamDefaultReader.
 *
 * Each frame is: 4-byte big-endian length + UTF-8 JSON payload.
 * Calls `callbacks.onSyncMessage` for SyncUpdate events and
 * `callbacks.onConnected` for Connected events.
 *
 * Returns when the stream ends or is aborted.
 */
export async function readBinaryFrames(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  callbacks: StreamCallbacks,
  logPrefix = "",
): Promise<void> {
  let buffer = new Uint8Array(0);

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    // Append chunk to buffer
    const newBuffer = new Uint8Array(buffer.length + value.length);
    newBuffer.set(buffer);
    newBuffer.set(value, buffer.length);
    buffer = newBuffer;

    // Read complete frames
    while (buffer.length >= 4) {
      const len = new DataView(buffer.buffer, buffer.byteOffset).getUint32(0, false);
      if (buffer.length < 4 + len) break;
      const json = new TextDecoder().decode(buffer.slice(4, 4 + len));
      buffer = buffer.slice(4 + len);

      let event: any;
      try {
        event = JSON.parse(json);
      } catch (error) {
        console.error(`${logPrefix}Stream parse error:`, error);
        continue;
      }

      try {
        if (event.type === "Connected" && event.client_id) {
          callbacks.onConnected?.(event.client_id);
        } else if (event.type === "SyncUpdate") {
          callbacks.onSyncMessage(JSON.stringify(event.payload));
        }
      } catch (error) {
        console.error(`${logPrefix}Stream callback error:`, error);
      }
    }
  }
}
