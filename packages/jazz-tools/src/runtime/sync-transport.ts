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

    this.streamAbortController = new AbortController();

    try {
      const eventsUrl = buildEventsUrl(serverUrl, this.options.getClientId(), serverPathPrefix);

      const response = await fetch(eventsUrl, {
        headers,
        signal: this.streamAbortController.signal,
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
      if (e?.name === "AbortError") return;
      console.error(`${this.logPrefix}Stream connect error:`, e);
    } finally {
      this.streamConnecting = false;
    }

    if (this.streamAbortController && !this.streamAbortController.signal.aborted) {
      this.detachServer();
      this.scheduleReconnect();
    }
  }
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
 * Catalogue payloads get the admin-secret header; everything else gets JWT.
 */
export async function sendSyncPayload(
  serverUrl: string,
  payload: any,
  auth: SyncAuth,
  logPrefix = "",
): Promise<void> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (isCataloguePayload(payload)) {
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
    response = await fetch(buildEndpointUrl(serverUrl, "/sync", auth.pathPrefix), {
      method: "POST",
      headers,
      body,
    });
  } catch (e) {
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
    response = await fetch(buildEndpointUrl(serverUrl, "/auth/link-external", auth.pathPrefix), {
      method: "POST",
      headers,
    });
  } catch (e) {
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
      try {
        const event = JSON.parse(json);
        if (event.type === "Connected" && event.client_id) {
          callbacks.onConnected?.(event.client_id);
        } else if (event.type === "SyncUpdate") {
          callbacks.onSyncMessage(JSON.stringify(event.payload));
        }
      } catch (e) {
        console.error(`${logPrefix}Stream parse error:`, e);
      }
    }
  }
}
