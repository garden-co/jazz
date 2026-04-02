/**
 * Shared sync transport utilities.
 *
 * Used by both `client.ts` (main thread) and `jazz-worker.ts` (worker)
 * to avoid duplicating binary frame parsing, sync POST logic, and
 * catalogue payload detection.
 */

import { fetchWithTimeout } from "./utils.js";

/** Auth and identity context for sync operations. */
export interface SyncAuth {
  jwtToken?: string;
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
  backendSecret?: string;
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
  onSyncMessage(payload: Uint8Array | string): void;
  onConnected?(clientId: string, catalogueStateHash?: string | null): void;
}

export interface SyncStreamControllerOptions {
  logPrefix?: string;
  getAuth(): Pick<SyncAuth, "jwtToken" | "localAuthMode" | "localAuthToken" | "backendSecret">;
  getClientId(): string;
  setClientId(clientId: string): void;
  onConnected(catalogueStateHash?: string | null): void;
  onDisconnected(): void;
  onSyncMessage(payload: Uint8Array | string): void;
}

/**
 * Minimal runtime surface required for sync stream lifecycle wiring.
 */
export interface RuntimeSyncTarget {
  addServer(serverCatalogueStateHash?: string | null): void;
  removeServer(): void;
  onSyncMessageReceived(payload: Uint8Array | string): void;
}

export interface RuntimeSyncStreamControllerOptions {
  logPrefix?: string;
  getRuntime(): RuntimeSyncTarget | null | undefined;
  getAuth(): Pick<SyncAuth, "jwtToken" | "localAuthMode" | "localAuthToken" | "backendSecret">;
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

export function isExpectedFetchAbortError(error: unknown, signal?: AbortSignal): boolean {
  if (signal?.aborted) return true;

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

function logSchemaWarningPayload(payload: any, logPrefix = ""): void {
  const warning = payload?.SchemaWarning;
  if (!warning) return;

  const rowCount = warning.rowCount ?? warning.row_count ?? 0;
  const tableName = warning.tableName ?? warning.table_name ?? "unknown";
  const fromHash = warning.fromHash ?? warning.from_hash ?? "unknown";
  const toHash = warning.toHash ?? warning.to_hash ?? "unknown";
  const shortHash = (hash: string) =>
    typeof hash === "string" && /^[0-9a-f]{12,}$/i.test(hash) ? hash.slice(0, 12) : hash;

  console.warn(
    `${logPrefix}Detected ${rowCount} rows of ${tableName} with differing schema versions. ` +
      `To ensure data visibility and forward/backward compatibility please create a new migration with ` +
      `\`npx jazz-tools migrations create ${shortHash(fromHash)} ${shortHash(toHash)}\``,
  );
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
    this.abortStream();
    this.detachServer();
    this.scheduleReconnect();
  }

  getServerUrl(): string | null {
    return this.activeServerUrl;
  }

  getPathPrefix(): string | undefined {
    return this.activeServerPathPrefix;
  }

  private attachServer(catalogueStateHash?: string | null): void {
    if (this.streamAttached) {
      this.options.onDisconnected();
    }
    this.options.onConnected(catalogueStateHash);
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
    applySyncAuthHeaders(headers, this.options.getAuth());

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
          onConnected: (clientId, catalogueStateHash) => {
            this.options.setClientId(clientId);
            if (!connected) {
              connected = true;
              this.attachServer(catalogueStateHash);
            }
          },
        },
        this.logPrefix,
      );
    } catch (e: any) {
      if (isExpectedFetchAbortError(e, abortController.signal)) return;
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
    onConnected: (catalogueStateHash) => options.getRuntime()?.addServer(catalogueStateHash),
    onDisconnected: () => options.getRuntime()?.removeServer(),
    onSyncMessage: (payload) => options.getRuntime()?.onSyncMessageReceived(payload),
  });
}

export interface SyncOutboxRouterOptions {
  logPrefix?: string;
  onServerPayload(payload: Uint8Array | string, isCatalogue: boolean): void | Promise<void>;
  onClientPayload?(payload: Uint8Array): void;
  onServerPayloadError?(error: unknown): void;
  retryServerPayloads?: boolean;
}

export type OutboxDestinationKind = "server" | "client";
export type RuntimeSyncOutboxCallbackArgs =
  | [
      destinationKind: OutboxDestinationKind,
      destinationId: string,
      payload: Uint8Array | string,
      isCatalogue: boolean,
    ]
  | [
      err: unknown,
      destinationKind: OutboxDestinationKind,
      destinationId: string,
      payload: Uint8Array | string,
      isCatalogue: boolean,
    ]
  | [
      err: unknown,
      message: [
        destinationKind: OutboxDestinationKind,
        destinationId: string,
        payload: Uint8Array | string,
        isCatalogue: boolean,
      ],
    ];
export type RuntimeSyncOutboxCallback = (...args: RuntimeSyncOutboxCallbackArgs) => void;

function isOutboxDestinationKind(value: unknown): value is OutboxDestinationKind {
  return value === "server" || value === "client";
}

function isOutboxPayload(value: unknown): value is Uint8Array | string {
  return typeof value === "string" || value instanceof Uint8Array;
}

function normalizeOutboxCallbackArgs(args: unknown[]): {
  destinationKind: OutboxDestinationKind;
  payload: Uint8Array | string;
  isCatalogue: boolean;
} | null {
  // WASM/RN-style callback: (destinationKind, destinationId, payloadJson, isCatalogue)
  if (isOutboxDestinationKind(args[0])) {
    const payload = args[2];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[0],
      payload: payload,
      isCatalogue: Boolean(args[3]),
    };
  }

  // NAPI callee-handled callback: (err, destinationKind, destinationId, payloadJson, isCatalogue)
  if (isOutboxDestinationKind(args[1])) {
    const payload = args[3];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[1],
      payload: payload,
      isCatalogue: Boolean(args[4]),
    };
  }

  // Real NAPI callback: (err, [destinationKind, destinationId, payloadJson, isCatalogue])
  if (Array.isArray(args[1]) && isOutboxDestinationKind(args[1][0])) {
    const payload = args[1][2];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[1][0],
      payload,
      isCatalogue: Boolean(args[1][3]),
    };
  }

  return null;
}

/**
 * Create a shared runtime outbox router for server/client destinations.
 */
export function createSyncOutboxRouter(
  options: SyncOutboxRouterOptions,
): RuntimeSyncOutboxCallback {
  const logPrefix = options.logPrefix ?? "";

  return (...args: RuntimeSyncOutboxCallbackArgs) => {
    const normalized = normalizeOutboxCallbackArgs(args);
    if (!normalized) {
      console.error(`${logPrefix}Invalid sync outbox callback arguments`, args);
      return;
    }

    const { destinationKind, payload, isCatalogue } = normalized;
    if (destinationKind === "client") {
      options.onClientPayload?.(payload as Uint8Array);
      return;
    }

    Promise.resolve(options.onServerPayload(payload, isCatalogue)).catch((error) => {
      if (options.onServerPayloadError) {
        options.onServerPayloadError(error);
        return;
      }
      console.error(`${logPrefix}Sync POST error:`, error);
    });
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
 * Apply runtime sync auth headers.
 *
 * Precedence:
 * 1. Backend privileged auth (`X-Jazz-Backend-Secret`)
 * 2. End-user auth (JWT/local)
 */
export function applySyncAuthHeaders(headers: Record<string, string>, auth: SyncAuth): void {
  if (auth.backendSecret) {
    headers["X-Jazz-Backend-Secret"] = auth.backendSecret;
    return;
  }

  applyUserAuthHeaders(headers, auth);
}

async function postSyncBatch(
  url: string,
  headers: Record<string, string>,
  body: BodyInit,
  logPrefix: string,
): Promise<void> {
  let response: Response;
  try {
    response = await fetchWithTimeout(
      url,
      { method: "POST", headers, body },
      SYNC_FETCH_TIMEOUT_MS,
    );
  } catch (e) {
    if ((e as { name?: string })?.name === "AbortError") {
      console.error(`${logPrefix}Sync POST timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
      throw new Error(`${logPrefix}Sync POST failed: timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
    }
    if (isExpectedFetchAbortError(e)) {
      throw new Error(`${logPrefix}Sync POST failed: ${errorMessage(e)}`);
    }
    console.error(`${logPrefix}Sync POST fetch error:`, e);
    throw new Error(`${logPrefix}Sync POST failed: ${errorMessage(e)}`);
  }

  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : "";
    const body = await response.text().catch(() => "");
    const bodySuffix = body ? `: ${body}` : "";
    throw new Error(`${logPrefix}Sync POST failed: ${response.status}${statusText}${bodySuffix}`);
  }
}

function toBinaryBody(bytes: Uint8Array): Blob {
  return new Blob([Uint8Array.from(bytes)]);
}

function decodeUtf8(bytes: Uint8Array, label: string): string {
  try {
    return new TextDecoder().decode(bytes);
  } catch (error) {
    throw new Error(`Invalid UTF-8 in ${label}: ${String(error)}`);
  }
}

function parseUuidString(uuid: string): Uint8Array {
  const compact = uuid.replace(/-/g, "");
  if (!/^[0-9a-fA-F]{32}$/.test(compact)) {
    throw new Error(`Invalid client id for binary sync batch: ${uuid}`);
  }

  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(compact.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function writeU32(view: DataView, offset: number, value: number): number {
  view.setUint32(offset, value, false);
  return offset + 4;
}

function readU32(view: DataView, offset: number, label: string): [number, number] {
  if (offset + 4 > view.byteLength) {
    throw new Error(`Truncated binary field: ${label}`);
  }
  return [view.getUint32(offset, false), offset + 4];
}

function readU64(view: DataView, offset: number, label: string): [number, number] {
  if (offset + 8 > view.byteLength) {
    throw new Error(`Truncated binary field: ${label}`);
  }
  const value = Number(view.getBigUint64(offset, false));
  return [value, offset + 8];
}

function readBytes(
  view: DataView,
  bytes: Uint8Array,
  offset: number,
  label: string,
): [Uint8Array, number] {
  const [len, nextOffset] = readU32(view, offset, label);
  if (nextOffset + len > view.byteLength) {
    throw new Error(`Truncated binary field: ${label}`);
  }
  return [bytes.slice(nextOffset, nextOffset + len), nextOffset + len];
}

function readOptionalU64(view: DataView, offset: number, label: string): [number | null, number] {
  if (offset + 1 > view.byteLength) {
    throw new Error(`Truncated binary field: ${label}`);
  }
  const present = view.getUint8(offset);
  if (present === 0) return [null, offset + 1];
  if (present !== 1) {
    throw new Error(`Invalid optional marker for ${label}`);
  }
  return readU64(view, offset + 1, label);
}

function readOptionalString(
  view: DataView,
  bytes: Uint8Array,
  offset: number,
  label: string,
): [string | null, number] {
  if (offset + 1 > view.byteLength) {
    throw new Error(`Truncated binary field: ${label}`);
  }
  const present = view.getUint8(offset);
  if (present === 0) return [null, offset + 1];
  if (present !== 1) {
    throw new Error(`Invalid optional marker for ${label}`);
  }
  const [valueBytes, nextOffset] = readBytes(view, bytes, offset + 1, label);
  return [decodeUtf8(valueBytes, label), nextOffset];
}

function encodeBinarySyncBatchRequest(clientId: string, payloads: Uint8Array[]): Uint8Array {
  const clientIdBytes = parseUuidString(clientId);
  const totalLength = 16 + 4 + payloads.reduce((sum, payload) => sum + 4 + payload.length, 0);
  const out = new Uint8Array(totalLength);
  const view = new DataView(out.buffer);
  let offset = 0;
  out.set(clientIdBytes, offset);
  offset += 16;
  offset = writeU32(view, offset, payloads.length);
  for (const payload of payloads) {
    offset = writeU32(view, offset, payload.length);
    out.set(payload, offset);
    offset += payload.length;
  }
  return out;
}

type DecodedBinaryServerEvent =
  | {
      type: "Connected";
      clientId: string;
      catalogueStateHash: string | null;
    }
  | {
      type: "SyncUpdate";
      payload: Uint8Array;
    }
  | {
      type: "Subscribed";
    }
  | {
      type: "Error";
      message: string;
      code: number;
    }
  | {
      type: "Heartbeat";
    };

function decodeBinaryServerEvent(frame: Uint8Array): DecodedBinaryServerEvent {
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  if (view.byteLength < 1) {
    throw new Error("Truncated binary server event");
  }
  let offset = 1;
  switch (view.getUint8(0)) {
    case 1: {
      [, offset] = readU64(view, offset, "connection_id");
      const [clientIdBytes, clientIdOffset] = readBytes(view, frame, offset, "client_id");
      offset = clientIdOffset;
      const clientId = decodeUtf8(clientIdBytes, "client_id");
      [, offset] = readOptionalU64(view, offset, "next_sync_seq");
      const [catalogueStateHash] = readOptionalString(view, frame, offset, "catalogue_state_hash");
      return { type: "Connected", clientId, catalogueStateHash };
    }
    case 2:
      return { type: "Subscribed" };
    case 3: {
      [, offset] = readOptionalU64(view, offset, "seq");
      const [payload] = readBytes(view, frame, offset, "sync_payload");
      return { type: "SyncUpdate", payload };
    }
    case 4: {
      if (offset + 1 > view.byteLength) {
        throw new Error("Truncated binary field: error_code");
      }
      const code = view.getUint8(offset);
      offset += 1;
      const [message] = readBytes(view, frame, offset, "error_message");
      return { type: "Error", code, message: decodeUtf8(message, "error_message") };
    }
    case 5:
      return { type: "Heartbeat" };
    default:
      throw new Error(`Invalid binary server event tag: ${view.getUint8(0)}`);
  }
}
function catalogueObjectTypeFromPayloadJson(payloadJson: string): string | null {
  try {
    const parsed = JSON.parse(payloadJson) as {
      ObjectUpdated?: {
        metadata?: {
          metadata?: {
            type?: unknown;
          };
        };
      };
    };
    const kind = parsed.ObjectUpdated?.metadata?.metadata?.type;
    return typeof kind === "string" ? kind : null;
  } catch {
    return null;
  }
}

function isStructuralSchemaCataloguePayload(payloadJson: string): boolean {
  return catalogueObjectTypeFromPayloadJson(payloadJson) === "catalogue_schema";
}
/**
 * POST a sync payload to the server.
 *
 * User auth headers are always applied first (JWT or local auth).
 * Structural schema catalogue payloads can also flow with ordinary user auth so
 * development servers can learn schemas without exposing an admin secret to the client.
 * Other catalogue payloads still require the admin secret.
 */
export async function sendSyncPayload(
  serverUrl: string,
  payload: Uint8Array | string,
  isCatalogue: boolean,
  auth: SyncAuth,
  logPrefix = "",
): Promise<void> {
  const payloadJson = typeof payload === "string" ? payload : null;
  const isSchemaCatalogue =
    isCatalogue && payloadJson !== null && isStructuralSchemaCataloguePayload(payloadJson);

  if (isCatalogue && !auth.adminSecret && !isSchemaCatalogue) {
    return;
  }

  const isBinary = payload instanceof Uint8Array;
  const headers: Record<string, string> = {
    "Content-Type": isBinary ? "application/octet-stream" : "application/json",
  };
  if (isCatalogue && auth.adminSecret) {
    headers["X-Jazz-Admin-Secret"] = auth.adminSecret!;
  } else {
    applySyncAuthHeaders(headers, auth);
  }

  const body =
    payload instanceof Uint8Array
      ? toBinaryBody(encodeBinarySyncBatchRequest(auth.clientId ?? fallbackClientId, [payload]))
      : `{"payloads":[${payload}],"client_id":${JSON.stringify(auth.clientId ?? fallbackClientId)}}`;
  await postSyncBatch(
    buildEndpointUrl(serverUrl, "/sync", auth.pathPrefix),
    headers,
    body,
    logPrefix,
  );
}

/**
 * POST an ordered batch of sync payloads to the server in a single request.
 *
 * Wire format: {"payloads":[<payload1>,<payload2>,…],"client_id":"…"}
 *
 * Each payload JSON string is embedded raw (no double-serialisation).
 * Non-catalogue payloads only — catalogue payloads are sent via sendSyncPayload.
 */
export async function sendSyncPayloadBatch(
  serverUrl: string,
  payloads: Array<Uint8Array | string>,
  auth: SyncAuth,
  logPrefix = "",
): Promise<void> {
  if (payloads.length === 0) return;

  const binaryPayloads = payloads.filter(
    (payload): payload is Uint8Array => payload instanceof Uint8Array,
  );
  const allBinary = binaryPayloads.length === payloads.length;
  const allJson = binaryPayloads.length === 0;
  if (!allBinary && !allJson) {
    throw new Error("Mixed sync payload encodings are not supported in a single batch");
  }
  const headers: Record<string, string> = {
    "Content-Type": allBinary ? "application/octet-stream" : "application/json",
  };
  applySyncAuthHeaders(headers, auth);

  const body = allBinary
    ? toBinaryBody(encodeBinarySyncBatchRequest(auth.clientId ?? fallbackClientId, binaryPayloads))
    : `{"payloads":[${payloads.join(",")}],"client_id":${JSON.stringify(auth.clientId ?? fallbackClientId)}}`;
  await postSyncBatch(
    buildEndpointUrl(serverUrl, "/sync", auth.pathPrefix),
    headers,
    body,
    logPrefix,
  );
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
    if (isExpectedFetchAbortError(e)) {
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(`${logPrefix}Link external failed: ${msg}`);
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
 * Each frame is: 4-byte big-endian length + binary event payload.
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
      const frame = buffer.slice(4, 4 + len);
      buffer = buffer.slice(4 + len);

      let decodedEvent:
        | { type: "Connected"; clientId: string; catalogueStateHash: string | null }
        | { type: "SyncUpdate"; payload: Uint8Array | string }
        | null = null;
      try {
        if (frame[0] === 123) {
          const json = new TextDecoder().decode(frame);
          const event = JSON.parse(json);
          if (event.type === "Connected" && event.client_id) {
            decodedEvent = {
              type: "Connected",
              clientId: event.client_id,
              catalogueStateHash: event.catalogue_state_hash ?? null,
            };
          } else if (event.type === "SyncUpdate") {
            logSchemaWarningPayload(event.payload, logPrefix);
            decodedEvent = {
              type: "SyncUpdate",
              payload: JSON.stringify(event.payload),
            };
          }
        } else {
          const event = decodeBinaryServerEvent(frame);
          if (event.type === "Connected") {
            decodedEvent = event;
          } else if (event.type === "SyncUpdate") {
            decodedEvent = event;
          }
        }
      } catch (error) {
        console.error(`${logPrefix}Stream parse error:`, error);
        continue;
      }

      if (!decodedEvent) {
        continue;
      }

      try {
        if (decodedEvent.type === "Connected") {
          callbacks.onConnected?.(decodedEvent.clientId, decodedEvent.catalogueStateHash);
        } else {
          callbacks.onSyncMessage(decodedEvent.payload);
        }
      } catch (error) {
        console.error(`${logPrefix}Stream callback error:`, error);
      }
    }
  }
}
