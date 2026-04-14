/**
 * Shared sync transport utilities.
 *
 * Provides auth header helpers, outbox routing, and URL builders used by
 * worker-bridge (main-thread ↔ worker postMessage path) and React Native.
 * HTTP/SSE transport code has been removed — server sync is now handled
 * by the Rust-owned WebSocket transport via `runtime.connect()`.
 */

export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";

/** Auth and identity context for sync operations. */
export interface SyncAuth {
  jwtToken?: string;
  backendSecret?: string;
  adminSecret?: string;
  clientId?: string;
  pathPrefix?: string;
}

type UnauthenticatedPayload = {
  error?: unknown;
  code?: unknown;
  message?: unknown;
};

export class SyncAuthError extends Error {
  readonly name = "SyncAuthError";

  constructor(
    readonly reason: AuthFailureReason,
    message: string,
    readonly status = 401,
  ) {
    super(message);
  }
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

export async function readSyncAuthError(response: Response): Promise<SyncAuthError | null> {
  if (response.status !== 401) {
    return null;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    return new SyncAuthError("invalid", `Sync auth failed: ${response.status}`);
  }

  try {
    const body = (await response.json()) as UnauthenticatedPayload;
    if (body.error !== "unauthenticated") {
      return new SyncAuthError("invalid", `Sync auth failed: ${response.status}`);
    }

    const reason: AuthFailureReason =
      body.code === "expired" ||
      body.code === "missing" ||
      body.code === "invalid" ||
      body.code === "disabled"
        ? body.code
        : "invalid";

    const message = typeof body.message === "string" ? body.message : `Unauthenticated (${reason})`;

    return new SyncAuthError(reason, message, response.status);
  } catch {
    return new SyncAuthError("invalid", `Sync auth failed: ${response.status}`);
  }
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
 * Apply end-user auth headers with stable precedence.
 *
 * Sets `Authorization: Bearer <token>` when a JWT is available.
 */
export function applyUserAuthHeaders(headers: Record<string, string>, auth: SyncAuth): void {
  if (auth.jwtToken) {
    headers["Authorization"] = `Bearer ${auth.jwtToken}`;
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
