/**
 * Shared sync transport utilities.
 *
 * Provides auth header helpers, outbox routing, and URL builders used by
 * worker-bridge (main-thread ↔ worker postMessage path) and React Native.
 * HTTP/SSE transport code has been removed — server sync is now handled
 * by the Rust-owned WebSocket transport via `runtime.connect()`.
 */

export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";

export interface SyncOutboxRouterOptions {
  logPrefix?: string;
  onServerPayload(
    payload: Uint8Array | string,
    isCatalogue: boolean,
    sequence: number | null,
  ): void | Promise<void>;
  onClientPayload?(payload: Uint8Array, sequence: number | null): void;
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
      sequence?: number | null,
    ]
  | [
      err: unknown,
      destinationKind: OutboxDestinationKind,
      destinationId: string,
      payload: Uint8Array | string,
      isCatalogue: boolean,
      sequence?: number | null,
    ]
  | [
      err: unknown,
      message: [
        destinationKind: OutboxDestinationKind,
        destinationId: string,
        payload: Uint8Array | string,
        isCatalogue: boolean,
        sequence?: number | null,
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
  sequence: number | null;
} | null {
  // WASM/RN-style callback: (destinationKind, destinationId, payloadJson, isCatalogue, sequence)
  if (isOutboxDestinationKind(args[0])) {
    const payload = args[2];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[0],
      payload: payload,
      isCatalogue: Boolean(args[3]),
      sequence: typeof args[4] === "number" ? args[4] : null,
    };
  }

  // NAPI callee-handled callback: (err, destinationKind, destinationId, payloadJson, isCatalogue, sequence)
  if (isOutboxDestinationKind(args[1])) {
    const payload = args[3];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[1],
      payload: payload,
      isCatalogue: Boolean(args[4]),
      sequence: typeof args[5] === "number" ? args[5] : null,
    };
  }

  // Real NAPI callback: (err, [destinationKind, destinationId, payloadJson, isCatalogue, sequence])
  if (Array.isArray(args[1]) && isOutboxDestinationKind(args[1][0])) {
    const payload = args[1][2];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[1][0],
      payload,
      isCatalogue: Boolean(args[1][3]),
      sequence: typeof args[1][4] === "number" ? args[1][4] : null,
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

    const { destinationKind, payload, isCatalogue, sequence } = normalized;
    if (destinationKind === "client") {
      options.onClientPayload?.(payload as Uint8Array, sequence);
      return;
    }

    Promise.resolve(options.onServerPayload(payload, isCatalogue, sequence)).catch((error) => {
      if (options.onServerPayloadError) {
        options.onServerPayloadError(error);
        return;
      }
      console.error(`${logPrefix}Sync POST error:`, error);
    });
  };
}

function trimTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

/**
 * Apply end-user auth headers. Sets `Authorization: Bearer <token>` when a JWT is available.
 */
export function applyUserAuthHeaders(
  headers: Record<string, string>,
  auth: { jwtToken?: string },
): void {
  if (auth.jwtToken) {
    headers["Authorization"] = `Bearer ${auth.jwtToken}`;
  }
}
