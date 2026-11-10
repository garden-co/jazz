/** Opens a compatible Jazz core. Implementations own hosting and authentication. */
export interface JazzCoreProvider {
  /** Optional process-wide reuse identity and settings. Functions in settings
   * are refreshed on hot reload; other changes require reopening the store.
   * Providers read these through `this.reuse.settings` when opening.
   */
  readonly reuse?: { readonly key: string; readonly settings: object };
  /** Each call owns a core handle that createJazz closes on failure or shutdown. */
  open(): Promise<JazzCore>;
}

/** Authority requested by the backend, not by an untrusted browser. */
export type JazzCoreAccess = "admin" | "backend" | "session";

export interface JazzCoreReply {
  status: number;
  headers: string[][];
  body: Uint8Array;
}

/** Transport-independent socket events. A callback error is terminal. */
export interface JazzCoreEvent {
  kind: "message" | "close";
  data?: Uint8Array;
  code?: number;
  reason?: string;
}

export interface JazzCoreSocket {
  /** Queue/send a frame in order. False means this session has ended. */
  send(data: Uint8Array): boolean;
  /** Wait for room before sending; do not interleave with send. */
  sendWithRoom?(data: Uint8Array): Promise<boolean>;
  close(): void;
}

/**
 * A transport to a Jazz core. Requests and frames use the core protocol.
 * Providers authenticate the requested access with credentials they keep private.
 * createJazz owns bundle pinning and the local client's transport registration.
 */
export interface JazzCore {
  readonly appId: string;
  handle(
    method: string,
    path: string,
    headers: string[][],
    body: Uint8Array,
    access?: JazzCoreAccess,
  ): Promise<JazzCoreReply>;
  /**
   * The first frame is the JSON session prelude, already pinned by createJazz.
   * Authenticate it as needed, then forward ordinary Jazz wire frames unchanged.
   * Buffer sends while opening; events must arrive asynchronously, and close
   * must cancel a pending connection too.
   */
  openSocket(
    headers: string[][],
    onEvent: (error: Error | null, event: JazzCoreEvent) => void,
    access?: JazzCoreAccess,
    /** Local transport: native hosts may omit the network handshake deadline. */
    options?: { inProcess?: boolean },
  ): JazzCoreSocket;
  /**
   * Deterministic, domain-separated MAC (base64url) for account-link challenges.
   * Handles serving the same app must agree when requests can move between them.
   */
  signLinkChallenge(payload: string): string | Promise<string>;
  /** Release resources without deleting data or stopping a shared core. */
  close(): Promise<void>;
}
