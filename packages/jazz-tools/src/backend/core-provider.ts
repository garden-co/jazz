import type { WsLikeSocket } from "./create-jazz.js";
import { randomUUID } from "node:crypto";
import {
  registerInProcessTransport,
  type InProcessTransport,
} from "../runtime/in-process-transport.js";

import type { JazzCoreSocketEvent } from "jazz-napi";

export interface CoreReply {
  status: number;
  headers: string[][];
  body: Uint8Array;
}

/** One sync session on the core. */
export interface CoreSocket {
  /** Queue one frame, waiting for capacity when the transport supports it.
   * Wait for the result before sending the next frame to preserve order.
   * False means the session can no longer accept frames. */
  send(data: Uint8Array): boolean | Promise<boolean>;
  close(): void;
  /**
   * Resolves once the session reaches the core. A session that never does
   * ends with its close event instead, and this stays pending. Without it,
   * the session counts as open at once (a core in this process).
   */
  readonly opened?: Promise<void>;
}

/**
 * The access this backend states for a request or socket: `admin` (schema,
 * permissions and account admin calls), `backend` (its own client, and
 * sessions it vouches for) or `session` (a guest's session, or a public
 * route, relayed as is).
 */
export type BackendAccess = "admin" | "backend" | "session";

/** Upgrade a validated incoming sync request and give Jazz the server socket. */
export type JazzWebSocketUpgrade = (
  request: Request,
  accept: (socket: WsLikeSocket) => void,
  options: { maxPayload: number },
) => Response | Promise<Response>;

/** A provider opens its authority on first use. A shared provider can opt into
 * process-local reuse across createJazz calls (including hot reloads). */
export interface JazzProvider {
  /** Fails if this provider cannot run where createJazz was called.
   * Checked before store reuse or I/O. Omit for portable providers. */
  assertSupportedEnvironment?(): void;
  /** A process-wide identity such as "my-provider:tenant-id". Omit to open
   * a separate connection for every createJazz instance. */
  readonly key?: string | undefined;
  /** Called on the live provider when its key is reused. Throw before changing
   * state if the new configuration is incompatible, or adopt live options
   * such as a refreshed credential callback. */
  reload?(next: JazzProvider): void;
  open(context: JazzCoreOpenContext): Promise<JazzCore>;
  /** Supply the incoming upgrade supported by this handler. Jazz also uses
   * availability to answer transport probes. Omit or return undefined to
   * use HTTP streaming when the application does not handle upgrades itself. */
  getWebSocketUpgrade?(
    handler: "fetch" | "next",
  ): JazzWebSocketUpgrade | undefined | Promise<JazzWebSocketUpgrade | undefined>;
}

/** Register an HTTP/socket transport for Jazz clients and pin sessions to
 * the bundle published by this createJazz instance. Providers may also
 * delegate to another provider by passing it this same context. */
export interface JazzCoreOpenContext {
  /** Wrap the backend client's fetch and WebSocket, e.g. to wake a suspended
   * core before reconnect. The returned route belongs to the calling host. */
  routeClient: typeof routeJazzCoreClient;
  connect(
    transport: JazzCoreTransport,
    options?: {
      adminSecret?: string;
      backendSecret?: string;
      openClientSocket?: (onEvent: Parameters<JazzCoreTransport["openSocket"]>[2]) => CoreSocket;
    },
  ): JazzCore;
}

export type JazzStore = JazzProvider;

/** A connection to a core speaking Jazz's HTTP and socket protocols.
 * Use the explicit access argument for authorization. Headers and prelude
 * secret slots may contain local routing markers, not provider credentials.
 */
export interface JazzCoreTransport {
  readonly appId: string;
  handle(
    method: string,
    path: string,
    headers: string[][],
    body: Uint8Array,
    access: BackendAccess,
  ): Promise<CoreReply>;
  openSocket(
    path: string,
    headers: string[][],
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
    access: BackendAccess,
  ): CoreSocket;
  /** Release the connection and any resources owned by this open. */
  close(): Promise<void>;
}

/** The core this backend fronts: in this process, or reached over the network. */
export interface JazzCore {
  /**
   * The app the core serves: the one `remote()` names, or the id an
   * in-process or Vercel core keeps with its data.
   */
  readonly appId: string;
  /** Optional key for signing inspector tokens. */
  readonly tokenSecret?: string;
  /** Shared app secret when available for signing requests. */
  readonly appSecret?: string;
  /** Where this process's own clients reach it. */
  readonly serverUrl: string;
  /** Local routing for this URL. Omit for a directly reachable network core. */
  readonly clientTransport?: InProcessTransport;
  /**
   * What requests and preludes carry in their secret slots: the core's own
   * secrets in this process. A remote core replaces them by the `access`
   * stated next to the backend credential.
   */
  readonly adminSecret: string;
  readonly backendSecret: string;
  handle(
    method: string,
    path: string,
    headers: string[][],
    body: Uint8Array,
    access?: BackendAccess,
  ): Promise<CoreReply>;
  openSocket(
    headers: string[][],
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
    access?: BackendAccess,
  ): CoreSocket;
  close(): Promise<void>;
}

/** Give a backend client's transport a process-local URL. A provider wrapping
 * another core can use this to wake or reconnect it on each client operation.
 * Release the route when the provider closes. */
export function routeJazzCoreClient(clientTransport: InProcessTransport) {
  const serverUrl = `http://jazz-${randomUUID()}.invalid`;
  const close = registerInProcessTransport(serverUrl, clientTransport);
  return { serverUrl, clientTransport, close };
}
