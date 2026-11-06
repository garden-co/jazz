import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { verifyChallenge } from "../local-first-auth/server.js";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { JazzCore, type JazzCoreSocketEvent } from "jazz-napi";
import { registerInProcessTransport } from "../runtime/in-process-transport.js";
import { accountAppId, localFirstAccountId } from "../accounts/local-first.js";
import { LOCAL_FIRST_JWT_ISSUER, parseJwtPayload } from "../runtime/client-session.js";
import { verifyLocalFirstToken } from "../runtime/local-first-token.js";
import { endpointScope } from "../runtime/client-scope.js";
import { PostcardWriter } from "../runtime/native-runtime/native-codec.js";
import { encodeWebSocketFrameBatch } from "../runtime/native-runtime/websocket.js";
import type {
  BrowserWebSocket,
  WebSocketConstructor,
} from "../runtime/native-runtime/websocket.js";
import {
  STREAM_RECORD_MESSAGE,
  STREAM_RECORD_SESSION,
  StreamRecordReader,
  encodeStreamClose,
  encodeStreamRecord,
} from "../runtime/native-runtime/http-stream-socket.js";
import { resolveSchemaSource } from "../schema-source.js";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../schema-permissions.js";
import { shippedMigrations } from "../runtime/client-migrations.js";
import { computeSchemaHash } from "../schema-hash.js";
import { validateSchemaAndPermissions } from "../dev/schema-validation.js";
import { deployArtifacts } from "../dev/catalogue.js";
import { fetchMigrationGraph } from "../dev/migration-graph.js";
import { DeploymentError, type PublishedTableLens } from "../dev/catalogue-api.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import type { JazzSession } from "../session/state.js";
import {
  backendDbAttributedTo,
  backendDbForSession,
  openJazzSession,
  type JazzClient,
} from "./create-jazz-session.js";
import type { BackendSchemaInput } from "./create-jazz-context.js";
import {
  BackendClientsUnavailable,
  backendClientVerifier,
  type BackendClientTrust,
} from "./backend-clients.js";
import { BACKEND_USER_ISSUER } from "./backend-issuer.js";
import { earlyDb, type EarlyDb } from "./early-db.js";
import {
  defaultCoreName,
  onVercelDeployment,
  onVercelHost,
  openVercelCore,
  refuseVercelAppId,
  type VercelStoreOptions,
} from "./vercel-store.js";

export type { VercelStoreOptions } from "./vercel-store.js";

import { files, type BlobStore } from "./blobs.js";
export {
  files,
  s3,
  vercelBlob,
  type BlobStore,
  type S3BlobStore,
  type VercelBlobStore,
} from "./blobs.js";
import { isSameAdmission, recheckEvery } from "./session-recheck.js";
import { NoJazzAccountError } from "./no-account-error.js";
import { RequestAuthenticationError } from "./request-auth.js";

/** Where the core keeps its data. */
export type JazzStore =
  | {
      readonly kind: "sqlite";
      readonly path: string;
      readonly blobs: BlobStore;
    }
  | { readonly kind: "memory" }
  | {
      readonly kind: "remote";
      readonly url: string;
      /** The app this core serves. */
      readonly appId: string;
      /** This backend's credential for the core, asked for each request and socket. */
      readonly backendCredential: BackendCredential;
    }
  | {
      readonly kind: "vercel";
      readonly options: VercelStoreOptions;
      /**
       * What runs outside a Vercel deployment, e.g. under `next dev`
       * (default: `sqlite({ path: ".jazz/local" })`).
       */
      readonly local: Extract<JazzStore, { kind: "sqlite" | "memory" }>;
    };

/**
 * A SQLite core in this process, kept in the directory `path`. It generates
 * its app id on first start and keeps it beside its data, in `<path>/app-id`
 * (`jazz-alpha59-alpha60-import` writes an alpha.59 app's id there). Large
 * values' bytes go to `blobs`, by default `files()` in `<path>/blobs`.
 *
 * Only one process can open `path` at a time (it holds `<path>/jazz.lock`
 * while the core is open); a second one fails with "already open". It
 * refuses to start on serverless hosts with no lasting disk (Vercel, AWS
 * Lambda, Netlify): use `vercel()` or `remote()` there.
 */
export function sqlite(options: {
  path: string;
  blobs?: BlobStore;
}): Extract<JazzStore, { kind: "sqlite" }> {
  refuseStoreAppId("sqlite", options);
  const blobs = options.blobs ?? files();
  if (
    blobs !== "sqlite" &&
    (typeof blobs !== "object" || (blobs.kind !== "files" && blobs.kind !== "s3"))
  ) {
    throw new Error('sqlite(): blobs must be files(), s3() or "sqlite"');
  }
  return { kind: "sqlite", path: options.path, blobs };
}

/**
 * An in-memory core in this process: for tests and throwaway previews. Each
 * one is its own store, with a new app id.
 */
export function memory(options?: { appId?: never }): Extract<JazzStore, { kind: "memory" }> {
  refuseStoreAppId("memory", options);
  return { kind: "memory" };
}

/** Stores keep their own app id; one passed from JavaScript is refused, not ignored. */
function refuseStoreAppId(store: string, options: unknown): void {
  if (typeof options === "object" && options !== null && "appId" in options) {
    throw new Error(
      `${store}(): appId is not an option: the store keeps its own app id. To keep an alpha.59 app's id, import its data with \`jazz-alpha59-alpha60-import import --app-id\`.`,
    );
  }
}

/**
 * The credential this backend presents to its core, fresh for each request
 * and socket: the app's secret, or a token that proves which workload this
 * is (a sync server started with `--trusted-workload-*` accepts its
 * platform's OIDC token, such as a Vercel function's).
 */
export type BackendCredential = () => string | Promise<string>;

/**
 * A core running elsewhere (Jazz Cloud, or a sync server you run), serving
 * the app `appId`, reached from this backend with the app's one secret (its
 * admin secret), or with a `backendCredential` of your own. Browsers still
 * talk only to your backend: it decides who each user is, and the core
 * trusts it to say so. The secret never leaves your server.
 */
export function remote(
  options:
    | { url: string; appId: string; secret: string }
    | { url: string; appId: string; backendCredential: BackendCredential },
): Extract<JazzStore, { kind: "remote" }> {
  const appId = options.appId;
  if (typeof appId !== "string" || !appId.trim()) {
    throw new Error("remote(): appId must name the app the core serves (it is empty)");
  }
  let backendCredential: BackendCredential;
  if ("backendCredential" in options) {
    backendCredential = options.backendCredential;
  } else {
    const secret = options.secret;
    if (secret === undefined && "adminSecret" in options) {
      throw new Error(
        "remote(): `adminSecret` and `backendSecret` were replaced by one `secret`, the app's admin secret",
      );
    }
    if (typeof secret !== "string" || !secret.trim()) {
      throw new Error("remote(): secret must be the app's admin secret (it is empty)");
    }
    backendCredential = () => secret;
  }
  return { kind: "remote", url: options.url.replace(/\/+$/, ""), appId, backendCredential };
}

/**
 * The store for apps deployed on Vercel. Use it in dev too: outside a
 * production or preview deployment (`next dev`, `vercel dev`, tests) it runs
 * `local`, by default `sqlite({ path: ".jazz/local" })`.
 *
 * Deployed, it runs a core in a persistent Vercel Sandbox in your own Vercel
 * project, with its data on a Drive: one for production and one per Git
 * branch for previews.
 * It is created on first use, wakes when a request needs it (about a second)
 * and stops after `idleMs` without requests or sockets. The function's Vercel
 * OIDC token is its only credential, so there are no secrets and no env vars:
 * the core admits only tokens naming the workload its first start pinned
 * (team, project and environment by name), and a deployment of another
 * environment refuses to start or retarget it. All previews share one
 * workload, so preview branches' cores are kept apart, not secured from each
 * other. Anyone with Sandbox access to the project controls its cores. The
 * token comes with each request (it can't be read at module scope), so the
 * core opens on the first request, not when `createJazz` is called; work
 * between requests uses the freshest one seen, and open sockets outlive its
 * expiry. A team or project rename (or
 * a change of OIDC issuer mode) changes the workload: delete `/data/trust` in
 * the Sandbox to re-pin the core. Install `@vercel/sandbox` and
 * `@vercel/functions` next to `jazz-tools`. A deployment of a newer jazz-tools
 * restarts the core on its version, and an older deployment never moves it
 * back. Branch cores are not deleted when a branch merges yet: each keeps its
 * Sandbox and a Drive of up to `maxDriveBytes` until you remove them.
 */
export function vercel(
  options: VercelStoreOptions & { local?: Extract<JazzStore, { kind: "sqlite" | "memory" }> } = {},
): JazzStore {
  // Refused here, not on the first deployed request: in dev the local
  // store runs and would never read it.
  refuseVercelAppId(options);
  const { local, ...rest } = options;
  return { kind: "vercel", options: rest, local: local ?? sqlite({ path: ".jazz/local" }) };
}

function blobOptions(blobs: BlobStore): {
  blobs: "files" | "s3" | "sqlite";
  blobsDir?: string;
  s3Bucket?: string;
  s3Region?: string;
  s3Endpoint?: string;
  s3Prefix?: string;
} {
  if (blobs === "sqlite") return { blobs: "sqlite" };
  if (blobs.kind === "s3") {
    return {
      blobs: "s3",
      ...(blobs.bucket === undefined ? {} : { s3Bucket: blobs.bucket }),
      ...(blobs.region === undefined ? {} : { s3Region: blobs.region }),
      ...(blobs.endpoint === undefined ? {} : { s3Endpoint: blobs.endpoint }),
      ...(blobs.prefix === undefined ? {} : { s3Prefix: blobs.prefix }),
    };
  }
  return { blobs: "files", ...(blobs.dir === undefined ? {} : { blobsDir: blobs.dir }) };
}

declare const localFirstGuestBrand: unique symbol;

/**
 * `authenticate`'s answer for a request from a local-first guest: an account
 * a browser made on its own, before (or without) signing in. Return
 * {@link acceptLocalFirstAuth} for the requests you let guests make; any other
 * answer refuses a guest.
 */
export interface LocalFirstGuest {
  readonly [localFirstGuestBrand]: true;
}

const LOCAL_FIRST_GUEST = Object.freeze({
  kind: "jazz:local-first-guest",
}) as unknown as LocalFirstGuest;

/**
 * Let this request through as a local-first guest, when it comes from one.
 * Guests are refused unless `authenticate` returns this: a guest has no user
 * in your auth, so only your permissions stand between it and your data.
 *
 * ```ts
 * authenticate: async (request) => {
 *   const session = await auth.api.getSession({ headers: request.headers });
 *   return session ? { userId: session.user.id } : acceptLocalFirstAuth(request);
 * },
 * ```
 *
 * An app with no sign-in of its own, where every browser is a guest, passes
 * it as is: `authenticate: acceptLocalFirstAuth`.
 *
 * For a request that is not a guest's, it means the same as `null`: no one
 * is signed in.
 */
export function acceptLocalFirstAuth(_request: Request): LocalFirstGuest {
  return LOCAL_FIRST_GUEST;
}

/** Who the request's user is, as the app's own auth decides. */
export interface JazzUser {
  /** Stable user id in your auth system. Becomes the identity's `sub`. */
  userId: string;
  /** Extra claims your permissions can read through `session.claims`. */
  claims?: Record<string, unknown>;
}

export interface CreateJazzOptions {
  app: BackendSchemaInput;
  permissions: CompiledPermissionsMap;
  store: JazzStore;
  /**
   * Decide who sent `request`, with your app's own session (a cookie, a
   * header). Return null for a signed-out request. Jazz calls it when a
   * browser signs in or opens a sync session, and in `forReqUser`.
   *
   * Local-first guests are refused unless it returns
   * `acceptLocalFirstAuth(request)` for them; an app with no sign-in passes
   * `authenticate: acceptLocalFirstAuth`. Without `authenticate`, only
   * backend code reaches the data.
   */
  authenticate?: (
    request: Request,
  ) => JazzUser | LocalFirstGuest | null | Promise<JazzUser | LocalFirstGuest | null>;
  /** The path Jazz is mounted under. Defaults to `/jazz`. */
  basePath?: string;
  /**
   * The URL browsers reach Jazz at, when a proxy in front of this backend
   * changes the host or path (e.g. pages call `https://app.example.com/api/jazz`
   * and the proxy forwards to `/jazz` here). A browser client is scoped by the
   * URL it uses, so its guests' proofs name that URL. Recommended in
   * production: without it, Jazz accepts the page's own origin (or an
   * `allowedOrigins` entry) and this request's Host, each under `basePath`,
   * and a caller that reaches the backend directly can name any Host. When
   * set, only these URLs are accepted, so list your dev URL too if a dev
   * server uses this backend.
   */
  publicUrl?: string | string[];
  /**
   * How long a browser's sync session lasts before `authenticate` is asked
   * again (the client reconnects on its own). Defaults to one hour.
   */
  sessionTtlSeconds?: number;
  /**
   * How often `authenticate` is asked again for each open sync session of a
   * signed-in user. When it no longer names that user (they signed out, in
   * another tab or anywhere else, or their session expired or was revoked),
   * the session ends as signed out. When it names them with other claims
   * (a role changed), the session ends too and the client reconnects with
   * the new claims, so this bounds how long a claims change takes to reach
   * an open session. Defaults to 60 seconds; 0 turns it off, and changes
   * then wait for the session to end (`sessionTtlSeconds`).
   */
  recheckSeconds?: number;
  /**
   * What happens to a local-first guest's key once the guest links to a
   * signed-in user (`linkWithBackend()`):
   * - `"keep"` (the default): the key stays an identity of the account, so
   *   the device can still sign in as the guest (e.g. offline) alongside
   *   the linked user.
   * - `"revoke"`: the key is retired. Only the linked user signs in to the
   *   account from then on, so a leaked guest key opens nothing.
   */
  guestKeyAfterLink?: "keep" | "revoke";
  /**
   * Browser origins, besides the backend's own, that may open sync sessions
   * and call sign-in (e.g. `["https://app.example.com"]` when the page is
   * served from another host than the backend). A request from any other
   * origin is refused, so a page elsewhere can't act with your users'
   * cookies.
   */
  allowedOrigins?: string[];
  /**
   * Set when your host upgrades sync sockets itself and hands them to
   * `acceptSocket` (Bun, Deno, uWebSockets, your own `ws` setup). Then a
   * socket that fails to open is retried as a socket, instead of the
   * browser switching to the HTTP stream. `attach()` and `next()` on Vercel
   * know this on their own.
   */
  upgradesSockets?: boolean;
  /**
   * Backend clients: machines outside this main backend (a CI job, a worker
   * elsewhere, a partner's service) that sync through it with their
   * platform's OIDC token instead of a user's session. Each is trusted with
   * {@link trustWorkload}, either as a trusted machine (backend authority,
   * your permissions not checked) or as a scoped machine acting as a user.
   * A request carrying a token from one of these issuers is decided here
   * alone: a token that doesn't verify is refused, never passed on to
   * `authenticate`.
   */
  backendClients?: BackendClientTrust[];
  /**
   * Confirms that this deployment may switch tables it declares
   * `.history("latest")` from full history: their past versions are dropped
   * at each row's next write, for good. Without it, starting a backend whose
   * schema makes that switch fails, naming the tables. Remove it again once
   * the deployment is out.
   */
  allowDroppingHistory?: boolean;
}

/** A WebSocket as the `ws` package (and Vercel's `upgradeWebSocket`) hands it over. */
export interface WsLikeSocket {
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
  on(event: "message", listener: (data: unknown, isBinary?: boolean) => void): unknown;
  on(event: "close", listener: () => void): unknown;
  on(event: "error", listener: (error: unknown) => void): unknown;
}

export interface Jazz {
  /**
   * The app id the store runs as. Known once the store has opened (on first
   * use, or `await jazz.ready()`); reading it before throws.
   */
  readonly appId: string;
  readonly basePath: string;
  /**
   * Open the store now, rather than on first use, and apply the app, schema
   * and permissions: to fail at startup instead of on the first request, or
   * to read `appId`. Resolves to this Jazz.
   */
  ready(): Promise<Jazz>;
  /** Answer one request under `basePath` (sync, sign-in, the HTTP stream). */
  handle(request: Request): Promise<Response>;
  /**
   * Route handlers for a Next.js catch-all route:
   *
   * ```ts
   * // app/jazz/[...path]/route.ts
   * import { jazz } from "@/lib/jazz.server"; // export const jazz = createJazz({ ... })
   *
   * export const { GET, POST, DELETE } = jazz.next();
   * ```
   */
  next(): {
    GET(request: Request): Promise<Response>;
    POST(request: Request): Promise<Response>;
    DELETE(request: Request): Promise<Response>;
  };
  /**
   * Accept sync sockets on a Node HTTP server (needs the `ws` package).
   * Plain requests still go through `nodeHandler` or `handle`.
   */
  attach(server: Server): Promise<void>;
  /** Node `(req, res)` handler for requests under `basePath` (Express, Fastify's raw mode). */
  nodeHandler(req: IncomingMessage, res: ServerResponse, next?: () => void): void;
  /** Bridge an upgraded socket (the `ws` package's shape) to a sync session. */
  acceptSocket(request: Request, socket: WsLikeSocket): void;
  /**
   * The backend's own database, with backend permissions. It works before
   * the store opens: writes return at once and apply as it opens, and reads
   * wait for it.
   */
  readonly db: Db;
  /**
   * The database as the request's user, judged by your permissions. Throws
   * a {@link NoJazzAccountError} (`code: "no_account"`) for a user who has no
   * account yet (see {@link createAccountFor}).
   */
  forReqUser(request: Request): Promise<Db>;
  /**
   * The Jazz account of the request's signed-in user, or null when
   * `authenticate` names no user or the user has no account yet. It never
   * creates one (see {@link createAccountFor}). For API routes that act on
   * the user's data with the backend's own `db`.
   */
  accountFor(request: Request): Promise<string | null>;
  /**
   * Create the Jazz account of `userId` (the id your `authenticate` returns
   * for them), or return the one they have. Jazz never creates accounts on
   * its own on the backend: a user gets one when they sign in from a client,
   * when a local-first guest links to them (`linkWithBackend`, so the
   * guest's data becomes theirs), or here. Call it for a user who never
   * opens a client (a webhook, an import, an API-only user) or to seed data
   * before their first visit. A user with an account can no longer link a
   * guest, so don't call it in a sign-up flow whose browser may hold one.
   * Returns the account id.
   */
  createAccountFor(userId: string): Promise<string>;
  /**
   * The backend's own database (backend permissions), with every write
   * stamped as the request's signed-in user: for server actions that do what
   * the user may not do directly, on their behalf.
   *
   * **Your permissions are not checked.** It writes with the backend's full
   * authority; only the attribution is the user's. Check yourself that the
   * user may do what the action does, and keep it in server-only code.
   *
   * Throws a {@link NoJazzAccountError} (`code: "no_account"`) for a user
   * who has no account yet (see {@link createAccountFor}).
   */
  withAttribution(request: Request): Promise<Db>;
  /**
   * Make `to` another identity of the account `from` already uses: after
   * this, `authenticate` returning `to` opens that same account and its data.
   * Call it once your own auth has verified both, as when an anonymous or
   * local-first user signs up, or a user adds a second sign-in method. `to`
   * must not have its own account yet. Returns the account id.
   */
  linkIdentity(link: { from: string; to: string }): Promise<{ account: string }>;
  /** Close every session, the backend database and the core. */
  close(): Promise<void>;
}

const DEFAULT_BASE_PATH = "/jazz";
// Routes a browser may reach. Everything else (admin, backend admission,
// shutdown) stays behind this backend.
/** How long a sync session may take to send its sign-in (its first message). */
const PRELUDE_DEADLINE_MS = 30_000;
/** On createJazz's 426 for a socket path: this endpoint serves the HTTP stream. */
const STREAM_AVAILABLE_HEADER = "x-jazz-stream";
/** The stream session's key, on each send and close (see `openStream`). */
const STREAM_KEY_HEADER = "x-jazz-stream-key";
const PUBLIC_APP_ROUTES = /^\/apps\/[^/]+\/(schemas|schema\/[^/]+)$/;
// What a browser can't hand the core: secrets, a backend credential or the
// access it states, or the core's own mark for sockets a credential vouched for.
// Nor a bearer token: this backend decides who a request is (`authenticate`,
// backend clients), never the core.
const STRIPPED_HEADERS = [
  "authorization",
  "x-jazz-backend-secret",
  "x-jazz-admin-secret",
  "x-jazz-session",
  "x-jazz-backend-credential",
  "x-jazz-backend-access",
  "x-jazz-internal-workload-vouched",
];
const CREDENTIAL_HEADER = "x-jazz-backend-credential";
const ACCESS_HEADER = "x-jazz-backend-access";
// What the secret slots carry to a remote core: the access stated next to
// the backend credential decides what a request gets, not these.
const STAND_IN_SECRET = "stated-by-backend-credential";
// What a browser's request to a public route carries on to a remote core:
// never its cookies or its app session.
const REMOTE_FORWARDED_HEADERS = ["accept", "content-type", "if-none-match"];
const MAX_STREAM_MESSAGE_BYTES = 2 * 1024 * 1024;

interface CoreReply {
  status: number;
  headers: string[][];
  body: Uint8Array;
}

/** One sync session on the core. */
export interface CoreSocket {
  /** False once the session ended. */
  send(data: Uint8Array): boolean;
  close(): void;
}

/**
 * The access this backend states for a request or socket: `admin` (schema,
 * permissions and account admin calls), `backend` (its own client, and
 * sessions it vouches for) or `session` (a guest's session, or a public
 * route, relayed as is).
 */
type BackendAccess = "admin" | "backend" | "session";

/** The core this backend fronts: in this process, or reached over the network. */
interface Core {
  /**
   * The app the core serves: the one `remote()` names, or the id an
   * in-process or Vercel core keeps with its data.
   */
  readonly appId: string;
  /** Where this process's own clients reach it. */
  readonly serverUrl: string;
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

function headerPairs(headers: Headers): string[][] {
  const pairs: string[][] = [];
  headers.forEach((value, name) => {
    if (!STRIPPED_HEADERS.includes(name.toLowerCase())) pairs.push([name, value]);
  });
  return pairs;
}

function coreResponse(response: {
  status: number;
  headers: string[][];
  body: Uint8Array;
}): Response {
  const headers = new Headers();
  for (const [name, value] of response.headers) headers.append(name!, value!);
  const nullBody = response.status === 204 || response.status === 304;
  return new Response(nullBody ? null : (response.body as BodyInit), {
    status: response.status,
    headers,
  });
}

function toBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (Array.isArray(data)) return Buffer.concat(data as Uint8Array[]);
  if (typeof data === "string") return new TextEncoder().encode(data);
  throw new Error("unsupported socket message");
}

function arrayBufferOf(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

/** A WebSocket-shaped session on the core, for clients in this process. */
function inProcessWebSocket(
  open: (
    url: string,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
  ) => CoreSocket,
): WebSocketConstructor {
  return class InProcessSocket implements BrowserWebSocket {
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    readyState = 0;
    private readonly listeners = new Map<string, Array<(event?: any) => void>>();
    private session: CoreSocket | undefined;

    constructor(url: string) {
      queueMicrotask(() => {
        if (this.readyState !== 0) return;
        try {
          this.session = open(url, (error, event) => this.onCoreEvent(error, event));
        } catch (error) {
          this.emit("error", error);
          this.finish(1011, error instanceof Error ? error.message : String(error));
          return;
        }
        this.readyState = 1;
        this.emit("open");
      });
    }

    addEventListener(type: string, listener: (event?: any) => void): void {
      const list = this.listeners.get(type) ?? [];
      list.push(listener);
      this.listeners.set(type, list);
    }

    send(data: Uint8Array | string): void {
      if (this.readyState !== 1 || !this.session) throw new Error("socket is not open");
      this.session.send(toBytes(data));
    }

    close(): void {
      if (this.readyState >= 2) return;
      this.session?.close();
      this.finish(1000, "client closed");
    }

    private onCoreEvent(error: Error | null, event: JazzCoreSocketEvent): void {
      if (error) {
        this.emit("error", error);
        this.finish(1011, error.message);
      } else if (event.kind === "message" && event.data) {
        this.emit("message", { data: arrayBufferOf(event.data) });
      } else if (event.kind === "close") {
        this.finish(event.code ?? 1000, event.reason ?? "");
      }
    }

    private emit(type: string, event?: unknown): void {
      for (const listener of this.listeners.get(type) ?? []) listener(event);
    }

    private finish(code: number, reason: string): void {
      if (this.readyState >= 2) return;
      this.readyState = 3;
      this.emit("close", { code, reason });
    }
  };
}

/** The app's schema versions, migrations and permissions, as its code declares them. */
async function applyBundle(
  origin: string,
  options: {
    appId: string;
    adminSecret: string;
    app: BackendSchemaInput;
    permissions: CompiledPermissionsMap;
    allowDroppingHistory?: boolean;
  },
): Promise<string> {
  const declared = resolveSchemaSource(options.app);
  const schema = mergePermissionsIntoWasmSchema(declared, {});
  await validateSchemaAndPermissions(schema, options.permissions);
  const targetSchemaHash = await computeSchemaHash(schema);
  const shipped = shippedMigrations(declared);
  const schemas = new Map<string, WasmSchema>();
  for (const version of shipped?.schemas ?? []) {
    schemas.set(version.hash, (version.schema as { tables: WasmSchema }).tables);
  }
  schemas.set(targetSchemaHash, schema);
  const server = { appId: options.appId, serverUrl: origin, adminSecret: options.adminSecret };
  const graph = await fetchMigrationGraph(server);
  const confirm = { ...server, allowDroppingHistory: options.allowDroppingHistory };
  // Published, never activated: this backend's sessions are pinned to its
  // bundle, so deployments of the app serve side by side and a rollback is
  // publishing the older bundle again.
  const deployed = await deployArtifacts(confirm, graph, {
    targetSchemaHash,
    schemas: [...schemas].map(([hash, schema]) => ({ hash, schema })),
    migrations: (shipped?.migrations ?? []).map((edge) => ({
      fromHash: edge.fromHash,
      toHash: edge.toHash,
      forward: edge.forward as PublishedTableLens[],
    })),
    permissions: options.permissions,
  }).catch((error: unknown) => {
    // A core without `/bundles` predates per-deployment permissions; it
    // must not be handed a deployment it would activate for every backend.
    if (error instanceof DeploymentError && (error.status === 404 || error.status === 405)) {
      throw new Error(
        "createJazz: the sync server predates per-deployment permissions bundles; upgrade it before this backend",
        { cause: error },
      );
    }
    throw error;
  });
  // An older core answers a publish without the bundle it made.
  if (!deployed.bundle) {
    throw new Error("createJazz: the core did not publish a permissions bundle; upgrade it");
  }
  return deployed.bundle;
}

/**
 * `socket`, its first frame (the prelude) pinned to the published bundle
 * this backend serves with. The core honours the pin only on this backend's
 * word, so whatever bundle a browser stated is replaced.
 */
export function pinnedSocket(socket: CoreSocket, bundle: () => string | undefined): CoreSocket {
  let first = true;
  return {
    send(data) {
      if (first) {
        first = false;
        const pinned = withBundle(data, bundle());
        // A prelude the pin cannot be stamped into would open unpinned.
        if (pinned === null) {
          socket.close();
          return false;
        }
        data = pinned;
      }
      return socket.send(data);
    },
    close: () => socket.close(),
  };
}

/**
 * `prelude` pinned to `bundle`, or with no pin while none is published yet
 * (the backend's own sockets while it boots): a bundle a browser stated
 * never reaches the core. `null` when the prelude is not a JSON object.
 */
function withBundle(prelude: Uint8Array, bundle: string | undefined): Uint8Array | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(prelude));
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const { bundle: _stated, ...rest } = parsed as Record<string, unknown>;
  return new TextEncoder().encode(
    JSON.stringify(bundle === undefined ? rest : { ...rest, bundle }),
  );
}

/** `bundle` is the published bundle every socket the core opens is pinned to. */
async function openInProcessCore(
  store: Extract<JazzStore, { kind: "sqlite" | "memory" }>,
  bundle: () => string | undefined,
): Promise<Core> {
  const backendSecret = randomBytes(32).toString("base64url");
  const adminSecret = randomBytes(32).toString("base64url");
  // The core holds `<path>/jazz.lock` with an OS lock while it's open.
  if (store.kind === "sqlite") refuseServerlessDisk(store.path);
  const appId = store.kind === "sqlite" ? await keptAppId(store.path) : randomUUID();
  const core = await JazzCore.open({
    appId,
    backendSecret,
    adminSecret,
    ...(store.kind === "sqlite"
      ? { dataDir: store.path, ...blobOptions(store.blobs) }
      : { inMemory: true }),
  });
  const handle = async (method: string, path: string, headers: string[][], body: Uint8Array) =>
    core.handle(method, path, headers, Buffer.from(body));
  const serverUrl = `http://jazz-${randomUUID()}.invalid`;
  const unregister = registerInProcessTransport(serverUrl, {
    fetch: async (input, init) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      return coreResponse(
        await handle(
          request.method,
          url.pathname + url.search,
          [...request.headers].map(([name, value]) => [name, value]),
          new Uint8Array(await request.arrayBuffer()),
        ),
      );
    },
    WebSocket: inProcessWebSocket((_url, onEvent) =>
      pinnedSocket(core.openSocket([], onEvent), bundle),
    ),
  });
  const opened: Core = {
    appId,
    serverUrl,
    adminSecret,
    backendSecret,
    handle,
    openSocket: (headers, onEvent) => pinnedSocket(core.openSocket(headers, onEvent), bundle),
    async close() {
      unregister();
      await core.close();
    },
  };
  return opened;
}

/**
 * The app id an in-process SQLite core keeps beside its data, in
 * `<path>/app-id`: a new one on first start, so the app's identities stay
 * the same across restarts without the app naming an id.
 */
async function keptAppId(dir: string): Promise<string> {
  const file = join(dir, "app-id");
  const kept = async (): Promise<string | undefined> => {
    try {
      return (await readFile(file, "utf8")).trim() || undefined;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      return undefined;
    }
  };
  let appId = await kept();
  if (appId === undefined) {
    await mkdir(dir, { recursive: true });
    // `wx`: two processes starting the same store at once agree on one id.
    try {
      appId = randomUUID();
      await writeFile(file, `${appId}\n`, { flag: "wx" });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      appId = (await kept())!;
    }
  }
  return appId;
}

/**
 * Serverless functions have no disk that outlives an instance (or none
 * writable at all), so a sqlite store there loses data silently.
 */
function refuseServerlessDisk(dir: string): void {
  const host = onVercelHost()
    ? "Vercel"
    : process.env.AWS_LAMBDA_FUNCTION_NAME
      ? "AWS Lambda"
      : process.env.NETLIFY
        ? "Netlify"
        : undefined;
  if (!host) return;
  throw new Error(
    `sqlite({ path: ${JSON.stringify(dir)} }) can't keep data on ${host}: its functions have no lasting disk. ` +
      (host === "Vercel" ? "Use vercel(), whose local store runs sqlite() in dev, or " : "Use ") +
      "remote({ url, appId, secret }) to a sync server or Jazz Cloud.",
  );
}

/** The access a request from this process's own clients asks for, by the secret slot it fills. */
function accessOffered(headers: Headers): BackendAccess {
  if (headers.has("x-jazz-admin-secret")) return "admin";
  if (headers.has("x-jazz-backend-secret")) return "backend";
  return "session";
}

/**
 * A core reached over the network. Every request and socket carries the
 * backend credential and the access it states; a core that trusts a workload
 * admits nothing else, and either way the secrets a request offers itself
 * never count next to it. `secrets` fill the secret slots (stand-ins unless
 * the caller holds the real ones).
 */
function openRemoteCore(
  appId: string,
  target: {
    url: string;
    backendCredential: BackendCredential;
    secrets?: { adminSecret: string; backendSecret: string };
  },
  bundle: () => string | undefined,
): Core {
  const socketUrl = `${target.url.replace(/^http/, "ws")}/apps/${encodeURIComponent(appId)}/ws`;
  if (!globalThis.WebSocket) {
    throw new Error("remote() needs a global WebSocket (Node 22 or newer)");
  }
  // Node's WebSocket (undici) takes upgrade headers as a second argument.
  const NativeWebSocket = globalThis.WebSocket as unknown as new (
    url: string,
    options?: { headers?: Record<string, string> },
  ) => WebSocket;
  const credentialHeaders = async (access: BackendAccess): Promise<string[][]> => {
    const credential = await target.backendCredential();
    if (typeof credential !== "string" || !credential) {
      throw new Error("backendCredential() returned no credential");
    }
    return access === "session"
      ? [[CREDENTIAL_HEADER, credential]]
      : [
          [CREDENTIAL_HEADER, credential],
          [ACCESS_HEADER, access],
        ];
  };
  const withoutStated = (headers: string[][]) =>
    headers.filter(([name]) => {
      const lower = name!.toLowerCase();
      return lower !== CREDENTIAL_HEADER && lower !== ACCESS_HEADER;
    });

  const core: Core = {
    appId,
    serverUrl: `http://jazz-remote-${randomUUID()}.invalid`,
    adminSecret: target.secrets?.adminSecret ?? STAND_IN_SECRET,
    backendSecret: target.secrets?.backendSecret ?? STAND_IN_SECRET,
    async handle(method, path, headers, body, access = "session") {
      const response = await fetch(`${target.url}${path}`, {
        method,
        headers: [...withoutStated(headers), ...(await credentialHeaders(access))].map(
          ([name, value]) => [name!, value!] as [string, string],
        ),
        ...(method === "GET" || method === "HEAD" ? {} : { body: body as BodyInit }),
      });
      const replyHeaders: string[][] = [];
      response.headers.forEach((value, name) => {
        // fetch already decoded the body.
        if (name !== "content-encoding" && name !== "content-length") {
          replyHeaders.push([name, value]);
        }
      });
      return {
        status: response.status,
        headers: replyHeaders,
        body: new Uint8Array(await response.arrayBuffer()),
      };
    },
    openSocket: (_headers, onEvent, access = "session") =>
      pinnedSocket(connect(socketUrl, onEvent, access), bundle),
    async close() {
      unregister();
    },
  };
  // Browsers' own headers stay here: the core sees only this backend.
  function connect(
    url: string,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
    access: BackendAccess,
  ): CoreSocket {
    let ws: WebSocket | undefined;
    const queued: Uint8Array[] = [];
    let ended = false;
    const finish = (code: number, reason: string) => {
      if (ended) return;
      ended = true;
      onEvent(null, { kind: "close", code, reason });
    };
    credentialHeaders(access)
      .then((headers) => {
        if (ended) return;
        ws = new NativeWebSocket(url, { headers: Object.fromEntries(headers) });
        ws.binaryType = "arraybuffer";
        ws.addEventListener("open", () => {
          for (const data of queued.splice(0)) ws!.send(data as Uint8Array<ArrayBuffer>);
        });
        ws.addEventListener("message", (event) => {
          const data =
            typeof event.data === "string"
              ? new TextEncoder().encode(event.data)
              : new Uint8Array(event.data as ArrayBuffer);
          onEvent(null, { kind: "message", data });
        });
        ws.addEventListener("close", (event) => finish(event.code, event.reason));
        // A socket that never connects (the core is down or restarting) can
        // report only the error, and stay CONNECTING: without this, the link
        // waits on it forever instead of retrying.
        ws.addEventListener("error", () => {
          finish(1006, "connection to the core failed");
          try {
            ws?.close();
          } catch {
            // Already closing.
          }
        });
      })
      // A failing credential hook, or a socket that can't even be created.
      .catch((error: unknown) =>
        finish(1011, error instanceof Error ? error.message : "no backend credential"),
      );
    return {
      send(data) {
        if (ended) return false;
        if (ws?.readyState === 1) ws.send(data as Uint8Array<ArrayBuffer>);
        else queued.push(data);
        return true;
      },
      close() {
        ended = true;
        ws?.close();
      },
    };
  }
  // This process's own clients (the backend's session) reach the core
  // through an in-process origin, so they present the credential too.
  const unregister = registerInProcessTransport(core.serverUrl, {
    fetch: async (input, init) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      const headers: string[][] = [];
      request.headers.forEach((value, name) => headers.push([name, value]));
      return coreResponse(
        await core.handle(
          request.method,
          url.pathname + url.search,
          headers,
          new Uint8Array(await request.arrayBuffer()),
          accessOffered(request.headers),
        ),
      );
    },
    WebSocket: inProcessWebSocket((url, onEvent) => {
      const { pathname, search } = new URL(url);
      return pinnedSocket(
        connect(`${target.url.replace(/^http/, "ws")}${pathname}${search}`, onEvent, "backend"),
        bundle,
      );
    }),
  });
  return core;
}

/**
 * A request `authenticate` names no user for: what an app answers with 401.
 * (A throwing `authenticate` passes its own error through instead.)
 */
function signedOut(operation: string): RequestAuthenticationError {
  return new RequestAuthenticationError(`${operation}: the request has no signed-in user`, {
    code: "signed_out",
  });
}

/** How long `close()` waits for pending writes to reach the core. */
const CLOSE_SYNC_TIMEOUT_MS = 10_000;

/** What `createJazz` derives from its options, checked when it is called. */
interface JazzSettings {
  basePath: string;
  publicUrls: string[];
  allowedOrigins: Set<string>;
}

function jazzSettings(options: CreateJazzOptions): JazzSettings {
  const basePath = (options.basePath ?? DEFAULT_BASE_PATH).replace(/\/+$/, "");
  const publicUrls = [options.publicUrl ?? []].flat().map((value) => {
    try {
      // The same form a browser client scopes itself by (`clientScope`).
      if (!/^(https?|wss?):\/\//.test(value)) throw new Error("relative");
      return endpointScope(value);
    } catch {
      throw new Error(
        `createJazz: publicUrl ${JSON.stringify(value)} is not an absolute URL like "https://app.example.com/jazz"`,
      );
    }
  });
  const allowedOrigins = new Set(
    (options.allowedOrigins ?? []).map((origin) => {
      let parsed: string | undefined;
      try {
        parsed = new URL(origin).origin;
      } catch {
        // Not a URL: refused below.
      }
      // "localhost:3000" parses with an opaque "null" origin, which would let
      // sandboxed iframes and file:// pages through.
      if (!parsed || parsed === "null") {
        throw new Error(
          `createJazz: allowedOrigins entry ${JSON.stringify(origin)} is not an origin like "https://app.example.com"`,
        );
      }
      return parsed;
    }),
  );
  if ("appId" in options) {
    throw new Error(
      "createJazz(): appId moved to the store: remote({ url, appId, secret }) names the app it serves, and sqlite(), memory() and vercel() keep their own id.",
    );
  }
  return { basePath, publicUrls, allowedOrigins };
}

/** The store this process runs: `vercel()` outside a Vercel deployment runs its `local` store. */
function runningStore(store: JazzStore): JazzStore {
  return store.kind === "vercel" && !onVercelDeployment() ? store.local : store;
}

/** A Jazz with its store open: what a `createJazz` facade hands each call to. */
interface OpenedJazz extends Omit<Jazz, "ready" | "db"> {
  readonly db: Db;
  /** Apply a later `createJazz` call's app, permissions and options to this open store. */
  reload(options: CreateJazzOptions): Promise<void>;
}

/**
 * Open `store` and apply the schema, migrations and permissions `options`
 * declare to its core.
 */
async function openJazz(initial: CreateJazzOptions, store: JazzStore): Promise<OpenedJazz> {
  let options = initial;
  let { basePath, publicUrls, allowedOrigins } = jazzSettings(options);
  // The bundle this backend published: every socket to the core carries it,
  // and the core governs this backend's sessions by its permissions alone.
  let pin: string | undefined;
  const bundle = () => pin;
  const core =
    store.kind === "remote"
      ? openRemoteCore(store.appId, store, bundle)
      : store.kind === "vercel"
        ? await openVercelCore({
            options: store.options,
            // The function's OIDC token is the core's only credential.
            openRemote: ({ appId, url, backendCredential }) =>
              openRemoteCore(appId, { url, backendCredential }, bundle),
          })
        : await openInProcessCore(store, bundle);
  const appId = core.appId;
  const issuer = BACKEND_USER_ISSUER;

  /** Publish `next`'s bundle: what it is pinned by. Publishing one already published changes nothing. */
  const publish = (next: CreateJazzOptions) =>
    applyBundle(core.serverUrl, { ...next, appId, adminSecret: core.adminSecret });

  /** Publish `next`'s bundle and open the backend's own session on it. */
  async function backendSession(
    next: CreateJazzOptions,
    published?: string,
  ): Promise<{ session: JazzSession<JazzClient>; client: JazzClient }> {
    pin = published ?? (await publish(next));
    const session = await openJazzSession({
      appId,
      app: next.app,
      permissions: next.permissions,
      serverUrl: core.serverUrl,
      driver: { type: "memory" },
      initial: { backendSecret: core.backendSecret },
    });
    const opened = session.getSnapshot();
    if (opened.status !== "ready" || !opened.client) {
      await session.close();
      throw opened.error ?? new Error(`Jazz backend session did not open (${opened.status})`);
    }
    return { session, client: opened.client };
  }

  let session: JazzSession<JazzClient>;
  let backend: JazzClient;
  try {
    ({ session, client: backend } = await backendSession(options));
  } catch (error) {
    await core.close();
    throw error;
  }

  let backendClients = backendClientVerifier(options.backendClients ?? []);

  /**
   * A later `createJazz` for this store (a dev server's hot reload): publish
   * its bundle, move the backend's own database to it, and read its options
   * from now on. Open sync sessions stay; the core reconnects those pinned
   * to the old bundle.
   */
  async function reload(next: CreateJazzOptions): Promise<void> {
    const settings = jazzSettings(next);
    // The same app and permissions (a reload of a module that imports them
    // unchanged, or a hot reload that rebuilt equal ones): the bundle, and
    // the backend's session with its writes and `forReqUser` dbs, stay.
    if (next.app !== options.app || next.permissions !== options.permissions) {
      const published = await publish(next);
      if (published !== pin) {
        const previousPin = pin;
        let fresh: { session: JazzSession<JazzClient>; client: JazzClient };
        try {
          fresh = await backendSession(next, published);
        } catch (error) {
          pin = previousPin;
          throw error;
        }
        retire(session);
        ({ session, client: backend } = fresh);
      }
    }
    options = next;
    ({ basePath, publicUrls, allowedOrigins } = settings);
    backendClients = backendClientVerifier(next.backendClients ?? []);
    upgradesSockets = attached || (next.upgradesSockets ?? false);
  }

  /**
   * Sessions a reload replaced. Each stays open for
   * `RETIRED_SESSION_GRACE_MS`, so the writes it has in flight and the
   * `forReqUser` dbs requests still hold finish, then closes (draining its
   * writes); a close of this Jazz closes those left.
   */
  const retiring = new Set<() => Promise<void>>();
  function retire(old: JazzSession<JazzClient>): void {
    let closed: Promise<void> | undefined;
    const closeOld = (): Promise<void> => {
      clearTimeout(timer);
      return (closed ??= old
        .close()
        .catch((error: unknown) => console.error("jazz: a retired session did not close", error))
        .finally(() => retiring.delete(closeOld)));
    };
    const timer = setTimeout(closeOld, RETIRED_SESSION_GRACE_MS);
    (timer as { unref?: () => void }).unref?.();
    retiring.add(closeOld);
  }

  /**
   * Who sent `request`: a scoped backend client's user, or the user the
   * app's own `authenticate` names. A trusted backend client has no user.
   */
  async function authenticate(request: Request): Promise<JazzUser | null> {
    const client = await backendClients.resolve(request);
    if (client !== null) return client !== "refused" && client.kind === "user" ? client.user : null;
    const answer = await askAuthenticate(request);
    return answer === LOCAL_FIRST_GUEST ? null : (answer as JazzUser | null);
  }

  async function askAuthenticate(request: Request): Promise<JazzUser | LocalFirstGuest | null> {
    if (!options.authenticate) return null;
    return (await options.authenticate(request)) ?? null;
  }

  /** Whether the app lets `request` in as a local-first guest (`acceptLocalFirstAuth`). */
  let warnedGuestsOff = false;
  async function acceptsGuest(request: Request): Promise<boolean> {
    if ((await askAuthenticate(request)) === LOCAL_FIRST_GUEST) return true;
    if (!warnedGuestsOff) {
      warnedGuestsOff = true;
      console.warn(
        "createJazz: refused a local-first guest. To let guests sync, return acceptLocalFirstAuth(request) from authenticate for them.",
      );
    }
    return false;
  }

  /** One admin account request to the core: this backend vouches for the identities. */
  async function adminAccounts(operation: string, body: unknown): Promise<{ account: string }> {
    const response = await core.handle(
      "POST",
      `/apps/${encodeURIComponent(accountAppId(appId))}/admin/accounts/${operation}`,
      [
        ["x-jazz-admin-secret", core.adminSecret],
        ["content-type", "application/json"],
      ],
      new TextEncoder().encode(JSON.stringify(body)),
      "admin",
    );
    const text = new TextDecoder().decode(response.body);
    if (response.status !== 200) {
      throw Object.assign(
        new Error(`Jazz account ${operation} failed (${response.status}): ${text}`),
        {
          status: response.status,
          code: text.trim(),
        },
      );
    }
    const parsed = JSON.parse(text) as { account: unknown };
    return { ...parsed, account: String(parsed.account) };
  }

  const identity = (userId: string) => ({ issuer, subject: userId });

  // A user's account never changes once assigned (linking only adds
  // identities to an account), so the core is asked once per user for a
  // while, not on every sign-in, socket and request. Only assignments are
  // cached: a user without an account may get one at any moment.
  const accounts = new Map<string, { account: string; until: number }>();
  const ACCOUNT_CACHE_LIMIT = 10_000;
  function remember(userId: string, account: string): string {
    if (accounts.size >= ACCOUNT_CACHE_LIMIT) {
      accounts.delete(accounts.keys().next().value!);
    }
    accounts.set(userId, {
      account,
      until: Date.now() + (options.sessionTtlSeconds ?? 3600) * 1000,
    });
    return account;
  }

  /**
   * The user's account, or null while they have none. Never creates one:
   * only a client's sign-in (`backend-sign-in`), a guest's link
   * (`backend-link`) and `createAccountFor` do, so a local-first guest can
   * still link to a user the backend has only looked up or acted for (V9).
   */
  async function existingAccountOf(userId: string): Promise<string | null> {
    const cached = accounts.get(userId);
    if (cached && cached.until > Date.now()) return cached.account;
    try {
      const { account } = await adminAccounts("resolve", { identity: identity(userId) });
      return remember(userId, account);
    } catch (error) {
      // Only the core's own "no account for this identity" answer: any other
      // 404 (an unknown app, a missing route) is a server error, not a user
      // without an account.
      const { status, code } = error as { status?: number; code?: string };
      if (status === 404 && code === "identity_not_assigned") return null;
      throw error;
    }
  }

  /** The user's account, created if they have none (a sign-in, or `createAccountFor`). */
  async function accountOf(userId: string): Promise<string> {
    const cached = accounts.get(userId);
    if (cached && cached.until > Date.now()) return cached.account;
    const { account } = await adminAccounts("login-or-register", { identity: identity(userId) });
    return remember(userId, account);
  }

  /** The user's session, on the account they already have. */
  async function sessionFor(user: JazzUser, caller: string): Promise<Session> {
    const account = await existingAccountOf(user.userId);
    if (account === null) throw new NoJazzAccountError(user.userId, caller);
    return {
      account_id: account,
      issuer,
      user_id: user.userId,
      claims: user.claims ?? {},
      authMode: "external",
    };
  }

  async function linkIdentity(link: { from: string; to: string }): Promise<{ account: string }> {
    if (link.from === link.to) throw new Error("linkIdentity: from and to are the same identity");
    const { account } = await adminAccounts("link", {
      from: identity(link.from),
      to: identity(link.to),
    });
    return { account };
  }

  /**
   * The account sign-in a browser makes with only the app's own session: the
   * backend's `authenticate` names the user, and the core resolves (or
   * creates) their account.
   */
  async function backendSignIn(request: Request): Promise<Response> {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const user = await authenticate(request);
    if (!user) return new Response("signed_out", { status: 401 });
    const account = await accountOf(user.userId);
    const expiresAt = Math.floor(Date.now() / 1000) + (options.sessionTtlSeconds ?? 3600);
    return Response.json(
      { account, identity: identity(user.userId), claims: user.claims ?? {}, expiresAt },
      { headers: { "Cache-Control": "no-store" } },
    );
  }

  /**
   * Who `request`'s user is, without changing anything: the user
   * `authenticate` names, and whether the core already holds an account for
   * them. A browser's session asks this to follow the backend; only a sign-in
   * (`backend-sign-in`) or a link (`backend-link`) assigns an account, so a
   * local-first guest whose user has none yet can still link to them.
   */
  async function backendUser(request: Request): Promise<Response> {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const user = await authenticate(request);
    if (!user) return new Response("signed_out", { status: 401 });
    let account: string | null;
    try {
      account = await existingAccountOf(user.userId);
    } catch (error) {
      // The core refuses this identity (it was revoked): say so, as the
      // core does, rather than fail as a server error.
      if ((error as { status?: number }).status === 403)
        return new Response("identity_not_authorized", { status: 403 });
      throw error;
    }
    return Response.json(
      { account, identity: identity(user.userId), claims: user.claims ?? {} },
      { headers: { "Cache-Control": "no-store" } },
    );
  }

  /**
   * Link challenges: a guest links only on a proof made for one challenge,
   * never on its sync token, so a leaked token can't link it to someone
   * else. A challenge names the user it links to and expires quickly; it is
   * signed with a key only this app's backend instances share (derived from
   * the core's admin secret), and its nonce becomes the link's single-use
   * intent id at the core, so a proof works once.
   */
  const challengeKey = createHmac("sha256", core.adminSecret)
    .update(`jazz-link-challenge-v1\0${accountAppId(appId)}`)
    .digest();
  const LINK_CHALLENGE_TTL_SECONDS = 120;
  const challengeMac = (payload: string) =>
    createHmac("sha256", challengeKey).update(payload).digest("base64url");

  function issueLinkChallenge(userId: string): { challenge: string; expiresAt: number } {
    const expiresAt = Math.floor(Date.now() / 1000) + LINK_CHALLENGE_TTL_SECONDS;
    const payload = Buffer.from(
      JSON.stringify({ v: 1, sub: userId, nonce: randomUUID(), exp: expiresAt }),
    ).toString("base64url");
    return { challenge: `${payload}.${challengeMac(payload)}`, expiresAt };
  }

  /** The nonce of a challenge this backend issued for `userId`, still fresh; else null. */
  function readLinkChallenge(challenge: unknown, userId: string): string | null {
    if (typeof challenge !== "string" || challenge.length > 1024) return null;
    const [payload, mac, extra] = challenge.split(".");
    if (!payload || !mac || extra !== undefined) return null;
    const expected = Buffer.from(challengeMac(payload));
    const given = Buffer.from(mac);
    if (expected.length !== given.length || !timingSafeEqual(expected, given)) return null;
    let claims: { v?: unknown; sub?: unknown; nonce?: unknown; exp?: unknown };
    try {
      claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    } catch {
      return null;
    }
    if (
      claims.v !== 1 ||
      claims.sub !== userId ||
      typeof claims.nonce !== "string" ||
      typeof claims.exp !== "number" ||
      claims.exp <= Date.now() / 1000
    ) {
      return null;
    }
    return claims.nonce;
  }

  /** A challenge for the guest to sign before it links to the signed-in user. */
  async function backendLinkChallenge(request: Request): Promise<Response> {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const user = await authenticate(request);
    if (!user) return new Response("signed_out", { status: 401 });
    return Response.json(issueLinkChallenge(user.userId), {
      headers: { "Cache-Control": "no-store" },
    });
  }

  /**
   * A local-first guest who just signed up: link the guest's identity to the
   * user `authenticate` now names, so the account and its data carry over.
   * The guest proves itself by signing a challenge issued for this user
   * (see {@link issueLinkChallenge}).
   */
  async function backendLink(request: Request): Promise<Response> {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const user = await authenticate(request);
    if (!user) return new Response("signed_out", { status: 401 });
    let body: { challenge?: unknown; proof?: unknown; audience?: unknown };
    try {
      body = (await request.json()) as typeof body;
    } catch {
      return new Response("local_first_proof_required", { status: 400 });
    }
    const nonce = readLinkChallenge(body?.challenge, user.userId);
    if (!nonce) return new Response("invalid_link_challenge", { status: 400 });
    let guest: { issuer: string; subject: string };
    try {
      const proven = verifyChallenge(String(body.proof ?? ""), body.challenge as string);
      guest = { issuer: proven.issuer, subject: proven.subject };
    } catch {
      return new Response("invalid_local_first_proof", { status: 401 });
    }
    // The guest's account follows from its key and the namespace it was
    // founded in: this app's id, or one of this endpoint's URLs for an
    // app-less client. Any other namespace is another app's guest.
    const founding = linkAudience(request, body.audience);
    if (!founding) return new Response("invalid_local_first_proof", { status: 401 });
    const account = localFirstAccountId(founding.endpoint ?? appId, guest.subject);
    const userIs = async (expected: string) =>
      (await adminAccounts("resolve", { identity: identity(user.userId) })).account === expected;
    let guestRevoked = false;
    try {
      try {
        await adminAccounts("found-local-first", {
          identity: guest,
          ...(founding.endpoint !== undefined ? { audience: founding.endpoint } : {}),
        });
      } catch (error) {
        // A guest key an earlier link retired: only that link's own user
        // gets an answer (the retry of a link whose response was lost).
        if ((error as { status?: number }).status !== 403 || !(await userIs(account))) throw error;
        guestRevoked = true;
      }
      if (!guestRevoked) {
        try {
          await adminAccounts("link", { from: guest, to: identity(user.userId), nonce });
        } catch (error) {
          // Linking is idempotent for this guest and this user: when the user
          // already belongs to the guest's account (an earlier link went
          // through, then perhaps its revoke failed, or another tab's link
          // won the race and revoked the key since), this link is done. A
          // used challenge (the core refuses its nonce first) and a user of
          // another account are still refused.
          const status = (error as { status?: number }).status;
          if ((status !== 409 && status !== 403) || !(await userIs(account))) throw error;
          // Refused for a retired key: the link that won already retired it.
          if (status === 403) guestRevoked = true;
        }
      }
    } catch (error) {
      const status = (error as { status?: number }).status;
      // Only the core's own refusals refuse the link for good: an invalid
      // link intent (400), a retired or foreign identity (403), a user with
      // no account to check (404), an identity of another account (409).
      // Its reason stays on the backend.
      if (status === 400 || status === 403 || status === 404 || status === 409)
        return new Response("link_refused", { status: 409 });
      // Anything else (the core down or failing, its credential check
      // refusing this backend mid-rotation, a proxy's 408 or 429) may pass on
      // a retry, so say so rather than strand the guest.
      console.error("Jazz: linking a guest failed on the core", error);
      return new Response("account_registry_unavailable", { status: 503 });
    }
    if (options.guestKeyAfterLink === "revoke" && !guestRevoked) {
      // The link already stands. If the revoke fails, the guest key stays
      // usable until the browser retries the link, which is idempotent and
      // revokes then: a server error, logged here, not a silent success.
      const revoked = await core.handle(
        "POST",
        `/apps/${encodeURIComponent(accountAppId(appId))}/admin/accounts/revoke`,
        [
          ["x-jazz-admin-secret", core.adminSecret],
          ["content-type", "application/json"],
        ],
        new TextEncoder().encode(
          JSON.stringify({ approver: identity(user.userId), target: guest }),
        ),
        "admin",
      );
      if (revoked.status !== 204) {
        console.error(
          `Jazz: revoking the guest key after a link failed (${revoked.status}): ${new TextDecoder().decode(revoked.body)}`,
        );
        return new Response("guest_key_revoke_failed", { status: 500 });
      }
    }
    const expiresAt = Math.floor(Date.now() / 1000) + (options.sessionTtlSeconds ?? 3600);
    return Response.json(
      { account, identity: identity(user.userId), claims: user.claims ?? {}, expiresAt },
      { headers: { "Cache-Control": "no-store" } },
    );
  }

  /**
   * A browser's sync session. The browser presents no credential: its first
   * message (the auth prelude) names the user it means to act as, and the
   * backend's own `authenticate` decides whether this request is that user.
   * The core then admits the session on the backend's word, with the user's
   * permissions. It ends after `sessionTtlSeconds`, and the client
   * reconnects and is asked again.
   */
  interface ClientSession {
    send(data: Uint8Array): boolean;
    close(): void;
    /** The user the session was admitted as, once it is. */
    readonly userId: string | undefined;
    /** Whether a trusted backend client opened it. */
    readonly trusted: boolean;
  }

  function openClientSession(
    request: Request,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
  ): ClientSession {
    // The core socket opens once the prelude is admitted, with the access
    // that admission needs: a vouched user's session is the backend's, and
    // so is the endpoint it vouches for on a guest's behalf; a guest signed
    // for the app itself rides on its own token alone.
    let socket: CoreSocket | undefined;
    let ended = false;
    let userId: string | undefined;
    let trusted = false;
    let recheck: (() => void) | undefined;
    // A session that never sends its prelude ends too.
    let expiry: ReturnType<typeof setTimeout> | undefined = setTimeout(() => {
      onEvent(null, { kind: "close", code: 1008, reason: "no sign-in in time" });
      end();
    }, PRELUDE_DEADLINE_MS);
    (expiry as unknown as { unref?: () => void }).unref?.();
    const end = () => {
      if (ended) return;
      ended = true;
      if (expiry) clearTimeout(expiry);
      recheck?.();
      socket?.close();
    };
    const admit = async (prelude: Uint8Array): Promise<boolean> => {
      const admitted = await admitPrelude(request, prelude);
      if (!admitted || ended) {
        // An auth failure, not a dropped link: the client stops retrying and
        // reports `offline: auth`, and its session asks the backend whether
        // anyone is still signed in.
        if (!ended)
          onEvent(null, {
            kind: "message",
            data: authFailedBatch("session missing: not signed in"),
          });
        onEvent(null, { kind: "close", code: 1008, reason: "not signed in" });
        end();
        return false;
      }
      userId = admitted.userId;
      trusted = admitted.trusted;
      if (expiry) clearTimeout(expiry);
      // A backend client's session ends with its token, so it reconnects
      // with a fresh one.
      const ttl = Math.min(
        (options.sessionTtlSeconds ?? 3600) * 1000,
        admitted.expiresAt === undefined ? Infinity : admitted.expiresAt * 1000 - Date.now(),
      );
      expiry = setTimeout(end, Math.max(ttl, 0));
      (expiry as unknown as { unref?: () => void }).unref?.();
      const every = (options.recheckSeconds ?? 60) * 1000;
      // The socket's request carries the cookies it opened with, so a
      // refusal may only mean they expired. The client reconnects with fresh
      // ones; if no one is signed in, that admission fails as an auth
      // failure (above), and otherwise it carries the user's claims now.
      const authenticateAgain = () => {
        if (ended) return;
        onEvent(null, { kind: "close", code: 1008, reason: "authenticate again" });
        end();
      };
      const admittedUser = admitted.user;
      if (admittedUser && every > 0) {
        // Signed out, someone else, or the same user with other claims.
        recheck = recheckEvery(
          every,
          async () => isSameAdmission(admittedUser, await authenticate(request)),
          authenticateAgain,
        );
      } else if (admitted.guest && every > 0) {
        // The app may stop letting guests in (or this one): ask again, as for users.
        recheck = recheckEvery(every, () => acceptsGuest(request), authenticateAgain);
      }
      socket = core.openSocket(headerPairs(request.headers), onEvent, admitted.access);
      return socket.send(admitted.prelude);
    };
    // Messages after the prelude wait for its admission, in order.
    let chain: Promise<boolean> | undefined;
    return {
      send(data) {
        if (ended) return false;
        chain = chain
          ? chain.then((ok) => ok && !ended && !!socket?.send(data))
          : admit(data).catch((error: unknown) => {
              if (!ended && isRefusal(error)) {
                // The core refused this identity (revoked, local-first auth
                // disabled, invalid): an auth failure the client must not
                // retry, reported as a sign-in refusal is.
                onEvent(null, {
                  kind: "message",
                  data: authFailedBatch(`session refused: ${refusalCode(error)}`),
                });
                onEvent(null, { kind: "close", code: 1008, reason: "sign-in refused" });
              } else {
                // The core is unreachable or failed (5xx), or the app's own
                // `authenticate` threw: the client tries again later.
                onEvent(null, { kind: "close", code: 1011, reason: "admission failed" });
              }
              end();
              return false;
            });
        return true;
      },
      close: end,
      get userId() {
        return userId;
      },
      get trusted() {
        return trusted;
      },
    };
  }

  /**
   * Turn a browser prelude into one the core admits on this backend's word:
   * the user `authenticate` finds on this request, with their account and
   * claims, vouched for with the backend secret. A local-first guest's
   * prelude passes through: the core verifies its self-signed token. Any
   * other credential, or a prelude naming another user, is refused.
   */
  async function admitPrelude(
    request: Request,
    prelude: Uint8Array,
  ): Promise<{
    prelude: Uint8Array;
    userId: string | undefined;
    /** The user (with the claims) `authenticate` named for a vouched session. */
    user?: JazzUser;
    access: BackendAccess;
    trusted: boolean;
    expiresAt?: number;
    /** A local-first guest, let in by `acceptLocalFirstAuth`. */
    guest?: true;
  } | null> {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(new TextDecoder().decode(prelude)) as Record<string, unknown>;
    } catch {
      return null;
    }
    const auth = (parsed.auth && typeof parsed.auth === "object" ? parsed.auth : parsed) as Record<
      string,
      unknown
    >;
    const client = await backendClients.resolve(request);
    if (client === "refused") return null;
    if (client?.kind === "backend") {
      // A trusted backend client syncs with this backend's authority: it
      // states a backend secret (any stand-in) and, to act for a user, that
      // user's session, as the main backend's own client does (the user's
      // policies then apply to those writes at the core). Never an admin
      // secret or a token of its own.
      for (const key of ["admin_secret", "jwt_token"]) {
        if (parsed[key] != null || auth[key] != null) return null;
      }
      if (parsed.backend_secret == null && auth.backend_secret == null) return null;
      const vouched = { backend_secret: core.backendSecret };
      const rewritten = { ...parsed, ...vouched, auth: { ...auth, ...vouched } };
      return {
        prelude: new TextEncoder().encode(JSON.stringify(rewritten)),
        userId: undefined,
        access: "backend",
        trusted: true,
        expiresAt: client.expiresAt,
      };
    }
    const others = ["backend_secret", "admin_secret", "backend_session"];
    // Browser worker transports send `jwt_token: null` alongside a backend
    // session; only a real token counts as one.
    const token = auth.jwt_token ?? parsed.jwt_token;
    if (token !== undefined && token !== null) {
      const localFirst =
        typeof token === "string" && parseJwtPayload(token)?.iss === LOCAL_FIRST_JWT_ISSUER;
      const alone = others.every((key) => parsed[key] == null && auth[key] == null);
      if (!localFirst || !alone || parsed.local_first_audience != null) return null;
      // A guest has no user here: its session is bound to its own key.
      if (client) return null;
      const audience = guestAudience(request, token);
      if (!audience) return null;
      // Guests are off unless the app's `authenticate` lets this one in.
      if (!(await acceptsGuest(request))) return null;
      if (audience.endpoint === undefined) {
        // Its own token decides who it is; the backend's secret only vouches
        // for the bundle this socket is pinned to (see `pinnedSocket`).
        const pinned = { ...parsed, auth: { ...auth, backend_secret: core.backendSecret } };
        return {
          prelude: new TextEncoder().encode(JSON.stringify(pinned)),
          userId: undefined,
          access: "session",
          trusted: false,
          guest: true,
        };
      }
      // Signed for this endpoint, which the core can't know: vouch for it.
      const vouched = {
        ...parsed,
        local_first_audience: audience.endpoint,
        auth: { ...auth, backend_secret: core.backendSecret },
      };
      // The backend's word on the endpoint needs backend access; the
      // guest's own token still decides who it is.
      return {
        prelude: new TextEncoder().encode(JSON.stringify(vouched)),
        userId: undefined,
        access: "backend",
        trusted: false,
        guest: true,
      };
    }
    const stated = auth.backend_session as { user_id?: unknown } | undefined;
    if (!stated || typeof stated !== "object") return null;
    for (const credential of ["backend_secret", "admin_secret"]) {
      if (parsed[credential] != null || auth[credential] != null) return null;
    }
    const user = await authenticate(request);
    if (!user || user.userId !== stated.user_id) return null;
    // A browser states a user's session only once its sign-in (or link)
    // gave them an account; this never makes one.
    let session: Session;
    try {
      session = await sessionFor(user, "a sync session");
    } catch (error) {
      if (error instanceof NoJazzAccountError) return null;
      throw error;
    }
    const vouched = { backend_secret: core.backendSecret, backend_session: session };
    const rewritten = { ...parsed, ...vouched, auth: { ...auth, ...vouched } };
    return {
      prelude: new TextEncoder().encode(JSON.stringify(rewritten)),
      userId: user.userId,
      user,
      access: "backend",
      trusted: false,
      expiresAt: client?.expiresAt,
    };
  }

  // HTTP stream sessions, by id, with the key that proves a request comes
  // from the client that opened them.
  const streams = new Map<
    string,
    { session: ClientSession; key: Buffer; end(code: number, reason: string): void }
  >();
  // Sockets accepted for clients, so close() can end them.
  const clientSockets = new Set<WsLikeSocket>();

  function openStream(request: Request): Response {
    const id = randomUUID();
    // The id goes in URLs (and so in logs); the key only in a header.
    const key = randomBytes(32);
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    let closed = false;
    const end = (code: number, reason: string) => {
      if (closed) return;
      closed = true;
      streams.delete(id);
      try {
        controller.enqueue(encodeStreamClose(code, reason));
        controller.close();
      } catch {
        // The client already went away.
      }
    };
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c;
      },
      cancel() {
        streams.get(id)?.session.close();
        end(1001, "client went away");
      },
    });
    let socket: ClientSession;
    try {
      socket = openClientSession(request, (error, event) => {
        if (error) return end(1011, error.message);
        if (event.kind === "message" && event.data) {
          if (!closed) controller.enqueue(encodeStreamRecord(STREAM_RECORD_MESSAGE, event.data));
        } else if (event.kind === "close") {
          end(event.code ?? 1000, event.reason ?? "");
        }
      });
    } catch (error) {
      return new Response(error instanceof Error ? error.message : String(error), { status: 503 });
    }
    streams.set(id, { session: socket, key, end });
    controller.enqueue(
      encodeStreamRecord(
        STREAM_RECORD_SESSION,
        new TextEncoder().encode(JSON.stringify({ id, key: key.toString("base64url") })),
      ),
    );
    return new Response(body, {
      headers: {
        "Content-Type": "application/octet-stream",
        "Cache-Control": "no-store",
        "X-Accel-Buffering": "no",
      },
    });
  }

  /**
   * The stream session `id`, when `request` comes from the client that
   * opened it (it carries the session's key) and, once the session is
   * admitted as a user, from that user still: a session id alone (which
   * can end up in logs) doesn't let anyone act in it, before admission or
   * for a guest either.
   */
  async function ownStream(id: string, request: Request): Promise<ClientSession | Response> {
    const entry = streams.get(id);
    if (!entry) {
      // Sessions live in the process that opened them.
      return new Response(
        "unknown stream session: HTTP stream sessions need a single backend instance or sticky routing",
        { status: 404 },
      );
    }
    const given = Buffer.from(request.headers.get(STREAM_KEY_HEADER) ?? "", "base64url");
    if (given.length !== entry.key.length || !timingSafeEqual(given, entry.key)) {
      return new Response("not this stream's client", { status: 403 });
    }
    const socket = entry.session;
    if (socket.trusted) {
      const client = await backendClients.resolve(request);
      if (client === null || client === "refused" || client.kind !== "backend") {
        return new Response("not this backend client", { status: 403 });
      }
    } else if (socket.userId !== undefined) {
      const user = await authenticate(request);
      if (user?.userId !== socket.userId) return new Response("not signed in", { status: 403 });
    }
    return socket;
  }

  async function streamSend(id: string, request: Request): Promise<Response> {
    const socket = await ownStream(id, request);
    if (socket instanceof Response) return socket;
    const reader = new StreamRecordReader();
    const records = reader.push(new Uint8Array(await request.arrayBuffer()));
    if (reader.pending > 0) return new Response("truncated record", { status: 400 });
    for (const record of records) {
      if (
        record.kind !== STREAM_RECORD_MESSAGE ||
        record.payload.byteLength > MAX_STREAM_MESSAGE_BYTES
      ) {
        socket.close();
        return new Response("invalid record", { status: 400 });
      }
      if (!socket.send(record.payload)) return new Response("session ended", { status: 410 });
    }
    return new Response(null, { status: 204 });
  }

  /** Close codes a socket may send: 1005/1006/1015 only describe a close. */
  function sendableCloseCode(code: number | undefined): number {
    if (code === undefined || code === 1005) return 1000;
    if (code === 1006 || code === 1015) return 1011;
    return code;
  }

  function acceptSocket(request: Request, ws: WsLikeSocket): void {
    if (!originAllowed(request)) {
      ws.close(1008, "origin not allowed");
      return;
    }
    let socket: ClientSession;
    try {
      socket = openClientSession(request, (error, event) => {
        if (error) return ws.close(1011, error.message);
        if (event.kind === "message" && event.data) ws.send(event.data);
        else if (event.kind === "close") {
          ws.close(sendableCloseCode(event.code), event.reason ?? "");
        }
      });
    } catch (error) {
      ws.close(1013, error instanceof Error ? error.message : String(error));
      return;
    }
    clientSockets.add(ws);
    ws.on("message", (data) => {
      if (!socket.send(toBytes(data))) ws.close(1000, "session ended");
    });
    ws.on("close", () => {
      clientSockets.delete(ws);
      socket.close();
    });
    ws.on("error", () => socket.close());
  }

  /**
   * Whether a browser on the request's origin may act as its user here.
   * Sign-in and sync ride the app's own session (usually a cookie), which a
   * browser also attaches to requests that other pages start, so a request
   * from another origin is refused: one with no Origin (a non-browser
   * client, which holds no ambient cookie) passes. The backend's own origin
   * is the host the request was sent to, as a proxy forwards it.
   */
  /** This process upgrades sockets for handle() and nodeHandler (set by attach()). */
  let upgradesSockets = options.upgradesSockets ?? false;
  let attached = false;

  function originAllowed(request: Request): boolean {
    const origin = request.headers.get("origin");
    if (origin === null) {
      const site = request.headers.get("sec-fetch-site");
      return site === null || site === "same-origin" || site === "none";
    }
    if (allowedOrigins.has(origin)) return true;
    let parsed: URL;
    try {
      parsed = new URL(origin);
    } catch {
      return false; // "null" (a sandboxed or opaque origin) or garbage
    }
    const requestHost =
      request.headers.get("x-forwarded-host")?.split(",")[0]?.trim() ||
      request.headers.get("host") ||
      new URL(request.url).host;
    if (parsed.host !== requestHost) return false;
    // A page over plain http may not act on a backend reached over https. The
    // other way round is allowed: a proxy that ends TLS may hand us plain http.
    const requestScheme =
      request.headers.get("x-forwarded-proto")?.split(",")[0]?.trim().toLowerCase() ||
      new URL(request.url).protocol.replace(/:$/, "");
    return !(requestScheme === "https" && parsed.protocol === "http:");
  }

  /**
   * A trusted backend client asks to sync as a backend (what a backend's
   * own client asks its core first). Only its token admits it: the core is
   * asked with this backend's secret. Anyone else gets the 404 this route
   * has without backend clients.
   */
  async function admitBackendClient(request: Request): Promise<Response> {
    const client = await backendClients.resolve(request);
    if (client === null) return new Response("Not found", { status: 404 });
    if (client === "refused" || client.kind !== "backend") {
      return new Response("not a trusted backend client", { status: 401 });
    }
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const response = await core.handle(
      "POST",
      `/apps/${encodeURIComponent(accountAppId(appId))}/backend/admit`,
      [["x-jazz-backend-secret", core.backendSecret]],
      new Uint8Array(0),
      "backend",
    );
    return new Response(null, { status: response.status === 204 ? 204 : 502 });
  }

  /**
   * The endpoint URLs a browser may have named this backend by, as its client
   * scope (see `clientScope`): the page's origin when it is one this backend
   * serves, and this request's own host, each with `basePath`.
   */
  function endpointAudiences(request: Request): string[] {
    if (publicUrls.length) return publicUrls;
    const audiences = new Set<string>();
    // The page origin when it is this Host or an allowed origin, and this
    // Host. A browser can't forge Host, but a caller reaching this backend
    // directly can, so production backends should set `publicUrl`.
    // X-Forwarded-Host is never read here.
    const url = new URL(request.url);
    const host = request.headers.get("host") || url.host;
    const forwardedProto = request.headers
      .get("x-forwarded-proto")
      ?.split(",")[0]
      ?.trim()
      .toLowerCase();
    const scheme = forwardedProto === "https" || url.protocol === "https:" ? "https" : "http";
    const origin = request.headers.get("origin");
    if (origin) {
      try {
        const parsed = new URL(origin);
        if (allowedOrigins.has(parsed.origin) || parsed.host === host)
          audiences.add(`${parsed.origin}${basePath}`);
      } catch {
        // Not an origin a page has.
      }
    }
    try {
      audiences.add(`${new URL(`${scheme}://${host}`).origin}${basePath}`);
    } catch {
      // A host header no URL takes.
    }
    return [...audiences];
  }

  const warnedAudiences = new Set<string>();
  let warnedHeaderAudience = false;
  /** Say once per audience why a guest's proof was refused, so a proxy mismatch is visible. */
  function warnUnknownAudience(token: string, accepted: string[], isAudience = false) {
    const audience = isAudience ? token : parseJwtPayload(token)?.aud;
    const named = typeof audience === "string" ? audience : JSON.stringify(audience);
    if (warnedAudiences.has(named) || warnedAudiences.size > 100) return;
    warnedAudiences.add(named);
    console.warn(
      `createJazz: refused a local-first guest whose proof is for ${named}; this endpoint accepts ${JSON.stringify([appId, ...accepted])}. If browsers reach Jazz through a proxy that changes the host or path, set publicUrl to the URL they use.`,
    );
  }

  /**
   * What a guest's local-first proof is for: this app (an app whose browser
   * clients still name its id, or carry it as their `scope`), one of this
   * endpoint's URLs, or neither (null).
   */
  function guestAudience(request: Request, token: string): { endpoint?: string } | null {
    try {
      verifyLocalFirstToken(token, appId);
      return {};
    } catch {
      // Not this app's id; try the endpoint.
    }
    const endpoints = endpointAudiences(request);
    for (const endpoint of endpoints) {
      try {
        verifyLocalFirstToken(token, endpoint);
        if (!publicUrls.length && !warnedHeaderAudience && !isLocalEndpoint(endpoint)) {
          warnedHeaderAudience = true;
          console.warn(
            `createJazz: vouched for a local-first guest of ${endpoint}, taken from the request's headers. Set publicUrl to the URL browsers use so a caller can't name another host.`,
          );
        }
        return { endpoint };
      } catch {
        // Not this one.
      }
    }
    warnUnknownAudience(token, endpoints);
    return null;
  }

  /**
   * The namespace a linking guest names for its account: this app (no
   * audience, or the app's id), one of this endpoint's URLs, or neither (null).
   */
  function linkAudience(request: Request, audience: unknown): { endpoint?: string } | null {
    // Tokens name their audience as given or as its app id (a UUID).
    const names = (value: string) => audience === value || audience === accountAppId(value);
    if (audience === undefined || (typeof audience === "string" && names(appId))) return {};
    if (typeof audience !== "string") return null;
    const endpoints = endpointAudiences(request);
    const endpoint = endpoints.find(names);
    if (endpoint !== undefined) return { endpoint };
    warnUnknownAudience(audience, endpoints, true);
    return null;
  }

  /** The path below `basePath`, or null for a request Jazz doesn't serve. */
  function innerPath(request: Request): string | null {
    const { pathname } = new URL(request.url);
    if (pathname === basePath) return "/";
    if (!pathname.startsWith(`${basePath}/`)) return null;
    return pathname.slice(basePath.length);
  }

  /**
   * Answer `request`. `upgrades`: whether the handler it came through
   * upgrades sockets on this host, which decides what a plain request for
   * the socket path is told.
   */
  async function handle(request: Request, upgrades = upgradesSockets): Promise<Response> {
    try {
      return await route(request, upgrades);
    } catch (error) {
      if (!(error instanceof BackendClientsUnavailable)) throw error;
      // The reason stays in the backend's logs.
      console.error(error);
      return new Response("backend client keys unavailable", {
        status: 503,
        headers: { "Retry-After": "5" },
      });
    }
  }

  async function route(request: Request, upgrades: boolean): Promise<Response> {
    const path = innerPath(request);
    if (path === null) return new Response("Not found", { status: 404 });
    if (request.method !== "GET" && request.method !== "HEAD" && !originAllowed(request)) {
      return new Response("origin not allowed", { status: 403 });
    }
    if (/^\/apps\/[^/]+\/accounts\/backend-sign-in$/.test(path)) {
      return backendSignIn(request);
    }
    if (/^\/apps\/[^/]+\/accounts\/backend-user$/.test(path)) {
      return backendUser(request);
    }
    if (/^\/apps\/[^/]+\/backend\/admit$/.test(path)) {
      return admitBackendClient(request);
    }
    if (/^\/apps\/[^/]+\/accounts\/backend-link$/.test(path)) {
      return backendLink(request);
    }
    if (/^\/apps\/[^/]+\/accounts\/backend-link\/challenge$/.test(path)) {
      return backendLinkChallenge(request);
    }
    const stream = /^\/apps\/[^/]+\/stream(?:\/([^/]+))?$/.exec(path);
    if (stream) {
      const id = stream[1] && decodeURIComponent(stream[1]);
      if (!id && request.method === "POST") return openStream(request);
      if (id && request.method === "POST") return streamSend(id, request);
      if (id && request.method === "DELETE") {
        const socket = await ownStream(id, request);
        if (socket instanceof Response) return socket;
        socket.close();
        return new Response(null, { status: 204 });
      }
      return new Response(null, { status: 405 });
    }
    if (path.endsWith("/ws")) {
      // A plain request for the socket path: the browser client asks this
      // after a socket failed before it opened. Where this host upgrades
      // sockets (attach(), or Vercel), that failure was transient (a
      // restart, a draining instance) and the client should retry the
      // socket. Only where it can't does the marker send it to the stream
      // at once; the client also switches after several failed sockets in
      // a row, when something in between doesn't pass upgrades on. It asks
      // with a POST, so a page origin the socket was refused for is refused
      // here too (403, above), and not taken for a proxy's doing.
      if (upgrades) {
        return new Response("Upgrade to a WebSocket", { status: 426 });
      }
      return new Response("WebSocket upgrade not available here; use the HTTP stream", {
        status: 426,
        headers: { [STREAM_AVAILABLE_HEADER]: "1" },
      });
    }
    if (!PUBLIC_APP_ROUTES.test(path)) return new Response("Not found", { status: 404 });
    const url = new URL(request.url);
    const body =
      request.method === "GET" || request.method === "HEAD"
        ? Buffer.alloc(0)
        : Buffer.from(await request.arrayBuffer());
    const headers = headerPairs(request.headers);
    // A browser names the app however its scope does; the core knows it by id.
    const corePath = path.replace(/^\/apps\/[^/]+/, `/apps/${encodeURIComponent(appId)}`);
    return coreResponse(
      await core.handle(
        request.method,
        corePath + url.search,
        store.kind === "remote" || store.kind === "vercel"
          ? headers.filter(([name]) => REMOTE_FORWARDED_HEADERS.includes(name!.toLowerCase()))
          : headers,
        body,
      ),
    );
  }

  async function nextHandler(request: Request): Promise<Response> {
    const path = innerPath(request);
    if (
      request.method === "GET" &&
      path?.endsWith("/ws") &&
      request.headers.get("upgrade")?.toLowerCase() === "websocket" &&
      onVercelHost()
    ) {
      if (!originAllowed(request)) return new Response("origin not allowed", { status: 403 });
      const { upgradeWebSocket } = (await import(
        /* @vite-ignore */ "@vercel/functions" as string
      )) as {
        upgradeWebSocket(
          handler: (ws: WsLikeSocket) => void,
          options?: { maxPayload?: number },
        ): Response;
      };
      return upgradeWebSocket((ws) => acceptSocket(request, ws), {
        maxPayload: MAX_STREAM_MESSAGE_BYTES,
      });
    }
    // On Vercel this handler upgrades sockets (above); elsewhere (next dev)
    // only if attach() runs on the same server.
    return handle(request, upgradesSockets || onVercelHost());
  }

  function nodeRequest(req: IncomingMessage): Request {
    const host = req.headers.host ?? "localhost";
    const headers = new Headers();
    for (const [name, value] of Object.entries(req.headers)) {
      if (value === undefined) continue;
      for (const item of Array.isArray(value) ? value : [value]) headers.append(name, item);
    }
    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    // A body parser mounted before nodeHandler (express.json(), express.raw())
    // has read the stream already: its buffer is the body, or nothing is.
    const parsed = (req as { body?: unknown }).body;
    let body: BodyInit | undefined;
    if (hasBody && req.readableEnded) {
      if (!(parsed instanceof Uint8Array)) {
        throw new Error(
          "jazz: the request body was already read by another handler; mount jazz.nodeHandler before body parsers such as express.json()",
        );
      }
      body = parsed as BodyInit;
    } else if (hasBody) {
      body = req as unknown as BodyInit;
    }
    return new Request(`http://${host}${req.url ?? "/"}`, {
      method: req.method,
      headers,
      ...(body === undefined ? {} : { body, duplex: "half" }),
    } as RequestInit);
  }

  function nodeHandler(req: IncomingMessage, res: ServerResponse, next?: () => void): void {
    // Only Jazz's own paths: a request for the app passes on untouched, its
    // body unread, whatever read it before.
    const { pathname } = new URL(req.url ?? "/", "http://localhost");
    if (pathname !== basePath && !pathname.startsWith(`${basePath}/`)) {
      if (next) return next();
      res.statusCode = 404;
      res.end();
      return;
    }
    let request: Request;
    try {
      request = nodeRequest(req);
    } catch (error) {
      console.error(error instanceof Error ? error.message : error);
      res.statusCode = 500;
      res.end("Internal error");
      return;
    }
    void handle(request).then(
      async (response) => {
        res.statusCode = response.status;
        response.headers.forEach((value, name) => res.setHeader(name, value));
        if (!response.body) return void res.end();
        res.flushHeaders();
        const reader = response.body.getReader();
        res.on("close", () => void reader.cancel().catch(() => undefined));
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          res.write(value);
        }
        res.end();
      },
      (error) => {
        // The reason stays in the backend's logs, not in the response.
        console.error("jazz: request failed", error);
        res.statusCode = 500;
        res.end("Internal error");
      },
    );
  }

  async function attach(server: Server): Promise<void> {
    const { WebSocketServer } = (await import("ws" as string)) as {
      WebSocketServer: new (options: { noServer: true; maxPayload: number }) => {
        handleUpgrade(
          req: IncomingMessage,
          socket: Duplex,
          head: Buffer,
          callback: (ws: WsLikeSocket) => void,
        ): void;
        close(): void;
      };
    };
    const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_STREAM_MESSAGE_BYTES });
    server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
      const request = nodeRequest(req);
      const path = innerPath(request);
      if (path === null || !/^\/apps\/[^/]+\/ws$/.test(path)) return;
      if (!originAllowed(request)) {
        socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
        return;
      }
      wss.handleUpgrade(req, socket, head, (ws) => acceptSocket(request, ws));
    });
    // Only now, with `ws` loaded and the listener on: this server upgrades.
    upgradesSockets = true;
    attached = true;
    server.on("close", () => wss.close());
  }

  let closing: Promise<void> | undefined;
  return {
    appId,
    get basePath() {
      return basePath;
    },
    reload,
    handle: (request) => handle(request),
    next: () => ({ GET: nextHandler, POST: nextHandler, DELETE: nextHandler }),
    attach,
    nodeHandler,
    acceptSocket,
    get db() {
      return backend.db;
    },
    linkIdentity,
    async forReqUser(request) {
      const user = await authenticate(request);
      if (!user) throw signedOut("forReqUser");
      return backendDbForSession(backend, await sessionFor(user, "forReqUser"));
    },
    async withAttribution(request) {
      const user = await authenticate(request);
      if (!user) throw signedOut("withAttribution");
      return backendDbAttributedTo(backend, await sessionFor(user, "withAttribution"));
    },
    async accountFor(request) {
      const user = await authenticate(request);
      return user ? await existingAccountOf(user.userId) : null;
    },
    async createAccountFor(userId) {
      if (typeof userId !== "string" || userId === "")
        throw new Error("createAccountFor: pass the user's id, as your authenticate names it");
      return accountOf(userId);
    },
    close() {
      closing ??= (async () => {
        // Every client connection ends, so the host's own shutdown (which
        // waits for the connections it serves) can finish, and the clients
        // reconnect to whichever instance serves next.
        for (const stream of [...streams.values()]) {
          stream.session.close();
          stream.end(1012, "server shutting down");
        }
        streams.clear();
        for (const ws of [...clientSockets]) ws.close(1012, "server shutting down");
        clientSockets.clear();
        await Promise.all([...retiring].map((close) => close()));
        // Writes that resolved at the "local" tier may still be on their way
        // to the core. Hand them over before closing it, so a close right
        // after a write never loses it.
        const timer = setTimeout(() => backend.db.abortGracefulShutdown(), CLOSE_SYNC_TIMEOUT_MS);
        await backend
          .shutdown({ waitForSync: true })
          .catch((error: unknown) => {
            console.warn("Jazz backend closed before every write reached the core.", error);
          })
          .finally(() => clearTimeout(timer));
        await session.close();
        await core.close();
      })();
      return closing;
    },
  };
}

/**
 * Run Jazz inside your backend. It applies the schema, migrations and
 * permissions your code declares to its core (in this process, or a remote
 * one), and serves sync on your own origin under `basePath`. There is no
 * separate deploy step: a rollback of your backend rolls Jazz back with it.
 *
 * It opens nothing when called, so a module can export it at its top level:
 * the store opens on first use (a request, a `db` call, `ready()`). A process
 * runs one Jazz per store (a `sqlite()` directory, a `vercel()` core): a
 * later `createJazz` for it (a dev server's hot reload, another module
 * graph) returns that same Jazz with the later
 * call's app, permissions and options. Each `remote()` and `memory()` store
 * is its own: a module that hot reloads keeps that Jazz across reloads
 * itself, or closes it.
 */
export function createJazz(options: CreateJazzOptions): Jazz {
  jazzSettings(options);
  const store = runningStore(options.store);
  const key = storeKey(store);
  const entries = registry();
  const shared = key === undefined ? undefined : entries.get(key);
  if (shared && !shared.closing) {
    adoptStore(shared, store);
    shared.options = options;
    return shared.jazz;
  }
  // A Jazz for this store still closing: the new one opens once it let go.
  const entry = newEntry(options, store, key, shared?.released);
  if (key !== undefined) entries.set(key, entry);
  return entry.jazz;
}

/** One store's Jazz in this process, shared by every `createJazz` for it. */
interface Entry {
  key: string | undefined;
  store: JazzStore;
  /** The latest call's options: the ones the next use applies. */
  options: CreateJazzOptions;
  opening?: Promise<OpenedJazz>;
  opened?: OpenedJazz;
  /** The options `opened` runs with. */
  applied?: CreateJazzOptions;
  reloading?: Promise<void>;
  /** The last open failed: the next try waits until `retryAt`. */
  failure?: { error: unknown; retryAt: number; delayMs: number };
  /** Set by `close()`: what it resolves with. */
  closing?: Promise<void>;
  /** Resolves once the store is closed, even after a `close()` that gave up waiting. */
  released?: Promise<void>;
  /** A previous Jazz for this store, closing: this one opens once it is released. */
  after?: Promise<void>;
  jazz: Jazz;
}

/** How long a session a reload replaced stays open for the requests still using it. */
const RETIRED_SESSION_GRACE_MS = 60_000;
/** How long `close()` waits for a store still opening before it gives up on it. */
export const CLOSE_WAITS_FOR_OPEN_MS = 30_000;
/** A failed open is tried again after this, doubling up to `MAX_OPEN_RETRY_MS`. */
const FIRST_OPEN_RETRY_MS = 1_000;
const MAX_OPEN_RETRY_MS = 30_000;
/** At most this many sockets wait for the store to open, each holding at most this much. */
export const MAX_HELD_SOCKETS = 1_000;
export const MAX_HELD_SOCKET_BYTES = 4 * 1024 * 1024;

const REGISTRY = Symbol.for("jazz-tools.createJazz.stores");

/**
 * Open stores by key, on `globalThis` so that two copies of this module
 * (a bundler's server and route chunks, a hot reload) share them.
 */
function registry(): Map<string, Entry> {
  const scope = globalThis as { [REGISTRY]?: Map<string, Entry> };
  return (scope[REGISTRY] ??= new Map());
}

/**
 * The store's identity in this process: one Jazz runs per store, so a hot
 * reload finds the one it opened. A `remote()` store has none: two
 * backends (deployments, tests) may each run their own Jazz on one remote
 * app, with their own permissions. Nor has a `memory()` store, a new,
 * empty database each time. A module that hot reloads keeps such a Jazz
 * across reloads itself (on `globalThis`), or closes it.
 */
function storeKey(store: JazzStore): string | undefined {
  if (store.kind === "sqlite") return `sqlite:${resolve(store.path)}`;
  if (store.kind === "vercel") {
    const project = store.options.credentials?.projectId ?? process.env.VERCEL_PROJECT_ID;
    let name = store.options.name;
    try {
      name ??= project ? defaultCoreName(project, process.env) : undefined;
    } catch {
      // A preview without a branch: its open reports that, not createJazz.
    }
    // Without a name the core is unknown: its open reports that.
    return name ? `vercel:${name}` : undefined;
  }
  return undefined;
}

/**
 * What a store's settings are. The functions it holds (a credential hook)
 * are not settings: the open store calls the latest call's. A Sandbox SDK
 * is: another one is another setting.
 */
function storeSettings(store: JazzStore): string {
  return JSON.stringify(store, (name, value: unknown) => {
    if (name === "sdk" && value !== undefined) return `sdk#${identity(value as object)}`;
    return typeof value === "function" ? "function" : value;
  });
}

const identities = new WeakMap<object, number>();
let lastIdentity = 0;
function identity(value: object): number {
  let id = identities.get(value);
  if (id === undefined) identities.set(value, (id = ++lastIdentity));
  return id;
}

/**
 * The entry's store, its function settings (a credential hook) calling
 * through to the latest `createJazz` call's, so a reload applies them.
 */
function liveStore(entry: Entry): JazzStore {
  const through = <T extends object>(value: T, current: () => T | undefined): T => {
    const out = { ...value } as Record<string, unknown>;
    for (const [name, field] of Object.entries(value)) {
      if (typeof field !== "function") continue;
      out[name] = (...args: unknown[]) => {
        const latest = (current() as Record<string, unknown> | undefined)?.[name];
        return ((typeof latest === "function" ? latest : field) as (...a: unknown[]) => unknown)(
          ...args,
        );
      };
    }
    return out as T;
  };
  const store = entry.store;
  if (store.kind === "vercel") {
    return {
      ...store,
      options: through(store.options, () =>
        entry.store.kind === "vercel" ? entry.store.options : undefined,
      ),
    };
  }
  return through(store, () => entry.store);
}

/**
 * A later `createJazz` for an entry's store. Before the store opens it takes
 * the new settings; once open, settings that would need another core are
 * refused, so a change never silently goes unapplied.
 */
function adoptStore(entry: Entry, store: JazzStore): void {
  if (!entry.opening && !entry.opened) {
    entry.store = store;
    // A later call (a hot reload after a fix) tries the open again at once.
    entry.failure = undefined;
    return;
  }
  if (storeSettings(store) !== storeSettings(entry.store)) {
    throw new Error(
      `createJazz(): ${entry.key} is already open in this process with other store settings; restart the process to change them`,
    );
  }
  // The same settings: its function settings are the latest call's now.
  entry.store = store;
}

function newEntry(
  options: CreateJazzOptions,
  store: JazzStore,
  key: string | undefined,
  after?: Promise<void>,
): Entry {
  const entry = { key, store, options, after } as Entry;
  entry.jazz = facade(entry);
  return entry;
}

/**
 * The entry's open Jazz, with the latest call's options applied. Once it is
 * closing, only `draining` (the writes made before the close) still gets it.
 */
async function settled(entry: Entry, draining = false): Promise<OpenedJazz> {
  for (;;) {
    if (entry.closing && !draining) throw new Error("createJazz: this Jazz was closed");
    const opened = await (entry.opening ?? openEntry(entry));
    if (entry.closing && !draining) throw new Error("createJazz: this Jazz was closed");
    if (entry.applied === entry.options) return opened;
    await (entry.reloading ??= reloadEntry(entry, opened));
  }
}

/**
 * Open the entry's store. A failed open is not tried again at once: until
 * its backoff has passed, every use fails with the same error, so a store
 * that is down is not asked again on every request.
 */
function openEntry(entry: Entry): Promise<OpenedJazz> {
  const failed = entry.failure;
  if (failed && Date.now() < failed.retryAt) return Promise.reject(failed.error);
  const options = entry.options;
  const opening: Promise<OpenedJazz> = (async () => {
    await entry.after;
    return openJazz(options, liveStore(entry));
  })().then(
    (opened) => {
      entry.failure = undefined;
      entry.opened = opened;
      entry.applied = options;
      drainOnSignal(entry);
      return opened;
    },
    (error: unknown) => {
      const delayMs = failed
        ? Math.min(failed.delayMs * 2, MAX_OPEN_RETRY_MS)
        : FIRST_OPEN_RETRY_MS;
      entry.failure = { error, retryAt: Date.now() + delayMs, delayMs };
      if (entry.opening === opening) entry.opening = undefined;
      throw error;
    },
  );
  entry.opening = opening;
  return opening;
}

/** The signals a host stops a server with (a deploy, a rolling restart, Ctrl-C). */
const SHUTDOWN_SIGNALS = ["SIGTERM", "SIGINT"] as const;
/** Every open Jazz in this process, closed when the process is asked to stop. */
const drained = new Set<Entry>();
const onShutdownSignal = (signal: NodeJS.Signals) => void shutDownOnSignal(signal);

/**
 * Close every open Jazz when the process is asked to stop. A host's own
 * shutdown (`next start`, an Effect runtime, `server.close()`) waits for the
 * connections it serves, and Jazz's sync sockets and streams last until Jazz
 * ends them: without this, the process lingers and keeps its store locked
 * for as long as a browser stays connected. The clients reconnect to
 * whichever instance serves next. With no other handler for the signal the
 * process then stops as it would have; otherwise that handler decides.
 */
function drainOnSignal(entry: Entry): void {
  if (typeof process === "undefined" || typeof process.on !== "function") return;
  if (drained.size === 0) {
    for (const signal of SHUTDOWN_SIGNALS) process.on(signal, onShutdownSignal);
  }
  drained.add(entry);
}

function undrain(entry: Entry): void {
  if (!drained.delete(entry) || drained.size > 0) return;
  for (const signal of SHUTDOWN_SIGNALS) process.off(signal, onShutdownSignal);
}

async function shutDownOnSignal(signal: NodeJS.Signals): Promise<void> {
  for (const each of SHUTDOWN_SIGNALS) process.off(each, onShutdownSignal);
  const open = [...drained];
  drained.clear();
  await Promise.allSettled(open.map((entry) => entry.jazz.close()));
  // Nobody else asked to handle it: stop the way the signal would have.
  if (process.listenerCount(signal) === 0) process.kill(process.pid, signal);
}

/**
 * Close the entry: apply the writes made before the close, then close its
 * store. A store still opening is waited for at most
 * `CLOSE_WAITS_FOR_OPEN_MS`; past that the writes still waiting fail and
 * the store closes once it has opened. Until it has, a new `createJazz` for
 * the store waits for it rather than finding the store held.
 */
async function closeEntry(entry: Entry, early: EarlyDb): Promise<void> {
  let release!: () => void;
  entry.released = new Promise<void>((resolve) => (release = resolve));
  const stopped = (async () => {
    await early.drain();
    const opened = await entry.opening?.catch(() => undefined);
    await entry.reloading?.catch(() => undefined);
    await opened?.close();
  })();
  void stopped
    .catch(() => undefined)
    .finally(() => {
      if (entry.key !== undefined && registry().get(entry.key) === entry) {
        registry().delete(entry.key);
      }
      undrain(entry);
      release();
    });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const gaveUp = new Promise<true>((resolve) => {
    timer = setTimeout(() => resolve(true), CLOSE_WAITS_FOR_OPEN_MS);
  });
  const late = await Promise.race([stopped.then(() => false), gaveUp]);
  clearTimeout(timer);
  if (!late) return;
  early.abandon(new Error("createJazz: closed before its store opened"));
  console.warn(
    `jazz: close() gave up after ${CLOSE_WAITS_FOR_OPEN_MS / 1000}s waiting for ${entry.key ?? "its store"} to open; it closes once it has`,
  );
}

async function reloadEntry(entry: Entry, opened: OpenedJazz): Promise<void> {
  const options = entry.options;
  try {
    await opened.reload(options);
    entry.applied = options;
  } finally {
    entry.reloading = undefined;
  }
}

/** The entry's open Jazz when nothing is left to apply, without waiting. */
function settledNow(entry: Entry): OpenedJazz | undefined {
  return !entry.closing && entry.applied === entry.options ? entry.opened : undefined;
}

/**
 * A socket handed over before the store opened: it keeps what arrives until
 * the session takes it over, so no message is lost while it waits. Past
 * `MAX_HELD_SOCKET_BYTES` held, it is closed (1013, try again later).
 */
function heldSocket(ws: WsLikeSocket): { socket: WsLikeSocket; refuse(reason: string): void } {
  type Event = ["message", unknown, boolean | undefined] | ["close"] | ["error", unknown];
  const held: Event[] = [];
  let heldBytes = 0;
  let listeners: {
    message?: (data: unknown, isBinary?: boolean) => void;
    close?: () => void;
    error?: (error: unknown) => void;
  } | null = null;
  const deliver = (event: Event) => {
    if (!listeners) {
      if (event[0] === "message") {
        heldBytes += byteLength(event[1]);
        if (heldBytes > MAX_HELD_SOCKET_BYTES) {
          held.length = 0;
          ws.close(1013, "store still opening");
          return;
        }
      }
      return void held.push(event);
    }
    if (event[0] === "message") listeners.message?.(event[1], event[2]);
    else if (event[0] === "close") listeners.close?.();
    else listeners.error?.(event[1]);
  };
  ws.on("message", (data, isBinary) => deliver(["message", data, isBinary]));
  ws.on("close", () => deliver(["close"]));
  ws.on("error", (error) => deliver(["error", error]));
  const socket: WsLikeSocket = {
    send: (data) => ws.send(data),
    close: (code, reason) => ws.close(code, reason),
    on(event: "message" | "close" | "error", listener: (...args: never[]) => void) {
      const first = listeners === null;
      listeners ??= {};
      (listeners as Record<string, unknown>)[event] = listener;
      // The session adds its listeners in one go: replay once it has.
      if (first) {
        queueMicrotask(() => {
          for (const event of held.splice(0)) deliver(event);
        });
      }
      return socket;
    },
  } as WsLikeSocket;
  return { socket, refuse: (reason) => ws.close(1011, reason) };
}

function byteLength(data: unknown): number {
  if (typeof data === "string") return data.length * 3;
  if (Array.isArray(data)) return data.reduce((sum: number, part) => sum + byteLength(part), 0);
  const sized = data as { byteLength?: unknown } | null;
  return typeof sized?.byteLength === "number" ? sized.byteLength : 0;
}

/** The `Jazz` every `createJazz` for one store returns. */
function facade(entry: Entry): Jazz {
  const use = () => settled(entry);
  const basePath = () => jazzSettings(entry.options).basePath;
  const handle = async (request: Request) => (await use()).handle(request);
  const nextHandler = async (request: Request) => (await use()).next().GET(request);
  // The writes made before a close still apply as it drains them.
  const early = earlyDb(
    async () => (await settled(entry, true)).db,
    () => settledNow(entry)?.db,
  );
  const db = early.db;
  let heldSockets = 0;
  const jazz = {
    get basePath() {
      return basePath();
    },
    handle,
    next: () => ({ GET: nextHandler, POST: nextHandler, DELETE: nextHandler }),
    async attach(server) {
      await (await use()).attach(server);
    },
    nodeHandler(req, res, next) {
      const now = settledNow(entry);
      if (now) return now.nodeHandler(req, res, next);
      const prefix = basePath();
      const pathname = new URL(req.url ?? "/", "http://localhost").pathname;
      // Requests Jazz doesn't serve pass on without opening the store.
      if (pathname !== prefix && !pathname.startsWith(`${prefix}/`)) {
        if (next) return next();
        res.statusCode = 404;
        res.end();
        return;
      }
      use().then(
        (opened) => opened.nodeHandler(req, res, next),
        (error: unknown) => {
          console.error("jazz: the store did not open", error);
          res.statusCode = 500;
          res.end("Internal error");
        },
      );
    },
    acceptSocket(request, ws) {
      const now = settledNow(entry);
      if (now) return now.acceptSocket(request, ws);
      if (heldSockets >= MAX_HELD_SOCKETS) return ws.close(1013, "store still opening");
      heldSockets++;
      const { socket, refuse } = heldSocket(ws);
      use()
        .then(
          (opened) => opened.acceptSocket(request, socket),
          (error: unknown) => {
            console.error("jazz: the store did not open", error);
            refuse("store unavailable");
          },
        )
        .finally(() => heldSockets--);
    },
    db,
    forReqUser: async (request) => (await use()).forReqUser(request),
    withAttribution: async (request) => (await use()).withAttribution(request),
    accountFor: async (request) => (await use()).accountFor(request),
    createAccountFor: async (userId) => (await use()).createAccountFor(userId),
    linkIdentity: async (link) => (await use()).linkIdentity(link),
    async ready() {
      await use();
      return jazz;
    },
    close() {
      return (entry.closing ??= closeEntry(entry, early));
    },
  } as Omit<Jazz, "appId"> as Jazz;
  // Not enumerable, so printing or comparing a Jazz before it opens reads it not.
  Object.defineProperty(jazz, "appId", {
    get() {
      const opened = entry.opened;
      if (!opened) {
        throw new Error(
          "jazz.appId is known once the store has opened: `await jazz.ready()` first",
        );
      }
      return opened.appId;
    },
  });
  return jazz;
}

/** An account request the core answered with a 4xx: a refusal, not an outage. */
function isRefusal(error: unknown): boolean {
  const status = (error as { status?: unknown } | null)?.status;
  // Timeouts and throttling are 4xx too, but pass.
  if (status === 408 || status === 425 || status === 429) return false;
  return typeof status === "number" && status >= 400 && status < 500;
}

function refusalCode(error: unknown): string {
  const message = error instanceof Error ? error.message : "";
  const code = /: ([a-z_]{1,80})$/.exec(message)?.[1];
  return code ?? "refused";
}

/** A WireFrame::Error batch with code AuthFailed and retry Never. */
function authFailedBatch(message: string): Buffer {
  const frame = new PostcardWriter();
  frame.u64(1); // WireFrame::Error
  frame.u64(3); // WireErrorCode::AuthFailed
  frame.u64(0); // WireRetry::Never
  frame.string(message);
  return Buffer.from(encodeWebSocketFrameBatch([frame.finish()]));
}

/** A development endpoint on this machine, where header-derived audiences are fine. */
function isLocalEndpoint(endpoint: string): boolean {
  try {
    const host = new URL(endpoint).hostname;
    return (
      host === "localhost" ||
      host.endsWith(".localhost") ||
      host === "127.0.0.1" ||
      host === "[::1]"
    );
  } catch {
    return false;
  }
}
