import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { verifyChallenge } from "../local-first-auth/server.js";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { JazzCore, type JazzCoreSocketEvent } from "jazz-napi";
import { registerInProcessTransport } from "../runtime/in-process-transport.js";
import { accountAppId, localFirstAccountId } from "../accounts/local-first.js";
import { LOCAL_FIRST_JWT_ISSUER, parseJwtPayload } from "../runtime/client-session.js";
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
import type { PublishedTableLens } from "../dev/catalogue-api.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import type { JazzSession } from "../session/state.js";
import {
  backendDbAttributedTo,
  backendDbForSession,
  createJazzSession,
  type JazzClient,
} from "./create-jazz-session.js";
import type { BackendSchemaInput } from "./create-jazz-context.js";
import {
  onVercelDeployment,
  onVercelHost,
  openVercelCore,
  type VercelStoreOptions,
} from "./vercel-store.js";

export type { VercelStoreOptions } from "./vercel-store.js";

/** Where the core keeps its data. */
export type JazzStore =
  | { readonly kind: "sqlite"; readonly path: string }
  | { readonly kind: "memory" }
  | {
      readonly kind: "remote";
      readonly url: string;
      /** This backend's credential for the core, asked for each request and socket. */
      readonly backendCredential: BackendCredential;
    }
  | {
      readonly kind: "vercel";
      readonly options: VercelStoreOptions;
      /** What runs outside a Vercel deployment, e.g. under `next dev`. */
      readonly local: Extract<JazzStore, { kind: "sqlite" | "memory" }>;
    };

/** A SQLite core in this process, kept in the directory `path`. */
export function sqlite(options: { path: string }): Extract<JazzStore, { kind: "sqlite" }> {
  return { kind: "sqlite", path: options.path };
}

/** An in-memory core in this process: for tests and throwaway previews. */
export function memory(): Extract<JazzStore, { kind: "memory" }> {
  return { kind: "memory" };
}

/**
 * The credential this backend presents to its core, fresh for each request
 * and socket: the app's secret, or a token that proves which workload this
 * is (a sync server started with `--trusted-workload-*` accepts its
 * platform's OIDC token, such as a Vercel function's).
 */
export type BackendCredential = () => string | Promise<string>;

/**
 * A core running elsewhere (Jazz Cloud, or a sync server you run), reached
 * from this backend with the app's one secret (its admin secret), or with a
 * `backendCredential` of your own. Browsers still talk only to your backend:
 * it decides who each user is, and the core trusts it to say so. The secret
 * never leaves your server.
 */
export function remote(
  options: { url: string; secret: string } | { url: string; backendCredential: BackendCredential },
): Extract<JazzStore, { kind: "remote" }> {
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
  return { kind: "remote", url: options.url.replace(/\/+$/, ""), backendCredential };
}

/**
 * A core in a persistent Vercel Sandbox in your own Vercel project, with its
 * data on a Drive: one for production and one per Git branch for previews.
 * It is created on first use, wakes when a request needs it (about a second)
 * and stops after `idleMs` without requests or sockets. The function's Vercel
 * OIDC token is its only credential, so there are no secrets and no env vars:
 * the core admits only tokens naming the workload its first start pinned
 * (team, project and environment by name), and a deployment of another
 * environment refuses to start or retarget it. All previews share one
 * workload, so preview branches' cores are kept apart, not secured from each
 * other. Anyone with Sandbox access to the project controls its cores. The
 * token comes with each request (it can't be read at module scope), so call
 * `createJazz` lazily from a handler; work between requests uses the freshest
 * one seen, and open sockets outlive its expiry. A team or project rename (or
 * a change of OIDC issuer mode) changes the workload: delete `/data/trust` in
 * the Sandbox to re-pin the core. Install `@vercel/sandbox` and
 * `@vercel/functions` next to `jazz-tools`. A deployment of a newer jazz-tools
 * restarts the core on its version, and an older deployment never moves it
 * back. Outside a production or preview deployment (`next dev`, `vercel dev`,
 * tests) it runs `local` instead: by default a SQLite core in `.jazz/local`.
 * Branch cores are not deleted when a branch merges yet: each keeps its
 * Sandbox and a Drive of up to `maxDriveBytes` until you remove them.
 */
export function vercel(
  options: VercelStoreOptions & { local?: Extract<JazzStore, { kind: "sqlite" | "memory" }> } = {},
): JazzStore {
  const { local, ...rest } = options;
  return { kind: "vercel", options: rest, local: local ?? sqlite({ path: ".jazz/local" }) };
}

/** Who the request's user is, as the app's own auth decides. */
export interface JazzUser {
  /** Stable user id in your auth system. Becomes the identity's `sub`. */
  userId: string;
  /** Extra claims your permissions can read through `session.claims`. */
  claims?: Record<string, unknown>;
}

export interface CreateJazzOptions {
  appId: string;
  app: BackendSchemaInput;
  permissions: CompiledPermissionsMap;
  store: JazzStore;
  /**
   * Decide who sent `request`, with your app's own session (a cookie, a
   * header). Return null for a signed-out request. Jazz calls it when a
   * browser signs in or opens a sync session, and in `forRequest`.
   */
  authenticate?: (request: Request) => JazzUser | null | Promise<JazzUser | null>;
  /** The path Jazz is mounted under. Defaults to `/jazz`. */
  basePath?: string;
  /**
   * How long a browser's sync session lasts before `authenticate` is asked
   * again (the client reconnects on its own). Defaults to one hour.
   */
  sessionTtlSeconds?: number;
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
  readonly appId: string;
  readonly basePath: string;
  /** Answer one request under `basePath` (sync, sign-in, the HTTP stream). */
  handle(request: Request): Promise<Response>;
  /** Route handlers for a Next.js catch-all route (`app/jazz/[...path]/route.ts`). */
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
  /** The backend's own database, with backend permissions. */
  readonly db: Db;
  /** The database as the request's user, judged by your permissions. */
  forRequest(request: Request): Promise<Db>;
  /**
   * The Jazz account of the request's signed-in user (created on first use),
   * or null when `authenticate` names no user. For API routes that act on
   * the user's data with the backend's own `db`.
   */
  accountFor(request: Request): Promise<string | null>;
  /**
   * The backend's own database (backend permissions), with every write
   * stamped as the request's signed-in user: for server actions that do what
   * the user may not do directly, on their behalf.
   *
   * **Your permissions are not checked.** It writes with the backend's full
   * authority; only the attribution is the user's. Check yourself that the
   * user may do what the action does, and keep it in server-only code.
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
const STRIPPED_HEADERS = [
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
interface CoreSocket {
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
  },
): Promise<void> {
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
  await deployArtifacts(server, graph, {
    targetSchemaHash,
    schemas: [...schemas].map(([hash, schema]) => ({ hash, schema })),
    migrations: (shipped?.migrations ?? []).map((edge) => ({
      fromHash: edge.fromHash,
      toHash: edge.toHash,
      forward: edge.forward as PublishedTableLens[],
    })),
    permissions: options.permissions,
  });
}

async function openInProcessCore(
  appId: string,
  store: Extract<JazzStore, { kind: "sqlite" | "memory" }>,
): Promise<Core> {
  const backendSecret = randomBytes(32).toString("base64url");
  const adminSecret = randomBytes(32).toString("base64url");
  // The core holds `<path>/jazz.lock` with an OS lock while it's open.
  if (store.kind === "sqlite") refuseServerlessDisk(store.path);
  const core = await JazzCore.open({
    appId,
    backendSecret,
    adminSecret,
    ...(store.kind === "sqlite" ? { dataDir: store.path } : { inMemory: true }),
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
    WebSocket: inProcessWebSocket((_url, onEvent) => core.openSocket([], onEvent)),
  });
  return {
    serverUrl,
    adminSecret,
    backendSecret,
    handle,
    openSocket: (headers, onEvent) => core.openSocket(headers, onEvent),
    async close() {
      unregister();
      await core.close();
    },
  };
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
      "Use remote({ url, secret }) to a sync server or Jazz Cloud.",
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
    openSocket: (_headers, onEvent, access = "session") => connect(socketUrl, onEvent, access),
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
      return connect(
        `${target.url.replace(/^http/, "ws")}${pathname}${search}`,
        onEvent,
        "backend",
      );
    }),
  });
  return core;
}

/**
 * Run Jazz inside your backend. It applies the schema, migrations and
 * permissions your code declares to its core (in this process, or a remote
 * one), and serves sync on your own origin under `basePath`. There is no
 * separate deploy step: a rollback of your backend rolls Jazz back with it.
 */
export async function createJazz(options: CreateJazzOptions): Promise<Jazz> {
  const basePath = (options.basePath ?? DEFAULT_BASE_PATH).replace(/\/+$/, "");
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
  const issuer = `jazz:app:${options.appId}`;
  const store =
    options.store.kind === "vercel" && !onVercelDeployment() ? options.store.local : options.store;
  const core =
    store.kind === "remote"
      ? openRemoteCore(options.appId, store)
      : store.kind === "vercel"
        ? await openVercelCore({
            appId: options.appId,
            options: store.options,
            // The function's OIDC token is the core's only credential.
            openRemote: ({ appId, url, backendCredential }) =>
              openRemoteCore(appId, { url, backendCredential }),
          })
        : await openInProcessCore(options.appId, store);

  let session: JazzSession<JazzClient>;
  try {
    await applyBundle(core.serverUrl, { ...options, adminSecret: core.adminSecret });
    session = await createJazzSession({
      appId: options.appId,
      app: options.app,
      permissions: options.permissions,
      serverUrl: core.serverUrl,
      driver: { type: "memory" },
      initial: { backendSecret: core.backendSecret },
    });
  } catch (error) {
    await core.close();
    throw error;
  }
  const opened = session.getSnapshot();
  if (opened.status !== "ready" || !opened.client) {
    await session.close();
    await core.close();
    throw opened.error ?? new Error(`Jazz backend session did not open (${opened.status})`);
  }
  const backend = opened.client;

  async function authenticate(request: Request): Promise<JazzUser | null> {
    if (!options.authenticate) return null;
    return (await options.authenticate(request)) ?? null;
  }

  /** One admin account request to the core: this backend vouches for the identities. */
  async function adminAccounts(operation: string, body: unknown): Promise<{ account: string }> {
    const response = await core.handle(
      "POST",
      `/apps/${encodeURIComponent(accountAppId(options.appId))}/admin/accounts/${operation}`,
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
        },
      );
    }
    const parsed = JSON.parse(text) as { account: unknown };
    return { ...parsed, account: String(parsed.account) };
  }

  const identity = (userId: string) => ({ issuer, subject: userId });

  /** The user's session: their account, created on first sign-in. */
  // A user's account never changes once assigned (linking only adds
  // identities to an account), so the core is asked once per user for a
  // while, not on every sign-in, socket and request.
  const accounts = new Map<string, { account: string; until: number }>();
  const ACCOUNT_CACHE_LIMIT = 10_000;
  async function accountOf(userId: string): Promise<string> {
    const now = Date.now();
    const cached = accounts.get(userId);
    if (cached && cached.until > now) return cached.account;
    const { account } = await adminAccounts("login-or-register", { identity: identity(userId) });
    if (accounts.size >= ACCOUNT_CACHE_LIMIT) {
      accounts.delete(accounts.keys().next().value!);
    }
    accounts.set(userId, { account, until: now + (options.sessionTtlSeconds ?? 3600) * 1000 });
    return account;
  }

  async function sessionFor(user: JazzUser): Promise<Session> {
    const account = await accountOf(user.userId);
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
   * Link challenges: a guest links only on a proof made for one challenge,
   * never on its sync token, so a leaked token can't link it to someone
   * else. A challenge names the user it links to and expires quickly; it is
   * signed with a key only this app's backend instances share (derived from
   * the core's admin secret), and its nonce becomes the link's single-use
   * intent id at the core, so a proof works once.
   */
  const challengeKey = createHmac("sha256", core.adminSecret)
    .update(`jazz-link-challenge-v1\0${accountAppId(options.appId)}`)
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
    let body: { challenge?: unknown; proof?: unknown };
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
    // The guest's account follows from its key alone, as the core founds it.
    const account = localFirstAccountId(options.appId, guest.subject);
    const userIs = async (expected: string) =>
      (await adminAccounts("resolve", { identity: identity(user.userId) })).account === expected;
    let guestRevoked = false;
    try {
      try {
        await adminAccounts("found-local-first", { identity: guest });
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
          // through, then perhaps its revoke failed), this link is done. A
          // used challenge (the core refuses its nonce first) and a user of
          // another account are still refused.
          if ((error as { status?: number }).status !== 409 || !(await userIs(account)))
            throw error;
        }
      }
    } catch {
      // The core's reason (e.g. the identity already belongs to another
      // account, or the challenge was used) stays on the backend.
      return new Response("link_refused", { status: 409 });
    }
    if (options.guestKeyAfterLink === "revoke" && !guestRevoked) {
      // The link already stands. If the revoke fails, the guest key stays
      // usable until the browser retries the link, which is idempotent and
      // revokes then: a server error, logged here, not a silent success.
      const revoked = await core.handle(
        "POST",
        `/apps/${encodeURIComponent(accountAppId(options.appId))}/admin/accounts/revoke`,
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
  }

  function openClientSession(
    request: Request,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
  ): ClientSession {
    // The core socket opens once the prelude is admitted, with the access
    // that admission needs: a vouched user's session is the backend's, a
    // guest's is its own.
    let socket: CoreSocket | undefined;
    let ended = false;
    let userId: string | undefined;
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
      socket?.close();
    };
    const admit = async (prelude: Uint8Array): Promise<boolean> => {
      const admitted = await admitPrelude(request, prelude);
      if (!admitted || ended) {
        onEvent(null, { kind: "close", code: 1008, reason: "not signed in" });
        end();
        return false;
      }
      userId = admitted.userId;
      if (expiry) clearTimeout(expiry);
      const ttl = (options.sessionTtlSeconds ?? 3600) * 1000;
      expiry = setTimeout(end, ttl);
      (expiry as unknown as { unref?: () => void }).unref?.();
      socket = core.openSocket(
        headerPairs(request.headers),
        onEvent,
        admitted.userId === undefined ? "session" : "backend",
      );
      return socket.send(admitted.prelude);
    };
    // Messages after the prelude wait for its admission, in order.
    let chain: Promise<boolean> | undefined;
    return {
      send(data) {
        if (ended) return false;
        chain = chain
          ? chain.then((ok) => ok && !ended && !!socket?.send(data))
          : admit(data).catch(() => {
              onEvent(null, { kind: "close", code: 1011, reason: "admission failed" });
              end();
              return false;
            });
        return true;
      },
      close: end,
      get userId() {
        return userId;
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
  ): Promise<{ prelude: Uint8Array; userId: string | undefined } | null> {
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
    const others = ["backend_secret", "admin_secret", "backend_session"];
    // Browser worker transports send `jwt_token: null` alongside a backend
    // session; only a real token counts as one.
    const token = auth.jwt_token ?? parsed.jwt_token;
    if (token !== undefined && token !== null) {
      const localFirst =
        typeof token === "string" && parseJwtPayload(token)?.iss === LOCAL_FIRST_JWT_ISSUER;
      const alone = others.every((key) => parsed[key] == null && auth[key] == null);
      // A guest has no user here: its session is bound to its own key.
      return localFirst && alone ? { prelude, userId: undefined } : null;
    }
    const stated = auth.backend_session as { user_id?: unknown } | undefined;
    if (!stated || typeof stated !== "object") return null;
    for (const credential of ["backend_secret", "admin_secret"]) {
      if (parsed[credential] != null || auth[credential] != null) return null;
    }
    const user = await authenticate(request);
    if (!user || user.userId !== stated.user_id) return null;
    const vouched = { backend_secret: core.backendSecret, backend_session: await sessionFor(user) };
    const rewritten = { ...parsed, ...vouched, auth: { ...auth, ...vouched } };
    return { prelude: new TextEncoder().encode(JSON.stringify(rewritten)), userId: user.userId };
  }

  // HTTP stream sessions, by id, with the key that proves a request comes
  // from the client that opened them.
  const streams = new Map<string, { session: ClientSession; key: Buffer }>();

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
    streams.set(id, { session: socket, key });
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
    if (socket.userId !== undefined) {
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
        else if (event.kind === "close") ws.close(event.code ?? 1000, event.reason ?? "");
      });
    } catch (error) {
      ws.close(1013, error instanceof Error ? error.message : String(error));
      return;
    }
    ws.on("message", (data) => {
      if (!socket.send(toBytes(data))) ws.close(1000, "session ended");
    });
    ws.on("close", () => socket.close());
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
    const path = innerPath(request);
    if (path === null) return new Response("Not found", { status: 404 });
    if (request.method !== "GET" && request.method !== "HEAD" && !originAllowed(request)) {
      return new Response("origin not allowed", { status: 403 });
    }
    if (/^\/apps\/[^/]+\/accounts\/backend-sign-in$/.test(path)) {
      return backendSignIn(request);
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
      // socket. Only where it can't does the marker send it to the stream.
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
    return coreResponse(
      await core.handle(
        request.method,
        path + url.search,
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
    return new Request(`http://${host}${req.url ?? "/"}`, {
      method: req.method,
      headers,
      ...(hasBody ? { body: req as unknown as BodyInit, duplex: "half" } : {}),
    } as RequestInit);
  }

  function nodeHandler(req: IncomingMessage, res: ServerResponse, next?: () => void): void {
    const request = nodeRequest(req);
    if (innerPath(request) === null) {
      if (next) return next();
      res.statusCode = 404;
      res.end();
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
    server.on("close", () => wss.close());
  }

  let closing: Promise<void> | undefined;
  return {
    appId: options.appId,
    basePath,
    handle: (request) => handle(request),
    next: () => ({ GET: nextHandler, POST: nextHandler, DELETE: nextHandler }),
    attach,
    nodeHandler,
    acceptSocket,
    get db() {
      return backend.db;
    },
    linkIdentity,
    async forRequest(request) {
      const user = await authenticate(request);
      if (!user) throw new Error("forRequest: the request has no signed-in user");
      return backendDbForSession(backend, await sessionFor(user));
    },
    async withAttribution(request) {
      const user = await authenticate(request);
      if (!user) throw new Error("withAttribution: the request has no signed-in user");
      return backendDbAttributedTo(backend, await sessionFor(user));
    },
    async accountFor(request) {
      const user = await authenticate(request);
      return user ? ((await sessionFor(user)).account_id ?? null) : null;
    },
    close() {
      closing ??= (async () => {
        for (const { session: stream } of streams.values()) stream.close();
        streams.clear();
        await session.close();
        await core.close();
      })();
      return closing;
    },
  };
}
