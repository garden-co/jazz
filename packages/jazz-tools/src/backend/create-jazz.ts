import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  randomBytes,
  randomUUID,
} from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import { SignJWT } from "jose";
import { JazzCore, type JazzCoreSocket, type JazzCoreSocketEvent } from "jazz-napi";
import { registerInProcessTransport } from "../runtime/in-process-transport.js";
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
import type { Db } from "../runtime/db.js";
import type { JazzSession } from "../session/state.js";
import { createJazzSession, type JazzClient } from "./create-jazz-session.js";
import type { BackendSchemaInput } from "./create-jazz-context.js";

/** Where the core keeps its data. */
export type JazzStore =
  | { readonly kind: "sqlite"; readonly path: string }
  | { readonly kind: "memory" };

/** A SQLite core in this process, kept in the directory `path`. */
export function sqlite(options: { path: string }): JazzStore {
  return { kind: "sqlite", path: options.path };
}

/** An in-memory core in this process: for tests and throwaway previews. */
export function memory(): JazzStore {
  return { kind: "memory" };
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
   * browser asks for its sync credential and in `forRequest`.
   */
  authenticate?: (request: Request) => JazzUser | null | Promise<JazzUser | null>;
  /** The path Jazz is mounted under. Defaults to `/jazz`. */
  basePath?: string;
  /** How long a browser's sync credential lasts. Defaults to one hour. */
  sessionTtlSeconds?: number;
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
  fetch(request: Request): Promise<Response>;
  /** Route handlers for a Next.js catch-all route (`app/jazz/[...path]/route.ts`). */
  next(): {
    GET(request: Request): Promise<Response>;
    POST(request: Request): Promise<Response>;
    DELETE(request: Request): Promise<Response>;
  };
  /**
   * Accept sync sockets on a Node HTTP server (needs the `ws` package).
   * Plain requests still go through `nodeHandler` or `fetch`.
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
  /** Close every session, the backend database and the core. */
  close(): Promise<void>;
}

const DEFAULT_BASE_PATH = "/jazz";
// Routes a browser may reach. Everything else (admin, backend admission,
// shutdown) stays inside this process.
const PUBLIC_APP_ROUTES = /^\/apps\/[^/]+\/(accounts\/[a-z/-]+|ws|schemas|schema\/[^/]+)$/;
const STRIPPED_HEADERS = ["x-jazz-backend-secret", "x-jazz-admin-secret", "x-jazz-session"];
const MAX_STREAM_MESSAGE_BYTES = 2 * 1024 * 1024;

interface SigningKey {
  privateKey: ReturnType<typeof createPrivateKey>;
  publicKeyPem: string;
}

async function loadSigningKey(store: JazzStore): Promise<SigningKey> {
  const fromPem = (pem: string): SigningKey => {
    const privateKey = createPrivateKey(pem);
    return {
      privateKey,
      publicKeyPem: createPublicKey(privateKey).export({ type: "spki", format: "pem" }).toString(),
    };
  };
  const generate = () =>
    generateKeyPairSync("ed25519").privateKey.export({ type: "pkcs8", format: "pem" }).toString();
  if (store.kind === "memory") return fromPem(generate());
  await mkdir(store.path, { recursive: true });
  const file = join(store.path, "auth-key.pem");
  try {
    return fromPem(await readFile(file, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const pem = generate();
  await writeFile(file, pem, { mode: 0o600, flag: "wx" }).catch(async (error) => {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  });
  return fromPem(await readFile(file, "utf8"));
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
function inProcessWebSocket(core: JazzCore): WebSocketConstructor {
  return class InProcessSocket implements BrowserWebSocket {
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    readyState = 0;
    private readonly listeners = new Map<string, Array<(event?: any) => void>>();
    private session: JazzCoreSocket | undefined;

    constructor(_url: string) {
      queueMicrotask(() => {
        if (this.readyState !== 0) return;
        try {
          this.session = core.openSocket([], (error, event) => this.onCoreEvent(error, event));
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

/**
 * Run Jazz inside your backend. The core opens in this process, applies the
 * schema, migrations and permissions your code declares, and serves sync on
 * your own origin under `basePath`. There is no separate Jazz server to run
 * and no deploy step: a rollback of your backend rolls Jazz back with it.
 */
export async function createJazz(options: CreateJazzOptions): Promise<Jazz> {
  const basePath = (options.basePath ?? DEFAULT_BASE_PATH).replace(/\/+$/, "");
  const issuer = `jazz:app:${options.appId}`;
  const audience = `jazz:${options.appId}`;
  const backendSecret = randomBytes(32).toString("base64url");
  const adminSecret = randomBytes(32).toString("base64url");
  const key = await loadSigningKey(options.store);
  const core = await JazzCore.open({
    appId: options.appId,
    backendSecret,
    adminSecret,
    ...(options.store.kind === "sqlite" ? { dataDir: options.store.path } : { inMemory: true }),
    jwtPublicKey: key.publicKeyPem,
    jwtIssuer: issuer,
    jwtAudience: audience,
  });

  const fetchCore: typeof fetch = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const body = new Uint8Array(await request.arrayBuffer());
    const response = await core.handle(
      request.method,
      url.pathname + url.search,
      [...request.headers].map(([name, value]) => [name, value]),
      Buffer.from(body),
    );
    return coreResponse(response);
  };
  const origin = `http://jazz-${randomUUID()}.invalid`;
  const unregister = registerInProcessTransport(origin, {
    fetch: fetchCore,
    WebSocket: inProcessWebSocket(core),
  });

  let session: JazzSession<JazzClient>;
  try {
    await applyBundle(origin, { ...options, adminSecret });
    session = await createJazzSession({
      appId: options.appId,
      app: options.app,
      permissions: options.permissions,
      serverUrl: origin,
      driver: { type: "memory" },
      jwtPublicKey: key.publicKeyPem,
      jwtIssuer: issuer,
      jwtAudience: audience,
      initial: { backendSecret },
    });
  } catch (error) {
    unregister();
    await core.close();
    throw error;
  }
  const opened = session.getSnapshot();
  if (opened.status !== "ready" || !opened.client) {
    await session.close();
    unregister();
    await core.close();
    throw opened.error ?? new Error(`Jazz backend session did not open (${opened.status})`);
  }
  const backend = opened.client;

  async function mintToken(user: JazzUser): Promise<{ token: string; expiresAt: number }> {
    const ttl = options.sessionTtlSeconds ?? 3600;
    const expiresAt = Math.floor(Date.now() / 1000) + ttl;
    const token = await new SignJWT({ ...user.claims })
      .setProtectedHeader({ alg: "EdDSA" })
      .setIssuer(issuer)
      .setAudience(audience)
      .setSubject(user.userId)
      .setIssuedAt()
      .setExpirationTime(expiresAt)
      .sign(key.privateKey);
    return { token, expiresAt };
  }

  async function authenticate(request: Request): Promise<JazzUser | null> {
    if (!options.authenticate) return null;
    return (await options.authenticate(request)) ?? null;
  }

  /**
   * A browser's sync session. The browser presents no credential: its first
   * message (the auth prelude) names the account it means to act as, and the
   * backend's own `authenticate` decides whether this request is that user.
   * The core then admits it as for any provider credential. The session ends
   * when that admission expires, and the client reconnects and is asked again.
   */
  interface ClientSession {
    send(data: Uint8Array): boolean;
    close(): void;
  }

  function openClientSession(
    request: Request,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
  ): ClientSession {
    const socket = core.openSocket(headerPairs(request.headers), onEvent);
    let ended = false;
    let expiry: ReturnType<typeof setTimeout> | undefined;
    const end = () => {
      if (ended) return;
      ended = true;
      if (expiry) clearTimeout(expiry);
      socket.close();
    };
    const admit = async (prelude: Uint8Array): Promise<boolean> => {
      const rewritten = await admitPrelude(request, prelude);
      if (!rewritten || ended) {
        onEvent(null, { kind: "close", code: 1008, reason: "not signed in" });
        end();
        return false;
      }
      if (rewritten.expiresAt) {
        expiry = setTimeout(end, Math.max(1000, rewritten.expiresAt * 1000 - Date.now()));
        (expiry as unknown as { unref?: () => void }).unref?.();
      }
      return socket.send(rewritten.bytes);
    };
    // Messages after the prelude wait for its admission, in order.
    let chain: Promise<boolean> | undefined;
    return {
      send(data) {
        if (ended) return false;
        chain = chain
          ? chain.then((ok) => ok && !ended && socket.send(data))
          : admit(data).catch(() => {
              end();
              return false;
            });
        return true;
      },
      close: end,
    };
  }

  /**
   * Replace a browser prelude's stated session with a credential the core
   * verifies, minted for the user `authenticate` finds on this request.
   * Preludes carrying their own credential pass through untouched.
   */
  async function admitPrelude(
    request: Request,
    prelude: Uint8Array,
  ): Promise<{ bytes: Uint8Array; expiresAt?: number } | null> {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(new TextDecoder().decode(prelude)) as Record<string, unknown>;
    } catch {
      return { bytes: prelude };
    }
    const stated = parsed.backend_session as { user_id?: unknown } | undefined;
    if (!stated || typeof stated !== "object" || parsed.backend_secret || parsed.jwt_token) {
      return { bytes: prelude };
    }
    const user = await authenticate(request);
    if (!user || user.userId !== stated.user_id) return null;
    const { token, expiresAt } = await mintToken(user);
    const strip = (auth: Record<string, unknown>) => {
      const { backend_session: _stated, ...rest } = auth;
      return { ...rest, jwt_token: token };
    };
    const inner =
      parsed.auth && typeof parsed.auth === "object"
        ? strip(parsed.auth as Record<string, unknown>)
        : undefined;
    const rewritten = { ...strip(parsed), ...(inner ? { auth: inner } : {}) };
    return { bytes: new TextEncoder().encode(JSON.stringify(rewritten)), expiresAt };
  }

  /**
   * The account sign-in a browser makes with only the app's cookie: the
   * backend's `authenticate` names the user, and the core resolves (or
   * creates) their account as it would for that user's own credential.
   */
  async function backendSignIn(request: Request, path: string): Promise<Response> {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const user = await authenticate(request);
    if (!user) return new Response("signed_out", { status: 401 });
    const { token, expiresAt } = await mintToken(user);
    const response = await core.handle(
      "POST",
      path.replace(/\/backend-sign-in$/, "/login-or-register"),
      [
        ["authorization", `Bearer ${token}`],
        ["content-type", "application/json"],
      ],
      Buffer.alloc(0),
    );
    if (response.status !== 200) return coreResponse(response);
    const assignment = JSON.parse(Buffer.from(response.body).toString("utf8")) as Record<
      string,
      unknown
    >;
    return Response.json(
      { ...assignment, claims: user.claims ?? {}, expiresAt },
      { headers: { "Cache-Control": "no-store" } },
    );
  }

  // HTTP stream sessions, by id.
  const streams = new Map<string, ClientSession>();

  function openStream(request: Request): Response {
    const id = randomUUID();
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
        streams.get(id)?.close();
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
    streams.set(id, socket);
    controller.enqueue(encodeStreamRecord(STREAM_RECORD_SESSION, new TextEncoder().encode(id)));
    return new Response(body, {
      headers: {
        "Content-Type": "application/octet-stream",
        "Cache-Control": "no-store",
        "X-Accel-Buffering": "no",
      },
    });
  }

  async function streamSend(id: string, request: Request): Promise<Response> {
    const socket = streams.get(id);
    if (!socket) return new Response("unknown stream session", { status: 404 });
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

  /** The path below `basePath`, or null for a request Jazz doesn't serve. */
  function innerPath(request: Request): string | null {
    const { pathname } = new URL(request.url);
    if (pathname === basePath) return "/";
    if (!pathname.startsWith(`${basePath}/`)) return null;
    return pathname.slice(basePath.length);
  }

  async function handle(request: Request): Promise<Response> {
    const path = innerPath(request);
    if (path === null) return new Response("Not found", { status: 404 });
    if (/^\/apps\/[^/]+\/accounts\/backend-sign-in$/.test(path)) {
      return backendSignIn(request, path);
    }
    const stream = /^\/apps\/[^/]+\/stream(?:\/([^/]+))?$/.exec(path);
    if (stream) {
      const id = stream[1] && decodeURIComponent(stream[1]);
      if (!id && request.method === "POST") return openStream(request);
      if (id && request.method === "POST") return streamSend(id, request);
      if (id && request.method === "DELETE") {
        streams.get(id)?.close();
        return new Response(null, { status: 204 });
      }
      return new Response(null, { status: 405 });
    }
    if (path.endsWith("/ws")) {
      // A socket upgrade this host couldn't perform. The browser client
      // switches to the HTTP stream when it sees this.
      return new Response("WebSocket upgrade not available here; use the HTTP stream", {
        status: 426,
      });
    }
    if (!PUBLIC_APP_ROUTES.test(path)) return new Response("Not found", { status: 404 });
    const url = new URL(request.url);
    const body =
      request.method === "GET" || request.method === "HEAD"
        ? Buffer.alloc(0)
        : Buffer.from(await request.arrayBuffer());
    return coreResponse(
      await core.handle(request.method, path + url.search, headerPairs(request.headers), body),
    );
  }

  async function nextHandler(request: Request): Promise<Response> {
    const path = innerPath(request);
    if (
      request.method === "GET" &&
      path?.endsWith("/ws") &&
      request.headers.get("upgrade")?.toLowerCase() === "websocket" &&
      process.env.VERCEL
    ) {
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
    return handle(request);
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
        res.statusCode = 500;
        res.end(error instanceof Error ? error.message : String(error));
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
      wss.handleUpgrade(req, socket, head, (ws) => acceptSocket(request, ws));
    });
    server.on("close", () => wss.close());
  }

  let closing: Promise<void> | undefined;
  return {
    appId: options.appId,
    basePath,
    fetch: handle,
    next: () => ({ GET: nextHandler, POST: nextHandler, DELETE: nextHandler }),
    attach,
    nodeHandler,
    acceptSocket,
    get db() {
      return backend.db;
    },
    async forRequest(request) {
      const user = await authenticate(request);
      if (!user) throw new Error("forRequest: the request has no signed-in user");
      const { token } = await mintToken(user);
      return backend.forRequest(
        { headers: { authorization: `Bearer ${token}` } },
        { account: "login-or-register" },
      );
    },
    close() {
      closing ??= (async () => {
        for (const socket of streams.values()) socket.close();
        streams.clear();
        await session.close();
        unregister();
        await core.close();
      })();
      return closing;
    },
  };
}
