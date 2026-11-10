import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Http2SecureServer, Http2Server } from "node:http2";
import type { Duplex } from "node:stream";
import {
  captureTextAnchor,
  captureTextRange,
  resolveTextAnchor,
  resolveTextRange,
} from "jazz-napi";
import { textAnchorsFrom } from "../runtime/text-anchors.js";
import { closingBeforeDeadline } from "./socket-deadline.js";
import { optionalPeerImportError } from "./optional-peer.js";
import {
  createJazzService,
  validateJazzOptions,
  type CreateJazzOptions,
  type JazzService,
  type JazzServiceApi,
  type JazzStore,
  type WsLikeSocket,
  MAX_STREAM_MESSAGE_BYTES,
} from "./create-jazz-shared.js";
import { validateSchemaAndPermissions } from "../dev/schema-validation.js";
import { onVercelHost } from "./runtime-environment.js";
export {
  acceptLocalFirstAuth,
  pinnedSocket,
  CLOSE_WAITS_FOR_OPEN_MS,
  MAX_HELD_SOCKETS,
  MAX_HELD_SOCKET_BYTES,
  type CreateJazzOptions,
  type JazzStore,
  type JazzUser,
  type LocalFirstGuest,
  type WsLikeSocket,
  type CoreSocket,
} from "./create-jazz-shared.js";
export {
  sqlite,
  memory,
  remote,
  vercel,
  durableObjects,
  files,
  s3,
  vercelBlob,
} from "./core-providers/index.js";

export interface Jazz extends Omit<JazzServiceApi, "ready"> {
  ready(): Promise<Jazz>;
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
   * Accept sync sockets on a Node HTTP server (needs the `ws` package),
   * such as the one Hono's `serve()` returns. It doesn't open the store:
   * that happens on the first socket or request, and while the store can't
   * open (a remote core that is down) sockets and requests get a 503 the
   * clients retry. Plain requests still go through `nodeHandler` or `handle`.
   */
  attach(server: Server | Http2Server | Http2SecureServer): Promise<void>;
  /** Node `(req, res)` handler for requests under `basePath` (Express, Fastify's raw mode). */
  nodeHandler(req: IncomingMessage, res: ServerResponse, next?: () => void): void;
}

interface ProcessJazz {
  service: JazzService;
  jazz: Jazz;
  store: JazzStore;
  after?: Promise<void>;
}
// Stable across module reloads. Request hosts never use this registry.
const REGISTRY = Symbol.for("jazz-tools.createJazz.services");
function registry(): Map<string, ProcessJazz> {
  const scope = globalThis as { [REGISTRY]?: Map<string, ProcessJazz> };
  return (scope[REGISTRY] ??= new Map());
}

/** Open lazily, reusing named stores within this Node process. */
export function createJazz(options: CreateJazzOptions): Jazz {
  validateJazzOptions(options);
  const key = options.store.reuse?.key;
  const entries = registry();
  const previous = key === undefined ? undefined : entries.get(key);
  if (previous && !previous.service.closing) {
    adoptStore(previous, options.store);
    previous.service.configure({ ...options, store: liveStore(previous) });
    return previous.jazz;
  }
  const entry = { store: options.store, after: previous?.service.whenReleased } as ProcessJazz;
  const service = createJazzService(
    { ...options, store: liveStore(entry) },
    validateSchemaAndPermissions,
    textAnchorsFrom({ captureTextAnchor, captureTextRange, resolveTextAnchor, resolveTextRange }),
  );
  const jazz = nodeAdapter(service);
  entry.service = service;
  entry.jazz = jazz;
  if (key !== undefined) entries.set(key, entry);
  void service.whenOpened.then(() => {
    if (!service.closing) drainOnSignal(entry);
  });
  void service.whenReleased.then(() => {
    if (key !== undefined && entries.get(key) === entry) entries.delete(key);
    undrain(entry);
  });
  return jazz;
}

function nodeAdapter(service: JazzService): Jazz {
  const attachedTo = new WeakSet<object>();
  async function nextHandler(request: Request): Promise<Response> {
    if (onVercelHost()) {
      if (
        request.method === "GET" &&
        isSocketUpgrade(request.url, service.basePath) &&
        request.headers.get("upgrade")?.toLowerCase() === "websocket"
      ) {
        const refusal = await service.checkSocket(request);
        if (refusal) return refusal;
        let functions;
        try {
          functions = await import("@vercel/functions");
        } catch (error) {
          throw optionalPeerImportError("Jazz on Vercel", "@vercel/functions", error);
        }
        const deadline = functions.getDeadline?.()?.getTime();
        return functions.upgradeWebSocket(
          (socket) =>
            service.acceptSocket(
              request,
              deadline === undefined ? socket : closingBeforeDeadline(socket, deadline),
            ),
          { maxPayload: MAX_STREAM_MESSAGE_BYTES },
        );
      }
    }
    return service.handle(request, onVercelHost() ? true : undefined);
  }
  const jazz: Jazz = {
    get appId() {
      return service.appId;
    },
    get basePath() {
      return service.basePath;
    },
    get db() {
      return service.db;
    },
    async ready() {
      await service.ready();
      return jazz;
    },
    handle: (request) => service.handle(request),
    next: () => ({ GET: nextHandler, POST: nextHandler, DELETE: nextHandler }),
    nodeHandler: (req, res, next) => nodeHandler(req, res, service, next),
    acceptSocket: (request, socket) => service.acceptSocket(request, socket),
    forReqUser: (request) => service.forReqUser(request),
    withAttribution: (request) => service.withAttribution(request),
    accountFor: (request) => service.accountFor(request),
    createAccountFor: (user) => service.createAccountFor(user),
    linkIdentity: (link) => service.linkIdentity(link),
    close: () => service.close(),
    async attach(server) {
      if (http2Only(server))
        throw new Error(
          "attach(): an HTTP/2 server has no WebSocket upgrades; pass an HTTP/1 server (or an HTTPS one with allowHTTP1), or serve Jazz with handle() alone, over its HTTP stream",
        );
      if (attachedTo.has(server)) return;
      attachedTo.add(server);
      const { WebSocketServer } = (await import("ws" as string)) as {
        WebSocketServer: new (options: { noServer: true; maxPayload: number }) => SocketUpgrader;
      };
      const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_STREAM_MESSAGE_BYTES });
      service.markSocketSupport();
      const upgradeJazz = async (req: IncomingMessage, socket: Duplex, head: Buffer) => {
        let request: Request;
        try {
          request = nodeRequest(req);
        } catch {
          socket.end(BAD_UPGRADE);
          return;
        }
        const refusal = await service.checkSocket(request);
        if (refusal) {
          socket.end(
            `HTTP/1.1 ${refusal.status} Refused\r\n${refusal.headers.has("retry-after") ? `Retry-After: ${refusal.headers.get("retry-after")}\r\n` : ""}Connection: close\r\n\r\n`,
          );
          return;
        }
        wss.handleUpgrade(req, socket, head, (ws) => service.acceptSocket(request, ws));
      };
      const events = server as unknown as NodeJS.EventEmitter;
      // Own Jazz upgrades so other listeners cannot consume the same socket.
      events.on("upgrade", () => undefined);
      const emit = events.emit;
      events.emit = function (
        this: NodeJS.EventEmitter,
        event: string | symbol,
        ...args: unknown[]
      ) {
        if (
          event === "upgrade" &&
          isSocketUpgrade((args[0] as IncomingMessage).url, service.basePath)
        ) {
          void upgradeJazz(...(args as [IncomingMessage, Duplex, Buffer])).catch((error) => {
            console.error("jazz: a socket upgrade failed", error);
            (args[1] as Duplex).destroy();
          });
          return true;
        }
        return emit.call(this, event, ...args);
      } as typeof events.emit;
      events.on("close", () => wss.close());
    },
  };
  // Reading appId before ready() must remain opt-in (including object inspection).
  Object.defineProperty(jazz, "appId", { enumerable: false, get: () => service.appId });
  return jazz;
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
  // No bytes to read: what a parser made of nothing (`{}`) doesn't matter.
  const empty =
    req.headers["content-length"] === "0" ||
    (req.headers["content-length"] === undefined && req.headers["transfer-encoding"] === undefined);
  const json = /^application\/json\b/i.test(req.headers["content-type"] ?? "");
  let body: BodyInit | undefined;
  if (hasBody && req.readableEnded) {
    if (parsed instanceof Uint8Array) {
      body = parsed as BodyInit;
    } else if (empty) {
      body = undefined;
    } else if (json && parsed !== null && typeof parsed === "object") {
      // express.json()'s result: Jazz's JSON routes read it as JSON again
      // (none signs its bytes), so it serves as the body.
      body = JSON.stringify(parsed);
      headers.delete("content-length");
    } else {
      throw new Error(
        "jazz: the request body was already read by another handler; mount jazz.nodeHandler before body parsers such as express.json()",
      );
    }
  } else if (hasBody) {
    body = req as unknown as BodyInit;
  }
  return new Request(`http://${host}${req.url ?? "/"}`, {
    method: req.method,
    headers,
    ...(body === undefined ? {} : { body, duplex: "half" }),
  } as RequestInit);
}

function nodeHandler(
  req: IncomingMessage,
  res: ServerResponse,
  service: JazzService,
  next?: () => void,
): void {
  // Only Jazz's own paths: a request for the app passes on untouched, its
  // body unread, whatever read it before.
  const basePath = service.basePath;
  const pathname = requestPathname(req.url);
  if (pathname === null || (pathname !== basePath && !pathname.startsWith(`${basePath}/`))) {
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
    // A code the client reports, so the browser doesn't fail silently.
    res.statusCode = 500;
    res.end("request_body_already_read");
    return;
  }
  void service.handle(request).then(
    async (response) => {
      res.statusCode = response.status;
      response.headers.forEach((value, name) => {
        if (req.httpVersionMajor < 2 || name !== "connection") res.setHeader(name, value);
      });
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

/** What `attach()` hands an upgrade to: the `ws` package's server, without one of its own. */
interface SocketUpgrader {
  handleUpgrade(
    req: IncomingMessage,
    socket: Duplex,
    head: Buffer,
    callback: (ws: WsLikeSocket) => void,
  ): void;
  close(): void;
}

/** A sync socket's path below `basePath`. */
const SOCKET_PATH = /^\/apps\/[^/]+\/ws$/;

/** Whether an upgrade's URL is a sync socket of this Jazz (`<basePath>/apps/<app>/ws`). */
function isSocketUpgrade(url: string | undefined, basePath: string): boolean {
  const pathname = requestPathname(url);
  return (
    pathname !== null &&
    pathname.startsWith(`${basePath}/`) &&
    SOCKET_PATH.test(pathname.slice(basePath.length))
  );
}

/** A Node request's path, or null for one no URL can be made of: not Jazz's. */
function requestPathname(url: string | undefined): string | null {
  try {
    return new URL(url ?? "/", "http://localhost").pathname;
  } catch {
    return null;
  }
}

/**
 * Whether `server` is an HTTP/2 server that can't serve HTTP/1: one without
 * upgrades (h2c, or TLS without `allowHTTP1`). Node keeps the options under
 * a symbol; a server that hides them is taken as able.
 */
function http2Only(server: object): boolean {
  const name = server.constructor?.name;
  if (name === "Http2Server") return true;
  if (name !== "Http2SecureServer") return false;
  const key = Object.getOwnPropertySymbols(server).find(
    (symbol) => symbol.description === "options",
  );
  const options = key
    ? (server as Record<symbol, { allowHTTP1?: boolean } | undefined>)[key]
    : undefined;
  return options !== undefined && options.allowHTTP1 !== true;
}

const BAD_UPGRADE = "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
/** The signals a host stops a server with (a deploy, a rolling restart, Ctrl-C). */
const SHUTDOWN_SIGNALS = ["SIGTERM", "SIGINT"] as const;
/** Every open Jazz in this process, closed when the process is asked to stop. */
const drained = new Set<ProcessJazz>();
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
function drainOnSignal(entry: ProcessJazz): void {
  if (typeof process === "undefined" || typeof process.on !== "function") return;
  if (drained.size === 0) {
    for (const signal of SHUTDOWN_SIGNALS) process.on(signal, onShutdownSignal);
  }
  drained.add(entry);
}

function undrain(entry: ProcessJazz): void {
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
 * What a store's settings are. The functions it holds (a credential hook)
 * are not settings: the open store calls the latest call's. A Sandbox SDK
 * is: another one is another setting.
 */
function storeSettings(store: JazzStore): string {
  return JSON.stringify(store.reuse?.settings ?? {}, (name, value: unknown) => {
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
function liveStore(entry: ProcessJazz): JazzStore {
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
  return {
    ...store,
    async open() {
      await entry.after;
      return store.open.call(this);
    },
    ...(store.reuse
      ? {
          reuse: {
            key: store.reuse.key,
            settings: through(store.reuse.settings, () => entry.store.reuse?.settings),
          },
        }
      : {}),
  };
}

/**
 * A later `createJazz` for an entry's store. Before the store opens it takes
 * the new settings; once open, settings that would need another core are
 * refused, so a change never silently goes unapplied.
 */
function adoptStore(entry: ProcessJazz, store: JazzStore): void {
  if (!entry.service.started) {
    entry.store = store;
    // A later call (a hot reload after a fix) tries the open again at once.
    return;
  }
  if (storeSettings(store) !== storeSettings(entry.store)) {
    throw new Error(
      `createJazz(): ${entry.store.reuse?.key} is already open in this process with other store settings; restart the process to change them`,
    );
  }
  // The same settings: its function settings are the latest call's now.
  entry.store = store;
}

export type {
  BlobStore,
  S3BlobStore,
  VercelBlobStore,
  BackendCredential,
  VercelStoreOptions,
  DurableObjectsOptions,
} from "./core-providers/index.js";
