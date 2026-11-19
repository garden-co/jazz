// The Jazz core a Cloudflare Durable Object hosts, around the Rust core in
// jazz-wasm's `durable-object` build (`DurableObjectCore`). The Worker
// sends everything to one object, which holds the app's store in its own
// SQL and serves the native server's routes for it:
//
//   GET  /health                       200 once the store is open
//   GET  /app                          {"appId"}: for the backend's credential
//   GET  /apps/{x}/ws                  WebSocket: prelude, Hello, then wire frames
//   POST /apps/{x}/backend/admit       204 for the app's backend
//   GET  /apps/{x}/admin/migrations/graph, GET|POST /apps/{x}/admin/bundles
//   POST /apps/{x}/admin/accounts/{resolve,login-or-register,link,
//        found-local-first,revoke,revoke-sessions}
//
// Nobody configures an app id: the object makes one on its first open and
// keeps it in its SQL (as a native core keeps `<data-dir>/app-id`), and
// serves its app whatever `{x}` is. Routes are checked, with the request's
// secrets, before a body (at most 8 MiB) is read.
//
// Bindings: JAZZ (this class's namespace), JAZZ_CORE_SECRET, and optionally
// JAZZ_SCHEMA (a public schema's JSON, served until one is published).
//
// What a deployment should know:
// - One secret, JAZZ_CORE_SECRET, admits the app's backend in every role a
//   native core keeps separate secrets for: backend links, admin routes
//   (publication, accounts) and authority links. The admin/backend split
//   (`X-Jazz-Backend-Access`, or which secret header is sent) is a
//   convention between the backend's own calls, not a boundary: whoever
//   holds the secret is admin. Keep it to the app's backend.
// - Sockets are held open by the object (`accept()`, no WebSocket
//   hibernation yet): they drop when the object is evicted, and clients
//   reconnect. A peer must send its prelude and Hello within
//   HANDSHAKE_TIMEOUT_MS, as on a native core.
// - If the Rust core panics, the trap leaves the wasm instance unusable:
//   the object drops its core, closes every socket (1011) and replaces the
//   instance with a fresh one; the next request opens a fresh core on it,
//   over the same storage.
import { DurableObject } from "cloudflare:workers";

const decoder = new TextDecoder();

/** The longest request body read (native's catalogue limit). */
const MAX_REQUEST_BYTES = 8 << 20;

/** How long a new socket may take to send its prelude and Hello (native's). */
const HANDSHAKE_TIMEOUT_MS = 2000;

function failure(status, code, message) {
  return Response.json({ error: { code, message } }, { status });
}

/** A binary message's bytes (an ArrayBuffer, a view, or a Blob), else null. */
async function bytes(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data))
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (typeof Blob !== "undefined" && data instanceof Blob)
    return new Uint8Array(await data.arrayBuffer());
  return null;
}

/** `request`'s body, or null when it is longer than `limit` bytes. */
async function readBody(request, limit) {
  if (Number(request.headers.get("content-length") ?? 0) > limit) return null;
  if (request.body === null) return new Uint8Array();
  const chunks = [];
  let length = 0;
  const reader = request.body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > limit) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function close(ws, code, reason) {
  try {
    ws.close(code, String(reason).slice(0, 120));
  } catch {}
}

/**
 * The Durable Object class over `wasm`: jazz-wasm's durable-object build
 * (initialised), as `{ DurableObjectCore, reinstantiate }`, where
 * `reinstantiate()` replaces its wasm instance after a trap. `index.js`
 * exports it ready over the packaged build.
 */
export function jazzCoreClass(wasm) {
  // Bumped whenever the instance is replaced: a core made on an older one
  // is dropped, never called.
  let instance = 0;
  return class JazzCore extends DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      this.env = env;
      this.core = null;
      this.sockets = new Map();
      this.flushQueued = false;
      this.alarmSet = false;
    }

    open() {
      if (this.current() === null) {
        this.core = this.guard(() =>
          wasm.DurableObjectCore.open(
            this.ctx.storage,
            this.env.JAZZ_CORE_SECRET,
            this.env.JAZZ_SCHEMA,
          ),
        );
        this.instance = instance;
      }
      return this.core;
    }

    /**
     * The core, if it is on the current wasm instance: one made on an
     * instance replaced since (after a panic in any object of this
     * isolate) is dropped, never called.
     */
    current() {
      if (this.core !== null && this.instance !== instance) this.drop();
      return this.core;
    }

    /**
     * Forget the core without calling into its instance (which may have
     * trapped, or been replaced): its memory goes with the instance.
     */
    drop() {
      try {
        this.core?.__destroy_into_raw();
      } catch {}
      this.core = null;
      for (const ws of this.sockets.values()) close(ws, 1011, "the core restarted");
      this.sockets.clear();
    }

    /**
     * Run `call` on the core. A Rust panic traps the wasm instance and
     * leaves the core unusable: drop it, close every socket, and reset the
     * object (the next request starts afresh on the same storage).
     */
    guard(call) {
      try {
        return call();
      } catch (error) {
        if (error instanceof WebAssembly.RuntimeError) {
          this.drop();
          instance += 1;
          wasm.reinstantiate();
        }
        throw error;
      }
    }

    async fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === "/health") {
        this.open();
        return new Response("ok");
      }
      if (!/^\/apps\/[^/]+\/ws$/.test(path)) {
        const header = (name) => request.headers.get(name) ?? undefined;
        const secrets = [
          header("X-Jazz-Admin-Secret"),
          header("X-Jazz-Backend-Secret"),
          header("X-Jazz-Backend-Credential"),
          header("X-Jazz-Backend-Access"),
        ];
        const core = this.open();
        // The route and secrets are checked before the body is read.
        let answer = this.guard(() => core.authorize(request.method, path, ...secrets));
        if (answer === undefined) {
          const body = await readBody(request, MAX_REQUEST_BYTES);
          if (body === null) {
            return failure(413, "payload_too_large", "the request body is too large");
          }
          const current = this.open();
          answer = this.guard(() => current.http(request.method, path, ...secrets, body));
          // A deployment may end sessions pinned to older bundles: tell them.
          this.flush();
        }
        return answer.contentType === ""
          ? new Response(null, { status: answer.status })
          : new Response(answer.body, {
              status: answer.status,
              headers: { "content-type": answer.contentType },
            });
      }
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return failure(426, "upgrade_required", "WebSocket upgrade required");
      }
      const core = this.open();
      const [client, server] = Object.values(new WebSocketPair());
      // Binary messages as ArrayBuffers, not Blobs (each Blob is one more
      // async read), whatever the Worker's compatibility date.
      server.binaryType = "arraybuffer";
      server.accept();
      const credential = request.headers.get("X-Jazz-Backend-Credential") ?? undefined;
      const access = request.headers.get("X-Jazz-Backend-Access") ?? undefined;
      let prelude = null;
      let socket = null;
      let ended = false;
      const end = (code, reason) => {
        if (ended) return;
        ended = true;
        clearTimeout(handshake);
        // A core replaced since (after a panic) numbers its own sockets:
        // only the core this socket was accepted by is told.
        if (socket !== null && this.current() === core) {
          // What the socket's session still had, its error frame included.
          try {
            for (const batch of this.guard(() => core.output(socket)).batches) {
              server.send(batch);
            }
          } catch {}
          this.sockets.delete(socket);
          try {
            if (this.current() === core) core.close(socket);
          } catch {}
        }
        socket = null;
        close(server, code, reason);
      };
      const handshake = setTimeout(() => {
        if (socket === null) end(1008, "the handshake timed out");
      }, HANDSHAKE_TIMEOUT_MS);
      // A socket's messages are taken in order, one at a time (reading a
      // Blob's bytes waits); a failure ends this socket alone, and the
      // queue never rejects, so later messages are not silently dropped.
      let queue = Promise.resolve();
      const take = async (data) => {
        if (ended) return;
        try {
          if (prelude === null) {
            prelude = typeof data === "string" ? data : decoder.decode(await bytes(data));
            return;
          }
          const message = await bytes(data);
          if (message === null) return end(1003, "wire frames are binary");
          if (this.current() !== core) return end(1011, "the core restarted");
          if (socket === null) {
            const accepted = this.guard(() => core.accept(prelude, message, credential, access));
            for (const frame of accepted.frames) server.send(frame);
            if (accepted.socket === undefined) return end(1008, "refused");
            clearTimeout(handshake);
            socket = accepted.socket;
            this.sockets.set(socket, server);
          } else {
            this.guard(() => core.receive(socket, message));
          }
          this.flush();
        } catch (error) {
          end(1011, error?.message ?? error);
        }
      };
      server.addEventListener("message", (event) => {
        queue = queue.then(() => take(event.data));
      });
      server.addEventListener("close", () => end(1000, "closed"));
      server.addEventListener("error", () => end(1011, "socket error"));
      return new Response(null, { status: 101, webSocket: client });
    }

    /**
     * Tick the engine once and send every socket what it has. While the core
     * wants another tick (more input waits, or taken output freed held
     * answers), the next one is queued behind the event loop, so output goes
     * out as it is made and no event runs the whole backlog. While sockets
     * are open, an alarm keeps the core's idle tick.
     */
    flush() {
      const core = this.current();
      if (core === null) return;
      try {
        this.guard(() => core.tick());
      } catch {
        return;
      }
      for (const [socket, ws] of this.sockets) {
        try {
          const out = this.guard(() => core.output(socket));
          for (const batch of out.batches) ws.send(batch);
          if (out.ended) {
            this.sockets.delete(socket);
            close(ws, 1000, "session ended");
          }
        } catch {
          // This socket is gone; the others are still served.
          this.sockets.delete(socket);
          try {
            if (this.current() === core) core.close(socket);
          } catch {}
        }
      }
      if (this.current() === core && core.wantsTick() && !this.flushQueued) {
        this.flushQueued = true;
        setTimeout(() => {
          this.flushQueued = false;
          this.flush();
        }, 0);
      }
      if (this.sockets.size > 0 && !this.alarmSet) {
        this.alarmSet = true;
        this.ctx.storage.setAlarm(Date.now() + wasm.DurableObjectCore.idleTickMs());
      }
    }

    /** The core's idle tick: time-based engine work, while sockets are open. */
    async alarm() {
      this.alarmSet = false;
      if (this.current() !== null && this.sockets.size > 0) this.flush();
    }
  };
}

/** The Worker's fetch: everything goes to the app's one object. */
export function routeJazzRequest(request, env) {
  return env.JAZZ.get(env.JAZZ.idFromName("jazz")).fetch(request);
}
