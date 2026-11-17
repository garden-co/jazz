// The Jazz core a Cloudflare Durable Object hosts, around the Rust core in
// jazz-wasm's `durable-object` build (`DurableObjectCore`). One object per
// app (named by its app id) holds the app's store in its own SQL and serves
// the native server's routes for it:
//
//   GET  /health                           200 once the store is open
//   GET  /apps/{appId}/ws                  WebSocket: prelude, Hello, then wire frames
//   POST /apps/{appId}/backend/admit       204 for the app's backend
//   GET  /apps/{appId}/admin/migrations/graph, GET|POST /apps/{appId}/admin/bundles
//
// Bindings: JAZZ (this class's namespace), JAZZ_APP_ID, JAZZ_CORE_SECRET,
// and optionally JAZZ_SCHEMA (a public schema's JSON, served until one is
// published).
//
// What a deployment should know:
// - One secret, JAZZ_CORE_SECRET, admits the app's backend in every role a
//   native core keeps separate secrets for: backend links, admin routes
//   (publication) and authority links. Keep it to the app's backend.
// - Sockets are held open by the object (`accept()`, no WebSocket
//   hibernation yet): they drop when the object is evicted, and clients
//   reconnect. A peer must send its prelude and Hello within
//   HANDSHAKE_TIMEOUT_MS, as on a native core.
// - If the Rust core panics, the object drops it, closes every socket and
//   resets itself (`ctx.abort`); the next request opens a fresh core on the
//   same storage.
import { DurableObject } from "cloudflare:workers";

const decoder = new TextDecoder();

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

function close(ws, code, reason) {
  try {
    ws.close(code, String(reason).slice(0, 120));
  } catch {}
}

/**
 * The Durable Object class over `wasm` (jazz-wasm's durable-object build,
 * initialised). `index.js` exports it ready over the packaged build.
 */
export function jazzCoreClass(wasm) {
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
      this.core ??= this.guard(() =>
        wasm.DurableObjectCore.open(
          this.ctx.storage,
          this.env.JAZZ_APP_ID,
          this.env.JAZZ_CORE_SECRET,
          this.env.JAZZ_SCHEMA,
        ),
      );
      return this.core;
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
          this.core = null;
          for (const ws of this.sockets.values()) close(ws, 1011, "the core restarted");
          this.sockets.clear();
          try {
            this.ctx.abort?.("the Jazz core panicked");
          } catch {}
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
      const prefix = `/apps/${this.env.JAZZ_APP_ID}/`;
      if (!path.startsWith(prefix)) {
        return failure(404, "not_found", "route not found");
      }
      if (path !== `${prefix}ws`) {
        const core = this.open();
        const headers = request.headers;
        const credentials = new wasm.HttpCredentials(
          headers.get("X-Jazz-Admin-Secret") ?? undefined,
          headers.get("X-Jazz-Backend-Secret") ?? undefined,
          headers.get("X-Jazz-Backend-Credential") ?? undefined,
          headers.get("X-Jazz-Backend-Access") ?? undefined,
        );
        const body = new Uint8Array(await request.arrayBuffer());
        const answer = this.guard(() =>
          core.http(request.method, path.slice(prefix.length), credentials, body),
        );
        // A deployment may end sessions pinned to older bundles: tell them.
        this.flush();
        return answer.status === 204
          ? new Response(null, { status: 204 })
          : new Response(answer.body, {
              status: answer.status,
              headers: { "content-type": "application/json" },
            });
      }
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return failure(426, "upgrade_required", "WebSocket upgrade required");
      }
      const core = this.open();
      const [client, server] = Object.values(new WebSocketPair());
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
        if (socket !== null) {
          // What the socket's session still had, its error frame included.
          try {
            for (const batch of this.guard(() => this.core.output(socket)).batches) {
              server.send(batch);
            }
          } catch {}
          this.sockets.delete(socket);
          try {
            this.core?.close(socket);
          } catch {}
          socket = null;
        }
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
      const core = this.core;
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
            core.close(socket);
          } catch {}
        }
      }
      if (this.core !== null && core.wantsTick() && !this.flushQueued) {
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
      if (this.core !== null && this.sockets.size > 0) this.flush();
    }
  };
}

/** The Worker's fetch: everything goes to the app's one object. */
export function routeJazzRequest(request, env) {
  return env.JAZZ.get(env.JAZZ.idFromName(env.JAZZ_APP_ID)).fetch(request);
}
