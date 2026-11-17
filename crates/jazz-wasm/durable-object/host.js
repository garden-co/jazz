// The Jazz core a Cloudflare Durable Object hosts, around the Rust core in
// jazz-wasm's `durable-object` build (`DurableObjectCore`). One object per
// app (named by its app id) holds the app's store in its own SQL and serves
// the native server's routes for it:
//
//   GET  /health              200 once the store is open
//   GET  /apps/{appId}/ws     WebSocket: prelude, Hello, then wire frames
//
// Bindings: JAZZ (this class's namespace), JAZZ_APP_ID, JAZZ_CORE_SECRET
// (admits the app's backend: `X-Jazz-Backend-Credential` +
// `X-Jazz-Backend-Access` on the upgrade, or the prelude's secrets), and,
// until publication reaches the object, JAZZ_SCHEMA (the app's public
// schema as JSON).
import { DurableObject } from "cloudflare:workers";

const decoder = new TextDecoder();

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
    }

    open() {
      this.core ??= wasm.DurableObjectCore.open(
        this.ctx.storage,
        this.env.JAZZ_APP_ID,
        this.env.JAZZ_CORE_SECRET,
        this.env.JAZZ_SCHEMA,
      );
      return this.core;
    }

    async fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === "/health") {
        this.open();
        return new Response("ok");
      }
      if (path !== `/apps/${this.env.JAZZ_APP_ID}/ws`) {
        return failure(404, "not_found", "route not found");
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
      const end = (code, reason) => {
        if (socket !== null) {
          this.sockets.delete(socket);
          core.close(socket);
          socket = null;
        }
        try {
          server.close(code, reason.slice(0, 120));
        } catch {}
      };
      // A socket's messages are taken in order, one at a time: reading a
      // Blob's bytes waits.
      let queue = Promise.resolve();
      const take = async (data) => {
        try {
          if (prelude === null) {
            prelude = typeof data === "string" ? data : decoder.decode(await bytes(data));
            return;
          }
          const message = await bytes(data);
          if (message === null) return end(1003, "wire frames are binary");
          if (socket === null) {
            const accepted = core.accept(prelude, message, credential, access);
            for (const frame of accepted.frames) server.send(frame);
            if (accepted.socket === undefined) return end(1008, "refused");
            socket = accepted.socket;
            this.sockets.set(socket, server);
          } else {
            core.receive(socket, message);
          }
          this.flush();
        } catch (error) {
          this.flush();
          end(1011, String(error?.message ?? error));
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
     * Run the engine and send every socket what it has, again while the
     * core wants a tick (taking output can free held answers), and keep an
     * alarm for the core's idle tick while sockets are open.
     */
    flush() {
      do {
        this.core.tick();
        for (const [socket, ws] of this.sockets) {
          const out = this.core.output(socket);
          for (const batch of out.batches) ws.send(batch);
          if (out.ended) {
            this.sockets.delete(socket);
            try {
              ws.close(1000, "session ended");
            } catch {}
          }
        }
      } while (this.core.wantsTick());
      if (this.sockets.size > 0) {
        this.ctx.storage.setAlarm(Date.now() + wasm.DurableObjectCore.idleTickMs());
      }
    }

    /** The core's idle tick: time-based engine work, while sockets are open. */
    async alarm() {
      if (this.core !== null && this.sockets.size > 0) this.flush();
    }
  };
}

/** The Worker's fetch: everything goes to the app's one object. */
export function routeJazzRequest(request, env) {
  return env.JAZZ.get(env.JAZZ.idFromName(env.JAZZ_APP_ID)).fetch(request);
}
