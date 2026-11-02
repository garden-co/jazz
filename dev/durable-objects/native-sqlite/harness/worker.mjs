// A Jazz authority in one Durable Object, on the object's own SQLite.
//
// GET /control (WebSocket): JSON requests, one reply each.
//   {op: "open", schema, config}  base64 public schema JSON and open config
//   {op: "stats", reset?}         tick timings, SQL counts, wasm memory
// GET /session?author=<canonical author JSON> (WebSocket): one sync
//   session; each binary message is one wire frame, both ways.
//
// The authority ticks whenever it has sync work, in the same event as
// the frames that caused it; each tick's wall time is recorded.
import { WasmDb, durableObjectSqlStats, initSync } from "./pkg/jazz_do.js";
import wasmModule from "./pkg/jazz_do_bg.wasm";

let wasm = null;

function bytes(base64) {
  return Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
}

export class Authority {
  constructor(ctx) {
    this.ctx = ctx;
    this.db = null;
    this.sessions = new Set();
    this.ticking = false;
    this.again = false;
    this.ticks = [];
    this.frames = { in: 0, out: 0, inBytes: 0, outBytes: 0 };
    this.sqlBase = {};
    this.errors = [];
  }

  open(schema, config) {
    wasm ??= initSync({ module: wasmModule });
    const start = performance.now();
    this.db = WasmDb.openDurableObject(this.ctx.storage.sql, bytes(schema), bytes(config));
    this.db.setTickScheduler(() => this.schedule());
    this.db.onMutationError?.((error) => this.errors.push(`mutation: ${error}`));
    return performance.now() - start;
  }

  schedule() {
    if (this.ticking) {
      this.again = true;
      return;
    }
    this.ticking = true;
    queueMicrotask(() => this.tick());
  }

  async tick() {
    do {
      this.again = false;
      const start = performance.now();
      try {
        await this.db.tick();
      } catch (error) {
        this.errors.push(String(error?.stack ?? error));
        console.error("tick failed", error);
      }
      for (const session of this.sessions) {
        for (const frame of session.transport.recvWireFrames()) {
          this.frames.out += 1;
          this.frames.outBytes += frame.byteLength;
          session.ws.send(frame);
        }
      }
      this.ticks.push(performance.now() - start);
    } while (this.again);
    this.ticking = false;
  }

  stats(reset) {
    const ticks = [...this.ticks].sort((a, b) => a - b);
    const at = (p) => ticks[Math.min(ticks.length - 1, Math.floor(p * ticks.length))] ?? 0;
    const out = {
      ticks: ticks.length,
      tickMsTotal: ticks.reduce((a, b) => a + b, 0),
      tickMsP50: at(0.5),
      tickMsP99: at(0.99),
      tickMsMax: ticks[ticks.length - 1] ?? 0,
      frames: { ...this.frames },
      sessions: this.sessions.size,
      sql: Object.fromEntries(
        Object.entries(durableObjectSqlStats()).map(([k, v]) => [k, v - (this.sqlBase[k] ?? 0)]),
      ),
      wasmMemoryBytes: wasm?.memory.buffer.byteLength ?? null,
      databaseBytes: this.ctx.storage.sql.databaseSize,
      errors: this.errors.slice(-5),
    };
    if (reset) {
      this.ticks = [];
      this.frames = { in: 0, out: 0, inBytes: 0, outBytes: 0 };
      this.sqlBase = durableObjectSqlStats();
    }
    return out;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    // Frames as ArrayBuffers: newer compatibility dates default to Blob.
    server.binaryType = "arraybuffer";
    if (url.pathname === "/control") {
      server.addEventListener("message", (event) => {
        const message = JSON.parse(event.data);
        try {
          if (message.op === "open") {
            server.send(
              JSON.stringify({ ok: true, openMs: this.open(message.schema, message.config) }),
            );
          } else if (message.op === "stats") {
            server.send(JSON.stringify({ ok: true, ...this.stats(message.reset) }));
          } else {
            server.send(JSON.stringify({ ok: false, error: `unknown op ${message.op}` }));
          }
        } catch (error) {
          server.send(JSON.stringify({ ok: false, error: String(error?.stack ?? error) }));
        }
      });
    } else if (url.pathname === "/session") {
      const author = new TextEncoder().encode(url.searchParams.get("author"));
      const transport = await this.db.acceptSubscriber(author, null);
      const session = { ws: server, transport };
      this.sessions.add(session);
      server.addEventListener("message", (event) => {
        if (typeof event.data === "string") {
          this.errors.push(`text message on a session: ${event.data.slice(0, 100)}`);
          return;
        }
        const frame = new Uint8Array(event.data);
        this.frames.in += 1;
        this.frames.inBytes += frame.byteLength;
        transport.sendWireFrame(frame);
        this.schedule();
      });
      server.addEventListener("close", () => {
        this.sessions.delete(session);
        transport.close?.();
      });
    } else {
      return new Response("not found", { status: 404 });
    }
    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  fetch(request, env) {
    return env.AUTHORITY.get(env.AUTHORITY.idFromName("app")).fetch(request);
  },
};
