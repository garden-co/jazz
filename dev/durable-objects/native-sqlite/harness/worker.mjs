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
import {
  WasmDb,
  WasmServerCore,
  durableObjectSqlQueries,
  durableObjectSqlStats,
  durableObjectSqlTrace,
  initSync,
} from "./pkg/jazz_do.js";
import wasmModule from "./pkg/jazz_do_bg.wasm";

let wasm = null;

function bytes(base64) {
  return Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
}

export class Authority {
  constructor(ctx, env) {
    this.ctx = ctx;
    // TRACE=1 counts execs by statement text; stats returns them.
    this.trace = Boolean(env.TRACE);
    this.db = null;
    this.sessions = new Set();
    this.ticking = false;
    this.again = false;
    this.ticks = [];
    this.frames = { in: 0, out: 0, inBytes: 0, outBytes: 0 };
    this.sqlBase = {};
    this.errors = [];
    this.schedules = 0;
  }

  async open(schema, config, reopen = false) {
    if (this.db || this.opening) throw new Error("authority is already open");
    this.opening = true;
    wasm ??= initSync({ module: wasmModule });
    if (this.trace) durableObjectSqlTrace(true);
    const start = performance.now();
    try {
      this.db = reopen
        ? await WasmServerCore.reopenDurableObject(this.ctx.storage, bytes(config), () =>
            this.schedule(),
          )
        : await WasmServerCore.openDurableObject(
            this.ctx.storage,
            bytes(schema),
            bytes(config),
            () => this.schedule(),
          );
      this.startup = reopen ? "catalogue" : "published";
    } finally {
      this.opening = false;
    }
    return performance.now() - start;
  }

  schedule() {
    this.schedules += 1;
    if (this.ticking) {
      this.again = true;
      return;
    }
    this.ticking = true;
    this.idle = new Promise((resolve) => {
      this.finishTick = resolve;
    });
    queueMicrotask(() => this.tick());
  }

  async waitForIdle() {
    while (this.ticking) await this.idle;
  }

  detach(session) {
    if (!this.sessions.delete(session)) return;
    session.transport.close();
    session.transport.free();
  }

  async tick() {
    try {
      do {
        this.again = false;
        const start = performance.now();
        await this.db.tick();
        for (const session of this.sessions) {
          try {
            for (const frame of session.transport.recvWireFrames()) {
              this.frames.out += 1;
              this.frames.outBytes += frame.byteLength;
              session.ws.send(frame);
            }
            if (session.transport.isClosed()) {
              session.ws.close(1000, "session ended");
              this.detach(session);
            }
          } catch (error) {
            this.errors.push(String(error?.stack ?? error));
            session.ws.close(1011, "session failed");
            this.detach(session);
          }
        }
        this.ticks.push(performance.now() - start);
      } while (this.again);
    } catch (error) {
      this.errors.push(String(error?.stack ?? error));
      console.error("tick failed", error);
    } finally {
      this.ticking = false;
      this.finishTick();
    }
  }

  stats(reset) {
    const ticks = [...this.ticks].sort((a, b) => a - b);
    const at = (p) => ticks[Math.min(ticks.length - 1, Math.floor(p * ticks.length))] ?? 0;
    const out = {
      runtime: "ServerCore",
      startup: this.startup,
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
      schedules: this.schedules,
      ...(this.trace ? { queries: durableObjectSqlQueries() } : {}),
    };
    if (reset) {
      this.ticks = [];
      this.frames = { in: 0, out: 0, inBytes: 0, outBytes: 0 };
      this.sqlBase = durableObjectSqlStats();
      this.schedules = 0;
    }
    return out;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/value-checks") {
      wasm ??= initSync({ module: wasmModule });
      WasmDb.checkDurableObjectValues(this.ctx.storage);
      return new Response("value checks passed");
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    // Frames as ArrayBuffers: newer compatibility dates default to Blob.
    server.binaryType = "arraybuffer";
    if (url.pathname === "/control") {
      server.addEventListener("message", async (event) => {
        const message = JSON.parse(event.data);
        try {
          await this.waitForIdle();
          if (message.op === "open" || message.op === "reopen") {
            if (message.op === "reopen" && "schema" in message)
              throw new Error("reopen takes no schema");
            server.send(
              JSON.stringify({
                ok: true,
                openMs: await this.open(message.schema, message.config, message.op === "reopen"),
              }),
            );
          } else if (message.op === "checkRejected") {
            this.db.checkRejectedRow(new Uint8Array(message.row), message.table);
            server.send(JSON.stringify({ ok: true }));
          } else if (message.op === "scopeChecks") {
            this.db.checkSqlScopes();
            server.send(JSON.stringify({ ok: true }));
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
      await this.waitForIdle();
      if (!this.db) return new Response("authority is not open", { status: 503 });
      const transport = this.db.acceptSubscriber(author);
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
        try {
          transport.sendWireFrame(frame);
          this.schedule();
        } catch (error) {
          this.errors.push(String(error?.stack ?? error));
          server.close(1008, "input refused");
          this.detach(session);
        }
      });
      server.addEventListener("close", () => this.detach(session));
      server.addEventListener("error", () => this.detach(session));
    } else {
      return new Response("not found", { status: 404 });
    }
    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);
    const name = parts.length === 2 ? parts.shift() : "app";
    url.pathname = `/${parts.join("/")}`;
    return env.AUTHORITY.get(env.AUTHORITY.idFromName(name)).fetch(new Request(url, request));
  },
};
