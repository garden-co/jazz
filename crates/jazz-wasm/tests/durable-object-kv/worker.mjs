// A SQLite-backed Durable Object running Jazz's store on its synchronous KV
// storage (`src/do_kv.rs`), driven over HTTP by `smoke.mjs`.
import * as jazz from "./pkg/jazz_wasm.js";
import wasm from "./pkg/jazz_wasm_bg.wasm";

jazz.initSync({ module: wasm });

const json = (value) => Response.json(value ?? null);

// Counts object instances in this isolate: a reset object comes back as a
// new one.
let instances = 0;

export class Store {
  constructor(ctx) {
    this.ctx = ctx;
    this.instance = ++instances;
    this.failNextCommit = false;
  }

  // `ctx` for the store, with a storage failure that can be injected.
  object() {
    const ctx = this.ctx;
    const storage = {
      kv: ctx.storage.kv,
      transactionSync: (callback) => {
        if (this.failNextCommit) {
          this.failNextCommit = false;
          throw new Error("injected storage failure");
        }
        return ctx.storage.transactionSync(callback);
      },
    };
    return { id: ctx.id, storage, abort: (reason) => ctx.abort(reason) };
  }

  async fetch(request) {
    const url = new URL(request.url);
    const args = JSON.parse(url.searchParams.get("args") ?? "[]");
    const storage = this.ctx.storage;
    try {
      switch (url.pathname) {
        case "/open":
          return json(await jazz.doKvBenchOpen(this.object(), args[0]));
        case "/write":
          return json(jazz.doKvBenchWrite(...args));
        case "/read":
          return json(jazz.doKvBenchRead());
        case "/close":
          return json(await jazz.doKvBenchClose());
        case "/destroy":
          return json(jazz.doKvBenchDestroy(this.object()));
        case "/instance":
          return json(this.instance);
        case "/fail-next-commit":
          this.failNextCommit = true;
          return json(true);
        case "/keys": {
          // What the store keeps in storage.
          let keys = 0;
          let bytes = 0;
          for (const [, value] of storage.kv.list({ prefix: "jazz-vfs:" })) {
            keys += 1;
            bytes += value.byteLength;
          }
          return json({ keys, bytes, all: [...storage.kv.list()].length });
        }
        case "/rollback-probe": {
          // The VFS's atomicity rests on this: a throw inside
          // transactionSync undoes the KV writes before it.
          storage.kv.put("probe-before", 1);
          try {
            storage.transactionSync(() => {
              storage.kv.put("probe-before", 2);
              storage.kv.put("probe-inside", 1);
              throw new Error("abort");
            });
          } catch {}
          const result = {
            before: storage.kv.get("probe-before"),
            inside: storage.kv.get("probe-inside") ?? null,
          };
          storage.kv.delete("probe-before");
          return json(result);
        }
        default:
          return new Response("not found", { status: 404 });
      }
    } catch (error) {
      return new Response(String(error?.stack ?? error), { status: 500 });
    }
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("object") ?? "default";
    return env.STORE.get(env.STORE.idFromName(name)).fetch(request);
  },
};
