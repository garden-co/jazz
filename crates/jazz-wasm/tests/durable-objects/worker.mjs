// A Durable Object running Jazz on its own SQL (`ctx.storage.sql`), for
// the smoke test (`smoke.test.mjs`). Each request runs one step of the
// fixture's `durableObjectSmoke` (crates/jazz-wasm/src/durable_object.rs).
import { DurableObject } from "cloudflare:workers";
import { initSync, durableObjectSmoke } from "./pkg/jazz_wasm.js";
import wasm from "./pkg/jazz_wasm_bg.wasm";

initSync({ module: wasm });

export class Store extends DurableObject {
  async fetch(request) {
    const step = new URL(request.url).pathname.slice(1);
    try {
      return new Response(durableObjectSmoke(this.ctx.storage, step));
    } catch (error) {
      return new Response(String(error?.message ?? error), { status: 500 });
    }
  }
}

export default {
  fetch(request, env) {
    return env.STORE.get(env.STORE.idFromName("smoke")).fetch(request);
  },
};
