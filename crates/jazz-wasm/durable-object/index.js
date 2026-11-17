// `jazz-wasm/durable-object` (the `workerd` condition): the Jazz core a
// Cloudflare Durable Object hosts, over jazz-wasm's durable-object build,
// initialised here from its statically imported module (Wrangler bundles
// it as CompiledWasm). A Worker exports `JazzCore` as its Durable Object
// class and `routeJazzRequest` as its fetch (see `host.js`).
import { initSync, DurableObjectCore, __jazzReinstantiate } from "./pkg/jazz_wasm_do.js";
import wasm from "./pkg/jazz_wasm_do_bg.wasm";
import { jazzCoreClass, routeJazzRequest } from "./host.js";

initSync({ module: wasm });

/** The Durable Object class holding an app's Jazz core. */
export const JazzCore = jazzCoreClass({
  DurableObjectCore,
  reinstantiate: () => __jazzReinstantiate(wasm),
});

/** `JazzCore`, for callers that prefer a factory. */
export function createJazzCore() {
  return JazzCore;
}

export { routeJazzRequest };
