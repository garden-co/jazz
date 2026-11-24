// `jazz-wasm/durable-object` (the `workerd` condition): the Jazz core a
// Cloudflare Durable Object hosts, over jazz-wasm's durable-object build,
// initialised here from its statically imported module (Wrangler bundles
// it as CompiledWasm). A Worker exports `JazzCore` as its Durable Object
// class and `routeJazzRequest` as its fetch (see `host.js`).
import { initSync, DurableObjectCore, __jazzReinstantiate } from "./pkg/jazz_wasm_do.js";
import wasm from "./pkg/jazz_wasm_do_bg.wasm";
import { jazzCoreClass, routeJazzRequest } from "./host.js";

initSync({ module: wasm });

export function r2(bucket) {
  if (
    !bucket ||
    typeof bucket.get !== "function" ||
    typeof bucket.put !== "function" ||
    typeof bucket.delete !== "function"
  ) {
    throw new Error("r2(): expected an R2 bucket binding");
  }
  return { kind: "r2", bucket };
}

export function createJazzCore(options) {
  if (typeof options?.blobs !== "function" && options?.blobs !== "sqlite") {
    throw new Error(
      'createJazzCore(): blobs must be "sqlite" or a function selecting a blob store',
    );
  }
  return jazzCoreClass(
    { DurableObjectCore, reinstantiate: () => __jazzReinstantiate(wasm) },
    options.blobs,
  );
}

export { routeJazzRequest };
