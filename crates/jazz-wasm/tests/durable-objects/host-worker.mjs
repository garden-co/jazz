// The Jazz core Durable Object (`durable-object/host.js`) over the fixture
// build, for `host.test.mjs`.
import { initSync, DurableObjectCore } from "./pkg/jazz_wasm.js";
import wasm from "./pkg/jazz_wasm_bg.wasm";
import { createJazzCore, routeJazzRequest } from "../../durable-object/host.js";

initSync({ module: wasm });

export const JazzCore = createJazzCore({ DurableObjectCore });

export default { fetch: routeJazzRequest };
