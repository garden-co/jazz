// The published Durable Object entry (`jazz-wasm/durable-object`) as
// `build:durable-object` built it, for `release.test.mjs`: the Worker a
// deployment writes.
import { createJazzCore, routeJazzRequest } from "../../durable-object/index.js";

export const JazzCore = createJazzCore({ blobs: "sqlite" });

export default { fetch: routeJazzRequest };
