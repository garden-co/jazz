// The published Durable Object entry (`jazz-wasm/durable-object`) as
// `build:durable-object` built it, for `release.test.mjs`: the Worker a
// deployment writes.
import { JazzCore, routeJazzRequest } from "../../durable-object/index.js";

export { JazzCore };

export default { fetch: routeJazzRequest };
