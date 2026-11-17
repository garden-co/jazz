// The packaged Jazz core Durable Object (`jazz-wasm/durable-object`), for
// `host.test.mjs`: the Worker a deployment writes.
import { JazzCore, routeJazzRequest } from "../../durable-object/index.js";

export { JazzCore };

export default { fetch: routeJazzRequest };
