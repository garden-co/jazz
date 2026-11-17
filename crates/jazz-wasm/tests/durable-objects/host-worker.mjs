// The packaged Jazz core Durable Object (`jazz-wasm/durable-object`, as
// `run.sh` copies it into `do/` with a test build), for
// `host.test.mjs`: the Worker a deployment writes.
import { JazzCore, routeJazzRequest } from "./do/index.js";

export { JazzCore };

export default { fetch: routeJazzRequest };
