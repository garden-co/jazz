// The packaged Jazz core Durable Object (`jazz-wasm/durable-object`, as
// `run.sh` copies it into `do/` with a test build), for
// `host.test.mjs`: the Worker a deployment writes.
import { createJazzCore, routeJazzRequest } from "./do/index.js";

export const JazzCore = createJazzCore({ blobs: "sqlite" });

export default { fetch: routeJazzRequest };
