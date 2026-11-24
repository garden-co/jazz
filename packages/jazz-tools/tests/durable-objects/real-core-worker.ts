// The Worker an app deploys for its core, exactly as the docs show it.
import { createJazzCore, routeJazzRequest } from "jazz-tools/durable-object";

export const JazzCore = createJazzCore({ blobs: "sqlite" });
export default { fetch: routeJazzRequest };
