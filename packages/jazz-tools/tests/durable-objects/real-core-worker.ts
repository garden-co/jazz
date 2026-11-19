// The Worker an app deploys for its core, exactly as the docs show it.
import { JazzCore, routeJazzRequest } from "jazz-tools/durable-object";

export { JazzCore };
export default { fetch: routeJazzRequest };
