// Build wiring only: all HTTP and startup behavior lives in production code.
import {
  initSync,
  WasmServerCore,
  authorizeDurableObjectAdmin,
  authorizeDurableObjectAccess,
  catalogueRequestsForTest,
} from "./pkg/jazz_do.js";
import wasmModule from "./pkg/jazz_do_bg.wasm";
import {
  createJazzDurableObject,
  routeJazzRequest,
} from "../../../../crates/jazz-wasm/durable-object-server.js";
initSync({ module: wasmModule });
export const JazzObject = createJazzDurableObject({
  WasmServerCore,
  authorizeDurableObjectAdmin,
  authorizeDurableObjectAccess,
});
export default {
  fetch(request, env) {
    // Fixture generation has no storage or access to host credentials.
    if (new URL(request.url).pathname === "/test-fixtures")
      return new Response(catalogueRequestsForTest());
    return routeJazzRequest(request, env);
  },
};
