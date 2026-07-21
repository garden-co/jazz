export {
  deploy,
  startLocalJazzServer,
  type DeployOptions,
  type LocalJazzServerHandle,
  type StartLocalJazzServerOptions,
} from "../dev/dev-server.js";
export { createPolicyTestApp, PolicyTestApp } from "./policy-test-app.js";
// In-code schema publishing for test servers: merge compiled permissions into
// the app's WasmSchema and publish it via `publishStoredSchema` — the
// code-first counterpart of the directory-based catalogue deploy.
export { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
export { startTestJwtIssuer, type TestJwtIssuerHandle } from "./test-jwt-issuer.js";
export type { TestDb } from "./policy-test-app.js";
