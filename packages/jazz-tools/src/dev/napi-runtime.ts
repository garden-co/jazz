// Stand-in for `jazz-napi` that `withJazz` aliases into Turbopack server
// bundles when `jazz-napi` is a workspace link (as in this monorepo's
// examples). Turbopack only externalizes `serverExternalPackages` that resolve
// into a `node_modules` directory; a workspace link resolves outside one, so
// Turbopack would bundle the native loader and fail to find its binding.
//
// Loading through `process.getBuiltinModule` keeps the require out of reach of
// Turbopack's static analysis (it follows `createRequire(import.meta.url)`), so
// Node loads the real package at runtime. Keep the named exports in step with
// jazz-napi's index.mjs; next.test.ts checks that they match.
import type * as JazzNapi from "jazz-napi";

const nodeModule = process.getBuiltinModule("module") as typeof import("node:module");
const napi = nodeModule.createRequire(import.meta.url)("jazz-napi") as typeof JazzNapi;

export default napi;
export const {
  JazzServer,
  NapiDb,
  StreamingMutation,
  Subscription,
  TestJwtIssuer,
  Transport,
  Write,
  mintLocalFirstToken,
  verifyLocalFirstIdentityProof,
  nativeArtifactFingerprint,
  validateSchema,
} = napi;
