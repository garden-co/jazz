// Keep real ESM named exports explicit. Node cannot reliably infer names from
// `module.exports = nativeBinding`, which is the correct CJS compatibility
// shape for napi-rs, so consumers such as jazz-tools must not depend on its
// static CJS-export heuristic.
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const nativeBinding = require("./index.cjs");

export default nativeBinding;
export const {
  captureTextAnchor,
  captureTextRange,
  checkClientMigrations,
  compileApp,
  detachStore,
  EncryptionRuntime,
  generate,
  generateForDir,
  JazzCore,
  JazzCoreSocket,
  JazzServer,
  loadAppArtifact,
  loadBackendApp,
  migrationDiff,
  migrationsCreate,
  migrationsCreateForDir,
  NapiDb,
  NapiThinRouter,
  StreamingMutation,
  Subscription,
  Transport,
  Write,
  mintLocalFirstToken,
  verifyGenerated,
  verifyLocalFirstIdentityProof,
  nativeArtifactFingerprint,
  resolveTextAnchor,
  resolveTextRange,
  schemaHash,
  schemasAreCompatible,
  schemaSourceHash,
  setStoreAppId,
  validateBindings,
  validateSchema,
  versionTables,
  writeAppArtifact,
  writeAppFiles,
} = nativeBinding;
