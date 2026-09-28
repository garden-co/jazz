// Keep real ESM named exports explicit. Node cannot reliably infer names from
// `module.exports = nativeBinding`, which is the correct CJS compatibility
// shape for napi-rs, so consumers such as jazz-tools must not depend on its
// static CJS-export heuristic.
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const nativeBinding = require("./index.cjs");

export default nativeBinding;
export const {
  e2eeSodiumDecrypt,
  e2eeSodiumEncrypt,
  e2eeSodiumHash,
  e2eeSodiumKeyPair,
  e2eeSodiumNonce,
  e2eeSodiumOpen,
  e2eeSodiumSeal,
  e2eeSodiumSigningKeyPair,
  e2eeSodiumSign,
  e2eeSodiumVerify,
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
} = nativeBinding;
