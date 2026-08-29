// This tracked bootstrap is part of the ABI contract. Builds atomically write
// only the ignored pointer, never this file. Published packages fall back to
// napi-rs's platform-aware loader when no local generation pointer exists.
const { existsSync } = require("node:fs");
const { join } = require("node:path");
// Correctness builds select a worktree-private content-addressed generation here. It is
// ignored and absent from packages, so ordinary local/release loading retains
// the normal generated pointer and napi-rs fallback below.
const correctnessPointer = join(__dirname, "correctness-native-binding.pointer.cjs");
const pointer = join(__dirname, "native-binding.pointer.cjs");
try {
  if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1") {
    const binding = process.env.JAZZ_CORRECTNESS_NAPI_BINDING;
    const fingerprint = process.env.JAZZ_CORRECTNESS_NAPI_FINGERPRINT;
    if (!binding || !fingerprint)
      throw new Error("sealed correctness consumer is missing its admitted NAPI binding");
    // This exact path is supplied by the producer-manifest preflight.  Do not
    // follow a mutable worktree pointer here: another producer may publish one
    // while this consumer is still running.
    module.exports = {
      nativeBinding: require(binding),
      expectedNativeArtifactFingerprint: fingerprint,
    };
  } else if (existsSync(correctnessPointer)) module.exports = require(correctnessPointer);
  else if (existsSync(pointer)) module.exports = require(pointer);
  else {
    const nativeBinding = require("./native-loader.cjs");
    const { expectedNativeArtifactFingerprint } = require("./native-artifact-fingerprint.cjs");
    module.exports = { nativeBinding, expectedNativeArtifactFingerprint };
  }
} catch (error) {
  throw new Error(
    "Jazz NAPI artifact is missing. In this monorepo run pnpm --filter jazz-napi build:debug; " +
      "for an installed package reinstall matching Jazz package versions. " +
      `(${error.message})`,
  );
}
