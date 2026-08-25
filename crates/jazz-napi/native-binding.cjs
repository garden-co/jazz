// This tracked bootstrap is part of the ABI contract. Builds atomically write
// only the ignored pointer, never this file. Published packages fall back to
// napi-rs's platform-aware loader when no local generation pointer exists.
const { existsSync } = require("node:fs");
const { join } = require("node:path");
const pointer = join(__dirname, "native-binding.pointer.cjs");
try {
  if (existsSync(pointer)) module.exports = require(pointer);
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
