// dev/artifacts/build.mjs atomically replaces this bootstrap with a pointer to
// a sealed local generation. Published packages instead carry napi-rs's
// platform-aware loader, which resolves the normal optional dependency.
try {
  const nativeBinding = require("./native-loader.cjs");
  const { expectedNativeArtifactFingerprint } = require("./native-artifact-fingerprint.cjs");
  module.exports = { nativeBinding, expectedNativeArtifactFingerprint };
} catch (error) {
  throw new Error(
    "Jazz NAPI artifact is missing. In this monorepo run pnpm --filter jazz-napi build:debug; " +
      "for an installed package reinstall matching Jazz package versions. " +
      `(${error.message})`,
  );
}
