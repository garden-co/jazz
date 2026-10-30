// This tracked bootstrap is part of the ABI contract. Builds atomically write
// only the ignored pointer, never this file. Published packages fall back to
// napi-rs's platform-aware loader when no local generation pointer exists.
const { existsSync } = require("node:fs");
const { join } = require("node:path");
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
    // A correctness snapshot is selected only by the explicit, manifest-admitted
    // environment above.  In particular, never let a leftover ignored test
    // pointer override a normal local/package generation: after sources are
    // restored, that snapshot may carry a binary from the prior source state.
  } else if (existsSync(pointer)) module.exports = require(pointer);
  else {
    const nativeBinding = require("./native-loader.cjs");
    const { expectedNativeArtifactFingerprint } = require("./native-artifact-fingerprint.cjs");
    module.exports = { nativeBinding, expectedNativeArtifactFingerprint };
  }
} catch (error) {
  // napi-rs chains failed platform candidates through `cause`; its outer
  // message alone can describe an incompatible installed binary as missing.
  const causes = [];
  const seen = new Set();
  for (let cause = error; cause && !seen.has(cause); cause = cause.cause) {
    seen.add(cause);
    causes.push(cause);
  }
  // Exclude only napi-rs's generic summary, not uncoded candidate errors:
  // a package version mismatch is uncoded and must not become "missing".
  const candidates =
    causes.length > 1 && error.message?.startsWith("Cannot find native binding.")
      ? causes.slice(1)
      : causes;
  const incompatible = candidates.find((cause) => cause.code === "ERR_DLOPEN_FAILED");
  const failure =
    incompatible ||
    candidates.find((cause) => cause.code !== "MODULE_NOT_FOUND") ||
    candidates[candidates.length - 1];
  const missing = candidates.every((cause) => cause.code === "MODULE_NOT_FOUND");
  const message = incompatible
    ? "Jazz NAPI binary was found but could not be loaded. Check that the installed binary is compatible with this operating system, architecture, and system libraries. "
    : missing
      ? "Jazz NAPI artifact is missing. In this monorepo run pnpm --filter jazz-napi build:debug; for an installed package reinstall matching Jazz package versions. "
      : "Jazz NAPI artifact could not be loaded. ";
  const diagnostic = new Error(message + `(${failure?.message || error})`, { cause: error });
  if (failure?.code) diagnostic.code = failure.code;
  throw diagnostic;
}
