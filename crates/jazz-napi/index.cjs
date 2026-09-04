// This tracked package entrypoint deliberately runs before napi-rs's ignored
// generated loader. It gives a fresh checkout the same deterministic failure
// as a rebuilt checkout.
const { nativeBinding, expectedNativeArtifactFingerprint } = require("./native-binding.cjs");
const { completePollableClose } = require("./close-pollable.cjs");

if (typeof nativeBinding.nativeArtifactFingerprint !== "function") {
  throw new Error(
    "Jazz NAPI artifact is stale or incomplete: missing nativeArtifactFingerprint. " +
      "In this monorepo run pnpm --filter jazz-napi build:debug; for an installed package reinstall matching Jazz package versions.",
  );
}
const actual = nativeBinding.nativeArtifactFingerprint();
if (actual !== expectedNativeArtifactFingerprint) {
  throw new Error(
    `Jazz NAPI artifact ABI mismatch: expected ${expectedNativeArtifactFingerprint}, got ${String(actual)}. ` +
      "In this monorepo rebuild with pnpm --filter jazz-napi build:debug; for an installed package reinstall matching Jazz package versions.",
  );
}
if (
  typeof nativeBinding.NapiDb !== "function" ||
  typeof nativeBinding.NapiDb.prototype.tick !== "function"
) {
  throw new Error(
    "Jazz NAPI artifact is incomplete despite a matching fingerprint (missing NapiDb.tick). " +
      "Rebuild the monorepo binding or reinstall matching package versions.",
  );
}

const closePollable = nativeBinding.NapiDb.prototype.__closePollable;
if (typeof closePollable !== "function") {
  throw new Error(
    "Jazz NAPI artifact is incomplete despite a matching fingerprint (missing NapiDb close operation). " +
      "Rebuild the monorepo binding or reinstall matching package versions.",
  );
}
Object.defineProperty(nativeBinding.NapiDb.prototype, "close", {
  configurable: true,
  writable: true,
  value: async function close() {
    const pending = closePollable.call(this);
    if (pending instanceof Uint8Array) return undefined;
    await completePollableClose(pending);
  },
});
module.exports = nativeBinding;
