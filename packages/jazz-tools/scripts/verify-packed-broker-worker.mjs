// Verify that a packed jazz-tools package ships a usable broker SharedWorker.
//
// The broker bundle must be one self-contained file with the jazz-broker-wasm
// bytes embedded base64 at build time (that package is private and never
// published). A packed tarball without the embedded bytes would break every
// persistent-mode browser consumer at runtime.
import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

const packageDirArg = process.argv[2];

if (!packageDirArg) {
  fail("Usage: node scripts/verify-packed-broker-worker.mjs <packed-package-dir>");
}

const packageDir = resolve(packageDirArg);
const brokerWorkerPath = join(packageDir, "dist", "worker", "jazz-broker-worker.js");

if (!existsSync(brokerWorkerPath)) {
  fail(`Packed broker worker bundle missing: ${brokerWorkerPath}`);
}

const brokerWorkerSource = readFileSync(brokerWorkerPath, "utf8");
const embeddedWasm = brokerWorkerSource.match(/JAZZ_BROKER_WASM_BASE64\s*=\s*"([^"]*)"/);

if (!embeddedWasm) {
  fail(
    "Packed broker worker bundle does not embed the jazz-broker-wasm bytes; " +
      "was jazz-broker-wasm built before bundling?",
  );
}

// Validate the payload, not just its presence: an empty or corrupted constant
// would still match the regex above. Real wasm starts with the `\0asm` magic.
const wasmBytes = Buffer.from(embeddedWasm[1], "base64");

if (wasmBytes.length < 10_000 || wasmBytes.readUInt32LE(0) !== 0x6d73_6100) {
  fail("Packed broker worker bundle embeds invalid or truncated wasm bytes.");
}

if (/from\s*["']jazz-broker-wasm["']/.test(brokerWorkerSource)) {
  fail("Packed broker worker bundle still contains a bare jazz-broker-wasm import.");
}

console.log(
  `Packed broker worker bundle OK (${Math.round(wasmBytes.length / 1024)} KB wasm embedded).`,
);
