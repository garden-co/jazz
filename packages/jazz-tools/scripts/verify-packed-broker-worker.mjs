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

if (!embeddedWasm || embeddedWasm[1].length < 10_000) {
  fail(
    "Packed broker worker bundle does not embed the jazz-broker-wasm bytes; " +
      "was jazz-broker-wasm built before bundling?",
  );
}

if (/from\s*["']jazz-broker-wasm["']/.test(brokerWorkerSource)) {
  fail("Packed broker worker bundle still contains a bare jazz-broker-wasm import.");
}

console.log(
  `Packed broker worker bundle OK (${Math.round((embeddedWasm[1].length * 3) / 4 / 1024)} KB wasm embedded).`,
);
