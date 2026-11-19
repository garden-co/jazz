// Fail unless `npm pack` puts the backend entry and its build in the
// jazz-wasm tarball: run after `build:backend`. wasm-pack writes a
// `.gitignore` of `*` into its output directory, which npm honours inside
// directories the `files` field names; `build:backend` deletes it, and this
// check keeps it that way.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const [packed] = JSON.parse(
  execFileSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], {
    cwd: root,
    encoding: "utf8",
  }),
);
const files = new Set(packed.files.map((file) => file.path));
const required = [
  "backend/index.js",
  "backend/index.d.ts",
  "backend/pkg/jazz_wasm_backend.js",
  "backend/pkg/jazz_wasm_backend.d.ts",
  "backend/pkg/jazz_wasm_backend_bg.wasm",
];
const missing = required.filter((file) => !files.has(file));
if (missing.length > 0) {
  console.error(`the jazz-wasm tarball lacks: ${missing.join(", ")}`);
  process.exit(1);
}
// It is the backend build: what the browser build leaves out is in it.
const glue = readFileSync(`${root}/backend/pkg/jazz_wasm_backend.js`, "utf8");
for (const name of ["serveThinTransaction", "WasmThinRouter", "loadBackendApp"]) {
  if (!glue.includes(name)) {
    console.error(`backend/pkg/jazz_wasm_backend.js lacks ${name}: is it a backend build?`);
    process.exit(1);
  }
}
console.log(`jazz-wasm tarball: ${required.join(", ")} present`);
