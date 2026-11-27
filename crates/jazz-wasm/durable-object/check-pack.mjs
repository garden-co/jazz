// Fail unless `npm pack` puts the Durable Object entry and its build in the
// jazz-wasm tarball: run after `build:durable-object`. wasm-pack writes a
// `.gitignore` of `*` into its output directory, which npm honours inside
// directories the `files` field names; `build:durable-object` deletes it,
// and this check keeps it that way.
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
  "durable-object/index.js",
  "durable-object/index.d.ts",
  "durable-object/host.js",
  "durable-object/blob-stores.js",
  "durable-object/blob-stores.d.ts",
  "durable-object/pkg/jazz_wasm_do.js",
  "durable-object/pkg/jazz_wasm_do_bg.wasm",
];
const missing = required.filter((file) => !files.has(file));
if (missing.length > 0) {
  console.error(`the jazz-wasm tarball lacks: ${missing.join(", ")}`);
  process.exit(1);
}
if (![...files].some((file) => file.startsWith("durable-object/pkg/snippets/"))) {
  console.error("the jazz-wasm tarball lacks durable-object/pkg/snippets/");
  process.exit(1);
}
const glue = execFileSync(
  "grep",
  ["-c", "__jazzReinstantiate", `${root}/durable-object/pkg/jazz_wasm_do.js`],
  {
    encoding: "utf8",
  },
);
if (Number(glue.trim()) === 0) {
  console.error("durable-object/pkg/jazz_wasm_do.js was not patched by reinstantiable.mjs");
  process.exit(1);
}
// A test build (`durable-object-tests`: a panic route, held-limit
// overrides) must never be packed.
const wasm = readFileSync(`${root}/durable-object/pkg/jazz_wasm_do_bg.wasm`);
for (const marker of ["test/panic", "test/limits", "a test asked the core to panic"]) {
  if (wasm.includes(marker)) {
    console.error(
      `durable-object/pkg is a test build (its wasm carries "${marker}"): run build:durable-object`,
    );
    process.exit(1);
  }
}
console.log(`jazz-wasm tarball: ${required.join(", ")} and snippets present`);
