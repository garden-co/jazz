// A Vite production build of a local-first app ships the broker worker once:
// the worker Vite builds from `new SharedWorker(new URL(...))`. Jazz used to
// name the published worker with a second `new URL(literal, import.meta.url)`,
// and Vite copied the whole unminified file into every app's build output as
// an asset that no page ever loaded (174 KB gzip).
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const packageDir =
  process.env.JAZZ_VITE_CONSUMER_DIR ?? fileURLToPath(new URL("..", import.meta.url));
const require = createRequire(join(packageDir, "package.json"));

/** A project that depends on jazz-tools the way an npm install lays it out. */
function createConsumerProject() {
  const root = mkdtempSync(join(tmpdir(), "jazz-vite-build-"));
  writeFileSync(
    join(root, "package.json"),
    JSON.stringify({ name: "vite-build-consumer", private: true, type: "module" }),
  );
  writeFileSync(
    join(root, "index.html"),
    '<!doctype html><html><body><script type="module" src="/src/main.js"></script></body></html>',
  );
  mkdirSync(join(root, "src"));
  writeFileSync(
    join(root, "src/main.js"),
    `import * as localFirst from "jazz-tools/local-first";
import { createJazzClient } from "jazz-tools/client";
window.__jazz = createJazzClient({ serverUrl: null, client: async () => localFirst });
`,
  );
  const modules = join(root, "node_modules");
  const installed = join(modules, "jazz-tools");
  mkdirSync(installed, { recursive: true });
  cpSync(join(packageDir, "package.json"), join(installed, "package.json"));
  cpSync(join(packageDir, "dist"), join(installed, "dist"), { recursive: true });
  const ownModules = join(packageDir, "node_modules");
  for (const entry of readdirSync(ownModules)) {
    if (entry === ".bin" || entry === "jazz-tools" || entry.startsWith(".")) continue;
    if (!existsSync(join(modules, entry)))
      symlinkSync(join(ownModules, entry), join(modules, entry));
  }
  return root;
}

function filesUnder(dir) {
  return readdirSync(dir, { withFileTypes: true, recursive: true })
    .filter((entry) => entry.isFile())
    .map((entry) => join(entry.parentPath, entry.name));
}

test("vite build emits the broker worker once", async (t) => {
  const { build } = await import(pathToFileURL(require.resolve("vite")).href);
  const root = createConsumerProject();
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await build({ root, configFile: false, logLevel: "silent", build: { outDir: "dist" } });

  const workers = filesUnder(join(root, "dist")).filter((path) =>
    /jazz-broker-worker[^/]*\.js$/.test(path),
  );
  assert.equal(workers.length, 1, `broker worker files in the build: ${workers.join(", ")}`);
  // The one that remains is the worker Vite built: it runs the broker.
  assert.match(readFileSync(workers[0], "utf8"), /onconnect/);
});
