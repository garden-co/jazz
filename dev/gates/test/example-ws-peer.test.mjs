import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

// An example whose tests run an app backend (`startTestJazz`, or the browser
// harness's `jazzAppFront`, which starts one) reaches `attach()`, which loads
// the optional `ws` peer. Under a strict install an example that doesn't
// declare `ws` fails at setup: "attach() needs the ws package" (#5396).

const root = path.resolve(import.meta.dirname, "../../..");
const SKIP = new Set(["node_modules", ".next", "dist", ".jazz", "test-results"]);
const USES_APP_BACKEND = /\b(?:startTestJazz|jazzAppFront)\b/;

function walk(dir, visit) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (SKIP.has(entry.name)) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full, visit);
    else visit(full);
  }
}

/** Example package dirs, each with the source files that belong to it. */
function examplePackages() {
  const packages = new Map();
  walk(path.join(root, "examples"), (file) => {
    if (path.basename(file) === "package.json") packages.set(path.dirname(file), []);
  });
  const dirs = [...packages.keys()].sort((a, b) => b.length - a.length);
  walk(path.join(root, "examples"), (file) => {
    if (!/\.(?:[cm]?[jt]sx?)$/.test(file)) return;
    const owner = dirs.find((dir) => file.startsWith(dir + path.sep));
    if (owner) packages.get(owner).push(file);
  });
  return packages;
}

test("examples that run an app backend in their tests declare ws", () => {
  const missing = [];
  for (const [dir, files] of examplePackages()) {
    const usesBackend = files.some((file) => USES_APP_BACKEND.test(fs.readFileSync(file, "utf8")));
    if (!usesBackend) continue;
    const manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
    const declared = { ...manifest.dependencies, ...manifest.devDependencies };
    if (!declared.ws) missing.push(path.relative(root, dir));
  }
  assert.deepEqual(missing, [], `add "ws" to these examples' devDependencies`);
});
