#!/usr/bin/env node
// Measures what Jazz adds to a web app's bundle, the way adopters see it: two
// minimal Next.js apps, identical except for <JazzProvider>, built against a
// published jazz-tools; the difference in gzipped JS the page loads before
// first render is the thin client's cost. Also records the Wasm engine's size.
//
// Usage: node dev/benchmarks/bundle-size/measure.mjs <jazz-tools version> <out.json>
import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

const [version, out] = process.argv.slice(2);
if (!version || !out) {
  console.error("usage: measure.mjs <jazz-tools version> <out.json>");
  process.exit(2);
}
const here = dirname(fileURLToPath(import.meta.url));
const docsPackage = JSON.parse(readFileSync(join(here, "../../../docs/package.json"), "utf8"));
const deps = { ...docsPackage.dependencies, ...docsPackage.devDependencies };
// The same Next.js and React the docs use, exactly ("catalog:" resolves
// through pnpm-workspace.yaml's default catalog).
const workspace = readFileSync(join(here, "../../../pnpm-workspace.yaml"), "utf8");
const pin = (name) => {
  const spec = deps[name];
  if (!spec.startsWith("catalog:")) return spec.replace(/^[\^~]/, "");
  const match = workspace.match(new RegExp(`^\\s+"?${name}"?:\\s*"?([^"\\s]+)"?\\s*$`, "m"));
  if (!match) throw new Error(`${name}: not in the pnpm catalog`);
  return match[1].replace(/^[\^~]/, "");
};
const nextVersion = pin("next");
const work = mkdtempSync(join(tmpdir(), "jazz-bundle-size-"));

function gz(buffer) {
  return gzipSync(buffer, { level: 9 }).length;
}

function buildFixture(name, extra) {
  const app = join(work, name);
  cpSync(join(here, "fixtures", name), app, { recursive: true });
  writeFileSync(
    join(app, "package.json"),
    JSON.stringify({ name: `bundle-size-${name}`, private: true }),
  );
  const packages = [
    `next@${nextVersion}`,
    `react@${pin("react")}`,
    `react-dom@${pin("react-dom")}`,
    ...extra,
  ];
  // Scripts stay off: nothing here needs a native build.
  execFileSync("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", ...packages], {
    cwd: app,
    stdio: "inherit",
  });
  execFileSync(join(app, "node_modules/.bin/next"), ["build"], {
    cwd: app,
    stdio: "inherit",
    env: { ...process.env, NEXT_TELEMETRY_DISABLED: "1" },
  });
  // Every script the prerendered page loads before first render.
  const html = readFileSync(join(app, ".next/server/app/index.html"), "utf8");
  const scripts = [
    ...new Set([...html.matchAll(/<script[^>]+src="\/_next\/([^"]+)"/g)].map((m) => m[1])),
  ];
  if (scripts.length === 0) throw new Error(`${name}: no scripts found in the prerendered page`);
  return {
    app,
    gzipBytes: scripts.reduce((sum, s) => sum + gz(readFileSync(join(app, ".next", s))), 0),
  };
}

const base = buildFixture("base", []);
const jazz = buildFixture("jazz", [`jazz-tools@${version}`]);
const installed = JSON.parse(
  readFileSync(join(jazz.app, "node_modules/jazz-tools/package.json"), "utf8"),
).version;
const wasmDir = join(jazz.app, "node_modules/jazz-wasm");
const wasmFile = readdirSync(wasmDir, { recursive: true }).find((f) => String(f).endsWith(".wasm"));
if (!wasmFile) throw new Error("jazz-wasm ships no .wasm file");
const wasm = readFileSync(join(wasmDir, String(wasmFile)));

const result = {
  jazzToolsVersion: installed,
  nextVersion,
  measuredAt: new Date().toISOString(),
  thinClientAddedGzipBytes: jazz.gzipBytes - base.gzipBytes,
  wasmGzipBytes: gz(wasm),
  wasmRawBytes: wasm.length,
};
writeFileSync(out, `${JSON.stringify(result, null, 2)}\n`);
console.log(result);
