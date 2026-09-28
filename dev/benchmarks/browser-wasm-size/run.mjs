#!/usr/bin/env node
// Runs the actual generated release package in a fresh Chromium process per sample.
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../..");
const require = createRequire(join(root, "package.json"));
const { chromium } = require("playwright");
const packageRequire = createRequire(join(root, "packages/jazz-tools/package.json"));
const { build } = packageRequire("esbuild");
const artifacts = new Map();
let order;
let rows = 2000;
let output;
for (let i = 2; i < process.argv.length; i += 2) {
  const [option, value] = process.argv.slice(i, i + 2);
  if (!value) throw new Error(`Missing value for ${option}`);
  if (option === "--artifact") {
    const separator = value.indexOf("=");
    const name = value.slice(0, separator);
    if (separator < 1 || !/^[a-zA-Z0-9_-]+$/.test(name) || artifacts.has(name))
      throw new Error("--artifact requires a unique label=/absolute/package/directory");
    artifacts.set(name, resolve(value.slice(separator + 1)));
  } else if (option === "--order") order = value.split(",");
  else if (option === "--rows") rows = Number(value);
  else if (option === "--out") output = resolve(value);
  else throw new Error(`Unknown option ${option}`);
}
if (!artifacts.size || !output)
  throw new Error(
    "Usage: run.mjs --artifact label=DIR [--artifact label=DIR] --out receipt.json [--order label,label] [--rows 2000]",
  );
if (!Number.isSafeInteger(rows) || rows < 200)
  throw new Error("--rows must be an integer of at least 200");
order ??= [...artifacts.keys()];
if (order.some((label) => !artifacts.has(label))) throw new Error("Unknown label in --order");
mkdirSync(dirname(output), { recursive: true });
const bundle = await build({
  entryPoints: [join(here, "workload.ts")],
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  target: "es2022",
});
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const files = new Map([
  [
    "/",
    [
      "text/html",
      Buffer.from(
        '<!doctype html><script type="module">import {runBundleWorkload} from "/workload.js"; globalThis.runBundleWorkload=runBundleWorkload;</script>',
      ),
    ],
  ],
  ["/workload.js", ["application/javascript", Buffer.from(bundle.outputFiles[0].contents)]],
]);
const provenance = {};
for (const [name, directory] of artifacts) {
  const hashes = {};
  for (const file of ["jazz_wasm.js", "jazz_wasm_bg.wasm"]) {
    const bytes = readFileSync(join(directory, file));
    hashes[file] = sha256(bytes);
    files.set(`/${name}/${file}`, [
      file.endsWith(".wasm") ? "application/wasm" : "application/javascript",
      bytes,
    ]);
  }
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(join(directory, ".jazz-artifact-manifest.json"), "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  if (manifest) {
    for (const [file, hash] of Object.entries(hashes)) {
      if (manifest.artifacts.find((entry) => entry.file === file)?.sha256 !== hash)
        throw new Error(`${name}: ${file} differs from its producer manifest`);
    }
  }
  provenance[name] = { directory, hashes, manifest: manifest ?? null };
}
const server = createServer((request, response) => {
  const item = files.get(request.url?.split("?")[0]);
  if (!item) {
    response.writeHead(404);
    response.end();
    return;
  }
  response.writeHead(200, { "Content-Type": item[0], "Cache-Control": "no-store" });
  response.end(item[1]);
});
await new Promise((ready) => server.listen(0, "127.0.0.1", ready));
const url = `http://127.0.0.1:${server.address().port}`;
const samples = [];
let expectedSignatures;
try {
  for (const label of order) {
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage();
      page.on("pageerror", (error) => console.error(error));
      await page.goto(url);
      await page.waitForFunction(
        () => typeof globalThis.runBundleWorkload === "function",
        undefined,
        { timeout: 10000 },
      );
      const result = await page.evaluate(
        async (args) => {
          let timeout;
          try {
            return await Promise.race([
              globalThis.runBundleWorkload(args),
              new Promise((_, reject) => {
                timeout = setTimeout(
                  () => reject(new Error("WASM size workload exceeded 60 seconds")),
                  60000,
                );
              }),
            ]);
          } finally {
            clearTimeout(timeout);
          }
        },
        { artifact: label, rowCount: rows },
      );
      const manifest = provenance[label].manifest;
      if (manifest && result.artifact_fingerprint !== manifest.nativeArtifactFingerprint)
        throw new Error(`${label}: runtime fingerprint differs from its producer manifest`);
      for (const scenario of result.scenarios) {
        scenario.signature_sha256 = sha256(scenario.signature);
        delete scenario.signature;
      }
      const signatures = result.scenarios.map((scenario) => scenario.signature_sha256);
      expectedSignatures ??= signatures;
      if (JSON.stringify(signatures) !== JSON.stringify(expectedSignatures))
        throw new Error(`${label}: row output differs from the first sample`);
      samples.push({ label, browser: browser.version(), ...result });
      writeFileSync(
        output,
        `${JSON.stringify({ format: 1, rows, order, provenance, samples }, null, 2)}\n`,
      );
      console.log(JSON.stringify(samples.at(-1)));
    } finally {
      await browser.close();
    }
  }
} finally {
  server.close();
}
