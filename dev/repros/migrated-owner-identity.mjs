#!/usr/bin/env node
/**
 * Optimized browser correctness receipt for #3000. First run pnpm build:core.
 * Uses actual release WASM, SharedWorker, IndexedDB, and persistent native server.
 * No benchmark flags or correctness-artifact profile overrides are used.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID, createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const requireTools = createRequire(join(root, "packages/jazz-tools/package.json"));
const { createServer } = await import(requireTools.resolve("vite"));
const wasm = (await import(requireTools.resolve("vite-plugin-wasm"))).default;
const topLevelAwait = (await import(requireTools.resolve("vite-plugin-top-level-await"))).default;
const { startLocalJazzServer } = await import("../../packages/jazz-tools/dist/dev/dev-server.js");
const { deploy } = await import("../../packages/jazz-tools/dist/dev/catalogue.js");
const { oldApp, newApp, oldPermissions, newPermissions, migration } =
  await import("./migrated-owner-identity.schema.mjs");

function verify() {
  for (const kind of ["wasm", "napi"]) {
    execFileSync(process.execPath, ["dev/artifacts/provenance.mjs", "verify", kind, "release"], {
      cwd: root,
      stdio: "inherit",
    });
  }
}
async function receipt() {
  const manifest = JSON.parse(
    await readFile(join(root, "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json"), "utf8"),
  );
  return {
    source: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
    status: execFileSync("git", ["status", "--porcelain"], { cwd: root, encoding: "utf8" }),
    wasmProfile: manifest.profile,
    wasmFingerprint: manifest.nativeArtifactFingerprint,
    wasmSha256: createHash("sha256")
      .update(await readFile(join(root, "crates/jazz-wasm/pkg/jazz_wasm_bg.wasm")))
      .digest("hex"),
  };
}
verify();
const before = await receipt();
assert.equal(before.wasmProfile, "release");
const dataDir = await mkdtemp(join(tmpdir(), "jazz-migrated-owner-"));
const settings = {
  appId: randomUUID(),
  adminSecret: randomUUID(),
  backendSecret: randomUUID(),
  dataDir,
  allowLocalFirstAuth: true,
};
const server = await startLocalJazzServer({ ...settings, schema: oldApp });
const vite = await createServer({
  configFile: false,
  root,
  appType: "custom",
  plugins: [wasm(), topLevelAwait()],
  worker: { format: "es", plugins: () => [wasm(), topLevelAwait()] },
  server: { host: "127.0.0.1", port: 0, fs: { allow: [root] } },
});
vite.middlewares.use((request, response, next) => {
  if (request.url !== "/") return next();
  response.setHeader("Content-Type", "text/html");
  response.end(
    '<!doctype html><script type="module" src="/dev/repros/migrated-owner-identity.browser.mjs"></script>',
  );
});
await vite.listen();
const browser = await chromium.launch({ headless: true });
const errors = [];
const phases = [];
const cdp = await browser.newBrowserCDPSession();
await cdp.send("Target.setDiscoverTargets", { discover: true });
const sharedWorkers = new Set();
cdp.on("Target.targetCreated", ({ targetInfo }) => {
  if (targetInfo.type === "shared_worker") sharedWorkers.add(targetInfo.targetId);
});
const page = await browser.newPage();
page.setDefaultTimeout(30_000);
page.on("pageerror", (error) => errors.push(error.message));
page.on("console", (message) => {
  if (message.type() === "error") errors.push(message.text());
});
async function phase(name, operation) {
  console.log(`START ${name}`);
  const result = await Promise.race([
    operation(),
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(new Error(`${name} exceeded 30 seconds`)), 30_000);
      timer.unref();
    }),
  ]);
  phases.push({ name, result });
  console.log(`PASS ${name}`);
  return result;
}
let failure;
try {
  await deploy({ ...settings, serverUrl: server.url, schema: oldApp, permissions: oldPermissions });
  await page.goto(vite.resolvedUrls.local[0]);
  await page.waitForFunction(() => Boolean(window.migratedOwnerIdentity));
  await phase("open A persistent owner", () =>
    page.evaluate((config) => window.migratedOwnerIdentity.initialize(config), {
      appId: settings.appId,
      serverUrl: server.url,
      driver: { type: "persistent", dbName: `identity-${settings.appId}` },
    }),
  );
  await deploy({
    ...settings,
    serverUrl: server.url,
    schema: newApp,
    permissions: newPermissions,
    migration,
  });
  await phase("admit migration and persist B-authored row", () =>
    page.evaluate(() => window.migratedOwnerIdentity.admitMigration()),
  );
  const local = await phase("fresh A follower local read", () =>
    page.evaluate(() => window.migratedOwnerIdentity.reopenOldLocal()),
  );
  assert.equal(local.filter((row) => row.text === "synthetic B-authored retained row").length, 1);
  const global = await phase("A-only strict global read", () =>
    page.evaluate(() => window.migratedOwnerIdentity.readOldGlobal()),
  );
  assert.equal(global.filter((row) => row.text === "synthetic B-authored retained row").length, 1);
  assert.ok(sharedWorkers.size > 0, "the receipt must use a real SharedWorker");
  assert.deepEqual(errors, []);
} catch (error) {
  failure = error;
} finally {
  await page.evaluate(() => window.migratedOwnerIdentity?.close()).catch(() => {});
  await browser.close();
  await vite.close();
  await server.stop();
  verify();
  const after = await receipt();
  assert.deepEqual(after, before, "source and optimized binary changed during receipt");
  const result = {
    ok: !failure,
    before,
    after,
    sharedWorkerCount: sharedWorkers.size,
    phases,
    errors,
    failure: failure?.stack,
    retainedSyntheticStore: dataDir,
  };
  const output = join(root, "target/migrated-owner-identity-receipt.json");
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, `${JSON.stringify(result, null, 2)}\n`);
  console.log(`Receipt: ${output}`);
}
if (failure) throw failure;
