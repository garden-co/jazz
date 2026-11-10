// The public Worker provider provisions through a local Cloudflare API double,
// then reaches the real packaged DO authority. Never contacts Cloudflare.
import assert from "node:assert/strict";
import { Miniflare, Response as WorkerResponse } from "miniflare";
import { createHash } from "node:crypto";
import { builtinModules, createRequire } from "node:module";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { tsImport } from "tsx/esm/api";
import { createServerHarness } from "./server-runtime.mjs";
const require = createRequire(new URL("../../package.json", import.meta.url));
const { build } = require("esbuild");
const { accountAppId } = await tsImport("../../src/accounts/local-first.ts", import.meta.url);
const account = "a".repeat(32),
  name = "worker-provider-test",
  secret = "worker-core-secret-".repeat(3);
const appId = accountAppId(`jazz-durable-objects-v1:${account}:${name}`);
const dir = await mkdtemp(join(tmpdir(), "jazz-worker-provider-"));
process.env.JAZZ_DO_PACKAGED = "1";
const core = createServerHarness(appId, secret);
const deadline = setTimeout(() => {
  console.error("Worker provider test timed out");
  process.exit(1);
}, 60000);
let worker,
  settings,
  enabled = false,
  uploads = 0,
  apiCalls = 0,
  upgrades = 0;
const manifest = JSON.parse(
  await readFile(new URL("../../durable-object-worker/manifest.json", import.meta.url), "utf8"),
);
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
async function outbound(request) {
  const url = new URL(request.url);
  if (url.hostname === `${name}.example.workers.dev`) {
    assert.equal(request.headers.get("authorization"), null, "API token must not reach core");
    assert.equal(request.headers.get("x-jazz-backend-credential"), secret);
    if (url.pathname.endsWith("/admin/host"))
      return WorkerResponse.json(settings.bindings.find((b) => b.name === "JAZZ_HOST").json);
    if (request.headers.get("upgrade") === "websocket") upgrades++;
    const response = await core.dispatchFetch(request);
    // Miniflare's Node WebSocket bridge negotiates compression on its own
    // hop; that extension was not negotiated by the application Worker.
    response.headers.delete("sec-websocket-extensions");
    return response;
  }
  assert.equal(url.hostname, "api.cloudflare.com", "unexpected network destination");
  assert.equal(request.headers.get("authorization"), "Bearer local-api-token");
  apiCalls++;
  const result = (value) => WorkerResponse.json({ success: true, result: value });
  if (url.pathname.endsWith("/workers/subdomain")) return result({ subdomain: "example" });
  if (url.pathname.endsWith("/settings"))
    return settings ? result(settings) : new WorkerResponse(null, { status: 404 });
  if (url.pathname.endsWith("/subdomain")) {
    if (request.method === "POST") enabled = true;
    return result({ enabled });
  }
  assert.equal(request.method, "PUT");
  assert.equal(url.pathname, `/client/v4/accounts/${account}/workers/scripts/${name}`);
  const form = await request.formData();
  const metadata = JSON.parse(await form.get("metadata").text());
  assert.equal(
    hash(new Uint8Array(await form.get("worker.mjs").arrayBuffer())),
    manifest.workerSha256,
  );
  assert.equal(
    hash(new Uint8Array(await form.get("jazz.wasm").arrayBuffer())),
    manifest.wasmSha256,
  );
  assert.deepEqual(metadata.migrations.steps, [{ new_sqlite_classes: ["JazzObject"] }]);
  assert.equal(metadata.bindings.find((b) => b.name === "JAZZ_ADMIN_SECRET").text, secret);
  settings = {
    bindings: metadata.bindings.map((b) =>
      b.name === "JAZZ" ? { ...b, namespace_id: "local-namespace" } : b,
    ),
  };
  uploads++;
  return result({});
}
try {
  await core.ready;
  const scriptPath = join(dir, "worker.mjs");
  const built = await build({
    entryPoints: [fileURLToPath(new URL("./worker-provider.fixture.ts", import.meta.url))],
    bundle: true,
    platform: "neutral",
    mainFields: ["browser", "module", "main"],
    conditions: ["workerd", "worker", "browser", "import"],
    format: "esm",
    outfile: scriptPath,
    external: ["node:*", ...builtinModules],
    metafile: true,
  });
  for (const output of Object.values(built.metafile.outputs)) {
    for (const [path, input] of Object.entries(output.inputs))
      if (input.bytesInOutput)
        assert.doesNotMatch(
          path,
          /jazz-napi|wasm-loader|@vercel|node_modules\/ws\/|runtime\/node-(host|socket|artifact)/,
        );
    for (const item of output.imports)
      if (item.kind !== "dynamic-import")
        assert.ok(["node:crypto", "node:buffer"].includes(item.path), item.path);
  }
  worker = new Miniflare({
    modulesRoot: dir,
    modules: true,
    scriptPath,
    compatibilityDate: "2026-07-01",
    compatibilityFlags: ["nodejs_compat"],
    bindings: { ACCOUNT: account, TOKEN: "local-api-token", SECRET: secret },
    outboundService: outbound,
  });
  async function request(init) {
    const response = await worker.dispatchFetch("https://app.example/", init);
    assert.equal(response.status, 200, await response.clone().text());
    return response.json();
  }
  assert.deepEqual(await request({ method: "POST", body: "first invocation" }), {
    appId,
    titles: ["first invocation"],
  });
  assert.equal(uploads, 1);
  const firstCalls = apiCalls;
  assert.deepEqual(await request(), { appId, titles: ["first invocation"] });
  assert.equal(uploads, 1, "another invocation must reuse the deployed authority");
  assert.ok(apiCalls > firstCalls, "reuse checks deployed settings");
  assert.ok(upgrades >= 2, "each invocation opens its own Worker WebSocket");
  console.log(
    "Worker provider uploads exact host bytes, reuses namespace and reads/writes over Worker sockets.",
  );
} finally {
  clearTimeout(deadline);
  await worker?.dispose();
  await core.dispose();
  await rm(dir, { recursive: true, force: true });
}
