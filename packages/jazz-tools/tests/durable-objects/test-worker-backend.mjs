// Real Worker entry -> packaged DO host, without NAPI or a local replica in the Worker.
import assert from "node:assert/strict";
import { Miniflare } from "miniflare";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { builtinModules, createRequire } from "node:module";
import { tsImport } from "tsx/esm/api";
import { createServerHarness } from "./server-runtime.mjs";
const require = createRequire(new URL("../../package.json", import.meta.url));
const { build } = require("esbuild");
const wire = await tsImport("../../src/runtime/native-runtime/websocket.ts", import.meta.url);
const dir = await mkdtemp(join(tmpdir(), "jazz-worker-backend-"));
const appId = randomUUID(),
  secret = "worker-backend-fixture";
process.env.JAZZ_DO_PACKAGED = "1";
const core = createServerHarness(appId, secret);
let worker;
const deadline = setTimeout(() => {
  console.error("Worker backend test timed out");
  process.exit(1);
}, 60000);
try {
  const scriptPath = join(dir, "worker.mjs");
  const built = await build({
    entryPoints: [fileURLToPath(new URL("./worker-backend.fixture.ts", import.meta.url))],
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
          /jazz-napi|wasm-loader|@vercel|node_modules\/ws\/|runtime\/node-(host|socket|artifact)|durable-object-worker\/artifact/,
        );
    for (const item of output.imports)
      if (item.kind !== "dynamic-import") assert.equal(item.path, "node:crypto");
  }
  assert.doesNotMatch(
    await readFile(scriptPath, "utf8"),
    /class NativeRuntimeAdapter|NativeRuntimeAdapter = class/,
  );
  const url = String(await core.ready).replace(/\/$/, "");
  worker = new Miniflare({
    modulesRoot: dir,
    modules: true,
    scriptPath,
    compatibilityDate: "2026-07-01",
    compatibilityFlags: ["nodejs_compat"],
    bindings: { CORE_URL: url, APP_ID: appId, SECRET: secret },
  });
  async function request(path = "/", init = {}) {
    const response = await worker.dispatchFetch(`https://backend.example${path}`, init);
    assert.equal(response.status, 200, await response.clone().text());
    return response.json();
  }
  await Promise.all(
    ["first", "second", "third", "fourth"].map((body) => request("/", { method: "POST", body })),
  );
  assert.deepEqual((await request()).map((row) => row.title).sort(), [
    "first",
    "fourth",
    "second",
    "third",
  ]);
  const headers = { "x-test-user": "alice" };
  const account = await request(`/jazz/apps/${appId}/accounts/backend-sign-in`, {
    method: "POST",
    headers,
  });
  assert.ok(account.account);
  assert.deepEqual(await request("/scoped", { headers }), []);
  await request("/attributed", { method: "POST", headers, body: "attributed" });
  assert.equal((await request()).length, 5);
  const forbidden = await worker.dispatchFetch(
    `https://backend.example/jazz/apps/${appId}/accounts/backend-user`,
    { method: "POST" },
  );
  assert.equal(forbidden.status, 401);
  const fallback = await worker.dispatchFetch(`https://backend.example/jazz/apps/${appId}/ws`, {
    method: "POST",
  });
  assert.equal(fallback.status, 426);
  assert.equal(fallback.headers.get("x-jazz-stream"), null);
  const stream = await worker.dispatchFetch(`https://backend.example/jazz/apps/${appId}/stream`, {
    method: "POST",
  });
  assert.equal(stream.status, 501);
  for (const user of ["alice", "bob"]) {
    const response = await worker.dispatchFetch(`https://backend.example/jazz/apps/${appId}/ws`, {
      headers: { Upgrade: "websocket", "x-test-user": user },
    });
    assert.equal(response.status, 101);
    const socket = response.webSocket;
    socket.accept();
    const answer = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("socket handshake timed out")), 5000);
      if (user === "alice")
        socket.addEventListener(
          "message",
          () => {
            clearTimeout(timer);
            resolve("message");
          },
          { once: true },
        );
      socket.addEventListener(
        "close",
        (e) => {
          clearTimeout(timer);
          resolve(e.code);
        },
        { once: true },
      );
    });
    socket.send(
      JSON.stringify({
        peer_identity: "worker",
        backend_session: { issuer: "jazz:backend", user_id: "alice" },
        auth: { backend_session: { user_id: "alice" } },
      }),
    );
    socket.send(wire.encodeWebSocketFrameBatch([wire.encodeWireClientHello()]));
    assert.equal(await answer, user === "alice" ? "message" : 1008);
    socket.close();
  }
  console.log(
    "Worker backend: concurrent writes, fresh reads, scoped permissions, attribution, HTTP authentication and WebSocket admission passed",
  );
} finally {
  clearTimeout(deadline);
  await worker?.dispose();
  await core.dispose();
  await rm(dir, { recursive: true, force: true });
}
