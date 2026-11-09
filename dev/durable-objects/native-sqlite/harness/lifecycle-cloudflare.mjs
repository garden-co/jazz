// Opt-in disposable deployment for test-lifecycle.mjs. Never reuse an existing
// Worker: cleanup deletes the test authority and its SQLite namespace.
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { tsImport } from "tsx/esm/api";
const { openDurableObjectsCore } = await tsImport(
  "../../../../packages/jazz-tools/src/backend/core-providers/durable-objects.ts",
  import.meta.url,
);
export async function cloudflareLifecycleHost() {
  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = process.env.CLOUDFLARE_API_TOKEN;
  if (!accountId || !apiToken)
    throw new Error("Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN");
  const name = `jazz-do-lifecycle-${randomBytes(6).toString("hex")}`;
  const options = {
    name,
    credentials: { accountId, apiToken },
    secret: randomBytes(32).toString("hex"),
  };
  const base = `https://api.cloudflare.com/client/v4/accounts/${accountId}`;
  const script = `/workers/scripts/${name}`;
  async function api(path, method = "GET") {
    return fetch(base + path, {
      method,
      headers: { Authorization: `Bearer ${apiToken}` },
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
    });
  }
  assert.equal((await api(`${script}/settings`)).status, 404, "test Worker must not already exist");
  const dir = new URL("../../../../packages/jazz-tools/durable-object-worker/", import.meta.url);
  let artifact = {
    manifest: JSON.parse(await readFile(new URL("manifest.json", dir), "utf8")),
    worker: await readFile(new URL("worker.mjs", dir)),
    wasm: await readFile(new URL("jazz.wasm", dir)),
  };
  const originalWorker = artifact.worker;
  const initialVersion = /^(\d+\.\d+\.\d+-alpha\.)(\d+)$/.exec(artifact.manifest.version);
  assert.ok(initialVersion, "upgrade fixture expects an alpha release");
  const hash = (value) => createHash("sha256").update(value).digest("hex");
  const provider = {
    open: () => openDurableObjectsCore(options, { loadArtifact: async () => artifact }),
  };
  let generation = 0;
  let identity;
  async function deploymentIdentity() {
    const response = await api(`${script}/settings`);
    assert.equal(response.status, 200);
    const { result } = await response.json();
    const binding = (key) => result.bindings.find((b) => b.name === key);
    return { appId: binding("JAZZ_APP_ID").text, namespace: binding("JAZZ").namespace_id };
  }
  async function cleanup() {
    const response = await api(script, "DELETE");
    assert.ok(
      response.ok || response.status === 404,
      `cleanup failed for ${name}: HTTP ${response.status}`,
    );
    assert.equal((await api(`${script}/settings`)).status, 404);
    // Use the documented paginated namespace list, not an unsupported GET-by-id.
    for (let page = 1; ; page++) {
      const namespaces = await api(`/workers/durable_objects/namespaces?page=${page}&per_page=100`);
      assert.equal(namespaces.status, 200);
      const { result, result_info } = await namespaces.json();
      assert.ok(
        !result.some((ns) => ns.script === name || ns.id === identity?.namespace),
        "deleted Worker must not retain its test namespace",
      );
      if (page >= (result_info?.total_pages ?? 1)) break;
    }
    console.log(`Deleted temporary Cloudflare Worker ${name}.`);
  }
  try {
    console.log(`Provisioning temporary Cloudflare Worker ${name}.`);
    const core = await provider.open();
    identity = await deploymentIdentity();
    assert.equal(core.appId, identity.appId);
    await core.close();
    return {
      provider,
      async restart() {
        generation++;
        const worker = Buffer.concat([
          originalWorker,
          Buffer.from(`\n// Lifecycle fixture upgrade ${generation}.\n`),
        ]);
        const manifest = {
          ...artifact.manifest,
          version: initialVersion[1] + (Number(initialVersion[2]) + generation),
          workerSha256: hash(worker),
        };
        manifest.digest = hash(
          `${manifest.format}\0${manifest.version}\0${manifest.workerSha256}\0${manifest.wasmSha256}`,
        );
        artifact = { ...artifact, manifest, worker };
        const upgraded = await provider.open();
        assert.equal(upgraded.appId, identity.appId);
        assert.deepEqual(await deploymentIdentity(), identity);
        await upgraded.close();
        console.log(`Cloudflare host upgrade ${generation} ready; app and namespace retained.`);
      },
      cleanup,
    };
  } catch (error) {
    await cleanup();
    throw error;
  }
}
