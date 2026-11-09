// Explicit opt-in deployment test. Uses only synthetic data and a dedicated Worker.
// Credentials come from the environment; the state file must live outside the repo.
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { resolve, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { tsImport } from "tsx/esm/api";
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const { createJazz, durableObjects } = await tsImport(
  "../../../../packages/jazz-tools/src/backend/index.ts",
  import.meta.url,
);
const { openDurableObjectsCore } = await tsImport(
  "../../../../packages/jazz-tools/src/backend/core-providers/durable-objects.ts",
  import.meta.url,
);
const { schema: s } = await tsImport(
  "../../../../packages/jazz-tools/src/index.ts",
  import.meta.url,
);
const name = "jazz-do-provisioning-test";
const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
const apiToken = process.env.CLOUDFLARE_API_TOKEN;
const statePath = process.env.JAZZ_DO_TEST_STATE;
if (!accountId || !apiToken || !statePath)
  throw new Error("Set CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN and JAZZ_DO_TEST_STATE");
if (!relative(root, resolve(statePath)).startsWith(".."))
  throw new Error("Keep the secret-bearing test state outside the repository");
let state;
try {
  state = JSON.parse(await readFile(statePath, "utf8"));
} catch (error) {
  if (error.code !== "ENOENT") throw error;
  state = { accountId, name, secret: randomBytes(32).toString("hex") };
  await writeFile(statePath, JSON.stringify(state), { mode: 0o600, flag: "wx" });
}
assert.equal(state.accountId, accountId);
assert.equal(state.name, name);
const options = { name, credentials: { accountId, apiToken }, secret: state.secret };
async function deploymentIdentity() {
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${name}/settings`,
    {
      headers: { Authorization: `Bearer ${apiToken}` },
      redirect: "error",
    },
  );
  assert.equal(response.status, 200);
  const { result } = await response.json();
  const binding = (name) => result.bindings.find((b) => b.name === name);
  assert.equal(binding("JAZZ_ADMIN_SECRET").type, "secret_text");
  return { namespace: binding("JAZZ").namespace_id, appId: binding("JAZZ_APP_ID").text };
}

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
const save = () => writeFile(statePath, JSON.stringify(state), { mode: 0o600 });
const deadline = setTimeout(() => {
  console.error("Cloudflare integration timed out");
  process.exit(1);
}, 180_000);
let jazz;
try {
  if (process.argv.includes("--upgrade")) {
    const dir = resolve(root, "packages/jazz-tools/durable-object-worker");
    const manifest = JSON.parse(await readFile(resolve(dir, "manifest.json"), "utf8"));
    const original = {
      manifest,
      worker: await readFile(resolve(dir, "worker.mjs")),
      wasm: await readFile(resolve(dir, "jazz.wasm")),
    };
    // Synthetic next release is confined to this test authority, never packaged/published.
    const match = /^(\d+\.\d+\.\d+-alpha\.)(\d+)$/.exec(manifest.version);
    assert.ok(match, "upgrade fixture expects an alpha release");
    const worker = Buffer.concat([
      original.worker,
      Buffer.from("\n// Deployment upgrade integration fixture.\n"),
    ]);
    const next = {
      ...manifest,
      version: match[1] + (Number(match[2]) + 1),
      workerSha256: createHash("sha256").update(worker).digest("hex"),
    };
    next.digest = createHash("sha256")
      .update(`${next.format}\0${next.version}\0${next.workerSha256}\0${next.wasmSha256}`)
      .digest("hex");
    const artifact = { manifest: next, worker, wasm: original.wasm };
    const provider = {
      open: () => openDurableObjectsCore(options, { loadArtifact: async () => artifact }),
    };
    const before = await deploymentIdentity();
    assert.deepEqual(before, state.identity);
    const core = await provider.open();
    assert.deepEqual(await deploymentIdentity(), before);
    assert.equal(core.appId, state.appId);
    await core.close();
    jazz = await createJazz({ app, permissions, store: provider }).ready();
    assert.deepEqual(
      (await jazz.db.all(app.notes.where({ id: state.rowId }))).map((r) => r.title),
      ["persisted deployment probe"],
    );
    await jazz.db.insert(app.notes, { title: "after host upgrade" }).wait({ tier: "global" });
    await assert.rejects(() => durableObjects(options).open(), /downgrade/);
    state.upgradeVersion = next.version;
    await save();
    console.log("Upgrade: retained app identity and rows; new writes succeed; downgrade refused.");
  } else {
    // Distinct opens exercise provisioning races independently of createJazz's process reuse.
    const cores = await Promise.all([
      durableObjects(options).open(),
      durableObjects(options).open(),
    ]);
    assert.equal(cores[0].appId, cores[1].appId);
    if (state.appId) assert.equal(cores[0].appId, state.appId);
    const identity = await deploymentIdentity();
    if (state.identity) assert.deepEqual(identity, state.identity);
    state.identity = identity;
    state.appId = cores[0].appId;
    await Promise.all(cores.map((c) => c.close()));
    jazz = await createJazz({ app, permissions, store: durableObjects(options) }).ready();
    if (state.rowId) {
      assert.deepEqual(
        (await jazz.db.all(app.notes.where({ id: state.rowId }))).map((r) => r.title),
        ["persisted deployment probe"],
      );
      console.log("Fresh backend: previously acknowledged row recovered.");
    } else {
      const row = await jazz.db
        .insert(app.notes, { title: "persisted deployment probe" })
        .wait({ tier: "global" });
      state.rowId = row.id;
      await save();
      console.log(
        "Provisioning: concurrent opens converged; createJazz ready; write globally acknowledged.",
      );
    }
    assert.deepEqual(
      (await jazz.db.all(app.notes.where({ id: state.rowId }))).map((r) => r.title),
      ["persisted deployment probe"],
    );
  }
} finally {
  clearTimeout(deadline);
  await jazz?.close();
}
