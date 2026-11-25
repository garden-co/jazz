// Run from the RC-installed app. Both cases use fresh Jazz clients for readback.
import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { setTimeout as sleep } from "node:timers/promises";
import { del, list } from "@vercel/blob";
import { Drive, Sandbox } from "@vercel/sandbox";
import { createJazz, files, remote, sqlite, vercelBlob } from "jazz-tools/backend";
import { app } from "../jazz.generated.ts";
import permissions from "../jazz.permissions.json" with { type: "json" };

const token = process.env.T4_BLOB_READ_WRITE_TOKEN;
assert.ok(token, "set T4_BLOB_READ_WRITE_TOKEN for a dedicated private Blob store");
const name = `jazz-ci-${randomUUID().slice(0, 18)}`;
const text = randomBytes(384 * 1024).toString("base64");

async function until(check, description) {
  const deadline = Date.now() + 60_000;
  do {
    if (await check()) return;
    await sleep(1000);
  } while (Date.now() < deadline);
  throw new Error(`Timed out waiting for ${description}`);
}

async function withJazz(store, action) {
  const jazz = createJazz({ app, permissions, store });
  try {
    return await action((await jazz.ready()).db);
  } finally {
    await jazz.close();
  }
}
const insert = async (db) =>
  (await db.insert(app.private_notes, { text, sent_at: new Date() }).wait({ tier: "global" })).id;
const checkRow = (id) => async (db) => {
  const [row] = await db.all(app.private_notes.where({ id }));
  assert.ok(row?.text === text, "cold Jazz read must return the original bytes");
};

async function privateBlob() {
  const dir = await mkdtemp(join(tmpdir(), "jazz-blob-"));
  const prefix = `${name}/`;
  const store = () =>
    sqlite({ path: dir, blobs: vercelBlob({ prefix, credentials: { readWriteToken: token } }) });
  try {
    const id = await withJazz(store(), async (db) => {
      const id = await insert(db);
      // An acknowledged write may still be entirely inline. Require an evicted
      // leaf and its corresponding object before opening the second core.
      const sql = new DatabaseSync(join(dir, "jazz.sqlite"), { readOnly: true });
      try {
        const leaf = sql.prepare(
          "SELECT lower(hex(hash)) AS hash FROM _jazz_lv_node WHERE bytes = X'' LIMIT 1",
        );
        await until(async () => {
          const hash = leaf.get()?.hash;
          if (!hash) return false;
          const { blobs } = await list({ prefix: prefix + hash, token });
          return blobs.some((blob) => blob.pathname === prefix + hash && blob.size > 0);
        }, "an offloaded chunk in private Vercel Blob");
      } finally {
        sql.close();
      }
      return id;
    });
    await withJazz(store(), checkRow(id));
  } finally {
    await rm(dir, { recursive: true, force: true });
    // List the first page repeatedly, including the owner marker. Never
    // delete outside this run's prefix, even if a service returns a bad page.
    await until(async () => {
      const { blobs } = await list({ prefix, token });
      if (!blobs.length) return true;
      assert.ok(blobs.every((blob) => blob.pathname.startsWith(prefix)));
      await del(
        blobs.map((blob) => blob.url),
        { token },
      );
      return false;
    }, "cleanup of private Blob objects");
  }
}

async function drive() {
  const credentials = {
    token: process.env.VERCEL_TOKEN,
    teamId: process.env.VERCEL_ORG_ID,
    projectId: process.env.VERCEL_PROJECT_ID,
  };
  const appId = randomUUID();
  const secret = randomBytes(32).toString("hex");
  const port = 1625;
  const options = {
    appId,
    port,
    dataDir: "/data/store",
    blobs: files(),
    adminSecret: secret,
    backendSecret: secret,
  };
  const archive = await readFile(process.argv[2]);
  const disk = await Drive.getOrCreate({ name, region: "iad1", ...credentials });
  // Two distinct VMs sharing only the Drive rule out readback from the
  // original process, its SQLite cache, or a restored root-disk snapshot.
  async function session(action) {
    const vm = await Sandbox.create({
      ...credentials,
      name: `${name}-${randomUUID().slice(0, 8)}`,
      persistent: false,
      image: "vercel/sandbox/node:24",
      region: "iad1",
      mounts: { "/data": { drive: name, mode: "read-write" } },
      ports: [port],
      timeout: 180000,
    });
    try {
      await vm.writeFiles([{ path: "/tmp/core.tgz", content: archive }]);
      const unpack = await vm.runCommand("tar", [
        "-xzf",
        "/tmp/core.tgz",
        "-C",
        "/tmp",
        "package/jazz-napi.linux-x64-gnu.node",
      ]);
      assert.equal(unpack.exitCode, 0, "extract RC native core");
      const launcher = `
        const core = require("/tmp/package/jazz-napi.linux-x64-gnu.node").serveCore(JSON.parse(process.env.CORE));
        process.once("SIGTERM", () => core.stop());
        core.closed().then(() => process.exit(0), () => process.exit(1));
      `;
      await vm.runCommand({
        cmd: "node",
        args: ["-e", launcher],
        env: { CORE: JSON.stringify(options) },
        detached: true,
      });
      const url = vm.domain(port);
      await until(
        async () =>
          fetch(url + "/health", { signal: AbortSignal.timeout(3000) }).then(
            (r) => r.ok,
            () => false,
          ),
        "Sandbox core readiness",
      );
      return await withJazz(remote({ appId, url, secret }), (db) => action(db, vm));
    } finally {
      try {
        await vm.stop();
      } finally {
        await vm.delete({ deleteOrphanSnapshots: true }).catch((error) => {
          if (error.response?.status !== 404) throw error;
        });
      }
    }
  }
  try {
    const id = await session(async (db, vm) => {
      const id = await insert(db);
      await until(async () => {
        const check = await vm.runCommand("sh", [
          "-c",
          "find /data/store/blobs -type f -size +0c | grep -Eq '/[0-9a-f]{2}/[0-9a-f]{64}$'",
        ]);
        return check.exitCode === 0;
      }, "chunk files on the mounted Drive");
      return id;
    });
    await session(checkRow(id));
  } finally {
    // The stopped VM's Drive can take a moment to detach.
    await until(async () => {
      try {
        await disk.delete();
        return true;
      } catch (e) {
        if (e?.status === 404 || e?.response?.status === 404) return true;
        if (e?.status === 409 || e?.response?.status === 409) return false;
        throw e;
      }
    }, "Drive removal");
  }
}

const results = await Promise.allSettled([
  drive().then(() =>
    console.log("PASS t4-files cold read from a second Sandbox on the same Drive"),
  ),
  privateBlob().then(() => console.log("PASS t4-blob cold read from private Vercel Blob")),
]);
const failures = results.filter((result) => result.status === "rejected");
if (failures.length)
  throw new AggregateError(
    failures.map((failure) => failure.reason),
    "Vercel storage smoke failed",
  );
