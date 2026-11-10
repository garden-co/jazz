// Exercise the host carried by the npm tarball, without deploying to Cloudflare.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, symlink } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { verifyDurableObjectArtifact } from "../../packages/jazz-tools/scripts/verify-durable-object-artifact.mjs";

const tools = fileURLToPath(new URL("../../packages/jazz-tools/", import.meta.url));
const require = createRequire(join(tools, "package.json"));
const { Miniflare } = require("miniflare");
const work = await mkdtemp(join(tmpdir(), "jazz-packed-do-"));
let host, jazz;
const deadline = setTimeout(() => {
  console.error("Packed DO smoke test timed out");
  process.exit(1);
}, 60_000);
try {
  assert.ok(process.argv[2], "usage: verify-packed-durable-object.mjs <jazz-tools.tgz>");
  execFileSync("tar", ["-xzf", resolve(process.argv[2]), "-C", work]);
  const pkg = join(work, "package");
  const artifact = await verifyDurableObjectArtifact(pkg);
  await symlink(join(tools, "node_modules"), join(pkg, "node_modules"), "dir");
  const {
    createJazz,
    remote,
    schema: s,
  } = await import(pathToFileURL(join(pkg, "dist/backend/index.js")));
  globalThis.WebSocket = require("ws");
  const appId = randomUUID(),
    secret = "packed-do-smoke-secret";
  host = new Miniflare({
    modulesRoot: work,
    modules: [
      {
        type: "ESModule",
        path: join(work, "worker.mjs"),
        contents: artifact.worker.toString("utf8"),
      },
      { type: "CompiledWasm", path: join(work, "jazz.wasm"), contents: artifact.wasm },
    ],
    compatibilityDate: "2026-07-01",
    durableObjects: { JAZZ: { className: "JazzObject", useSQLite: true } },
    bindings: { JAZZ_APP_ID: appId, JAZZ_ADMIN_SECRET: secret },
    host: "127.0.0.1",
    port: 0,
  });
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
  const permissions = s.definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.never();
    policy.notes.allowInsert.never();
    policy.notes.allowUpdate.never();
    policy.notes.allowDelete.never();
  });
  jazz = createJazz({
    app,
    permissions,
    store: remote({ url: String(await host.ready), appId, secret }),
  });
  await jazz.ready();
  await jazz.db.insert(app.notes, { title: "packed artifact" });
  const rows = await jazz.db.all(app.notes);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].title, "packed artifact");
  console.log("Packed Durable Object host: readiness and read/write passed");
} finally {
  await jazz?.close();
  await host?.dispose();
  await rm(work, { recursive: true, force: true });
  clearTimeout(deadline);
}
