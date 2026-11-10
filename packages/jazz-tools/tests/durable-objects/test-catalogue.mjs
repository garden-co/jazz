import assert from "node:assert/strict";
import { createServerHarness } from "./server-runtime.mjs";
import { tsImport } from "tsx/esm/api";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const root = resolve(here, "../../../..");
const app = "11111111-1111-4111-8111-111111111111";
const secret = "catalogue-test-admin";
const persist = await mkdtemp(join(tmpdir(), "jazz-do-catalogue-"));
let mf;
async function start() {
  mf = createServerHarness(app, secret, { durableObjectsPersist: persist });
  await mf.ready;
}
async function request(path, { body, headers, method } = {}) {
  return mf.dispatchFetch(`http://jazz.test/apps/${app}/${path}`, {
    method: method ?? (body === undefined ? "GET" : "POST"),
    headers: headers ?? {
      "x-jazz-backend-credential": secret,
      "x-jazz-backend-access": "admin",
      "content-type": "application/json",
    },
    ...(body === undefined ? {} : { body: typeof body === "string" ? body : JSON.stringify(body) }),
  });
}
async function ok(path, options) {
  const response = await request(path, options);
  const text = await response.text();
  assert.equal(response.status, 200, text);
  return JSON.parse(text);
}
try {
  await start();
  const fixtures = await (await mf.dispatchFetch("http://jazz.test/test-fixtures")).json();
  for (const headers of [
    {},
    { "x-jazz-admin-secret": secret },
    { "x-jazz-backend-credential": "wrong", "x-jazz-backend-access": "admin" },
    { "x-jazz-backend-credential": secret },
    { "x-jazz-backend-credential": secret, "x-jazz-backend-access": "backend" },
  ])
    assert.equal((await request("admin/bundles", { body: fixtures.valid, headers })).status, 401);
  assert.deepEqual(await ok("schemas"), { hashes: [], schemas: [] });
  assert.equal((await request("admin/bundles", { body: "{" })).status, 400);
  assert.equal((await request("admin/bundles", { body: " ".repeat((8 << 20) + 1) })).status, 413);
  assert.equal(
    (await request("admin/bundles", { body: { ...fixtures.valid, targetSchemaHash: "invalid" } }))
      .status,
    400,
  );
  // Schema creation fails inside DO SQLite, after startup validation. The
  // transaction must remove its admitted genesis, allowing another deployment.
  assert.equal((await request("admin/bundles", { body: fixtures.wide })).status, 500);
  assert.deepEqual(await ok("schemas"), { hashes: [], schemas: [] });
  assert.equal((await request("admin/bundles", { body: fixtures.invalidPolicy })).status, 422);
  assert.deepEqual(await ok("schemas"), { hashes: [], schemas: [] });
  const replies = await Promise.all([
    ok("admin/bundles", { body: fixtures.valid }),
    ok("admin/bundles", { body: fixtures.valid }),
  ]);
  assert.equal(replies.filter((reply) => reply.changed).length, 1);
  assert.equal(replies[0].bundle, replies[1].bundle);
  // Exercise the actual provider's credential/access headers over HTTP, not
  // just an approximation of its transport in the test helpers.
  const { remote } = await tsImport(
    join(root, "packages/jazz-tools/src/backend/core-providers/remote.ts"),
    import.meta.url,
  );
  const remoteCore = await remote({ url: String(await mf.ready), appId: app, secret }).open();
  const remoteReply = await remoteCore.handle(
    "POST",
    `/apps/${app}/admin/bundles`,
    [["content-type", "application/json"]],
    new TextEncoder().encode(JSON.stringify(fixtures.valid)),
    "admin",
  );
  assert.equal(remoteReply.status, 200, new TextDecoder().decode(remoteReply.body));
  assert.equal(JSON.parse(new TextDecoder().decode(remoteReply.body)).changed, false);
  const denied = await remoteCore.handle(
    "GET",
    `/apps/${app}/schemas`,
    [],
    new Uint8Array(),
    "session",
  );
  assert.equal(denied.status, 401);
  await remoteCore.close();
  const before = await ok("admin/bundles");
  assert.equal(before.bundles.length, 1);
  const hash = fixtures.valid.targetSchemaHash;
  assert.equal((await ok(`schema/${hash}`)).publishedAt, before.bundles[0].publishedAt);
  assert.deepEqual((await ok("admin/migrations/graph")).schemas, [hash]);
  assert.equal((await request("schema/invalid")).status, 400);
  assert.equal((await request(`schema/${"0".repeat(64)}`)).status, 404);
  assert.equal((await request("admin/bundles", { body: fixtures.different })).status, 422);
  assert.deepEqual(await ok("admin/bundles"), before);
  assert.equal((await mf.dispatchFetch("http://jazz.test/apps/other/schemas")).status, 404);
  await mf.dispose();
  await start();
  assert.deepEqual(await ok("admin/bundles"), before);
  assert.equal((await ok("admin/bundles", { body: fixtures.valid })).changed, false);
  assert.deepEqual((await ok("schemas")).hashes, [hash]);
  console.log(
    "Production DO catalogue routes: auth, limits, failed startup rollback, concurrent publication, and restart passed.",
  );
} finally {
  await mf?.dispose();
  await rm(persist, { recursive: true, force: true });
}
