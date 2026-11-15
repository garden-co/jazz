import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
import { bindApp } from "jazz-tools";
import * as backend from "jazz-tools/backend";
import * as esm from "jazz-napi";
const require = createRequire(import.meta.url);
const cjs = require("jazz-napi");
assert.equal(cjs.NapiDb, esm.NapiDb);
assert.equal(typeof backend.createJazzClient, "function");
const platform = `linux-${process.arch}-gnu`;
const direct = require(`@garden-co/jazz-napi-${platform}`);
assert.equal(direct.nativeArtifactFingerprint(), cjs.nativeArtifactFingerprint());
// Exercise the packed implementation's offline native context. Schema is built
// with the public API; bypassing account enrollment keeps this deployment gate
// independent of external services. Hosted acceptance separately tests sessions.
const { createJazzContext } = await import(
  pathToFileURL(join(require.resolve("jazz-tools/backend"), "..", "create-jazz-context.js"))
);
// The app as its jazz.generated.ts would bind it, compiled by the packed
// jazz-napi, and its permissions as the packed Rust loader compiles them.
const bound = JSON.parse(
  cjs.compileApp(
    JSON.stringify({ format: 1, tables: { entries: { columns: { text: "string" } } } }),
  ),
);
const app = bindApp(bound);
const permissions = cjs.loadBackendApp(
  JSON.stringify({ schema: bound.schema, relations: bound.relations }),
  JSON.stringify({ format: 1, tables: { entries: { read: "true", insert: "true" } } }),
).permissions;
const directory = await mkdtemp(join(tmpdir(), "jazz-packed-native-"));
const appId = randomUUID();
let context;
try {
  for (const type of ["memory", "persistent"]) {
    const driver = type === "memory" ? { type } : { type, dataPath: join(directory, "db") };
    const open = () =>
      createJazzContext({
        appId,
        app,
        permissions,
        driver,
        // The memory backend requires a configured URL. db() does not enable
        // authenticated transport; all operations explicitly use the local tier.
        ...(type === "memory" ? { serverUrl: "http://127.0.0.1:9" } : {}),
      });
    context = open();
    const row = await context
      .db()
      .insert(app.entries, { text: `${type} receipt` })
      .wait({ tier: "local" });
    const check = async () => {
      const stored = await context
        .db()
        .one(app.entries.where({ id: row.id }), { tier: "local-first" });
      assert.equal(stored?.text, `${type} receipt`);
    };
    await check();
    await context.shutdown();
    context = undefined;
    if (type === "persistent") {
      context = open();
      await check();
      await context.shutdown();
      context = undefined;
    }
  }
  console.log(
    JSON.stringify({
      ok: true,
      arch: process.arch,
      node: process.version,
      glibc: process.report.getReport().header.glibcVersionRuntime,
      fingerprint: cjs.nativeArtifactFingerprint(),
      checks: [
        "CJS",
        "ESM",
        "scoped native",
        "backend import",
        "memory write/read",
        "persistent write/read/reopen/close",
      ],
    }),
  );
} finally {
  await context?.shutdown();
  await rm(directory, { recursive: true, force: true });
}
