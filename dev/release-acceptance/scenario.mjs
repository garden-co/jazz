import assert from "node:assert/strict";
import { readFileSync, writeFileSync, renameSync, existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
const [configPath, phase] = process.argv.slice(2);
const c = JSON.parse(readFileSync(configPath, "utf8"));
const require = createRequire(join(c.project, "package.json"));
const { schema: s } = await import(pathToFileURL(require.resolve("jazz-tools")));
const { createJazzSession } = await import(pathToFileURL(require.resolve("jazz-tools/backend")));
const app = s.defineApp({
  docs: s.table({ label: s.string(), body: s.string(), metadata: s.json() }, {}),
  denied: s.table({ value: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  for (const op of ["allowRead", "allowInsert", "allowUpdate", "allowDelete"])
    policy.docs[op].always();
});
const sessions = new Set();
const timeout = async (promise, label, ms = 20000) => {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`TIMEOUT ${label}`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};
const log = (check, detail = {}) =>
  console.log(JSON.stringify({ phase: c.phase, scenario: phase, check, ...detail }));
const statePath = join(c.state, "rows.json");
const state = existsSync(statePath) ? JSON.parse(readFileSync(statePath, "utf8")) : {};
function store(name) {
  const file = join(c.state, `${name}-accounts.json`);
  return {
    async read() {
      return existsSync(file) ? readFileSync(file, "utf8") : null;
    },
    // Single owner per profile; atomic replace. Never concurrently open this profile.
    async update(transform) {
      writeFileSync(`${file}.tmp`, transform(await this.read()), { mode: 0o600 });
      renameSync(`${file}.tmp`, file);
    },
  };
}
async function open(name, backend = false) {
  const session = await timeout(
    createJazzSession({
      appId: c.appId,
      serverUrl: c.serverUrl,
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(c.state, name) },
      store: store(name),
      initial: backend ? { backendSecret: c.backendSecret } : "local-first",
    }),
    `open ${name}`,
  );
  sessions.add(session);
  const snap = session.getSnapshot();
  assert(snap.client, snap.error?.message ?? snap.status);
  if (!backend) {
    assert(snap.account);
    assert.notEqual(snap.account.id, "00000000-0000-0000-0000-000000000000");
  }
  return { session, db: snap.client.db, account: snap.account?.id };
}
async function close(x) {
  await timeout(x.session.getSnapshot().client.shutdown({ waitForSync: false }), "shutdown");
  await timeout(x.session.close(), "close");
  sessions.delete(x.session);
}
async function one(db, id, tier = "global") {
  return timeout(db.one(app.docs.where({ id }), { tier }), `read ${tier}`);
}
async function wait(write, tier = "global") {
  return timeout(write.wait({ tier }), `settle ${tier}`);
}
try {
  if (phase === "seed") {
    const writer = await open("writer");
    const reader = await open("reader");
    state.account = writer.account;
    const keep = await wait(
      writer.db.insert(app.docs, { label: "keep", body: "retained", metadata: { keep: true } }),
    );
    const remove = await wait(
      writer.db.insert(app.docs, { label: "remove", body: "original", metadata: { remove: true } }),
    );
    state.keep = keep.id;
    state.remove = remove.id;
    assert.equal((await one(reader.db, keep.id)).body, "retained");
    await wait(writer.db.update(app.docs, remove.id, { body: "updated" }));
    assert.equal((await one(reader.db, remove.id)).body, "updated");
    await wait(writer.db.delete(app.docs, remove.id));
    assert.equal(await one(reader.db, remove.id), null);
    assert.equal((await one(reader.db, keep.id)).body, "retained");
    log("ordinary-crud-delete-one-keep-one");
    const backend = await open("backend", true);
    const hidden = await wait(backend.db.insert(app.denied, { value: "control-confirmed" }));
    assert.equal(
      (
        await timeout(
          backend.db.one(app.denied.where({ id: hidden.id }), { tier: "global" }),
          "control read",
        )
      ).value,
      "control-confirmed",
    );
    assert.deepEqual(
      await timeout(reader.db.all(app.denied, { tier: "global" }), "denied read"),
      [],
    );
    log("backend-auth-and-ordinary-default-deny-read");
    for (const bytes of [65536, 65537, 800000]) {
      const body = "x".repeat(bytes),
        metadata = { text: "", n: 42, yes: true, nil: null };
      metadata.text = "j".repeat(bytes - Buffer.byteLength(JSON.stringify(metadata)));
      assert.equal(Buffer.byteLength(JSON.stringify(metadata)), bytes);
      const row = await wait(
        writer.db.insert(app.docs, { label: `bytes-${bytes}`, body, metadata }),
      );
      const fresh = await open(`large-${bytes}`);
      const value = await one(fresh.db, row.id);
      assert.equal(value.body, body);
      assert.deepEqual(value.metadata, metadata);
      await close(fresh);
      log("large-full-value", { bytes });
    }
    await writer.db.disconnect();
    const pending = await wait(
      writer.db.insert(app.docs, {
        label: "pending",
        body: "offline",
        metadata: { pending: true },
      }),
      "local",
    );
    state.pending = pending.id;
    assert.equal(await one(reader.db, pending.id), null);
    assert.equal((await one(writer.db, pending.id, "local")).body, "offline");
    writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
    log("pending-local-write-not-visible-at-authority");
  } else if (phase === "offline") {
    const writer = await open("writer");
    assert.equal(writer.account, state.account);
    assert.equal((await one(writer.db, state.pending, "local")).body, "offline");
    assert.equal((await one(writer.db, state.keep, "local")).body, "retained");
    assert.equal(await one(writer.db, state.remove, "local"), null);
    log("offline-process-reopen-same-account-and-store");
  } else if (phase === "reconnect") {
    const writer = await open("writer");
    assert.equal(writer.account, state.account);
    await writer.db.reconnect();
    assert.equal((await one(writer.db, state.pending)).body, "offline");
    const reader = await open("independent-after-restart");
    assert.equal((await one(reader.db, state.keep)).body, "retained");
    assert.equal(await one(reader.db, state.remove), null);
    const rows = await timeout(
      reader.db.all(app.docs.where({ id: state.pending }), { tier: "global" }),
      "pending authority read",
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].body, "offline");
    log("cli-server-restart-pending-reconnect-exactly-one");
  } else throw new Error(`Unknown phase ${phase}`);
} finally {
  for (const session of sessions) await close({ session });
}
