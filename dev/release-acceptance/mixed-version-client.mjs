#!/usr/bin/env node
// One Jazz client process for the mixed-version harness. It resolves
// `jazz-tools` from an installed external project (so each process runs exactly
// one package version) and executes JSON-line commands from stdin. Every
// command gets exactly one JSON-line reply on stdout: {rpc, ok, value|error}.
import { readFileSync, writeFileSync, renameSync, existsSync, mkdirSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";

const [project, stateDir] = process.argv.slice(2);
if (!project || !stateDir) throw new Error("usage: mixed-version-client.mjs <project> <state-dir>");
mkdirSync(stateDir, { recursive: true, mode: 0o700 });
const require = createRequire(join(project, "package.json"));
const { schema: s } = await import(pathToFileURL(require.resolve("jazz-tools")));
const { createJazzSession } = await import(pathToFileURL(require.resolve("jazz-tools/backend")));
const version = JSON.parse(
  readFileSync(join(project, "node_modules", "jazz-tools", "package.json"), "utf8"),
).version;

// Keep in sync with FIXTURE_SCHEMA in mixed-version.mjs (deployed via the CLI).
const app = s.defineApp({
  docs: s.table({ label: s.string(), body: s.string(), author: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  for (const op of ["allowRead", "allowInsert", "allowUpdate", "allowDelete"])
    policy.docs[op].always();
});

let session, db;
const subs = new Map();
const out = (msg) => process.stdout.write(`${JSON.stringify(msg)}\n`);

function accountStore(name) {
  const file = join(stateDir, `${name}-accounts.json`);
  return {
    async read() {
      return existsSync(file) ? readFileSync(file, "utf8") : null;
    },
    async update(transform) {
      writeFileSync(`${file}.tmp`, transform(await this.read()), {
        mode: 0o600,
      });
      renameSync(`${file}.tmp`, file);
    },
  };
}

const project_ = (row) =>
  row && { id: row.id, label: row.label, body: row.body, author: row.author };
const summarize = (row) =>
  row && {
    ...project_(row),
    body: row.body.length > 64 ? `<${row.body.length} chars>` : row.body,
  };

async function waitFor(check, label, ms) {
  const deadline = Date.now() + ms;
  for (;;) {
    const result = check();
    if (result.done) return result.value;
    if (Date.now() > deadline)
      throw new Error(`TIMEOUT ${label}; last=${JSON.stringify(result.value)}`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

const ops = {
  async open({ name, appId, serverUrl }) {
    session = await createJazzSession({
      appId,
      serverUrl,
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(stateDir, name) },
      store: accountStore(name),
      initial: "local-first",
    });
    const snap = session.getSnapshot();
    if (!snap.client) throw new Error(snap.error?.message ?? `session status ${snap.status}`);
    db = snap.client.db;
    return { version, account: snap.account?.id };
  },
  async insert({ values, wait }) {
    const write = db.insert(app.docs, values);
    const row = wait ? await write.wait({ tier: wait }) : (write.value ?? write);
    return { id: row.id };
  },
  async update({ id, values, wait }) {
    const write = db.update(app.docs, id, values);
    if (wait) await write.wait({ tier: wait });
    return null;
  },
  async delete({ id, wait }) {
    const write = db.delete(app.docs, id);
    if (wait) await write.wait({ tier: wait });
    return null;
  },
  async one({ id, tier }) {
    return project_(await db.one(app.docs.where({ id }), { tier }));
  },
  async subscribe({ sub, tier }) {
    const state = { rows: null, error: null, updates: 0 };
    subs.set(sub, state);
    state.unsubscribe = db.subscribe(
      app.docs,
      {
        onUpdate: (rows) => {
          state.rows = rows.map(project_);
          state.updates++;
        },
        onError: (error) => {
          state.error = String(error?.message ?? error);
        },
      },
      tier ? { tier } : undefined,
    );
    return null;
  },
  // Wait until a subscription shows row `id` with `body` (or `absent: true`).
  async expectSub({ sub, id, body, absent, ms = 20000 }) {
    const state = subs.get(sub);
    return waitFor(
      () => {
        if (state.error) throw new Error(`subscription error: ${state.error}`);
        const row = state.rows?.find((r) => r.id === id);
        const ok = absent ? state.rows && !row : row && (body === undefined || row.body === body);
        return {
          done: ok,
          value: { updates: state.updates, row: summarize(row) },
        };
      },
      `sub ${sub} ${absent ? "absent" : "has"} ${id}`,
      ms,
    ).then(() => ({ updates: state.updates }));
  },
  async disconnect() {
    await db.disconnect();
    return null;
  },
  async reconnect() {
    await db.reconnect();
    return null;
  },
  async close() {
    for (const state of subs.values()) state.unsubscribe?.();
    subs.clear();
    if (session) {
      await session.getSnapshot().client?.shutdown({ waitForSync: false });
      await session.close();
    }
    session = db = undefined;
    return null;
  },
};

const trace = process.env.JAZZ_MIXED_TRACE === "1";
if (trace) setInterval(() => process.stderr.write(`[tick ${Date.now()}]\n`), 2000).unref();
const lines = createInterface({ input: process.stdin });
for await (const line of lines) {
  if (!line.trim()) continue;
  const { rpc, op, ms = 20000, ...args } = JSON.parse(line);
  let timer;
  if (trace)
    process.stderr.write(`[${Date.now()}] start ${op} ${JSON.stringify(args).slice(0, 200)}\n`);
  try {
    const value = await Promise.race([
      ops[op]({ ms, ...args }),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`TIMEOUT ${op} after ${ms}ms`)), ms + 1000);
      }),
    ]);
    out({ rpc, ok: true, value });
  } catch (error) {
    out({ rpc, ok: false, error: String(error?.stack ?? error) });
  } finally {
    clearTimeout(timer);
  }
}
process.exit(0);
