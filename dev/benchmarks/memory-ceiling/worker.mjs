// A Jazz client process for the memory-ceiling harness. The harness forks
// one long-lived writer (the importer) and a fresh reader per cold load, so
// each one's memory is measured on its own, apart from the server's.
//
// Writers and readers are ordinary Node clients of the shipped runtime
// (jazz-napi): trusted bootstrap writes use the backend secret, and every
// other write and read acts as an app user through the deployed policies,
// as the app's own server routes do with `forRequest()`.
import { pathToFileURL } from "node:url";
import { cpuSecondsOf } from "./processes.mjs";

let adapter;
let scenario;
let context;
const sessions = new Map();

const send = (message) => process.send?.(message);

async function init({ adapterPath, scenarioName, appId, serverUrl, backendSecret, writes }) {
  writeAs = writes ?? "import";
  adapter = (await import(pathToFileURL(adapterPath).href)).default;
  scenario = adapter.scenarios[scenarioName];
  if (!scenario) throw new Error(`${adapter.name} has no scenario "${scenarioName}"`);
  // createJazzContext is the backend context BigLabel's policy tests use;
  // the public `jazz-tools/backend` entry point wraps it in an account
  // session, which needs a JWT issuer this harness does not run.
  const root = new URL("./", import.meta.resolve("jazz-tools/package.json"));
  const { createJazzContext } = await import(
    new URL("dist/backend/create-jazz-context.js", root).href
  );
  context = createJazzContext({
    appId,
    app: adapter.app,
    permissions: adapter.permissions,
    driver: { type: "memory" },
    serverUrl,
    backendSecret,
    env: "dev",
  });
}

// "import" sends every growth write through the trusted path, as an app's
// importer or demo-data route does; "app" writes each as the actor the
// adapter names, through the deployed policies (much slower: see README).
let writeAs = "import";

function dbFor(actor) {
  if (actor === "backend") {
    if (!sessions.has(actor)) sessions.set(actor, context.asBackend());
    return sessions.get(actor);
  }
  if (!sessions.has(actor)) {
    sessions.set(
      actor,
      context.forSession({
        issuer: "https://memory-ceiling.jazz.test",
        user_id: actor,
        account_id: actor,
        claims: {},
        authMode: "external",
      }),
    );
  }
  return sessions.get(actor);
}

function apply(db, tx) {
  return db.transaction((t) => {
    for (const op of tx.ops) {
      if (op.update) t.update(adapter.app[op.table], op.id, op.values);
      else t.insert(adapter.app[op.table], op.values, { id: op.id });
    }
  });
}

/**
 * App-shaped user edits at the current size, one at a time and always
 * through the policies: what a user's write costs the server at this size.
 */
async function probe({ scale, count, serverPid }) {
  const serverCpu = () => (serverPid ? cpuSecondsOf(serverPid) : 0);
  let transactions = 0;
  let writeMs = 0;
  let readMs = 0;
  let writeCpu = 0;
  const signedIn = new Set();
  for (const tx of scenario.probe(scale, count)) {
    const db = dbFor(tx.actor);
    // What the app loads when this user signs in (the label switcher, say).
    // A new session's policy-filtered reads can come back empty until it
    // has (see README), so the edits start from where the app would.
    if (!signedIn.has(tx.actor)) {
      signedIn.add(tx.actor);
      for (const query of scenario.signIn?.(tx.actor) ?? []) await db.all(query);
    }
    // The user's screen shows what they edit: load each row first, as the
    // app has, so the update names the version it replaces.
    const r0 = Date.now();
    for (const op of tx.ops) {
      if (op.update) await db.all(adapter.app[op.table].where({ id: op.id }));
    }
    const w0 = Date.now();
    const c0 = serverCpu();
    const write = await apply(db, tx);
    await write.wait({ tier: "global" });
    writeCpu += serverCpu() - c0;
    writeMs += Date.now() - w0;
    readMs += w0 - r0;
    transactions++;
  }
  const per = (v) => (transactions ? Math.round(v / transactions) : null);
  return {
    transactions,
    msPerTransaction: per(writeMs),
    serverCpuMsPerTransaction: serverPid ? per(writeCpu * 1000) : null,
    readMsPerTransaction: per(readMs),
  };
}

/** Write transactions [from, to) of the scenario, a bounded window in flight. */
async function seed({ from, to, window = 64 }) {
  const started = Date.now();
  const inFlight = new Set();
  let transactions = 0;
  let rows = 0;
  for (const tx of scenario.transactions(from, to)) {
    const db = dbFor(writeAs === "import" ? "backend" : tx.actor);
    const pending = Promise.resolve(apply(db, tx)).then((write) => write.wait({ tier: "global" }));
    const tracked = pending.finally(() => inFlight.delete(tracked));
    inFlight.add(tracked);
    transactions++;
    rows += tx.ops.length;
    if (inFlight.size >= window) await Promise.race(inFlight);
  }
  await Promise.all(inFlight);
  return { transactions, rows, ms: Date.now() - started };
}

/** Run a user's reads once, as their browser does when it opens the app. */
async function read({ users }) {
  const started = Date.now();
  const results = await Promise.all(
    users.map(async ({ actor, reads }) => {
      const db = dbFor(actor);
      const out = {};
      for (const { name, query } of reads) {
        const t0 = Date.now();
        const rows = await db.all(query);
        out[name] = { rows: rows.length, ms: Date.now() - t0 };
      }
      return out;
    }),
  );
  return { ms: Date.now() - started, results };
}

process.on("message", async (message) => {
  try {
    let result;
    if (message.op === "init") result = await init(message);
    else if (message.op === "seed") result = await seed(message);
    else if (message.op === "probe") result = await probe(message);
    else if (message.op === "read") {
      const { kind, size, scale } = message;
      const users = kind === "client" ? [scenario.client(scale)] : scenario.crowd(scale, size ?? 8);
      result = await read({ users });
    } else if (message.op === "exit") {
      await context?.shutdown?.();
      process.exit(0);
    }
    send({ id: message.id, ok: true, result });
  } catch (error) {
    // The message and where it was thrown; the rest is the runtime's frames.
    const text = String(error?.stack ?? error)
      .split("\n")
      .slice(0, 3)
      .join("\n");
    send({ id: message.id, ok: false, error: text });
  }
});
send({ ready: true });
