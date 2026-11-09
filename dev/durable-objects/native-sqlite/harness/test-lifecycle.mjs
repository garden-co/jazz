// Alice loses transport before upload or after commit but before acknowledgement.
// Bob observes the authority independently; a fresh client verifies persisted history.
// Faults live in the test provider, never in the production Worker or sync engine.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { tsImport } from "tsx/esm/api";
import { createServerHarness } from "./server-runtime.mjs";
const { createJazz, remote } = await tsImport(
  "../../../../packages/jazz-tools/src/backend/index.ts",
  import.meta.url,
);
const { schema: s } = await tsImport(
  "../../../../packages/jazz-tools/src/index.ts",
  import.meta.url,
);

// Always exercise the packaged production host, without fixture routes.
process.env.JAZZ_DO_PACKAGED = "1";
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
const deployed = process.argv.includes("--cloudflare");
const appId = randomUUID();
const secret = "lifecycle-test-secret";
const persist = await mkdtemp(join(tmpdir(), "jazz-do-lifecycle-"));
const fault = { mode: undefined, droppedSends: 0, droppedReplies: 0 };
const sockets = new Set();
function interrupted(provider, inject = false) {
  return {
    async open() {
      const core = await provider.open();
      return {
        ...core,
        openSocket(headers, onEvent, access) {
          const socket = core.openSocket(
            headers,
            (error, event) => {
              if (!error && event.kind === "message" && inject && fault.mode === "reply") {
                fault.droppedReplies++;
                return;
              }
              onEvent(error, event);
            },
            access,
          );
          const connection = {
            disconnect() {
              socket.close();
              sockets.delete(connection);
              onEvent(null, { kind: "close", code: 1006, reason: "test deployment reconnect" });
            },
          };
          sockets.add(connection);
          return {
            send(data) {
              if (inject && fault.mode === "upload") {
                fault.droppedSends++;
                return true; // The transport accepted bytes, but the network lost them.
              }
              return socket.send(data);
            },
            close: () => {
              sockets.delete(connection);
              socket.close();
            },
          };
        },
      };
    },
  };
}
async function within(promise, description) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out: ${description}`)), 30_000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
async function until(check, description) {
  const deadline = Date.now() + 20_000;
  do {
    if (await check()) return;
    await delay(25);
  } while (Date.now() < deadline);
  throw new Error(`Timed out: ${description}`);
}
let mf, alice, bob, cloudflare;
const timeout = setTimeout(
  async () => {
    console.error("DO lifecycle test timed out");
    try {
      await cloudflare?.cleanup();
    } finally {
      process.exit(1);
    }
  },
  deployed ? 900_000 : 90_000,
);
try {
  let store, restart;
  if (deployed) {
    const { cloudflareLifecycleHost } = await import("./lifecycle-cloudflare.mjs");
    cloudflare = await cloudflareLifecycleHost();
    store = () => cloudflare.provider;
    let recovery = 0;
    restart = async () => {
      // One real upgrade, with the committed write still unacknowledged.
      // The other boundaries exercise network reconnect and fresh clients.
      if (++recovery === 2) await cloudflare.restart();
      for (const socket of [...sockets]) socket.disconnect();
    };
  } else {
    mf = createServerHarness(appId, secret, { durableObjectsPersist: persist });
    const url = String(await mf.ready).replace(/\/$/, "");
    const port = Number(new URL(url).port);
    store = () => remote({ url, appId, secret });
    restart = async () => {
      await mf.dispose();
      mf = createServerHarness(appId, secret, { durableObjectsPersist: persist, port });
      await mf.ready;
    };
  }
  const open = (inject = false) =>
    createJazz({ app, permissions, store: interrupted(store(), inject) }).ready();
  alice = await within(open(true), "Alice readiness");
  console.log("Lifecycle: Alice ready");
  bob = await within(open(), "Bob readiness");
  console.log("Lifecycle: Bob ready");
  const before = await alice.db
    .insert(app.notes, { title: "before upload" })
    .wait({ tier: "global" });
  const after = await alice.db
    .insert(app.notes, { title: "before acknowledgement" })
    .wait({ tier: "global" });
  const title = async (client, id) => (await client.db.all(app.notes.where({ id })))[0]?.title;
  const history = (client, id) =>
    client.db.all(app.$history.notes.where({ id }).orderBy("$acceptedAt"));
  await until(
    async () => (await title(bob, after.id)) === "before acknowledgement",
    "Bob receives initial writes",
  );
  console.log("Lifecycle: initial writes visible to Bob");
  const beforeHistory = await history(bob, before.id);
  assert.equal(beforeHistory.length, 1);

  // Drop Alice's upload, prove Bob still sees the old authority state, then
  // reconstruct the object. Alice must upload her pending write on reconnect.
  fault.mode = "upload";
  let uploaded = false;
  const upload = alice.db
    .update(app.notes, before.id, { title: "retried upload" })
    .wait({ tier: "global" })
    .then((row) => {
      uploaded = true;
      return row;
    });
  upload.catch(() => {}); // Observed below; avoid unhandled rejection while checking Bob.
  await until(() => fault.droppedSends > 0, "Alice attempts an upload");
  assert.equal(uploaded, false);
  assert.equal(await title(bob, before.id), "before upload");
  assert.deepEqual(await history(bob, before.id), beforeHistory);
  console.log("Lifecycle: reconstructing host");
  await restart();
  fault.mode = undefined;
  await within(upload, "upload settlement after reconnect");
  console.log("Lifecycle: lost upload recovered");
  await until(
    async () => (await title(bob, before.id)) === "retried upload",
    "Bob receives retried upload",
  );
  assert.equal((await history(bob, before.id)).length, 2);

  // Let the authority commit, but discard every reply to Alice. Bob's read
  // proves acceptance independently, while Alice's global receipt is pending.
  fault.mode = "reply";
  let acknowledged = false;
  const acknowledgement = alice.db
    .update(app.notes, after.id, { title: "committed without acknowledgement" })
    .wait({ tier: "global" })
    .then((row) => {
      acknowledged = true;
      return row;
    });
  acknowledgement.catch(() => {}); // Observed after reconstruction.
  await until(
    async () => (await title(bob, after.id)) === "committed without acknowledgement",
    "Bob observes committed write",
  );
  await until(() => fault.droppedReplies > 0, "Alice's replies are dropped");
  assert.equal(acknowledged, false, "Alice must not settle before receiving acknowledgement");
  const acceptedHistory = await history(bob, after.id);
  assert.equal(acceptedHistory.length, 2);
  console.log("Lifecycle: reconstructing host");
  await restart();
  fault.mode = undefined;
  await within(acknowledgement, "acknowledgement after reconnect");
  console.log("Lifecycle: lost acknowledgement recovered");
  assert.deepEqual(
    await history(bob, after.id),
    acceptedHistory,
    "reconnect preserves acceptance timestamps and transaction identities",
  );

  // Remove both client caches and reconstruct again: only persisted SQLite
  // can supply these rows/history. A subsequent write proves forward progress.
  await alice.close();
  alice = undefined;
  await bob.close();
  bob = undefined;
  console.log("Lifecycle: reconstructing host");
  await restart();
  bob = await within(open(), "Bob readiness");
  console.log("Lifecycle: Bob ready");
  assert.equal(await title(bob, before.id), "retried upload");
  assert.equal((await history(bob, before.id)).length, 2);
  assert.deepEqual(await history(bob, after.id), acceptedHistory);
  assert.equal((await bob.db.all(app.notes)).length, 2);
  await bob.db.update(app.notes, after.id, { title: "after recovery" }).wait({ tier: "global" });
  assert.equal((await history(bob, after.id)).length, 3);
  console.log(
    "Production DO lifecycle: lost upload, lost acknowledgement, reconnect settlement and persisted history passed.",
  );
} catch (error) {
  console.error("Lifecycle scenario failed:", error);
  throw error;
} finally {
  clearTimeout(timeout);
  fault.mode = undefined;
  try {
    const closed = await Promise.allSettled([alice?.close(), bob?.close(), mf?.dispose()]);
    for (const result of closed)
      if (result.status === "rejected") console.error("Client cleanup:", result.reason);
  } finally {
    try {
      await cloudflare?.cleanup();
    } finally {
      await rm(persist, { recursive: true, force: true });
    }
  }
}
