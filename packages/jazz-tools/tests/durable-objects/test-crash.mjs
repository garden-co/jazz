// Alice uploads atomic batches; Bob inspects persisted state after SIGKILL,
// before Alice may replay. No shutdown hook or production fault route is used.
import assert from "node:assert/strict";
import { fork, execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { tsImport } from "tsx/esm/api";
const { createJazz, remote } = await tsImport("../../src/backend/index.ts", import.meta.url);
const { schema: s } = await tsImport("../../src/index.ts", import.meta.url);
assert.notEqual(process.platform, "win32", "this crash harness requires POSIX process groups");
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
const persist = await mkdtemp(join(tmpdir(), "jazz-do-crash-"));
const appId = randomUUID(),
  secret = "crash-test-secret";
let child, childExit, url, alice, bob;
let paused = false,
  dropReplies = false,
  onSend;
const sockets = new Set();
async function bounded(promise, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out: ${label}`)), 30_000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
async function start() {
  child = fork(
    new URL("./crash-server.mjs", import.meta.url),
    [appId, secret, persist, url ? new URL(url).port : "0"],
    { detached: true, stdio: ["ignore", "inherit", "inherit", "ipc"] },
  );
  childExit = once(child, "exit");
  const ready = await bounded(
    Promise.race([
      once(child, "message").then(([message]) => message),
      childExit.then(([code, signal]) => {
        throw new Error(`server exited during startup: ${code}/${signal}`);
      }),
    ]),
    "server startup",
  );
  url = ready.url;
  // Verify workerd inherits this dedicated group before starting any workload.
  // Doing this immediately before SIGKILL would delay the fault by spawning ps.
  // Fail explicitly if Miniflare changes its process-group ownership.
  const members = execFileSync("ps", ["-axo", "pid,pgid,comm"], { encoding: "utf8" })
    .split("\n")
    .map((line) => line.trim().split(/\s+/));
  assert.ok(
    members.some(
      ([pid, group, ...command]) =>
        Number(group) === child.pid &&
        Number(pid) !== child.pid &&
        command.join(" ").includes("workerd"),
    ),
    "dedicated process group must contain workerd",
  );
}
async function kill() {
  paused = true;
  process.kill(-child.pid, "SIGKILL");
  const [, signal] = await bounded(childExit, "SIGKILL exit");
  assert.equal(signal, "SIGKILL");
  child = undefined;
}
function writerProvider() {
  return {
    async open() {
      const core = await remote({ url, appId, secret }).open();
      return {
        ...core,
        openSocket(headers, receive, access) {
          const socket = core.openSocket(
            headers,
            (error, event) => {
              if (!error && event.kind === "message" && dropReplies) return;
              receive(error, event);
            },
            access,
          );
          const connection = {
            disconnect() {
              socket.close();
              sockets.delete(connection);
              receive(null, { kind: "close", code: 1006, reason: "crash test reconnect" });
            },
          };
          sockets.add(connection);
          return {
            send(data) {
              const sent = paused ? true : socket.send(data);
              if (sent && onSend) {
                const notify = onSend;
                onSend = undefined;
                notify();
              }
              return sent;
            },
            close() {
              sockets.delete(connection);
              socket.close();
            },
          };
        },
      };
    },
  };
}
const openReader = () =>
  bounded(
    createJazz({ app, permissions, store: remote({ url, appId, secret }) }).ready(),
    "fresh reader readiness",
  );
const history = (db, id) => db.all(app.$history.notes.where({ id }).orderBy("$acceptedAt"));
async function until(check, label) {
  const deadline = Date.now() + 20_000;
  do {
    if (await bounded(check(), label)) return;
    await delay(10);
  } while (Date.now() < deadline);
  throw new Error(`Timed out: ${label}`);
}
try {
  await start();
  alice = await bounded(
    createJazz({ app, permissions, store: writerProvider() }).ready(),
    "writer readiness",
  );
  const durable = await alice.db
    .insert(app.notes, { title: "acknowledged before crashes" })
    .wait({ tier: "global" });
  const completed = [];
  // Upload timing is a race, not proof of a particular SQLite instruction.
  // The final case establishes a known committed-but-unacknowledged boundary.
  for (const timing of ["before-delivery", 0, 1, 5, 20, "committed"]) {
    bob = await openReader();
    const ids = Array.from({ length: 20 }, () => randomUUID());
    const query = app.notes.where({ id: { in: ids } });
    // Establish Bob's subscription before the fault, independently of Alice.
    assert.equal((await bob.db.all(query)).length, 0);
    dropReplies = true;
    paused = timing === "before-delivery";
    const sent = new Promise((resolve) => {
      onSend = resolve;
    });
    const tx = alice.db.beginTransaction();
    ids.forEach((id, i) => tx.insert(app.notes, { title: `${timing}:${i}` }, { id }));
    const receipt = await tx.commit();
    let settled = false;
    const settlement = receipt.wait({ tier: "global" }).then(() => {
      settled = true;
    });
    settlement.catch(() => {}); // Observed after replay; cleanup may abandon it on failure.
    await bounded(sent, "batch upload attempt");
    let accepted;
    if (timing === "committed") {
      await until(
        async () => (await bob.db.all(query)).length === ids.length,
        "Bob observes committed batch",
      );
      accepted = await Promise.all(ids.map((id) => history(bob.db, id)));
      assert.ok(accepted.every((versions) => versions.length === 1));
    } else if (timing > 0) await delay(timing);
    assert.equal(settled, false, "writer acknowledgement must still be withheld");
    await kill();
    await bob.close();
    bob = undefined;
    await start();
    bob = await openReader();
    // Alice's outgoing frames remain dropped until these persisted-state checks
    // finish. Retry cannot repair a partial batch before Bob inspects it.
    const recovered = await bob.db.all(query);
    assert.ok(
      recovered.length === 0 || recovered.length === ids.length,
      `partial batch after SIGKILL (${timing}): ${recovered.length}/${ids.length}`,
    );
    if (timing === "committed") assert.equal(recovered.length, ids.length);
    if (timing === "before-delivery") assert.equal(recovered.length, 0);
    const durableRows = await bob.db.all(app.notes.where({ id: durable.id }));
    assert.deepEqual(
      durableRows.map((row) => row.title),
      ["acknowledged before crashes"],
    );
    const recoveredHistory = await Promise.all(ids.map((id) => history(bob.db, id)));
    assert.ok(recoveredHistory.every((versions) => versions.length === (recovered.length ? 1 : 0)));
    if (accepted) assert.deepEqual(recoveredHistory, accepted);
    paused = false;
    dropReplies = false;
    for (const socket of [...sockets]) socket.disconnect();
    await bounded(settlement, "batch settlement after crash");
    await until(
      async () => (await bob.db.all(query)).length === ids.length,
      "retried batch visible",
    );
    const finalRows = await bob.db.all(query);
    const titles = new Map(finalRows.map((row) => [row.id, row.title]));
    ids.forEach((id, i) => assert.equal(titles.get(id), `${timing}:${i}`));
    const finalHistory = await Promise.all(ids.map((id) => history(bob.db, id)));
    assert.ok(
      finalHistory.every((versions) => versions.length === 1),
      "retry must not duplicate history",
    );
    if (recovered.length) assert.deepEqual(finalHistory, recoveredHistory);
    completed.push(...ids);
    console.log(
      `SIGKILL ${timing}: recovered ${recovered.length}/${ids.length} rows before retry; settled once.`,
    );
    await bob.close();
    bob = undefined;
  }
  await alice.close();
  alice = undefined;
  bob = await openReader();
  assert.equal((await bob.db.all(app.notes)).length, completed.length + 1);
  const versions = await bob.db.all(app.$history.notes.where({ id: { in: completed } }));
  assert.equal(versions.length, completed.length);
  assert.equal(new Set(versions.map((row) => row.id)).size, completed.length);
  console.log(
    "Abrupt workerd termination: acknowledged durability, atomic batches and replay history passed.",
  );
} finally {
  paused = false;
  dropReplies = false;
  try {
    await Promise.allSettled([alice?.close(), bob?.close()]);
  } finally {
    if (child) {
      // Cleanup may be graceful; only kill() is used for the fault under test.
      child.send("stop");
      try {
        await bounded(childExit, "server cleanup");
      } catch (error) {
        process.kill(-child.pid, "SIGKILL");
        throw error;
      }
    }
    await rm(persist, { recursive: true, force: true });
  }
}
