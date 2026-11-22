// The harness must not pass what it didn't see. No network, nothing installed: `node --test`.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { isJazzName } from "../build/names.mjs";
import { LANGS } from "../lib/common.mjs";
import { checkFiles, checkManifest, checkRun } from "../lib/provenance.mjs";
import { ANDROID_RULES, judgeAndroid } from "../suite/android-rules.mjs";
import { closed, storeHeld } from "../suite/errors.mjs";
import { Proxy } from "../suite/proxy.mjs";
import { redact } from "../suite/redact.mjs";
import { Broken, Round } from "../suite/round.mjs";
import { RULES } from "../suite/rules/index.mjs";

const here = fileURLToPath(new URL(".", import.meta.url));
const sha = "a".repeat(40);
const want = { sha, run: "7", repo: "garden-co/jazz" };
const goodRun = {
  id: 7,
  head_sha: sha,
  event: "push",
  path: ".github/workflows/release-artifacts.yml",
  conclusion: "success",
  repository: { full_name: want.repo },
  head_repository: { full_name: want.repo },
};

test("provenance fails closed", () => {
  checkRun(goodRun, want);
  checkRun({ ...goodRun, event: "workflow_dispatch" }, want);
  for (const wrong of [
    { head_sha: "b".repeat(40) },
    { event: "pull_request" },
    { path: ".github/workflows/lifecycle-acceptance.yml" },
    { conclusion: "failure" },
    { head_repository: { full_name: "someone/jazz" } },
    { head_repository: null },
    { id: 8 },
  ])
    assert.throws(() => checkRun({ ...goodRun, ...wrong }, want), undefined, JSON.stringify(wrong));

  const manifest = {
    schema: 1,
    sha,
    tree: "c".repeat(40),
    run_id: 7,
    repository: want.repo,
    workflow: "release-artifacts.yml",
    version: "2.0.0-beta.0",
    files: [{ artifact: "a", path: "x", sha256: "1" }],
  };
  checkManifest(manifest, want);
  assert.throws(() => checkManifest({ ...manifest, sha: "b".repeat(40) }, want));
  assert.throws(() => checkManifest({ ...manifest, run_id: 9 }, want));
  checkFiles(manifest, "a", new Map([["x", "1"]]));
  assert.throws(() => checkFiles(manifest, "a", new Map([["x", "2"]])), /sha256/);
  assert.throws(() => checkFiles(manifest, "a", new Map()), /missing/);
  assert.throws(() => checkFiles(manifest, "a", new Map([["x", "1"], ["y", "1"]])), /not in the manifest/);
  assert.throws(() => checkFiles(manifest, "b", new Map()), /lists nothing/);
});

test("the blocking registry's names", () => {
  for (const name of ["jazz-tools", "jazz-napi", "@garden-co/jazz-napi-linux-x64-gnu", "create-jazz"]) assert.ok(isJazzName(name));
  for (const name of ["hono", "tsx", "@hono/node-server"]) assert.ok(!isJazzName(name));
});

test("errors match by kind alone, and only the expected one", () => {
  assert.ok(storeHeld.matches({ kind: "StoreHeld", msg: "" }, "rust"));
  for (const lang of ["go", "swift"]) assert.ok(storeHeld.matches({ kind: "refused", msg: "anything" }, lang));
  assert.ok(storeHeld.matches({ kind: "Refused", msg: "" }, "kotlin"));
  assert.ok(!storeHeld.matches({ kind: "Jazz", msg: "database is locked" }, "rust"));
  assert.ok(!storeHeld.matches({ kind: "Refused", msg: "the local store is held" }, "rust"));
  assert.ok(!storeHeld.matches({ kind: "io", msg: "the local store is held" }, "go"));
  assert.ok(!storeHeld.matches({ kind: "Io", msg: "database is locked" }, "kotlin"));
  assert.ok(!storeHeld.matches({ kind: "harness.timeout", msg: "" }, "kotlin"));
  assert.ok(closed.matches({ kind: "Stopped" }, "rust") && !closed.matches({ kind: "Replaced" }, "rust"));
  assert.ok(closed.matches({ kind: "closed" }, "swift") && !closed.matches({ kind: "replaced" }, "swift"));
  assert.ok(!closed.matches({ kind: "harness.timeout" }, "go"));
});

for (const [fake, ids] of [
  ["yes", ["persist.clean-exit", "persist.sigkill", "store.second-open", "store.second-open.race", "close.ends-waits", "close.drain", "close.session", "signout.derived", "signout.follow"]],
  ["throws", ["store.second-open", "store.second-open.race"]],
])
  for (const id of ids)
    test(`a driver where ${fake === "yes" ? "everything works" : "an open throws something"} fails ${id}`, async () => {
      const driver = { cmd: [process.execPath, join(here, "fake-driver.mjs")], env: { FAKE: fake } };
      const round = new Round({ lang: "go", driver, stack: { appId: "app", backendPort: 9, token: async () => "t" } }, 0);
      try {
        await assert.rejects(RULES.find((rule) => rule.id === id).run(round), Broken);
      } finally {
        await round.cleanup();
      }
    });

test("a proxy's port can't be taken as an ephemeral one while it refuses", async () => {
  const proxies = await Promise.all(Array.from({ length: 20 }, () => Proxy.start(9)));
  try {
    for (const proxy of proxies) {
      assert.ok(proxy.port >= 10_000 && proxy.port < 32_768, String(proxy.port));
      await proxy.refuse();
      await proxy.up();
    }
    assert.equal(new Set(proxies.map((proxy) => proxy.port)).size, proxies.length);
  } finally {
    for (const proxy of proxies) await proxy.stop();
  }
});

test("the proxy's outages are real", async () => {
  const server = createServer((socket) => socket.on("error", () => {}).pipe(socket)).listen(0, "127.0.0.1");
  await once(server, "listening");
  const proxy = await Proxy.start(server.address().port);
  const answered = () =>
    fetchEcho(proxy.port).then(
      (bytes) => bytes === "ping",
      () => false,
    );
  try {
    assert.ok(await answered());
    await proxy.refuse();
    await assert.rejects(fetchEcho(proxy.port), /ECONNREFUSED/);
    await proxy.hole();
    assert.equal(await fetchEcho(proxy.port), "no answer");
    await proxy.up();
    assert.ok(await answered());
  } finally {
    await proxy.stop();
    server.close();
  }
});

async function fetchEcho(port) {
  const { createConnection } = await import("node:net");
  const socket = createConnection(port, "127.0.0.1");
  try {
    await once(socket, "connect");
    const data = once(socket, "data").then(([bytes]) => String(bytes), () => "no answer");
    socket.write("ping");
    let timer;
    const late = new Promise((resolve) => (timer = setTimeout(() => resolve("no answer"), 300)));
    return await Promise.race([data, late]).finally(() => clearTimeout(timer));
  } finally {
    socket.destroy();
  }
}

test("the Android leg", () => {
  const head = "testing jazz-kotlin-android 2.0.0-beta.1: f00  /repo/x.aar\n";
  const rules = (outcomes) =>
    `${head}r1 RULE persist.reopen.device PASS: logcat, not the summary\n--- rules, jazz-kotlin-android 2.0.0-beta.1\n` +
    Object.entries(outcomes).map(([id, o]) => `RULE ${id} ${o}: detail${id === "offline.bearer-open" ? " (observe only)" : ""}`).join("\n");
  const options = { aarSha256: "f00", version: "2.0.0-beta.1", code: 1 };
  const all = Object.fromEntries(ANDROID_RULES.map(({ id }) => [id.replace(/^android\.(?!memory)/, ""), "PASS"]));
  const by = (log, more) => Object.fromEntries(judgeAndroid(log, { ...options, ...more }).map((r) => [r.id, r.status]));

  const green = by(rules(all), { code: 0 });
  assert.equal(green["android.persist.reopen.device"], "PASS");
  assert.equal(green["android.store.second-open"], "NOT_COVERED", "awaiting Anselm");
  assert.equal(green["android.store.per-account"], "NOT_COVERED", "no such check");
  assert.equal(by(rules({ ...all, "persist.reopen.device": "BLOCKED" }))["android.persist.reopen.device"], "FAIL", "BLOCKED is a FAIL");
  assert.equal(by(rules({ ...all, "persist.reopen.device": "FAIL" }))["android.persist.reopen.device"], "FAIL");
  const missing = { ...all };
  delete missing["sync.offline-write"];
  assert.equal(by(rules(missing))["android.sync.offline-write"], "FAIL", "a rule not reported");
  assert.equal(by(`${head}r1 RULE android.memory PASS: x\n`)["android.memory"], "FAIL", "only the summary counts");
  assert.ok(Object.values(by(rules(all), { aarSha256: "other" })).every((s) => s === "FAIL"), "another AAR");
  const observed = judgeAndroid(rules({ ...all, "store.second-open": "FAIL" }), options).find((r) => r.id === "android.store.second-open");
  assert.deepEqual([observed.status, observed.observed], ["NOT_COVERED", "FAIL"]);
});

function verdict(edit, mustFail = []) {
  const dir = mkdtempSync(join(tmpdir(), "la-verdict-"));
  try {
    const results = new Map();
    for (const lang of [...LANGS, "android"]) {
      const rules = (lang === "android" ? ANDROID_RULES : RULES).map(({ id, rounds = 1 }) => ({
        id,
        status: "PASS",
        rounds,
        passed: rounds,
        failures: {},
      }));
      results.set(lang, { name: "c", lang, sha, run: "7", ms: 1, review: "reviewed", results: rules });
    }
    edit(results);
    for (const [lang, result] of results) writeFileSync(join(dir, `c-${lang}.json`), JSON.stringify(result));
    const env = { ...process.env, CANDIDATES: JSON.stringify([{ name: "c", sha, run: "7", mustFail }]) };
    delete env.GITHUB_STEP_SUMMARY;
    return spawnSync(process.execPath, [join(here, "..", "verdict.mjs"), dir], { env, encoding: "utf8" });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("the verdict fails closed", () => {
  assert.equal(verdict(() => {}).status, 0);
  assert.equal(verdict((results) => results.delete("go")).status, 1, "a language without results");
  assert.equal(verdict((results) => results.delete("android")).status, 1, "no Android results");
  assert.equal(verdict((results) => results.get("rust").results.pop()).status, 1, "a rule without a result");
  assert.equal(verdict((results) => (results.get("go").sha = "b".repeat(40))).status, 1, "another candidate's results");

  assert.equal(verdict((results) => (results.get("go").results[1].passed = 1)).status, 1, "a PASS of too few rounds");
  const gap = verdict((results) => (results.get("go").results[1].status = "NOT_COVERED"));
  assert.match(gap.stdout, /VERDICT c INCOMPLETE/, "a gap no rule declares");

  const undecided = RULES.findIndex((rule) => rule.basis === "pending");
  const incomplete = verdict((results) => (results.get("go").results[undecided].status = "NOT_COVERED"));
  assert.equal(incomplete.status, 1);
  assert.match(incomplete.stdout, /VERDICT c INCOMPLETE/);

  const control = verdict(() => {}, ["store.second-open"]);
  assert.equal(control.status, 1);
  assert.match(control.stdout, /CONTROL_BROKEN c rust store\.second-open /);
  // A pending rule's control is what it observed.
  const pendingIndex = RULES.findIndex((rule) => rule.id === "store.second-open");
  const observedFail = verdict((results) => {
    for (const lang of LANGS) Object.assign(results.get(lang).results[pendingIndex], { status: "NOT_COVERED", observed: "FAIL" });
  }, ["store.second-open"]);
  assert.match(observedFail.stdout, /CONTROL_OK c go store\.second-open/);
  assert.doesNotMatch(observedFail.stdout, /CONTROL_BROKEN/);
  // Kotlin declares it has no session: it isn't held to a session rule's control.
  const sessionIndex = RULES.findIndex((rule) => rule.id === "signout.derived");
  const declared = verdict((results) => {
    for (const lang of LANGS) results.get(lang).results[sessionIndex].status = lang === "kotlin" ? "NOT_COVERED" : "FAIL";
  }, ["signout.derived"]);
  assert.doesNotMatch(declared.stdout, /CONTROL_BROKEN/);
  assert.match(declared.stdout, /CONTROL_OK c go signout\.derived/);
});

test("secrets don't reach a log", () => {
  const key = `jazz-auth-v1:${"k".repeat(43)}`;
  const token = `eyJ${"h".repeat(20)}.${"p".repeat(20)}.${"s".repeat(20)}`;
  const out = redact(`key ${key} token ${token} adminSecret: abc123`);
  for (const secret of [key, token, "abc123"]) assert.ok(!out.includes(secret), secret);
});
