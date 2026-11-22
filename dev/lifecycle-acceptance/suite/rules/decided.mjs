// The five rules #native-language-apis decided (relayed 22:37 UTC). Scored.
import { closed, inLang, notSupported, unsynced } from "../errors.mjs";
import { Broken, equal, refusal, show, value } from "../round.mjs";
import { waiting } from "./waits.mjs";

const SLACK_MS = 3_000;
const DRAIN_MS = 2_000;
const NO_SESSION = { kotlin: "jazz-kotlin has no JazzSession" };
const VARIANTS = ["sync", "read", "write"].flatMap((wait) => ["refuse", "hole"].map((outage) => ({ wait, outage })));

/** The pending call ends as `closed` within the slack after the close. */
async function endedAsClosed(r, started, step) {
  const reply = await Promise.race([started.reply, r.sleep(SLACK_MS).then(() => null)]);
  if (!reply) throw new Broken(`${step}: still waiting ${SLACK_MS} ms after the close`);
  refusal(reply, inLang(closed, r.lang), step);
}

async function endsWaits(r) {
  const variant = VARIANTS[r.index % VARIANTS.length];
  const { proxy, dir, process, key, title, started } = await waiting(r, variant);
  const shut = process.send("close", { h: "a", ...(r.lang === "rust" ? { ms: 0 } : {}) }, 40_000);
  await endedAsClosed(r, started, `the pending ${variant.wait} (${variant.outage})`);
  const reply = await shut.reply;
  r.time("close", reply.ms);
  value(reply, "close");
  if (reply.ms > SLACK_MS) throw new Broken(`close took over ${SLACK_MS} ms`, `${reply.ms} ms`);
  await r.endCleanly(process);
  // What was committed locally is still there: it may reach the server later.
  await proxy.refuse();
  const next = await r.process();
  value(await r.open(next, { dir, proxy, key }), "open after the close");
  equal(value(await next.call("read", { h: "a", tier: "local" }), "local read"), [`online ${r.id}`, title].sort(), "the close lost a local write");
}

async function drain(r) {
  if (r.index % 2 === 0) {
    const { process, started } = await waiting(r, { wait: "sync", outage: "hole" });
    const reply = await process.call("close", { h: "a", ms: DRAIN_MS }, 40_000);
    r.time("close(T) with a wait pending", reply.ms);
    value(reply, `close with timeout ${DRAIN_MS} ms`);
    if (reply.ms > DRAIN_MS + SLACK_MS) throw new Broken(`close(T) took over T + ${SLACK_MS} ms`, `${reply.ms} ms`);
    await endedAsClosed(r, started, "the pending sync");
    return;
  }
  // A memory store with a write the server never got.
  const proxy = await r.proxy();
  const process = await r.process();
  const key = value(await process.call("genkey"), "genkey");
  value(await process.call("open", { ...r.openArgs({ dir: r.dir(), proxy, key }), storage: "memory" }), "memory open");
  const owner = value(await process.call("account", { h: "a" }), "account");
  await proxy.refuse();
  value(await process.call("write", { h: "a", title: `unsent ${r.id}`, owner, tier: "local" }), "offline write");
  const reply = await process.call("close", { h: "a", ms: DRAIN_MS }, 40_000);
  r.time("close(T) of a memory store", reply.ms);
  refusal(reply, inLang(unsynced, r.lang), "close of a memory store with an unsent write");
  if (reply.ms > DRAIN_MS + SLACK_MS) throw new Broken(`close(T) took over T + ${SLACK_MS} ms`, `${reply.ms} ms`);
}

async function sessionAndHandle(r, start, token = "") {
  const proxy = await r.proxy();
  const process = await r.process();
  const args = { h: "s", dir: r.dir(), url: proxy.url, app: r.stack.appId, start, token };
  value(await process.call("sopen", args), "session open");
  value(await process.call("sjazz", { h: "s", as: "a" }), "the session's client");
  const owner = value(await process.call("account", { h: "a" }), "account");
  return { proxy, process, owner };
}

async function closeSession(r) {
  const { proxy, process, owner } = await sessionAndHandle(r, "device");
  value(await process.call("write", { h: "a", title: `online ${r.id}`, owner, tier: "global" }), "online write");
  await proxy.hole();
  value(await process.call("write", { h: "a", title: `offline ${r.id}`, owner, tier: "local" }), "offline write");
  const started = process.send("sync", { h: "a" }, 40_000);
  const early = await Promise.race([started.reply, r.sleep(500).then(() => null)]);
  if (early) throw new Broken("the sync didn't wait for the unreachable server", show(early));
  const shut = process.send("sclose", { h: "s" }, 40_000);
  await endedAsClosed(r, started, "the handle's pending sync");
  const reply = await shut.reply;
  r.time("session close", reply.ms);
  value(reply, "session close");
}

async function derivedSignOut(r) {
  const { process, owner } = await sessionAndHandle(r, "device");
  const title = `kept ${r.id}`;
  value(await process.call("write", { h: "a", title, owner, tier: "global" }), "write");
  refusal(await process.call("signout", { h: "a" }), inLang(notSupported, r.lang), "signOut on a session's handle");
  equal(value(await process.call("read", { h: "a", tier: "local" }), "the handle's read"), [title], "the handle closed");
  equal(value(await process.call("ssnap", { h: "s" }), "snapshot").status, "ready", "the session didn't stay signed in");
}

async function follow(r) {
  const token = await r.stack.token(`follow-${r.id}`);
  const { process } = await sessionAndHandle(r, "backend", token);
  const before = value(await process.call("ssnap", { h: "s" }), "snapshot");
  equal(before.status, "ready", "the session didn't sign in");
  value(await process.call("stoken", { h: "s", token: "" }), "the provider gives no token");
  value(await process.call("srefresh", { h: "s" }), "refresh");
  const after = value(await process.call("ssnap", { h: "s" }), "snapshot after");
  equal(after.status, "signed-out", "the session doesn't read signed out");
  if (after.clientEpoch === before.clientEpoch) throw new Broken("clientEpoch didn't change", String(after.clientEpoch));
}

export default [
  {
    id: "close.ends-waits",
    title: "close with a wait pending on an unreachable server: the wait fails as closed, close returns within 3 s",
    basis: "contract",
    rounds: 48,
    jobs: 8,
    idle: true,
    run: endsWaits,
  },
  {
    id: "close.drain",
    title: "close with timeout T returns within T + 3 s; a memory store with unsent writes reports unsynced",
    basis: "contract",
    rounds: 16,
    jobs: 8,
    idle: true,
    notCovered: {
      swift: "Jazz for Swift's close() takes no timeout",
      kotlin: "jazz-kotlin's close() takes no timeout",
    },
    run: drain,
  },
  {
    id: "close.session",
    title: "a session closes while its handle's call is pending: the call fails as closed, never replaced",
    basis: "contract",
    rounds: 16,
    jobs: 8,
    idle: true,
    notCovered: NO_SESSION,
    run: closeSession,
  },
  {
    id: "signout.derived",
    title: "signOut on a session's handle is not supported, and the handle and session stay open",
    basis: "contract",
    rounds: 100,
    notCovered: NO_SESSION,
    run: derivedSignOut,
  },
  {
    id: "signout.follow",
    title: "the token provider gives none: the session reads signed out and clientEpoch changes",
    basis: "contract",
    rounds: 100,
    notCovered: NO_SESSION,
    run: follow,
  },
];
