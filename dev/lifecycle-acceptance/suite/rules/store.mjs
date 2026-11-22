// One open app per store (jazz#6269), awaiting Anselm: run, reported NOT_COVERED with what was seen.
import { inLang, storeHeld } from "../errors.mjs";
import { Broken, equal, refusal, show, value } from "../round.mjs";

async function holder(r) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const first = await r.process();
  const key = value(await first.call("genkey"), "genkey");
  return { proxy, dir, first, key };
}

async function secondOpen(r, { otherProcess }) {
  const { proxy, dir, first, key } = await holder(r);
  value(await r.open(first, { h: "a", dir, proxy, key }), "first open");
  const owner = value(await first.call("account", { h: "a" }), "account");
  const second = otherProcess ? await r.process() : first;
  refusal(await r.open(second, { h: "b", dir, proxy, key }), inLang(storeHeld, r.lang), "second open of a held store");
  const title = `holder ${r.id}`;
  value(await first.call("write", { h: "a", title, owner, tier: "global" }), "the holder's write after the refusal");
  value(await first.call("close", { h: "a" }), "the holder's close");
  value(await r.open(second, { h: "b", dir, proxy, key }), "open once the holder closed");
  equal(value(await second.call("read", { h: "b", tier: "local" }), "read after taking over"), [title], "the holder's write is gone");
  value(await second.call("close", { h: "b" }), "close");
}

async function simultaneousOpens(r, { otherProcess }) {
  const { proxy, dir, first, key } = await holder(r);
  const second = otherProcess ? await r.process() : first;
  const replies = await Promise.all([
    r.open(first, { h: "a", dir, proxy, key }),
    r.open(second, { h: "b", dir, proxy, key }),
  ]);
  const held = inLang(storeHeld, r.lang);
  const opened = replies.filter((reply) => reply.ok).length;
  if (opened === 2) throw new Broken("two simultaneous opens of one store both succeeded");
  if (opened === 0) throw new Broken("two simultaneous opens both failed", replies.map(show).join(" | "));
  refusal(replies.find((reply) => !reply.ok), held, "the losing open");
  const [process, h] = replies[0].ok ? [first, "a"] : [second, "b"];
  value(await process.call("read", { h, tier: "local" }), "the winner's read");
  value(await process.call("close", { h }), "the winner's close");
}

/** The bug that made the contract: two apps on one store both write. */
async function twoWriters(r) {
  const { proxy, dir, first, key } = await holder(r);
  value(await r.open(first, { h: "a", dir, proxy, key }), "first open");
  const owner = value(await first.call("account", { h: "a" }), "account");
  const second = await r.open(first, { h: "b", dir, proxy, key });
  const handles = second.ok ? ["a", "b"] : ["a"];
  if (!second.ok) refusal(second, inLang(storeHeld, r.lang), "second app's open");
  const titles = handles.flatMap((h) => Array.from({ length: 10 }, (_, i) => `${h}${i} ${r.id}`));
  const replies = await Promise.all(
    titles.map((title) => first.call("write", { h: title[0], title, owner, tier: "global" }, 15_000)),
  );
  for (const [i, reply] of replies.entries()) {
    if (reply.ok) continue;
    const locked = /database is locked/.test(reply.msg) ? " (database is locked)" : "";
    throw new Broken(`an app's write fails: ${reply.kind}${locked}`, `${titles[i]}: ${reply.msg}`);
  }
  for (const h of handles) value(await first.call("close", { h }), "close");
  const fresh = await r.process();
  value(await r.open(fresh, { dir: r.dir("fresh"), proxy, key }), "fresh open");
  equal(
    value(await fresh.call("read", { h: "a", tier: "remote" }), "fresh read"),
    titles.sort(),
    "acknowledged writes are missing on the server",
  );
}

async function reopen(r) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const first = await r.process();
  const key = value(await first.call("genkey"), "genkey");
  const variant = ["after close", "while the holder stops", "after the holder was killed"][r.index % 3];
  if (variant === "after close") {
    value(await r.open(first, { dir, proxy, key }), "open");
    value(await first.call("close", { h: "a" }), "close");
    const again = await r.open(first, { h: "b", dir, proxy, key });
    r.time("reopen after close", again.ms);
    value(again, "reopen right after close returned");
  } else if (variant === "while the holder stops") {
    value(await r.open(first, { dir, proxy, key }), "open");
    const owner = value(await first.call("account", { h: "a" }), "account");
    await proxy.hole();
    value(await first.call("write", { h: "a", title: `x ${r.id}`, owner, tier: "local" }), "offline write");
    first.send("sync", { h: "a" }, 60_000);
    await r.sleep(300);
    first.send("close", { h: "a" }, 60_000); // stopping: its wait is pending
    await r.sleep(100);
    const again = await r.open(first, { h: "b", dir, proxy, key });
    r.time("open while the holder stops", again.ms);
    if (again.ms > 5_500) throw new Broken("an open waited over 5 s for a stopping holder", show(again));
    // Refused after the wait is within the rule; nothing else is.
    if (!again.ok) refusal(again, inLang(storeHeld, r.lang), "open while the holder stops");
  } else {
    value(await r.open(first, { dir, proxy, key }), "open");
    await r.killed(first);
    const next = await r.process();
    const again = await r.open(next, { dir, proxy, key });
    r.time("open after the holder was killed", again.ms);
    value(again, "open after the holder was killed");
  }
}

export default [
  {
    id: "store.second-open",
    title: "a second open, in this process or another, is refused; the first still works; open after it closes",
    basis: "pending",
    rounds: 100,
    run: (r) => secondOpen(r, { otherProcess: r.index % 2 === 1 }),
  },
  {
    id: "store.second-open.race",
    title: "of two simultaneous opens, in one process or two, exactly one wins",
    basis: "pending",
    rounds: 100,
    run: (r) => simultaneousOpens(r, { otherProcess: r.index % 2 === 1 }),
  },
  {
    id: "store.no-two-writers",
    title: "two apps write: one open is refused, and every acknowledged write reaches the server",
    basis: "pending",
    rounds: 100,
    run: twoWriters,
  },
  {
    id: "store.reopen",
    title: "reopen after close at once; while a holder stops within 5 s; after a killed holder",
    basis: "pending",
    rounds: 30,
    jobs: 6,
    run: reopen,
  },
];
