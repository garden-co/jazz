// One open app per store: a second open is refused with StoreHeld, and the holder goes on working.
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

export default [
  {
    id: "store.second-open.same-process",
    title: "a second open in the same process is refused with StoreHeld",
    basis: "proposed",
    rounds: 100,
    run: (r) => secondOpen(r, { otherProcess: false }),
  },
  {
    id: "store.second-open.other-process",
    title: "a second open in another process is refused with StoreHeld",
    basis: "proposed",
    rounds: 100,
    run: (r) => secondOpen(r, { otherProcess: true }),
  },
  {
    id: "store.open-race.same-process",
    title: "of two simultaneous opens in one process exactly one wins",
    basis: "proposed",
    rounds: 100,
    run: (r) => simultaneousOpens(r, { otherProcess: false }),
  },
  {
    id: "store.open-race.other-process",
    title: "of two simultaneous opens in two processes exactly one wins",
    basis: "proposed",
    rounds: 100,
    run: (r) => simultaneousOpens(r, { otherProcess: true }),
  },
  {
    id: "store.no-two-writers",
    title: "apps on one store never fail each other's writes (database is locked)",
    basis: "proposed",
    rounds: 100,
    run: twoWriters,
  },
];
