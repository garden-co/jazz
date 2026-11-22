// What the contract hasn't decided: run and reported with what happened, never PASS.
import { inLang, storeHeld } from "../errors.mjs";
import { Broken, equal, refusal, value } from "../round.mjs";

async function deleteWhileHeld(r, { otherProcess }) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const holder = await r.process();
  const key = value(await holder.call("genkey"), "genkey");
  value(await r.open(holder, { h: "a", dir, proxy, key }), "open");
  const owner = value(await holder.call("account", { h: "a" }), "account");
  const title = `held ${r.id}`;
  value(await holder.call("write", { h: "a", title, owner, tier: "global" }), "write");
  const other = otherProcess ? await r.process() : holder;
  const held = inLang(storeHeld, r.lang);
  const opened = await r.open(other, { h: "b", dir, proxy, key });
  if (opened.ok) {
    refusal(await other.call("delete", { h: "b" }), held, "delete of a store another app holds");
  } else {
    // With one app per store the only handle that could delete can't be had.
    refusal(opened, held, "open to delete a held store");
  }
  equal(value(await holder.call("read", { h: "a", tier: "local" }), "the holder's read"), [title], "the holder lost its store");
}

async function bearerColdOpen(r) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const token = await r.stack.token(`cold-${r.id}`);
  const one = await r.process();
  value(await r.open(one, { dir, proxy, token }), "online open");
  const owner = value(await one.call("account", { h: "a" }), "account");
  const title = `cold ${r.id}`;
  value(await one.call("write", { h: "a", title, owner, tier: "global" }), "write");
  value(await one.call("close", { h: "a" }), "close");
  await r.endCleanly(one);
  await proxy.refuse();
  const two = await r.process();
  const opened = await r.open(two, { dir, proxy, token });
  r.time("open", opened.ms);
  if (!opened.ok) throw new Broken(`a bearer user's app doesn't open offline: ${opened.kind}`, opened.msg);
  equal(value(await two.call("read", { h: "a", tier: "local" }), "offline read"), [title], "the offline read lacks the write");
}

export default [
  {
    id: "delete.held.same-process",
    title: "delete while another app in the process holds the store",
    basis: "pending",
    rounds: 20,
    run: (r) => deleteWhileHeld(r, { otherProcess: false }),
  },
  {
    id: "delete.held.other-process",
    title: "delete while another process holds the store",
    basis: "pending",
    rounds: 20,
    run: (r) => deleteWhileHeld(r, { otherProcess: true }),
  },
  {
    id: "open.offline.bearer",
    title: "a signed-in bearer user's app opens its store with no server",
    basis: "pending",
    rounds: 8,
    run: bearerColdOpen,
  },
];
