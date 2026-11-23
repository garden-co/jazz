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

async function sessionSecondOpen(r) {
  const proxy = await r.proxy();
  const args = { dir: r.dir(), url: proxy.url, app: r.stack.appId, start: "device" };
  const process = await r.process();
  value(await process.call("sopen", { ...args, h: "first" }), "first session");
  value(await process.call("sjazz", { h: "first", as: "a" }), "first session's client");
  const account = value(await process.call("account", { h: "a" }), "account");
  // A session's open reports a failed sign-in in its snapshot, so either is how StoreHeld can arrive.
  const second = await process.call("sopen", { ...args, h: "second" });
  const held = inLang(storeHeld, r.lang);
  if (!second.ok) {
    refusal(second, held, "a second session on the dir");
  } else {
    const snapshot = value(await process.call("ssnap", { h: "second" }), "second session's snapshot");
    if (snapshot.status !== "error") throw new Broken(`a second session on the dir is ${snapshot.status}`);
    const kind = snapshot.error?.kind;
    if (kind !== "storeHeld") throw new Broken(`a second session fails with ${kind}, expected storeHeld`, snapshot.error?.message);
    value(await process.call("sclose", { h: "second" }), "second session's close");
  }
  value(await process.call("sclose", { h: "first" }), "first session's close");
  // The refusal mustn't cost the device account.
  value(await process.call("sopen", { ...args, h: "again" }), "session open afterwards");
  value(await process.call("sjazz", { h: "again", as: "b" }), "client afterwards");
  equal(value(await process.call("account", { h: "b" }), "account afterwards"), account, "the device account changed");
}

/**
 * A cancelled open gives the store back: the same user's next open in this process succeeds.
 * Even rounds bring the network back and open at once: the cancelled attempt then races the next
 * open (on beta.0, Go's background open meets it as `database is locked`). Odd rounds keep the
 * cancelled attempt on the dead connection and open through a live one: a store it still holds
 * shows deterministically, on a build that holds stores exclusively.
 */
async function cancelledOpen(r) {
  const stuck = r.index % 2 === 1;
  const proxy = await r.proxy();
  const live = stuck ? await r.proxy() : proxy;
  const dir = r.dir();
  const token = await r.stack.token(`cancel-${r.id}`);
  const process = await r.process();
  await proxy.hole(); // the open waits on its sign-in
  const opening = process.send("open", r.openArgs({ h: "a", dir, proxy, token }), 40_000);
  await r.sleep(300);
  value(await process.call("cancel", { target: opening.id }), "cancel");
  const ended = await opening.reply;
  r.time("cancelled open ends", ended.ms - 300);
  if (ended.ok) r.note("the cancelled open completed anyway");
  if (!stuck) await proxy.up();
  const again = await r.open(process, { h: "b", dir, proxy: live, token });
  r.time("next open", again.ms);
  value(again, stuck ? "the next open, the cancelled one still stuck" : "the next open, the network back");
  if (again.ms > 5_000) throw new Broken("the next open waited over 5 s for the cancelled one", `${again.ms} ms`);
  value(await process.call("close", { h: "b" }), "close");
}

export default [
  {
    id: "cancel.no-leak",
    title: "a cancelled open gives the store back: the next open in the process succeeds within 5 s",
    basis: "contract",
    rounds: 48,
    jobs: 8,
    idle: true,
    run: cancelledOpen,
  },
  // Decided (Anselm approved the lifecycle card's rules 1-4, 23:09 UTC) but not built here: no PASS
  // over them until they are.
  ...["close.dns", "close.token-callback"].map((id) => ({
    id,
    title: `${id}: the contract's rule`,
    basis: "contract",
    rounds: 1,
    external: true,
    notCovered: { all: "decided, not built in this harness yet" },
  })),
  {
    id: "close.main-thread",
    title: "close.main-thread: the contract's rule (awaiting Anselm)",
    basis: "pending",
    rounds: 1,
    notCovered: { all: "awaiting Anselm, and not built in this harness yet" },
  },
  {
    id: "store.second-session",
    title: "a second JazzSession on a held dir reports storeHeld and keeps the device account",
    basis: "pending",
    rounds: 20,
    notCovered: { kotlin: "jazz-kotlin has no JazzSession" },
    run: sessionSecondOpen,
  },
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
    id: "offline.bearer-open",
    title: "a bearer user reopens a persistent store offline and reads their data",
    basis: "contract",
    rounds: 8,
    run: bearerColdOpen,
  },
];
