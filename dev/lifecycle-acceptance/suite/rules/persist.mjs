// Open and persist: what was written is on this device, and then on the server.
import { equal, value } from "../round.mjs";

async function persists(r, { killed }) {
  const [proxy, direct] = [await r.proxy(), await r.proxy()];
  const dir = r.dir();
  const global = `global ${r.id}`;
  const offline = `offline ${r.id}`;
  const both = [global, offline].sort();

  const one = await r.process();
  const key = value(await one.call("genkey"), "genkey");
  value(await r.open(one, { dir, proxy, key }), "process 1 open");
  const owner = value(await one.call("account", { h: "a" }), "process 1 account");
  value(await one.call("write", { h: "a", title: global, owner, tier: "global" }), "process 1 Global write");
  await proxy.refuse();
  value(await one.call("write", { h: "a", title: offline, owner, tier: "local" }), "process 1 offline write");
  if (killed) {
    await r.killed(one); // both writes acknowledged, nothing closed
  } else {
    value(await one.call("close", { h: "a" }), "process 1 close");
    await r.endCleanly(one);
  }

  // Process 2 can't reach the server, so what it reads is this device's. Online would prove nothing.
  const two = await r.process();
  const sent = proxy.forwarded;
  value(await r.open(two, { dir, proxy, key }), "process 2 offline open");
  equal(value(await two.call("account", { h: "a" }), "process 2 account"), owner, "process 2 is another account");
  equal(
    value(await two.call("read", { h: "a", tier: "local" }), "process 2 offline read"),
    both,
    "process 2 doesn't read both writes back offline",
  );
  equal(proxy.forwarded, sent, "process 2 reached the server while it was unreachable");

  // The offline write really was one: the server has only the Global write so far.
  const before = await r.process();
  value(await r.open(before, { dir: r.dir("before"), proxy: direct, key }), "observer open");
  equal(
    value(await before.call("read", { h: "a", tier: "remote" }), "observer read"),
    [global],
    "before the resync the server doesn't have exactly the Global write",
  );
  value(await before.call("close", { h: "a" }), "observer close");
  await r.endCleanly(before);

  await proxy.up();
  value(await two.call("sync", { h: "a" }, 60_000), "process 2 sync once the server is back");
  value(await two.call("close", { h: "a" }), "process 2 close");
  await r.endCleanly(two);

  const three = await r.process();
  value(await r.open(three, { dir: r.dir("fresh"), proxy: direct, key }), "process 3 open");
  equal(
    value(await three.call("read", { h: "a", tier: "remote" }), "process 3 remote read"),
    both,
    "a fresh store doesn't read both writes from the server",
  );
  value(await three.call("close", { h: "a" }), "process 3 close");
  await r.endCleanly(three);
}

export default [
  {
    id: "persist.clean-exit",
    title: "Global and offline writes survive close + exit, are read offline, then sync",
    basis: "brief",
    rounds: 100,
    run: (r) => persists(r, { killed: false }),
  },
  {
    id: "persist.sigkill",
    title: "the same after SIGKILL once both writes were acknowledged",
    basis: "brief",
    rounds: 100,
    run: (r) => persists(r, { killed: true }),
  },
];
