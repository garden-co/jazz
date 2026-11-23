// store.per-account (awaiting Anselm, relayed 22:41 UTC): each account's store is its own. A signs
// out, B opens the same dir: B sees none of A's rows, and A's pending writes upload only as A.
// The rule's shape: stores live in <dataDir>/<account>/ (owner #jazz-ffi-core, on jazz#6269).
import { Broken, equal, value } from "../round.mjs";

async function identity(r, process, kind, name) {
  if (kind === "device") return { key: value(await process.call("genkey"), "genkey") };
  return { token: await r.stack.token(`${name}-${r.id}`) };
}

/** The plain client, in all four languages; for Kotlin, sign out and then a new open is the only way. */
async function plainClient(r) {
  const kind = r.index % 2 ? "bearer" : "device";
  const [proxy, direct] = [await r.proxy(), await r.proxy()];
  const dir = r.dir();
  const one = await r.process();
  const a = await identity(r, one, kind, "a");
  const b = await identity(r, one, kind, "b");
  value(await r.open(one, { dir, proxy, ...a }), "A's open");
  const accountA = value(await one.call("account", { h: "a" }), "A's account");
  const synced = `A's synced ${r.id}`;
  const pending = `A's pending ${r.id}`;
  value(await one.call("write", { h: "a", title: synced, owner: accountA, tier: "global" }), "A's write");
  await proxy.refuse();
  value(await one.call("write", { h: "a", title: pending, owner: accountA, tier: "local" }), "A's offline write");
  value(await one.call("signout", { h: "a" }), "A signs out");
  await r.endCleanly(one);

  // B, on the same dir. A device account opens offline, so its first read is only this device's.
  if (kind === "bearer") await proxy.up();
  const two = await r.process();
  value(await r.open(two, { dir, proxy, ...b }), "B's open on A's dir");
  const accountB = value(await two.call("account", { h: "a" }), "B's account");
  if (accountB === accountA) throw new Broken("B opened as A's account");
  const offline = value(await two.call("read", { h: "a", tier: "local" }), "B's local read");
  if (offline.length) throw new Broken("B reads A's rows", JSON.stringify(offline.map((t) => t.split(" ")[1])));
  await proxy.up();
  const synced2 = await two.call("sync", { h: "a" }, 20_000);
  if (!synced2.ok) throw new Broken(`B's sync fails: ${synced2.kind}`, synced2.msg);
  equal(value(await two.call("read", { h: "a", tier: "remote" }), "B's remote read"), [], "B's server rows aren't empty");
  value(await two.call("close", { h: "a" }), "B's close");
  await r.endCleanly(two);

  // A again: the pending write is A's, kept, and goes up as A.
  const three = await r.process();
  value(await r.open(three, { dir, proxy, ...a }), "A's open again");
  equal(value(await three.call("read", { h: "a", tier: "local" }), "A's local read"), [synced, pending].sort(), "A's rows aren't all there for A");
  value(await three.call("sync", { h: "a" }, 20_000), "A's sync");
  value(await three.call("close", { h: "a" }), "A's close");
  await r.endCleanly(three);
  const fresh = await r.process();
  value(await r.open(fresh, { dir: r.dir("fresh"), proxy: direct, ...a }), "A on a fresh store");
  equal(value(await fresh.call("read", { h: "a", tier: "remote" }), "A's server rows"), [synced, pending].sort(), "A's pending write didn't reach the server as A");
}

async function session(r) {
  const device = r.index % 2 === 0;
  const restart = r.index % 4 < 2;
  const proxy = await r.proxy();
  const args = { h: "s", dir: r.dir(), url: proxy.url, app: r.stack.appId };
  const tokenA = device ? "" : await r.stack.token(`a-${r.id}`);
  const tokenB = device ? "" : await r.stack.token(`b-${r.id}`);
  let process = await r.process();
  value(await process.call("sopen", { ...args, start: device ? "device" : "backend", token: tokenA }), "session open");
  value(await process.call("sjazz", { h: "s", as: "a" }), "A's client");
  const accountA = value(await process.call("account", { h: "a" }), "A's account");
  value(await process.call("write", { h: "a", title: `A's ${r.id}`, owner: accountA, tier: "global" }), "A's write");
  value(await process.call("ssignout", { h: "s" }), "A signs out");
  if (restart) {
    value(await process.call("sclose", { h: "s" }), "session close");
    await r.endCleanly(process);
    process = await r.process();
    value(await process.call("sopen", { ...args, start: "signed-out", token: tokenB }), "session open after the restart");
  } else {
    value(await process.call("stoken", { h: "s", token: tokenB }), "the app's own sign-in");
  }
  value(await process.call(device ? "screate" : "srefresh", { h: "s" }), "B signs in");
  value(await process.call("sjazz", { h: "s", as: "b" }), "B's client");
  if (value(await process.call("account", { h: "b" }), "B's account") === accountA) throw new Broken("B signed in as A");
  await proxy.refuse();
  const local = value(await process.call("read", { h: "b", tier: "local" }), "B's local read");
  if (local.length) throw new Broken("B reads A's rows");
}

async function rootStore(r) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const one = await r.process();
  const key = value(await one.call("genkey"), "genkey");
  value(await r.open(one, { dir, proxy, key }), "A's open");
  const accountA = value(await one.call("account", { h: "a" }), "A's account");
  value(await one.call("write", { h: "a", title: `A's ${r.id}`, owner: accountA, tier: "global" }), "A's write");
  value(await one.call("close", { h: "a" }), "A's close");
  await r.endCleanly(one);
  const two = await r.process();
  const opened = await two.call("open", { h: "a", dir, app: r.stack.appId, local: true });
  // With per-account stores (jazz#6275) such an open is refused; reading nothing is fine too.
  if (!opened.ok) {
    if (!["Refused", "refused", "StoreHeld"].includes(opened.kind)) throw new Broken(`an open with no account fails as ${opened.kind}`, opened.msg);
    return;
  }
  const rows = value(await two.call("read", { h: "a", tier: "local" }), "its local read");
  if (rows.length) throw new Broken("an open with no account reads a signed-in user's rows");
}

export default [
  {
    id: "store.per-account",
    title: "plain client, device and bearer: A signs out, B opens the same dir, sees none of A's rows; A's pending writes upload only as A",
    basis: "contract", // decided for beta.1: Anselm's option B (relayed 03:05 UTC)
    rounds: 100,
    run: plainClient,
  },
  {
    id: "store.per-account.session",
    title: "JazzSession, device and bearer, with and without a restart: B sees none of A's rows",
    basis: "contract", // decided for beta.1: Anselm's option B (relayed 03:05 UTC)
    rounds: 100,
    notCovered: { kotlin: "jazz-kotlin has no JazzSession: store.per-account is its sign out, then a new open" },
    run: session,
  },
  {
    id: "store.per-account.root",
    title: "an open with no account is refused the store a signed-in user wrote in that dir, or reads none of it",
    basis: "contract", // decided for beta.1: Anselm's option B (relayed 03:05 UTC)
    rounds: 20,
    run: rootStore,
  },
];
