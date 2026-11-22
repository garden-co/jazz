// Sign out, restart, sign in again as the same user: their data is there.
// Both surfaces, with both kinds of sign-in: the plain client, and JazzSession where there is one.
import { Broken, equal, value } from "../round.mjs";

async function identity(r, process, kind, name) {
  if (kind === "device") return { key: value(await process.call("genkey"), "genkey") };
  return { token: await r.stack.token(`${name}-${r.id}`) };
}

async function plainClient(r, { kind }) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const one = await r.process();
  const a = await identity(r, one, kind, "a");
  value(await r.open(one, { dir, proxy, ...a }), "A's open");
  const accountA = value(await one.call("account", { h: "a" }), "A's account");
  const secret = `A's ${r.id}`;
  value(await one.call("write", { h: "a", title: secret, owner: accountA, tier: "global" }), "A's write");
  value(await one.call("signout", { h: "a" }), "A signs out");
  if ((await one.call("read", { h: "a", tier: "local" })).ok)
    throw new Broken("the signed-out client still reads A's data");
  await r.endCleanly(one);

  const two = await r.process();
  // A device account signs in offline, so what it reads is only this device's.
  if (kind === "device") await proxy.refuse();
  value(await r.open(two, { dir, proxy, ...a }), "A's open again");
  await proxy.refuse();
  const account = value(await two.call("account", { h: "a" }), "the account after the restart");
  const local = value(await two.call("read", { h: "a", tier: "local" }), "local read after the restart");
  equal(account, accountA, "A signed in again as another account");
  equal(local, [secret], "A's data isn't there after signing in again");
  await proxy.up();
  const title = `after restart ${r.id}`;
  value(await two.call("write", { h: "a", title, owner: account, tier: "global" }), "a write after signing in");
  equal(
    value(await two.call("read", { h: "a", tier: "remote" }), "remote read after the restart"),
    [secret, title].sort(),
    "A's remote read isn't A's data",
  );
  value(await two.call("close", { h: "a" }), "close");
}

async function session(r, { kind, restart }) {
  const proxy = await r.proxy();
  const args = { h: "s", dir: r.dir(), url: proxy.url, app: r.stack.appId };
  const device = kind === "device";
  const token = device ? "" : await r.stack.token(`a-${r.id}`);
  let process = await r.process();
  value(await process.call("sopen", { ...args, start: device ? "device" : "backend", token }), "session open");
  value(await process.call("sjazz", { h: "s", as: "a" }), "A's client");
  const accountA = value(await process.call("account", { h: "a" }), "A's account");
  const secret = `A's ${r.id}`;
  value(await process.call("write", { h: "a", title: secret, owner: accountA, tier: "global" }), "A's write");
  value(await process.call("ssignout", { h: "s" }), "A signs out");
  equal(value(await process.call("ssnap", { h: "s" }), "snapshot").status, "signed-out", "the session isn't signed out");
  if ((await process.call("read", { h: "a", tier: "local" })).ok)
    throw new Broken("the signed-out session's client still reads A's data");

  if (restart) {
    value(await process.call("sclose", { h: "s" }), "session close");
    await r.endCleanly(process);
    process = await r.process();
    value(await process.call("sopen", { ...args, start: "signed-out", token }), "session open after the restart");
  } else {
    value(await process.call("stoken", { h: "s", token }), "the app's own sign-in");
  }
  const signIn = device ? "sreopen" : "srefresh";
  value(await process.call(signIn, { h: "s" }), "sign in");
  value(await process.call("sjazz", { h: "s", as: "b" }), "the client after signing in");
  await proxy.refuse();
  const account = value(await process.call("account", { h: "b" }), "the account after signing in");
  const local = value(await process.call("read", { h: "b", tier: "local" }), "local read after signing in");
  equal(account, accountA, "A signed in again as another account");
  equal(local, [secret], "A's data isn't there after signing in again");
  await proxy.up();
  const title = `after sign-in ${r.id}`;
  value(await process.call("write", { h: "b", title, owner: account, tier: "global" }), "a write after signing in");
  equal(
    value(await process.call("read", { h: "b", tier: "remote" }), "remote read after signing in"),
    [secret, title].sort(),
    "A's remote read isn't A's data",
  );
  value(await process.call("sclose", { h: "s" }), "session close");
}

const NO_SESSION = {
  kotlin: "jazz-kotlin has no JazzSession: signOut() closes the app and signing in again is a new open (see signout.client.*)",
};
const rules = [];
for (const kind of ["device", "bearer"]) {
  rules.push({
    id: `signout.client.${kind}.same-user`,
    title: `client (${kind}): sign out, restart, sign in on the same dir: the user has their data`,
    basis: "brief",
    rounds: 100,
    run: (r) => plainClient(r, { kind }),
  });
  for (const restart of [true, false])
    rules.push({
      id: `signout.session.${kind}.same-user${restart ? "" : ".no-restart"}`,
      title: `JazzSession (${kind}): sign out${restart ? ", restart" : ""}, sign in: the user has their data`,
      basis: "brief",
      rounds: 100,
      notCovered: NO_SESSION,
      run: (r) => session(r, { kind, restart }),
    });
  for (const surface of ["client", "session"])
    for (const suffix of surface === "client" ? [""] : ["", ".no-restart"])
      rules.push({
        id: `signout.${surface}.${kind}.other-user${suffix}`,
        title: `${surface} (${kind}): sign out, sign in as another user`,
        basis: "brief",
        rounds: 100,
        external: true, // its result has to be supplied: without one the verdict can't be PASS
        notCovered: { all: "not covered by this workflow" },
      });
}
export default rules;
