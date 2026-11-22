// Cancel while the server is unreachable: not decided (cancel.*), so these report NOT_COVERED with
// what was observed. waiting() sets up a call pending on that server, for close.* too.
import { CANCELLED } from "../errors.mjs";
import { Broken, equal, value } from "../round.mjs";

const CANCEL_MS = 2_000;
const MEASURE_MS = 40_000; // past jazz-app's 30 s drain, to tell slow from never
const ROUNDS = 8; // all at once: these rounds wait, they don't compute

export async function waiting(r, { wait, outage }) {
  const proxy = await r.proxy();
  const dir = r.dir();
  const process = await r.process();
  const key = value(await process.call("genkey"), "genkey");
  value(await r.open(process, { dir, proxy, key }), "open");
  const owner = value(await process.call("account", { h: "a" }), "account");
  value(await process.call("write", { h: "a", title: `online ${r.id}`, owner, tier: "global" }), "online write");
  await proxy[outage]();
  const title = `offline ${r.id}`;
  let started;
  if (wait === "write") {
    started = process.send("write", { h: "a", title, owner, tier: "global" }, MEASURE_MS);
  } else {
    value(await process.call("write", { h: "a", title, owner, tier: "local" }), "offline write");
    if (wait === "sync") started = process.send("sync", { h: "a" }, MEASURE_MS);
    if (wait === "read") started = process.send("read", { h: "a", tier: "remote" }, MEASURE_MS);
  }
  if (started) {
    const early = await Promise.race([started.reply, r.sleep(500).then(() => null)]);
    if (early?.ok) throw new Broken(`${wait} didn't wait for the unreachable server: it succeeded`);
    if (early) throw new Broken(`${wait} didn't wait for the unreachable server: ${early.kind}`, early.msg);
  }
  return { proxy, dir, process, key, title, started };
}

async function cancel(r, options) {
  const { process, started, title } = await waiting(r, options);
  const at = Date.now();
  value(await process.call("cancel", { target: started.id }), "cancel");
  const reply = await started.reply;
  const took = Date.now() - at;
  r.time("cancel", took);
  if (reply.ok) throw new Broken("the cancelled wait succeeded with no server");
  if (reply.kind === "harness.timeout") throw new Broken("cancel doesn't end the wait", `still waiting ${took} ms after the cancel`);
  if (reply.kind !== CANCELLED[r.lang]) throw new Broken(`a cancelled wait ends as ${reply.kind}`, reply.msg);
  if (took > CANCEL_MS) throw new Broken(`cancel took over ${CANCEL_MS} ms`, `${took} ms`);
  const local = value(await process.call("read", { h: "a", tier: "local" }), "local read after the cancel");
  equal(local, [`online ${r.id}`, title].sort(), "cancelling the wait lost a write");
}

const rules = [];
for (const outage of ["refuse", "hole"]) {
  const how = outage === "refuse" ? "refused" : "black-holed";
  for (const wait of ["sync", "read", "write"])
    rules.push({
      id: `cancel.${wait}.${outage}`,
      title: `cancel a ${wait} waiting on a ${how} server, within ${CANCEL_MS} ms`,
      basis: "pending",
      rounds: ROUNDS,
      jobs: ROUNDS,
      idle: true,
      run: (r) => cancel(r, { wait, outage }),
    });
}
export default rules;
