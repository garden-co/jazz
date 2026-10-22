import { Miniflare } from "miniflare";

const mf = new Miniflare({
  modules: true,
  scriptPath: new URL("./probe.js", import.meta.url).pathname,
  compatibilityDate: "2026-07-01",
  durableObjects: { PROBE: { className: "Probe", useSQLite: true } },
});
const results = await (await mf.dispatchFetch("http://probe/")).json();
for (const [name, value] of Object.entries(results)) console.log(name.padEnd(48), value);
await mf.dispose();
