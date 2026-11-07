// Usage: node run-replay.mjs JOBS.json FAILURES.json
import fs from "node:fs";
import { Miniflare } from "miniflare";

const [jobsPath, failuresPath] = process.argv.slice(2);
const jobs = JSON.parse(fs.readFileSync(jobsPath));
const mf = new Miniflare({
  modules: true,
  scriptPath: new URL("./replay.js", import.meta.url).pathname,
  compatibilityDate: "2026-07-01",
  durableObjects: { R: { className: "Replay", useSQLite: true } },
});
const failures = [];
for (const [i, ops] of jobs.entries()) {
  const response = await mf.dispatchFetch(`http://replay/job${i}`, {
    method: "POST",
    body: JSON.stringify(ops),
  });
  (await response.json()).forEach((error, k) => {
    if (error) failures.push({ kind: ops[k][0], sql: ops[k][1], params: ops[k][2], error });
  });
}
fs.writeFileSync(failuresPath, JSON.stringify(failures, null, 1));
console.log(`jobs ${jobs.length}, failing statements ${failures.length}`);
await mf.dispose();
