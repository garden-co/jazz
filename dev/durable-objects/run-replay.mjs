// Usage: node run-replay.mjs JOBS.json FAILURES.json
// START/END select a job range; DEBUG_JOB prints each operation of one job.
import fs from "node:fs";
import { Miniflare } from "miniflare";

const [jobsPath, failuresPath] = process.argv.slice(2);
const jobs = JSON.parse(fs.readFileSync(jobsPath));
const create = () =>
  new Miniflare({
    modules: true,
    scriptPath: new URL("./replay.js", import.meta.url).pathname,
    compatibilityDate: "2026-07-01",
    durableObjects: { R: { className: "Replay", useSQLite: true } },
  });
let mf = create();
const failures = [];
const start = Number(process.env.START ?? 0);
const end = Math.min(Number(process.env.END ?? jobs.length), jobs.length);
const save = () => fs.writeFileSync(failuresPath, JSON.stringify(failures, null, 1));
try {
  for (let i = start; i < end; i++) {
    const ops = jobs[i];
    let timer;
    try {
      const errors = await Promise.race([
        (async () => {
          const response = await mf.dispatchFetch(`http://replay/job${i}`, {
            method: "POST",
            headers: process.env.DEBUG_JOB === String(i) ? { "x-replay-debug": "1" } : {},
            body: JSON.stringify(ops),
          });
          if (!response.ok)
            throw new Error(`replay HTTP ${response.status}: ${await response.text()}`);
          return response.json();
        })(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error("job exceeded 30 seconds")), 30_000);
        }),
      ]);
      errors.forEach((error, k) => {
        if (error)
          failures.push({
            job: i,
            operation: k,
            kind: ops[k][0],
            sql: ops[k][1],
            params: ops[k][2],
            error,
          });
      });
    } catch (error) {
      failures.push({ job: i, kind: "incomplete", error: String(error) });
      console.error(`job ${i} incomplete: ${error}`);
      save();
      // A stuck SQL cursor prevents further work in this isolate. Each job
      // has independent storage, so a new runtime can check the remaining jobs.
      await mf.dispose();
      mf = create();
    } finally {
      clearTimeout(timer);
    }
    if ((i + 1) % 100 === 0) {
      console.log(`jobs ${i + 1}/${end}, failures ${failures.length}`);
      save();
    }
  }
  save();
  console.log(`jobs ${end - start}, failures ${failures.length}`);
  if (failures.length > 0) process.exitCode = 1;
} finally {
  await mf.dispose();
}
