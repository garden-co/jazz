const r = JSON.parse(require("fs").readFileSync(process.argv[2] || "results.json"));
const med = (a) => {
  a = a.filter((x) => x != null && !Number.isNaN(x)).sort((x, y) => x - y);
  return a.length ? a[Math.floor((a.length - 1) / 2)] : NaN;
};
const f = (x, d = 1) => (Number.isNaN(x) ? "–" : x.toFixed(d));
for (const size of Object.keys(r)) {
  const s = r[size];
  console.log(`\n## ${size}: file ${(s.seed.seed.fileBytes / 1048576).toFixed(1)} MB`);
  const groups = {};
  for (const run of s.runs) {
    const k =
      run.mode === "preload"
        ? "preload"
        : `paged ${run.budget ? run.budget / 1024 + " MB" : "unlimited"} ra${run.readahead}`;
    (groups[k] = groups[k] || []).push(run);
  }
  console.log(
    "| mode | open ms | open rounds | doc ms | doc rounds | room p1 ms | p1 rounds | room p2 ms | point cold p50/p95 | cold rounds/op | point warm p50/p95 | write p50/p95 | write rounds/op | write txs/op | resident MB (peak) | page+worker MB | wasm MB | errors | swallowed |",
  );
  console.log("|---|" + "---|".repeat(18));
  for (const [k, runs] of Object.entries(groups)) {
    const g = (fn) =>
      med(
        runs.map((x) => {
          try {
            return fn(x);
          } catch {
            return NaN;
          }
        }),
      );
    const errs = runs.flatMap((x) =>
      Object.keys(x)
        .filter((k) => k.endsWith("Error"))
        .map((k) => k + ": " + x[k]),
    );
    console.log(
      `| ${k} (${runs.length}) | ${f(g((x) => x.open.openMs))} | ${f(
        g((x) => x.open.openRounds ?? 0),
        0,
      )} | ${f(g((x) => x.doc.docMs))} | ${f(
        g((x) => x.doc.docRounds),
        0,
      )} | ${f(g((x) => x.room.page1Ms))} | ${f(
        g((x) => x.room.page1Rounds),
        0,
      )} | ${f(g((x) => x.room.page2Ms))} | ${f(
        g((x) => x.points.coldP50Ms),
        2,
      )} / ${f(
        g((x) => x.points.coldP95Ms),
        2,
      )} | ${f(
        g((x) => x.points.coldRoundsPerOp),
        2,
      )} | ${f(
        g((x) => x.points.warmP50Ms),
        2,
      )} / ${f(
        g((x) => x.points.warmP95Ms),
        2,
      )} | ${f(
        g((x) => x.writes.writeP50Ms),
        2,
      )} / ${f(
        g((x) => x.writes.writeP95Ms),
        2,
      )} | ${f(
        g((x) => x.writes.writeRoundsPerOp),
        2,
      )} | ${f(
        g((x) => x.writes.writeIdbTxsPerOp),
        2,
      )} | ${f(g((x) => (x.count || x.writes).residentBytes / 1048576))} (${f(g((x) => (x.count || x.writes).peakResidentBytes / 1048576))}) | ${f(g((x) => x.memEnd.totalBytes / 1048576))} | ${f(g((x) => (x.count || x.writes).wasmMemoryBytes / 1048576))} | ${errs.length ? errs.join("; ") : 0} | ${f(
        g((x) => (x.count || x.writes).swallowed),
        0,
      )} |`,
    );
  }
}
