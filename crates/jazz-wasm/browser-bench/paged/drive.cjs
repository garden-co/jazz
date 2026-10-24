const { chromium } = require("playwright");
const fs = require("fs");
const PROFILE = (s) => require("path").join(require("os").tmpdir(), `jazz-paged-bench-${s}`);
const PATH = "fault.sqlite";
async function session(size, steps) {
  const ctx = await chromium.launchPersistentContext(PROFILE(size), {
    headless: true,
    args: ["--enable-blink-features=ForceEagerMeasureMemory"],
  });
  const page = await ctx.newPage();
  page.on("pageerror", (e) => console.error("pageerror:", e.message));
  page.on("console", (m) => {
    if (m.type() === "warning" || m.type() === "error") console.error("console:", m.text());
  });
  await page.goto("http://127.0.0.1:8767/index.html");
  await page.waitForFunction(() => window.ready === true);
  await page.evaluate(() => window.start(false));
  const out = {};
  for (const [name, fn, ...args] of steps) {
    if (fn === "memory") {
      out[name] = await page.evaluate(() => window.memory());
      continue;
    }
    const t = Date.now();
    const r = await page.evaluate((m) => window.call(m), { fn, args });
    if (!r.ok) {
      out[name + "Error"] = r.error;
      console.error(name, r.error);
      break;
    }
    out[name] = { ...r.out, wallMs: Date.now() - t };
  }
  await ctx.close();
  return out;
}
const SIZES = {
  s: { messages: 500, rooms: 10, logs: 1 },
  m: { messages: 60000, rooms: 200, logs: 5 },
  l: { messages: 600000, rooms: 2000, logs: 40 },
};
const MODES = [
  ["preload", 0, 0],
  ["paged", 1024, 0],
  ["paged", 4096, 0],
  ["paged", 16384, 0],
  ["paged", 0, 0],
  ["paged", 1024, 8],
  ["paged", 4096, 8],
  ["paged", 16384, 8],
  ["paged", 0, 8],
];
(async () => {
  const sizes = (process.argv[2] || "s").split(",");
  const reps = Number(process.argv[3] || 3);
  const modes = process.argv[4]
    ? MODES.filter((_, i) => process.argv[4].split(",").includes(String(i)))
    : MODES;
  const outFile = process.argv[5] || "results.json";
  const results = fs.existsSync(outFile) ? JSON.parse(fs.readFileSync(outFile)) : {};
  for (const size of sizes) {
    const cfg = SIZES[size];
    results[size] = results[size] || { runs: [] };
    if (!results[size].seed || process.env.RESEED) {
      fs.rmSync(PROFILE(size), { recursive: true, force: true });
      const t = Date.now();
      results[size].seed = await session(size, [
        ["seed", "seed", PATH, cfg.messages, cfg.rooms, cfg.logs],
      ]);
      console.log(size, "seed", (Date.now() - t) / 1000, "s", JSON.stringify(results[size].seed));
      fs.writeFileSync(outFile, JSON.stringify(results, null, 1));
    }
    for (let rep = 0; rep < reps; rep++) {
      for (const [mode, budget, readahead] of modes) {
        const seed = rep * 100 + 7;
        const r = await session(size, [
          ["open", "pagedBenchOpen", PATH, mode, budget, readahead, 0, "sieve"],
          ["memOpen", "memory"],
          ["doc", "pagedBenchDoc", rep % cfg.logs],
          ["room", "pagedBenchRoom", (seed * 13) % cfg.rooms],
          ["points", "pagedBenchPoints", 200, cfg.messages, seed],
          [
            "writes",
            "pagedBenchWrites",
            100,
            cfg.messages,
            cfg.rooms,
            results[size].runs.length + 1 + Number(process.env.SEEDOFF || 0),
          ],
          ...(process.env.NOCOUNT ? [] : [["count", "pagedBenchCount"]]),
          ["memEnd", "memory"],
          ["close", "pagedBenchClose", 0],
        ]);
        const run = { size, mode, budget, readahead, rep, ...r };
        results[size].runs.push(run);
        console.log(
          JSON.stringify({
            size,
            mode,
            budget,
            readahead,
            rep,
            open: r.open?.openMs,
            doc: r.doc?.docMs,
            docR: r.doc?.docRounds,
            p1: r.room?.page1Ms,
            p2: r.room?.page2Ms,
            cold: r.points?.coldP50Ms,
            coldR: r.points?.coldRoundsPerOp,
            warm: r.points?.warmP50Ms,
            w: r.writes?.writeP50Ms,
            wR: r.writes?.writeRoundsPerOp,
            count: r.count?.messages,
            sw: r.count?.swallowed,
            mem: r.memEnd?.totalBytes,
            err: Object.keys(r)
              .filter((k) => k.endsWith("Error"))
              .map((k) => r[k]),
          }),
        );
        fs.writeFileSync(outFile, JSON.stringify(results, null, 1));
      }
    }
  }
})();
