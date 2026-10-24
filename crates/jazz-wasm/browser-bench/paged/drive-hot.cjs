const { chromium } = require("playwright");
const fs = require("fs");
const PROFILE = (s) => require("path").join(require("os").tmpdir(), `jazz-paged-bench-${s}`);
const PATH = "fault.sqlite";
async function session(size, steps) {
  const ctx = await chromium.launchPersistentContext(PROFILE(size), { headless: true, args: ["--enable-blink-features=ForceEagerMeasureMemory"] });
  const page = await ctx.newPage();
  page.on("pageerror", (e) => console.error("pageerror:", e.message));
  page.on("console", (m) => { if (m.type() === "warning" || m.type() === "error") console.error("console:", m.text()); });
  await page.goto("http://127.0.0.1:8767/index.html");
  await page.waitForFunction(() => window.ready === true);
  await page.evaluate(() => window.start(false));
  const out = {};
  for (const [name, fn, ...args] of steps) {
    if (fn === "memory") { out[name] = await page.evaluate(() => window.memory()); continue; }
    const t = Date.now();
    const r = await page.evaluate((m) => window.call(m), { fn, args });
    if (!r.ok) { out[name + "Error"] = r.error; console.error(name, r.error); break; }
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
(async () => {
  const sizes = (process.argv[2] || "s").split(",");
  const hots = (process.argv[3] || "4096,16384,65536").split(",").map(Number);
  const reps = Number(process.argv[4] || 2);
  const outFile = process.argv[5] || "results-hot.json";
  const fs = require("fs");
  const results = fs.existsSync(outFile) ? JSON.parse(fs.readFileSync(outFile)) : {};
  let writeSeed = 600 + Number(process.env.SEEDOFF || 0);
  for (const size of sizes) {
    const cfg = SIZES[size];
    results[size] = results[size] || { runs: [] };
    for (const hot of hots) {
      const rA = 3 % cfg.rooms, rB = 7 % cfg.rooms, docB = 1 % cfg.logs;
      const warm = await session(size, [
        ["open", "pagedBenchOpen", PATH, "paged", hot, 0, hot, "mru"],
        ["doc", "pagedBenchDoc", 0],
        ["room", "pagedBenchRoom", rA],
        ["points", "pagedBenchPoints", 200, cfg.messages, 11],
        ["writes", "pagedBenchWrites", 50, cfg.messages, cfg.rooms, writeSeed++],
        ["close", "pagedBenchClose", 4096],
      ]);
      for (let rep = 0; rep < reps; rep++) {
        const r = await session(size, [
          ["open", "pagedBenchOpen", PATH, "paged", hot, 0, hot, "mru"],
          ["memOpen", "memory"],
          ["docHot", "pagedBenchDoc", 0],
          ["roomHot", "pagedBenchRoom", rA],
          ["pointsHot", "pagedBenchPoints", 200, cfg.messages, 11],
          ["roomCold", "pagedBenchRoom", rB + rep * 10],
          ["pointsCold", "pagedBenchPoints", 200, cfg.messages, 97 + rep],
          ["docCold", "pagedBenchDoc", (docB + rep) % cfg.logs],
          ["writes", "pagedBenchWrites", 100, cfg.messages, cfg.rooms, writeSeed++],
          ["memEnd", "memory"],
          ["close", "pagedBenchClose", 4096],
        ]);
        const run = { size, hot, rep, warmErrors: Object.keys(warm).filter((k) => k.endsWith("Error")).map((k) => warm[k]), ...r };
        results[size].runs.push(run);
        console.log(JSON.stringify({ size, hot, rep, open: r.open?.openMs, attach: r.open?.attachMs, hotPages: r.open?.hotPages, docHot: r.docHot?.docMs, docHotR: r.docHot?.docRounds, p1Hot: r.roomHot?.page1Ms, p1HotR: r.roomHot?.page1Rounds, ptHot: r.pointsHot?.coldP50Ms, p1Cold: r.roomCold?.page1Ms, ptCold: r.pointsCold?.coldP50Ms, docCold: r.docCold?.docMs, w: r.writes?.writeP50Ms, mem: r.memEnd?.totalBytes, err: Object.keys(r).filter((k) => k.endsWith("Error")).map((k) => r[k]), warmErr: run.warmErrors }));
        fs.writeFileSync(outFile, JSON.stringify(results, null, 1));
      }
    }
  }
})();
