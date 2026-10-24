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
const pick = (r) => ({ open: r.open?.openMs, attach: r.open?.attachMs, recordCounts: r.open?.recordCounts, hotPages: r.open?.hotPages, mem: r.memEnd?.totalBytes, hintBytes: r.close?.hintBytes, err: Object.keys(r).filter((k) => k.endsWith("Error")).map((k) => r[k]) });
(async () => {
  const mode = process.argv[2];
  const sizes = (process.argv[3] || "m,l").split(",");
  const policies = (process.argv[4] || "mru,lfu,lfu-slow").split(",");
  const hots = (process.argv[5] || "65536").split(",").map(Number);
  const outFile = process.argv[6];
  const results = fs.existsSync(outFile) ? JSON.parse(fs.readFileSync(outFile)) : {};
  let writeSeed = 800 + Number(process.env.SEEDOFF || 0);
  for (const size of sizes) {
    const cfg = SIZES[size];
    for (const policy of policies) for (const hot of hots) {
      const key = `${size} ${policy} ${hot / 1024}MB`;
      const runs = (results[key] = []);
      const open = ["open", "pagedBenchOpen", PATH, "paged", hot, 0, hot, policy];
      const close = ["close", "pagedBenchClose", 4096];
      const hotOps = [["doc", "pagedBenchDoc", 0], ["room", "pagedBenchRoom", 3 % cfg.rooms], ["points", "pagedBenchPoints", 200, cfg.messages, 11]];
      await session(size, [["clear", "clearHint", PATH]]);
      if (mode === "pollution") {
        const plan = [
          ["warm1", hotOps], ["warm2", hotOps],
          ["pollute", [["doc", "pagedBenchDoc", 1 % cfg.logs], ["points", "pagedBenchPoints", 200, cfg.messages, 97]]],
          ["reopen", hotOps],
        ];
        for (const [name, ops] of plan) {
          const r = await session(size, [open, ...ops, ["memEnd", "memory"], close]);
          const last = r.points || r.doc || {};
          const row = { name, ...pick(r), docMs: r.doc?.docMs, docRounds: r.doc?.docRounds, roomMs: r.room?.page1Ms, roomRounds: r.room?.page1Rounds, pointP50: r.points?.coldP50Ms, pointRounds: r.points?.coldRoundsPerOp, hintPages: last.hintPages, hintUsed: last.hintPagesUsed, touched: last.pagesTouched, raw: r };
          runs.push(row);
          console.log(key, JSON.stringify({ ...row, raw: undefined }));
          fs.writeFileSync(outFile, JSON.stringify(results, null, 1));
        }
      } else {
        const warm = await session(size, [open, ...hotOps, ["writes", "pagedBenchWrites", 50, cfg.messages, cfg.rooms, writeSeed++], close]);
        for (let rep = 0; rep < 2; rep++) {
          const r = await session(size, [
            open, ["memOpen", "memory"],
            ["docHot", "pagedBenchDoc", 0], ["roomHot", "pagedBenchRoom", 3 % cfg.rooms], ["pointsHot", "pagedBenchPoints", 200, cfg.messages, 11],
            ["roomCold", "pagedBenchRoom", (7 + rep * 10) % cfg.rooms], ["pointsCold", "pagedBenchPoints", 200, cfg.messages, 97 + rep],
            ["docCold", "pagedBenchDoc", (1 + rep) % cfg.logs],
            ["writes", "pagedBenchWrites", 100, cfg.messages, cfg.rooms, writeSeed++],
            ["memEnd", "memory"], close,
          ]);
          const row = { rep, ...pick(r), warmErr: pick(warm).err, docHot: r.docHot?.docMs, docHotR: r.docHot?.docRounds, p1Hot: r.roomHot?.page1Ms, ptHot: r.pointsHot?.coldP50Ms, p1Cold: r.roomCold?.page1Ms, ptCold: r.pointsCold?.coldP50Ms, docCold: r.docCold?.docMs, w: r.writes?.writeP50Ms, hintPages: r.pointsHot?.hintPages, hintUsedAfterHot: r.pointsHot?.hintPagesUsed, touchedAfterHot: r.pointsHot?.pagesTouched, raw: r };
          runs.push(row);
          console.log(key, JSON.stringify({ ...row, raw: undefined }));
          fs.writeFileSync(outFile, JSON.stringify(results, null, 1));
        }
      }
    }
  }
})();
