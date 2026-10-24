// Persisted SIEVE (A) vs aged LFU (B) vs MRU: one or two polluted sessions
// before the hot doc reopens, a session killed mid-way, and a corrupt hint.
const { chromium } = require("playwright");
const fs = require("fs");
const PROFILE = (s) => require("path").join(require("os").tmpdir(), `jazz-paged-bench-${s}`);
const PATH = "fault.sqlite";
const EVERY = Number(process.env.EVERY || 1000);
async function session(size, steps, kill) {
  const ctx = await chromium.launchPersistentContext(PROFILE(size), { headless: true, args: ["--enable-blink-features=ForceEagerMeasureMemory"] });
  const page = await ctx.newPage();
  page.on("pageerror", (e) => console.error("pageerror:", e.message));
  await page.goto("http://127.0.0.1:8767/index.html");
  await page.waitForFunction(() => window.ready === true);
  await page.evaluate(() => window.start(false));
  const out = {};
  for (const [name, fn, ...args] of steps) {
    if (fn === "memory") { out[name] = await page.evaluate(() => window.memory()); continue; }
    if (fn === "sleep") { await new Promise((r) => setTimeout(r, args[0])); continue; }
    const t = Date.now();
    const r = await page.evaluate((m) => window.call(m), { fn, args });
    if (!r.ok) { out[name + "Error"] = r.error; console.error(name, r.error); break; }
    out[name] = { ...r.out, wallMs: Date.now() - t };
  }
  // A kill: the browser goes away with no close and no hint save.
  if (kill) await ctx.browser()?.close().catch(() => {});
  await ctx.close().catch(() => {});
  return out;
}
const SIZES = { m: { messages: 60000, rooms: 200, logs: 5 }, l: { messages: 600000, rooms: 2000, logs: 40 } };
(async () => {
  const plans = (process.argv[2] || "p1,p2").split(",");
  const sizes = (process.argv[3] || "l").split(",");
  const policies = (process.argv[4] || "sieve,lfu,lfu-slow,mru").split(",");
  const hot = Number(process.argv[5] || 65536);
  const outFile = process.argv[6];
  const results = fs.existsSync(outFile) ? JSON.parse(fs.readFileSync(outFile)) : {};
  for (const size of sizes) for (const plan of plans) for (const policy of policies) {
    const cfg = SIZES[size];
    const key = `${size} ${plan} ${policy} ${hot / 1024}MB`;
    const runs = (results[key] = []);
    const timing = ["timing", "pagedBenchHintTiming", EVERY, 3600000];
    const open = ["open", "pagedBenchOpen", PATH, "paged", hot, 0, hot, policy];
    const close = ["close", "pagedBenchClose", 4096];
    const hotOps = [["doc", "pagedBenchDoc", 0], ["room", "pagedBenchRoom", 3 % cfg.rooms], ["points", "pagedBenchPoints", 200, cfg.messages, 11]];
    const pollute = (doc, seed) => [["doc", "pagedBenchDoc", doc % cfg.logs], ["points", "pagedBenchPoints", 200, cfg.messages, seed]];
    await session(size, [["clear", "clearHint", PATH]]);
    const steps = {
      p1: [["warm1", hotOps], ["warm2", hotOps], ["pollute", pollute(1, 97)], ["reopen", hotOps]],
      p2: [["warm1", hotOps], ["warm2", hotOps], ["pollute", pollute(1, 97)], ["pollute2", pollute(2, 98)], ["reopen", hotOps]],
      p4: [["warm1", hotOps], ["warm2", hotOps], ["pollute", pollute(1, 97)], ["pollute2", pollute(2, 98)], ["pollute3", pollute(3, 99)], ["pollute4", pollute(4, 100)], ["reopen", hotOps]],
      p6: [["warm1", hotOps], ["warm2", hotOps], ...[1, 2, 3, 4, 5, 6].map((n) => [`pollute${n}`, pollute(n, 96 + n)]), ["reopen", hotOps]],
      // Killed after the hot ops (and one more operation once the store
      // interval has passed): only periodic stores reach the reopen.
      kill: [["killed", [...hotOps, ["wait", "sleep", EVERY + 200], ["tick", "pagedBenchPoints", 1, cfg.messages, 5]], true], ["reopen", hotOps]],
      corrupt: [["warm1", hotOps], ["corrupt", [], false, true], ["reopen", hotOps]],
    }[plan];
    for (const [name, ops, kill, corrupt] of steps) {
      if (corrupt) { await session(size, [["corrupt", "corruptHint", PATH]]); continue; }
      const r = await session(size, [timing, open, ...ops, ["memEnd", "memory"], ...(kill ? [] : [close])], kill);
      const last = r.tick || r.points || r.doc || {};
      const row = {
        name, open: r.open?.openMs, attach: r.open?.attachMs, recordCounts: r.open?.recordCounts, hotPages: r.open?.hotPages,
        mem: r.memEnd?.totalBytes, err: Object.keys(r).filter((k) => k.endsWith("Error")).map((k) => r[k]),
        docMs: r.doc?.docMs, docRounds: r.doc?.docRounds, roomMs: r.room?.page1Ms, roomRounds: r.room?.page1Rounds,
        pointP50: r.points?.coldP50Ms, pointRounds: r.points?.coldRoundsPerOp,
        hintPages: last.hintPages, hintUsed: last.hintPagesUsed, touched: last.pagesTouched,
        persists: (r.close || last).hintPersists, persistBytes: (r.close || last).hintPersistBytes, soloPersists: (r.close || last).hintSoloPersists, soloMs: (r.close || last).hintSoloMs, buildMs: (r.close || last).hintBuildMs,
        closeBytes: r.close?.hintBytes, raw: r,
      };
      runs.push(row);
      console.log(key, JSON.stringify({ ...row, raw: undefined }));
      fs.writeFileSync(outFile, JSON.stringify(results, null, 1));
    }
  }
  console.log("ALLDONE");
})();
