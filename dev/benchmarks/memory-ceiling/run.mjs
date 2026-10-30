#!/usr/bin/env node
// Memory ceiling: the most data an example app can hold, in the app's own
// units, before the sync server or a client crosses a fixed memory budget.
// See README.md in this directory.
import {
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  statfsSync,
  writeFileSync,
} from "node:fs";
import { readdirSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { register } from "node:module";
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import { sampleMemory, startServer, startWorker } from "./processes.mjs";
import { summarizeHeap } from "./pprof.mjs";
import { fmt, renderSummary } from "./report.mjs";

const MiB = 1024 * 1024;
const GiB = 1024 * MiB;

export function parseBytes(text) {
  const m = /^(\d+(?:\.\d+)?)\s*(b|k|kb|ki|kib|m|mb|mi|mib|g|gb|gi|gib)?$/i.exec(
    String(text).trim(),
  );
  if (!m) throw new Error(`not a size: ${text}`);
  const unit = (m[2] ?? "b").toLowerCase();
  const scale = unit.startsWith("g")
    ? GiB
    : unit.startsWith("m")
      ? MiB
      : unit.startsWith("k")
        ? 1024
        : 1;
  return Math.round(Number(m[1]) * scale);
}

const { values: args } = parseArgs({
  options: {
    app: { type: "string" },
    scenario: { type: "string", default: "" },
    "server-bin": { type: "string" },
    packages: { type: "string" },
    // The default tenant pod limit (garden-co/infra tenant-manager
    // CORE_BASELINE, limits.memory). Some tenants run with a 4Gi override.
    "server-budget": { type: "string", default: process.env.MEMCEIL_SERVER_BUDGET ?? "1GiB" },
    // One browser tab's Jazz client: see README.md for the reasoning.
    "client-budget": { type: "string", default: process.env.MEMCEIL_CLIENT_BUDGET ?? "512MiB" },
    growth: { type: "string", default: "1.5" },
    start: { type: "string" },
    "max-scale": { type: "string", default: "1000000" },
    "max-minutes": { type: "string", default: "120" },
    bisect: { type: "string", default: "3" },
    crowd: { type: "string", default: "8" },
    churn: { type: "string", default: "3" },
    window: { type: "string", default: "64" },
    writes: { type: "string", default: "import" },
    probe: { type: "string", default: "20" },
    "min-free-disk": { type: "string", default: "1.5GiB" },
    out: { type: "string" },
    "keep-data": { type: "boolean", default: false },
  },
});

if (!args.app) {
  console.error(
    "usage: run.mjs --app <adapter.ts> [--scenario name] [--server-bin path] [--packages node_modules]\n" +
      "       [--server-budget 1GiB] [--client-budget 512MiB] [--growth 1.5] [--out dir]",
  );
  process.exit(2);
}

const packagesDir = args.packages ? resolve(args.packages) : undefined;
if (packagesDir) process.env.JAZZ_MEMCEIL_PACKAGES = packagesDir;
// Load the app's TypeScript schema, and the chosen Jazz build, as workers do.
register(new URL("./resolve-hook.mjs", import.meta.url));
const adapterPath = resolve(args.app);
const adapter = (await import(pathToFileURL(adapterPath).href)).default;
const scenarioNames = args.scenario ? args.scenario.split(",") : Object.keys(adapter.scenarios);
const serverBin = resolve(
  args["server-bin"] ??
    join(
      packagesDir ?? join(dirname(adapterPath), "node_modules"),
      `jazz-tools/bin/native/jazz-tools-${process.platform}-${process.arch}`,
    ),
);
if (!existsSync(serverBin)) throw new Error(`server binary not found: ${serverBin}`);
const budgets = {
  server: parseBytes(args["server-budget"]),
  client: parseBytes(args["client-budget"]),
};
const growth = Number(args.growth);
const maxScale = Number(args["max-scale"]);
const deadline = Date.now() + Number(args["max-minutes"]) * 60_000;
const bisectSteps = Number(args.bisect);
const crowdSize = Number(args.crowd);
const minFreeDisk = parseBytes(args["min-free-disk"]);
const outRoot = resolve(args.out ?? join(process.cwd(), `memory-ceiling-${adapter.name}`));
mkdirSync(outRoot, { recursive: true });

const { deploy } = await import("jazz-tools/dev");
const jazzVersion = JSON.parse(
  readFileSync(new URL(import.meta.resolve("jazz-tools/package.json")), "utf8"),
).version;

const appId = "00000000-0000-4000-8000-00000000ce11";
const adminSecret = "memory-ceiling-admin";
const backendSecret = "memory-ceiling-backend";

function freeDisk(path) {
  const s = statfsSync(path);
  return s.bavail * s.bsize;
}

function dirBytes(path) {
  let total = 0;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    const p = join(path, entry.name);
    total += entry.isDirectory() ? dirBytes(p) : statSync(p).size;
  }
  return total;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runScenario(name) {
  const scenario = adapter.scenarios[name];
  const out = join(outRoot, name);
  rmSync(out, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  const log = (line) => {
    const stamped = `[${new Date().toISOString().slice(11, 19)}] ${name}: ${line}`;
    console.log(stamped);
    writeFileSync(join(out, "progress.log"), stamped + "\n", { flag: "a" });
  };
  const workerInit = (serverUrl) => ({
    adapterPath,
    scenarioName: name,
    appId,
    serverUrl,
    backendSecret,
    writes: args.writes,
  });

  const launch = async (dataDir) =>
    await startServer({ bin: serverBin, appId, dataDir, outDir: out, adminSecret, backendSecret });

  /** Grow from `from` to `to` units on a running server, watching the budget. */
  async function ingest(server, from, to, label) {
    const writer = await startWorker({
      name: "writer",
      outDir: out,
      init: workerInit(server.url),
      packagesDir,
    });
    const serverSampler = sampleMemory(server.pid);
    const writerSampler = sampleMemory(writer.pid);
    const chunks = [];
    const chunkCount = Math.min(10, to - from);
    let at = from;
    let rows = 0;
    const started = Date.now();
    let error = null;
    try {
      for (let i = 1; i <= chunkCount; i++) {
        const end = i === chunkCount ? to : from + Math.round(((to - from) * i) / chunkCount);
        if (end <= at) continue;
        const r = await writer.call({ op: "seed", from: at, to: end, window: Number(args.window) });
        rows += r.rows;
        at = end;
        chunks.push({ units: end, serverPeak: serverSampler.peak, serverRss: server.memory().rss });
        if (server.exit) break;
      }
    } catch (e) {
      error = String(e.message ?? e);
    }
    const ms = Date.now() - started;
    let probe = null;
    if (!error && !server.exit && Number(args.probe) > 0) {
      const probeSampler = sampleMemory(server.pid);
      try {
        probe = await writer.call({
          op: "probe",
          scale: at,
          count: Number(args.probe),
          serverPid: server.pid,
        });
      } catch (e) {
        probe = { error: String(e.message ?? e) };
      }
      probe.serverPeak = probeSampler.stop();
    }
    const result = {
      probe,
      label,
      from,
      to: at,
      rows,
      ms,
      rowsPerSecond: Math.round((rows / ms) * 1000),
      serverPeak: serverSampler.stop(),
      writerPeak: writerSampler.stop(),
      chunks,
      error,
      serverExit: server.exit,
    };
    await writer.stop();
    if (!server.exit) {
      await sleep(2000);
      result.serverSettled = server.memory().rss;
      const profile = await server.heapProfile();
      if (profile) {
        writeFileSync(join(out, `${label}-ingest.pb.gz`), profile);
        result.heap = summarizeHeap(profile);
      }
    }
    return result;
  }

  /** Restart on the data and open the app as its users would. */
  async function coldLoad(dataDir, scale, label) {
    const server = await launch(dataDir);
    const result = { label, scale, readyMs: server.readyMs, serverAfterOpen: server.memory().rss };
    await sleep(1000);
    result.serverIdle = server.memory().rss;
    const serverSampler = sampleMemory(server.pid);
    const client = await startWorker({
      name: "client",
      outDir: out,
      init: workerInit(server.url),
      packagesDir,
    });
    result.clientBaseline = client.memory().rss;
    const clientSampler = sampleMemory(client.pid);
    let crowd;
    try {
      const reads = await client.call({ op: "read", kind: "client", scale });
      result.clientMs = reads.ms;
      result.reads = reads.results[0];
      result.clientPeak = clientSampler.stop();
      result.clientRetained = client.memory().rss;
      result.serverPeakClientOnly = serverSampler.peak;
      if (crowdSize > 0) {
        crowd = await startWorker({
          name: "crowd",
          outDir: out,
          init: workerInit(server.url),
          packagesDir,
        });
        const r = await crowd.call({ op: "read", kind: "crowd", size: crowdSize, scale });
        result.crowd = { users: r.results.length, ms: r.ms };
      }
    } catch (e) {
      result.error = String(e.message ?? e);
      result.clientPeak ??= clientSampler.stop();
    }
    result.serverPeak = serverSampler.stop();
    result.serverHwm = server.memory().hwm;
    result.serverExit = server.exit;
    result.clientGrowth = (result.clientPeak ?? 0) - result.clientBaseline;
    if (!server.exit) {
      const profile = await server.heapProfile();
      if (profile) {
        writeFileSync(join(out, `${label}-cold.pb.gz`), profile);
        result.heap = summarizeHeap(profile);
      }
    }
    await client.stop();
    await crowd?.stop();
    // What the server keeps once its users have gone: retained, not peak.
    if (!server.exit) {
      await sleep(3000);
      result.serverAfterDisconnect = server.memory().rss;
      const profile = await server.heapProfile();
      if (profile) {
        writeFileSync(join(out, `${label}-after-disconnect.pb.gz`), profile);
        result.heapAfterDisconnect = summarizeHeap(profile, { top: 5 });
      }
      // Session churn: the same user opens the app and leaves, again and
      // again. Memory still held after each departure is per-session state
      // the server never frees; reconnect storms after a restart grow it.
      const cycles = [];
      for (let i = 0; i < Number(args.churn) && !server.exit; i++) {
        const again = await startWorker({
          name: "churn",
          outDir: out,
          init: workerInit(server.url),
          packagesDir,
        });
        try {
          await again.call({ op: "read", kind: "client", scale });
        } finally {
          await again.stop();
        }
        await sleep(2000);
        const heap = await server.heapProfile();
        cycles.push({
          rss: server.memory().rss,
          heap: heap ? summarizeHeap(heap, { top: 0 }).inUseBytes : null,
        });
      }
      if (cycles.length > 1) {
        const first = cycles[0];
        const last = cycles.at(-1);
        result.churn = {
          cycles,
          rssPerSession: Math.round((last.rss - first.rss) / (cycles.length - 1)),
          heapPerSession:
            first.heap == null ? null : Math.round((last.heap - first.heap) / (cycles.length - 1)),
        };
      }
    }
    return { server, result };
  }

  const verdict = (ingestStep, cold) => ({
    serverImport:
      Math.max(0, ...ingestStep.chunks.map((c) => c.serverPeak)) > budgets.server ||
      (Boolean(ingestStep.serverExit) && !ingestStep.probe),
    serverEdit:
      (ingestStep.probe?.serverPeak ?? 0) > budgets.server ||
      (Boolean(ingestStep.serverExit) && Boolean(ingestStep.probe)),
    serverCold:
      Boolean(cold) &&
      (Math.max(cold.serverPeak, cold.serverHwm) > budgets.server || Boolean(cold.serverExit)),
    client: Boolean(cold) && cold.clientGrowth > budgets.client,
  });

  const dataDir = join(out, "data");
  const okDir = join(out, "data-ok");
  log(`server budget ${fmt(budgets.server)}, client budget ${fmt(budgets.client)}`);
  let server = await launch(dataDir);
  await deploy({
    appId,
    serverUrl: server.url,
    adminSecret,
    schema: adapter.app,
    permissions: adapter.permissions,
  });

  const steps = [];
  let scale = 0;
  let next = Number(args.start ?? scenario.start);
  let lastOk = { scale: 0 };
  let over = null;
  while (next <= maxScale) {
    if (Date.now() > deadline) {
      log("time limit reached");
      break;
    }
    if (freeDisk(out) < minFreeDisk) {
      log("disk limit reached");
      break;
    }
    const label = `step-${String(steps.length + 1).padStart(2, "0")}`;
    const ingestStep = await ingest(server, scale, next, label);
    log(
      `${label} ingest ${scale}→${ingestStep.to} ${scenario.unit}: ${ingestStep.rows} rows in ${(ingestStep.ms / 1000).toFixed(1)} s, server peak ${fmt(ingestStep.serverPeak)}` +
        (ingestStep.probe?.msPerTransaction
          ? `; user edit ${ingestStep.probe.msPerTransaction} ms (server CPU ${ingestStep.probe.serverCpuMsPerTransaction} ms)`
          : "") +
        (ingestStep.error ? ` ERROR ${ingestStep.error}` : ""),
    );
    let cold = null;
    if (!server.exit) {
      await server.stop();
      const c = await coldLoad(dataDir, ingestStep.to, label);
      server = c.server;
      cold = c.result;
      log(
        `${label} cold @${ingestStep.to}: ready ${cold.readyMs} ms, server idle ${fmt(cold.serverIdle)}, one user ${fmt(cold.serverPeakClientOnly ?? 0)}, with crowd ${fmt(Math.max(cold.serverPeak, cold.serverHwm))}, after disconnect ${fmt(cold.serverAfterDisconnect ?? 0)}; client +${fmt(cold.clientGrowth)} (abs ${fmt(cold.clientPeak ?? 0)}, reads ${cold.clientMs} ms)` +
          (cold.churn
            ? `; each extra session keeps ${fmt(cold.churn.rssPerSession)} RSS, ${fmt(cold.churn.heapPerSession ?? 0)} heap`
            : "") +
          (cold.error ? ` ERROR ${cold.error}` : ""),
      );
    }
    const step = {
      label,
      scale: ingestStep.to,
      counts: scenario.describe(ingestStep.to),
      diskBytes: existsSync(dataDir) ? dirBytes(dataDir) : null,
      ingest: ingestStep,
      cold,
    };
    step.over = verdict(ingestStep, cold);
    steps.push(step);
    writeFileSync(join(out, "steps.json"), JSON.stringify(steps, null, 2));
    if (Object.values(step.over).some(Boolean) || ingestStep.error) {
      over = step;
      break;
    }
    // Keep the last data under budget, to bisect from.
    await server.stop();
    rmSync(okDir, { recursive: true, force: true });
    cpSync(dataDir, okDir, { recursive: true });
    server = await launch(dataDir);
    lastOk = step;
    scale = ingestStep.to;
    next = Math.max(scale + 1, Math.ceil(scale * growth));
  }
  await server.stop();

  // Bisect each side that crossed between the last step under budget and
  // the first over it, by growing a copy of the last good data.
  const ceilings = {};
  if (over) {
    // Ingest: the chunk at which the server first crossed is the answer
    // (within a tenth of the step), no re-run needed.
    if (over.over.serverImport) {
      const firstOver = over.ingest.chunks.find((c) => c.serverPeak > budgets.server);
      const before = over.ingest.chunks.filter((c) => c.units < (firstOver?.units ?? Infinity));
      ceilings.serverImport = {
        units: before.at(-1)?.units ?? lastOk.scale,
        overAt: firstOver?.units ?? over.scale,
      };
    }
    for (const side of ["serverEdit", "serverCold", "client"]) {
      if (!over.over[side]) continue;
      let lo = lastOk.scale;
      let hi = over.scale;
      const probes = [];
      for (let i = 0; i < bisectSteps && hi - lo > 1 && lo > 0; i++) {
        if (Date.now() > deadline) break;
        const mid = Math.round((lo + hi) / 2);
        const probeDir = join(out, "data-probe");
        rmSync(probeDir, { recursive: true, force: true });
        cpSync(okDir, probeDir, { recursive: true });
        const s = await launch(probeDir);
        const ingestStep = await ingest(s, lo, mid, `bisect-${side}-${i + 1}`);
        await s.stop();
        const { server: s2, result: cold } = await coldLoad(
          probeDir,
          mid,
          `bisect-${side}-${i + 1}`,
        );
        await s2.stop();
        const v = verdict(ingestStep, cold);
        probes.push({
          scale: mid,
          counts: scenario.describe(mid),
          ingest: ingestStep,
          cold,
          over: v,
        });
        log(
          `bisect ${side} @${mid}: import peak ${fmt(ingestStep.serverPeak)}, edits peak ${fmt(ingestStep.probe?.serverPeak ?? 0)}, cold peak ${fmt(Math.max(cold.serverPeak, cold.serverHwm))}, client +${fmt(cold.clientGrowth)} → ${v[side] ? "over" : "under"}`,
        );
        if (v[side]) hi = mid;
        else {
          lo = mid;
          rmSync(okDir, { recursive: true, force: true });
          cpSync(probeDir, okDir, { recursive: true });
        }
        rmSync(probeDir, { recursive: true, force: true });
      }
      ceilings[side] = { units: lo, overAt: hi, probes };
    }
  }

  // Growth per unit: least squares over the steps (scale, memory).
  const slope = (points) => {
    if (points.length < 2) return null;
    const n = points.length;
    const mx = points.reduce((a, p) => a + p[0], 0) / n;
    const my = points.reduce((a, p) => a + p[1], 0) / n;
    const num = points.reduce((a, p) => a + (p[0] - mx) * (p[1] - my), 0);
    const den = points.reduce((a, p) => a + (p[0] - mx) ** 2, 0);
    return den ? Math.round(num / den) : null;
  };
  const withCold = steps.filter((s) => s.cold && !s.cold.error);
  const perUnit = {
    serverImportPeak: slope(
      steps.map((s) => [s.scale, Math.max(0, ...s.ingest.chunks.map((c) => c.serverPeak))]),
    ),
    serverEditPeak: slope(
      steps
        .filter((s) => s.ingest.probe?.serverPeak)
        .map((s) => [s.scale, s.ingest.probe.serverPeak]),
    ),
    serverColdPeak: slope(
      withCold.map((s) => [s.scale, Math.max(s.cold.serverPeak, s.cold.serverHwm)]),
    ),
    serverIdleAfterRestart: slope(withCold.map((s) => [s.scale, s.cold.serverIdle])),
    clientPeak: slope(withCold.map((s) => [s.scale, s.cold.clientGrowth])),
    disk: slope(steps.filter((s) => s.diskBytes).map((s) => [s.scale, s.diskBytes])),
  };
  const sides = Object.entries(ceilings).sort((a, b) => a[1].units - b[1].units);
  const summary = {
    app: adapter.name,
    scenario: name,
    unit: scenario.unit,
    jazzTools: jazzVersion,
    serverBin,
    writes: args.writes,
    budgets,
    reached: over ? null : `no budget crossed up to ${steps.at(-1)?.scale ?? 0} ${scenario.unit}`,
    firstToCross: sides[0]?.[0] ?? null,
    ceilings: Object.fromEntries(
      Object.entries(ceilings).map(([side, c]) => [
        side,
        { ...c, counts: scenario.describe(c.units) },
      ]),
    ),
    bytesPerUnit: perUnit,
    lastStep: steps.at(-1)
      ? {
          scale: steps.at(-1).scale,
          counts: steps.at(-1).counts,
          probe: steps.at(-1).ingest.probe,
          serverIdle: steps.at(-1).cold?.serverIdle,
          churn: steps.at(-1).cold?.churn,
          // Sessions of this user the server can absorb, from idle, before
          // retained per-session memory alone crosses the budget.
          sessionsToBudget:
            steps.at(-1).cold?.churn?.rssPerSession > 0
              ? Math.floor(
                  (budgets.server - steps.at(-1).cold.serverIdle) /
                    steps.at(-1).cold.churn.rssPerSession,
                )
              : null,
          heapIngest: steps.at(-1).ingest.heap,
          heapCold: steps.at(-1).cold?.heap,
          heapAfterDisconnect: steps.at(-1).cold?.heapAfterDisconnect,
        }
      : null,
  };
  writeFileSync(join(out, "summary.json"), JSON.stringify(summary, null, 2));
  writeFileSync(join(out, "summary.md"), renderSummary(summary, steps));
  if (!args["keep-data"]) {
    for (const dir of [dataDir, okDir]) rmSync(dir, { recursive: true, force: true });
  }
  log(
    `summary: ${JSON.stringify({ firstToCross: summary.firstToCross, ceilings: Object.fromEntries(Object.entries(summary.ceilings).map(([k, v]) => [k, v.units])), reached: summary.reached })}`,
  );
  return summary;
}

const summaries = [];
for (const name of scenarioNames) summaries.push(await runScenario(name));
writeFileSync(join(outRoot, "summary.json"), JSON.stringify(summaries, null, 2));
