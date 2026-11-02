// CPU-profile the object while a command runs: start serve.mjs with
// INSPECTOR_PORT=9229, then
//   node profile.mjs out.cpuprofile -- doc-shelf-do-ceiling-ws ws://127.0.0.1:8787
// Prints self time by function, heaviest first.
import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";

const [out, , cmd, ...args] = process.argv.slice(2);
const port = process.env.INSPECTOR_PORT ?? 9229;
const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
const target = targets.find((t) => t.webSocketDebuggerUrl) ?? targets[0];
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener("open", r, { once: true }));
let id = 0;
const pending = new Map();
ws.addEventListener("message", (e) => {
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) pending.get(m.id)(m);
});
const call = (method, params = {}) =>
  new Promise((r) => {
    pending.set(++id, r);
    ws.send(JSON.stringify({ id, method, params }));
  });
await call("Profiler.enable");
await call("Profiler.setSamplingInterval", { interval: 200 });
// Profile only between the command's "phase: writing" and "phase: written"
// lines on stderr.
const child = spawn(cmd, args, { stdio: ["ignore", "inherit", "pipe"], env: process.env });
let result;
let buffered = "";
const done = new Promise((resolve) => {
  child.stderr.on("data", async (chunk) => {
    process.stderr.write(chunk);
    buffered += chunk;
    if (buffered.includes("phase: writing") && !buffered.includes("started")) {
      buffered += "started";
      await call("Profiler.start");
    }
    if (buffered.includes("phase: written") && !result) {
      result = (await call("Profiler.stop")).result;
      resolve();
    }
  });
});
await new Promise((r) => child.on("exit", r));
await done;
writeFileSync(out, JSON.stringify(result.profile));
const { nodes, samples, timeDeltas } = result.profile;
const byId = new Map(nodes.map((n) => [n.id, n]));
const self = new Map();
samples.forEach((s, i) => {
  const n = byId.get(s);
  const name = n.callFrame.functionName || `(${n.callFrame.url.split("/").pop()})`;
  self.set(name, (self.get(name) ?? 0) + (timeDeltas[i] ?? 0));
});
const total = [...self.values()].reduce((a, b) => a + b, 0);
console.error(`total sampled ${(total / 1e3).toFixed(0)} ms`);
for (const [name, us] of [...self].sort((a, b) => b[1] - a[1]).slice(0, 60))
  console.error(`${((100 * us) / total).toFixed(1).padStart(5)}%  ${name.slice(0, 140)}`);
ws.close();
