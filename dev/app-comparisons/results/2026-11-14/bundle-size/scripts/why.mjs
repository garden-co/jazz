// Usage: node why.mjs <graph.json> <module-substring> [--static]
// Prints the shortest import chain from an entry to each matching module.
// With --static, only static imports are followed (what lands in the initial load).
import { readFileSync } from "node:fs";
const [file, target, flag] = process.argv.slice(2);
const g = JSON.parse(readFileSync(file, "utf8"));
const short = (id) => id.replace(/.*node_modules\/(\.pnpm\/[^/]+\/node_modules\/)?/, "").replace(/.*stage-plan\/jazz\//, "app:");
const prev = new Map(); const q = [];
for (const [id, m] of Object.entries(g)) if (m.entry) { prev.set(id, null); q.push(id); }
while (q.length) {
  const id = q.shift(); const m = g[id]; if (!m) continue;
  const next = [...m.imports.map((i) => [i, "→"]), ...(flag === "--static" ? [] : m.dynamic.map((i) => [i, "⇢ (dynamic)"]))];
  for (const [n, how] of next) if (!prev.has(n)) { prev.set(n, [id, how]); q.push(n); }
}
for (const id of Object.keys(g).filter((i) => i.includes(target))) {
  if (!prev.has(id)) { console.log(`${short(id)}: not reachable${flag ? " statically" : ""}`); continue; }
  const chain = []; let cur = id;
  while (cur) { const p = prev.get(cur); chain.unshift((p ? p[1] + " " : "") + short(cur)); cur = p?.[0]; }
  console.log(chain.join("\n  ") + "\n");
}
