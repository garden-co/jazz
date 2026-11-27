// Self time per function (and per bucket: wasm / jazz-js / react / app / native) of a .cpuprofile.
//   node profile-top.mjs <file.cpuprofile> [--per N] [--top 25]
import { readFileSync } from "node:fs";
const [file, ...rest] = process.argv.slice(2);
const per = Number(rest[rest.indexOf("--per") + 1]) || 1;
const top = Number(rest[rest.indexOf("--top") + 1]) || 25;
const p = JSON.parse(readFileSync(file, "utf8"));
const byId = new Map(p.nodes.map((n) => [n.id, n]));
const self = new Map();
const dt = p.timeDeltas;
for (let i = 0; i < p.samples.length; i++) {
  const n = byId.get(p.samples[i]);
  self.set(n.id, (self.get(n.id) ?? 0) + (dt[i] ?? 0) / 1000);
}
const bucket = (cf) => {
  const u = cf.url || "";
  if (/wasm/.test(u) || /^wasm-function|^\$/.test(cf.functionName) || u.startsWith("wasm://")) return "wasm";
  if (!u) return ["(idle)", "(program)", "(garbage collector)"].includes(cf.functionName) ? cf.functionName : "native";
  if (/jazz/i.test(u)) return "jazz-js";
  if (/react-dom|scheduler/.test(u)) return "react";
  return "app/other-js";
};
const fn = new Map(), bk = new Map();
for (const [id, ms] of self) {
  const cf = byId.get(id).callFrame;
  const k = `${cf.functionName || "(anon)"}  ${(cf.url || "").split("/").pop()}:${cf.lineNumber}`;
  fn.set(k, (fn.get(k) ?? 0) + ms);
  const b = bucket(cf);
  bk.set(b, (bk.get(b) ?? 0) + ms);
}
const fmt = (m) => [...m].sort((a, b) => b[1] - a[1]).map(([k, v]) => `${(v / per).toFixed(1).padStart(7)} ms  ${k}`);
console.log(`# ${file.split("/").pop()} (per ${per})`);
console.log(fmt(bk).join("\n"));
console.log("--");
console.log(fmt(fn).filter((l) => !/\(idle\)/.test(l)).slice(0, top).join("\n"));
