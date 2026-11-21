#!/usr/bin/env node
// Prints the timeline tables for results/<date>/breakdown/*/summary.json.
//   node breakdown-report.mjs [date]
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const date = process.argv[2] ?? new Date().toISOString().slice(0, 10);
const dir = join(root, "results", date, "breakdown");
const load = (name) => {
  const f = join(dir, name, "summary.json");
  return existsSync(f) ? JSON.parse(readFileSync(f, "utf8")) : undefined;
};
const cases = Object.fromEntries(readdirSync(dir).map((n) => [n, load(n)]).filter(([, s]) => s));

const SP_ROWS = [
  ["nav:responseEnd", "HTML received"],
  ["page app:main", "app JS running (main.tsx)"],
  ["page res:local-first-js", "local-first chunk fetched"],
  ["page page:shared-worker-created", "SharedWorker created"],
  ["worker timeOrigin", "worker started"],
  ["page wasm:ready", "page WASM instantiated"],
  ["page jazz:client-ready", "client ready (JazzProvider children)"],
  ["worker wasm:ready", "worker WASM instantiated"],
  ["page port:out:connect-runtime", "page asks worker for runtime"],
  ["page port:in:runtime-ready", "worker runtime ready (store open)"],
  ["page ws:open", "page WebSocket open (thin)"],
  ["page ws:first-in", "page: first server frame"],
  ["worker ws:open", "worker WebSocket open"],
  ["worker ws:first-in", "worker: first server frame"],
  ["page join:accepted", "join accepted (navigate to board)"],
  ["page data:show", "first render with the show"],
  ["page data:tasks-100", "first render with 100 tasks"],
  ["page render:board-100", "board committed (layout effect)"],
  ["page dom:cards-100", "100 cards in the DOM"],
];
const SP_LOG_ROWS = [
  ["page log:client→upstream:Join", "page → upstream: Join"],
  ["page log:upstream→client:Welcome", "page ← upstream: Welcome"],
  ["worker log:session→store:Read", "worker: first tab read hits store"],
  ["worker log:store→session:Rows(tasks)", "worker store → tab: Rows(tasks)"],
  ["worker log:client→upstream:Join", "worker → server: Join"],
  ["worker log:upstream→client:Welcome", "worker ← server: Welcome"],
  ["worker log:upstream→client:Rows(tasks)", "worker ← server: Rows(tasks)"],
  ["page log:upstream→client:Rows(tasks)", "page ← upstream: Rows(tasks)"],
  ["page log:delivered:tasks", "page: tasks subscription delivered"],
  ["page log:settled:tasks:Global", "page: tasks settled (Global)"],
];

function table(title, rows, cols) {
  const present = rows.filter(([k]) => cols.some(([, s]) => s?.[k]));
  if (!present.length) return "";
  let out = `\n${title}\n\n| milestone | ${cols.map(([h]) => h).join(" | ")} |\n|---|${cols.map(() => "---:").join("|")}|\n`;
  for (const [k, label] of present) {
    out += `| ${label} | ${cols.map(([, s]) => (s?.[k] ? Math.round(s[k].median) : "")).join(" | ")} |\n`;
  }
  return out;
}

let md = "";
for (const kind of ["marks", "log-debug"]) {
  for (const phase of ["reload", "join"]) {
    const cols = [
      ["thin", `stage-plan.thin.${kind}`],
      ["default", `stage-plan.default.${kind}`],
      ["now", `stage-plan.now.${kind}`],
      ["default, wait 0", `stage-plan.default.wait0.${kind}`],
      ["now, wait 0", `stage-plan.now.wait0.${kind}`],
    ].map(([h, n]) => [h, cases[n]?.[phase]]);
    md += table(`StagePlan ${phase}, ${kind === "marks" ? "marks only" : 'logLevel "debug"'} (median ms from navigation start)`, kind === "marks" ? SP_ROWS : [...SP_ROWS, ...SP_LOG_ROWS], cols);
  }
}

const BC_ROWS = [
  ["a page: send:start", "a: send handler starts"],
  ["a page: send:committed", "a: transaction committed locally"],
  ["a page: ws out", "a page → server (thin WebSocket)"],
  ["a page: port out", "a page → worker (port)"],
  ["a worker: port in", "a worker ← page"],
  ["a worker: ws out", "a worker → server"],
  ["b page: ws in", "b page ← server (thin WebSocket)"],
  ["b worker: ws in", "b worker ← server"],
  ["b worker: port out", "b worker → page"],
  ["b page: port in", "b page ← worker"],
  ["b page: data:newest", "b: React render with the message"],
  ["b page: dom", "b: message in the DOM"],
];
for (const kind of ["marks", "log-debug"]) {
  const cols = [
    ["thin", `band-chat.thin.${kind}`],
    ["default", `band-chat.default.${kind}`],
    ["thin, settled 30 s", `band-chat.thin.settle30s.${kind}`],
    ["default, settled 30 s", `band-chat.default.settle30s.${kind}`],
  ].map(([h, n]) => [h, cases[n]?.cross]);
  md += table(`BandChat cross-client, ${kind === "marks" ? "marks only" : 'logLevel "debug"'} (median ms after the click on Send)`, BC_ROWS, cols);
}
console.log(md);
