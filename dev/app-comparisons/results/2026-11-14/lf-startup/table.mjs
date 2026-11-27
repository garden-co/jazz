#!/usr/bin/env node
// Markdown table of the local-first milestones from breakdown summaries.
//   node table.mjs <label>=<summary.json>:<reload|reloadLast|join> ...
import { readFileSync } from "node:fs";

const ROWS = [
  ["app JS running (main.tsx)", "page app:main"],
  ["warm-up starts (prototype)", "page lf:warm:start"],
  ["warm-up: worker spawned (prototype)", "page lf:warm:worker-spawned"],
  ["warm-up: worker alive (prototype)", "page lf:warm:worker-alive"],
  ["warm-up: page WASM requested (prototype)", "page lf:warm:wasm-requested"],
  ["warm-up: hands over to session (prototype)", "page lf:warm:handoff"],
  ["session: account manager starts", "page lf:session:accounts:start"],
  ["session: account manager ready", "page lf:session:accounts:ready"],
  ["session: local-first chunk import starts", "page lf:chunk:import:start"],
  ["local-first chunk fetched", "page res:local-first-js"],
  ["createDb starts (account resolved)", "page lf:createDb:start"],
  ["SharedWorker created", "page page:shared-worker-created"],
  ["page: jazz-wasm glue imported", "page lf:wasm:import:end"],
  ["page: WASM compile starts", "page wasm:instantiateStreaming:start"],
  ["worker: script running (connect)", "worker worker:connect"],
  ["worker: lease store open (IDB) done", "worker lf:store:epoch-claimed"],
  ["page: lease granted", "page port:in:foreground-node-lease-ready"],
  ["worker: WASM compile starts (prefetch)", "worker wasm:instantiateStreaming:start"],
  ["page: WASM bytes downloaded", "page res:wasm"],
  ["page: WASM instantiated", "page wasm:instantiateStreaming:end"],
  ["createDb returns", "page lf:createDb:end"],
  ["client ready (JazzProvider children)", "page jazz:client-ready"],
  ["page runtime created (first query)", "page lf:page-runtime:new:end"],
  ["page → worker: connect-runtime", "page port:out:connect-runtime"],
  ["worker ← connect-runtime", "worker port:in:connect-runtime"],
  ["worker: WASM instantiated", "worker wasm:instantiateStreaming:end"],
  ["worker: WasmDb.openBrowser starts", "worker lf:init:openBrowser:start"],
  ["worker: WasmDb.openBrowser done", "worker lf:init:openBrowser:end"],
  ["page ← runtime-ready", "page port:in:runtime-ready"],
  ["page ← first frames from worker", "page port:in:frames"],
  ["worker WebSocket open", "worker ws:open"],
  ["first render with 100 tasks", "page data:tasks-100"],
  ["board committed (layout effect)", "page render:board-100"],
  ["100 cards in the DOM", "page dom:cards-100"],
];

const cols = process.argv.slice(2).map((arg) => {
  const [label, rest] = arg.split("=");
  const [file, part = "reload"] = rest.split(":");
  return { label, data: JSON.parse(readFileSync(file, "utf8"))[part] ?? {} };
});
console.log(`| milestone | ${cols.map((c) => c.label).join(" | ")} |`);
console.log(`|---|${cols.map(() => "---:").join("|")}|`);
for (const [label, key] of ROWS) {
  const vals = cols.map((c) => (c.data[key] ? Math.round(c.data[key].median) : ""));
  if (vals.every((v) => v === "")) continue;
  console.log(`| ${label} | ${vals.join(" | ")} |`);
}
