#!/usr/bin/env node
// Markdown tables of the variant medians: node make-tables.mjs [prefix]  (prefix "" = local, "rtt40-")
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
const here = dirname(fileURLToPath(import.meta.url));
const prefix = process.argv[2] ?? "";
const variants = ["marks", "fix-w4", "fix-mint", "fix-socket", "fix-pipe", "fix-all"].filter((v) => existsSync(join(here, "runs", prefix + v, "summary.json")));
const S = Object.fromEntries(variants.map((v) => [v, JSON.parse(readFileSync(join(here, "runs", prefix + v, "summary.json"), "utf8"))]));
const rows = [
  ["app:main", "main.tsx runs"], ["jz:mint:start", "mint start"], ["jz:mint:end", "mint end"], ["jazz:client-ready", "client ready"],
  ["jz:client-created", "runtime client created"], ["ws:create", "WebSocket created"], ["ws:open", "WebSocket open"],
  ["jz:thin:subscribe-sent", "first subscribe sent"], ["jz:thin:negotiated", "Welcome"], ["jz:thin:schema-bound", "schema bound"],
  ["jz:thin:delta", "first result (delta)"], ["data:show", "first render with the show"], ["data:tasks-100", "first render with 100 tasks"],
  ["dom:cards-100", "**100 cards in the DOM**"],
];
const name = (v) => (v === "marks" ? "unpatched" : v);
console.log(`| milestone | ${variants.map(name).join(" | ")} |`);
console.log(`|---|${variants.map(() => "---:").join("|")}|`);
for (const [k, label] of rows) console.log(`| ${label} | ${variants.map((v) => S[v].medianMsFromNavStart[k]?.toFixed(0) ?? "").join(" | ")} |`);
const base = S.marks?.medianMsFromNavStart["dom:cards-100"];
console.log(`| **saved vs. unpatched** | ${variants.map((v) => (v === "marks" ? "" : `**${(base - S[v].medianMsFromNavStart["dom:cards-100"]).toFixed(0)}**`)).join(" | ")} |`);
console.log(`| runs | ${variants.map((v) => S[v].runs).join(" | ")} |`);
