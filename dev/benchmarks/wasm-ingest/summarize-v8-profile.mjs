#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const profilePath = resolve(process.argv[2] ?? "");
if (!process.argv[2]) {
  console.error("usage: node dev/benchmarks/wasm-ingest/summarize-v8-profile.mjs isolate-*.cpuprofile");
  process.exit(2);
}

const profile = JSON.parse(await readFile(profilePath, "utf8"));
const nodes = new Map(profile.nodes.map((node) => [node.id, node]));
const samples = profile.samples ?? [];
const deltas = profile.timeDeltas ?? [];
const selfUs = new Map();

for (let index = 0; index < samples.length; index += 1) {
  const node = nodes.get(samples[index]);
  if (!node) continue;
  const key = frameName(node.callFrame);
  selfUs.set(key, (selfUs.get(key) ?? 0) + (deltas[index] ?? 0));
}

const rows = Array.from(selfUs, ([name, us]) => ({ name, ms: us / 1000 }))
  .sort((left, right) => right.ms - left.ms)
  .slice(0, Number(process.env.JAZZ_PROFILE_TOP ?? "40"));

console.log("| Frame | Self ms |");
console.log("| --- | ---: |");
for (const row of rows) {
  console.log(`| ${escapeCell(row.name)} | ${Math.round(row.ms)} |`);
}

function frameName(callFrame) {
  const name = callFrame.functionName || "(anonymous)";
  const url = callFrame.url ? callFrame.url.replace(/^file:\/\/\/?/, "") : "";
  if (!url) return name;
  const shortUrl = url.includes("/jazz_core/") ? url.slice(url.indexOf("/jazz_core/") + 11) : url;
  return `${name} ${shortUrl}`;
}

function escapeCell(value) {
  return String(value).replace(/\|/g, "\\|");
}
