#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const profilePath = resolve(process.argv[2] ?? "");
if (!process.argv[2]) {
  console.error(
    "usage: node dev/benchmarks/wasm-ingest/summarize-v8-profile.mjs isolate-*.cpuprofile",
  );
  process.exit(2);
}

const profile = JSON.parse(await readFile(profilePath, "utf8"));
const nodes = new Map(profile.nodes.map((node) => [node.id, node]));
const samples = profile.samples ?? [];
const deltas = profile.timeDeltas ?? [];
const parentById = new Map();
for (const node of profile.nodes) {
  for (const childId of node.children ?? []) {
    parentById.set(childId, node.id);
  }
}

const selfUs = new Map();
const selfGroupUs = new Map();
const selfModuleUs = new Map();
const inclusiveUs = new Map();
const callerUs = new Map();
let totalUs = 0;

for (let index = 0; index < samples.length; index += 1) {
  const node = nodes.get(samples[index]);
  if (!node) continue;
  const delta = deltas[index] ?? 0;
  totalUs += delta;
  const key = frameName(node.callFrame);
  selfUs.set(key, (selfUs.get(key) ?? 0) + delta);

  const group = groupName(node.callFrame);
  selfGroupUs.set(group, (selfGroupUs.get(group) ?? 0) + delta);

  const module = moduleName(node.callFrame);
  selfModuleUs.set(module, (selfModuleUs.get(module) ?? 0) + delta);

  const stack = stackForNode(node.id);
  for (const stackNode of stack) {
    const inclusiveKey = frameName(stackNode.callFrame);
    inclusiveUs.set(inclusiveKey, (inclusiveUs.get(inclusiveKey) ?? 0) + delta);
  }
  if (stack.length >= 2) {
    const leaf = stack[0];
    const caller = stack[1];
    const edge = `${frameName(caller.callFrame)} -> ${frameName(leaf.callFrame)}`;
    callerUs.set(edge, (callerUs.get(edge) ?? 0) + delta);
  }
}

const top = Number(process.env.JAZZ_PROFILE_TOP ?? "40");

console.log(`Sampled time: ${Math.round(totalUs / 1000)} ms across ${samples.length} samples`);
console.log("");
printRows("Self Frames", "Frame", selfUs, top);
console.log("");
printRows("Self By Group", "Group", selfGroupUs, top);
console.log("");
printRows("Self By Module", "Module", selfModuleUs, top);
console.log("");
printRows("Inclusive Frames", "Frame", inclusiveUs, top);
console.log("");
printRows("Caller Edges", "Caller -> Callee", callerUs, top);

function printRows(title, firstColumn, map, limit) {
  const rows = Array.from(map, ([name, us]) => ({
    name,
    ms: us / 1000,
    pct: totalUs === 0 ? 0 : (us / totalUs) * 100,
  }))
    .sort((left, right) => right.ms - left.ms)
    .slice(0, limit);

  console.log(`## ${title}`);
  console.log(`| ${firstColumn} | Self/Incl ms | % sampled |`);
  console.log("| --- | ---: | ---: |");
  for (const row of rows) {
    console.log(`| ${escapeCell(row.name)} | ${Math.round(row.ms)} | ${row.pct.toFixed(1)}% |`);
  }
}

function stackForNode(nodeId) {
  const stack = [];
  let currentId = nodeId;
  while (currentId != null) {
    const node = nodes.get(currentId);
    if (!node) break;
    stack.push(node);
    currentId = parentById.get(currentId);
  }
  return stack;
}

function groupName(callFrame) {
  const name = callFrame.functionName || "(anonymous)";
  const rustGroup = rustPathParts(name, 1);
  if (rustGroup) return rustGroup;
  const url = callFrame.url ?? "";
  if (url.startsWith("node:")) return "node";
  if (url.includes("/packages/")) return "packages";
  if (url.startsWith("wasm://")) return "wasm-unknown";
  if (!url) return name;
  return "js-other";
}

function moduleName(callFrame) {
  const name = callFrame.functionName || "(anonymous)";
  const rustModule = rustPathParts(name, 3);
  if (rustModule) return rustModule;
  const url = callFrame.url ?? "";
  if (url.includes("/packages/")) {
    const packageIndex = url.indexOf("/packages/");
    return url
      .slice(packageIndex + 1)
      .split("/")
      .slice(0, 4)
      .join("/");
  }
  if (url.startsWith("node:")) return url.split("/").slice(0, 2).join("/");
  if (url.startsWith("wasm://")) return "wasm-unknown";
  if (!url) return name;
  return url;
}

function rustPathParts(name, count) {
  const parts = name
    .replace(/^<|>$/g, "")
    .split("::")
    .map((part) => part.replace(/<.*$/, "").replace(/\[.*$/, ""))
    .filter(Boolean);
  if (parts.length < 2) return null;
  const crate = parts[0];
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(crate)) return null;
  if (
    ["core", "alloc", "std", "hashbrown", "talc", "sha1_smol", "groove", "jazz"].includes(crate)
  ) {
    return parts.slice(0, count).join("::");
  }
  return null;
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
