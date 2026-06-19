#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const here = __dirname;
const sourceData = path.join(here, "bench-data");
const targetData = path.join(here, "harness", "public", "data");

if (!fs.existsSync(sourceData)) {
  throw new Error(`missing generated bench data: ${sourceData}`);
}

fs.mkdirSync(targetData, { recursive: true });
for (const entry of fs.readdirSync(sourceData)) {
  if (!entry.endsWith(".kv") && !entry.endsWith(".ops")) continue;
  fs.copyFileSync(path.join(sourceData, entry), path.join(targetData, entry));
}

console.log(`copied benchmark data to ${targetData}`);
