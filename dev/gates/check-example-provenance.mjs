#!/usr/bin/env node

// Keep examples and starters from normalizing duplicate Jazz provenance fields.
// Imported external records may use the visible, narrowly named escape hatch.
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const duplicate =
  /^[ \t]*(createdAt|createdBy|updatedAt|updatedBy):(?![ \t]*s\.allowExternalProvenanceName\()/m;
const offenders = [];

function visit(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) visit(file);
    else if (
      entry.isFile() &&
      entry.name === "schema.ts" &&
      duplicate.test(fs.readFileSync(file, "utf8"))
    ) {
      offenders.push(path.relative(root, file));
    }
  }
}

visit(path.join(root, "examples"));
visit(path.join(root, "starters"));
if (offenders.length) {
  console.error(
    "Examples must use Jazz $ provenance columns or s.allowExternalProvenanceName(...):\n" +
      offenders.join("\n"),
  );
  process.exit(1);
}
console.log("Example provenance guidance OK.");
