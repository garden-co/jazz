#!/usr/bin/env node
// Drift detector for the nine Jazz starters.
//
// Verifies that files meant to be identical actually are, and that every
// README follows the same section order. Runs in CI / lefthook and also
// as `node dev/scripts/check-starters-parity.mjs`.
//
// When a check fails, the output names the file(s) that disagree and the
// hashes involved, so you can `diff` them directly.

import { readFileSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..");

const STARTERS = {
  next: ["starters/next-betterauth", "starters/next-localfirst", "starters/next-hybrid"],
  sveltekit: [
    "starters/sveltekit-betterauth",
    "starters/sveltekit-localfirst",
    "starters/sveltekit-hybrid",
  ],
  react: ["starters/react-betterauth", "starters/react-localfirst", "starters/react-hybrid"],
};

// File → the relative path within each starter, keyed by framework.
// Files listed here must be byte-identical across every variant of the
// same framework (a "horizontal" parity rule).
const HORIZONTAL_FILES = {
  next: ["schema.ts", "permissions.ts", "components/todo-widget.tsx"],
  sveltekit: ["src/lib/schema.ts", "src/lib/permissions.ts", "src/lib/TodoWidget.svelte"],
  react: ["schema.ts", "permissions.ts", "src/todo-widget.tsx"],
};

// Files that must be byte-identical across all nine starters regardless of
// framework or auth variant.
const ALL_STARTERS_FILES = ["AGENTS.md"];

// Files that should be byte-identical across both frameworks (a "vertical"
// parity rule on top of the horizontal one). We resolve them per framework
// via HORIZONTAL_FILES — the logical name is the dict key.
const CROSS_FRAMEWORK_FILES = [
  { logical: "schema", next: "schema.ts", sveltekit: "src/lib/schema.ts", react: "schema.ts" },
  {
    logical: "permissions",
    next: "permissions.ts",
    sveltekit: "src/lib/permissions.ts",
    react: "permissions.ts",
  },
];

// README sections required in every starter, in this order. Per-mode
// optional sections ("Removing BetterAuth", "Adding BetterAuth later")
// are handled separately.
const REQUIRED_README_SECTIONS = [
  "What this starter gives you",
  "Getting started",
  "Architecture",
  "How it works",
  "Extending the schema",
  "Environment variables",
  "Deploying to production",
  "Known limitations",
  "Where to go next",
];

// The "Extending the schema" section should be byte-identical across all 6.
const SHARED_README_SECTIONS = ["Extending the schema"];

// ---------------------------------------------------------------------------

const errors = [];

function hash(content) {
  return createHash("sha256").update(content).digest("hex");
}

function read(path) {
  const abs = resolve(repoRoot, path);
  if (!existsSync(abs)) return null;
  return readFileSync(abs, "utf8");
}

function checkHorizontalParity() {
  for (const [framework, dirs] of Object.entries(STARTERS)) {
    for (const rel of HORIZONTAL_FILES[framework]) {
      const hashes = new Map();
      for (const dir of dirs) {
        const content = read(`${dir}/${rel}`);
        if (content === null) {
          errors.push(`Missing file: ${dir}/${rel}`);
          continue;
        }
        const h = hash(content);
        if (!hashes.has(h)) hashes.set(h, []);
        hashes.get(h).push(`${dir}/${rel}`);
      }
      if (hashes.size > 1) {
        const groups = [...hashes.entries()]
          .map(([h, files]) => `  ${h.slice(0, 12)}  ${files.join(", ")}`)
          .join("\n");
        errors.push(
          `Horizontal drift in ${framework}: ${rel} disagrees across variants\n${groups}`,
        );
      }
    }
  }
}

function checkCrossFrameworkParity() {
  for (const { logical, next, sveltekit } of CROSS_FRAMEWORK_FILES) {
    // Both frameworks already passed horizontal parity if we got this far,
    // so one exemplar per framework is enough.
    const nextContent = read(`${STARTERS.next[0]}/${next}`);
    const svelteContent = read(`${STARTERS.sveltekit[0]}/${sveltekit}`);
    if (nextContent === null || svelteContent === null) continue;
    if (hash(nextContent) !== hash(svelteContent)) {
      errors.push(
        `Cross-framework drift in ${logical}: ` +
          `${STARTERS.next[0]}/${next} vs ${STARTERS.sveltekit[0]}/${sveltekit}`,
      );
    }
  }
}

function extractSections(content) {
  return content
    .split("\n")
    .filter((line) => line.startsWith("## "))
    .map((line) => line.slice(3).trim());
}

function extractSectionBody(content, heading) {
  const lines = content.split("\n");
  const start = lines.findIndex((l) => l.trim() === `## ${heading}`);
  if (start === -1) return null;
  let end = start + 1;
  while (end < lines.length && !lines[end].startsWith("## ")) end++;
  return lines.slice(start, end).join("\n").trim();
}

function checkReadmeStructure() {
  const allDirs = [...STARTERS.next, ...STARTERS.sveltekit];
  for (const dir of allDirs) {
    const content = read(`${dir}/README.md`);
    if (content === null) {
      errors.push(`Missing README: ${dir}/README.md`);
      continue;
    }
    const sections = extractSections(content);

    // Must contain REQUIRED_README_SECTIONS in order.
    let idx = 0;
    for (const s of sections) {
      if (s === REQUIRED_README_SECTIONS[idx]) idx++;
    }
    if (idx < REQUIRED_README_SECTIONS.length) {
      errors.push(
        `${dir}/README.md: missing or out-of-order section ` +
          `"${REQUIRED_README_SECTIONS[idx]}"\n` +
          `  actual order: ${sections.join(" → ")}`,
      );
    }
  }
}
function checkSharedReadmeBlocks() {
  const allDirs = [...STARTERS.next, ...STARTERS.sveltekit];
  for (const heading of SHARED_README_SECTIONS) {
    const hashes = new Map();
    for (const dir of allDirs) {
      const content = read(`${dir}/README.md`);
      if (content === null) continue;
      const body = extractSectionBody(content, heading);
      if (body === null) continue;
      const h = hash(body);
      if (!hashes.has(h)) hashes.set(h, []);
      hashes.get(h).push(dir);
    }
    if (hashes.size > 1) {
      const groups = [...hashes.entries()]
        .map(([h, dirs]) => `  ${h.slice(0, 12)}  ${dirs.join(", ")}`)
        .join("\n");
      errors.push(
        `README section drift: "${heading}" is not byte-identical across starters\n${groups}`,
      );
    }
  }
}

function checkAllStartersParity() {
  const allDirs = Object.values(STARTERS).flat();
  for (const rel of ALL_STARTERS_FILES) {
    const hashes = new Map();
    for (const dir of allDirs) {
      const content = read(`${dir}/${rel}`);
      if (content === null) {
        errors.push(`Missing file: ${dir}/${rel}`);
        continue;
      }
      const h = hash(content);
      if (!hashes.has(h)) hashes.set(h, []);
      hashes.get(h).push(`${dir}/${rel}`);
    }
    if (hashes.size > 1) {
      const groups = [...hashes.entries()]
        .map(([h, files]) => `  ${h.slice(0, 12)}  ${files.join(", ")}`)
        .join("\n");
      errors.push(`All-starters drift: ${rel} disagrees across starters\n${groups}`);
    }
  }
}

checkHorizontalParity();
checkCrossFrameworkParity();
checkAllStartersParity();
checkReadmeStructure();
checkSharedReadmeBlocks();

if (errors.length > 0) {
  console.error("Starters parity check FAILED:\n");
  for (const err of errors) console.error(`- ${err}\n`);
  process.exit(1);
}

console.log(`Starters parity OK (${Object.values(STARTERS).flat().length} starters).`);
