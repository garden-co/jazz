#!/usr/bin/env node
// Layering gate for the `jazz` crate, ahead of splitting it into crates.
//
// Every source file of crates/jazz/src belongs to a layer that will become its
// own crate. A file may reference items in its own layer or in the layers it
// is allowed to depend on. Cargo will enforce this for free once the layers are
// crates; until then this gate keeps the module graph acyclic so each layer can
// be cut out mechanically.
//
// References are read from `crate::` and `super::` paths, including `use`
// trees, after comments, strings and `#[cfg(test)]` items are removed. A path
// resolves to the deepest module file it names, so a re-export counts against
// the module that re-exports it: `crate::tools::ObjectId` is a reference to the
// facade, whereas `crate::object::ObjectId` is not.
//
// Production code must be clean. Test-only code (`#[cfg(test)]` items, modules
// declared under it, and files `include!`d from them) must also stay within
// its layer before that layer becomes a crate, because an upward
// dev-dependency would compile the lower crate twice. Test references that
// still cross layers are listed in jazz-module-layers.allow as
// `<file> -> <file>` pairs; the gate fails on any pair not listed there, on
// any listed pair that no longer occurs (so the list only shrinks), and on any
// production reference at all.
//
// Usage:
//   node dev/gates/jazz-module-layers.mjs                check (production + tests)
//   node dev/gates/jazz-module-layers.mjs --report       list production violations
//   node dev/gates/jazz-module-layers.mjs --report --tests   include test-only code
//   node dev/gates/jazz-module-layers.mjs --write-allow  rewrite the test allowlist

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const SRC = path.join(repoRoot, "crates/jazz/src");
const ALLOW = path.join(repoRoot, "dev/gates/jazz-module-layers.allow");

// External crates re-exported from lib.rs (`pub use groove;`). Paths through
// them leave the jazz crate, so they are not layer references.
const EXTERNAL_REEXPORTS = new Set(["groove"]);

// Layers, lowest first. `deps` lists the layers a layer may reference besides
// itself. Layers that are not each other's dependency compile in parallel once
// split. `peer` drives `NodeState` directly, so it sits above `node`.
export const LAYERS = {
  types: { deps: [] },
  model: { deps: ["types"] },
  protocol: { deps: ["types", "model"] },
  engine: { deps: ["types", "model", "protocol"] },
  node: { deps: ["types", "model", "protocol", "engine"] },
  peer: { deps: ["types", "model", "protocol", "engine", "node"] },
  db: { deps: ["types", "model", "protocol", "engine", "node", "peer"] },
  facade: {
    deps: ["types", "model", "protocol", "engine", "node", "peer", "db"],
  },
};

// File-path prefixes (relative to crates/jazz/src) and their layer. The
// longest matching prefix wins; anything unmatched is facade.
export const LAYER_OF_PATH = [
  ["ids.rs", "types"],
  ["time.rs", "types"],
  ["debug_env.rs", "types"],
  ["object.rs", "types"],
  ["app_id.rs", "types"],
  ["identity.rs", "types"],
  ["account_registry.rs", "types"],
  ["account_registry/", "types"],
  ["delivery_diagnostics.rs", "types"],
  ["postcard_exact.rs", "types"],
  ["local_executor.rs", "types"],

  ["schema.rs", "model"],
  ["query.rs", "model"],
  ["query/", "model"],
  ["tx.rs", "model"],
  ["model/", "model"],

  ["protocol.rs", "protocol"],
  ["protocol/", "protocol"],
  ["wire.rs", "protocol"],
  ["wire/", "protocol"],
  ["protocol_limits.rs", "protocol"],
  ["authorization_scope.rs", "protocol"],
  ["storage_codec_profile.rs", "protocol"],

  ["node/query_engine.rs", "engine"],
  ["node/query_engine/", "engine"],
  ["node/", "node"],

  ["peer.rs", "peer"],
  ["peer/", "peer"],

  ["db.rs", "db"],
  ["db/", "db"],
  ["foreground_node_lease.rs", "db"],
  ["cold_settle_attribution.rs", "db"],
  ["result_tree.rs", "db"],
];

export function layerOf(rel) {
  let best = null;
  for (const [prefix, layer] of LAYER_OF_PATH) {
    if (rel === prefix || (prefix.endsWith("/") && rel.startsWith(prefix))) {
      if (!best || prefix.length > best[0].length) best = [prefix, layer];
    }
  }
  return best ? best[1] : "facade";
}

function walk(dir, out = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full, out);
    else if (entry.name.endsWith(".rs")) out.push(full);
  }
  return out;
}

function modulePathOf(rel) {
  const parts = rel.replace(/\.rs$/, "").split("/");
  if (parts.at(-1) === "mod") parts.pop();
  if (parts.length === 1 && parts[0] === "lib") return [];
  return parts;
}

// Blank out comments, string/char literals, keeping offsets and newlines so
// reported line numbers stay right.
export function scrub(src) {
  const out = src.split("");
  const blank = (from, to) => {
    for (let k = from; k < to; k++) if (out[k] !== "\n") out[k] = " ";
  };
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    const d = src[i + 1];
    if (c === "/" && d === "/") {
      const end = src.indexOf("\n", i);
      const stop = end < 0 ? n : end;
      blank(i, stop);
      i = stop;
    } else if (c === "/" && d === "*") {
      let depth = 1;
      let j = i + 2;
      while (j < n && depth > 0) {
        if (src[j] === "/" && src[j + 1] === "*") {
          depth++;
          j += 2;
        } else if (src[j] === "*" && src[j + 1] === "/") {
          depth--;
          j += 2;
        } else j++;
      }
      blank(i, j);
      i = j;
    } else if (c === "r" && (d === "#" || d === '"') && !/[A-Za-z0-9_]/.test(src[i - 1] ?? "")) {
      let j = i + 1;
      let hashes = 0;
      while (src[j] === "#") {
        hashes++;
        j++;
      }
      if (src[j] !== '"') {
        i++;
        continue;
      }
      const close = '"' + "#".repeat(hashes);
      const end = src.indexOf(close, j + 1);
      const stop = end < 0 ? n : end + close.length;
      blank(i, stop);
      i = stop;
    } else if (c === '"') {
      let j = i + 1;
      while (j < n && src[j] !== '"') j += src[j] === "\\" ? 2 : 1;
      blank(i, j + 1);
      i = j + 1;
    } else if (c === "'") {
      const m = /^'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'/.exec(
        src.slice(i, i + 12),
      );
      if (m) {
        blank(i, i + m[0].length);
        i += m[0].length;
      } else i++;
    } else i++;
  }
  return out.join("");
}

// Find `#[cfg(test)]` items; return [start, end) ranges and the names of
// `mod x;` declarations among them.
function testRanges(s) {
  const ranges = [];
  const testMods = [];
  const re = /#\[cfg\(test\)\]/g;
  let m;
  while ((m = re.exec(s))) {
    let j = m.index + m[0].length;
    // Skip further attributes on the same item.
    for (;;) {
      while (/\s/.test(s[j] ?? "")) j++;
      if (s[j] === "#" && s[j + 1] === "[") {
        let depth = 0;
        for (; j < s.length; j++) {
          if (s[j] === "[") depth++;
          else if (s[j] === "]" && --depth === 0) {
            j++;
            break;
          }
        }
      } else break;
    }
    const brace = s.indexOf("{", j);
    const semi = s.indexOf(";", j);
    if (semi >= 0 && (brace < 0 || semi < brace)) {
      const decl = /^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*$/.exec(
        s.slice(j, semi),
      );
      if (decl) testMods.push(decl[1]);
      ranges.push([m.index, semi + 1]);
      re.lastIndex = semi + 1;
      continue;
    }
    if (brace < 0) break;
    let depth = 0;
    let k = brace;
    for (; k < s.length; k++) {
      if (s[k] === "{") depth++;
      else if (s[k] === "}" && --depth === 0) break;
    }
    ranges.push([m.index, k + 1]);
    re.lastIndex = k + 1;
  }
  return { ranges, testMods };
}

function expandUseTree(tree) {
  tree = tree.replace(/\s+/g, "");
  const brace = tree.indexOf("{");
  if (brace < 0) return [tree.split("::").filter((seg) => seg && seg !== "self" && seg !== "*")];
  const head = tree.slice(0, brace).split("::").filter(Boolean);
  const body = tree.slice(brace + 1, tree.lastIndexOf("}"));
  const parts = [];
  let depth = 0;
  let cur = "";
  for (const ch of body) {
    if (ch === "{") depth++;
    if (ch === "}") depth--;
    if (ch === "," && depth === 0) {
      parts.push(cur);
      cur = "";
    } else cur += ch;
  }
  parts.push(cur);
  const out = [];
  for (const part of parts)
    if (part.trim()) for (const p of expandUseTree(part)) out.push([...head, ...p]);
  if (out.length === 0) out.push(head);
  return out;
}

// Inline `mod name { ... }` blocks, so `super::` inside them resolves right.
function inlineModules(s) {
  const spans = [];
  for (const m of s.matchAll(/\bmod\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{/g)) {
    let depth = 0;
    let k = m.index + m[0].length - 1;
    for (; k < s.length; k++) {
      if (s[k] === "{") depth++;
      else if (s[k] === "}" && --depth === 0) break;
    }
    spans.push([m.index, k + 1, m[1]]);
  }
  return spans;
}

function lineAt(s, offset) {
  let line = 1;
  for (let k = 0; k < offset; k++) if (s.charCodeAt(k) === 10) line++;
  return line;
}

export function analyze({ includeTests = false, src = SRC } = {}) {
  const files = walk(src).map((full) => path.relative(src, full).split(path.sep).join("/"));
  const moduleFile = new Map(); // "a::b" -> rel
  for (const rel of files) moduleFile.set(modulePathOf(rel).join("::"), rel);

  const resolve = (segs) => {
    for (let k = segs.length; k >= 0; k--) {
      const key = segs.slice(0, k).join("::");
      if (moduleFile.has(key)) return moduleFile.get(key);
    }
    return "lib.rs";
  };

  // Test-only modules: declared under #[cfg(test)], or conventional test dirs.
  const testModulePrefixes = new Set();
  const includedTestFiles = new Set();
  const scrubbed = new Map();
  for (const rel of files) {
    const raw = fs.readFileSync(path.join(src, rel), "utf8");
    const s = scrub(raw);
    const { ranges, testMods } = testRanges(s);
    scrubbed.set(rel, { s, ranges });
    const own = modulePathOf(rel);
    for (const name of testMods) testModulePrefixes.add([...own, name].join("::"));
    // `include!("x.rs")` inside a #[cfg(test)] item makes x.rs test code.
    for (const m of raw.matchAll(/include!\(\s*"([^"]+\.rs)"\s*\)/g)) {
      if (ranges.some(([a, b]) => m.index >= a && m.index < b))
        includedTestFiles.add(path.posix.normalize(path.posix.join(path.posix.dirname(rel), m[1])));
    }
  }
  const isTestFile = (rel) => {
    if (includedTestFiles.has(rel)) return true;
    const segs = modulePathOf(rel);
    for (let k = 1; k <= segs.length; k++)
      if (testModulePrefixes.has(segs.slice(0, k).join("::"))) return true;
    return false;
  };

  const violations = [];
  for (const rel of files) {
    const testFile = isTestFile(rel);
    if (testFile && !includeTests) continue;
    const from = layerOf(rel);
    const allowed = new Set([from, ...LAYERS[from].deps]);
    const { s, ranges } = scrubbed.get(rel);
    const inTest = (offset) => testFile || ranges.some(([a, b]) => offset >= a && offset < b);
    const own = modulePathOf(rel);
    const spans = inlineModules(s);
    const moduleAt = (offset) => [
      ...own,
      ...spans.filter(([a, b]) => offset > a && offset < b).map(([, , name]) => name),
    ];
    const refs = [];
    const useSpans = [];
    for (const m of s.matchAll(/\buse\s+((?:crate|super|self)(?:::[\s\S]*?))\s*;/g)) {
      useSpans.push([m.index, m.index + m[0].length]);
      for (const segs of expandUseTree(m[1])) refs.push([m.index, segs]);
    }
    // Plain paths outside `use` statements; use-trees were expanded above.
    for (const m of s.matchAll(/\b(crate|super)((?:\s*::\s*(?:super|[A-Za-z_][A-Za-z0-9_]*))+)/g)) {
      if (useSpans.some(([a, b]) => m.index >= a && m.index < b)) continue;
      refs.push([
        m.index,
        [
          m[1],
          ...m[2]
            .split("::")
            .map((x) => x.trim())
            .filter(Boolean),
        ],
      ]);
    }
    for (const [offset, segs] of refs) {
      if (!includeTests && inTest(offset)) continue;
      let target;
      if (segs[0] === "crate") {
        if (EXTERNAL_REEXPORTS.has(segs[1])) continue;
        target = segs.slice(1);
      } else if (segs[0] === "super") {
        // `super` names the parent of the module the reference sits in.
        let base = moduleAt(offset).slice(0, -1);
        let k = 0;
        while (segs[k] === "super") {
          if (k > 0) base = base.slice(0, -1);
          k++;
        }
        target = [...base, ...segs.slice(k)];
      } else continue;
      const to = resolve(target);
      const toLayer = layerOf(to);
      if (!allowed.has(toLayer)) {
        violations.push({
          from: rel,
          line: lineAt(s, offset),
          to,
          fromLayer: from,
          toLayer,
          path: segs.join("::"),
          test: testFile || inTest(offset),
        });
      }
    }
  }
  return violations;
}

function pairKey(v) {
  return `${v.from} -> ${v.to}`;
}

function readAllow() {
  if (!fs.existsSync(ALLOW)) return new Set();
  return new Set(
    fs
      .readFileSync(ALLOW, "utf8")
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith("#")),
  );
}

function main(argv) {
  const report = argv.includes("--report");
  const violations = analyze({
    includeTests: !report || argv.includes("--tests"),
  });
  if (report) {
    for (const v of violations)
      console.log(
        `${v.test ? "[test] " : ""}${v.from}:${v.line} (${v.fromLayer}) -> ${v.to} (${v.toLayer}): ${v.path}`,
      );
    const pairs = new Set(violations.map(pairKey));
    console.log(`${violations.length} references, ${pairs.size} file pairs`);
    return 0;
  }
  const production = violations.filter((v) => !v.test);
  for (const v of production)
    console.error(
      `production upward reference: ${v.from}:${v.line} ${v.path} (${v.fromLayer} -> ${v.toLayer})`,
    );
  const pairs = [...new Set(violations.filter((v) => v.test).map(pairKey))].sort();
  if (argv.includes("--write-allow")) {
    if (production.length) return 1;
    fs.writeFileSync(
      ALLOW,
      "# Known upward references from test-only code in crates/jazz/src, as\n" +
        "# `<file> -> <file>`. Production code has none and may not gain any.\n" +
        "# Generated by `node dev/gates/jazz-module-layers.mjs --write-allow`.\n" +
        "# This list may only shrink; see the header of jazz-module-layers.mjs.\n" +
        pairs.map((p) => p + "\n").join(""),
    );
    console.log(`wrote ${pairs.length} pairs`);
    return 0;
  }
  const allow = readAllow();
  const unexpected = pairs.filter((p) => !allow.has(p));
  const stale = [...allow].filter((p) => !pairs.includes(p));
  for (const p of unexpected) {
    console.error(`new upward reference from test code: ${p}`);
    for (const v of violations.filter((v) => v.test && pairKey(v) === p))
      console.error(`  ${v.from}:${v.line} ${v.path} (${v.fromLayer} -> ${v.toLayer})`);
  }
  for (const p of stale) console.error(`fixed, remove from jazz-module-layers.allow: ${p}`);
  if (production.length || unexpected.length || stale.length) return 1;
  console.log(`jazz module layers: ok (production clean; ${pairs.length} known test pairs remain)`);
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exit(main(process.argv.slice(2)));
}
