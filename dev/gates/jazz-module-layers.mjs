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

// Modules that lib.rs re-exports from other crates (`pub use groove;`,
// `pub use jazz_types::ids;`). Paths through them leave the jazz crate, so
// they are not layer references: Cargo already keeps the extracted layers
// below this crate.
function externalReexports(src) {
  const names = new Set(["groove"]);
  let lib = "";
  try {
    lib = fs.readFileSync(path.join(src, "lib.rs"), "utf8");
  } catch {
    return names;
  }
  for (const m of lib.matchAll(
    /^\s*(?:pub(?:\([a-z]+\))?\s+)?use\s+(jazz_[a-z_]+)::([a-z_]+)\s*;/gm,
  )) {
    names.add(m[2]);
  }
  return names;
}

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
  // types, model and protocol: extracted to
  // crates/jazz/layers/{types,model,protocol}.

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
  return defaultLayerOf(rel);
}

function defaultLayerOf(rel) {
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

// `impl` headers: [offset, trait text or null, self type text]. Generic
// parameter lists are skipped with angle-bracket depth, so `impl<T: A<B>>`
// and `impl X for Y<Z>` both split at the top-level `for`.
function implHeaders(s) {
  const out = [];
  for (const m of s.matchAll(/(?<![A-Za-z0-9_$])impl(?![A-Za-z0-9_])/g)) {
    let k = m.index + 4;
    while (/\s/.test(s[k] ?? "")) k++;
    if (s[k] === "<") {
      for (let depth = 0; k < s.length; k++) {
        if (s[k] === "<") depth++;
        else if (s[k] === ">" && s[k - 1] !== "-" && --depth === 0) break;
      }
      k++;
    }
    let depth = 0;
    let end = k;
    for (; end < s.length; end++) {
      const ch = s[end];
      if (ch === "<" || ch === "(" || ch === "[") depth++;
      else if ((ch === ">" && s[end - 1] !== "-") || ch === ")" || ch === "]") depth--;
      else if (depth === 0 && (ch === "{" || ch === ";")) break;
    }
    let header = s.slice(k, end).replace(/\s+/g, " ").trim();
    header = header.replace(/ where .*$/, "");
    const parts = splitTopLevel(header, " for ");
    if (parts.length === 2) out.push([m.index, parts[0], parts[1]]);
    else out.push([m.index, null, header]);
  }
  return out;
}

function splitTopLevel(text, sep) {
  let depth = 0;
  for (let k = 0; k < text.length; k++) {
    const ch = text[k];
    if (ch === "<" || ch === "(" || ch === "[") depth++;
    else if ((ch === ">" && text[k - 1] !== "-") || ch === ")" || ch === "]") depth--;
    else if (depth === 0 && text.startsWith(sep, k))
      return [text.slice(0, k), text.slice(k + sep.length)];
  }
  return [text];
}

// The named type an impl targets: `&'a crate::x::Foo<T>` -> "Foo".
function headName(type) {
  const t = type.replace(/^&\s*('[A-Za-z_]+\s*)?(mut\s+)?/, "").replace(/^dyn\s+/, "");
  const m = /^((?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*)([A-Za-z_][A-Za-z0-9_]*)/.exec(t);
  return m ? m[2] : null;
}

function identifiers(text) {
  return new Set(text.match(/[A-Za-z_][A-Za-z0-9_]*/g) ?? []);
}

export function analyze({ includeTests = false, src = SRC, layerOf = defaultLayerOf } = {}) {
  const EXTERNAL_REEXPORTS = externalReexports(src);
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
  // An `include!("x.rs")`d file is spliced into its includer, so it lives in
  // the includer's module (plus any inline `mod` around the include), not in
  // the module its path suggests: target rel -> [includer rel, inline names].
  const includedBy = new Map();
  for (const rel of files) {
    const raw = fs.readFileSync(path.join(src, rel), "utf8");
    const s = scrub(raw);
    const { ranges } = testRanges(s);
    const spans = inlineModules(s);
    scrubbed.set(rel, { s, ranges, spans });
    for (const m of raw.matchAll(/include!\(\s*"([^"]+\.rs)"\s*\)/g)) {
      const target = path.posix.normalize(path.posix.join(path.posix.dirname(rel), m[1]));
      const inline = spans.filter(([a, b]) => m.index > a && m.index < b).map(([, , name]) => name);
      includedBy.set(target, [rel, inline]);
      // `include!` inside a #[cfg(test)] item makes the target test code.
      if (ranges.some(([a, b]) => m.index >= a && m.index < b)) includedTestFiles.add(target);
    }
  }
  const moduleOfFile = (rel, seen = new Set()) => {
    const inc = includedBy.get(rel);
    if (!inc || seen.has(rel)) return modulePathOf(rel);
    seen.add(rel);
    return [...moduleOfFile(inc[0], seen), ...inc[1]];
  };
  for (const rel of files) {
    const own = moduleOfFile(rel);
    for (const name of testRanges(scrubbed.get(rel).s).testMods)
      testModulePrefixes.add([...own, name].join("::"));
  }
  const isTestFile = (rel) => {
    if (includedTestFiles.has(rel)) return true;
    if (includedBy.has(rel) && isTestFile(includedBy.get(rel)[0])) return true;
    const segs = moduleOfFile(rel);
    for (let k = 1; k <= segs.length; k++)
      if (testModulePrefixes.has(segs.slice(0, k).join("::"))) return true;
    return false;
  };

  // Where each type and trait name is defined. A name defined in several
  // files maps to all of them; impl checks treat any same-layer definition as
  // local, so a collision can hide a violation but never invent one.
  const typeDefs = new Map();
  const traitDefs = new Map();
  for (const rel of files) {
    const { s } = scrubbed.get(rel);
    for (const m of s.matchAll(/\b(struct|enum|union|type|trait)\s+([A-Za-z_][A-Za-z0-9_]*)/g)) {
      const map = m[1] === "trait" ? traitDefs : typeDefs;
      if (!map.has(m[2])) map.set(m[2], new Set());
      map.get(m[2]).add(rel);
    }
  }
  const definedIn = (map, name, layer) => {
    const where = [...(map.get(name) ?? [])];
    return {
      local: where.some((f) => layerOf(f) === layer),
      lower: where.filter((f) => layerOf(f) !== layer),
    };
  };

  const violations = [];
  for (const rel of files) {
    const testFile = isTestFile(rel);
    if (testFile && !includeTests) continue;
    const from = layerOf(rel);
    const allowed = new Set([from, ...LAYERS[from].deps]);
    const { s, ranges, spans } = scrubbed.get(rel);
    const inTest = (offset) => testFile || ranges.some(([a, b]) => offset >= a && offset < b);
    const own = moduleOfFile(rel);
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
    // Coherence: once each layer is a crate, an inherent impl must sit with
    // its type, and a trait impl needs a local trait or local type in it.
    const implSites = implHeaders(s).map(([offset, trait, self]) => [
      offset,
      trait,
      headName(self),
      self,
    ]);
    for (const m of s.matchAll(/\bimpl_record_field_[a-z0-9_]+!\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)/g))
      implSites.push([m.index, "groove::records::RecordField", m[1], m[1]]);
    for (const [offset, trait, name, self] of implSites) {
      if (!name || (!includeTests && inTest(offset))) continue;
      const selfDef = definedIn(typeDefs, name, from);
      if (selfDef.local || selfDef.lower.length === 0) continue;
      if (trait) {
        if (definedIn(traitDefs, headName(trait), from).local) continue;
        const mentioned = [...identifiers(trait), ...identifiers(self)].filter((id) => id !== name);
        if (mentioned.some((id) => definedIn(typeDefs, id, from).local)) continue;
      }
      const to = selfDef.lower[0];
      violations.push({
        from: rel,
        line: lineAt(s, offset),
        to,
        fromLayer: from,
        toLayer: layerOf(to),
        path: trait ? `impl ${trait} for ${self}` : `impl ${self}`,
        test: testFile || inTest(offset),
      });
    }
    for (const [offset, segs] of refs) {
      if (!includeTests && inTest(offset)) continue;
      let target;
      if (segs[0] === "crate") {
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
      if (EXTERNAL_REEXPORTS.has(target[0])) continue;
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
