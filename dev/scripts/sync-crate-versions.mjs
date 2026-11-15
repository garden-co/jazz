#!/usr/bin/env node
// Sets the version of Jazz's published Rust crates, and of the Go module's
// version constant, to the jazz-tools npm version. `pnpm release:version`
// runs it after `changeset version`, so the Version Packages PR bumps them
// together. It edits text only (Cargo.toml, Cargo.lock, version.go), so the
// release job needs no Rust toolchain.
//
//   node dev/scripts/sync-crate-versions.mjs [--root <repo>]

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { GO_VERSION_FILE } from "./check-lockstep-versions.mjs";

const rootFlag = process.argv.indexOf("--root");
const root = rootFlag === -1 ? process.cwd() : process.argv[rootFlag + 1];
const version = JSON.parse(readFileSync(join(root, "packages/jazz-tools/package.json"), "utf8")).version;

function edit(relative, change) {
  const path = join(root, relative);
  const before = readFileSync(path, "utf8");
  const after = change(before);
  if (after !== before) {
    writeFileSync(path, after);
    console.log(`${relative}: ${version}`);
  }
}

// [workspace.package] version, inherited by every lockstep crate.
let previous;
edit("Cargo.toml", (text) => {
  const section = text.match(/^\[workspace\.package\]\n(?:(?!\[)[^\n]*\n)*/m);
  if (!section) throw new Error("Cargo.toml has no [workspace.package]");
  const current = section[0].match(/^version = "([^"]+)"$/m);
  if (!current) throw new Error("[workspace.package] has no version");
  previous = current[1];
  let next = text.replace(section[0], section[0].replace(current[0], `version = "${version}"`));
  // Exact requirements between lockstep crates in [workspace.dependencies].
  next = next.replace(
    /^(jazz[a-z0-9-]* = \{[^\n]*version = "=)([^"]+)(")/gm,
    (line, start, req, end) => (req === previous ? `${start}${version}${end}` : line),
  );
  return next;
});

// Cargo.lock: the workspace crates that inherit the version.
const members = [];
const cargoToml = readFileSync(join(root, "Cargo.toml"), "utf8");
const membersBlock = cargoToml.match(/^members = \[\n([\s\S]*?)^\]/m)[1];
for (const member of membersBlock.matchAll(/^\s*"([^"]+)"/gm)) {
  const manifest = join(root, member[1], "Cargo.toml");
  if (!existsSync(manifest)) continue;
  const text = readFileSync(manifest, "utf8");
  if (/^version\.workspace = true$/m.test(text))
    members.push(text.match(/^name = "([^"]+)"$/m)[1]);
}
edit("Cargo.lock", (text) =>
  text.replace(
    /^\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"\n(?!source)/gm,
    (block, name, old) =>
      members.includes(name) ? block.replace(`version = "${old}"`, `version = "${version}"`) : block,
  ),
);

// The Go module, once it has a version constant.
if (existsSync(join(root, GO_VERSION_FILE)))
  edit(GO_VERSION_FILE, (text) => text.replace(/\bVersion(\s*=\s*)"[^"]+"/, `Version$1"${version}"`));
