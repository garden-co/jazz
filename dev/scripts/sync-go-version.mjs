#!/usr/bin/env node
// jazz-go's versions are Jazz's, in lockstep: its `Version` constant
// (packages/jazz-go/version.go) is jazz-tools' version, and the module is
// tagged packages/jazz-go/v<version>. `changeset version` bumps only npm
// packages, so `pnpm release:version` runs this after it.
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const VERSION_CONST = /^const Version = "[^"]*"$/m;

/** Sets jazz-go's Version to jazz-tools' version; true when it changed. */
export function syncGoVersion(root = fileURLToPath(new URL("../..", import.meta.url))) {
  const { version } = JSON.parse(
    readFileSync(join(root, "packages", "jazz-tools", "package.json"), "utf8"),
  );
  const file = join(root, "packages", "jazz-go", "version.go");
  const source = readFileSync(file, "utf8");
  if (!VERSION_CONST.test(source)) {
    throw new Error(`${file} has no \`const Version = "…"\` line`);
  }
  const synced = source.replace(VERSION_CONST, `const Version = ${JSON.stringify(version)}`);
  if (synced === source) return false;
  writeFileSync(file, synced);
  return true;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const changed = syncGoVersion();
  console.log(changed ? "jazz-go: Version synced to jazz-tools" : "jazz-go: Version already current");
}
