#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync, copyFileSync, readFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";
const root = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const [directory, platform] = process.argv.slice(2);
if (!directory || !["linux-x64-gnu", "linux-arm64-gnu"].includes(platform))
  throw new Error("usage: prepare-packed-smoke.mjs <new-directory> <linux-platform>");
const fixture = resolve(directory);
mkdirSync(fixture); // Fail if a previous install could mask a missing package.
const tarballs = join(fixture, "tarballs");
mkdirSync(tarballs);
const packages = [
  "crates/jazz-wasm",
  "crates/jazz-napi",
  `crates/jazz-napi/npm/${platform}`,
  "packages/jazz-tools",
];
const dependencies = {};
for (const path of packages) {
  const manifest = JSON.parse(readFileSync(join(root, path, "package.json"), "utf8"));
  execFileSync("pnpm", ["--dir", join(root, path), "pack", "--pack-destination", tarballs], {
    stdio: "inherit",
  });
  const filename = `${manifest.name.replace(/^@/, "").replaceAll("/", "-")}-${manifest.version}.tgz`;
  dependencies[manifest.name] = `file:./tarballs/${filename}`;
}
writeFileSync(
  join(fixture, "package.json"),
  JSON.stringify(
    { name: "jazz-linux-packed-smoke", private: true, type: "module", dependencies },
    null,
    2,
  ),
);
copyFileSync(new URL("./packed-smoke.mjs", import.meta.url), join(fixture, "smoke.mjs"));
// Explicit platform dependency remains installed even when unrelated optional
// platforms and peers are omitted. All Jazz packages come from these tarballs.
execFileSync(
  "npm",
  [
    "install",
    "--ignore-scripts",
    "--omit=optional",
    "--legacy-peer-deps",
    "--no-audit",
    "--no-fund",
  ],
  { cwd: fixture, stdio: "inherit" },
);
