#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const nativePackages = [
  "@garden-co/jazz-napi-linux-x64-gnu",
  "@garden-co/jazz-napi-linux-arm64-gnu",
  "@garden-co/jazz-napi-darwin-x64",
  "@garden-co/jazz-napi-darwin-arm64",
  "@garden-co/jazz-napi-win32-x64-msvc",
];

/** Verify the published package boundary, not an ignored build-stage index.js. */
export function verifyPackedNapi(tarball) {
  const files = new Set(execFileSync("tar", ["-tf", tarball], { encoding: "utf8" }).split("\n"));
  const read = (name) => {
    if (!files.has(`package/${name}`)) throw new Error(`packed jazz-napi is missing ${name}`);
    return execFileSync("tar", ["-xOf", tarball, `package/${name}`], { encoding: "utf8" });
  };
  // npm installs the matching @garden-co/jazz-napi-<platform> package; a
  // binary in the main package ships every platform's to every install (and
  // past Vercel's 250 MB function limit, jazz-napi 2.0.0-beta.3).
  const bundled = [...files].filter((name) => name.endsWith(".node"));
  if (bundled.length)
    throw new Error(
      `packed jazz-napi must not contain native binaries (they live in the @garden-co/jazz-napi-<platform> packages): ${bundled.join(", ")}`,
    );
  const manifest = JSON.parse(read("package.json"));
  const entry = manifest.exports?.["."];
  if (
    manifest.name !== "jazz-napi" ||
    manifest.main !== "index.cjs" ||
    manifest.types !== "index.d.ts" ||
    entry?.require !== "./index.cjs" ||
    entry?.import !== "./index.mjs" ||
    entry?.types !== "./index.d.ts" ||
    entry?.default !== "./index.cjs"
  ) {
    throw new Error(
      "packed jazz-napi manifest must expose the current CJS, ESM, and type entrypoints",
    );
  }
  for (const name of [
    "index.cjs",
    "index.mjs",
    "index.d.ts",
    "native-binding.cjs",
    "close-pollable.cjs",
    "native-artifact-fingerprint.cjs",
  ])
    read(name);
  const optional = manifest.optionalDependencies ?? {};
  for (const [name, version] of Object.entries(optional)) {
    if (typeof version === "string" && version.startsWith("workspace:"))
      throw new Error(`optionalDependencies.${name} uses workspace protocol in packed manifest`);
  }
  for (const name of nativePackages) {
    if (!optional[name]) throw new Error(`missing optional dependency ${name} in packed manifest`);
  }
  const loader = read("native-loader.cjs");
  const requests = [...loader.matchAll(/\brequire\s*\(\s*(["'])([^"']+)\1\s*\)/g)].map(
    (match) => match[2],
  );
  const unscoped = requests.filter((name) => name.startsWith("jazz-napi-"));
  if (unscoped.length)
    throw new Error(
      `packed native-loader.cjs references unscoped native packages: ${unscoped.join(", ")}`,
    );
  for (const name of nativePackages) {
    if (!requests.includes(name))
      throw new Error(`packed native-loader.cjs is missing scoped native package ${name}`);
  }
  return manifest;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.length !== 3) throw new Error("usage: verify-packed-napi.mjs <tarball>");
    const manifest = verifyPackedNapi(process.argv[2]);
    console.log(
      `Packed public loaders and native dependencies verified for ${manifest.name}@${manifest.version}`,
    );
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
