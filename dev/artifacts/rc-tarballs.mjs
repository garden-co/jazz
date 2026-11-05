#!/usr/bin/env node
// npm tarballs for a GitHub release candidate (v2.0.0-alpha.N-rc.M). Nothing
// goes to npm: the tarballs are release assets, and each one's dependencies on
// the other Jazz packages point at those assets' URLs, so
// `npm i <release>/jazz-tools-<version>.tgz` installs jazz-wasm, jazz-napi
// and its platform binaries (and jazz-rn's native payloads) with no overrides.
//
//   rc-tarballs.mjs stamp <version>
//     Set every packed package (and jazz-napi's optional platform deps) to
//     <version>, after the CI build artifacts are in place.
//   rc-tarballs.mjs link <dir> <base-url>
//     Rewrite the internal dependency specs inside every <dir>/*.tgz to
//     <base-url>/<tarball>, then write <dir>/NPM-SHA256SUMS.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const rcVersion = /^2\.0\.0-alpha\.[0-9]+-rc\.[0-9]+$/;
const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));
const writeJson = (path, value) => writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
const subdirs = (dir) =>
  readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(dir, entry.name));

export function packageDirs(base = root) {
  return [
    "crates/jazz-wasm",
    "crates/jazz-napi",
    ...subdirs(join(base, "crates/jazz-napi/npm")).map((dir) => dir.slice(base.length + 1)),
    "crates/jazz-rn",
    "crates/jazz-rn/npm/android",
    "crates/jazz-rn/npm/ios",
    "packages/jazz-tools",
    "packages/create-jazz",
  ].map((dir) => join(base, dir));
}

export function stamp(version, base = root) {
  if (!rcVersion.test(version))
    throw new Error(`version must look like 2.0.0-alpha.N-rc.M, got ${JSON.stringify(version)}`);
  for (const dir of packageDirs(base)) {
    const path = join(dir, "package.json");
    const pkg = readJson(path);
    pkg.version = version;
    writeJson(path, pkg);
  }
  const napiPath = join(base, "crates/jazz-napi/package.json");
  const napi = readJson(napiPath);
  const platforms = subdirs(join(base, "crates/jazz-napi/npm")).map(
    (dir) => readJson(join(dir, "package.json")).name,
  );
  if (platforms.length === 0) throw new Error("no jazz-napi platform packages are staged");
  napi.optionalDependencies = Object.fromEntries(platforms.sort().map((name) => [name, version]));
  writeJson(napiPath, napi);
}

const dependencyFields = ["dependencies", "optionalDependencies"];

export function link(dir, baseUrl) {
  const tarballs = readdirSync(dir)
    .filter((file) => file.endsWith(".tgz"))
    .sort();
  if (tarballs.length === 0) throw new Error(`no tarballs in ${dir}`);
  const work = mkdtempSync(join(tmpdir(), "jazz-rc-tarballs-"));
  try {
    const packages = tarballs.map((file) => {
      const extract = join(work, file);
      execFileSync("mkdir", ["-p", extract]);
      execFileSync("tar", ["-xzf", join(dir, file), "-C", extract]);
      const manifestPath = join(extract, "package", "package.json");
      return { file, extract, manifestPath, manifest: readJson(manifestPath) };
    });
    const versions = new Set(packages.map((pkg) => pkg.manifest.version));
    if (versions.size !== 1 || !rcVersion.test([...versions][0]))
      throw new Error(`tarballs must share one rc version, got ${[...versions].join(", ")}`);
    const [version] = versions;
    const urls = new Map(packages.map((pkg) => [pkg.manifest.name, `${baseUrl}/${pkg.file}`]));
    for (const pkg of packages) {
      let changed = false;
      for (const field of dependencyFields)
        for (const [name, spec] of Object.entries(pkg.manifest[field] ?? {})) {
          if (!urls.has(name)) continue;
          if (spec !== version)
            throw new Error(
              `${pkg.manifest.name} ${field}.${name} is ${spec}, expected ${version}`,
            );
          pkg.manifest[field][name] = urls.get(name);
          changed = true;
        }
      // Peers stay exact versions: jazz-rn installed from its tarball satisfies them.
      for (const [name, spec] of Object.entries(pkg.manifest.peerDependencies ?? {}))
        if (urls.has(name) && spec !== version)
          throw new Error(`${pkg.manifest.name} peer ${name} is ${spec}, expected ${version}`);
      if (!changed) continue;
      writeJson(pkg.manifestPath, pkg.manifest);
      execFileSync("tar", ["-czf", join(dir, pkg.file), "-C", pkg.extract, "package"]);
      console.log(`linked ${pkg.file}`);
    }
    const sums = tarballs
      .map((file) => {
        const sha = createHash("sha256")
          .update(readFileSync(join(dir, file)))
          .digest("hex");
        return `${sha}  ${file}\n`;
      })
      .join("");
    writeFileSync(join(dir, "NPM-SHA256SUMS"), sums);
    process.stdout.write(sums);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [mode, ...args] = process.argv.slice(2);
  // The CLI acts on the checkout it runs in, which may be older than this script.
  if (mode === "stamp" && args.length === 1) stamp(args[0], process.cwd());
  else if (mode === "link" && args.length === 2) link(resolve(args[0]), args[1].replace(/\/$/, ""));
  else {
    console.error("usage: rc-tarballs.mjs stamp <version> | link <dir> <base-url>");
    process.exit(2);
  }
}
