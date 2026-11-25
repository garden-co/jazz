#!/usr/bin/env node
// Budget for a serverless function that imports `createJazz` from
// `jazz-tools/backend`, measured the way Vercel measures it: install the
// release tarballs for linux-x64 (glibc) with npm, then trace the function
// with @vercel/nft (Vercel's own file tracer) and add up the traced files.
// Vercel refuses functions over 250 MB uncompressed; jazz-tools 2.0.0-beta.3
// traced to ~400 MB because jazz-napi bundled every platform's binary.
//
//   node dev/artifacts/verify-backend-function-size.mjs <dir with *.tgz> <version>
//
// Fails (never warns) when the trace is over MAX_TRACED_BYTES, when it lacks
// the linux-x64 binary, or when it carries any other .node file.
import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export const MAX_TRACED_BYTES = 150_000_000;
// Exact pin: this runs in the release checks, on registry code.
const NFT_VERSION = "1.11.12";
const PLATFORM_PACKAGE = "@garden-co/jazz-napi-linux-x64-gnu";
const LINUX_BINARY = "jazz-napi.linux-x64-gnu.node";

function tarballFor(dir, files, name, version) {
  const file = `${name.replace(/^@/, "").replace("/", "-")}-${version}.tgz`;
  if (!files.includes(file)) throw new Error(`${dir} has no ${file}`);
  return join(dir, file);
}

/** Checks the traced file list; returns the receipt. Exported for tests. */
export function judgeTrace(fileSizes) {
  let total = 0;
  const byPackage = {};
  for (const [file, size] of fileSizes) {
    total += size;
    const parts = file.split("/");
    const at = parts.lastIndexOf("node_modules");
    const key =
      at === -1
        ? "(function)"
        : parts[at + 1].startsWith("@")
          ? `${parts[at + 1]}/${parts[at + 2]}`
          : parts[at + 1];
    byPackage[key] = (byPackage[key] ?? 0) + size;
  }
  const binaries = fileSizes.map(([file]) => file).filter((file) => file.endsWith(".node"));
  const problems = [];
  if (total > MAX_TRACED_BYTES)
    problems.push(
      `traced function is ${(total / 1e6).toFixed(1)} MB, over the ${MAX_TRACED_BYTES / 1e6} MB budget (Vercel's limit is 250 MB)`,
    );
  if (!binaries.some((file) => file.endsWith(`${PLATFORM_PACKAGE}/${LINUX_BINARY}`)))
    problems.push(
      `trace lacks ${PLATFORM_PACKAGE}/${LINUX_BINARY}: the function could not load Jazz`,
    );
  const extra = binaries.filter((file) => !file.endsWith(`${PLATFORM_PACKAGE}/${LINUX_BINARY}`));
  if (extra.length) problems.push(`trace carries other native binaries: ${extra.join(", ")}`);
  const top = Object.entries(byPackage)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name, bytes]) => `${(bytes / 1e6).toFixed(1)} MB ${name}`);
  return { totalBytes: total, maxBytes: MAX_TRACED_BYTES, files: fileSizes.length, top, problems };
}

export async function verifyBackendFunctionSize(dirArg, version) {
  const dir = resolve(dirArg);
  const files = readdirSync(dir);
  const work = mkdtempSync(join(tmpdir(), "jazz-backend-size-"));
  try {
    // Every jazz tarball as an override, so nothing at <version> comes from
    // the registry (it isn't there before publish).
    const overrides = {};
    for (const name of ["jazz-wasm", "jazz-napi"])
      overrides[name] = `file:${tarballFor(dir, files, name, version)}`;
    for (const file of files) {
      const match = /^garden-co-(jazz-napi-[a-z0-9-]+)-\d.*\.tgz$/.exec(file);
      if (match) overrides[`@garden-co/${match[1]}`] = `file:${join(dir, file)}`;
    }
    if (!overrides[PLATFORM_PACKAGE]) throw new Error(`${dir} has no ${PLATFORM_PACKAGE} tarball`);
    writeFileSync(
      join(work, "package.json"),
      JSON.stringify(
        {
          name: "jazz-backend-size-check",
          private: true,
          type: "module",
          dependencies: {
            "jazz-tools": `file:${tarballFor(dir, files, "jazz-tools", version)}`,
            "@vercel/nft": NFT_VERSION,
          },
          overrides,
        },
        null,
        2,
      ),
    );
    writeFileSync(
      join(work, "api.mjs"),
      'import { createJazz } from "jazz-tools/backend";\nexport default () => new Response(typeof createJazz);\n',
    );
    execFileSync(
      "npm",
      [
        "install",
        "--no-audit",
        "--no-fund",
        "--ignore-scripts",
        "--os=linux",
        "--cpu=x64",
        "--libc=glibc",
      ],
      { cwd: work, stdio: ["ignore", "inherit", "inherit"] },
    );
    const { nodeFileTrace } = await import(
      pathToFileURL(join(work, "node_modules/@vercel/nft/out/index.js")).href
    );
    const { fileList } = await nodeFileTrace([join(work, "api.mjs")], { base: work });
    const sizes = [];
    for (const file of fileList) {
      if (file === "api.mjs" || file.includes("node_modules/@vercel/nft/")) continue;
      const stat = statSync(join(work, file));
      if (stat.isFile()) sizes.push([file, stat.size]);
    }
    return judgeTrace(sizes);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [dir, version] = process.argv.slice(2);
    if (!dir || !version)
      throw new Error("usage: verify-backend-function-size.mjs <dir with *.tgz> <version>");
    const receipt = await verifyBackendFunctionSize(dir, version);
    console.log(
      `jazz-tools/backend function for linux-x64: ${(receipt.totalBytes / 1e6).toFixed(1)} MB traced (budget ${receipt.maxBytes / 1e6} MB, ${receipt.files} files)`,
    );
    for (const line of receipt.top) console.log(`  ${line}`);
    if (receipt.problems.length) {
      for (const problem of receipt.problems) console.log(`::error::${problem}`);
      process.exitCode = 1;
    }
  } catch (error) {
    console.log(`::error::backend function size check failed: ${error.message}`);
    process.exitCode = 1;
  }
}
