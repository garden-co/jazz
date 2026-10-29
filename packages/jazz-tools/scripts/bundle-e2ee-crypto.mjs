import { build } from "esbuild";
import { createHash } from "node:crypto";
import { mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const output = resolve(root, "dist/e2ee");
const sourceHashes = {
  libsodium: "29087d1719326c68006fe88cd110110e6c198ea078bd9c417ba3975875c89819",
  "libsodium-wrappers": "09d5c4ff3603f3da2747700d692c8ab3d5982b8b33665220c3787e7a95f52435",
};

export async function bundleE2eeCrypto(directory = output) {
  if (process.env.JAZZ_TEST_SEALED_TOOLS_DIST === "1" && resolve(directory) === output) {
    throw new Error("Crypto output is sealed for concurrent tests");
  }
  const notices = [];
  const sources = [];
  for (const name of ["libsodium", "libsodium-wrappers"]) {
    const dependency = resolve(root, "node_modules", name);
    const pkg = JSON.parse(await readFile(resolve(dependency, "package.json"), "utf8"));
    if (pkg.version !== "0.8.3") throw new Error(`Unqualified ${name} version: ${pkg.version}`);
    const source = await realpath(resolve(dependency, `dist/modules-esm/${name}.mjs`));
    const hash = createHash("sha256")
      .update(await readFile(source))
      .digest("hex");
    if (hash !== sourceHashes[name]) throw new Error(`Unqualified ${name} source`);
    sources.push(source);
    notices.push(
      `${name} ${pkg.version}\n${await readFile(resolve(dependency, "LICENSE"), "utf8")}`,
    );
  }
  await mkdir(directory, { recursive: true });
  const result = await build({
    absWorkingDir: root,
    metafile: true,
    entryPoints: [resolve(root, "src/e2ee/sodium-browser.ts")],
    outfile: resolve(directory, "sodium-browser.js"),
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    legalComments: "eof",
    // Only the vendor shim is bundled; Jazz source maps remain unchanged.
    sourcemap: false,
  });
  const inputs = new Set(Object.keys(result.metafile.inputs).map((path) => resolve(root, path)));
  if (sources.some((source) => !inputs.has(source))) {
    throw new Error("Crypto bundle did not include the qualified vendor sources");
  }
  await writeFile(resolve(directory, "SODIUM-SOURCES.json"), JSON.stringify(sourceHashes, null, 2));
  await writeFile(resolve(directory, "SODIUM-LICENSE.txt"), notices.join("\n\n"));
  await rm(resolve(directory, "sodium-browser.js.map"), { force: true });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) await bundleE2eeCrypto();
