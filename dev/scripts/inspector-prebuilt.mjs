#!/usr/bin/env node
// The trusted CI producer builds against the verified SDK/WASM once. Consumers
// verify this receipt before handing only these bytes to Vercel, without Git.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const hash = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");
function inventory(directory, prefix = "") {
  return readdirSync(directory, { withFileTypes: true })
    .sort((a, b) => a.name.localeCompare(b.name))
    .flatMap((entry) => {
      const name = prefix + entry.name;
      if (entry.isDirectory()) return inventory(join(directory, entry.name), `${name}/`);
      if (!entry.isFile()) throw new Error(`Inspector output contains a non-regular file: ${name}`);
      return [{ file: name, sha256: hash(join(directory, entry.name)) }];
    });
}
function requireSha(sha) {
  if (!/^[a-f0-9]{40}$/.test(sha ?? ""))
    throw new Error("Expected an exact 40-character source SHA");
}
export function sealInspectorCheckout(dist, destination, cwd = process.cwd()) {
  // GITHUB_SHA can name a synthetic PR merge while the reusable package build
  // deliberately checks out the PR head. Seal the actual source checkout.
  const sourceSha = execFileSync("git", ["rev-parse", "HEAD"], { cwd, encoding: "utf8" }).trim();
  sealInspector(dist, destination, sourceSha);
}

export function sealInspector(dist, destination, sourceSha) {
  requireSha(sourceSha);
  if (existsSync(destination))
    throw new Error("Inspector staging destination already exists; use a fresh directory");
  const files = inventory(dist);
  if (!files.some(({ file }) => file === "index.html"))
    throw new Error("Inspector web output lacks index.html");
  mkdirSync(join(destination, ".vercel/output"), { recursive: true });
  cpSync(dist, join(destination, ".vercel/output/static"), { recursive: true });
  writeFileSync(
    join(destination, ".vercel/output/config.json"),
    JSON.stringify({
      version: 3,
      routes: [{ handle: "filesystem" }, { src: "/(.*)", dest: "/index.html" }],
    }) + "\n",
  );
  const output = inventory(join(destination, ".vercel/output"));
  writeFileSync(
    join(destination, "inspector-build.json"),
    JSON.stringify({ schema: 1, sourceSha, files: output }, null, 2) + "\n",
  );
  verifyInspector(destination, sourceSha);
}
export function verifyInspector(directory, expectedSha) {
  requireSha(expectedSha);
  const receipt = JSON.parse(readFileSync(join(directory, "inspector-build.json"), "utf8"));
  if (receipt.schema !== 1 || receipt.sourceSha !== expectedSha)
    throw new Error("Inspector build source SHA mismatch");
  const actual = inventory(join(directory, ".vercel/output"));
  if (JSON.stringify(actual) !== JSON.stringify(receipt.files))
    throw new Error("Inspector output file inventory/hash mismatch");
  if (!actual.some(({ file }) => file === "static/index.html"))
    throw new Error("Inspector web output lacks index.html");
  return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [command, directory, argument] = process.argv.slice(2);
    if (command === "seal" && directory && argument)
      sealInspectorCheckout(resolve(directory), resolve(argument));
    else if (command === "verify" && directory) verifyInspector(resolve(directory), argument);
    else
      throw new Error("Usage: inspector-prebuilt.mjs seal DIST DESTINATION | verify DIRECTORY SHA");
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
