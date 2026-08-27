#!/usr/bin/env node
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyPublishedNapiManifest } from "./provenance.mjs";

const platforms = {
  "linux-x64-gnu": "x86_64-unknown-linux-gnu",
  "darwin-x64": "x86_64-apple-darwin",
  "darwin-arm64": "aarch64-apple-darwin",
  "win32-x64-msvc": "x86_64-pc-windows-msvc",
};

function addPublishedFile(packagePath, file) {
  const packageJson = JSON.parse(readFileSync(packagePath, "utf8"));
  packageJson.files ??= [];
  if (!packageJson.files.includes(file)) packageJson.files.push(file);
  writeFileSync(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`);
}

export function stageNapiManifests(root, selectedPlatforms = platforms) {
  const napiRoot = join(root, "crates/jazz-napi");
  const artifacts = join(napiRoot, "artifacts");
  const provenance = join(napiRoot, "provenance");
  let shared;
  mkdirSync(provenance, { recursive: true });
  for (const [platform, target] of Object.entries(selectedPlatforms)) {
    const file = `jazz-napi.${platform}.manifest.json`;
    const source = join(artifacts, file);
    const node = join(napiRoot, "npm", platform, `jazz-napi.${platform}.node`);
    if (!existsSync(source))
      throw new Error(`missing provenance manifest for ${platform}: ${source}`);
    const manifest = JSON.parse(readFileSync(source, "utf8"));
    const problem = verifyPublishedNapiManifest(manifest, target, node);
    if (problem) throw new Error(`invalid provenance for ${platform}: ${problem}`);
    if (
      !/^[a-f0-9]{64}$/.test(manifest.nativeArtifactFingerprint ?? "") ||
      !/^[a-f0-9]{64}$/.test(manifest.packageInputs ?? "")
    )
      throw new Error(
        `invalid provenance for ${platform}: missing native fingerprint or package inputs`,
      );
    const identity = `${manifest.nativeArtifactFingerprint}\0${manifest.packageInputs}`;
    if (!shared) shared = identity;
    else if (shared !== identity)
      throw new Error(`NAPI target ${platform} has a different ABI fingerprint or package inputs`);
    copyFileSync(source, join(provenance, file));
    copyFileSync(source, join(napiRoot, "npm", platform, file));
    addPublishedFile(join(napiRoot, "npm", platform, "package.json"), file);
  }
  addPublishedFile(join(napiRoot, "package.json"), "provenance/*.manifest.json");
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
  try {
    const requestedPlatforms = process.argv.slice(2);
    const selectedPlatforms = requestedPlatforms.length
      ? Object.fromEntries(
          requestedPlatforms.map((platform) => {
            const target = platforms[platform];
            if (!target) throw new Error(`unknown NAPI platform: ${platform}`);
            return [platform, target];
          }),
        )
      : platforms;
    stageNapiManifests(root, selectedPlatforms);
  } catch (error) {
    console.error(`stage NAPI manifests: ${error.message}`);
    process.exitCode = 1;
  }
}
