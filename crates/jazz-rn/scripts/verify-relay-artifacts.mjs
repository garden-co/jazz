#!/usr/bin/env node

/**
 * Verify the relay artifacts that are about to be assembled into an npm
 * package. Build and assemble jobs are intentionally separate runners, so a
 * successful build alone is not evidence that the downloaded bytes still
 * match this checkout's C ABI and source revision.
 */
import { createHash } from "node:crypto";
import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const root = resolve(import.meta.dirname, "../../..");
const packageRoot = join(root, "crates/jazz-rn");
const relaySource = join(root, "crates/jazz-native-relay");
const sourceRevision = process.env.JAZZ_NATIVE_RELAY_SOURCE_REVISION;
if (!/^[0-9a-f]{40}$/i.test(sourceRevision ?? ""))
  throw new Error("JAZZ_NATIVE_RELAY_SOURCE_REVISION must be the exact 40-character source commit");

const abiSource = readFileSync(join(relaySource, "src/lib.rs"), "utf8");
const abi = Number(/pub const NATIVE_RELAY_ABI_VERSION: u16 = (\d+);/.exec(abiSource)?.[1]);
if (!Number.isSafeInteger(abi)) throw new Error("could not read native relay ABI from Rust source");

const targets = {
  android: {
    root: join(packageRoot, "android/src/main/jniLibs"),
    manifest: join(packageRoot, "android/jazz-native-relay.manifest.json"),
    required: [
      "arm64-v8a/libjazz_native_relay.a",
      "armeabi-v7a/libjazz_native_relay.a",
      "x86/libjazz_native_relay.a",
      "x86_64/libjazz_native_relay.a",
    ],
  },
  ios: {
    root: join(packageRoot, "JazzNativeRelay.xcframework"),
    manifest: join(packageRoot, "ios/jazz-native-relay.manifest.json"),
    required: ["Info.plist"],
  },
};

function filesUnder(root, directory = root) {
  const files = [];
  for (const name of readdirSync(directory).sort()) {
    const file = join(directory, name);
    const stat = lstatSync(file);
    if (stat.isSymbolicLink()) throw new Error(`relay artifact contains symbolic link: ${file}`);
    if (stat.isDirectory()) files.push(...filesUnder(root, file));
    else if (stat.isFile()) files.push(relative(root, file).split("\\").join("/"));
    else throw new Error(`relay artifact is not a regular file: ${file}`);
  }
  return files;
}

for (const requested of process.argv.slice(2)) {
  const target = targets[requested];
  if (!target) throw new Error(`usage: verify-relay-artifacts.mjs <android|ios>... (unknown ${requested})`);
  const manifest = JSON.parse(readFileSync(target.manifest, "utf8"));
  if (manifest.format !== 1 || manifest.nativeRelayAbi !== abi || manifest.sourceRevision !== sourceRevision)
    throw new Error(`${requested} relay manifest does not match source revision ${sourceRevision} and ABI ${abi}`);
  if (!Array.isArray(manifest.files) || manifest.files.length === 0)
    throw new Error(`${requested} relay manifest has no file inventory`);
  const expected = new Map();
  for (const entry of manifest.files) {
    if (typeof entry?.path !== "string" || !/^[0-9a-f]{64}$/.test(entry?.sha256 ?? ""))
      throw new Error(`${requested} relay manifest has malformed file entry`);
    if (expected.has(entry.path)) throw new Error(`${requested} relay manifest has duplicate ${entry.path}`);
    expected.set(entry.path, entry.sha256);
  }
  for (const required of target.required)
    if (!expected.has(required)) throw new Error(`${requested} relay manifest omits required ${required}`);
  const actual = filesUnder(target.root);
  if (actual.length !== expected.size || actual.some((file) => !expected.has(file)))
    throw new Error(`${requested} relay artifact inventory differs from its manifest`);
  for (const [file, hash] of expected) {
    const actualHash = createHash("sha256").update(readFileSync(join(target.root, file))).digest("hex");
    if (actualHash !== hash) throw new Error(`${requested} relay artifact hash differs for ${file}`);
  }
}

const stagedHeader = readFileSync(join(packageRoot, "native/include/jazz_native_relay.h"));
const sourceHeader = readFileSync(join(relaySource, "include/jazz_native_relay.h"));
if (!stagedHeader.equals(sourceHeader)) throw new Error("staged relay C header differs from this source revision");
console.log(`verified native relay artifacts for ABI ${abi} at ${sourceRevision}`);
