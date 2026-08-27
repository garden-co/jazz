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
const packageRootArgument = process.argv.indexOf("--package-root");
const packageRoot =
  packageRootArgument === -1
    ? join(root, "crates/jazz-rn")
    : resolve(process.argv[packageRootArgument + 1] ?? "");
const requestedTargets = process.argv
  .slice(2)
  .filter(
    (argument, index, arguments_) =>
      argument !== "--package-root" && arguments_[index - 1] !== "--package-root",
  );
const relaySource = join(root, "crates/jazz-native-relay");
const sourceRevision = process.env.JAZZ_NATIVE_RELAY_SOURCE_REVISION;
const cargoNdkVersion = process.env.JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION;
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

function plistString(dictionary, key) {
  const match = new RegExp(`<key>${key}</key>\\s*<string>([^<]+)</string>`).exec(dictionary);
  return match?.[1];
}

function verifyIosSlices(expected) {
  const info = readFileSync(join(targets.ios.root, "Info.plist"), "utf8");
  const availableLibraries = /<key>AvailableLibraries<\/key>\s*<array>([\s\S]*?)<\/array>/.exec(
    info,
  )?.[1];
  if (!availableLibraries)
    throw new Error("iOS XCFramework Info.plist has no AvailableLibraries array");

  const slices = [...availableLibraries.matchAll(/<dict>([\s\S]*?)<\/dict>/g)].map(
    (match) => match[1],
  );
  const requiredRoles = new Map([
    ["device", false],
    ["simulator", false],
  ]);
  for (const slice of slices) {
    if (plistString(slice, "SupportedPlatform") !== "ios") continue;
    const variant = plistString(slice, "SupportedPlatformVariant");
    const role =
      variant === undefined ? "device" : variant === "simulator" ? "simulator" : undefined;
    if (!role) continue;
    const identifier = plistString(slice, "LibraryIdentifier");
    const library = plistString(slice, "LibraryPath");
    if (!identifier || !library || !library.endsWith(".a"))
      throw new Error(`iOS ${role} XCFramework slice is missing its static library path`);
    const path = `${identifier}/${library}`;
    if (!expected.has(path))
      throw new Error(
        `iOS ${role} XCFramework slice ${path} is absent from its manifest inventory`,
      );
    requiredRoles.set(role, true);
  }
  for (const [role, present] of requiredRoles)
    if (!present) throw new Error(`iOS XCFramework is missing its ${role} static-library slice`);
}

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

for (const requested of requestedTargets) {
  const target = targets[requested];
  if (!target)
    throw new Error(`usage: verify-relay-artifacts.mjs <android|ios>... (unknown ${requested})`);
  const manifest = JSON.parse(readFileSync(target.manifest, "utf8"));
  if (
    manifest.format !== 1 ||
    manifest.nativeRelayAbi !== abi ||
    manifest.sourceRevision !== sourceRevision
  )
    throw new Error(
      `${requested} relay manifest does not match source revision ${sourceRevision} and ABI ${abi}`,
    );
  if (
    requested === "android" &&
    (!/^[0-9]+\.[0-9]+\.[0-9]+$/.test(manifest.toolchain?.cargoNdk ?? "") ||
      (cargoNdkVersion !== undefined && manifest.toolchain.cargoNdk !== cargoNdkVersion))
  )
    throw new Error(
      `Android relay manifest does not match cargo-ndk provenance ${cargoNdkVersion ?? ""}`,
    );
  if (!Array.isArray(manifest.files) || manifest.files.length === 0)
    throw new Error(`${requested} relay manifest has no file inventory`);
  const expected = new Map();
  for (const entry of manifest.files) {
    if (typeof entry?.path !== "string" || !/^[0-9a-f]{64}$/.test(entry?.sha256 ?? ""))
      throw new Error(`${requested} relay manifest has malformed file entry`);
    if (expected.has(entry.path))
      throw new Error(`${requested} relay manifest has duplicate ${entry.path}`);
    expected.set(entry.path, entry.sha256);
  }
  for (const required of target.required)
    if (!expected.has(required))
      throw new Error(`${requested} relay manifest omits required ${required}`);
  const actual = filesUnder(target.root);
  if (actual.length !== expected.size || actual.some((file) => !expected.has(file)))
    throw new Error(`${requested} relay artifact inventory differs from its manifest`);
  for (const [file, hash] of expected) {
    const actualHash = createHash("sha256")
      .update(readFileSync(join(target.root, file)))
      .digest("hex");
    if (actualHash !== hash)
      throw new Error(`${requested} relay artifact hash differs for ${file}`);
  }
  if (requested === "ios") verifyIosSlices(expected);
}

const stagedHeader = readFileSync(join(packageRoot, "native/include/jazz_native_relay.h"));
const sourceHeader = readFileSync(join(relaySource, "include/jazz_native_relay.h"));
if (!stagedHeader.equals(sourceHeader))
  throw new Error("staged relay C header differs from this source revision");
console.log(`verified native relay artifacts for ABI ${abi} at ${sourceRevision}`);
