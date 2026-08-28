#!/usr/bin/env node

/**
 * Verify the relay artifacts that are about to be assembled into an npm
 * package. Build and assemble jobs are intentionally separate runners, so a
 * successful build alone is not evidence that the downloaded bytes still
 * match this checkout's C ABI and source revision.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
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
const nativeSourceFingerprint = createHash("sha256")
  .update(
    execFileSync(
      "git",
      [
        "-C",
        root,
        "ls-tree",
        "-r",
        "--full-tree",
        "HEAD",
        "--",
        "Cargo.lock",
        "Cargo.toml",
        "crates/groove",
        "crates/jazz",
        "crates/jazz-native-relay",
        "crates/jazz-storage-sqlite",
        "crates/jazz-rn/scripts/build-relay-artifacts.sh",
      ],
      { encoding: "utf8" },
    ),
  )
  .digest("hex");

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

function decodeXmlText(text) {
  return text.replace(/&(?:amp|apos|gt|lt|quot|#x[0-9a-fA-F]+|#[0-9]+);/g, (entity) => {
    if (entity === "&amp;") return "&";
    if (entity === "&apos;") return "'";
    if (entity === "&gt;") return ">";
    if (entity === "&lt;") return "<";
    if (entity === "&quot;") return '"';
    const numeric = entity.slice(2, -1);
    const codePoint = numeric.startsWith("x")
      ? Number.parseInt(numeric.slice(1), 16)
      : Number.parseInt(numeric, 10);
    if (!Number.isSafeInteger(codePoint) || codePoint < 0 || codePoint > 0x10ffff)
      throw new Error(`invalid XML character reference ${entity}`);
    return String.fromCodePoint(codePoint);
  });
}

function parseXmlPlist(xml) {
  let cursor = 0;
  const skipWhitespace = () => {
    while (/\s/.test(xml[cursor] ?? "")) cursor += 1;
  };
  const readTag = () => {
    if (xml[cursor] !== "<") throw new Error("expected XML tag in iOS XCFramework Info.plist");
    const end = xml.indexOf(">", cursor + 1);
    if (end === -1) throw new Error("unterminated XML tag in iOS XCFramework Info.plist");
    const raw = xml.slice(cursor + 1, end).trim();
    cursor = end + 1;
    return raw;
  };
  const expectClosing = (name) => {
    skipWhitespace();
    const tag = readTag();
    if (tag !== `/${name}`) throw new Error(`expected </${name}> in iOS XCFramework Info.plist`);
  };
  const parseValue = () => {
    skipWhitespace();
    const tag = readTag();
    if (tag === "dict") {
      const dictionary = Object.create(null);
      while (true) {
        skipWhitespace();
        if (xml.startsWith("</dict>", cursor)) {
          expectClosing("dict");
          return dictionary;
        }
        const key = parseValue();
        if (typeof key !== "string")
          throw new Error("iOS XCFramework Info.plist dictionary has a non-string key");
        if (Object.hasOwn(dictionary, key))
          throw new Error(`iOS XCFramework Info.plist repeats key ${key}`);
        dictionary[key] = parseValue();
      }
    }
    if (tag === "array") {
      const array = [];
      while (true) {
        skipWhitespace();
        if (xml.startsWith("</array>", cursor)) {
          expectClosing("array");
          return array;
        }
        array.push(parseValue());
      }
    }
    if (tag === "string" || tag === "key") {
      const end = xml.indexOf("<", cursor);
      if (end === -1) throw new Error(`unterminated <${tag}> in iOS XCFramework Info.plist`);
      const text = decodeXmlText(xml.slice(cursor, end));
      cursor = end;
      expectClosing(tag);
      return text;
    }
    throw new Error(`unsupported <${tag}> in iOS XCFramework Info.plist`);
  };

  skipWhitespace();
  while (xml.startsWith("<?", cursor) || xml.startsWith("<!", cursor)) {
    const end = xml.indexOf(">", cursor + 2);
    if (end === -1) throw new Error("unterminated XML preamble in iOS XCFramework Info.plist");
    cursor = end + 1;
    skipWhitespace();
  }
  if (!xml.startsWith("<plist", cursor))
    throw new Error("iOS XCFramework Info.plist has no plist root");
  const plistOpeningEnd = xml.indexOf(">", cursor + 6);
  if (plistOpeningEnd === -1)
    throw new Error("unterminated plist root in iOS XCFramework Info.plist");
  cursor = plistOpeningEnd + 1;
  const plist = parseValue();
  expectClosing("plist");
  skipWhitespace();
  if (cursor !== xml.length) throw new Error("trailing data in iOS XCFramework Info.plist");
  if (typeof plist !== "object" || plist === null || Array.isArray(plist))
    throw new Error("iOS XCFramework Info.plist root is not a dictionary");
  return plist;
}

function verifyIosSlices(expected) {
  const plist = parseXmlPlist(readFileSync(join(targets.ios.root, "Info.plist"), "utf8"));
  const slices = plist.AvailableLibraries;
  if (!Array.isArray(slices))
    throw new Error("iOS XCFramework Info.plist has no AvailableLibraries array");
  const requiredRoles = new Map([
    ["device", false],
    ["simulator", false],
  ]);
  let libraryBasename;
  for (const slice of slices) {
    if (typeof slice !== "object" || slice === null || Array.isArray(slice))
      throw new Error("iOS XCFramework AvailableLibraries contains a non-dictionary slice");
    if (slice.SupportedPlatform !== "ios") continue;
    const variant = slice.SupportedPlatformVariant;
    const role =
      variant === undefined ? "device" : variant === "simulator" ? "simulator" : undefined;
    if (!role) continue;
    const identifier = slice.LibraryIdentifier;
    const library = slice.LibraryPath;
    if (!identifier || !library || !library.endsWith(".a"))
      throw new Error(`iOS ${role} XCFramework slice is missing its static library path`);
    const path = `${identifier}/${library}`;
    if (!expected.has(path))
      throw new Error(
        `iOS ${role} XCFramework slice ${path} is absent from its manifest inventory`,
      );
    requiredRoles.set(role, true);
    if (libraryBasename === undefined) libraryBasename = library;
    else if (library !== libraryBasename)
      throw new Error(
        `iOS XCFramework slices use inconsistent static-library names: ${libraryBasename} and ${library}`,
      );
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
    manifest.format !== 2 ||
    manifest.nativeRelayAbi !== abi ||
    manifest.sourceRevision !== sourceRevision ||
    manifest.nativeSourceFingerprint !== nativeSourceFingerprint
  )
    throw new Error(
      `${requested} relay manifest does not match source revision ${sourceRevision}, native source fingerprint ${nativeSourceFingerprint}, and ABI ${abi}`,
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
