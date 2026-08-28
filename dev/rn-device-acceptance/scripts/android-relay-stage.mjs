import { createHash } from "node:crypto";
import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";

export const androidRelayFiles = Object.freeze([
  "arm64-v8a/libjazz_native_relay.a",
  "armeabi-v7a/libjazz_native_relay.a",
  "x86/libjazz_native_relay.a",
  "x86_64/libjazz_native_relay.a",
]);

function filesUnder(root, directory = root) {
  const files = [];
  for (const name of readdirSync(directory).sort()) {
    const file = join(directory, name);
    const stat = lstatSync(file);
    if (stat.isSymbolicLink())
      throw new Error(`staged Android relay contains symbolic link: ${file}`);
    if (stat.isDirectory()) files.push(...filesUnder(root, file));
    else if (stat.isFile()) files.push(relative(root, file).split("\\\\").join("/"));
    else throw new Error(`staged Android relay is not a regular file: ${file}`);
  }
  return files;
}

/** Verify the artifact downloaded/staged for the APK, not an emulator-only slice. */
export function verifyAndroidRelayStage({ packageRoot, sourceRevision }) {
  if (!/^[0-9a-f]{40}$/i.test(sourceRevision ?? ""))
    throw new Error(
      "JAZZ_DEVICE_RELAY_SOURCE_REVISION must be the exact staged relay source commit",
    );
  const root = resolve(packageRoot);
  const manifest = JSON.parse(
    readFileSync(join(root, "android/jazz-native-relay.manifest.json"), "utf8"),
  );
  if (
    manifest.format !== 2 ||
    manifest.nativeRelayAbi !== 3 ||
    manifest.sourceRevision !== sourceRevision
  )
    throw new Error(
      `Android relay manifest does not match staged source revision ${sourceRevision} and ABI 3`,
    );
  if (!Array.isArray(manifest.files))
    throw new Error("Android relay manifest has no file inventory");
  const expected = new Map();
  for (const entry of manifest.files) {
    if (typeof entry?.path !== "string" || !/^[0-9a-f]{64}$/.test(entry?.sha256 ?? ""))
      throw new Error("Android relay manifest has a malformed file entry");
    if (expected.has(entry.path)) throw new Error(`Android relay manifest repeats ${entry.path}`);
    expected.set(entry.path, entry.sha256);
  }
  if (
    expected.size !== androidRelayFiles.length ||
    androidRelayFiles.some((file) => !expected.has(file))
  )
    throw new Error("Android relay manifest must contain exactly the four supported ABI slices");
  const libraries = join(root, "android/src/main/jniLibs");
  const actual = filesUnder(libraries);
  if (actual.length !== androidRelayFiles.length || actual.some((file) => !expected.has(file)))
    throw new Error("staged Android relay inventory differs from the exact four-ABI manifest");
  for (const [file, hash] of expected) {
    const observed = createHash("sha256")
      .update(readFileSync(join(libraries, file)))
      .digest("hex");
    if (observed !== hash) throw new Error(`staged Android relay hash differs for ${file}`);
  }
  return manifest;
}
