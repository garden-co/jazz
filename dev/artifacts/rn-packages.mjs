import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  appendFileSync,
  writeFileSync,
} from "node:fs";
import { resolve, join } from "node:path";
import { tmpdir } from "node:os";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { verifyRnPackSize } from "./verify-rn-pack-size.mjs";

const root = resolve(import.meta.dirname, "../..");
const wrapper = join(root, "crates/jazz-rn");
const metadata = (path) => JSON.parse(readFileSync(join(path, "package.json"), "utf8"));
const platforms = ["android", "ios"];
function verify(path, platform) {
  execFileSync(
    "node",
    [join(wrapper, "scripts/verify-relay-artifacts.mjs"), "--package-root", path, platform],
    { stdio: "inherit" },
  );
}
function checkVersions() {
  const version = metadata(wrapper).version;
  for (const platform of platforms) {
    const name = `jazz-rn-${platform}`;
    const pkg = metadata(join(wrapper, "npm", platform));
    if (
      pkg.name !== name ||
      pkg.version !== version ||
      pkg.os ||
      pkg.cpu ||
      pkg.codegenConfig ||
      !["workspace:*", version].includes(metadata(wrapper).dependencies?.[name])
    )
      throw new Error(
        `RN payload ${name} must be an unconditional exact-version dependency at ${version}`,
      );
  }
  return version;
}
const mode = process.argv[2];
checkVersions();
if (mode === "stage") {
  const selected = process.argv[3] ? [process.argv[3]] : platforms;
  if (selected.some((platform) => !platforms.includes(platform)))
    throw new Error("Unknown RN staging platform");
  process.env.JAZZ_NATIVE_RELAY_SOURCE_REVISION ??= execFileSync(
    "git",
    ["-C", root, "rev-parse", "HEAD"],
    { encoding: "utf8" },
  ).trim();
  for (const platform of selected) {
    verify(wrapper, platform);
    const destination = join(wrapper, "npm", platform);
    for (const name of ["android", "ios", "native", "JazzNativeRelay.xcframework"])
      rmSync(join(destination, name), { force: true, recursive: true });
    const paths =
      platform === "android"
        ? ["android/src/main/jniLibs", "android/jazz-native-relay.manifest.json", "native/include"]
        : ["JazzNativeRelay.xcframework", "ios/jazz-native-relay.manifest.json", "native/include"];
    for (const path of paths) {
      mkdirSync(resolve(destination, path, ".."), { recursive: true });
      cpSync(join(wrapper, path), join(destination, path), { recursive: true });
    }
    verify(destination, platform);
  }
  // Assembly transports only the split payloads, avoiding duplicate binary
  // archives in the reusable CI artifact. Producer jobs still use raw staging.
  if (!process.argv[3])
    for (const path of [
      "android/src/main/jniLibs",
      "android/jazz-native-relay.manifest.json",
      "ios/jazz-native-relay.manifest.json",
      "JazzNativeRelay.xcframework",
    ])
      rmSync(join(wrapper, path), { recursive: true, force: true });
} else if (mode === "verify") {
  for (const platform of platforms) verify(join(wrapper, "npm", platform), platform);
} else if (mode === "pack") {
  const destination = resolve(process.argv[3]);
  mkdirSync(destination, { recursive: true });
  const version = checkVersions();
  const receipts = [];
  for (const name of ["jazz-rn-android", "jazz-rn-ios", "jazz-rn"]) {
    const platform = name.slice("jazz-rn-".length);
    const packageDir = name === "jazz-rn" ? wrapper : join(wrapper, "npm", platform);
    execFileSync("pnpm", ["--dir", packageDir, "pack", "--pack-destination", destination], {
      stdio: "inherit",
    });
    const tarball = join(destination, `${name}-${version}.tgz`);
    const bytes = readFileSync(tarball);
    const receipt = {
      name,
      version,
      tarball,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      ...verifyRnPackSize(bytes.length),
    };
    const extract = mkdtempSync(join(tmpdir(), "jazz-rn-pack-"));
    try {
      execFileSync("tar", ["-xzf", tarball, "-C", extract]);
      const packed = join(extract, "package");
      const pkg = metadata(packed);
      if (pkg.name !== name || pkg.version !== version)
        throw new Error("Packed RN identity mismatch");
      if (name === "jazz-rn") {
        for (const target of platforms)
          if (pkg.dependencies?.[`jazz-rn-${target}`] !== version)
            throw new Error("Packed RN dependencies must be exact-version ordinary dependencies");
        const walk = (dir) =>
          readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
            entry.isDirectory() ? walk(join(dir, entry.name)) : [entry.name],
          );
        if (
          walk(packed).some(
            (file) => file.endsWith(".a") || file === "jazz-native-relay.manifest.json",
          )
        )
          throw new Error("RN wrapper must not contain native payloads");
      } else verify(packed, platform);
    } finally {
      rmSync(extract, { recursive: true, force: true });
    }
    receipts.push(receipt);
  }
  const receiptFile = join(destination, "rn-packages.json");
  writeFileSync(receiptFile, JSON.stringify(receipts, null, 2) + "\n");
  // Export only after every tarball has passed. Publication consumes and hashes
  // this receipt, so a missing export cannot silently repack workspace sources.
  if (process.env.GITHUB_ENV)
    appendFileSync(process.env.GITHUB_ENV, `JAZZ_RN_VERIFIED_RECEIPT=${receiptFile}\n`);
  console.log(JSON.stringify(receipts));
} else throw new Error("usage: rn-packages.mjs <stage|verify|pack [destination]>");
