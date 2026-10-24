import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmodSync,
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

const android = {
  "arm64-v8a": "aarch64-linux-android",
  "armeabi-v7a": "armv7-linux-androideabi",
  x86_64: "x86_64-linux-android",
};
const ios = ["aarch64-apple-ios", "aarch64-apple-ios-sim", "x86_64-apple-ios"];
const producer = new URL(
  "../../../crates/jazz-rn/scripts/build-relay-artifacts.sh",
  import.meta.url,
);

// Controlled platform tools exercise the real packaging script on Linux.
// Cargo's documented manifest/env/flag precedence determines where fresh
// outputs go. Stale workspace outputs must never become a new sealed package.
const platformTool = String.raw`
const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
const tool = path.basename(process.argv[1]);
const value = (flag) => args[args.indexOf(flag) + 1];
const write = (file, contents) => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, contents);
};
if (tool === "cargo") {
  if (args.join(" ") === "ndk --version") {
    console.log("cargo-ndk 4.1.2");
    process.exit(0);
  }
  const target = args[0] === "ndk" ? {
    "arm64-v8a": "aarch64-linux-android",
    "armeabi-v7a": "armv7-linux-androideabi",
    "x86_64": "x86_64-linux-android",
  }[value("-t")] : value("--target");
  const output = args.includes("--target-dir") ? value("--target-dir")
    : process.env.CARGO_TARGET_DIR || path.join(path.dirname(value("--manifest-path")), "target");
  write(path.join(output, target, "release/libjazz_native_relay.a"), "fresh:" + target);
} else if (tool === "lipo") {
  write(value("-output"), args.slice(1, args.indexOf("-output"))
    .map((file) => fs.readFileSync(file, "utf8")).join("\n"));
} else if (tool === "xcodebuild") {
  const libraries = args.flatMap((arg, i) => arg === "-library" ? [args[i + 1]] : []);
  const out = value("-output");
  for (const [i, name] of ["ios-arm64", "ios-arm64_x86_64-simulator"].entries())
    write(path.join(out, name, "libjazz_native_relay.a"), fs.readFileSync(libraries[i]));
  write(path.join(out, "Info.plist"), "controlled platform fixture");
} else {
  throw new Error("unexpected platform tool: " + tool);
}
`;

for (const platform of ["android", "ios"]) {
  for (const redirected of [false, true]) {
    test(`${platform} packages fresh standalone relay output with redirected target ${redirected}`, (t) => {
      const root = mkdtempSync(join(tmpdir(), "jazz native output-"));
      t.after(() => rmSync(root, { recursive: true, force: true }));
      const put = (relative, contents) => {
        const file = join(root, relative);
        mkdirSync(dirname(file), { recursive: true });
        writeFileSync(file, contents);
        return file;
      };
      const script = put("crates/jazz-rn/scripts/build-relay-artifacts.sh", "");
      cpSync(producer, script);
      mkdirSync(join(root, "crates/jazz-rn/ios"), { recursive: true });
      put("crates/jazz-native-relay/src/lib.rs", "pub const NATIVE_RELAY_ABI_V1: u16 = 1;\n");
      put("crates/jazz-native-relay/include/jazz_native_relay.h", "/* fixture header */\n");
      put(
        "crates/jazz-native-relay/Cargo.toml",
        '[package]\nname = "jazz-native-relay"\nversion = "0.1.0"\n',
      );
      execFileSync("git", ["init", "--quiet", root]);
      execFileSync("git", ["-C", root, "add", "crates"]);
      execFileSync("git", [
        "-C",
        root,
        "-c",
        "user.name=Build fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "-c",
        "core.hooksPath=/dev/null",
        "commit",
        "--quiet",
        "-m",
        "Fixture",
      ]);
      for (const target of [...Object.values(android), ...ios])
        put(`target/${target}/release/libjazz_native_relay.a`, `stale:${target}`);
      for (const tool of ["cargo", "cargo-ndk", "lipo", "xcodebuild"]) {
        const file = put(`bin/${tool}`, `#!${process.execPath}\n${platformTool}`);
        chmodSync(file, 0o755);
      }
      const env = { ...process.env, PATH: `${join(root, "bin")}:${process.env.PATH}` };
      delete env.CARGO_TARGET_DIR;
      delete env.JAZZ_NATIVE_RELAY_SOURCE_REVISION;
      delete env.JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION;
      if (redirected) env.CARGO_TARGET_DIR = join(root, "another target directory");
      execFileSync("bash", [script, platform], { cwd: root, env, stdio: "pipe" });

      const packageRoot = join(root, "crates/jazz-rn");
      const outputRoot = join(
        packageRoot,
        platform === "android" ? "android/src/main/jniLibs" : "JazzNativeRelay.xcframework",
      );
      const expected =
        platform === "android"
          ? Object.entries(android).map(([abi, target]) => [
              `${abi}/libjazz_native_relay.a`,
              `fresh:${target}`,
            ])
          : [
              ["ios-arm64/libjazz_native_relay.a", `fresh:${ios[0]}`],
              [
                "ios-arm64_x86_64-simulator/libjazz_native_relay.a",
                `fresh:${ios[1]}\nfresh:${ios[2]}`,
              ],
            ];
      const manifest = JSON.parse(
        readFileSync(join(packageRoot, platform, "jazz-native-relay.manifest.json"), "utf8"),
      );
      for (const [file, contents] of expected) {
        const bytes = readFileSync(join(outputRoot, file));
        assert.equal(
          bytes.toString(),
          contents,
          "fresh platform output must replace stale workspace bytes",
        );
        assert.equal(
          manifest.files.find((entry) => entry.path === file)?.sha256,
          createHash("sha256").update(bytes).digest("hex"),
        );
      }
    });
  }
}
