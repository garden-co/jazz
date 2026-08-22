import { access } from "node:fs/promises";
import { resolve } from "node:path";

const packageRoot = resolve(import.meta.dirname, "..");
const requiredArtifacts = [
  "JazzRnFramework.xcframework/Info.plist",
  "android/src/main/jniLibs/arm64-v8a/libjazz_rn.a",
  "android/src/main/jniLibs/armeabi-v7a/libjazz_rn.a",
  "android/src/main/jniLibs/x86/libjazz_rn.a",
  "android/src/main/jniLibs/x86_64/libjazz_rn.a",
];

const missing = [];
for (const artifact of requiredArtifacts) {
  try {
    await access(resolve(packageRoot, artifact));
  } catch {
    missing.push(artifact);
  }
}

if (missing.length > 0) {
  console.error("jazz-rn native artifact verification failed. Missing:");
  for (const artifact of missing) console.error(`  - ${artifact}`);
  console.error("Run `pnpm build:rn` on a macOS release builder before packing.");
  process.exitCode = 1;
} else {
  console.log("jazz-rn native artifact verification passed.");
}
