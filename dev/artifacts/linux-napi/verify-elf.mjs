#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

export const ceilings = { GLIBC: "2.34", GLIBCXX: "3.4.29", CXXABI: "1.3.13" };
const allowedLibraries = new Set([
  "libc.so.6",
  "libm.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "librt.so.1",
  "libgcc_s.so.1",
  "libstdc++.so.6",
  "ld-linux-x86-64.so.2",
  "ld-linux-aarch64.so.1",
]);
function newer(actual, limit) {
  const a = actual.split(".").map(Number),
    b = limit.split(".").map(Number);
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    if ((a[i] ?? 0) !== (b[i] ?? 0)) return (a[i] ?? 0) > (b[i] ?? 0);
  }
  return false;
}
export function verifyElf(path, platform) {
  const machine = {
    "linux-x64-gnu": "Advanced Micro Devices X86-64",
    "linux-arm64-gnu": "AArch64",
  }[platform];
  if (!machine) throw new Error(`unknown Linux NAPI platform ${platform}`);
  const read = (...args) =>
    execFileSync("readelf", ["--wide", ...args, path], { encoding: "utf8" });
  const header = read("--file-header");
  if (!header.includes(machine) || !/Class:\s+ELF64/.test(header) || !/Type:\s+DYN/.test(header))
    throw new Error(`Linux NAPI artifact is not an ELF64 shared object for ${platform}`);
  const libraries = [...read("--dynamic").matchAll(/\(NEEDED\).*\[([^\]]+)\]/g)].map((m) => m[1]);
  if (!libraries.includes("libc.so.6"))
    throw new Error("GNU NAPI binding must depend on libc.so.6");
  for (const library of libraries)
    if (!allowedLibraries.has(library))
      throw new Error(`unsupported runtime dependency ${library}`);
  const versions = {};
  for (const line of read("--version-info").split("\n")) {
    for (const [, family, version] of line.matchAll(/Name:\s+(GLIBCXX|GLIBC|CXXABI)_([\w.]+)/g)) {
      if (!/^\d+(\.\d+)+$/.test(version) || newer(version, ceilings[family]))
        throw new Error(
          `${platform} requires ${family}_${version}, exceeding ${family}_${ceilings[family]}: ${line.trim()}`,
        );
      if (!versions[family] || newer(version, versions[family])) versions[family] = version;
    }
  }
  if (!versions.GLIBC) throw new Error("GNU NAPI binding has no versioned glibc requirements");
  return { platform, libraries, versions, ceilings };
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    console.log(JSON.stringify(verifyElf(process.argv[2], process.argv[3]), null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
