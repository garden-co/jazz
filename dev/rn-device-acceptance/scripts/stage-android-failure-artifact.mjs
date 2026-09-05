import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const MAX_FILE_BYTES = 512 * 1024 * 1024;
function fingerprint(file) {
  const stat = lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > MAX_FILE_BYTES)
    throw new Error("artifact file unavailable or exceeds bound");
  return {
    bytes: stat.size,
    sha256: createHash("sha256").update(readFileSync(file)).digest("hex"),
  };
}
function buildId(file) {
  const output = execFileSync("readelf", ["-n", file], {
    encoding: "utf8",
    timeout: 3000,
    maxBuffer: 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const id = /Build ID:\s*([0-9a-f]{16,128})\b/i.exec(output)?.[1];
  if (!id) throw new Error("library has no ELF build id");
  return id.toLowerCase();
}
function candidates(root) {
  const results = [];
  let entries = 0;
  function walk(dir, depth = 0) {
    if (depth > 6 || !existsSync(dir)) return;
    for (const name of readdirSync(dir).sort()) {
      if (++entries > 2000) throw new Error("native library inventory exceeds bound");
      const file = join(dir, name),
        stat = lstatSync(file);
      if (stat.isSymbolicLink()) continue;
      if (stat.isDirectory()) walk(file, depth + 1);
      else if (file.endsWith("/obj/x86_64/libjazzrelay.so")) {
        results.push(file);
        if (results.length > 16) throw new Error("native library candidates exceed bound");
      }
    }
  }
  walk(root);
  return results;
}

/** Retain only built synthetic artifacts, never launch arguments or runtime
 * files. The ELF build id binds debug symbols to the exact library in the APK. */
export function stageAndroidFailureArtifact({ root, output, sourceRevision, runId, runAttempt }) {
  if (
    !/^[0-9a-f]{40}$/.test(sourceRevision ?? "") ||
    !/^\d+$/.test(runId ?? "") ||
    !/^\d+$/.test(runAttempt ?? "")
  )
    throw new Error("invalid artifact provenance");
  const head = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
    timeout: 3000,
  }).trim();
  if (head !== sourceRevision) throw new Error("artifact head does not match requested source");
  const apk = join(
    root,
    "dev/rn-device-acceptance/android/app/build/outputs/apk/release/app-release.apk",
  );
  if (!existsSync(apk)) return { status: "apk-unavailable" };
  const apkFingerprint = fingerprint(apk);
  const producer = JSON.parse(
    readFileSync(join(root, "crates/jazz-rn/android/jazz-native-relay.manifest.json"), "utf8"),
  );
  if (producer.sourceRevision !== sourceRevision || !Number.isSafeInteger(producer.nativeRelayAbi))
    throw new Error("native producer source mismatch");
  mkdirSync(output, { recursive: true });
  copyFileSync(apk, join(output, "app-release.apk"));
  const manifest = {
    format: 1,
    sourceRevision,
    runId,
    runAttempt,
    nativeRelayAbi: producer.nativeRelayAbi,
    apk: apkFingerprint,
    symbols: "unavailable",
  };
  const scratch = mkdtempSync(join(tmpdir(), "jazz-android-symbol-match-"));
  try {
    const packed = join(scratch, "libjazzrelay.so");
    writeFileSync(
      packed,
      execFileSync("unzip", ["-p", apk, "lib/x86_64/libjazzrelay.so"], {
        timeout: 10000,
        maxBuffer: MAX_FILE_BYTES,
        stdio: ["ignore", "pipe", "pipe"],
      }),
    );
    const id = buildId(packed);
    const matches = candidates(join(root, "crates/jazz-rn/android/build/intermediates/cxx")).filter(
      (file) => {
        fingerprint(file);
        return (
          buildId(file) === id &&
          /\.symtab\b/.test(
            execFileSync("readelf", ["-S", "--wide", file], {
              encoding: "utf8",
              timeout: 3000,
              maxBuffer: 1024 * 1024,
              stdio: ["ignore", "pipe", "pipe"],
            }),
          )
        );
      },
    );
    manifest.elfBuildId = id;
    if (matches.length) {
      const symbols = fingerprint(matches[0]);
      copyFileSync(matches[0], join(output, "libjazzrelay-x86_64-unstripped.so"));
      manifest.symbols = symbols;
    }
    writeFileSync(join(output, "provenance.json"), JSON.stringify(manifest, null, 2) + "\n");
    return { status: "staged", symbols: matches.length > 0 };
  } catch {
    writeFileSync(join(output, "provenance.json"), JSON.stringify(manifest, null, 2) + "\n");
    return { status: "staged", symbols: false };
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const result = stageAndroidFailureArtifact({
      root: resolve(import.meta.dirname, "../../.."),
      output: process.env.JAZZ_ANDROID_FAILURE_ARTIFACT,
      sourceRevision: process.env.GITHUB_SHA,
      runId: process.env.GITHUB_RUN_ID,
      runAttempt: process.env.GITHUB_RUN_ATTEMPT,
    });
    console.log(JSON.stringify(result));
  } catch {
    console.log('{"status":"artifact-unavailable"}');
  }
}
