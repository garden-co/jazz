// The Android leg: the pinned emulator test (android.json) on the candidate's release-maven AAR,
// with the candidate's core binary as its server.
// usage: node android.mjs <checkout of android.json's sha> <candidate name> <out dir>
import { spawn } from "node:child_process";
import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { WORK, assert, die, run } from "./lib/common.mjs";
import { candidate, cleanEnv } from "./build/consumer.mjs";
import { within } from "./suite/process.mjs";
import { redact } from "./suite/redact.mjs";
import { ANDROID_RULES, judgeAndroid } from "./suite/android-rules.mjs";

const [source, name, out] = process.argv.slice(2);
try {
  const { sha: pinned, predatesOneStore } = JSON.parse(readFileSync(new URL("android.json", import.meta.url), "utf8"));
  const head = run("git", ["-C", source, "rev-parse", "HEAD"], { capture: true }).trim();
  assert(head === pinned, `the checkout is at ${head}, not the pinned ${pinned}`);
  const { sha, run: runId, version } = candidate();
  // The script takes the version to resolve from its checkout, so this throwaway checkout is given
  // the candidate's. Nothing else of it changes, and the AAR is still checked by sha256 below.
  const pkgFile = join(source, "packages", "jazz-tools", "package.json");
  const pkgText = readFileSync(pkgFile, "utf8");
  const own = JSON.parse(pkgText).version;
  if (own !== version) {
    writeFileSync(pkgFile, pkgText.replace(`"version": "${own}"`, `"version": "${version}"`));
    console.log(`NOTE the pinned test's checkout says ${own}: it resolves the candidate's ${version}`);
  }

  const tree = join(WORK, "artifacts", "release-maven");
  const aar = `jazz-kotlin-android/${version}/jazz-kotlin-android-${version}.aar`;
  const manifest = JSON.parse(readFileSync(join(WORK, "artifacts", "release-manifest", "manifest.json"), "utf8"));
  const listed = manifest.files.find((file) => file.artifact === "release-maven" && file.path === aar);
  assert(listed, `the manifest lists no ${aar}`);
  const server = join(WORK, "artifacts", "release-server", "jazz-tools-linux-x64");
  chmodSync(server, 0o755);

  const started = Date.now();
  const child = spawn("bash", ["dev/scripts/test-kotlin-android-emulator.sh", tree], {
    cwd: source,
    env: cleanEnv({ JAZZ_TOOLS: server }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  let log = "";
  for (const stream of [child.stdout, child.stderr])
    createInterface({ input: stream }).on("line", (line) => {
      log += `${line}\n`;
      console.log(redact(line));
    });
  const exit = new Promise((resolve) => child.on("close", (code) => resolve(code)));
  const code = await within(exit, 20 * 60_000, () => {
    child.kill("SIGTERM"); // its trap stops what it started, by PID
    return "timeout";
  });

  // What was tested is the candidate's: the script prints the AAR's sha256 before it uses it.
  const judged = judgeAndroid(log, { aarSha256: listed.sha256, code, predatesOneStore });
  const results = judged.map(({ id, status, why }) => ({
    id,
    title: ANDROID_RULES.find((rule) => rule.id === id).title,
    basis: "brief",
    status,
    why,
    rounds: 1,
    passed: status === "PASS" ? 1 : 0,
    failures: status === "FAIL" ? { [why]: { count: 1, example: why } } : {},
  }));
  assert(code === 0 || /: \w+ FAILED/.test(log), `the script exited ${code} without a failed phase (see the log above)`);
  for (const { status, id, why } of results) console.log(`${status} android ${id}${why ? ` - ${why}` : ""}`);
  mkdirSync(out, { recursive: true });
  writeFileSync(
    join(out, `${name}-android.json`),
    JSON.stringify({ name, lang: "android", sha, run: runId, version, pinned, ms: Date.now() - started, results }, null, 2),
  );
  process.exitCode = results.some((result) => result.status === "FAIL") ? 1 : 0;
} catch (error) {
  die(`android: ${redact(error.message)}`);
}
