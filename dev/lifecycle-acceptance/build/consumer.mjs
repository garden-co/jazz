// What the four consumer builds share.
import { cpSync, existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { ROOT, WORK, assert, filesUnder, run } from "../lib/common.mjs";
import { generate } from "./npm.mjs";

export function candidate() {
  return JSON.parse(readFileSync(join(WORK, "candidate.json"), "utf8"));
}

/** The candidate's checkout, for what isn't in release-artifacts (crates, the Go module's source). */
export function candidateSource() {
  const { sha, tree } = candidate();
  const dir = resolve(process.env.LA_SOURCE ?? "");
  assert(process.env.LA_SOURCE && existsSync(join(dir, ".git")), "LA_SOURCE must be a checkout of the candidate");
  const git = (...args) => run("git", ["-C", dir, ...args], { capture: true }).trim();
  assert(git("rev-parse", "HEAD") === sha, `LA_SOURCE is at ${git("rev-parse", "HEAD")}, not ${sha}`);
  assert(git("rev-parse", "HEAD^{tree}") === tree, "LA_SOURCE's tree isn't the manifest's");
  assert(git("status", "--porcelain", "--untracked-files=no") === "", "LA_SOURCE has local changes");
  return dir;
}

/** The consumer project: the driver's sources and what the shipped generator writes beside them. */
export function consumerProject(lang, values) {
  const { version } = candidate();
  const from = join(ROOT, "drivers", lang);
  const files = {};
  for (const name of filesUnder(from)) {
    let text = readFileSync(join(from, name), "utf8").replaceAll("__VERSION__", version);
    for (const [key, value] of Object.entries(values ?? {})) text = text.replaceAll(key, value);
    files[name] = text;
  }
  const generated = generate(lang, files);
  const dir = join(WORK, "consumers", lang);
  rmSync(dir, { recursive: true, force: true });
  cpSync(generated, dir, { recursive: true });
  rmSync(join(dir, "schema.ts"));
  return dir;
}

/** Outside any Jazz checkout, so no workspace, settings file or path default can reach the source. */
export function assertOutsideCheckouts() {
  for (let dir = WORK; dir !== resolve(dir, ".."); dir = resolve(dir, ".."))
    assert(
      !existsSync(join(dir, "Cargo.toml")) && !existsSync(join(dir, ".git")),
      `LA_WORK (${WORK}) is inside a project (${dir}): consumers must be external`,
    );
  assert(relative(ROOT, WORK).startsWith(".."), "LA_WORK is inside the suite");
}

/** The environment without anything that could point a consumer at another Jazz. */
export function cleanEnv(extra = {}) {
  const env = {};
  for (const [name, value] of Object.entries(process.env))
    if (!/^(JAZZ_|LD_LIBRARY_PATH$|GOPROXY$|GOFLAGS$|GOPRIVATE$|GONOPROXY$|GOWORK$|JAVA_TOOL_OPTIONS$|JAVA_OPTS$|CLASSPATH$|PKG_CONFIG_PATH$|GH_TOKEN$|GITHUB_TOKEN$)/.test(name))
      env[name] = value;
  return { ...env, ...extra };
}

export function writeDriver(lang, driver) {
  writeFileSync(join(WORK, `driver-${lang}.json`), JSON.stringify({ lang, ...driver }, null, 2));
  for (const { what, sha256 } of driver.resolved) console.log(`RESOLVED ${lang} ${what} sha256 ${sha256}`);
}
