#!/usr/bin/env node
// Maven Central for .github/workflows/release-publish.yml, from the prebuilt
// `release-maven` artifact (the ~/.m2/repository/tools/jazz tree that
// dev/scripts/publish-kotlin.sh <natives> local leaves, signed with a
// throwaway key). Nothing is built here.
//
//   maven-central.mjs bundle <tools/jazz tree> <version> <out.zip>
//       Copies <artifact>/<version>/ of jazz-kotlin and jazz-kotlin-android
//       (and jazz-kotlin-jvm, once jazz-kotlin is Kotlin Multiplatform),
//       drops the throwaway signatures, signs every file with the key in
//       MAVEN_SIGNING_KEY (passphrase MAVEN_SIGNING_KEY_PASSWORD), adds the
//       .md5 and .sha1 Central requires, checks the set, and zips it in the
//       Central Portal's bundle layout (tools/jazz/<artifact>/<version>/…).
//   maven-central.mjs published <version>
//       Exit 0 when both artifacts are already published at <version>.
//   maven-central.mjs upload <out.zip> <version> <USER_MANAGED|AUTOMATIC> [--drop]
//       Skips when Central already has <version>. Otherwise uploads the
//       bundle and polls the deployment: USER_MANAGED until VALIDATED (and
//       then drops it with --drop: a dry run), AUTOMATIC until PUBLISHING.
//       Reports how long each state took; writes deployment_id and
//       uploaded_at (ms) to GITHUB_OUTPUT.
//   maven-central.mjs await-served <version> <minutes> [uploaded_at]
//       Waits (no credentials) until repo1.maven.org serves both POMs;
//       exit 1 after <minutes>.
//
// Auth: MAVEN_CENTRAL_USERNAME and MAVEN_CENTRAL_PASSWORD (a Portal user token).
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  cpSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const PORTAL = "https://central.sonatype.com/api/v1/publisher";
const GROUP = "tools/jazz";
const NAMESPACE = "tools.jazz";
export const ARTIFACTS = { "jazz-kotlin": "jar", "jazz-kotlin-android": "aar" };
// Kotlin Multiplatform adds the JVM variant (the root jazz-kotlin is then metadata).
const KMP_JVM = "jazz-kotlin-jvm";

/** The artifacts a tools/jazz tree publishes at `version`: KMP's jazz-kotlin-jvm when present. */
export function treeArtifacts(tree, version) {
  const names = Object.keys(ARTIFACTS);
  if (existsSync(join(resolve(tree), KMP_JVM, version))) names.splice(1, 0, KMP_JVM);
  return names;
}

export function requiredFiles(artifact, version) {
  const main = ARTIFACTS[artifact] ?? "jar";
  return [
    `${artifact}-${version}.${main}`,
    `${artifact}-${version}-sources.jar`,
    `${artifact}-${version}-javadoc.jar`,
    `${artifact}-${version}.pom`,
    `${artifact}-${version}.module`,
  ];
}

/** Whether `name` is something Central gets signed and checksummed. */
export function isPayload(name) {
  return (
    !/\.(asc|md5|sha1|sha256|sha512)$/.test(name) &&
    !name.startsWith("maven-metadata") &&
    name !== "_remote.repositories"
  );
}

function summary(text) {
  console.log(text);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`);
}

function bundle(tree, version, out) {
  const key = process.env.MAVEN_SIGNING_KEY;
  if (!key) throw new Error("MAVEN_SIGNING_KEY is not set");
  const passphrase = process.env.MAVEN_SIGNING_KEY_PASSWORD ?? "";
  const work = mkdtempSync(join(tmpdir(), "maven-bundle-"));
  const gnupg = mkdtempSync(join(tmpdir(), "gnupg-"));
  try {
    execFileSync(
      "gpg",
      [
        "--homedir",
        gnupg,
        "--batch",
        "--quiet",
        "--pinentry-mode",
        "loopback",
        "--passphrase",
        passphrase,
        "--import",
      ],
      { input: key, stdio: ["pipe", "inherit", "inherit"] },
    );
    for (const artifact of treeArtifacts(tree, version)) {
      const from = join(resolve(tree), artifact, version);
      const to = join(work, GROUP, artifact, version);
      if (!existsSync(from)) throw new Error(`${from} doesn't exist`);
      cpSync(from, to, { recursive: true });
      for (const name of readdirSync(to)) if (!isPayload(name)) rmSync(join(to, name));
      const files = readdirSync(to);
      for (const name of requiredFiles(artifact, version))
        if (!files.includes(name)) throw new Error(`${artifact} ${version} lacks ${name}`);
      for (const name of files) {
        const path = join(to, name);
        execFileSync("gpg", [
          "--homedir",
          gnupg,
          "--batch",
          "--quiet",
          "--yes",
          "--pinentry-mode",
          "loopback",
          "--passphrase",
          passphrase,
          "--armor",
          "--detach-sign",
          "--output",
          `${path}.asc`,
          path,
        ]);
        execFileSync(
          "gpg",
          ["--homedir", gnupg, "--batch", "--quiet", "--verify", `${path}.asc`, path],
          { stdio: "ignore" },
        );
        const bytes = readFileSync(path);
        for (const algorithm of ["md5", "sha1"])
          writeFileSync(`${path}.${algorithm}`, createHash(algorithm).update(bytes).digest("hex"));
      }
      console.log(`${artifact} ${version}: ${files.length} files signed: ${files.join(", ")}`);
    }
    rmSync(resolve(out), { force: true });
    execFileSync("zip", ["-qr", resolve(out), "tools"], { cwd: work });
    console.log(`bundle: ${resolve(out)}`);
  } finally {
    rmSync(work, { recursive: true, force: true });
    rmSync(gnupg, { recursive: true, force: true });
  }
}

function auth() {
  const { MAVEN_CENTRAL_USERNAME: user, MAVEN_CENTRAL_PASSWORD: password } = process.env;
  if (!user || !password)
    throw new Error("MAVEN_CENTRAL_USERNAME and MAVEN_CENTRAL_PASSWORD are needed");
  return { Authorization: `Bearer ${Buffer.from(`${user}:${password}`).toString("base64")}` };
}

async function portal(method, path, body) {
  const response = await fetch(`${PORTAL}${path}`, { method, headers: auth(), body });
  const text = await response.text();
  if (!response.ok) throw new Error(`${method} ${path}: HTTP ${response.status} ${text}`);
  return text;
}

async function isPublished(version) {
  for (const artifact of Object.keys(ARTIFACTS)) {
    const query = new URLSearchParams({ namespace: NAMESPACE, name: artifact, version });
    const { published } = JSON.parse(await portal("GET", `/published?${query}`));
    if (!published) return false;
  }
  return true;
}

async function upload(zip, version, type, drop) {
  if (!["USER_MANAGED", "AUTOMATIC"].includes(type)) throw new Error(`publishing type ${type}?`);
  if (await isPublished(version)) {
    summary(`### Maven Central: ${version} is already published; nothing uploaded`);
    return;
  }
  const started = Date.now();
  const form = new FormData();
  form.append("bundle", new Blob([readFileSync(zip)]), "bundle.zip");
  const query = new URLSearchParams({ name: `jazz-kotlin ${version}`, publishingType: type });
  const id = (await portal("POST", `/upload?${query}`, form)).trim();
  const seen = [[`uploaded (${id})`, Date.now()]];
  console.log(`deployment ${id} (${type})`);
  // AUTOMATIC returns once Central is PUBLISHING: it can't fail validation
  // any more, and the rest (~11 min for alpha.68) is Central's own work,
  // which the maven-published job watches off the critical path.
  const target = type === "AUTOMATIC" ? "PUBLISHING" : "VALIDATED";
  if (process.env.GITHUB_OUTPUT)
    appendFileSync(process.env.GITHUB_OUTPUT, `deployment_id=${id}\nuploaded_at=${started}\n`);
  let state = "";
  // Up to 60 minutes: Central's publishing can take that long on a bad day.
  for (let i = 0; i < 360; i++) {
    const status = JSON.parse(await portal("POST", `/status?id=${encodeURIComponent(id)}`));
    if (status.deploymentState !== state) {
      state = status.deploymentState;
      seen.push([state, Date.now()]);
      console.log(`${new Date().toISOString()} ${state}`);
    }
    if (state === "FAILED") {
      // A re-run racing a deployment that is still publishing the same version.
      if (await isPublished(version)) break;
      throw new Error(`deployment ${id} failed: ${JSON.stringify(status.errors ?? status)}`);
    }
    if (state === target || (target === "PUBLISHING" && state === "PUBLISHED")) break;
    await new Promise((r) => setTimeout(r, 10_000));
  }
  if (state !== target && state !== "FAILED" && !(target === "PUBLISHING" && state === "PUBLISHED"))
    throw new Error(`deployment ${id} is ${state} after 60 minutes`);
  if (drop) {
    await portal("DELETE", `/deployment/${encodeURIComponent(id)}`);
    seen.push(["dropped (dry run)", Date.now()]);
  }
  const lines = [
    `### Maven Central (${type}): ${target} after ${Math.round((Date.now() - started) / 1000)}s`,
    "",
    "| state | reached after (s) |",
    "|---|---|",
    ...seen.map(([s, at]) => `| ${s} | ${Math.round((at - started) / 1000)} |`),
    "",
  ];
  summary(lines.join("\n"));
}

/** The POM URLs Maven Central serves for `version`: what consumers resolve. */
export function servedUrls(version) {
  return Object.keys(ARTIFACTS).map(
    (artifact) =>
      `https://repo1.maven.org/maven2/${GROUP}/${artifact}/${version}/${artifact}-${version}.pom`,
  );
}

/**
 * Waits until Maven Central serves both artifacts at `version` (no
 * credentials: repo1.maven.org is public), for at most `minutes`. A deployment
 * that FAILED never appears there, so it ends as a timeout.
 */
async function awaitServed(version, minutes, uploadedAt) {
  const started = Number(uploadedAt) || Date.now();
  const deadline = Date.now() + Number(minutes) * 60_000;
  const urls = servedUrls(version);
  let missing = urls;
  while (missing.length > 0 && Date.now() < deadline) {
    const still = [];
    for (const url of missing) {
      const response = await fetch(url, { method: "HEAD" }).catch(() => null);
      if (!response?.ok) still.push(url);
    }
    missing = still;
    if (missing.length > 0) await new Promise((r) => setTimeout(r, 20_000));
  }
  const after = Math.round((Date.now() - started) / 1000);
  if (missing.length > 0) {
    summary(
      `### Maven Central: NOT served ${after}s after upload (${minutes} min ceiling)\n\n${missing.map((u) => `- ${u}`).join("\n")}\n`,
    );
    process.exit(1);
  }
  summary(`### Maven Central: served on repo1.maven.org ${after}s after upload\n`);
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  if (command === "bundle") return bundle(args[0], args[1], args[2]);
  if (command === "published") process.exit((await isPublished(args[0])) ? 0 : 1);
  if (command === "upload") return upload(args[0], args[1], args[2], args.includes("--drop"));
  if (command === "await-served") return awaitServed(args[0], args[1], args[2]);
  throw new Error(
    "usage: maven-central.mjs bundle <tree> <version> <out.zip> | published <version> | upload <zip> <version> <type> [--drop]",
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}
