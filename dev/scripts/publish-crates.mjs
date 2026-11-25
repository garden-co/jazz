#!/usr/bin/env node
// Publishes Jazz's Rust crates to crates.io in dependency order. Used by
// .github/workflows/release-publish.yml (its crates legs); see dev/RELEASE_RUNBOOK.md, "Rust
// crates on crates.io".
//
//   node dev/scripts/publish-crates.mjs --list       the crates, in order
//   node dev/scripts/publish-crates.mjs --verify     cargo package, every crate together
//   node dev/scripts/publish-crates.mjs --dry-run [--no-verify]
//                                                    cargo publish --dry-run, every crate together
//   node dev/scripts/publish-crates.mjs --plan       which auth each unpublished crate needs
//   node dev/scripts/publish-crates.mjs --publish [--stop-before-oidc]
//                                                    publish each crate not yet on crates.io
//
// The workflow runs --plan first, which fails before anything is published
// if a crate's credential is missing, and proves the OIDC exchange works
// before the first publish. Then it publishes up to the first crate that
// needs OIDC (--stop-before-oidc), fetches a fresh OIDC token, and publishes
// the rest, so the 30-minute token can't expire during the bootstrap publishes.
//
// --publish picks a token per crate by asking crates.io whether the crate
// exists. An existing crate is published with CRATES_IO_OIDC_TOKEN (crates.io
// trusted publishing); a crate that doesn't exist yet can't have a trusted
// publisher, so it is published with CRATES_IO_BOOTSTRAP_TOKEN, a short-lived
// publish-new token. A version that is already on crates.io is skipped, so a
// failed run can be re-run.

import { spawnSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { cargoMetadata, publishPlan } from "./rust-crates.mjs";

const CRATES_IO = "https://crates.io/api/v1/crates";
const USER_AGENT = "jazz-release (https://github.com/garden-co/jazz)";

function cargo(args, env = {}) {
  console.log(`==> cargo ${args.join(" ")}`);
  const result = spawnSync("cargo", args, { stdio: "inherit", env: { ...process.env, ...env } });
  if (result.status !== 0) throw new Error(`cargo ${args[0]} failed (exit ${result.status})`);
}

/**
 * When crates.io's rate limit lets the next publish through, from its 429
 * message ("... Please try again after Tue, 17 Nov 2026 00:06:08 GMT", or
 * just "after 00:06:08 GMT"). Null if `output` isn't a rate-limit refusal.
 */
export function rateLimitRetryAt(output, now = new Date()) {
  // cargo reports the registry's answer as "(status 429 Too Many Requests)";
  // a bare "429" can be anything (a size like 429.1KiB).
  if (!/status 429\b|Too Many Requests/.test(output)) return null;
  const after = output.match(/try again after ([^\n]*?GMT)/);
  if (!after) return new Date(now.getTime() + 10 * 60_000);
  const full = Date.parse(after[1]);
  if (!Number.isNaN(full)) return new Date(full);
  const time = after[1].match(/(\d{1,2}):(\d{2}):(\d{2})/);
  if (!time) return new Date(now.getTime() + 10 * 60_000);
  const at = new Date(now);
  at.setUTCHours(Number(time[1]), Number(time[2]), Number(time[3]), 0);
  if (at <= now) at.setUTCDate(at.getUTCDate() + 1);
  return at;
}

/**
 * CRATES_IO_MAX_WAIT_MINUTES as a whole number of minutes: unset or empty
 * means the default (180); anything else that isn't a non-negative integer
 * is refused, so a typo can't remove the cap.
 */
export function maxWaitMinutes(value) {
  if (value === undefined || value.trim() === "") return 180;
  if (!/^\d+$/.test(value.trim()))
    throw new Error(`CRATES_IO_MAX_WAIT_MINUTES must be a whole number of minutes, got '${value}'`);
  return Number(value.trim());
}

/**
 * `cargo publish` for one crate. On crates.io's rate limit (new crates: a
 * burst of 5, then 1 per 10 minutes) it waits until the time crates.io names,
 * plus a margin, and tries again, for at most CRATES_IO_MAX_WAIT_MINUTES
 * (default 180) in all, so one approved run publishes every new crate.
 */
let waitedMs = 0;
async function publishCrate(name, token) {
  const maxWaitMs = maxWaitMinutes(process.env.CRATES_IO_MAX_WAIT_MINUTES) * 60_000;
  const args = ["publish", "--allow-dirty", "--no-verify", "-p", name];
  for (;;) {
    console.log(`==> cargo ${args.join(" ")}`);
    const result = spawnSync("cargo", args, {
      encoding: "utf8",
      env: { ...process.env, CARGO_REGISTRY_TOKEN: token },
      maxBuffer: 64 * 1024 * 1024,
    });
    process.stdout.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    if (result.status === 0) return;
    const retryAt = rateLimitRetryAt(`${result.stdout}\n${result.stderr}`);
    if (!retryAt) throw new Error(`cargo publish -p ${name} failed (exit ${result.status})`);
    const waitMs = Math.max(0, retryAt.getTime() - Date.now()) + 15_000;
    if (waitedMs + waitMs > maxWaitMs)
      throw new Error(
        `crates.io rate limit: ${name} would wait past CRATES_IO_MAX_WAIT_MINUTES; re-run to resume`,
      );
    console.log(
      `==> crates.io rate limit: waiting until ${new Date(Date.now() + waitMs).toISOString()} to publish ${name}`,
    );
    waitedMs += waitMs;
    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }
}

/** `{ exists, published }` for one crate and version, from the crates.io API. */
export async function registryState(name, version, fetchImpl = fetch) {
  const headers = { "User-Agent": USER_AGENT, Accept: "application/json" };
  const crate = await fetchImpl(`${CRATES_IO}/${name}`, { headers });
  if (crate.status === 404) return { exists: false, published: false };
  if (!crate.ok) throw new Error(`crates.io ${name}: HTTP ${crate.status}`);
  const release = await fetchImpl(`${CRATES_IO}/${name}/${version}`, { headers });
  if (release.status === 404) return { exists: true, published: false };
  if (!release.ok) throw new Error(`crates.io ${name}@${version}: HTTP ${release.status}`);
  return { exists: true, published: true };
}

const OIDC = ["CRATES_IO_OIDC_TOKEN", "trusted publishing"];
const BOOTSTRAP = ["CRATES_IO_BOOTSTRAP_TOKEN", "bootstrap token (new crate)"];

async function main() {
  const mode = process.argv[2];
  const stopBeforeOidc = process.argv.includes("--stop-before-oidc");
  // With the vendored crates as members the lockfile gains their optional
  // dependencies, so cargo runs without --locked here; Cargo.lock is restored
  // afterwards, and CI checks it with --locked everywhere else.
  const plan = publishPlan(cargoMetadata({ locked: false }));
  if (plan.problems.length > 0) {
    throw new Error(
      `The workspace can't be published as is:\n${plan.problems.map((p) => `  - ${p}`).join("\n")}`,
    );
  }
  const packages = plan.order.flatMap((name) => ["-p", name]);

  switch (mode) {
    case "--list":
      for (const name of plan.order) console.log(`${name} ${plan.byName.get(name).version}`);
      return;
    // Cargo 1.90+ packages local path dependencies together, through a
    // temporary registry, so crates that aren't on crates.io yet verify.
    case "--verify":
      cargo(["package", "--allow-dirty", ...packages]);
      return;
    case "--dry-run":
      // --no-verify: packaging only, when --verify already ran (release-publish.yml).
      cargo([
        "publish",
        "--dry-run",
        "--allow-dirty",
        ...(process.argv.includes("--no-verify") ? ["--no-verify"] : []),
        ...packages,
      ]);
      return;
    case "--plan":
    case "--publish":
      break;
    default:
      throw new Error(
        "Usage: publish-crates.mjs --list | --verify | --dry-run | --plan | --publish [--stop-before-oidc]",
      );
  }

  if (mode === "--plan") {
    const needs = [];
    let needsOidc = false;
    for (const name of plan.order) {
      const { version } = plan.byName.get(name);
      const state = await registryState(name, version);
      const auth = state.published ? "already published" : state.exists ? OIDC[1] : BOOTSTRAP[1];
      console.log(`${name}@${version}: ${auth}`);
      if (state.published) continue;
      if (state.exists) needsOidc = true;
      else if (!process.env[BOOTSTRAP[0]]) needs.push(name);
    }
    if (process.env.GITHUB_OUTPUT)
      appendFileSync(process.env.GITHUB_OUTPUT, `needs_oidc=${needsOidc}\n`);
    if (needs.length > 0)
      throw new Error(
        `New crates need ${BOOTSTRAP[0]} (a publish-new token in the publish environment): ${needs.join(", ")}`,
      );
    return;
  }

  const summary = [];
  for (const name of plan.order) {
    const { version } = plan.byName.get(name);
    const state = await registryState(name, version);
    if (state.published) {
      console.log(`==> ${name}@${version} is already on crates.io, skipping`);
      summary.push(`${name}@${version}: already published`);
      continue;
    }
    const [variable, auth] = state.exists ? OIDC : BOOTSTRAP;
    if (state.exists && stopBeforeOidc) {
      console.log(`==> stopping before ${name}@${version}, which needs ${auth}`);
      break;
    }
    const token = process.env[variable];
    if (!token) {
      throw new Error(
        `${name}@${version} needs ${auth}, but ${variable} is not set. ` +
          (state.exists
            ? `Check ${name}'s trusted publisher on crates.io (workflow release-publish.yml, environment publish).`
            : "Store a publish-new CRATES_IO_BOOTSTRAP_TOKEN in the publish environment."),
      );
    }
    // `--verify` already built every crate against the others; publishing
    // without it keeps the run inside the trusted-publishing token's lifetime.
    // Cargo waits until each crate is in the index before the next.
    console.log(`==> ${name}@${version} via ${auth}`);
    await publishCrate(name, token);
    summary.push(`${name}@${version}: published via ${auth}`);
  }
  console.log("\nCrates:");
  for (const line of summary) console.log(`  ${line}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}
