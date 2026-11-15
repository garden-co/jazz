#!/usr/bin/env node
// Publishes Jazz's Rust crates to crates.io in dependency order. Used by
// .github/workflows/publish-crates.yml; see dev/RELEASE_RUNBOOK.md, "Rust
// crates on crates.io".
//
//   node dev/scripts/publish-crates.mjs --list       the crates, in order
//   node dev/scripts/publish-crates.mjs --verify     cargo package, every crate together
//   node dev/scripts/publish-crates.mjs --dry-run    cargo publish --dry-run, every crate together
//   node dev/scripts/publish-crates.mjs --publish    publish each crate not yet on crates.io
//
// --publish picks a token per crate by asking crates.io whether the crate
// exists. An existing crate is published with CRATES_IO_OIDC_TOKEN (crates.io
// trusted publishing); a crate that doesn't exist yet can't have a trusted
// publisher, so it is published with CRATES_IO_BOOTSTRAP_TOKEN, a short-lived
// publish-new token. A version that is already on crates.io is skipped, so a
// failed run can be re-run.

import { spawnSync } from "node:child_process";
import { cargoMetadata, publishPlan, withVendoredMembers } from "./rust-crates.mjs";

const CRATES_IO = "https://crates.io/api/v1/crates";
const USER_AGENT = "jazz-release (https://github.com/garden-co/jazz)";

function cargo(args, env = {}) {
  console.log(`==> cargo ${args.join(" ")}`);
  const result = spawnSync("cargo", args, { stdio: "inherit", env: { ...process.env, ...env } });
  if (result.status !== 0) throw new Error(`cargo ${args[0]} failed (exit ${result.status})`);
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

async function main() {
  const mode = process.argv[2];
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
      cargo(["publish", "--dry-run", "--allow-dirty", ...packages]);
      return;
    case "--publish":
      break;
    default:
      throw new Error("Usage: publish-crates.mjs --list | --verify | --dry-run | --publish");
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
    const [variable, auth] = state.exists
      ? ["CRATES_IO_OIDC_TOKEN", "trusted publishing"]
      : ["CRATES_IO_BOOTSTRAP_TOKEN", "bootstrap token (new crate)"];
    const token = process.env[variable];
    if (!token) {
      throw new Error(
        `${name}@${version} needs ${auth}, but ${variable} is not set. ` +
          (state.exists
            ? `Check ${name}'s trusted publisher on crates.io (workflow publish-crates.yml, environment publish).`
            : "Store a publish-new CRATES_IO_BOOTSTRAP_TOKEN in the publish environment."),
      );
    }
    // `--verify` already built every crate against the others; publishing
    // without it keeps the run inside the trusted-publishing token's lifetime.
    // Cargo waits until each crate is in the index before the next.
    console.log(`==> ${name}@${version} via ${auth}`);
    cargo(["publish", "--allow-dirty", "--no-verify", "-p", name], {
      CARGO_REGISTRY_TOKEN: token,
    });
    summary.push(`${name}@${version}: published via ${auth}`);
  }
  console.log("\nCrates:");
  for (const line of summary) console.log(`  ${line}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  withVendoredMembers(main).catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}
