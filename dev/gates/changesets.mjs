#!/usr/bin/env node
// Plans the next release from .changeset/ the way `changeset version` will, so a
// changeset it would refuse (for example one naming the private `@jazz/rust`
// next to `jazz-tools`) fails on its own PR instead of on the release PR.
//
// It also refuses a changeset that names only private packages: the release
// plan accepts that one but drops its note without a word.
//
// Unlike `changeset status`, this does not require a changeset for every change.
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);
const changesetsCli = createRequire(require.resolve("@changesets/cli/package.json"));
const getReleasePlan = changesetsCli("@changesets/get-release-plan").default;
const readChangesets = changesetsCli("@changesets/read").default;
const { getPackages } = changesetsCli("@manypkg/get-packages");

export async function planRelease(cwd) {
  const { packages } = await getPackages(cwd);
  const privateNames = new Set(
    packages.filter((pkg) => pkg.packageJson.private === true).map((pkg) => pkg.packageJson.name),
  );
  const naming = (await readChangesets(cwd))
    .filter((changeset) => changeset.releases.some((release) => privateNames.has(release.name)))
    .map((changeset) => changeset.id);
  if (naming.length > 0) throw new Error(`changesets name private packages: ${naming.join(", ")}`);
  return getReleasePlan(cwd);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const cwd = path.resolve(process.argv[2] ?? path.join(import.meta.dirname, "../.."));
  try {
    const plan = await planRelease(cwd);
    console.log(`changesets: ${plan.changesets.length} unreleased`);
  } catch (error) {
    console.error(`changesets: ${error.message}`);
    console.error(
      "Name only published packages; put Rust-facing notes under jazz-tools, whose version the crates ship at.",
    );
    process.exit(1);
  }
}
