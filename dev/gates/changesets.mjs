#!/usr/bin/env node
// Plans the next release from .changeset/ the way `changeset version` will, so a
// changeset it would refuse (for example one naming the private `@jazz/rust`
// next to `jazz-tools`) fails on its own PR instead of on the release PR.
//
// Unlike `changeset status`, this does not require a changeset for every change.
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);
const changesetsCli = createRequire(require.resolve("@changesets/cli/package.json"));
const getReleasePlan = changesetsCli("@changesets/get-release-plan").default;

export async function planRelease(cwd) {
  return getReleasePlan(cwd);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const cwd = path.resolve(process.argv[2] ?? path.join(import.meta.dirname, "../.."));
  try {
    const plan = await planRelease(cwd);
    console.log(
      `changesets: ${plan.changesets.length} unreleased, ${plan.releases.length} packages to release`,
    );
  } catch (error) {
    console.error(`changesets: \`changeset version\` would fail: ${error.message}`);
    console.error(
      "Private packages (such as @jazz/rust) can't appear in a changeset; put Rust-facing notes under jazz-tools.",
    );
    process.exit(1);
  }
}
