#!/usr/bin/env node

/**
 * Select the source commit whose native artifacts are being verified by an
 * alpha release. A successful PR preview is reusable after its release PR is
 * merged because the release workflow separately proves the source trees are
 * equivalent. The merge commit is nevertheless a different commit object, so
 * its SHA must not be substituted for the SHA sealed into the preview's
 * native-artifact manifests.
 */
function commit(name, value) {
  if (!/^[0-9a-f]{40}$/i.test(value ?? ""))
    throw new Error(`${name} must be an exact 40-character commit SHA`);
  return value;
}

const reuse = process.env.JAZZ_REUSE_PREVIEW_ARTIFACTS;
if (reuse !== "true" && reuse !== "false")
  throw new Error("JAZZ_REUSE_PREVIEW_ARTIFACTS must be true or false");

const current = commit("GITHUB_SHA", process.env.GITHUB_SHA);
const source =
  reuse === "true"
    ? commit("JAZZ_RELEASE_PR_HEAD_SHA", process.env.JAZZ_RELEASE_PR_HEAD_SHA)
    : current;

process.stdout.write(`${source}\n`);
