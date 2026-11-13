#!/usr/bin/env node
// After a pkg.pr.new preview publish, check that every published package
// installs from its short-SHA URL (the form pkg.pr.new prints and people
// copy), and list those install commands in the job summary. A package
// that only resolves by full SHA fails the job instead of a later install.
//
// usage: check-pkg-pr-new-urls.mjs <full commit sha> <package dir>...
import { appendFileSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const repository = "garden-co/jazz";
export const shortShaLength = 7;

/** The short-SHA install URL pkg.pr.new serves for `name` at `commit`. */
export function previewUrl(name, commit) {
  if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error(`expected a full commit SHA, got ${commit}`);
  return `https://pkg.pr.new/${repository}/${name}@${commit.slice(0, shortShaLength)}`;
}

/** Markdown listing one `pnpm add` line per package. */
export function summary(urls) {
  return [
    "### pkg.pr.new preview",
    "",
    "```sh",
    ...urls.map((url) => `pnpm add ${url}`),
    "```",
    "",
  ].join("\n");
}

/**
 * Whether `url` serves a tarball, retrying while pkg.pr.new settles. Each
 * attempt is bounded by `timeoutMs`, so one URL gives up after about
 * attempts × (timeoutMs + delayMs) at worst (~4 minutes by default).
 */
export async function resolves(
  url,
  { attempts = 12, delayMs = 10_000, timeoutMs = 10_000, fetchImpl = fetch } = {},
) {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetchImpl(url, {
        method: "HEAD",
        redirect: "follow",
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (response.ok) return true;
    } catch {
      // Network hiccup: retry like a non-2xx answer.
    }
    if (attempt < attempts) await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  return false;
}

async function main([commit, ...directories]) {
  if (!commit || directories.length === 0)
    throw new Error("usage: check-pkg-pr-new-urls.mjs <full commit sha> <package dir>...");
  const urls = directories.map((directory) =>
    previewUrl(JSON.parse(readFileSync(path.join(directory, "package.json"), "utf8")).name, commit),
  );
  // Check all packages at once, so an outage costs one URL's retry budget
  // rather than one per package.
  const results = await Promise.all(urls.map((url) => resolves(url)));
  const missing = urls.filter((_, index) => !results[index]);
  for (const [index, url] of urls.entries())
    console.log(`${results[index] ? "ok     " : "missing"} ${url}`);
  if (process.env.GITHUB_STEP_SUMMARY)
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary(urls));
  if (missing.length > 0) {
    for (const url of missing) console.log(`::error::pkg.pr.new does not serve ${url}`);
    process.exit(1);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  await main(process.argv.slice(2));
