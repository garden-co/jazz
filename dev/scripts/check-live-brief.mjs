#!/usr/bin/env node
// The agent brief `npm create jazz` prints is fetched live from the docs site
// (packages/create-jazz/src/brief.ts), so docs prod is the brief. This checks
// that the live /quickstart.md matches docs/public/quickstart.md in this repo.
//
// usage: check-live-brief.mjs [--url <url>] [--ref <git ref>] [--grace-hours <n>]
//
//   --url          the live brief (default https://jazz.tools/quickstart.md)
//   --ref          the commit whose docs/public/quickstart.md to compare with
//                  (default HEAD)
//   --grace-hours  pass while they differ if the brief changed at --ref less
//                  than this long ago, so a docs deploy can follow a merge
//                  (default 0: any difference fails; nightly uses 24)
//
// Run it as the last step of a docs prod deploy (no grace), and nightly
// against main (with --grace-hours 24). Exit codes: 0 the same (or within the
// grace period), 1 different, 2 the live brief couldn't be fetched.
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

export const BRIEF_PATH = "docs/public/quickstart.md";
export const LIVE_URL = "https://jazz.tools/quickstart.md";

/** The brief with line endings and trailing whitespace normalized. */
export function normalize(text) {
  return text
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.trimEnd())
    .join("\n")
    .trim();
}

/** The first differing lines, numbered, for the log. */
export function firstDifferences(expected, actual, limit = 8) {
  const want = expected.split("\n");
  const got = actual.split("\n");
  const out = [];
  for (let i = 0; i < Math.max(want.length, got.length) && out.length < limit; i++) {
    if (want[i] === got[i]) continue;
    out.push(
      `line ${i + 1}:`,
      `  repo: ${want[i] ?? "(missing)"}`,
      `  live: ${got[i] ?? "(missing)"}`,
    );
  }
  return out;
}

/**
 * Compare `expected` (the repo's brief) with `actual` (the live one). Within
 * `graceMs` of the brief's last change (`changedAt`, ms), a difference passes.
 */
export function verdict({ expected, actual, changedAt, now = Date.now(), graceMs = 0 }) {
  if (normalize(expected) === normalize(actual)) return { ok: true, reason: "the same" };
  const age = now - changedAt;
  if (age < graceMs) {
    return {
      ok: true,
      reason: `different, but the brief changed ${Math.round(age / 3_600_000)} h ago (grace ${Math.round(graceMs / 3_600_000)} h)`,
    };
  }
  return {
    ok: false,
    reason: "different",
    diff: firstDifferences(normalize(expected), normalize(actual)),
  };
}

export async function fetchLive(url, fetchImpl = fetch) {
  const response = await fetchImpl(url, {
    headers: { "cache-control": "no-cache" },
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`${url} answered HTTP ${response.status}`);
  return response.text();
}

function git(...args) {
  return execFileSync("git", args, { encoding: "utf8" });
}

async function main() {
  const { values } = parseArgs({
    options: {
      url: { type: "string", default: LIVE_URL },
      ref: { type: "string", default: "HEAD" },
      "grace-hours": { type: "string", default: "0" },
    },
  });
  const expected = git("show", `${values.ref}:${BRIEF_PATH}`);
  const changedAt = Number(git("log", "-1", "--format=%ct", values.ref, "--", BRIEF_PATH)) * 1000;
  let actual;
  try {
    actual = await fetchLive(values.url);
  } catch (error) {
    console.error(`Could not fetch the live brief: ${error.message}`);
    process.exit(2);
  }
  const result = verdict({
    expected,
    actual,
    changedAt,
    graceMs: Number(values["grace-hours"]) * 3_600_000,
  });
  const where = `${values.url} vs ${BRIEF_PATH} at ${values.ref}`;
  if (result.ok) {
    console.log(`Live brief ${result.reason}: ${where}`);
    return;
  }
  console.error(`Live brief differs from the repo: ${where}`);
  console.error("npm create jazz prints the live one. Deploy docs prod from this commit.");
  for (const line of result.diff) console.error(line);
  process.exit(1);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}
