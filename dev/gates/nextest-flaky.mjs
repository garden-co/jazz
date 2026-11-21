/**
 * Flaky passes from a Nextest JUnit report: tests that failed at least once
 * and then passed on a retry.  Only tests with an explicit `retries` override
 * in .config/nextest.toml can retry, and each one that needed it is listed in
 * the job summary, so a flake stays visible instead of silently green.
 */
import fs from "node:fs";

const unescape = (value) =>
  value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");

const attribute = (tag, name) => {
  const match = tag.match(new RegExp(`\\s${name}="([^"]*)"`));
  return match ? unescape(match[1]) : null;
};

/** `[{ binary, test, failedAttempts }]` for each test that passed after failing. */
export function flakyPasses(junitXml) {
  const flaky = [];
  // Each <testcase ...> up to the next one (or the end of its suite).
  const cases = junitXml.split(/(?=<testcase\b)/).slice(1);
  for (const chunk of cases) {
    const open = chunk.match(/^<testcase\b[^>]*>/)?.[0];
    if (!open || open.endsWith("/>")) continue;
    const body = chunk.slice(open.length).split(/<\/testcase>/)[0];
    const failedAttempts = (body.match(/<flaky(Failure|Error)\b/g) ?? []).length;
    if (!failedAttempts) continue;
    // A test that failed every attempt carries <failure>: it is a failure, not a flake.
    if (/<(failure|error)\b/.test(body)) continue;
    flaky.push({
      binary: attribute(open, "classname"),
      test: attribute(open, "name"),
      failedAttempts,
    });
  }
  return flaky;
}

/** Reads `junitPath` (if Nextest wrote one), reports flaky passes, returns them. */
export function reportFlakyPasses(
  junitPath,
  { summaryPath = process.env.GITHUB_STEP_SUMMARY, log = console.log } = {},
) {
  if (!fs.existsSync(junitPath)) return [];
  const flaky = flakyPasses(fs.readFileSync(junitPath, "utf8"));
  if (!flaky.length) return flaky;
  for (const { binary, test, failedAttempts } of flaky) {
    // A GitHub annotation: shown on the run's summary page.
    log(
      `::warning title=Flaky pass::${binary} ${test} failed ${failedAttempts} time(s) before passing on retry`,
    );
  }
  if (summaryPath) {
    const rows = flaky
      .map(
        ({ binary, test, failedAttempts }) => `| \`${binary}\` | \`${test}\` | ${failedAttempts} |`,
      )
      .join("\n");
    fs.appendFileSync(
      summaryPath,
      `\n### Flaky passes (Rust)\n\nPassed only on a retry (see .config/nextest.toml):\n\n| binary | test | failed attempts |\n| --- | --- | --- |\n${rows}\n`,
    );
  }
  return flaky;
}
