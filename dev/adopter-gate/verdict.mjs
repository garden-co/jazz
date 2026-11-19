#!/usr/bin/env node
// Collects every job's PASS/FAIL/REVIEW/SKIP lines into a verdict.
// Usage: verdict.mjs <results dir> [expected job names, comma-separated]
// Writes <results dir>/verdict.json and, in Actions, a table to $GITHUB_STEP_SUMMARY.
// Verdict: FAIL if any FAIL line or an expected job left no results; NEEDS_REVIEW if any REVIEW or SKIP; else PASS.
import fs from "node:fs";
import path from "node:path";
const [dir, expected = ""] = process.argv.slice(2);
const files = fs.existsSync(dir)
  ? fs.readdirSync(dir, { recursive: true }).filter((f) => f.endsWith(".txt"))
  : [];
const jobs = {};
const timings = [];
for (const f of files) {
  const name = path.basename(f, ".txt");
  const lines = fs
    .readFileSync(path.join(dir, f), "utf8")
    .split("\n")
    .filter((l) => /^(PASS|FAIL|REVIEW|SKIP) /.test(l));
  jobs[name] = lines;
  for (const l of fs.readFileSync(path.join(dir, f), "utf8").split("\n")) {
    const m = /^TIME (\S+) (\d+)s (.*)$/.exec(l);
    if (m) timings.push({ job: m[1], seconds: Number(m[2]), step: m[3] });
  }
}
for (const [name, ls] of Object.entries(jobs))
  if (!ls.length) jobs[name] = ["FAIL ran no checks (no PASS/FAIL lines)"];
for (const name of expected.split(",").filter(Boolean))
  if (!jobs[name])
    jobs[name] = ["FAIL no results (the job did not run or crashed before reporting)"];
const count = (k) =>
  Object.values(jobs)
    .flat()
    .filter((l) => l.startsWith(k + " ")).length;
const status = count("FAIL") ? "FAIL" : count("REVIEW") || count("SKIP") ? "NEEDS_REVIEW" : "PASS";
const sizesFile = fs
  .readdirSync(dir, { recursive: true })
  .find((f) => path.basename(f) === "sizes.json");
const sizes = sizesFile ? JSON.parse(fs.readFileSync(path.join(dir, sizesFile), "utf8")) : [];
const summary = Object.fromEntries(
  Object.entries(jobs).map(([n, ls]) => [
    n,
    ls.some((l) => l.startsWith("FAIL"))
      ? "FAIL"
      : ls.some((l) => /^(REVIEW|SKIP)/.test(l))
        ? "NEEDS_REVIEW"
        : ls.length
          ? "PASS"
          : "FAIL",
  ]),
);
const verdict = {
  status,
  counts: {
    pass: count("PASS"),
    fail: count("FAIL"),
    review: count("REVIEW"),
    skip: count("SKIP"),
  },
  jobs: summary,
  sizes,
  timings,
  attention: Object.entries(jobs).flatMap(([n, ls]) =>
    ls.filter((l) => !l.startsWith("PASS ")).map((l) => `${n}: ${l}`),
  ),
};
fs.writeFileSync(path.join(dir, "verdict.json"), JSON.stringify(verdict, null, 2));
const icon = { PASS: "✅", FAIL: "❌", NEEDS_REVIEW: "🟡" };
let md = `## Adopter gate: ${icon[status]} ${status}\n\n| Job | Result |\n|---|---|\n`;
for (const [n, s] of Object.entries(summary).sort()) md += `| ${n} | ${icon[s]} ${s} |\n`;
if (verdict.attention.length)
  md += `\n### Needs attention\n\n${verdict.attention.map((l) => `- ${l}`).join("\n")}\n`;
if (sizes.length)
  md += `\n### Sizes\n\n| What | Previous | RC | gzip change |\n|---|---|---|---|\n${sizes.map((s) => `| ${s.label} | ${s.gzip[0]} B | ${s.gzip[1]} B | ${s.pct >= 0 ? "+" : ""}${s.pct}% |`).join("\n")}\n`;
if (timings.length)
  md += `\n### Step times\n\n| Job | Step | s |\n|---|---|---|\n${timings.map((t) => `| ${t.job} | ${t.step} | ${t.seconds} |`).join("\n")}\n`;
if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, md);
console.log(md);
process.exit(status === "FAIL" ? 1 : 0);
