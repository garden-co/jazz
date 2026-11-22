#!/usr/bin/env node
// The release candidate gate's verdict, for release-publish.yml's
// pre-approval summary. The newest completed release-candidate.yml run on the
// Version Packages head writes `rc-verdict` (rc-verdict.json):
//   GO      nothing held;
//   HOLD    every leg passed but some ask a person (`holds`, each with an owner):
//           shown at the top of the summary, for whoever approves `publish`;
//   NO-GO   a leg failed: refused (in a dry run, reported only).
// Gate runs from before rc-verdict existed have none: said so, not refused
// (release-test-gate.mjs still requires the runs to have passed).
//
//   HEAD_SHA=<vp head> MODE=publish|dry-run GH_REPO=owner/repo node dev/release-publish/rc-verdict.mjs
import { execFileSync } from "node:child_process";
import { appendFileSync, mkdtempSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

/** `{ lines, annotations, refuse }` for a verdict (null: none found) and its run. */
export function summarize(verdict, run) {
  const where = run
    ? `[release-candidate run ${run.id}](${run.html_url})`
    : "no release-candidate run";
  if (!run)
    return {
      lines: [`### RC gate: no release-candidate.yml run on this head`, ""],
      annotations: [
        [
          "warning",
          "No release-candidate.yml run on the Version Packages head: no RC verdict to show.",
        ],
      ],
      refuse: false,
    };
  if (!verdict) {
    const failed = run.conclusion !== "success";
    return {
      lines: [
        `### RC gate: ${failed ? "NO-GO" : "no verdict"} (${where})`,
        "",
        failed
          ? `The newest gate run concluded \`${run.conclusion}\`.`
          : "This gate run predates `rc-verdict`: no holds can be shown. Check the run before approving.",
        "",
      ],
      annotations: [
        failed
          ? ["error", `RC gate run ${run.id} concluded ${run.conclusion}: NO-GO.`]
          : ["warning", `RC gate run ${run.id} has no rc-verdict (it predates it): holds unknown.`],
      ],
      refuse: failed,
    };
  }
  const holds = Array.isArray(verdict.holds) ? verdict.holds : [];
  const state = verdict.verdict;
  if (!["GO", "HOLD", "NO-GO"].includes(state))
    throw new Error(`rc-verdict.json has verdict ${JSON.stringify(state)}`);
  const lines = [`### RC gate: ${state} (${where})`, ""];
  const annotations = [];
  if (holds.length > 0) {
    lines.push(
      `**${holds.length} hold${holds.length === 1 ? "" : "s"} to decide before approving \`publish\`:**`,
      "",
    );
    for (const h of holds) {
      lines.push(`- **${h.name}** (owner: ${h.owner ?? "?"})${h.detail ? `: ${h.detail}` : ""}`);
      annotations.push([
        "warning",
        `HOLD: ${h.name} (owner: ${h.owner ?? "?"})${h.detail ? `: ${h.detail}` : ""}`,
      ]);
    }
    lines.push("");
  }
  const failedJobs = (verdict.jobs ?? []).filter((j) => j.ok === false);
  if (failedJobs.length > 0) {
    lines.push(
      "Failed legs:",
      "",
      ...failedJobs.map((j) => `- ${j.job}: ${j.result}${j.note ? ` (${j.note})` : ""}`),
      "",
    );
  }
  if (state === "NO-GO") annotations.unshift(["error", `RC gate run ${run.id}: NO-GO.`]);
  return { lines, annotations, refuse: state === "NO-GO" };
}

/**
 * The gate run whose verdict counts, of the completed release-candidate.yml
 * runs on the head: never a Cloud RC probe-only run (it runs no gate); a real
 * candidate ("Release candidate v<version>") over a dry run when one exists;
 * the newest of those. Null when none is left.
 */
export function pickGateRun(runs) {
  const gates = runs.filter((r) => !(r.display_title ?? "").startsWith("Cloud RC probe only"));
  const real = gates.filter((r) => (r.display_title ?? "").startsWith("Release candidate v"));
  const pool = real.length > 0 ? real : gates;
  return [...pool].sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))[0] ?? null;
}

function gh(args) {
  return execFileSync("gh", args, {
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function main() {
  const { HEAD_SHA: head, MODE: mode, GH_REPO: repo } = process.env;
  if (!/^[0-9a-f]{40}$/.test(head ?? "")) throw new Error("HEAD_SHA must be a full commit SHA");
  const runs =
    JSON.parse(
      gh([
        "api",
        `repos/${repo}/actions/workflows/release-candidate.yml/runs?head_sha=${head}&status=completed&per_page=20`,
      ]),
    ).workflow_runs ?? [];
  const run = pickGateRun(runs);
  let verdict = null;
  if (run) {
    const dir = mkdtempSync(join(tmpdir(), "rc-verdict-"));
    try {
      gh(["run", "download", String(run.id), "-R", repo, "-n", "rc-verdict", "-D", dir]);
    } catch {
      // No rc-verdict: an older gate run.
    }
    const file = join(dir, "rc-verdict.json");
    if (existsSync(file)) verdict = JSON.parse(readFileSync(file, "utf8"));
  }
  const { lines, annotations, refuse } = summarize(verdict, run);
  for (const [level, text] of annotations) console.log(`::${level}::${text}`);
  console.log(lines.join("\n"));
  if (process.env.GITHUB_STEP_SUMMARY)
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${lines.join("\n")}\n`);
  if (refuse && mode === "publish") {
    console.error("The newest RC gate run is NO-GO: not publishing.");
    process.exit(1);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
