#!/usr/bin/env node
/**
 * The release candidate gate's verdict (release-candidate.yml, job `verdict`):
 *   NO-GO  some leg failed: the run fails, so publish's release test gate
 *          (dev/scripts/release-test-gate.mjs) refuses this head;
 *   HOLD   every leg passed, but a leg asks a person to look (the Cloud RC
 *          probe's needs-infra, the adopter gate's NEEDS_REVIEW): the run
 *          succeeds, and each hold is listed by name and owner, here and in
 *          rc-verdict.json, for release-publish.yml to show where the publish
 *          approval is given. Clearing a hold is a person's step before go;
 *   GO     every leg passed, nothing held.
 *
 * Env: RESULTS (toJSON(needs)), DRY_RUN, DRY_RUN_SKIPPABLE (comma-separated
 * jobs a dry run skips), ALTERNATIVES (comma-separated groups of jobs joined
 * by `|`, of which exactly one runs: one success and the rest skipped pass as
 * one leg), PROBE_ONLY, PROBE (the probe's
 * verdict), ADOPTER (the adopter gate's status), ADOPTER_ATTENTION (its
 * attention lines, JSON array, optional).
 */
import { appendFileSync, writeFileSync } from "node:fs";

export function rcVerdict({
  needs,
  dryRun,
  probeOnly,
  dryRunSkippable = ["attach-server", "publish"],
  alternatives = [],
  probe = "",
  adopter = "",
  adopterAttention = [],
}) {
  // A dry run publishes nothing; a probe-only run runs nothing but the probe.
  const skippable = probeOnly
    ? new Set(Object.keys(needs).filter((job) => !["resolve", "cloud-rc-probe"].includes(job)))
    : dryRun
      ? new Set(dryRunSkippable)
      : new Set();
  for (const group of alternatives) {
    const results = group.map((job) => needs[job]?.result);
    if (
      results.filter((result) => result === "success").length === 1 &&
      results.every((result) => result === "success" || result === "skipped")
    )
      for (const job of group) skippable.add(job);
  }
  const jobs = Object.entries(needs).map(([job, { result }]) => {
    const ok = result === "success" || (skippable.has(job) && result === "skipped");
    let note = result;
    if (job === "cloud-rc-probe" && probe) note += ` (${probe})`;
    if (job === "adopter-verdict" && adopter) note += ` (${adopter})`;
    return { job, result, ok, note };
  });
  const holds = [];
  if (probe === "needs-infra")
    holds.push({
      name: "Cloud RC probe: needs-infra",
      owner: "#infra",
      detail: "the probe's verdict.json lists the held checks",
    });
  if (adopter === "NEEDS_REVIEW")
    holds.push({
      name: "Adopter gate: NEEDS_REVIEW",
      owner: "Anselm",
      detail:
        adopterAttention.length > 0
          ? adopterAttention.join("; ")
          : "see the adopter gate's verdict.json",
    });
  const verdict = jobs.some((job) => !job.ok) ? "NO-GO" : holds.length > 0 ? "HOLD" : "GO";
  return { verdict, holds, jobs };
}

export function summary({ verdict, holds, jobs }) {
  const lines = [`## Release candidate: ${verdict}`];
  if (verdict === "HOLD") {
    lines.push("", "HOLD: every leg passed; a person clears these before go:");
    for (const hold of holds)
      lines.push(`- **${hold.name}** (owner: ${hold.owner}): ${hold.detail}`);
  }
  lines.push("", ...jobs.map((job) => `- ${job.ok ? "✅" : "❌"} ${job.job}: ${job.note}`));
  return `${lines.join("\n")}\n`;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const result = rcVerdict({
    needs: JSON.parse(process.env.RESULTS ?? "{}"),
    dryRun: process.env.DRY_RUN === "true",
    probeOnly: process.env.PROBE_ONLY === "true",
    ...(process.env.DRY_RUN_SKIPPABLE
      ? { dryRunSkippable: process.env.DRY_RUN_SKIPPABLE.split(",") }
      : {}),
    alternatives: (process.env.ALTERNATIVES ?? "")
      .split(",")
      .filter(Boolean)
      .map((group) => group.split("|")),
    probe: process.env.PROBE ?? "",
    adopter: process.env.ADOPTER ?? "",
    adopterAttention: JSON.parse(process.env.ADOPTER_ATTENTION || "[]"),
  });
  const text = summary(result);
  process.stdout.write(text);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, text);
  writeFileSync("rc-verdict.json", `${JSON.stringify(result, null, 2)}\n`);
  if (process.env.GITHUB_OUTPUT)
    appendFileSync(process.env.GITHUB_OUTPUT, `verdict=${result.verdict}\n`);
  if (result.verdict === "NO-GO") process.exit(1);
}
