// The release candidate gate's Cloud RC probe and adopter gate legs.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";

const root = path.resolve(import.meta.dirname, "../..");
const rc = parse(
  fs.readFileSync(path.join(root, ".github/workflows/release-candidate.yml"), "utf8"),
);
const jobs = rc.jobs;

test("the probe runs from the artifacts, after proving both runs built this commit", () => {
  const probe = jobs["cloud-rc-probe"];
  assert.deepEqual(probe.needs, ["resolve", "artifacts", "rc-tarballs"]);
  const runs = probe.steps.map((step) => step.run ?? "");
  const check = runs.findIndex((run) => run.includes("dev/scripts/require-run-commit.mjs"));
  const dispatch = runs.findIndex((run) => run.includes("jazz_artifacts_run_ids"));
  assert.ok(check !== -1 && dispatch !== -1 && check < dispatch);
  assert.match(runs[check], /RUN_ID="\$\{ARTIFACTS_RUN_ID\}"/);
  assert.match(runs[check], /RUN_ID="\$\{GITHUB_RUN_ID\}"/);
  assert.equal(probe.permissions?.actions, "read");
});

test("a dry run can never run t4 against the long-lived eval core", () => {
  const adopter = jobs["adopter-gate"];
  assert.equal(adopter.uses, "./.github/workflows/adopter-gate.yml");
  assert.equal(
    adopter.with.t4_mode,
    "${{ needs.resolve.outputs.dry_run == 'true' && 'preview' || 'production' }}",
  );
  assert.match(adopter.with.rc_tag, /needs\.resolve\.outputs\.dry_run != 'true'/);
  assert.equal(
    rc.on.workflow_dispatch.inputs.t4_mode,
    undefined,
    "t4_mode is derived, never an input",
  );
});

test("a NO-GO candidate is never published: publish waits for every pre-publish gate", () => {
  const publish = jobs.publish;
  for (const gate of ["examples-e2e", "preview", "rc-tarballs", "adopter-verdict", "attach-server"])
    assert.ok(publish.needs.includes(gate), `publish needs ${gate}`);
  assert.match(publish.if, /needs\.adopter-verdict\.outputs\.status == 'PASS'/);
  assert.doesNotMatch(publish.if, /FAIL/);
});

test("the verdict names the adopter gate's NEEDS_REVIEW as a hold", () => {
  const verdict = jobs.verdict;
  assert.ok(verdict.needs.includes("adopter-verdict") && verdict.needs.includes("cloud-rc-probe"));
  const step = verdict.steps.find((s) => s.id === "verdict");
  assert.equal(step.env.ADOPTER, "${{ needs.adopter-verdict.outputs.status }}");
  assert.equal(
    step.env.DRY_RUN_SKIPPABLE,
    "attach-server,publish",
    "the probe and adopter legs run in dry runs too",
  );
});
