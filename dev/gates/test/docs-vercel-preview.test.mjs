import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";

const root = path.resolve(import.meta.dirname, "../../..");
const workflowPath = path.join(root, ".github/workflows/docs-vercel-preview.yml");
const docsPackagePath = path.join(root, "docs/package.json");
const docsReadmePath = path.join(root, "docs/README.md");
const lockfilePath = path.join(root, "pnpm-lock.yaml");
const workflowSource = fs.readFileSync(workflowPath, "utf8");
const docsPackageSource = fs.readFileSync(docsPackagePath, "utf8");
const docsReadmeSource = fs.readFileSync(docsReadmePath, "utf8");
const lockfileSource = fs.readFileSync(lockfilePath, "utf8");

const trustedAssociations = new Set(["OWNER", "MEMBER", "COLLABORATOR"]);
const previewWouldRun = ({
  event = "pull_request",
  action = "labeled",
  number = 1,
  labels = [],
  sameRepository = true,
  association = "MEMBER",
}) =>
  Number.isInteger(number) &&
  number > 0 &&
  event === "pull_request" &&
  ["labeled", "synchronize", "reopened"].includes(action) &&
  sameRepository &&
  trustedAssociations.has(association) &&
  labels.includes("docs");

function readWorkflow(source) {
  return parse(source);
}

function previewSteps(workflow) {
  return workflow.jobs.preview.steps;
}

function assertPreviewWorkflowContract(source, packageSource = docsPackageSource) {
  const workflow = readWorkflow(source);
  const condition = workflow.jobs.preview.if.replace(/\s+/g, " ");
  const steps = previewSteps(workflow);
  const commands = steps
    .map((step) => step.run)
    .filter(Boolean)
    .join("\n");
  const guardIndex = steps.findIndex((step) => step.name === "Guard Vercel preview configuration");
  const checkoutIndex = steps.findIndex((step) => step.uses?.startsWith("actions/checkout@"));
  const deploySteps = steps.filter((step) => step.run?.includes("vercel deploy"));

  assert.deepEqual(workflow.on.pull_request.types, [
    "labeled",
    "unlabeled",
    "synchronize",
    "reopened",
    "closed",
  ]);
  assert.equal(workflow.on.push, undefined, "production must not be triggerable before release");
  assert.equal(workflow.on.pull_request_target, undefined);
  assert.equal(workflow.on.workflow_dispatch, undefined);
  assert.equal(
    workflow.concurrency.group,
    "docs-vercel-preview-${{ github.event.pull_request.number }}",
    "all PR events, including close, must share one cancellation group",
  );
  assert.equal(workflow.concurrency["cancel-in-progress"], true);

  assert.match(condition, /head\.repo\.full_name == github\.repository/);
  assert.match(condition, /\["OWNER","MEMBER","COLLABORATOR"\]/);
  assert.match(condition, /labels\.\*\.name, 'docs'/);
  assert.match(condition, /event\.action != 'closed'/);
  assert.deepEqual(workflow.jobs.preview.permissions, { contents: "read" });
  assert.equal(workflow.jobs.preview.env, undefined, "PR-code steps must not inherit credentials");

  assert.ok(guardIndex >= 0 && guardIndex < checkoutIndex, "guard must precede checkout");
  assert.match(steps[guardIndex].run, /\$\{!name\}/);
  assert.doesNotMatch(steps[guardIndex].run, /echo.*\$\{!name\}/);

  assert.equal(deploySteps.length, 1, "there must be one credentialed Vercel deployment step");
  assert.match(deploySteps[0].run, /pnpm --filter docs exec vercel deploy --yes/);
  assert.match(deploySteps[0].run, /git_sha="\$\{\{ github\.event\.pull_request\.head\.sha \}\}"/);
  assert.match(deploySteps[0].env.VERCEL_TOKEN, /secrets\.VERCEL_DOCS_TOKEN/);
  assert.match(deploySteps[0].env.VERCEL_PROJECT_ID, /vars\.VERCEL_DOCS_PROJECT_ID/);
  for (const step of steps) {
    if (step !== deploySteps[0] && step !== steps[guardIndex]) {
      assert.doesNotMatch(JSON.stringify(step), /VERCEL_(?:TOKEN|ORG_ID|PROJECT_ID)/);
    }
  }
  assert.match(commands, /pnpm build:vercel-docs/);
  assert.doesNotMatch(commands, /\b(?:pnpm dlx|npx)\s+vercel/);
  assert.doesNotMatch(commands, /\bvercel@(?:latest|\^|~)/);
  assert.doesNotMatch(commands, /\b(?:pull|build)\s+.*vercel|vercel\s+(?:pull|build)/);
  assert.doesNotMatch(commands, /\b--prebuilt\b/);
  assert.doesNotMatch(commands, /\b--prod\b/);
  assert.match(workflow.jobs.production.if.replace(/\s+/g, " "), /false/);

  const docsPackage = JSON.parse(packageSource);
  assert.match(docsPackage.devDependencies.vercel, /^\d+\.\d+\.\d+$/, "Vercel must be exact");
  assert.match(lockfileSource, new RegExp(`vercel@${docsPackage.devDependencies.vercel.replaceAll(".", "\\.")}:`));
  assert.match(docsReadmeSource, /disable or disconnect its automatic\s+Git deployments/i);
  assert.match(docsReadmeSource, /Preview\*\* environment must contain no sensitive values/);
}

test("docs preview event truth table is explicit", () => {
  const cases = [
    [{ number: 42, labels: ["docs"] }, true, "labeled trusted branch"],
    [{ number: 43, labels: ["docs"] }, true, "stacked child PR has its own preview group"],
    [{ action: "synchronize", labels: ["docs"] }, true, "new commit retains label"],
    [{ action: "reopened", labels: ["docs"] }, true, "reopen retains label"],
    [{ action: "unlabeled", labels: [] }, false, "label removal only cancels"],
    [{ action: "closed", labels: ["docs"] }, false, "close only cancels"],
    [{ action: "synchronize", labels: [] }, false, "unlabeled branch"],
    [{ labels: ["docs"], sameRepository: false }, false, "fork"],
    [{ labels: ["docs"], association: "NONE" }, false, "untrusted author"],
    [{ event: "pull_request_target", labels: ["docs"] }, false, "target event"],
    [{ event: "push", labels: ["docs"] }, false, "push"],
    [{ event: "workflow_dispatch", labels: ["docs"] }, false, "manual dispatch"],
  ];
  for (const [input, expected, description] of cases) {
    assert.equal(previewWouldRun(input), expected, description);
  }
});

test("workflow enforces tokenless local build and one pinned remote preview deploy", () => {
  assertPreviewWorkflowContract(workflowSource);
});

test("contract tests reject planted close, credential, and version regressions", () => {
  assert.throws(
    () => assertPreviewWorkflowContract(workflowSource.replace("&& github.event.action != 'closed'", "")),
    (error) => error.message.includes("event\\.action != 'closed'"),
  );
  assert.throws(
    () =>
      assertPreviewWorkflowContract(
        workflowSource.replace("run: pnpm build:vercel-docs", "env:\n          VERCEL_TOKEN: leaked\n        run: pnpm build:vercel-docs"),
      ),
    (error) => error.message.includes("VERCEL_(?:TOKEN|ORG_ID|PROJECT_ID)"),
  );
  assert.throws(
    () => assertPreviewWorkflowContract(workflowSource, docsPackageSource.replace('"59.10.0"', '"^59.10.0"')),
    /Vercel must be exact/,
  );
});
