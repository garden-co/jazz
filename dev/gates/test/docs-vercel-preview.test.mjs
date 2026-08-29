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

function assertPreviewWorkflowContract(source, packageSource = docsPackageSource) {
  const workflow = readWorkflow(source);
  const build = workflow.jobs.build;
  const deploy = workflow.jobs.deploy;
  const buildCondition = build.if.replace(/\s+/g, " ");
  const deployCondition = deploy.if.replace(/\s+/g, " ");
  const buildSteps = build.steps;
  const deploySteps = deploy.steps;
  const buildCommands = buildSteps
    .map((step) => step.run)
    .filter(Boolean)
    .join("\n");
  const deployCommands = deploySteps
    .map((step) => step.run)
    .filter(Boolean)
    .join("\n");
  const buildCheckoutIndex = buildSteps.findIndex((step) => step.uses?.startsWith("actions/checkout@"));
  const deployCheckoutIndex = deploySteps.findIndex((step) => step.uses?.startsWith("actions/checkout@"));
  const installIndex = deploySteps.findIndex((step) => step.name === "Install pinned Vercel uploader");
  const deployStep = deploySteps.find((step) => step.name === "Deploy preview");

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

  for (const condition of [buildCondition, deployCondition]) {
    assert.match(condition, /head\.repo\.full_name == github\.repository/);
    assert.match(condition, /\["OWNER","MEMBER","COLLABORATOR"\]/);
    assert.match(condition, /labels\.\*\.name, 'docs'/);
    assert.match(condition, /event\.action != 'closed'/);
    assert.match(condition, /event\.action != 'unlabeled'/);
  }
  assert.equal(build.env, undefined, "untrusted build job must not inherit credentials");
  assert.deepEqual(build.permissions, { contents: "read" });
  assert.deepEqual(deploy.permissions, { contents: "read" });
  assert.deepEqual(deploy.needs, "build");
  for (const step of buildSteps) {
    assert.doesNotMatch(JSON.stringify(step), /VERCEL_(?:TOKEN|ORG_ID|PROJECT_ID)/);
  }
  assert.ok(buildCheckoutIndex >= 0, "build checks out the exact PR head");
  assert.equal(buildSteps[buildCheckoutIndex].with.ref, "${{ github.event.pull_request.head.sha }}");
  assert.match(buildCommands, /pnpm install --frozen-lockfile/);
  assert.match(buildCommands, /pnpm build:vercel-docs/);

  assert.ok(installIndex >= 0 && installIndex < deployCheckoutIndex, "install trusted CLI before PR checkout");
  assert.match(deploySteps[installIndex].run, /npm install --global --ignore-scripts vercel@59\.10\.0/);
  assert.match(deploySteps[installIndex].run, /realpath "\$\(command -v vercel\)"/);
  assert.match(deploySteps[installIndex].run, /\/\*\) ;;/, "captured uploader path must be absolute");
  assert.match(deploySteps[installIndex].run, /echo "bin=\$vercel_bin" >> "\$GITHUB_OUTPUT"/);
  assert.equal(deploySteps[deployCheckoutIndex].with.ref, "${{ github.event.pull_request.head.sha }}");
  assert.ok(deployStep, "there must be one credentialed Vercel deployment step");
  assert.match(deployStep.run, /"\$\{\{ steps\.vercel\.outputs\.bin \}\}" deploy --yes/);
  assert.match(deployStep.run, /git_sha="\$\{\{ github\.event\.pull_request\.head\.sha \}\}"/);
  assert.match(deployStep.run, /\$\{!name\}/);
  assert.doesNotMatch(deployStep.run, /echo.*\$\{!name\}/);
  assert.match(deployStep.env.VERCEL_TOKEN, /secrets\.VERCEL_DOCS_TOKEN/);
  assert.match(deployStep.env.VERCEL_PROJECT_ID, /vars\.VERCEL_DOCS_PROJECT_ID/);
  assert.equal(
    deploySteps.filter((step) => /VERCEL_(?:TOKEN|ORG_ID|PROJECT_ID)/.test(JSON.stringify(step))).length,
    1,
    "exactly one deploy step may receive Vercel configuration",
  );
  const afterCheckout = deploySteps.slice(deployCheckoutIndex + 1).map((step) => step.run).filter(Boolean).join("\n");
  assert.doesNotMatch(afterCheckout, /\b(?:pnpm|npm|npx|yarn|bun)\b/, "deploy must not execute workspace tooling");
  assert.doesNotMatch(deployCommands, /\b(?:pnpm dlx|npx)\s+vercel/);
  assert.doesNotMatch(deployCommands, /\bvercel@(?:latest|\^|~)/);
  assert.doesNotMatch(deployCommands, /\b(?:pull|build)\s+.*vercel|vercel\s+(?:pull|build)/);
  assert.doesNotMatch(deployCommands, /\b--prebuilt\b/);
  assert.doesNotMatch(deployCommands, /\b--prod\b/);
  assert.match(workflow.jobs.production.if.replace(/\s+/g, " "), /false/);

  const docsPackage = JSON.parse(packageSource);
  assert.equal(docsPackage.devDependencies.vercel, undefined, "workspace must not provide the credentialed CLI");
  assert.doesNotMatch(lockfileSource, /\n\s{2}vercel@59\.10\.0:/, "lockfile must not carry the workflow-only CLI");
  assert.match(docsReadmeSource, /disable or disconnect its automatic\s+Git deployments/i);
  assert.match(docsReadmeSource, /Preview\*\* environment must contain no sensitive values/);
  assert.match(docsReadmeSource, /maintainer trust decision: it trusts the workflow/i);
}

test("docs preview event truth table is explicit", () => {
  const cases = [
    [{ number: 42, labels: ["docs"] }, true, "labeled trusted branch"],
    [{ number: 43, labels: ["docs"] }, true, "stacked child PR has its own preview group"],
    [{ action: "synchronize", labels: ["docs"] }, true, "new commit retains label"],
    [{ action: "reopened", labels: ["docs"] }, true, "reopen retains label"],
    [{ action: "unlabeled", labels: [] }, false, "label removal only cancels"],
    [{ action: "unlabeled", labels: ["docs"] }, false, "unrelated-label removal only cancels"],
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

test("workflow separates tokenless build from one pinned credentialed remote preview deploy", () => {
  assertPreviewWorkflowContract(workflowSource);
});

test("contract tests reject planted event, credential, and version regressions", () => {
  assert.throws(
    () => assertPreviewWorkflowContract(workflowSource.replace("&& github.event.action != 'closed'", "")),
    (error) => error.message.includes("event\\.action != 'closed'"),
  );
  assert.throws(
    () => assertPreviewWorkflowContract(workflowSource.replace("&& github.event.action != 'unlabeled'", "")),
    (error) => error.message.includes("event\\.action != 'unlabeled'"),
  );
  assert.throws(
    () =>
      assertPreviewWorkflowContract(
        workflowSource.replace("run: pnpm build:vercel-docs", "env:\n          VERCEL_TOKEN: leaked\n        run: pnpm build:vercel-docs"),
      ),
    (error) => error.message.includes("VERCEL_(?:TOKEN|ORG_ID|PROJECT_ID)"),
  );
  assert.throws(
    () => assertPreviewWorkflowContract(workflowSource.replace("--ignore-scripts vercel@59.10.0", "--ignore-scripts vercel@latest")),
    /vercel@59\\.10\\.0/,
  );
  assert.throws(
    () => assertPreviewWorkflowContract(workflowSource.replace('"${{ steps.vercel.outputs.bin }}" deploy', "pnpm exec vercel deploy")),
    /workspace tooling|steps\\.vercel\\.outputs\\.bin/,
  );
});
