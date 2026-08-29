import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";

const root = path.resolve(import.meta.dirname, "../../..");
const workflowPath = path.join(root, ".github/workflows/docs-vercel-preview.yml");
const workflowSource = fs.readFileSync(workflowPath, "utf8");
const workflow = parse(workflowSource);

const trustedAssociations = new Set(["OWNER", "MEMBER", "COLLABORATOR"]);
const previewWouldRun = ({
  event = "pull_request",
  action = "labeled",
  labels = [],
  sameRepository = true,
  association = "MEMBER",
}) =>
  event === "pull_request" &&
  ["labeled", "synchronize", "reopened"].includes(action) &&
  sameRepository &&
  trustedAssociations.has(association) &&
  labels.includes("docs");

test("docs previews react to label changes and new commits without enabling main production", () => {
  assert.deepEqual(workflow.on.pull_request.types, [
    "labeled",
    "unlabeled",
    "synchronize",
    "reopened",
  ]);
  assert.equal(workflow.on.push, undefined, "production must not be triggerable before release");
  assert.equal(
    workflow.concurrency.group,
    "docs-vercel-preview-${{ github.event.pull_request.number }}",
  );
  assert.equal(workflow.concurrency["cancel-in-progress"], true);
});

test("only an explicitly labeled trusted same-repository PR can receive Vercel credentials", () => {
  assert.equal(previewWouldRun({ labels: ["docs"] }), true, "labeled branch preview");
  assert.equal(previewWouldRun({ action: "synchronize", labels: ["docs"] }), true, "new commit");
  assert.equal(previewWouldRun({ action: "reopened", labels: ["docs"] }), true, "reopened PR");
  assert.equal(previewWouldRun({ action: "unlabeled", labels: [] }), false, "label removed");
  assert.equal(previewWouldRun({ action: "synchronize", labels: [] }), false, "unlabeled branch");
  assert.equal(previewWouldRun({ labels: ["docs"], sameRepository: false }), false, "fork");
  assert.equal(
    previewWouldRun({ labels: ["docs"], association: "NONE" }),
    false,
    "untrusted author",
  );

  const condition = workflow.jobs.preview.if.replace(/\s+/g, " ");
  assert.match(condition, /head\.repo\.full_name == github\.repository/);
  assert.match(condition, /\["OWNER","MEMBER","COLLABORATOR"\]/);
  assert.match(condition, /labels\.\*\.name, 'docs'/);
  assert.deepEqual(workflow.jobs.preview.permissions, { contents: "read" });
});

test("the preview builds and deploys the Vercel artifact, while production remains impossible", () => {
  const steps = workflow.jobs.preview.steps;
  const commands = steps
    .map((step) => step.run)
    .filter(Boolean)
    .join("\n");
  assert.match(commands, /pnpm build:vercel-docs/);
  assert.match(commands, /vercel@latest build --cwd docs/);
  assert.match(commands, /vercel@latest deploy --prebuilt/);
  assert.doesNotMatch(commands, /\b--prod\b/);
  assert.match(workflow.jobs.production.if.replace(/\s+/g, " "), /false/);
  assert.match(workflowSource, /To enable it: add push branches main above/);
});
