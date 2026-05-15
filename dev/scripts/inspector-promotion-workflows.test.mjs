import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

test("standalone inspector promotion workflow can be triggered manually", () => {
  const workflow = fs.readFileSync(
    ".github/workflows/promote-inspector-production.yml",
    "utf8",
  );

  assert.match(workflow, /^name: Promote inspector production$/m);
  assert.match(workflow, /^  workflow_dispatch:$/m);
  assert.match(workflow, /^  workflow_call:$/m);
  assert.match(workflow, /VERCEL_ORG_ID: \$\{\{ secrets\.VERCEL_INSPECTOR_ORG_ID \}\}/);
  assert.match(workflow, /VERCEL_PROJECT_ID: \$\{\{ secrets\.VERCEL_INSPECTOR_PROJECT_ID \}\}/);
  assert.match(workflow, /VERCEL_TOKEN: \$\{\{ secrets\.VERCEL_INSPECTOR_TOKEN \}\}/);
  assert.match(workflow, /run: node dev\/scripts\/resolve-inspector-deployment\.mjs/);
  assert.match(workflow, /pnpm dlx vercel@latest promote/);
});

test("release workflow delegates inspector promotion to the standalone workflow", () => {
  const workflow = fs.readFileSync(
    ".github/workflows/publish-jazz-tools-alpha.yml",
    "utf8",
  );

  const deployJob = workflow.match(
    /deploy-inspector-production:[\s\S]*?(?=\n  [a-zA-Z0-9_-]+:|\n?$)/,
  )?.[0];

  assert.ok(deployJob);
  assert.match(
    deployJob,
    /uses: \.\/\.github\/workflows\/promote-inspector-production\.yml/,
  );
  assert.match(deployJob, /secrets: inherit/);
});
