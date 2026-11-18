import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { actionFiles, findUnpinned, isPinned } from "../actions-pinned.mjs";

const sha = "de0fac2e4500dabe0009e67214ff5f5447ce83dd";
const digest = "a".repeat(64);

test("pinned and local references pass", () => {
  for (const value of [
    `actions/checkout@${sha}`,
    `github/codeql-action/init@${sha}`,
    "./.github/actions/setup-build",
    "./.github/workflows/ci-suite.yml",
    `docker://alpine@sha256:${digest}`,
  ])
    assert.ok(isPinned(value), value);
});

test("tags, branches, short SHAs and tagged images fail", () => {
  for (const value of [
    "actions/checkout@v4",
    "dtolnay/rust-toolchain@stable",
    "google/oss-fuzz/infra/cifuzz/actions/build_fuzzers@master",
    `actions/checkout@${sha.slice(0, 7)}`,
    `actions/checkout@${sha.toUpperCase()}`,
    "actions/checkout",
    "docker://alpine:3.20",
  ])
    assert.ok(!isPinned(value), value);
});

test("every uses: form is read, with quotes and comments stripped", () => {
  const text = [
    "jobs:",
    "  a:",
    "    uses: ./.github/workflows/reusable.yml",
    "  b:",
    "    uses: org/repo/.github/workflows/x.yml@main",
    "    steps:",
    `      - uses: actions/checkout@${sha} # v6`,
    "      - uses: actions/setup-node@v4 # v4",
    `      - name: quoted`,
    `        uses: "pnpm/action-setup@v4"`,
    `      - uses: 'actions/cache@${sha}'`,
    "      - run: echo 'uses: not/a-step@v1'",
  ].join("\n");
  assert.deepEqual(findUnpinned(text), [
    { line: 5, value: "org/repo/.github/workflows/x.yml@main" },
    { line: 8, value: "actions/setup-node@v4" },
    { line: 10, value: "pnpm/action-setup@v4" },
  ]);
});

test("scans .github YAML and action.yml anywhere, skipping node_modules", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actions-pinned-"));
  try {
    for (const file of [
      ".github/workflows/ci.yml",
      ".github/actions/setup/action.yml",
      "tools/action.yaml",
      "crates/x/.github/workflows/test.yaml",
      "node_modules/pkg/.github/workflows/ci.yml",
      "config/app.yml",
    ]) {
      fs.mkdirSync(path.join(root, path.dirname(file)), { recursive: true });
      fs.writeFileSync(path.join(root, file), "");
    }
    assert.deepEqual(actionFiles(root), [
      ".github/actions/setup/action.yml",
      ".github/workflows/ci.yml",
      "crates/x/.github/workflows/test.yaml",
      "tools/action.yaml",
    ]);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
