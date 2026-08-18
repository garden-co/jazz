import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { removeAbandonedNapiStages } from "./napi-staging.mjs";

test("abandoned NAPI stages are removed before artifact provenance", () => {
  const directory = mkdtempSync(join(tmpdir(), "jazz-napi-staging-"));
  try {
    const binding = join(directory, "jazz-napi.linux-x64-gnu.node");
    const abandoned = `${binding}.staged-123-456`;
    const unrelated = join(directory, "other.node.staged-123-456");
    writeFileSync(abandoned, "abandoned");
    writeFileSync(unrelated, "unrelated");

    removeAbandonedNapiStages(binding, () => false);

    assert.equal(readFileSync(unrelated, "utf8"), "unrelated");
    assert.throws(() => readFileSync(abandoned), /ENOENT/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a live NAPI stage blocks concurrent artifact publication", () => {
  const directory = mkdtempSync(join(tmpdir(), "jazz-napi-staging-"));
  try {
    const binding = join(directory, "jazz-napi.linux-x64-gnu.node");
    const active = `${binding}.staged-321-654`;
    writeFileSync(active, "active");

    assert.throws(
      () => removeAbandonedNapiStages(binding, (pid) => pid === 321),
      /another NAPI build retains.*process 321/,
    );
    assert.equal(readFileSync(active, "utf8"), "active");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
