import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");

const unmarkedConventionalProvenance =
  /^[ \t]*(createdAt|createdBy|updatedAt|updatedBy):(?![ \t]*s\.allowExternalProvenanceName\()/m;

test("provenance gate targets only unmarked Jazz provenance aliases", () => {
  assert.equal(unmarkedConventionalProvenance.test("createdAt: s.timestamp(),"), true);
  assert.equal(
    unmarkedConventionalProvenance.test("createdAt: s.allowExternalProvenanceName(s.timestamp()),"),
    false,
  );
  for (const domainEventTime of ["createdOn", "creationTime", "occurredAt"]) {
    assert.equal(unmarkedConventionalProvenance.test(`${domainEventTime}: s.timestamp(),`), false);
  }
});

test("official examples do not duplicate Jazz provenance without an explicit external marker", () => {
  const offenders = [];

  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(file);
      } else if (
        entry.isFile() &&
        entry.name === "schema.ts" &&
        unmarkedConventionalProvenance.test(fs.readFileSync(file, "utf8"))
      ) {
        offenders.push(path.relative(root, file));
      }
    }
  }

  visit(path.join(root, "examples"));
  assert.deepEqual(
    offenders,
    [],
    "Examples must use Jazz $ provenance or s.allowExternalProvenanceName(...)",
  );
});
