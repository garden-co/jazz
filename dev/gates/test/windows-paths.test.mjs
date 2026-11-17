import assert from "node:assert/strict";
import { test } from "node:test";
import { windowsPathProblems, windowsProblem } from "../windows-paths.mjs";

test("refuses the characters Windows forbids in a name", () => {
  for (const c of ["<", ">", ":", '"', "|", "?", "*", "\t"]) {
    assert.ok(windowsProblem(`a/b${c}c.json`), JSON.stringify(c));
  }
  assert.match(
    windowsProblem("fixtures/<stamp>-d2a80e62507d.json"),
    /`<` in `<stamp>-d2a80e62507d.json`/,
  );
});

test("refuses names ending in a dot or a space, and reserved device names", () => {
  assert.ok(windowsProblem("docs/notes."));
  assert.ok(windowsProblem("docs/notes /a.md"));
  for (const name of ["CON", "prn.txt", "Aux", "nul.json", "COM1", "lpt9.log"]) {
    assert.ok(windowsProblem(`dir/${name}`), name);
  }
});

test("accepts ordinary paths, including ones that only look like reserved names", () => {
  for (const path of [
    "STAMP-d2a80e62507d.json",
    "src/console.ts",
    "com10.txt",
    "aux-files/x.rs",
    ".github/ci.yml",
  ]) {
    assert.equal(windowsProblem(path), undefined, path);
  }
});

test("refuses paths that collide once case is folded", () => {
  assert.deepEqual(windowsPathProblems(["a/Readme.md", "a/README.md", "b/x"]), [
    "a/README.md: differs from a/Readme.md only in case",
  ]);
});
