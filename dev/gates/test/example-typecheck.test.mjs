import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import { planFor } from "../local-ci-equivalent.mjs";

const root = path.resolve(import.meta.dirname, "../../..");

// Examples that are deliberately not typechecked. Each entry must name the
// issue that tracks bringing it back, like the ignored-test markers.
const EXCLUDED_EXAMPLES = new Map([
  // ["examples/some-example", "#1234"],
]);

// The workspace globs under examples/ are single-`*` patterns, so a
// segment-by-segment expansion is enough to list the member packages.
function workspaceExamplePackages() {
  const workspace = fs.readFileSync(path.join(root, "pnpm-workspace.yaml"), "utf8");
  const patterns = [...workspace.matchAll(/^\s+-\s+"?(!?examples\/[^"\s]+)"?\s*$/gm)].map(
    (match) => match[1],
  );
  const excluded = new Set(patterns.filter((p) => p.startsWith("!")).map((p) => p.slice(1)));
  const members = new Set();
  for (const pattern of patterns.filter((p) => !p.startsWith("!"))) {
    let dirs = [""];
    for (const segment of pattern.split("/")) {
      dirs = dirs.flatMap((dir) => {
        if (segment !== "*") return [path.posix.join(dir, segment)];
        const abs = path.join(root, dir);
        if (!fs.existsSync(abs)) return [];
        return fs
          .readdirSync(abs, { withFileTypes: true })
          .filter((entry) => entry.isDirectory() && entry.name !== "node_modules")
          .map((entry) => path.posix.join(dir, entry.name));
      });
    }
    for (const dir of dirs)
      if (!excluded.has(dir) && fs.existsSync(path.join(root, dir, "package.json")))
        members.add(dir);
  }
  return [...members].sort();
}

test("every workspace example defines a typecheck script or an issue-linked exclusion", () => {
  const packages = workspaceExamplePackages();
  assert.ok(packages.length > 0, "found no example packages");
  const missing = packages.filter((dir) => {
    const manifest = JSON.parse(fs.readFileSync(path.join(root, dir, "package.json"), "utf8"));
    return !manifest.scripts?.typecheck && !EXCLUDED_EXAMPLES.has(dir);
  });
  assert.deepEqual(missing, [], "add a `typecheck` script to these examples");
  for (const [dir, issue] of EXCLUDED_EXAMPLES) {
    assert.match(issue, /^#\d+$/, `${dir} exclusion must name its tracking issue`);
    assert.ok(packages.includes(dir), `${dir} is excluded but is not a workspace example`);
  }
});

test("the TypeScript partition typechecks the examples after Jazz Tools is built", () => {
  const labels = planFor({ partition: "typescript" }).map(({ label }) => label);
  const typecheck = labels.indexOf("example typecheck");
  assert.ok(typecheck >= 0, "TypeScript partition omits the example typecheck");
  assert.ok(
    typecheck > labels.indexOf("TypeScript consumers"),
    "examples must be typechecked against the jazz-tools build the consumers produced",
  );
});
