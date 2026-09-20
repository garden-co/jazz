#!/usr/bin/env node
// Explicit selfhosted scaffold only; never provisions a Cloud app.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFileSync, existsSync, mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
const c = JSON.parse(readFileSync(process.argv[2], "utf8"));
assert(["baseline", "final-preview", "published"].includes(c.phase));
assert.match(c.sourceSha, /^[a-f0-9]{40}$/);
assert(c.createJazzSpec && c.packageVersion && c.parent && c.name);
assert.match(c.name, /^[a-z][a-z0-9-]*$/);
const parent = resolve(c.parent);
mkdirSync(parent, { recursive: true });
assert(!existsSync(join(parent, c.name)), "Never overwrite an existing scaffold");
// Preview package URL must name the pinned source; published uses immutable version.
if (c.phase !== "published")
  assert(c.createJazzSpec.includes(c.sourceSha), "Preview spec must contain exact source SHA");
else assert.equal(c.createJazzSpec, `create-jazz@${c.packageVersion}`);
const args =
  c.phase === "published"
    ? ["create", "--yes", `jazz@${c.packageVersion}`, "--", c.name]
    : ["exec", "--yes", `--package=${c.createJazzSpec}`, "--", "create-jazz", c.name];
args.push("--starter", c.starter ?? "react-localfirst", "--hosting", "selfhosted", "--no-git");
const child = spawn("npm", args, {
  cwd: parent,
  stdio: "inherit",
  env: { ...process.env, CI: "true", JAZZ_STARTER_PATH: "" },
});
const timer = setTimeout(() => child.kill("SIGKILL"), 180000);
try {
  const [code] = await once(child, "exit");
  assert.equal(code, 0, "Scaffold failed");
  const pkg = JSON.parse(readFileSync(join(parent, c.name, "package.json"), "utf8"));
  const deps = { ...pkg.dependencies, ...pkg.devDependencies };
  assert(deps["jazz-tools"], "Missing Jazz dependency");
  if (c.phase !== "published")
    assert(deps["jazz-tools"].includes(c.sourceSha), "Scaffold did not preserve preview pin");
  else assert.equal(deps["jazz-tools"], c.packageVersion);
  console.log(
    JSON.stringify({
      check: "scaffold",
      phase: c.phase,
      sourceSha: c.sourceSha,
      packageVersion: c.packageVersion,
      status: "PASS",
      dependencies: deps,
    }),
  );
} finally {
  clearTimeout(timer);
}
