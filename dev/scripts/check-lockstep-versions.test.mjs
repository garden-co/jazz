import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { checkLockstep } from "./check-lockstep-versions.mjs";
import { publishPlan } from "./rust-crates.mjs";

const V = "2.0.0-alpha.7";

function pkg(name, version, deps = [], extra = {}) {
  return {
    id: `path+file:///w/${name}#${version}`,
    name,
    version,
    publish: null,
    description: "d",
    license: "MIT",
    repository: "https://github.com/garden-co/jazz",
    rust_version: "1.93",
    dependencies: deps.map(([dep, req, kind = null]) => ({
      name: dep,
      req,
      kind,
      path: `/w/${dep}`,
    })),
    ...extra,
  };
}

function metadata(packages) {
  return { packages, workspace_members: packages.map((p) => p.id) };
}

function workspace(version = V) {
  return [
    pkg("jazz-types", V),
    pkg("jazz-ruzstd", "0.8.3"),
    pkg("jazz-core", V, [
      ["jazz-types", `=${V}`],
      ["jazz-ruzstd", "=0.8.3"],
    ]),
    pkg("jazz-codegen", V, [["jazz-core", `=${V}`]]),
    pkg("jazz-app", V, [["jazz-core", `=${V}`], ["jazz-codegen", "*", "dev"]]),
    pkg("jazz-ffi", V, [["jazz-app", `=${V}`]]),
    pkg("jazz-tools", version, [["jazz-core", `=${V}`], ["jazz-codegen", `=${V}`]]),
    pkg("jazz-sim", "0.1.0", [["jazz-core", "*"]], { publish: [] }),
  ];
}

function repo(npmVersion = V) {
  const root = mkdtempSync(join(tmpdir(), "lockstep-"));
  mkdirSync(join(root, ".changeset"));
  writeFileSync(
    join(root, ".changeset/config.json"),
    JSON.stringify({ fixed: [["jazz-tools", "jazz-wasm"]] }),
  );
  for (const [dir, name, version] of [
    ["packages/jazz-tools", "jazz-tools", npmVersion],
    ["crates/jazz-wasm", "jazz-wasm", npmVersion],
  ]) {
    mkdirSync(join(root, dir), { recursive: true });
    writeFileSync(join(root, dir, "package.json"), JSON.stringify({ name, version }));
  }
  return root;
}

test("orders the closure so every crate follows its dependencies", () => {
  const plan = publishPlan(metadata(workspace()));
  assert.deepEqual(plan.problems, []);
  assert.ok(!plan.order.includes("jazz-sim"));
  for (const [before, after] of [
    ["jazz-types", "jazz-core"],
    ["jazz-ruzstd", "jazz-core"],
    ["jazz-core", "jazz-app"],
    ["jazz-app", "jazz-ffi"],
    ["jazz-codegen", "jazz-tools"],
  ])
    assert.ok(plan.order.indexOf(before) < plan.order.indexOf(after), `${before} < ${after}`);
});

test("a needed crate with publish = false is a problem", () => {
  const packages = workspace();
  packages[0].publish = [];
  assert.match(publishPlan(metadata(packages)).problems.join("\n"), /jazz-types .*publish = false/);
});

test("a publishable crate outside the closure is a problem", () => {
  const packages = workspace();
  packages.at(-1).publish = null;
  assert.match(publishPlan(metadata(packages)).problems.join("\n"), /jazz-sim is not needed/);
});

test("passes when npm, crates and requirements agree", () => {
  assert.deepEqual(checkLockstep({ root: repo(), metadata: metadata(workspace()) }).problems, []);
});

test("reports a crate behind npm", () => {
  const { problems } = checkLockstep({
    root: repo(),
    metadata: metadata(workspace("2.0.0-alpha.6")),
  });
  assert.deepEqual(problems, [`crate jazz-tools is 2.0.0-alpha.6, expected ${V}`]);
});

test("reports crates behind a bumped npm version, and the Go constant", () => {
  const root = repo("2.0.0-alpha.8");
  mkdirSync(join(root, "packages/jazz-go"));
  writeFileSync(join(root, "packages/jazz-go/version.go"), `package jazz\n\nconst Version = "${V}"\n`);
  const { problems } = checkLockstep({ root, metadata: metadata(workspace()) });
  assert.ok(problems.includes(`crate jazz-core is ${V}, expected 2.0.0-alpha.8`));
  assert.ok(problems.includes(`crate jazz-app requires jazz-core =${V}, expected =2.0.0-alpha.8`));
  assert.ok(problems.some((p) => p.startsWith("Go packages/jazz-go/version.go")));
  assert.ok(!problems.some((p) => p.includes("jazz-ruzstd is")));
});
