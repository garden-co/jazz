// Every example and fixture app is its files (SPEC 10.10): jazz.app.json,
// jazz.permissions.json and the jazz.generated.ts `jazz-tools generate`
// writes from the first, all committed. None keeps an alpha.64 source
// (schema.ts, permissions.ts, migrations/) beside them: `jazz-tools upgrade`
// deletes those, and a leftover would read as a second source of truth.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

/** A Swift package of the Jazz Swift runtime (it uses the `Jazz` product): its bindings are JazzApp.swift. */
function isSwiftApp(dir) {
  const manifest = join(root, dir, "Package.swift");
  return existsSync(manifest) && usesJazzSwift(readFileSync(manifest, "utf8"));
}

/** As jazz-codegen's `swift::is_app_manifest`: the `Jazz` product, or the jazz-swift package. */
function usesJazzSwift(manifest) {
  return /\.product\(\s*name:\s*"Jazz"/.test(manifest) || /["/]jazz-swift(\.git)?"/.test(manifest);
}

/**
 * TS apps: every app with a jazz.app.json, except Rust crates (a Cargo.toml
 * beside it), whose bindings are `src/jazz_app.rs` and which the `jazz-app`
 * crate's checks cover, Swift packages (a Package.swift naming jazz-swift),
 * whose bindings are `JazzApp.swift` and which dev/gates/jazz-swift.sh
 * covers, and Go modules (a go.mod beside it), whose bindings are
 * `jazzapp/jazz_app.go` and which jazz-codegen's golden tests cover.
 */
function projects() {
  return execFileSync(
    "git",
    ["ls-files", "--", "examples/**/jazz.app.json", "dev/stress-tests/**/jazz.app.json"],
    { cwd: root, encoding: "utf8" },
  )
    .split("\n")
    .filter(Boolean)
    .map(dirname)
    .filter((dir) => !existsSync(join(root, dir, "Cargo.toml")) && !isSwiftApp(dir))
    .filter((dir) => !existsSync(join(root, dir, "go.mod")));
}

test("every example app commits its files and its bindings", () => {
  const listed = projects();
  assert.ok(listed.length > 30, `found only ${listed.length} projects`);
  const missing = listed.flatMap((dir) =>
    ["jazz.permissions.json", "jazz.generated.ts"]
      .filter((file) => !existsSync(join(root, dir, file)))
      .map((file) => `${dir}/${file}`),
  );
  assert.deepEqual(
    missing,
    [],
    `Run \`jazz-tools generate\` in these and commit what it writes:\n${missing.join("\n")}`,
  );
});

test("no example keeps an alpha.64 schema source beside its files", () => {
  const leftovers = projects().flatMap((dir) =>
    ["schema.ts", "permissions.ts", "migrations"]
      .filter((file) => existsSync(join(root, dir, file)))
      .map((file) => `${dir}/${file}`),
  );
  assert.deepEqual(
    leftovers,
    [],
    `jazz.app.json and jazz.permissions.json are the app's only source; \`jazz-tools upgrade\` removes these:\n${leftovers.join("\n")}`,
  );
});

function tracked(...patterns) {
  return execFileSync("git", ["ls-files", "--", ...patterns], { cwd: root, encoding: "utf8" })
    .split("\n")
    .filter(Boolean);
}

/**
 * Found independently of jazz.app.json, so a deleted one is caught: every
 * jazz.generated.ts or jazz.permissions.json is generated from or checked
 * against the jazz.app.json beside it.
 */
test("every committed app file has its jazz.app.json beside it", () => {
  const orphans = tracked(
    "examples/**/jazz.generated.ts",
    "examples/**/jazz.permissions.json",
    "dev/stress-tests/**/jazz.generated.ts",
    "dev/stress-tests/**/jazz.permissions.json",
  ).filter((file) => !existsSync(join(root, dirname(file), "jazz.app.json")));
  assert.deepEqual(
    orphans,
    [],
    `These have no jazz.app.json, the app's source, beside them:\n${orphans.join("\n")}`,
  );
});

/** Example packages: a package.json that depends on jazz-tools. */
function jazzPackages() {
  return tracked("examples/**/package.json", "dev/stress-tests/**/package.json")
    .filter((file) => {
      const manifest = JSON.parse(readFileSync(join(root, file), "utf8"));
      return ["dependencies", "devDependencies", "peerDependencies"].some(
        (field) => manifest[field]?.["jazz-tools"],
      );
    })
    .map(dirname);
}

/**
 * Packages that depend on jazz-tools but are not apps, each with why. Keep
 * this short: an example is an app and commits its jazz.app.json.
 */
const NOT_APPS = new Map([
  [
    "examples/rust-backend-clients",
    "a test harness: it serves the Rust examples' own app files from a TS main backend",
  ],
]);

test("every example package that depends on jazz-tools commits a jazz.app.json", () => {
  const packages = jazzPackages().filter((dir) => !NOT_APPS.has(dir));
  assert.ok(packages.length > 30, `found only ${packages.length} jazz-tools packages`);
  const apps = tracked("examples/**/jazz.app.json", "dev/stress-tests/**/jazz.app.json");
  const without = packages.filter((dir) => !apps.some((file) => file.startsWith(`${dir}/`)));
  assert.deepEqual(
    without,
    [],
    `These depend on jazz-tools but commit no jazz.app.json:\n${without.join("\n")}`,
  );
});
