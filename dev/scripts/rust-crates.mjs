// The Rust crates Jazz publishes to crates.io, read from `cargo metadata`.
//
// The published set is exactly the dependency closure (normal and build
// dependencies, optional ones included, since crates.io needs those too) of
// the crates users install. Its crates go to crates.io in the returned
// topological order. Every Jazz crate in it is versioned in lockstep with the
// jazz-tools npm package.

import { execFileSync } from "node:child_process";

/** The crates users install; everything they need is published with them. */
export const ROOT_CRATES = Object.freeze(["jazz-app", "jazz-tools", "jazz-codegen", "jazz-ffi"]);

/**
 * Published crates that are versioned apart from Jazz: vendored forks keep
 * their upstream version.
 */
export const INDEPENDENTLY_VERSIONED = Object.freeze({
  "jazz-ruzstd": "the patched ruzstd 0.8.3 fork (vendor/ruzstd)",
});

/**
 * Workspace crates outside the closure that may stay publishable, each with
 * its own release flow.
 */
export const PUBLISHED_OUTSIDE_CLOSURE = Object.freeze({
  "jazz-wasm-tracing": "published by publish-jazz-crates-alpha.yml at its own version",
});

export function cargoMetadata({ cwd = process.cwd(), noDeps = true } = {}) {
  const args = ["metadata", "--format-version", "1", "--locked"];
  if (noDeps) args.push("--no-deps");
  return JSON.parse(
    execFileSync("cargo", args, { cwd, encoding: "utf8", maxBuffer: 256 * 1024 * 1024 }),
  );
}

export function isPublishable(pkg) {
  return pkg.publish === null || (Array.isArray(pkg.publish) && pkg.publish.includes("crates-io"));
}

/**
 * Returns `{ order, byName, problems }`: the published crates in an order
 * where every crate comes after the crates it depends on, and any reason the
 * workspace can't be published as is.
 */
export function publishPlan(metadata) {
  const members = new Set(metadata.workspace_members);
  const byName = new Map(
    metadata.packages.filter((pkg) => members.has(pkg.id)).map((pkg) => [pkg.name, pkg]),
  );
  const problems = [];

  // Dependencies that ship: normal and build, optional or not. Dev
  // dependencies without a version are stripped by `cargo package`.
  const shipped = (pkg) =>
    pkg.dependencies.filter((dep) => dep.kind !== "dev" && byName.has(dep.name) && dep.path);

  const closure = new Set();
  const visit = (name, from) => {
    if (closure.has(name)) return;
    const pkg = byName.get(name);
    if (!pkg) {
      problems.push(`${from ?? "ROOT_CRATES"} names ${name}, which is not a workspace crate`);
      return;
    }
    closure.add(name);
    for (const dep of shipped(pkg)) visit(dep.name, name);
  };
  for (const root of ROOT_CRATES) visit(root);

  const order = [];
  const done = new Set();
  const visiting = new Set();
  const place = (name) => {
    if (done.has(name)) return;
    if (visiting.has(name)) {
      problems.push(`dependency cycle through ${name}`);
      return;
    }
    visiting.add(name);
    for (const dep of [...shipped(byName.get(name))].sort((a, b) => a.name.localeCompare(b.name)))
      place(dep.name);
    visiting.delete(name);
    done.add(name);
    order.push(name);
  };
  for (const name of [...closure].sort()) place(name);

  for (const name of order) {
    const pkg = byName.get(name);
    if (!isPublishable(pkg))
      problems.push(`${name} is needed by ${ROOT_CRATES.join(", ")} but has publish = false`);
    for (const field of ["description", "license", "repository", "rust_version"])
      if (!pkg[field]) problems.push(`${name} has no ${field.replace("_", "-")}`);
    for (const dep of shipped(pkg))
      if (!dep.req || dep.req === "*")
        problems.push(`${name}'s dependency on ${dep.name} has no version`);
  }
  for (const [name, pkg] of byName)
    if (!closure.has(name) && isPublishable(pkg) && !(name in PUBLISHED_OUTSIDE_CLOSURE))
      problems.push(`${name} is not needed by ${ROOT_CRATES.join(", ")}; give it publish = false`);

  return { order, byName, problems };
}

export function lockstepCrates(plan) {
  return plan.order.filter((name) => !(name in INDEPENDENTLY_VERSIONED));
}
