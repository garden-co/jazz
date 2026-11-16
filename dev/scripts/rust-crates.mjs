// The Rust crates Jazz publishes to crates.io, read from `cargo metadata`.
//
// The published set is exactly the dependency closure (normal and build
// dependencies, optional ones included, since crates.io needs those too) of
// the crates users install. Its crates go to crates.io in the returned
// topological order. Every Jazz crate in it is versioned in lockstep with the
// jazz-tools npm package.

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/** The crates users install; everything they need is published with them. */
export const ROOT_CRATES = Object.freeze(["jazz-app", "jazz-tools", "jazz-codegen", "jazz-ffi"]);

/**
 * Published crates that are versioned apart from Jazz: vendored forks keep
 * their upstream version.
 */
export const INDEPENDENTLY_VERSIONED = Object.freeze({
  "jazz-ruzstd": "the patched ruzstd 0.8.3 fork (vendor/ruzstd)",
  "jazz-wasm-tracing": "the maintained wasm-tracing fork (crates/wasm-tracing)",
});

/**
 * Workspace crates outside the closure that are published too, after it (the
 * wasm-tracing fork jazz-wasm uses, on crates.io since before the closure).
 */
export const PUBLISHED_OUTSIDE_CLOSURE = Object.freeze({
  "jazz-wasm-tracing": "jazz-wasm's tracing subscriber, a fork published at its own version",
});

/**
 * Vendored crates Jazz publishes that are not workspace members (their
 * upstream tests aren't vendored, so `cargo test --workspace` can't build
 * them). `withVendoredMembers` adds them to the workspace while packaging.
 */
export const VENDORED_CRATES = Object.freeze({ "jazz-ruzstd": "vendor/ruzstd" });

/**
 * Runs `fn` with the vendored crates as workspace members, so `cargo package`
 * verifies them together with the crates that depend on them. Cargo.toml and
 * Cargo.lock are restored afterwards, whatever happens.
 */
export async function withVendoredMembers(fn, root = process.cwd()) {
  const files = ["Cargo.toml", "Cargo.lock"].map((name) => join(root, name));
  const saved = files.map((file) => readFileSync(file, "utf8"));
  let manifest = saved[0];
  for (const dir of Object.values(VENDORED_CRATES)) {
    const excluded = `  "${dir}",\n`;
    if (!manifest.includes(excluded)) throw new Error(`Cargo.toml doesn't exclude ${dir}`);
    manifest = manifest
      .replace(excluded, "")
      .replace(/^members = \[\n/m, (start) => `${start}${excluded}`);
  }
  writeFileSync(files[0], manifest);
  try {
    return await fn();
  } finally {
    files.forEach((file, i) => writeFileSync(file, saved[i]));
  }
}

export function cargoMetadata({ cwd = process.cwd(), noDeps = true, locked = true } = {}) {
  const args = ["metadata", "--format-version", "1"];
  if (locked) args.push("--locked");
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
  for (const name of Object.keys(PUBLISHED_OUTSIDE_CLOSURE))
    if (byName.has(name) && !closure.has(name)) {
      closure.add(name);
      place(name);
    }

  for (const name of order) {
    const pkg = byName.get(name);
    if (!isPublishable(pkg))
      problems.push(`${name} is needed by ${ROOT_CRATES.join(", ")} but has publish = false`);
    const required =
      name in INDEPENDENTLY_VERSIONED
        ? ["description", "license"]
        : ["description", "license", "repository", "rust_version"];
    for (const field of required)
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
