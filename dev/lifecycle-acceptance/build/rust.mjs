// Rust: the .crate files packaged from the candidate as the publish does, through [patch.crates-io].
import { appendFileSync, mkdirSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { WORK, assert, fetchStep, run, sha256, timed } from "../lib/common.mjs";
import { candidate, candidateSource, cleanEnv, consumerProject, writeDriver } from "./consumer.mjs";

export async function buildRust() {
  const { sha, version } = candidate();
  const source = candidateSource();
  const toolchain = readFileSync(join(source, "rust-toolchain.toml"), "utf8").match(/channel = "([^"]+)"/)[1];
  const env = cleanEnv({ RUSTUP_TOOLCHAIN: toolchain, CARGO_TERM_COLOR: "never" });
  fetchStep("rustup", ["toolchain", "install", toolchain, "--profile", "minimal"], { env });

  const { cargoMetadata, publishPlan, withVendoredMembers } = await import(
    pathToFileURL(join(source, "dev", "scripts", "rust-crates.mjs")).href
  );
  const target = join(WORK, "package-target");
  rmSync(target, { recursive: true, force: true });
  const crates = await timed("rust cargo package", async () => {
    const back = process.cwd();
    process.chdir(source);
    try {
      fetchStep("cargo", ["fetch"], { cwd: source, env });
      return await withVendoredMembers(async () => {
        const plan = publishPlan(cargoMetadata({ locked: false }));
        assert(plan.problems.length === 0, `the publish plan has problems: ${plan.problems.join("; ")}`);
        const packages = plan.order.flatMap((name) => ["-p", name]);
        run("cargo", ["package", "--allow-dirty", "--no-verify", ...packages], {
          cwd: source,
          env: { ...env, CARGO_TARGET_DIR: target },
        });
        return plan.order.map((name) => ({ name, version: plan.byName.get(name).version }));
      }, source);
    } finally {
      process.chdir(back);
    }
  });

  const unpacked = join(WORK, "crates");
  rmSync(unpacked, { recursive: true, force: true });
  mkdirSync(unpacked, { recursive: true });
  const resolved = [];
  for (const crate of crates) {
    const file = join(target, "package", `${crate.name}-${crate.version}.crate`);
    run("tar", ["-xzf", file, "-C", unpacked], { capture: true });
    crate.dir = join(unpacked, `${crate.name}-${crate.version}`);
    crate.sha256 = await sha256(file);
    const vcs = JSON.parse(readFileSync(join(crate.dir, ".cargo_vcs_info.json"), "utf8"));
    assert(vcs.git.sha1 === sha, `${crate.name}.crate was packaged from ${vcs.git.sha1}`);
  }
  const app = crates.find((crate) => crate.name === "jazz-app-support");
  assert(app?.version === version, `jazz-app-support is ${app?.version}, the manifest says ${version}`);

  const dir = consumerProject("rust");
  const patches = crates.map((crate) => `${crate.name} = { path = ${JSON.stringify(crate.dir)} }`);
  appendFileSync(join(dir, "Cargo.toml"), `\n[patch.crates-io]\n${patches.join("\n")}\n`);
  await timed("rust resolve", () => {
    fetchStep("cargo", ["generate-lockfile"], { cwd: dir, env });
    fetchStep("cargo", ["fetch", "--locked"], { cwd: dir, env });
  });

  // Before anything builds: no Jazz crate from a registry, and each one ours.
  const metadata = JSON.parse(
    run("cargo", ["metadata", "--format-version", "1", "--locked", "--offline"], { cwd: dir, env, capture: true }),
  );
  for (const pkg of metadata.packages) {
    const crate = crates.find((c) => c.name === pkg.name);
    if (!crate && !/^jazz(-|$)/.test(pkg.name)) continue;
    assert(crate, `${pkg.name} looks like a Jazz crate and isn't in the publish plan`);
    assert(pkg.source === null, `cargo resolved ${pkg.name} ${pkg.version} from ${pkg.source}`);
    assert(
      pkg.manifest_path === join(crate.dir, "Cargo.toml"),
      `cargo resolved ${pkg.name} at ${pkg.manifest_path}, not the packaged crate`,
    );
    resolved.push({ what: `${crate.name}-${crate.version}.crate`, sha256: crate.sha256 });
  }
  assert(resolved.some(({ what }) => what.startsWith("jazz-app-support-")), "jazz-app-support wasn't resolved");

  await timed("rust build", () => run("cargo", ["build", "--locked", "--offline"], { cwd: dir, env }));
  writeDriver("rust", { cmd: [join(dir, "target", "debug", "lifecycle-driver")], resolved });
}
