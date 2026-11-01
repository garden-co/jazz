#!/usr/bin/env node
/**
 * Examples E2E gate: install each hero example variant the way an adopter
 * would, outside the workspace and from the packed release packages, build it
 * for production, start it, and drive it with its Playwright suite.
 *
 *   node dev/examples-e2e/run.mjs <variant>... [--tarball-dir <dir>] [--keep] [--verbose] [--skip-e2e]
 *   node dev/examples-e2e/run.mjs --all
 *
 * Without --tarball-dir the harness packs jazz-tools, jazz-napi and jazz-wasm
 * from this checkout, so build them first (`pnpm build:examples-e2e`).
 */
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

import { findVariant, VARIANTS } from "./variants.mjs";

const REPO_ROOT = path.resolve(fileURLToPath(new URL("../..", import.meta.url)));
const require = createRequire(import.meta.url);

/** Workspace packages a variant may depend on; each is packed and pinned. */
export const PACKAGES_TO_PACK = Object.freeze(["jazz-tools", "jazz-napi", "jazz-wasm"]);

/**
 * Upstream versions pinned regardless of what a variant resolves.
 * `@better-auth/kysely-adapter` declares kysely ^0.29 support but still
 * imports symbols kysely 0.29 removed, which crashes SSR builds.
 */
const UPSTREAM_PINS = Object.freeze({ kysely: "0.28.17" });

/** Copied variant files the harness never takes along. */
const SKIPPED_ENTRIES = new Set([
  "node_modules",
  ".next",
  ".jazz",
  "test-results",
  "playwright-report",
  ".env",
  ".env.local",
]);

export const ORIGIN = "http://localhost:3000";

// ---------------------------------------------------------------------------
// Manifest and workspace rewriting (pure, unit-tested)
// ---------------------------------------------------------------------------

/**
 * Resolve a variant's `workspace:` and `catalog:` ranges against the packed
 * tarballs and the monorepo's catalogs. Any other workspace package is an
 * error: a variant may only depend on what Jazz publishes.
 */
export function resolveManifest(manifest, { tarballs, catalogs }) {
  const resolved = structuredClone(manifest);
  for (const field of ["dependencies", "devDependencies"]) {
    const deps = resolved[field];
    if (!deps) continue;
    for (const [name, range] of Object.entries(deps)) {
      if (range.startsWith("workspace:")) {
        if (!Object.hasOwn(tarballs, name)) {
          throw new Error(
            `${name}@${range} is a workspace package Jazz does not publish for adopters`,
          );
        }
        deps[name] = `file:${tarballs[name]}`;
      } else if (range.startsWith("catalog:")) {
        const catalogName = range.slice("catalog:".length) || "default";
        const version = catalogs?.[catalogName]?.[name];
        if (!version) throw new Error(`${name}@${range} is not in the workspace catalogs`);
        deps[name] = version;
      }
    }
  }
  return resolved;
}

/** The standalone project's pnpm-workspace.yaml: pin the packed packages everywhere. */
export function renderPnpmWorkspace(tarballs) {
  const lines = [
    ...Object.entries(tarballs).map(([pkg, tgz]) => [pkg, `file:${tgz}`]),
    ...Object.entries(UPSTREAM_PINS),
  ].map(([pkg, value]) => `  ${JSON.stringify(pkg)}: ${JSON.stringify(value)}`);
  return `overrides:\n${lines.join("\n")}\n`;
}

/** The environment a variant is built, started and tested with. */
export function variantEnv(variant, { dataPath, baseEnv = process.env } = {}) {
  const env = { ...baseEnv };
  // Never inherit a sync server or app identity from the caller's shell: the
  // gate tests Jazz running inside the app.
  for (const key of Object.keys(env)) {
    if (key.startsWith("JAZZ_") || key.startsWith("NEXT_PUBLIC_")) delete env[key];
  }
  delete env.BETTER_AUTH_SECRET;
  delete env.BACKEND_SECRET;
  return {
    ...env,
    NEXT_PUBLIC_APP_ORIGIN: ORIGIN,
    NEXT_PUBLIC_JAZZ_APP_ID: randomUUID(),
    BETTER_AUTH_SECRET: randomBytes(32).toString("hex"),
    JAZZ_DATA_PATH: dataPath,
    NEXT_TELEMETRY_DISABLED: "1",
    ...variant.env,
  };
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

function runChild(cmd, args, { cwd, env = process.env, verbose, description, timeoutMs }) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd,
      env,
      stdio: verbose ? "inherit" : ["ignore", "pipe", "pipe"],
    });
    const chunks = [];
    child.stdout?.on("data", (d) => chunks.push(d.toString()));
    child.stderr?.on("data", (d) => chunks.push(d.toString()));
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(() => {
            child.kill("SIGKILL");
            reject(new Error(`${description} timed out after ${timeoutMs}ms`));
          }, timeoutMs);
    child.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
    child.on("exit", (code, signal) => {
      clearTimeout(timer);
      if (code === 0) return resolve();
      const tail = chunks.join("").split("\n").slice(-80).join("\n");
      reject(
        new Error(`${description} exited with code=${code} signal=${signal ?? "none"}\n${tail}`),
      );
    });
  });
}

function findTarball(dir, pkg) {
  const matches = fs
    .readdirSync(dir)
    .filter((f) => f.startsWith(`${pkg}-`) && f.endsWith(".tgz"))
    .sort();
  if (matches.length === 0) throw new Error(`${dir} has no tarball for ${pkg}`);
  return path.join(dir, matches.at(-1));
}

async function packTarballs(destDir, verbose) {
  fs.mkdirSync(destDir, { recursive: true });
  for (const pkg of PACKAGES_TO_PACK) {
    const pkgDir = ["packages", "crates"]
      .map((parent) => path.join(REPO_ROOT, parent, pkg))
      .find((dir) => fs.existsSync(path.join(dir, "package.json")));
    if (!pkgDir) throw new Error(`Could not find workspace package ${pkg}`);
    await runChild("pnpm", ["pack", "--pack-destination", destDir], {
      cwd: pkgDir,
      verbose,
      description: `pnpm pack ${pkg}`,
    });
  }
  return discoverTarballs(destDir);
}

function discoverTarballs(dir) {
  if (!fs.existsSync(dir)) throw new Error(`--tarball-dir ${dir} does not exist`);
  return Object.fromEntries(PACKAGES_TO_PACK.map((pkg) => [pkg, findTarball(dir, pkg)]));
}

function copyVariant(sourceDir, appDir) {
  fs.cpSync(sourceDir, appDir, {
    recursive: true,
    filter: (src) => !SKIPPED_ENTRIES.has(path.basename(src)),
  });
}

function stageVariant(variant, appDir, tarballs) {
  const sourceDir = path.join(REPO_ROOT, variant.dir);
  copyVariant(sourceDir, appDir);
  const workspace = parseYaml(fs.readFileSync(path.join(REPO_ROOT, "pnpm-workspace.yaml"), "utf8"));
  const manifestPath = path.join(appDir, "package.json");
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  for (const script of ["build", "start", "test:e2e"]) {
    if (!manifest.scripts?.[script]) throw new Error(`${variant.dir} has no "${script}" script`);
  }
  const resolved = resolveManifest(manifest, { tarballs, catalogs: workspace.catalogs });
  fs.writeFileSync(manifestPath, `${JSON.stringify(resolved, null, 2)}\n`);
  fs.writeFileSync(path.join(appDir, "pnpm-workspace.yaml"), renderPnpmWorkspace(tarballs));
}

/**
 * jazz-napi's tarball leaves the platform `.node` binaries to per-platform
 * packages that are not published during a release run. Copy this checkout's
 * binary into every installed jazz-napi, then load it through the installed
 * loader and check its fingerprint against the packed receipt, so a binary
 * from another build can never pass for the candidate.
 */
function patchInstalledJazzNapi(appDir) {
  const store = path.join(appDir, "node_modules", ".pnpm");
  const dirs = fs.existsSync(store)
    ? fs
        .readdirSync(store)
        .filter((entry) => entry.startsWith("jazz-napi@"))
        .map((entry) => path.join(store, entry, "node_modules", "jazz-napi"))
        .filter((dir) => fs.existsSync(dir))
    : [];
  if (dirs.length === 0) return;
  const sourceDir = path.join(REPO_ROOT, "crates", "jazz-napi");
  const binaries = fs.readdirSync(sourceDir).filter((f) => f.endsWith(".node"));
  for (const dir of dirs) {
    if (!fs.readdirSync(dir).some((f) => f.endsWith(".node"))) {
      if (binaries.length === 0) {
        throw new Error("No jazz-napi binary in crates/jazz-napi; build or restore it first");
      }
      for (const binary of binaries) {
        fs.copyFileSync(path.join(sourceDir, binary), path.join(dir, binary));
      }
    }
    const receipt = fs.readFileSync(path.join(dir, "native-artifact-fingerprint.cjs"), "utf8");
    const expected = receipt.match(
      /expectedNativeArtifactFingerprint:\s*["']([0-9a-f]{64})["']/,
    )?.[1];
    const actual = require(path.join(dir, "index.cjs")).nativeArtifactFingerprint?.();
    if (!expected || actual !== expected) {
      throw new Error(
        `Installed jazz-napi in ${dir} loads fingerprint ${actual}, expected ${expected}. Do not mix tarballs and binaries from different builds.`,
      );
    }
  }
}

/** The SharedWorker and its WASM ship inside jazz-tools; an app must never copy its own. */
function assertPackagedWorker(appDir) {
  const workerDir = path.join(appDir, "node_modules", "jazz-tools", "dist", "worker");
  for (const file of ["jazz-broker-worker.js", "jazz_wasm_bg.wasm"]) {
    const artifact = path.join(workerDir, file);
    if (!fs.existsSync(artifact) || fs.statSync(artifact).size === 0) {
      throw new Error(`Installed jazz-tools is missing ${artifact}`);
    }
  }
  const appLocal = path.join(appDir, "public", "_jazz", "jazz_wasm_bg.wasm");
  if (fs.existsSync(appLocal)) throw new Error(`Variant build created ${appLocal}`);
}

export async function runVariant(variant, opts) {
  const { verbose = false, keep = false, skipE2E = false } = opts;
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), `examples-e2e-${variant.name}-`));
  const appDir = path.join(workDir, "app");
  const phases = [];
  const phase = async (name, fn) => {
    const started = Date.now();
    try {
      return await fn();
    } finally {
      phases.push({ name, ms: Date.now() - started });
    }
  };
  try {
    const tarballs = await phase("pack", () =>
      opts.tarballDir
        ? discoverTarballs(opts.tarballDir)
        : packTarballs(path.join(workDir, "tarballs"), verbose),
    );
    await phase("stage", () => stageVariant(variant, appDir, tarballs));
    const env = variantEnv(variant, { dataPath: path.join(workDir, "jazz-data") });
    await phase("install", () =>
      // --ignore-scripts: pnpm refuses unlisted postinstalls, and the few
      // dependencies with one (esbuild, sharp) ship binaries as optional deps.
      runChild(
        "pnpm",
        ["install", "--no-frozen-lockfile", "--prefer-offline", "--ignore-scripts"],
        {
          cwd: appDir,
          verbose,
          description: `pnpm install (${variant.name})`,
        },
      ),
    );
    patchInstalledJazzNapi(appDir);
    await phase("build", () =>
      runChild("pnpm", ["build"], {
        cwd: appDir,
        env,
        verbose,
        description: `pnpm build (${variant.name})`,
      }),
    );
    assertPackagedWorker(appDir);
    if (!skipE2E) {
      await runChild("pnpm", ["exec", "playwright", "install", "chromium"], {
        cwd: appDir,
        verbose,
        description: `playwright install (${variant.name})`,
      });
      await phase("e2e", () =>
        runChild("pnpm", ["test:e2e"], {
          cwd: appDir,
          env: { ...env, JAZZ_E2E_PROD: "1", CI: process.env.CI ?? "1" },
          verbose,
          description: `pnpm test:e2e (${variant.name})`,
        }),
      );
    }
    return { variant: variant.name, ok: true, phases, workDir };
  } catch (err) {
    return { variant: variant.name, ok: false, phases, workDir, error: err };
  } finally {
    if (!keep) fs.rmSync(workDir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function usage(message) {
  if (message) console.error(message);
  console.error(
    "Usage: node dev/examples-e2e/run.mjs (<variant>... | --all) [--tarball-dir <dir>] [--keep] [--verbose] [--skip-e2e]",
  );
  console.error(`Variants: ${VARIANTS.map((v) => v.name).join(", ")}`);
  process.exit(1);
}

async function main(argv) {
  const args = [...argv];
  let tarballDir;
  const tarballIndex = args.indexOf("--tarball-dir");
  if (tarballIndex !== -1) {
    tarballDir = args[tarballIndex + 1];
    if (!tarballDir) usage("--tarball-dir needs a directory");
    args.splice(tarballIndex, 2);
  }
  const flags = new Set(args.filter((a) => a.startsWith("-")));
  const names = args.filter((a) => !a.startsWith("-"));
  const targets = flags.has("--all")
    ? [...VARIANTS]
    : names.map((name) => findVariant(name) ?? usage(`Unknown variant: ${name}`));
  if (targets.length === 0) usage();

  const results = [];
  for (const variant of targets) {
    console.log(`\n▶ ${variant.name} (${variant.dir})`);
    const result = await runVariant(variant, {
      tarballDir: tarballDir && path.resolve(tarballDir),
      keep: flags.has("--keep"),
      verbose: flags.has("--verbose"),
      skipE2E: flags.has("--skip-e2e"),
    });
    results.push(result);
    const timing = result.phases.map((p) => `${p.name}=${(p.ms / 1000).toFixed(1)}s`).join(" ");
    console.log(`${result.ok ? "✔" : "✘"} ${variant.name} ${timing}`);
    if (!result.ok) console.log(String(result.error?.message ?? result.error));
    if (flags.has("--keep")) console.log(`  kept ${result.workDir}`);
  }
  const failed = results.filter((r) => !r.ok);
  if (failed.length) {
    console.log(
      `\n${failed.length}/${results.length} variant(s) failed: ${failed.map((r) => r.variant).join(", ")}`,
    );
    process.exit(1);
  }
  console.log(`\nAll ${results.length} variant(s) passed.`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main(process.argv.slice(2));
}
