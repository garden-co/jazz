import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/dev";

import { getStarterConfig, type StarterName } from "./starters.js";

const APP_NAME = "test-app";

/**
 * Workspace packages every starter's dependency closure can transitively pull
 * in. We pack each as a tarball and pin them via `overrides` in the
 * scaffolded project's `pnpm-workspace.yaml`, so `pnpm install` resolves
 * against the local workspace even when the alpha versions aren't on npm
 * yet (e.g. during a release PR).
 */
const PACKAGES_TO_PACK = ["jazz-tools", "jazz-napi", "jazz-wasm"] as const;

export interface RunStarterOptions {
  starter: StarterName;
  repoRoot: string;
  /** If provided, scaffold under here; else mkdtemp. */
  workDir?: string;
  /** Skip the playwright step (build-only). Useful for a fast smoke pass. */
  skipE2E?: boolean;
  /** Leave the scaffolded dir on disk so it can be inspected after a failure. */
  keepTempDir?: boolean;
  /** Stream child stdout/stderr instead of buffering. */
  verbose?: boolean;
  /**
   * If provided, skip the in-process `pnpm pack` step and resolve workspace
   * tarballs from this directory (one `*.tgz` per package in PACKAGES_TO_PACK).
   * The CI workflow uses this to build tarballs once in a prepare job and reuse
   * them across the matrix without rebuilding the workspace each time.
   */
  tarballDir?: string;
}

export interface PhaseTiming {
  name: string;
  durationMs: number;
}

export interface RunStarterResult {
  starter: StarterName;
  success: boolean;
  durations: PhaseTiming[];
  appDir: string;
  errorMessage?: string;
}

async function runChild(
  cmd: string,
  args: string[],
  opts: {
    cwd: string;
    /**
     * If provided, this is the COMPLETE environment for the child — runChild
     * does not merge in `process.env`. Callers that want to inherit the parent
     * env should spread it themselves (`{ ...process.env, EXTRA: "..." }`).
     * This is so callers can `delete` keys (e.g. `npm_config_user_agent` to
     * stop create-jazz auto-detecting a package manager) without runChild
     * re-introducing them via the spread.
     */
    env?: NodeJS.ProcessEnv;
    verbose?: boolean;
    description: string;
    /** Soft timeout — kills the child and rejects after N ms. */
    timeoutMs?: number;
  },
): Promise<void> {
  return await new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: opts.cwd,
      env: opts.env ?? process.env,
      stdio: opts.verbose ? "inherit" : ["ignore", "pipe", "pipe"],
    });
    const chunks: string[] = [];
    if (!opts.verbose) {
      child.stdout?.on("data", (d) => chunks.push(d.toString()));
      child.stderr?.on("data", (d) => chunks.push(d.toString()));
    }
    let timer: NodeJS.Timeout | undefined;
    if (opts.timeoutMs !== undefined) {
      timer = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new Error(`${opts.description} timed out after ${opts.timeoutMs}ms`));
      }, opts.timeoutMs);
    }
    child.on("error", (err) => {
      if (timer) clearTimeout(timer);
      reject(err);
    });
    child.on("exit", (code, signal) => {
      if (timer) clearTimeout(timer);
      if (code === 0) {
        resolve();
        return;
      }
      const tail = chunks.join("").split("\n").slice(-80).join("\n");
      reject(
        new Error(
          `${opts.description} exited with code=${code} signal=${signal ?? "none"}\n${tail}`,
        ),
      );
    });
  });
}

/**
 * jazz-napi's tarball intentionally excludes the per-platform `.node` files
 * (napi-rs's standard publish layout puts them in sibling packages like
 * `@garden-co/jazz-napi-darwin-arm64`). During an e2e run those siblings
 * aren't published, so we'd have an empty install. After `pnpm install`, copy
 * the locally-built `.node` files into every installed jazz-napi directory so
 * the bundled `require('./jazz-napi.<platform>.node')` lookup succeeds.
 *
 * We deliberately don't use `NAPI_RS_NATIVE_LIBRARY_PATH` here — that env var
 * is honoured by every napi-rs package (including rolldown, which vite 8
 * uses), so setting it to jazz-napi's binary breaks the build.
 */
function patchInstalledJazzNapi(appDir: string, repoRoot: string): void {
  const napiSourceDir = path.join(repoRoot, "crates/jazz-napi");
  if (!fs.existsSync(napiSourceDir)) return;
  const binaries = fs
    .readdirSync(napiSourceDir)
    .filter((f) => f.endsWith(".node"))
    .map((f) => path.join(napiSourceDir, f));
  if (binaries.length === 0) return;

  const installedDirs: string[] = [];
  const visited = new Set<string>();
  function walk(dir: string, depth = 0): void {
    if (depth > 8 || visited.has(dir) || !fs.existsSync(dir)) return;
    visited.add(dir);
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      const child = path.join(dir, e.name);
      if (e.name === "jazz-napi") {
        installedDirs.push(child);
      }
      walk(child, depth + 1);
    }
  }
  walk(path.join(appDir, "node_modules"));

  for (const dir of installedDirs) {
    for (const src of binaries) {
      fs.copyFileSync(src, path.join(dir, path.basename(src)));
    }
  }
}

function discoverPrebuiltTarballs(dir: string): Record<string, string> {
  if (!fs.existsSync(dir)) {
    throw new Error(`--tarball-dir ${dir} does not exist`);
  }
  const entries = fs.readdirSync(dir);
  const tarballs: Record<string, string> = {};
  for (const pkg of PACKAGES_TO_PACK) {
    const matches = entries.filter((f) => f.startsWith(`${pkg}-`) && f.endsWith(".tgz")).sort();
    if (matches.length === 0) {
      throw new Error(`--tarball-dir ${dir} has no tarball for "${pkg}"`);
    }
    tarballs[pkg] = path.join(dir, matches[matches.length - 1]);
  }
  return tarballs;
}

async function packWorkspaceTarballs(
  repoRoot: string,
  destDir: string,
  verbose: boolean,
): Promise<Record<string, string>> {
  const tarballs: Record<string, string> = {};
  for (const pkg of PACKAGES_TO_PACK) {
    const candidates = [path.join(repoRoot, "packages", pkg), path.join(repoRoot, "crates", pkg)];
    const pkgDir = candidates.find((d) => fs.existsSync(path.join(d, "package.json")));
    if (!pkgDir) {
      throw new Error(`Could not locate workspace package "${pkg}" under packages/ or crates/`);
    }
    await runChild("pnpm", ["pack", "--pack-destination", destDir], {
      cwd: pkgDir,
      verbose,
      description: `pnpm pack ${pkg}`,
    });
    const entries = fs
      .readdirSync(destDir)
      .filter((f) => f.startsWith(`${pkg}-`) && f.endsWith(".tgz"));
    if (entries.length === 0) {
      throw new Error(`pnpm pack ${pkg} produced no tarball in ${destDir}`);
    }
    entries.sort();
    tarballs[pkg] = path.join(destDir, entries[entries.length - 1]);
  }
  return tarballs;
}

/**
 * Pinned upstream versions the harness needs to override regardless of what
 * any individual starter's transitive resolution lands on.
 *
 * `kysely`: `@better-auth/kysely-adapter@1.6.12` declares
 * `peerDependencies.kysely: "^0.28.17 || ^0.29.0"` but still imports
 * `DEFAULT_MIGRATION_LOCK_TABLE` and `DEFAULT_MIGRATION_TABLE` from kysely —
 * symbols kysely removed in 0.29.0 (a legitimate breaking change under
 * pre-1.0 semver). The widened peer range opts the adapter into a kysely it
 * doesn't actually work with, and the SSR-bundled starters (next-*,
 * sveltekit-*) crash trying to resolve the missing exports. Pinning to the
 * last 0.28.x keeps the e2e build green until upstream catches up.
 */
const UPSTREAM_PINS: Record<string, string> = {
  kysely: "0.28.17",
};

/**
 * pnpm 10+ stopped reading the `pnpm` field in package.json — it emits
 * `[WARN] The "pnpm" field in package.json is no longer read by pnpm` and
 * ignores anything underneath, including `overrides`. The canonical home is
 * `pnpm-workspace.yaml` (yes, even in a non-workspace single-project setup).
 *
 * Build-script approval is handled at install time via `--ignore-scripts`
 * rather than `onlyBuiltDependencies` here, because the allowlist mechanism
 * has shifted between pnpm 10 and 11 and skipping postinstalls is safe for
 * the e2e harness — the few packages affected ship working binaries via
 * optionalDependencies.
 */
function writeScaffoldedPnpmConfig(appDir: string, tarballs: Record<string, string>): void {
  const overrideLines = [
    ...Object.entries(tarballs).map(([pkg, tgz]) => `  "${pkg}": "file:${tgz}"`),
    ...Object.entries(UPSTREAM_PINS).map(([pkg, v]) => `  "${pkg}": "${v}"`),
  ].join("\n");
  const yaml = `overrides:\n${overrideLines}\n`;
  fs.writeFileSync(path.join(appDir, "pnpm-workspace.yaml"), yaml, "utf-8");
}

function writeEnvFile(
  appDir: string,
  starter: StarterName,
  server: LocalJazzServerHandle,
  config: ReturnType<typeof getStarterConfig>,
): void {
  const prefix = config.envPrefix;
  const lines: string[] = [
    `${prefix}_JAZZ_APP_ID=${server.appId}`,
    `${prefix}_JAZZ_SERVER_URL=${server.url}`,
    `APP_ORIGIN=${config.appOrigin}`,
  ];
  if (server.backendSecret) {
    lines.push(`BACKEND_SECRET=${server.backendSecret}`);
  }
  if (starter.endsWith("-betterauth") || starter.endsWith("-hybrid")) {
    lines.push(`BETTER_AUTH_SECRET=${randomBytes(32).toString("base64url")}`);
  }
  fs.writeFileSync(path.join(appDir, ".env"), lines.join("\n") + "\n", "utf-8");
}

export async function runStarter(opts: RunStarterOptions): Promise<RunStarterResult> {
  const verbose = !!opts.verbose;
  const config = getStarterConfig(opts.starter);
  const durations: PhaseTiming[] = [];
  let server: LocalJazzServerHandle | undefined;
  const workDir = opts.workDir ?? fs.mkdtempSync(path.join(os.tmpdir(), `cje2e-${opts.starter}-`));
  fs.mkdirSync(workDir, { recursive: true });
  const tarballDir = path.join(workDir, "_tarballs");
  fs.mkdirSync(tarballDir, { recursive: true });
  const appDir = path.join(workDir, APP_NAME);

  const recordPhase = async <T>(name: string, fn: () => Promise<T>): Promise<T> => {
    const t0 = Date.now();
    try {
      return await fn();
    } finally {
      durations.push({ name, durationMs: Date.now() - t0 });
    }
  };

  try {
    const tarballs = await recordPhase("pack", async () =>
      opts.tarballDir
        ? discoverPrebuiltTarballs(opts.tarballDir)
        : packWorkspaceTarballs(opts.repoRoot, tarballDir, verbose),
    );

    await recordPhase("scaffold", async () => {
      const cliEntry = path.join(opts.repoRoot, "packages/create-jazz/src/index.ts");
      const tsxBin = path.join(opts.repoRoot, "packages/create-jazz/node_modules/.bin/tsx");
      const starterPath = path.join(opts.repoRoot, "starters", opts.starter);
      const env: NodeJS.ProcessEnv = {
        ...process.env,
        JAZZ_STARTER_PATH: starterPath,
        GIT_AUTHOR_NAME: "create-jazz-e2e",
        GIT_AUTHOR_EMAIL: "tests@create-jazz.invalid",
        GIT_COMMITTER_NAME: "create-jazz-e2e",
        GIT_COMMITTER_EMAIL: "tests@create-jazz.invalid",
      };
      // create-jazz auto-installs if it can detect a package manager via
      // `npm_config_user_agent`. We unset it so the CLI exits after scaffolding;
      // we run pnpm install ourselves below, after writing the workspace
      // overrides. Otherwise the install runs first and resolves against npm.
      delete env.npm_config_user_agent;

      await runChild(
        tsxBin,
        [cliEntry, APP_NAME, "--starter", opts.starter, "--hosting", "selfhosted", "--no-git"],
        { cwd: workDir, env, verbose, description: `create-jazz ${opts.starter}` },
      );
    });

    writeScaffoldedPnpmConfig(appDir, tarballs);

    await recordPhase("install", () =>
      // No `--ignore-workspace` here: that would make pnpm skip the
      // pnpm-workspace.yaml we just wrote (which is where pnpm 10+ reads
      // `overrides` from). The scaffolded folder lives in $TMPDIR, so there's
      // no risk of pnpm walking up into the jazz monorepo's workspace by
      // accident.
      //
      // `--ignore-scripts`: pnpm 10+ exits non-zero from `pnpm install` if it
      // sees postinstall scripts for packages that aren't on the project's
      // build allowlist (`ERR_PNPM_IGNORED_BUILDS`). The allowlist mechanism
      // in pnpm-workspace.yaml isn't reliable across pnpm major versions —
      // and the few build scripts we'd hit (esbuild, sharp, protobufjs) all
      // ship their working binaries via optionalDependencies, so skipping
      // their postinstalls is safe for the e2e purpose.
      runChild(
        "pnpm",
        ["install", "--no-frozen-lockfile", "--prefer-offline", "--ignore-scripts"],
        {
          cwd: appDir,
          verbose,
          description: `pnpm install ${opts.starter}`,
        },
      ),
    );

    patchInstalledJazzNapi(appDir, opts.repoRoot);

    // Start the sync server before we write .env, so we can write the real
    // appId + serverUrl in one go and the build picks them up.
    server = await startLocalJazzServer({ inMemory: true });
    writeEnvFile(appDir, opts.starter, server, config);

    await recordPhase("build", () =>
      runChild("pnpm", ["build"], {
        cwd: appDir,
        verbose,
        description: `pnpm build ${opts.starter}`,
      }),
    );

    if (!opts.skipE2E) {
      // Each scaffolded starter pins its own @playwright/test version, which
      // can differ from whatever's installed globally. Run `playwright install`
      // from inside the scaffolded dir so we fetch exactly the chromium build
      // that version expects. Cheap no-op if it's already on disk.
      await runChild("pnpm", ["exec", "playwright", "install", "chromium"], {
        cwd: appDir,
        verbose,
        description: `playwright install chromium ${opts.starter}`,
      });
      await recordPhase("e2e", () =>
        runChild("pnpm", ["exec", "playwright", "test", "--reporter=line"], {
          cwd: appDir,
          env: { ...process.env, JAZZ_E2E_PROD: "1" },
          verbose,
          description: `playwright test ${opts.starter}`,
        }),
      );
    }

    return { starter: opts.starter, success: true, durations, appDir };
  } catch (err) {
    return {
      starter: opts.starter,
      success: false,
      durations,
      appDir,
      errorMessage: err instanceof Error ? err.message : String(err),
    };
  } finally {
    if (server) {
      try {
        await server.stop();
      } catch {
        // Best-effort.
      }
    }
    if (!opts.keepTempDir) {
      try {
        fs.rmSync(workDir, { recursive: true, force: true });
      } catch {
        // Best-effort.
      }
    }
  }
}
