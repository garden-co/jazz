import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync, spawn } from "node:child_process";
import {
  resolveLocalDeps,
  resolveRemoteDeps,
  type PackageManifest,
  type ResolveProgressCallback,
} from "./deps.js";
import { installJazzSkills } from "./agent-skills.js";

const REPO = "garden-co/jazz";
const PACKAGE_DIR = path.resolve(import.meta.dirname, "..");
const CREATE_JAZZ_VERSION = (
  JSON.parse(fs.readFileSync(path.join(PACKAGE_DIR, "package.json"), "utf-8")) as {
    version: string;
  }
).version;
const RELEASE_REF = `v${CREATE_JAZZ_VERSION}`;
const PREVIEW_SNAPSHOT_FILE = "jazz-source-snapshot.json";
const DEFAULT_STARTER = "next-betterauth";

export const KNOWN_STARTERS = [
  "next-betterauth",
  "next-localfirst",
  "next-hybrid",
  "sveltekit-betterauth",
  "sveltekit-localfirst",
  "sveltekit-hybrid",
  "react-betterauth",
  "react-localfirst",
  "react-hybrid",
  "ts-betterauth",
  "ts-localfirst",
  "ts-hybrid",
  "ts-effect-localfirst",
  "ts-effect-betterauth",
] as const;
export type StarterName = (typeof KNOWN_STARTERS)[number];

function isKnownStarter(name: string): name is StarterName {
  return (KNOWN_STARTERS as readonly string[]).includes(name);
}

/**
 * npm package-name rules: lowercase, URL-safe, no whitespace, no leading dot
 * or underscore, no slashes except in a single leading scope. We apply them
 * to the scaffolded project's directory name too, since it becomes the
 * package.json `name` field downstream.
 */
const APP_NAME_RE = /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]{0,213}$/;

export function validateAppName(name: string): void {
  if (!APP_NAME_RE.test(name)) {
    throw new Error(
      `Invalid app name "${name}". Use lowercase letters, numbers, hyphens, dots, or underscores; no spaces, slashes, or leading dots.`,
    );
  }
}

export interface ScaffoldOptions {
  appName: string;
  targetDir: string;
  pm: string | null;
  starter?: string;
  git?: boolean;
  onStep?: (label: string) => void;
  /**
   * Runs after git init and before `pnpm install`. Use for anything that must
   * land in the scaffolded project before the install runs — e.g. cloud
   * provisioning that writes `.env`. Receives `onStep` so long-running work
   * (e.g. a provisioning HTTP request) can advance the caller's spinner
   * rather than leaving it stuck on the previous step.
   */
  preInstall?: (ctx: { dir: string; onStep: (label: string) => void }) => Promise<void>;
}

const SCAFFOLD_COPY_SKIP = new Set(["node_modules", ".next", ".jazz", ".turbo", ".env", ".git"]);

export interface SourceSnapshot {
  ref: string;
  remoteRef: string;
  label: string;
  previewPackages?: Record<string, string>;
}

/** Resolve the immutable source snapshot bundled with this CLI package. */
export function readSourceSnapshot(packageDir = PACKAGE_DIR): SourceSnapshot {
  const snapshotPath = path.join(packageDir, PREVIEW_SNAPSHOT_FILE);
  if (!fs.existsSync(snapshotPath)) {
    return {
      ref: RELEASE_REF,
      remoteRef: `refs/tags/${RELEASE_REF}`,
      label: RELEASE_REF,
    };
  }
  let snapshot: {
    schema?: unknown;
    packageVersion?: unknown;
    commit?: unknown;
    packages?: unknown;
  };
  try {
    snapshot = JSON.parse(fs.readFileSync(snapshotPath, "utf8")) as typeof snapshot;
  } catch (cause) {
    throw new Error(`Invalid bundled preview source snapshot: ${String(cause)}`, { cause });
  }
  if (
    !snapshot ||
    snapshot.schema !== 2 ||
    snapshot.packageVersion !== CREATE_JAZZ_VERSION ||
    typeof snapshot.commit !== "string" ||
    !/^[a-f0-9]{40}$/.test(snapshot.commit) ||
    !validPreviewPackages(snapshot.packages, snapshot.commit)
  )
    throw new Error(
      "Invalid bundled preview source snapshot; refusing to fall back to a release tag or main.",
    );
  return {
    ref: snapshot.commit,
    remoteRef: snapshot.commit,
    label: `preview commit ${snapshot.commit}`,
    previewPackages: snapshot.packages as Record<string, string>,
  };
}

function validPreviewPackages(value: unknown, commit: string): value is Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const entries = Object.entries(value);
  return (
    Object.hasOwn(value, "create-jazz") &&
    Object.hasOwn(value, "jazz-tools") &&
    entries.every(
      ([name, url]) =>
        /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/.test(name) &&
        url === `https://pkg.pr.new/${REPO}/${name}@${commit}`,
    )
  );
}

function sourceSnapshotError(action: string, snapshot: SourceSnapshot, cause: unknown): Error {
  return new Error(
    `Could not ${action} from immutable source snapshot ${snapshot.label} for create-jazz@${CREATE_JAZZ_VERSION}. ` +
      "Jazz deliberately does not fall back to main, because that could scaffold code incompatible with the installed CLI. " +
      "This release snapshot may be unavailable; upgrade with `npm create jazz@latest` and try again.",
    { cause },
  );
}

async function fetchStarter(
  starter: StarterName,
  dir: string,
  snapshot: SourceSnapshot,
): Promise<void> {
  const localPath = process.env.JAZZ_STARTER_PATH;
  if (localPath) {
    await fs.promises.cp(localPath, dir, {
      recursive: true,
      filter: (src) => !SCAFFOLD_COPY_SKIP.has(path.basename(src)),
    });
    return;
  }
  const tiged = (await import("tiged")).default;
  const emitter = tiged(`${REPO}/starters/${starter}#${snapshot.ref}`, { disableCache: true });
  try {
    await emitter.clone(dir);
  } catch (cause) {
    throw sourceSnapshotError(`fetch starter "${starter}"`, snapshot, cause);
  }
}

async function resolveManifest(
  manifest: PackageManifest,
  snapshot: SourceSnapshot,
  onProgress?: ResolveProgressCallback,
): Promise<PackageManifest> {
  const localPath = process.env.JAZZ_STARTER_PATH;
  if (localPath) {
    return resolveLocalDeps(
      manifest,
      path.resolve(localPath, "../.."),
      onProgress,
      snapshot.previewPackages,
    );
  }
  try {
    return await resolveRemoteDeps(
      manifest,
      { repo: REPO, ref: snapshot.remoteRef },
      onProgress,
      snapshot.previewPackages,
    );
  } catch (cause) {
    if (snapshot.previewPackages)
      throw new Error(
        `Could not resolve preview dependencies: ${cause instanceof Error ? cause.message : String(cause)}`,
        { cause },
      );
    throw sourceSnapshotError("resolve starter dependencies", snapshot, cause);
  }
}

export async function scaffold(
  options: ScaffoldOptions,
  sourceSnapshot = readSourceSnapshot(),
): Promise<void> {
  validateAppName(options.appName);

  const starter = options.starter ?? DEFAULT_STARTER;
  if (!isKnownStarter(starter)) {
    throw new Error(
      `Unknown starter "${starter}". Available starters: ${KNOWN_STARTERS.join(", ")}.`,
    );
  }

  // Refuse to touch a pre-existing directory — the transactional cleanup
  // below is only safe for a directory we just created ourselves.
  if (fs.existsSync(options.targetDir)) {
    throw new Error(
      `Target directory "${options.targetDir}" already exists. Choose a different name or remove it first.`,
    );
  }
  fs.mkdirSync(options.targetDir, { recursive: true });

  // Fetch → resolve deps → git init are transactional: on failure, remove the
  // directory we just created.
  try {
    options.onStep?.("Fetching starter");
    await fetchStarter(starter, options.targetDir, sourceSnapshot);

    options.onStep?.("Resolving dependencies");
    const pkgJsonPath = path.join(options.targetDir, "package.json");
    const rawManifest = JSON.parse(fs.readFileSync(pkgJsonPath, "utf-8")) as PackageManifest;
    const resolved = await resolveManifest(rawManifest, sourceSnapshot, (done, total) => {
      options.onStep?.(`Resolving dependencies (${done}/${total})`);
    });
    const finalManifest = { ...resolved, name: options.appName };
    fs.writeFileSync(pkgJsonPath, JSON.stringify(finalManifest, null, 2) + "\n", "utf-8");

    options.onStep?.("Installing Jazz agent skills");
    installJazzSkills(options.targetDir);

    // Inherits the user's git identity from `~/.gitconfig`, GIT_AUTHOR_*, etc.
    if (options.git !== false) {
      options.onStep?.("Initialising git");
      runGitInit(options.targetDir);
    }
  } catch (err) {
    fs.rmSync(options.targetDir, { recursive: true, force: true });
    throw err;
  }

  // Pre-install hook runs after the transactional steps; failures from here
  // on leave the project intact so the user can inspect / retry manually.
  if (options.preInstall) {
    await options.preInstall({
      dir: options.targetDir,
      onStep: (label) => options.onStep?.(label),
    });
  }

  // Install is not transactional — a failure leaves the project intact so the
  // user can inspect it or retry manually. Stream both pipes to avoid buffering
  // an install's full output while keeping successful output hidden.
  if (options.pm) {
    options.onStep?.("Installing dependencies");
    try {
      await runPackageManagerInstall(options.pm, options.targetDir);
    } catch (err) {
      throw new Error(
        `${options.pm} install failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
}

const INSTALL_OUTPUT_SAMPLE_BYTES = 8 * 1024;
const INSTALL_OUTPUT_HALF_SAMPLE_BYTES = INSTALL_OUTPUT_SAMPLE_BYTES / 2;

interface OutputCapture {
  head: Buffer;
  tail: Buffer;
  totalBytes: number;
}

function createOutputCapture(): OutputCapture {
  return { head: Buffer.alloc(0), tail: Buffer.alloc(0), totalBytes: 0 };
}

function captureOutput(capture: OutputCapture, chunk: Buffer): void {
  capture.totalBytes += chunk.length;

  const headBytes = Math.max(0, INSTALL_OUTPUT_HALF_SAMPLE_BYTES - capture.head.length);
  if (headBytes > 0) {
    capture.head = Buffer.concat([capture.head, chunk.subarray(0, headBytes)]);
  }

  const tail = Buffer.concat([capture.tail, chunk]);
  const tailStart = Math.max(0, tail.length - INSTALL_OUTPUT_HALF_SAMPLE_BYTES);
  capture.tail = Buffer.from(tail.subarray(tailStart));
}

function formatOutputCapture(capture: OutputCapture): string {
  if (capture.totalBytes <= INSTALL_OUTPUT_SAMPLE_BYTES) {
    const overlap = Math.max(0, capture.head.length + capture.tail.length - capture.totalBytes);
    return Buffer.concat([capture.head, capture.tail.subarray(overlap)])
      .toString("utf8")
      .trim();
  }

  return [
    capture.head.toString("utf8"),
    "[... output truncated ...]",
    capture.tail.toString("utf8"),
  ]
    .join("")
    .trim();
}

function formatInstallDiagnostics(stdout: OutputCapture, stderr: OutputCapture): string {
  return [
    ["stdout", formatOutputCapture(stdout)],
    ["stderr", formatOutputCapture(stderr)],
  ]
    .filter(([, output]) => output)
    .map(([stream, output]) => `${stream}:\n${output}`)
    .join("\n");
}

function runPackageManagerInstall(pm: string, cwd: string): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  const stdout = createOutputCapture();
  const stderr = createOutputCapture();
  const child = spawn(pm, ["install"], {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let spawnError: Error | undefined;

  child.stdout.on("data", (chunk: Buffer) => captureOutput(stdout, chunk));
  child.stderr.on("data", (chunk: Buffer) => captureOutput(stderr, chunk));
  child.on("error", (err) => {
    spawnError = err;
  });
  child.on("close", (code, signal) => {
    if (!spawnError && code === 0) {
      resolve();
      return;
    }

    const diagnostics = formatInstallDiagnostics(stdout, stderr);
    reject(
      new Error(
        diagnostics ||
          spawnError?.message ||
          (signal ? `terminated by signal ${signal}` : `exited with code ${code}`),
      ),
    );
  });

  return promise;
}

function runGitInit(cwd: string): void {
  const execOpts = { cwd, stdio: "pipe" as const };
  try {
    execFileSync("git", ["init"], execOpts);
    execFileSync("git", ["add", "."], execOpts);
    execFileSync("git", ["commit", "--no-gpg-sign", "-m", "Initial commit"], execOpts);
  } catch (err) {
    const stderr = getStderr(err);
    if (/auto-detect|author identity unknown|please tell me who you are/i.test(stderr)) {
      throw new Error(
        [
          "Git commit failed: no git identity configured.",
          "Set one globally:",
          '  git config --global user.email "you@example.com"',
          '  git config --global user.name  "Your Name"',
          "Or re-run create-jazz with --no-git to skip the initial commit.",
        ].join("\n"),
      );
    }
    throw new Error(`git init failed: ${stderr || String(err)}`);
  }
}

function getStderr(err: unknown): string {
  return err instanceof Error && "stderr" in err
    ? String((err as { stderr: Buffer | string }).stderr)
    : "";
}
