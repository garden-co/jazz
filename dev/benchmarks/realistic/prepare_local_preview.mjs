#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {
    repo: "garden-co/jazz2",
    branch: "",
    out: "bench-out/local-preview",
    runId: "",
    includeMain: true,
    mainRunId: "",
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--repo") {
      out.repo = argv[++i] ?? "";
      continue;
    }
    if (arg === "--branch") {
      out.branch = argv[++i] ?? "";
      continue;
    }
    if (arg === "--out") {
      out.out = argv[++i] ?? "";
      continue;
    }
    if (arg === "--run-id") {
      out.runId = argv[++i] ?? "";
      continue;
    }
    if (arg === "--main-run-id") {
      out.mainRunId = argv[++i] ?? "";
      continue;
    }
    if (arg === "--no-main") {
      out.includeMain = false;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  return out;
}

const RETIRED_WORKFLOW_MESSAGE =
  "The Realistic Benchmarks workflow (.github/workflows/benchmarks.yml) was retired on " +
  "2026-10-15, so there are no new runs to discover. Pass --run-id (and optionally " +
  "--main-run-id) for a historical run whose realistic-site artifact is still retained.";

function printHelp() {
  console.log(`Usage:
  node dev/benchmarks/realistic/prepare_local_preview.mjs \\
    --run-id 22833327994 \\
    [--main-run-id 22833300000] \\
    [--branch codex/benchmark-runner] \\
    [--out bench-out/local-preview]

The Realistic Benchmarks workflow was retired on 2026-10-15. This script only
previews realistic-site artifacts from historical runs, so --run-id is
required; main history is merged only when --main-run-id is given.

Defaults:
  --repo garden-co/jazz2
  --branch current git branch (label only)
  --out bench-out/local-preview
`);
}

function run(command, args, options = {}) {
  try {
    return execFileSync(command, args, {
      cwd: options.cwd || process.cwd(),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  } catch (error) {
    const stderr =
      error && typeof error.stderr === "string" && error.stderr.trim()
        ? error.stderr.trim()
        : error && error.message
          ? String(error.message)
          : String(error);
    fail(`${command} ${args.join(" ")} failed\n${stderr}`);
  }
}

function currentBranch() {
  return run("git", ["branch", "--show-current"]);
}

function artifactInfo(repo, runId) {
  const raw = run("gh", ["api", `repos/${repo}/actions/runs/${runId}/artifacts`]);
  const parsed = JSON.parse(raw);
  const artifacts = Array.isArray(parsed.artifacts) ? parsed.artifacts : [];
  return (
    artifacts.find(
      (artifact) =>
        typeof artifact.name === "string" && artifact.name.startsWith("realistic-site-"),
    ) || null
  );
}

function downloadSiteHistory(repo, runId, label, tempRoot) {
  const artifact = artifactInfo(repo, runId);
  if (!artifact) return null;

  const dest = path.join(tempRoot, label);
  fs.mkdirSync(dest, { recursive: true });
  run("gh", ["run", "download", runId, "--repo", repo, "--name", artifact.name, "--dir", dest]);

  const historyPath = findFile(dest, "history.json");
  if (!historyPath) {
    fail(`Downloaded artifact ${artifact.name} from run ${runId}, but no history.json was found`);
  }

  return {
    runId,
    artifactName: artifact.name,
    historyPath,
  };
}

function findFile(rootDir, fileName) {
  const entries = fs.readdirSync(rootDir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isFile() && entry.name === fileName) return fullPath;
    if (entry.isDirectory()) {
      const nested = findFile(fullPath, fileName);
      if (nested) return nested;
    }
  }
  return "";
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const branch = args.branch || currentBranch();
  if (!branch) fail("Could not determine git branch");

  const branchRunId = args.runId;
  if (!branchRunId) fail(RETIRED_WORKFLOW_MESSAGE);

  const outDir = path.resolve(args.out);
  fs.rmSync(outDir, { recursive: true, force: true });
  fs.mkdirSync(outDir, { recursive: true });

  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-bench-preview-"));
  const histories = [];
  const downloads = [];

  try {
    if (args.includeMain) {
      const mainRunId = args.mainRunId;
      if (mainRunId) {
        const mainDownload = downloadSiteHistory(args.repo, mainRunId, "main", tempRoot);
        if (mainDownload) {
          histories.push(mainDownload.historyPath);
          downloads.push({ label: "main", ...mainDownload });
        }
      }
    }

    const branchDownload = downloadSiteHistory(args.repo, branchRunId, "branch", tempRoot);
    if (!branchDownload) {
      fail(`Run ${branchRunId} does not contain a realistic-site artifact`);
    }
    histories.push(branchDownload.historyPath);
    downloads.push({ label: branch, ...branchDownload });

    const mergedHistory = path.join(outDir, "history.json");
    const mergeArgs = ["dev/benchmarks/realistic/merge_histories.mjs", "--out", mergedHistory];
    for (const historyPath of histories) {
      mergeArgs.push("--history", historyPath);
    }
    run("node", mergeArgs);

    const siteDir = path.join(outDir, "site");
    run("node", [
      "dev/benchmarks/realistic/build_site.mjs",
      "--history",
      mergedHistory,
      "--out",
      siteDir,
    ]);

    const metadata = {
      repo: args.repo,
      branch,
      branch_run_id: branchRunId,
      include_main: args.includeMain,
      histories: downloads,
      generated_at: new Date().toISOString(),
      merged_history: mergedHistory,
      site_dir: siteDir,
    };
    fs.writeFileSync(
      path.join(outDir, "preview_metadata.json"),
      `${JSON.stringify(metadata, null, 2)}\n`,
    );

    console.log(JSON.stringify(metadata, null, 2));
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

main();
