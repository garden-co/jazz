#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import {
  benchmarksForSuite,
  DEFAULT_BENCHMARK_TIMEOUT_SECONDS,
  readSkipSet,
  skipIds,
} from "./ci_benchmarks.mjs";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/run_ci_benchmarks.mjs \\
    --suite native|browser \\
    --out-dir bench-out/native \\
    [--profile s] \\
    [--skip-set benchmarks/realistic/ci_skip_set.json] \\
    [--timeout-seconds 60]
`);
}

function parseArgs(argv) {
  const out = {
    suite: "",
    outDir: "",
    profile: "s",
    skipSet: "benchmarks/realistic/ci_skip_set.json",
    timeoutSeconds: DEFAULT_BENCHMARK_TIMEOUT_SECONDS,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--suite") {
      out.suite = argv[++i] ?? "";
      continue;
    }
    if (arg === "--out-dir") {
      out.outDir = argv[++i] ?? "";
      continue;
    }
    if (arg === "--profile") {
      out.profile = argv[++i] ?? "";
      continue;
    }
    if (arg === "--skip-set") {
      out.skipSet = argv[++i] ?? "";
      continue;
    }
    if (arg === "--timeout-seconds") {
      out.timeoutSeconds = Number(argv[++i] ?? String(DEFAULT_BENCHMARK_TIMEOUT_SECONDS));
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  if (!out.suite) fail("--suite is required");
  if (!out.outDir) fail("--out-dir is required");
  if (!Number.isFinite(out.timeoutSeconds) || out.timeoutSeconds < 1) {
    fail("--timeout-seconds must be a number >= 1");
  }

  return out;
}

function mkdirp(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function rel(file) {
  return path.relative(process.cwd(), file);
}

function isoNow() {
  return new Date().toISOString();
}

function fmtDuration(durationMs) {
  if (!Number.isFinite(durationMs)) return "n/a";
  if (durationMs < 1000) return `${durationMs.toFixed(0)} ms`;
  return `${(durationMs / 1000).toFixed(2)} s`;
}

function shellQuote(parts) {
  return parts
    .map((part) => {
      if (/^[A-Za-z0-9_./:=+-]+$/.test(part)) return part;
      return `'${String(part).replace(/'/g, `'\\''`)}'`;
    })
    .join(" ");
}

function createLineBuffer(onLine) {
  let buffer = "";
  return (chunk) => {
    buffer += chunk;
    while (true) {
      const idx = buffer.indexOf("\n");
      if (idx === -1) break;
      const line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      onLine(line);
    }
  };
}

async function killChild(child) {
  if (!child || child.killed) return;
  if (process.platform !== "win32" && typeof child.pid === "number") {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
  } else {
    try {
      child.kill("SIGTERM");
    } catch {}
  }

  await new Promise((resolve) => setTimeout(resolve, 1500));

  if (child.exitCode == null) {
    if (process.platform !== "win32" && typeof child.pid === "number") {
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {}
    } else {
      try {
        child.kill("SIGKILL");
      } catch {}
    }
  }
}

async function runCommand({
  command,
  cwd,
  env,
  timeoutSeconds,
  stdoutFile,
  logFile,
  streamStdoutToConsole = false,
}) {
  const startedAt = Date.now();
  let timedOut = false;

  mkdirp(path.dirname(logFile));
  const logStream = fs.createWriteStream(logFile, { flags: "w" });
  if (stdoutFile) mkdirp(path.dirname(stdoutFile));
  const stdoutStream = stdoutFile ? fs.createWriteStream(stdoutFile, { flags: "w" }) : null;
  const child = spawn(command[0], command.slice(1), {
    cwd,
    env,
    detached: process.platform !== "win32",
    stdio: ["ignore", "pipe", "pipe"],
  });

  const stdoutLines = [];
  const captureStdoutLine = createLineBuffer((line) => stdoutLines.push(line));
  const stderrLines = [];
  const captureStderrLine = createLineBuffer((line) => stderrLines.push(line));

  child.stdout.on("data", (chunk) => {
    const text = chunk.toString("utf8");
    if (stdoutStream) stdoutStream.write(text);
    if (streamStdoutToConsole) process.stdout.write(text);
    logStream.write(text);
    captureStdoutLine(text);
  });

  child.stderr.on("data", (chunk) => {
    const text = chunk.toString("utf8");
    process.stderr.write(text);
    logStream.write(text);
    captureStderrLine(text);
  });

  const timer = setTimeout(async () => {
    timedOut = true;
    const notice = `\n[benchmark-timeout] exceeded ${timeoutSeconds}s budget\n`;
    process.stderr.write(notice);
    logStream.write(notice);
    await killChild(child);
  }, timeoutSeconds * 1000);

  const settled = await new Promise((resolve) => {
    child.on("error", (error) => resolve({ code: null, signal: null, error }));
    child.on("close", (code, signal) => resolve({ code, signal, error: null }));
  });
  clearTimeout(timer);

  await Promise.all([
    new Promise((resolve) => logStream.end(resolve)),
    stdoutStream ? new Promise((resolve) => stdoutStream.end(resolve)) : Promise.resolve(),
  ]);

  return {
    ...settled,
    timedOut,
    durationMs: Date.now() - startedAt,
    stdoutLines,
    stderrLines,
  };
}

function statusForRun(result) {
  if (result.timedOut) return "timed_out";
  if (result.error) return "failed";
  if (result.code === 0) return "passed";
  return "failed";
}

function failureNote(result) {
  if (result.timedOut) {
    return `[benchmark-timeout] exceeded budget`;
  }

  const lines = [...(result.stderrLines ?? []), ...(result.stdoutLines ?? [])]
    .map((line) => String(line ?? "").trim())
    .filter(Boolean);
  for (const line of lines) {
    if (
      line.startsWith("Error:") ||
      line.includes("Unhandled Error") ||
      line.includes("Executable doesn't exist")
    ) {
      return line;
    }
  }

  return null;
}

function summarizeBenchmark(benchmark, status, durationMs, extra = {}) {
  return {
    id: benchmark.id,
    suite: benchmark.suite,
    label: benchmark.label,
    kind: benchmark.kind,
    status,
    duration_ms: durationMs,
    ...extra,
  };
}

async function runNativeBenchmark(benchmark, args) {
  const logFile = path.resolve(args.outDir, benchmark.log_path);

  if (benchmark.kind === "native-example") {
    const outputFile = path.resolve(args.outDir, benchmark.output_path);
    const tempOutputFile = `${outputFile}.partial`;
    fs.rmSync(tempOutputFile, { force: true });
    const command = [
      "cargo",
      "run",
      "--release",
      "-p",
      "jazz-tools",
      "--features",
      "client",
      "--example",
      "realistic_bench",
      "--",
      "--profile",
      `benchmarks/realistic/profiles/${args.profile}.json`,
      "--scenario",
      benchmark.scenario_path,
    ];

    console.log(`\n==> ${benchmark.label}`);
    console.log(shellQuote(command));
    const result = await runCommand({
      command,
      cwd: process.cwd(),
      env: process.env,
      timeoutSeconds: args.timeoutSeconds,
      stdoutFile: tempOutputFile,
      logFile,
    });

    const status = statusForRun(result);
    const tempExists = fs.existsSync(tempOutputFile) && fs.statSync(tempOutputFile).size > 0;
    if (status === "passed" && tempExists) {
      fs.renameSync(tempOutputFile, outputFile);
    } else {
      fs.rmSync(tempOutputFile, { force: true });
    }
    const fileExists = fs.existsSync(outputFile) && fs.statSync(outputFile).size > 0;
    const finalStatus = status === "passed" && !fileExists ? "failed" : status;
    return summarizeBenchmark(benchmark, finalStatus, result.durationMs, {
      command,
      scenario_path: benchmark.scenario_path,
      output_path: fileExists ? rel(outputFile) : null,
      log_path: rel(logFile),
      exit_code: result.code,
      signal: result.signal,
      timeout_seconds: args.timeoutSeconds,
      note:
        finalStatus === "failed" && status === "passed" && !fileExists
          ? "Benchmark completed without emitting JSON output."
          : failureNote(result),
    });
  }

  const command = [
    "cargo",
    "bench",
    "-p",
    "jazz-tools",
    "--features",
    "surrealkv",
    "--bench",
    "realistic_phase1",
    "--",
    benchmark.criterion_filter,
  ];

  console.log(`\n==> ${benchmark.label}`);
  console.log(shellQuote(command));
  const result = await runCommand({
    command,
    cwd: process.cwd(),
    env: process.env,
    timeoutSeconds: args.timeoutSeconds,
    logFile,
    streamStdoutToConsole: true,
  });

  return summarizeBenchmark(benchmark, statusForRun(result), result.durationMs, {
    command,
    criterion_filter: benchmark.criterion_filter,
    log_path: rel(logFile),
    exit_code: result.code,
    signal: result.signal,
    timeout_seconds: args.timeoutSeconds,
    note: failureNote(result),
  });
}

function parseBrowserReport(lines) {
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i]?.trim();
    if (!line || !line.startsWith("[realistic-bench]")) continue;
    const payload = line.slice("[realistic-bench]".length).trim();
    return JSON.parse(payload);
  }
  return null;
}

async function runBrowserBenchmark(benchmark, args) {
  const logFile = path.resolve(args.outDir, benchmark.log_path);
  const outputFile = path.resolve(args.outDir, benchmark.output_path);
  const command = ["pnpm", "--dir", "packages/jazz-tools", "run", "bench:realistic:browser"];
  const env = {
    ...process.env,
    JAZZ_REALISTIC_BROWSER_SCENARIOS: benchmark.scenario_id,
  };

  console.log(`\n==> ${benchmark.label}`);
  console.log(`JAZZ_REALISTIC_BROWSER_SCENARIOS=${benchmark.scenario_id} ${shellQuote(command)}`);
  const result = await runCommand({
    command,
    cwd: process.cwd(),
    env,
    timeoutSeconds: args.timeoutSeconds,
    logFile,
    streamStdoutToConsole: true,
  });

  const status = statusForRun(result);
  let scenario = null;
  let note = null;
  if (status === "passed") {
    try {
      const report = parseBrowserReport(result.stdoutLines);
      const scenarios = Array.isArray(report?.scenarios) ? report.scenarios : [];
      scenario = scenarios.find((entry) => entry?.scenario_id === benchmark.scenario_id) ?? null;
      if (!scenario) {
        note = `Browser report did not include scenario ${benchmark.scenario_id}.`;
      } else {
        mkdirp(path.dirname(outputFile));
        fs.writeFileSync(outputFile, `${JSON.stringify(scenario, null, 2)}\n`);
      }
    } catch (error) {
      note = error instanceof Error ? error.message : String(error);
    }
  }

  return summarizeBenchmark(
    benchmark,
    status === "passed" && !scenario ? "failed" : status,
    result.durationMs,
    {
      command,
      scenario_id: benchmark.scenario_id,
      output_path: scenario ? rel(outputFile) : null,
      log_path: rel(logFile),
      exit_code: result.code,
      signal: result.signal,
      timeout_seconds: args.timeoutSeconds,
      note: note ?? failureNote(result),
      scenario,
    },
  );
}

function countByStatus(results) {
  const counts = new Map();
  for (const result of results) {
    counts.set(result.status, (counts.get(result.status) ?? 0) + 1);
  }
  return counts;
}

function summaryMarkdown(suite, results, timeoutSeconds) {
  const counts = countByStatus(results);
  const lines = [];
  lines.push(`## ${suite === "native" ? "Native" : "Browser"} benchmark status`);
  lines.push("");
  lines.push(`Budget per benchmark: ${timeoutSeconds}s`);
  lines.push(
    `Passed: ${counts.get("passed") ?? 0}, timed out: ${counts.get("timed_out") ?? 0}, failed: ${counts.get("failed") ?? 0}, configured skip: ${counts.get("skipped_configured") ?? 0}`,
  );
  lines.push("");
  lines.push("| Benchmark | Status | Duration | Note |");
  lines.push("|---|---|---:|---|");
  for (const result of results) {
    lines.push(
      `| ${result.label} | ${result.status} | ${fmtDuration(result.duration_ms)} | ${result.note ?? ""} |`,
    );
  }
  lines.push("");

  const skipCandidates = results.filter((result) => result.status === "timed_out");
  if (skipCandidates.length > 0) {
    lines.push("Recommended skip-set additions:");
    lines.push("");
    for (const result of skipCandidates) {
      lines.push(`- \`${result.id}\``);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function writeJson(file, value) {
  mkdirp(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outDir = path.resolve(args.outDir);
  mkdirp(outDir);

  const skipSet = readSkipSet(path.resolve(args.skipSet));
  const skipIdSet = skipIds(skipSet);
  const catalog = benchmarksForSuite(args.suite);
  const results = [];

  for (const benchmark of catalog) {
    if (skipIdSet.has(benchmark.id)) {
      results.push(
        summarizeBenchmark(benchmark, "skipped_configured", 0, {
          timeout_seconds: args.timeoutSeconds,
          note: "Skipped by benchmarks/realistic/ci_skip_set.json",
        }),
      );
      continue;
    }

    if (args.suite === "native") {
      results.push(await runNativeBenchmark(benchmark, args));
      continue;
    }

    results.push(await runBrowserBenchmark(benchmark, args));
  }

  const generatedAt = isoNow();
  const statusFile = path.join(outDir, "suite_status.json");
  const summaryFile = path.join(outDir, "summary.md");
  const skipCandidatesFile = path.join(outDir, "skip_candidates.json");
  const statusPayload = {
    version: 1,
    generated_at: generatedAt,
    suite: args.suite,
    profile: args.profile,
    timeout_seconds: args.timeoutSeconds,
    benchmarks: results,
  };
  writeJson(statusFile, statusPayload);
  fs.writeFileSync(summaryFile, summaryMarkdown(args.suite, results, args.timeoutSeconds));
  writeJson(skipCandidatesFile, {
    version: 1,
    generated_at: generatedAt,
    suite: args.suite,
    timeout_seconds: args.timeoutSeconds,
    benchmark_ids: results
      .filter((result) => result.status === "timed_out")
      .map((result) => result.id),
  });

  if (args.suite === "browser") {
    const scenarios = results
      .filter((result) => result.status === "passed" && result.scenario)
      .map((result) => result.scenario);
    writeJson(path.join(outDir, "realistic.json"), {
      runner: "jazz-ts-browser-opfs",
      generated_at: generatedAt,
      profile: args.profile,
      scenarios,
      benchmark_statuses: results.map(({ scenario, ...rest }) => rest),
    });
  }

  console.log(fs.readFileSync(summaryFile, "utf8"));
}

main().catch((error) => {
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  process.exit(1);
});
