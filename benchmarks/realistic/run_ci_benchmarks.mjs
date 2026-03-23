#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import {
  benchmarksForSuite,
  DEFAULT_BENCHMARK_TIMEOUT_SECONDS,
  DEFAULT_NOISE_REPEAT_COUNT,
  readSkipSet,
  repeatCountForBenchmark,
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
    [--repeat-count 3] \\
    [--timeout-seconds 60]
`);
}

function parseArgs(argv) {
  const out = {
    suite: "",
    outDir: "",
    profile: "s",
    skipSet: "benchmarks/realistic/ci_skip_set.json",
    repeatCount: DEFAULT_NOISE_REPEAT_COUNT,
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
    if (arg === "--repeat-count") {
      out.repeatCount = Number(argv[++i] ?? String(DEFAULT_NOISE_REPEAT_COUNT));
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
  if (!Number.isFinite(out.repeatCount) || out.repeatCount < 1) {
    fail("--repeat-count must be a number >= 1");
  }

  return out;
}

function mkdirp(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function rel(file) {
  return path.relative(process.cwd(), file);
}

function fileSafeId(value) {
  return String(value)
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function withAttemptSuffix(file, attemptIndex) {
  const parsed = path.parse(file);
  return path.join(parsed.dir, `${parsed.name}.attempt_${attemptIndex}${parsed.ext}`);
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

function readJsonIfExists(file) {
  if (!file || !fs.existsSync(file)) return null;
  const raw = fs.readFileSync(file, "utf8").trim();
  if (!raw) return null;
  return JSON.parse(raw);
}

function cloneJson(value) {
  if (value == null) return {};
  return JSON.parse(JSON.stringify(value));
}

function mean(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

function sampleStddev(values, avg) {
  if (values.length <= 1) return 0;
  const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

function medianAbsoluteDeviation(values, center) {
  const deviations = values.map((value) => Math.abs(value - center));
  return median(deviations);
}

function summarizeSamples(values) {
  const nums = values.map((value) => Number(value)).filter(Number.isFinite);
  if (nums.length === 0) return null;
  const avg = mean(nums);
  const med = median(nums);
  const stddev = sampleStddev(nums, avg);
  const mad = medianAbsoluteDeviation(nums, med);
  return {
    sample_count: nums.length,
    samples: nums,
    median: med,
    mean: avg,
    min: Math.min(...nums),
    max: Math.max(...nums),
    stddev,
    cv_pct: avg === 0 ? null : (Math.abs(stddev) / Math.abs(avg)) * 100,
    mad,
    rel_mad_pct: med === 0 ? null : (Math.abs(mad) / Math.abs(med)) * 100,
  };
}

function metricNoisePct(noiseMetric) {
  if (!noiseMetric || typeof noiseMetric !== "object") return null;
  if (Number.isFinite(Number(noiseMetric.relative_half_width_pct))) {
    return Number(noiseMetric.relative_half_width_pct);
  }
  const candidates = [noiseMetric.cv_pct, noiseMetric.rel_mad_pct]
    .map((value) => Number(value))
    .filter(Number.isFinite);
  if (candidates.length === 0) return null;
  return Math.max(...candidates);
}

function scenarioNoiseMetric(scenario, metric) {
  const noise = scenario?.extra?.noise;
  if (!noise || typeof noise !== "object") return null;
  if (metric === "wall_time_ms" || metric === "throughput_ops_per_sec") {
    return noise.metrics?.[metric] ?? null;
  }
  if (!metric.startsWith("op:")) return null;
  const slash = metric.lastIndexOf("/");
  if (slash <= 3) return null;
  const opName = metric.slice(3, slash);
  const field = metric.slice(slash + 1);
  return noise.operations?.[opName]?.[field] ?? null;
}

function scenarioNoiseSummary(scenario) {
  const wallNoisePct = metricNoisePct(scenarioNoiseMetric(scenario, "wall_time_ms"));
  const repeatsCompleted = Number(scenario?.extra?.noise?.repeats_completed);
  if (!Number.isFinite(wallNoisePct) || !Number.isFinite(repeatsCompleted)) return null;
  return `${repeatsCompleted} runs, wall noise ±${wallNoisePct.toFixed(1)}%`;
}

function aggregateScenarioAttempts(template, scenarios, requestedRepeatCount) {
  const extra = cloneJson(template?.extra ?? {});
  const metricStats = {};

  const wallStats = summarizeSamples(scenarios.map((scenario) => scenario?.wall_time_ms));
  if (wallStats) metricStats.wall_time_ms = wallStats;
  const throughputStats = summarizeSamples(
    scenarios.map((scenario) => scenario?.throughput_ops_per_sec),
  );
  if (throughputStats) metricStats.throughput_ops_per_sec = throughputStats;

  const operationNames = [
    ...new Set(scenarios.flatMap((scenario) => Object.keys(scenario?.operation_summaries ?? {}))),
  ].sort();
  const operationSummaries = {};
  const operationNoise = {};

  for (const opName of operationNames) {
    const counts = summarizeSamples(
      scenarios.map((scenario) => scenario?.operation_summaries?.[opName]?.count),
    );
    const summary = {};
    const noiseSummary = {};
    if (counts) {
      summary.count = Math.round(counts.median);
    }

    for (const field of ["avg_ms", "p50_ms", "p95_ms", "p99_ms"]) {
      const stats = summarizeSamples(
        scenarios.map((scenario) => scenario?.operation_summaries?.[opName]?.[field]),
      );
      if (!stats) continue;
      summary[field] = stats.median;
      noiseSummary[field] = stats;
    }

    if (Object.keys(summary).length > 0) {
      operationSummaries[opName] = summary;
      if (Object.keys(noiseSummary).length > 0) {
        operationNoise[opName] = noiseSummary;
      }
    }
  }

  extra.noise = {
    source: "repeated_runs",
    estimator: "median",
    repeats_requested: requestedRepeatCount,
    repeats_completed: scenarios.length,
    metrics: metricStats,
    operations: operationNoise,
  };

  const totalOperations = summarizeSamples(scenarios.map((scenario) => scenario?.total_operations));

  return {
    ...cloneJson(template),
    total_operations: totalOperations
      ? Math.round(totalOperations.median)
      : (template?.total_operations ?? null),
    wall_time_ms: wallStats ? wallStats.median : (template?.wall_time_ms ?? null),
    throughput_ops_per_sec: throughputStats
      ? throughputStats.median
      : (template?.throughput_ops_per_sec ?? null),
    operation_summaries: operationSummaries,
    extra,
  };
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
      line.startsWith("Caused by:") ||
      line.startsWith("ReferenceError:") ||
      line.startsWith("Error:") ||
      line.includes("Unhandled Error") ||
      line.includes("Failed to import test file") ||
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

function stripAnsi(value) {
  return String(value ?? "").replace(/\x1B\[[0-9;]*m/g, "");
}

async function runNativeBenchmark(benchmark, args) {
  if (benchmark.kind === "native-example") {
    const outputFile = path.resolve(args.outDir, benchmark.output_path);
    const profilePath =
      benchmark.profile_path ?? `benchmarks/realistic/profiles/${args.profile}.json`;
    const env = { ...process.env, ...(benchmark.env ?? {}) };
    const baseCommand = [
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
      profilePath,
      "--scenario",
      benchmark.scenario_path,
    ];
    const repeatCount = repeatCountForBenchmark(benchmark, args.repeatCount);
    const attempts = [];
    const scenarios = [];
    let totalDurationMs = 0;

    fs.rmSync(outputFile, { force: true });

    for (let attemptIndex = 1; attemptIndex <= repeatCount; attemptIndex += 1) {
      const attemptAppId = `realistic-ci-${fileSafeId(benchmark.id)}-attempt-${attemptIndex}`;
      const attemptOutputFile = path.resolve(
        args.outDir,
        withAttemptSuffix(benchmark.output_path, attemptIndex),
      );
      const tempOutputFile = `${attemptOutputFile}.partial`;
      const logFile = path.resolve(
        args.outDir,
        withAttemptSuffix(benchmark.log_path, attemptIndex),
      );
      let prepareDurationMs = null;
      let prepareLogPath = null;
      let prepareCommand = null;
      const command = [...baseCommand, "--app-id", attemptAppId];

      fs.rmSync(tempOutputFile, { force: true });
      fs.rmSync(attemptOutputFile, { force: true });

      if (benchmark.prepare_seed) {
        const fixtureDir = path.resolve(
          args.outDir,
          "fixtures",
          fileSafeId(benchmark.id),
          `attempt_${attemptIndex}`,
        );
        const seedStateFile = path.join(fixtureDir, "seed_state.json");
        const prepareLogFile = `${logFile}.prepare`;
        prepareLogPath = rel(prepareLogFile);
        fs.rmSync(fixtureDir, { recursive: true, force: true });
        mkdirp(fixtureDir);

        prepareCommand = [
          ...command,
          "--data-dir",
          fixtureDir,
          "--seed-state",
          seedStateFile,
          "--prepare-only",
        ];
        console.log(`\n==> ${benchmark.label} (prepare ${attemptIndex}/${repeatCount})`);
        console.log(shellQuote(prepareCommand));
        const prepareResult = await runCommand({
          command: prepareCommand,
          cwd: process.cwd(),
          env,
          timeoutSeconds: args.timeoutSeconds,
          logFile: prepareLogFile,
        });
        prepareDurationMs = prepareResult.durationMs;
        totalDurationMs += prepareResult.durationMs;
        const prepareStatus = statusForRun(prepareResult);
        if (prepareStatus !== "passed") {
          attempts.push({
            attempt: attemptIndex,
            status: prepareStatus,
            duration_ms: prepareResult.durationMs,
            prepare_duration_ms: prepareDurationMs,
            prepare_log_path: prepareLogPath,
            exit_code: prepareResult.code,
            signal: prepareResult.signal,
            output_path: null,
            log_path: rel(logFile),
            note: failureNote(prepareResult) ?? "Seed preparation failed.",
          });
          return summarizeBenchmark(benchmark, prepareStatus, totalDurationMs, {
            command: baseCommand,
            scenario_path: benchmark.scenario_path,
            profile_path: profilePath,
            output_path: null,
            log_path: rel(logFile),
            timeout_seconds: args.timeoutSeconds,
            repeat_count: repeatCount,
            completed_attempts: scenarios.length,
            attempts,
            note: failureNote(prepareResult) ?? "Seed preparation failed.",
          });
        }

        command.push("--data-dir", fixtureDir, "--seed-state", seedStateFile, "--reuse-seed");
      }

      console.log(`\n==> ${benchmark.label} (${attemptIndex}/${repeatCount})`);
      console.log(shellQuote(command));
      const result = await runCommand({
        command,
        cwd: process.cwd(),
        env,
        timeoutSeconds: args.timeoutSeconds,
        stdoutFile: tempOutputFile,
        logFile,
      });
      totalDurationMs += result.durationMs;

      const status = statusForRun(result);
      const tempExists = fs.existsSync(tempOutputFile) && fs.statSync(tempOutputFile).size > 0;
      if (status === "passed" && tempExists) {
        fs.renameSync(tempOutputFile, attemptOutputFile);
      } else {
        fs.rmSync(tempOutputFile, { force: true });
      }

      const scenario = readJsonIfExists(attemptOutputFile);
      const finalStatus =
        status === "passed" && scenario && typeof scenario === "object"
          ? "passed"
          : status === "passed"
            ? "failed"
            : status;
      attempts.push({
        attempt: attemptIndex,
        status: finalStatus,
        duration_ms: result.durationMs,
        prepare_duration_ms: prepareDurationMs,
        prepare_log_path: prepareLogPath,
        prepare_command: prepareCommand,
        exit_code: result.code,
        signal: result.signal,
        output_path: finalStatus === "passed" ? rel(attemptOutputFile) : null,
        log_path: rel(logFile),
        note:
          finalStatus === "failed" && status === "passed" && !scenario
            ? "Benchmark completed without emitting JSON output."
            : failureNote(result),
      });

      if (finalStatus !== "passed") {
        return summarizeBenchmark(benchmark, finalStatus, totalDurationMs, {
          command: baseCommand,
          scenario_path: benchmark.scenario_path,
          profile_path: profilePath,
          output_path: null,
          log_path: rel(logFile),
          timeout_seconds: args.timeoutSeconds,
          repeat_count: repeatCount,
          completed_attempts: scenarios.length,
          attempts,
          note: attempts.at(-1)?.note ?? failureNote(result),
        });
      }

      scenarios.push(scenario);
    }

    const aggregatedScenario = aggregateScenarioAttempts(scenarios[0], scenarios, repeatCount);
    writeJson(outputFile, aggregatedScenario);
    return summarizeBenchmark(benchmark, "passed", totalDurationMs, {
      command: baseCommand,
      scenario_path: benchmark.scenario_path,
      profile_path: profilePath,
      output_path: rel(outputFile),
      timeout_seconds: args.timeoutSeconds,
      repeat_count: repeatCount,
      completed_attempts: scenarios.length,
      attempts,
      note: scenarioNoiseSummary(aggregatedScenario),
      scenario: aggregatedScenario,
    });
  }

  const logFile = path.resolve(args.outDir, benchmark.log_path);

  const command = [
    "cargo",
    "bench",
    "-p",
    "jazz-tools",
    "--features",
    "fjall",
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
    env: { ...process.env, ...(benchmark.env ?? {}) },
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
  const marker = "[realistic-bench]";
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = stripAnsi(lines[i]).trim();
    if (!line) continue;
    const markerIndex = line.indexOf(marker);
    if (markerIndex === -1) continue;
    const payload = line.slice(markerIndex + marker.length).trim();
    return JSON.parse(payload);
  }
  return null;
}

async function runBrowserBenchmark(benchmark, args) {
  const outputFile = path.resolve(args.outDir, benchmark.output_path);
  const command = ["pnpm", "--dir", "packages/jazz-tools", "run", "bench:realistic:browser"];
  const repeatCount = repeatCountForBenchmark(benchmark, args.repeatCount);
  const attempts = [];
  const scenarios = [];
  let totalDurationMs = 0;

  fs.rmSync(outputFile, { force: true });

  for (let attemptIndex = 1; attemptIndex <= repeatCount; attemptIndex += 1) {
    const logFile = path.resolve(args.outDir, withAttemptSuffix(benchmark.log_path, attemptIndex));
    const attemptOutputFile = path.resolve(
      args.outDir,
      withAttemptSuffix(benchmark.output_path, attemptIndex),
    );
    const env = {
      ...process.env,
      ...(benchmark.env ?? {}),
      JAZZ_REALISTIC_BROWSER_SCENARIOS: benchmark.scenario_id,
      JAZZ_REALISTIC_BROWSER_RUN_ID: `${fileSafeId(benchmark.id)}-attempt-${attemptIndex}`,
    };

    fs.rmSync(attemptOutputFile, { force: true });
    console.log(`\n==> ${benchmark.label} (${attemptIndex}/${repeatCount})`);
    console.log(`JAZZ_REALISTIC_BROWSER_SCENARIOS=${benchmark.scenario_id} ${shellQuote(command)}`);
    const result = await runCommand({
      command,
      cwd: process.cwd(),
      env,
      timeoutSeconds: args.timeoutSeconds,
      logFile,
      streamStdoutToConsole: true,
    });
    totalDurationMs += result.durationMs;

    const status = statusForRun(result);
    let scenario = null;
    let note = null;
    if (status === "passed") {
      try {
        const report = parseBrowserReport(result.stdoutLines);
        const reportedScenarios = Array.isArray(report?.scenarios) ? report.scenarios : [];
        scenario =
          reportedScenarios.find((entry) => entry?.scenario_id === benchmark.scenario_id) ?? null;
        if (!scenario) {
          note = `Browser report did not include scenario ${benchmark.scenario_id}.`;
        } else {
          writeJson(attemptOutputFile, scenario);
        }
      } catch (error) {
        note = error instanceof Error ? error.message : String(error);
      }
    }

    const finalStatus = status === "passed" && !scenario ? "failed" : status;
    attempts.push({
      attempt: attemptIndex,
      status: finalStatus,
      duration_ms: result.durationMs,
      output_path: scenario ? rel(attemptOutputFile) : null,
      log_path: rel(logFile),
      exit_code: result.code,
      signal: result.signal,
      note: note ?? failureNote(result),
    });

    if (finalStatus !== "passed" || !scenario) {
      return summarizeBenchmark(benchmark, finalStatus, totalDurationMs, {
        command,
        scenario_id: benchmark.scenario_id,
        output_path: null,
        timeout_seconds: args.timeoutSeconds,
        repeat_count: repeatCount,
        completed_attempts: scenarios.length,
        attempts,
        note: attempts.at(-1)?.note ?? failureNote(result),
      });
    }

    scenarios.push(scenario);
  }

  const aggregatedScenario = aggregateScenarioAttempts(scenarios[0], scenarios, repeatCount);
  writeJson(outputFile, aggregatedScenario);
  return summarizeBenchmark(benchmark, "passed", totalDurationMs, {
    command,
    scenario_id: benchmark.scenario_id,
    output_path: rel(outputFile),
    timeout_seconds: args.timeoutSeconds,
    repeat_count: repeatCount,
    completed_attempts: scenarios.length,
    attempts,
    note: scenarioNoiseSummary(aggregatedScenario),
    scenario: aggregatedScenario,
  });
}

function countByStatus(results) {
  const counts = new Map();
  for (const result of results) {
    counts.set(result.status, (counts.get(result.status) ?? 0) + 1);
  }
  return counts;
}

function summaryMarkdown(suite, results, timeoutSeconds, repeatCount) {
  const counts = countByStatus(results);
  const lines = [];
  lines.push(`## ${suite === "native" ? "Native" : "Browser"} benchmark status`);
  lines.push("");
  lines.push(`Budget per benchmark: ${timeoutSeconds}s`);
  lines.push(`Repeated scenario benchmarks: ${repeatCount}x`);
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
    repeat_count: args.repeatCount,
    timeout_seconds: args.timeoutSeconds,
    benchmarks: results,
  };
  writeJson(statusFile, statusPayload);
  fs.writeFileSync(
    summaryFile,
    summaryMarkdown(args.suite, results, args.timeoutSeconds, args.repeatCount),
  );
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
