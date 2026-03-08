#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {
    history: "benchmarks/realistic/history/bench_history.json",
    baseBranch: "main",
    headBranch: "",
    suite: "all",
    profile: "all",
    limit: 12,
    out: "",
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--history") {
      out.history = argv[++i] ?? "";
      continue;
    }
    if (arg === "--base-branch") {
      out.baseBranch = argv[++i] ?? "";
      continue;
    }
    if (arg === "--head-branch") {
      out.headBranch = argv[++i] ?? "";
      continue;
    }
    if (arg === "--suite") {
      out.suite = argv[++i] ?? "";
      continue;
    }
    if (arg === "--profile") {
      out.profile = argv[++i] ?? "";
      continue;
    }
    if (arg === "--limit") {
      out.limit = Number(argv[++i] ?? "12");
      continue;
    }
    if (arg === "--out") {
      out.out = argv[++i] ?? "";
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  if (!out.headBranch) fail("--head-branch is required");
  if (!Number.isFinite(out.limit) || out.limit < 1) fail("--limit must be >= 1");
  return out;
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/render_history_report.mjs \\
    --history benchmarks/realistic/history/bench_history.json \\
    --base-branch main \\
    --head-branch my-branch \\
    [--suite all|native|browser|native-criterion] \\
    [--profile all|s|m] \\
    [--limit 12] \\
    [--out report.md]
`);
}

function byGenerated(a, b) {
  return Date.parse(b.generated_at ?? "") - Date.parse(a.generated_at ?? "");
}

function scenarioMap(run) {
  const map = new Map();
  for (const scenario of run?.scenarios ?? []) {
    if (scenario && typeof scenario.scenario_id === "string") {
      map.set(scenario.scenario_id, scenario);
    }
  }
  return map;
}

function metricKeysForScenario(scenario) {
  const keys = [];
  if (Number.isFinite(Number(scenario?.wall_time_ms))) keys.push("wall_time_ms");
  if (Number.isFinite(Number(scenario?.throughput_ops_per_sec))) keys.push("throughput_ops_per_sec");

  const ops = scenario?.operation_summaries ?? {};
  for (const opName of Object.keys(ops).sort()) {
    const summary = ops[opName];
    if (!summary || typeof summary !== "object") continue;
    if (Number.isFinite(Number(summary.avg_ms))) keys.push(`op:${opName}/avg_ms`);
    if (Number.isFinite(Number(summary.p95_ms))) keys.push(`op:${opName}/p95_ms`);
  }

  return keys;
}

function metricValue(scenario, metric) {
  if (metric === "wall_time_ms") return Number(scenario?.wall_time_ms);
  if (metric === "throughput_ops_per_sec") return Number(scenario?.throughput_ops_per_sec);
  if (!metric.startsWith("op:")) return Number.NaN;

  const slash = metric.lastIndexOf("/");
  if (slash <= 3) return Number.NaN;
  const opName = metric.slice(3, slash);
  const field = metric.slice(slash + 1);
  return Number(scenario?.operation_summaries?.[opName]?.[field]);
}

function toPct(delta, base) {
  if (!Number.isFinite(delta) || !Number.isFinite(base) || base === 0) return Number.NaN;
  return (delta / base) * 100;
}

function trend(metric, delta) {
  if (!Number.isFinite(delta) || delta === 0) return "flat";
  if (metric.includes("throughput_ops_per_sec")) return delta > 0 ? "better" : "worse";
  if (metric.includes("_ms")) return delta < 0 ? "better" : "worse";
  return delta > 0 ? "up" : "down";
}

function fmt(value) {
  if (!Number.isFinite(value)) return "n/a";
  const abs = Math.abs(value);
  if (abs >= 1000) return value.toFixed(1);
  if (abs >= 10) return value.toFixed(2);
  return value.toFixed(3);
}

function latestRun(runs, branch, suite, profile) {
  return (
    runs.find((run) => run.branch === branch && run.suite === suite && run.profile === profile) ?? null
  );
}

function comparableRows(baseRun, headRun) {
  const rows = [];
  if (!baseRun || !headRun) return rows;

  const baseScenarios = scenarioMap(baseRun);
  const headScenarios = scenarioMap(headRun);
  const scenarioIds = [...baseScenarios.keys()].filter((id) => headScenarios.has(id)).sort();

  for (const scenarioId of scenarioIds) {
    const baseScenario = baseScenarios.get(scenarioId);
    const headScenario = headScenarios.get(scenarioId);
    const keys = metricKeysForScenario(baseScenario).filter((key) =>
      metricKeysForScenario(headScenario).includes(key),
    );

    for (const metric of keys.sort()) {
      const baseValue = metricValue(baseScenario, metric);
      const headValue = metricValue(headScenario, metric);
      if (!Number.isFinite(baseValue) || !Number.isFinite(headValue)) continue;
      const delta = headValue - baseValue;
      rows.push({
        scenarioId,
        metric,
        baseValue,
        headValue,
        delta,
        deltaPct: toPct(delta, baseValue),
        trend: trend(metric, delta),
      });
    }
  }

  rows.sort((a, b) => Math.abs(b.deltaPct || 0) - Math.abs(a.deltaPct || 0));
  return rows;
}

function unique(values) {
  return [...new Set(values)];
}

function render(history, args) {
  const runs = Array.isArray(history?.runs) ? [...history.runs].sort(byGenerated) : [];
  const suiteFilter = args.suite === "all" ? null : args.suite;
  const profileFilter = args.profile === "all" ? null : args.profile;

  const candidateRuns = runs.filter(
    (run) =>
      [args.baseBranch, args.headBranch].includes(run.branch) &&
      (!suiteFilter || run.suite === suiteFilter) &&
      (!profileFilter || run.profile === profileFilter),
  );

  const suites = unique(candidateRuns.map((run) => run.suite).filter(Boolean)).sort();
  const profiles = unique(candidateRuns.map((run) => run.profile).filter(Boolean)).sort();

  const sections = [];
  sections.push(`# Benchmark Report`);
  sections.push(``);
  sections.push(`Base branch: \`${args.baseBranch}\``);
  sections.push(`Head branch: \`${args.headBranch}\``);
  sections.push(`Generated: ${new Date().toISOString()}`);
  sections.push(``);

  let renderedComparisons = 0;

  for (const suite of suites) {
    const suiteProfiles = profileFilter ? [profileFilter] : profiles.filter(Boolean);
    for (const profile of suiteProfiles) {
      const baseRun = latestRun(runs, args.baseBranch, suite, profile);
      const headRun = latestRun(runs, args.headBranch, suite, profile);
      if (!baseRun || !headRun) continue;

      const rows = comparableRows(baseRun, headRun);
      sections.push(`## ${suite} / profile ${profile}`);
      sections.push(``);
      sections.push(
        `Base ${String(baseRun.sha ?? "").slice(0, 12)} (${baseRun.generated_at ?? "n/a"}) vs Head ${String(
          headRun.sha ?? "",
        ).slice(0, 12)} (${headRun.generated_at ?? "n/a"})`,
      );
      sections.push(``);

      if (rows.length === 0) {
        sections.push(`No overlapping metrics.`);
        sections.push(``);
        continue;
      }

      sections.push(`| Scenario | Metric | Base | Head | Delta | Delta % | Trend |`);
      sections.push(`|---|---|---:|---:|---:|---:|---|`);
      for (const row of rows.slice(0, args.limit)) {
        sections.push(
          `| ${row.scenarioId} | ${row.metric} | ${fmt(row.baseValue)} | ${fmt(row.headValue)} | ${fmt(row.delta)} | ${fmt(row.deltaPct)}% | ${row.trend} |`,
        );
      }
      sections.push(``);
      renderedComparisons += 1;
    }
  }

  if (renderedComparisons === 0) {
    sections.push(`No comparable benchmark runs found in history for these filters.`);
    sections.push(``);
  }

  return `${sections.join("\n")}\n`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const historyPath = path.resolve(args.history);
  if (!fs.existsSync(historyPath)) fail(`History file not found: ${historyPath}`);

  const history = JSON.parse(fs.readFileSync(historyPath, "utf8"));
  const report = render(history, args);

  if (args.out) {
    const outPath = path.resolve(args.out);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, report);
  } else {
    process.stdout.write(report);
  }
}

main();
