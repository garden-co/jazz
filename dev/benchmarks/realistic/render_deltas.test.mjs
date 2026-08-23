import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync, spawnSync } from "node:child_process";

const REPO_ROOT = path.resolve(new URL("../../..", import.meta.url).pathname);

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function writeText(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, value);
}

function writeJazzSimRun(root, sha, elapsedUs, replayRate, failedElapsedUs) {
  const dir = path.join(root, "native", "jazz-sim");
  writeJson(path.join(dir, "manifest.json"), {
    kind: "realistic-bench-jazz-sim",
    generated_at: "2026-04-09T10:00:00Z",
    sha,
    files: [
      { path: "suite_status.json" },
      { path: "s2_canvas.jsonl", sha256: "abc" },
      { path: "wire_frames/s2_canvas.jsonl", sha256: "def" },
    ],
  });
  writeJson(path.join(dir, "suite_status.json"), {
    benchmarks: [
      { id: "jazz-sim:s2_canvas", status: "passed" },
      { id: "jazz-sim:s2_canvas:wire_frames", status: "failed" },
    ],
  });
  writeText(
    path.join(dir, "s2_canvas.jsonl"),
    `${JSON.stringify({
      scenario: "s2_canvas",
      phase: "canvas_replay",
      elapsed_us: elapsedUs,
      replay_edits_per_sec: replayRate,
      local_echo_p95_us: 120,
      edits: 100,
      seed: 1,
    })}\n`,
  );
  writeText(
    path.join(dir, "wire_frames/s2_canvas.jsonl"),
    `${JSON.stringify({
      scenario: "s2_canvas",
      phase: "canvas_replay",
      elapsed_us: failedElapsedUs,
    })}\n`,
  );
}

test("render_deltas compares jazz-sim JSONL metrics from manifests", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-bench-deltas-"));
  const baseRoot = path.join(tempRoot, "base");
  const headRoot = path.join(tempRoot, "head");
  writeJazzSimRun(baseRoot, "base-sha", 2500, 40000, 9999);
  writeJazzSimRun(headRoot, "head-sha", 2000, 50000, 8888);

  const output = execFileSync(
    "node",
    [
      "dev/benchmarks/realistic/render_deltas.mjs",
      "--base",
      baseRoot,
      "--head",
      headRoot,
      "--kind",
      "jazz-sim",
    ],
    {
      cwd: REPO_ROOT,
      encoding: "utf8",
    },
  );

  assert.match(output, /## Jazz Sim/);
  assert.match(
    output,
    /\| jazz-sim\/s2_canvas\/canvas_replay\/elapsed_us \| 2500\.0 \| 2000\.0 \| -500\.00 \| -20\.00% \| better \|/,
  );
  assert.match(
    output,
    /\| jazz-sim\/s2_canvas\/canvas_replay\/replay_edits_per_sec \| 40000\.0 \| 50000\.0 \| 10000\.0 \| 25\.00% \| better \|/,
  );
  assert.doesNotMatch(output, /8888|9999|\/edits|\/seed/);
});

test("render_deltas help and validation include jazz-sim kind", () => {
  const help = execFileSync("node", ["dev/benchmarks/realistic/render_deltas.mjs", "--help"], {
    cwd: REPO_ROOT,
    encoding: "utf8",
  });
  assert.match(help, /all\|native\|browser\|jazz-sim/);

  const invalid = spawnSync(
    "node",
    ["dev/benchmarks/realistic/render_deltas.mjs", "--base", ".", "--head", ".", "--kind", "nope"],
    {
      cwd: REPO_ROOT,
      encoding: "utf8",
    },
  );
  assert.notEqual(invalid.status, 0);
  assert.match(invalid.stderr, /all, native, browser, jazz-sim/);
});
