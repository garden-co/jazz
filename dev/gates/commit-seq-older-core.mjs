#!/usr/bin/env node
// The older-core round trip for the commit seq row (jazz#6367): this tree
// and an older release take turns on one file-backed store, and every
// commit of either must leave the `commit_seq.v1` row at its seq, while
// every open resumes above everything committed before it.
//
// The older core knows nothing of the row: its commits raise it only
// through the triggers in the file. Its steps run with a clock a day or two
// ahead, so it numbers far above this tree's wall clock. This tree's next
// open reads the row without scanning history, so it only resumes above
// the older core's commits if the triggers fired inside them.
//
// Each tag is checked out in a temporary worktree. The step itself
// (`crates/jazz/tests/compat/older_core_step.rs`) is copied into it and
// included into that tag's `tests/durable_file.rs`, which has the
// `config`, `item` and `row` helpers the step uses.
//
// Usage: node dev/gates/commit-seq-older-core.mjs [tag...]
// (default: JAZZ_OLDER_CORE_REFS, or v2.0.0-beta.0, what Jazz Cloud runs).
import { execFileSync, spawnSync } from "node:child_process";
import { appendFileSync, copyFileSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../..", import.meta.url));
const STEP = "crates/jazz/tests/compat/older_core_step.rs";
const DAY_MS = 86_400_000;

const refs = process.argv.slice(2).length
  ? process.argv.slice(2)
  : (process.env.JAZZ_OLDER_CORE_REFS ?? "v2.0.0-beta.0").split(",").filter(Boolean);

/** The `durable_file` test binary built in `dir`. */
function buildDriver(dir, features) {
  const out = execFileSync(
    "cargo",
    [
      "test",
      "-p",
      "jazz-core",
      "--no-default-features",
      "--features",
      features,
      "--test",
      "durable_file",
      "--no-run",
      "--message-format=json",
    ],
    { cwd: dir, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 1 << 28 },
  );
  for (const line of out.split("\n")) {
    if (!line.startsWith("{")) continue;
    const message = JSON.parse(line);
    if (message.reason === "compiler-artifact" && message.target?.name === "durable_file") {
      if (message.executable) return message.executable;
    }
  }
  throw new Error(`no durable_file test binary built in ${dir}`);
}

/** Run one step; what it saw. */
function step(binary, store, { from, count, ahead = 0 }) {
  const run = spawnSync(
    binary,
    ["--exact", "older_core_step::older_core_round_trip_step", "--nocapture", "--test-threads=1"],
    {
      encoding: "utf8",
      env: {
        ...process.env,
        JAZZ_OLDER_CORE_STORE: store,
        JAZZ_OLDER_CORE_FROM: String(from),
        JAZZ_OLDER_CORE_COUNT: String(count),
        JAZZ_OLDER_CORE_CLOCK_AHEAD_MS: String(ahead),
      },
    },
  );
  // libtest prints `test … ... ` before the step's own output, on its line.
  const found = run.stdout.match(/JAZZ_OLDER_CORE_STEP ((?:\w+=-?\d+ ?)+)/);
  if (run.status !== 0 || !found) {
    throw new Error(`step failed (${run.status}):\n${run.stdout}\n${run.stderr}`);
  }
  return Object.fromEntries(
    found[1]
      .trim()
      .split(" ")
      .map((pair) => {
        const [key, value] = pair.split("=");
        return [key, BigInt(value)];
      }),
  );
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

const current = buildDriver(root, "testing");
let failed = false;
for (const ref of refs) {
  const scratch = mkdtempSync(join(tmpdir(), "jazz-older-core-"));
  const tree = join(scratch, "tree");
  try {
    execFileSync("git", ["worktree", "add", "--detach", tree, ref], {
      cwd: root,
      stdio: "inherit",
    });
    mkdirSync(dirname(join(tree, STEP)), { recursive: true });
    copyFileSync(join(root, STEP), join(tree, STEP));
    appendFileSync(
      join(tree, "crates/jazz/tests/durable_file.rs"),
      '\n#[path = "compat/older_core_step.rs"]\nmod older_core_step;\n',
    );
    const older = buildDriver(tree, "testing");
    const store = join(scratch, "store.sqlite");
    const plan = [
      { who: "current", binary: current, from: 0, count: 5 },
      { who: ref, binary: older, from: 100, count: 5, ahead: DAY_MS },
      { who: "current", binary: current, from: 200, count: 5 },
      { who: ref, binary: older, from: 300, count: 5, ahead: 2 * DAY_MS },
      { who: "current", binary: current, from: 400, count: 1 },
    ];
    let previous;
    let triggers;
    for (const [i, planned] of plan.entries()) {
      const seen = step(planned.binary, store, planned);
      console.log(
        `${ref} step ${i + 1} (${planned.who}): opened after ${seen.open}, ` +
          `last commit ${seen.last}, row ${seen.row}, ${seen.triggers} triggers`,
      );
      check(seen.row === seen.last, `step ${i + 1}: the row is not at its last commit's seq`);
      check(seen.triggers > 0n, `step ${i + 1}: the store has no seq_v1 triggers`);
      triggers ??= seen.triggers;
      check(seen.triggers === triggers, `step ${i + 1}: the triggers changed`);
      if (previous) {
        check(seen.open >= previous.last, `step ${i + 1}: opened below the last commit`);
        check(seen.last > previous.last, `step ${i + 1}: reused or went below a seq`);
      }
      previous = seen;
    }
    console.log(`${ref}: round trip ok`);
  } catch (error) {
    failed = true;
    console.error(`${ref}: ${error.message}`);
  } finally {
    spawnSync("git", ["worktree", "remove", "--force", tree], { cwd: root, stdio: "inherit" });
    rmSync(scratch, { recursive: true, force: true });
  }
}
process.exit(failed ? 1 : 0);
