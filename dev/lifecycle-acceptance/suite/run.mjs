// Runs every rule against one built consumer.
// usage: node suite/run.mjs --lang <lang> [--name <candidate>] [--out <dir>] [--only <id prefix>]
//                           [--rounds <at most>] [--jobs <n>]
// A round that fails fails the rule: no retries, no pass-on-retry.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { LANGS, WORK, die } from "../lib/common.mjs";
import { candidate } from "../build/consumer.mjs";
import { within } from "./process.mjs";
import { redact } from "./redact.mjs";
import { Broken, NotCovered, Round } from "./round.mjs";
import { RULES } from "./rules/index.mjs";
import { startStack } from "./stack.mjs";

const ROUND_MS = Number(process.env.LA_ROUND_MS ?? 120_000);

const { values: options } = parseArgs({
  options: {
    lang: { type: "string" },
    name: { type: "string", default: "candidate" },
    out: { type: "string", default: join(WORK, "results") },
    only: { type: "string", default: "" },
    rounds: { type: "string", default: "100" },
    jobs: { type: "string", default: String(Math.min(8, Math.max(2, availableParallelism()))) },
  },
});
const { lang } = options;
if (!LANGS.includes(lang)) die(`suite: --lang must be one of ${LANGS.join(", ")}`);
const [cap, jobs] = [Number(options.rounds), Number(options.jobs)];
if (!(cap >= 1) || !(jobs >= 1)) die("suite: --rounds and --jobs must be at least 1");
const rules = RULES.filter((rule) => rule.id.startsWith(options.only));
if (!rules.length) die(`suite: no rule matches '${options.only}'`);

async function oneRound(context, rule, index) {
  const round = new Round(context, index);
  const started = Date.now();
  let outcome;
  try {
    const body = rule
      .run(round)
      .then(() => ({ status: "PASS" }))
      .catch((error) => {
        if (error instanceof NotCovered) return { status: "NOT_COVERED", why: error.message };
        const tag = error instanceof Broken ? error.tag : `harness: ${error.message}`;
        return { status: "FAIL", tag: redact(tag), why: redact(error.message) };
      });
    outcome = await within(body, ROUND_MS, () => {
      const tag = `the round didn't finish within ${ROUND_MS} ms`;
      return { status: "FAIL", tag, why: tag };
    });
  } catch (error) {
    outcome = { status: "FAIL", tag: `harness: ${redact(error.message)}`, why: redact(error.message) };
  } finally {
    await round.cleanup();
  }
  return { ...outcome, notes: round.notes, times: round.times, ms: Date.now() - started };
}

async function runRule(context, rule) {
  const total = Math.min(cap, rule.rounds);
  const started = Date.now();
  const outcomes = [];
  const gap = rule.notCovered?.[lang] ?? rule.notCovered?.all;
  if (gap) {
    outcomes.push({ status: "NOT_COVERED", why: gap, notes: [], times: [], ms: 0 });
  } else {
    let next = 0;
    const worker = async () => {
      while (next < total) {
        const index = next++;
        outcomes[index] = await oneRound(context, rule, index);
        // A gap is the same in every round.
        if (outcomes[index].status === "NOT_COVERED") next = total;
      }
    };
    await Promise.all(Array.from({ length: Math.min(rule.jobs ?? jobs, total) }, worker));
  }
  const done = outcomes.filter(Boolean);
  const count = (status) => done.filter((outcome) => outcome.status === status).length;
  const [passed, failed, gaps] = [count("PASS"), count("FAIL"), count("NOT_COVERED")];

  const failures = {};
  for (const outcome of done.filter((o) => o.status === "FAIL"))
    (failures[outcome.tag] ??= { count: 0, example: outcome.why }).count++;
  const notes = {};
  for (const note of done.flatMap((outcome) => outcome.notes)) notes[note] = (notes[note] ?? 0) + 1;
  const timings = {};
  for (const [what, took] of done.flatMap((outcome) => outcome.times)) (timings[what] ??= []).push(took);
  for (const took of Object.values(timings)) took.sort((a, b) => a - b);
  const times = Object.fromEntries(
    Object.entries(timings).map(([what, took]) => [what, { median: took[Math.floor(took.length / 2)], max: took.at(-1) }]),
  );

  let status = failed ? "FAIL" : gaps ? "NOT_COVERED" : "PASS";
  let why = done.find((outcome) => outcome.status === "NOT_COVERED")?.why;
  if (rule.basis === "pending") {
    why = `the contract doesn't decide it yet; observed ${status} ${passed}/${done.length}${why ? ` (${why})` : ""}`;
    status = "NOT_COVERED";
  }
  const { id, title, basis } = rule;
  return { id, title, basis, status, rounds: done.length, passed, failed, why, failures, notes, times, ms: Date.now() - started };
}

const started = Date.now();
let stack;
try {
  const driver = JSON.parse(readFileSync(join(WORK, `driver-${lang}.json`), "utf8"));
  stack = await startStack();
  console.log(`SUITE ${options.name} ${lang}: ${rules.length} rules, up to ${cap} rounds, ${jobs} at a time`);
  const results = [];
  for (const rule of rules) {
    const result = await runRule({ lang, driver, stack }, rule);
    results.push(result);
    const count = result.status === "NOT_COVERED" ? "" : ` ${result.passed}/${result.rounds}`;
    console.log(`${result.status} ${lang} ${result.id}${count} (${(result.ms / 1000).toFixed(1)}s)${result.why ? ` - ${result.why}` : ""}`);
    for (const [tag, { count: n, example }] of Object.entries(result.failures))
      console.log(`  ${n}x ${tag}${example === tag ? "" : `\n      e.g. ${example.slice(0, 700)}`}`);
    for (const [what, { median, max }] of Object.entries(result.times))
      console.log(`  ${what}: median ${median} ms, max ${max} ms`);
    for (const [note, n] of Object.entries(result.notes).slice(0, 8)) console.log(`  note ${n}x ${note}`);
  }
  const ms = Date.now() - started;
  mkdirSync(options.out, { recursive: true });
  writeFileSync(
    join(options.out, `${options.name}-${lang}.json`),
    JSON.stringify({ name: options.name, lang, ...candidate(), resolved: driver.resolved, ms, results }, null, 2),
  );
  console.log(`TIME suite ${lang} ${(ms / 1000).toFixed(1)}s`);
  process.exitCode = results.some((result) => result.status === "FAIL") ? 1 : 0;
} catch (error) {
  console.log(`SETUP_FAIL suite ${lang}: ${redact(error.message)}`);
  process.exitCode = 1;
} finally {
  stack?.stop();
}
