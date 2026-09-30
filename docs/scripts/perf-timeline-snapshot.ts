// Builds the benchmark snapshot served on /examples. Run from a full-history
// checkout (tags included):
//   node --experimental-strip-types scripts/perf-timeline-snapshot.ts <out-dir> [previous-cache.json]
// Writes <out-dir>/timeline.json (what the site serves) and
// <out-dir>/codspeed-results.json (settled CodSpeed results, reused next run).
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { Release } from "../lib/perf-timeline/model.ts";
import { isVersionTag } from "../lib/perf-timeline/releases.ts";
import { buildSnapshot, type ResultCache } from "../lib/perf-timeline/snapshot.ts";

const [outDir, previousPath] = process.argv.slice(2);
if (!outDir) throw new Error("usage: perf-timeline-snapshot.ts <out-dir> [previous-cache.json]");

function git(...args: string[]): string {
  return execFileSync("git", args, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
}

// Oldest first. %(*objectname) is the peeled commit of an annotated tag.
const tags: Release[] = git(
  "for-each-ref",
  "--sort=creatordate",
  "--format=%(refname:short)%09%(objectname)%09%(*objectname)",
  "refs/tags",
)
  .split("\n")
  .filter(Boolean)
  .map((line) => line.split("\t"))
  .filter(([name]) => isVersionTag(name))
  .map(([name, object, peeled]) => ({
    name,
    sha: peeled || object,
    url: `https://github.com/garden-co/jazz/tree/${encodeURIComponent(name)}`,
  }));
if (!tags.length) throw new Error("No version tags found; fetch tags before building.");

function containingTags(sha: string): ReadonlySet<string> | null {
  if (!/^[0-9a-f]{40}$/.test(sha)) return null;
  try {
    execFileSync("git", ["cat-file", "-e", `${sha}^{commit}`], { stdio: "ignore" });
  } catch {
    return null;
  }
  return new Set(git("tag", "--contains", sha).split("\n").filter(Boolean));
}

const previous: ResultCache | null =
  previousPath && existsSync(previousPath)
    ? (JSON.parse(readFileSync(previousPath, "utf8")) as ResultCache)
    : null;

const { timeline, cache } = await buildSnapshot({ tags, containingTags, previous });
if (!timeline.benchmarks.length) throw new Error("Snapshot has no benchmarks; not publishing it.");

mkdirSync(outDir, { recursive: true });
writeFileSync(join(outDir, "timeline.json"), JSON.stringify(timeline));
writeFileSync(join(outDir, "codspeed-results.json"), JSON.stringify(cache));
const reused = previous
  ? Object.keys(cache.runs).filter((id) => previous.runs[id] === cache.runs[id]).length
  : 0;
console.log(
  `${timeline.benchmarks.length} benchmarks from ${timeline.runCount} runs; ` +
    `${Object.keys(cache.runs).length} runs with results (${reused} reused); ` +
    `${tags.length} releases; warnings: ${JSON.stringify(timeline.warnings)}`,
);
