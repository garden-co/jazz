// Where each comparison target lives in the jazz repo (see ../README.md).
// A target is "<hero>/<stack>", e.g. "stage-plan/zero" or "band-chat/jazz".
//
// - The other stacks' ports are variants next to their hero's Jazz app:
//   examples/<hero>/apps/<base>-<stack>, with harness.mjs and loc.json at the app root.
// - "jazz" is the real upstream hero app; its bench hooks (harness.mjs, seed.ts,
//   loc.json) sit in the app's comparison/ folder.
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** dev/app-comparisons */
export const comparisonsDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
/** The jazz repo root. */
export const repoRoot = resolve(comparisonsDir, "..", "..");
export const resultsDir = join(comparisonsDir, "results");

export const HEROES = {
  "stage-plan": { jazz: "examples/stage-plan/apps/react-localfirst", base: "examples/stage-plan/apps/react" },
  "band-chat": { jazz: "examples/band-chat/apps/react-hono-betterauth", base: "examples/band-chat/apps/react-hono" },
  wequencer: { jazz: "examples/wequencer/apps/next-betterauth", base: "examples/wequencer/apps/next" },
  "big-label": { jazz: "examples/big-label", base: "examples/big-label/apps/next" },
};
export const STACKS = ["jazz", "zero", "convex", "instantdb", "postgres", "neon"];

/**
 * { hero, stack, appDir, hooksDir } for a target. appDir is the app (build output,
 * loc.json globs are relative to it); hooksDir holds harness.mjs and loc.json.
 */
export function resolveTarget(target) {
  const [hero, stack] = target.split("/");
  const h = HEROES[hero];
  if (!h || !STACKS.includes(stack)) throw new Error(`unknown target ${target} (expected <${Object.keys(HEROES).join("|")}>/<${STACKS.join("|")}>)`);
  const appDir = join(repoRoot, stack === "jazz" ? h.jazz : `${h.base}-${stack}`);
  const hooksDir = stack === "jazz" ? join(appDir, "comparison") : appDir;
  return { hero, stack, appDir, hooksDir };
}

/** Every target whose loc.json exists. */
export function allTargets() {
  return Object.keys(HEROES).flatMap((hero) =>
    STACKS.map((stack) => `${hero}/${stack}`).filter((t) => existsSync(join(resolveTarget(t).hooksDir, "loc.json"))),
  );
}
