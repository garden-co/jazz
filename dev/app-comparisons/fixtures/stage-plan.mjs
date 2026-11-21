// StagePlan fixture: stack-agnostic records. Keys (`key`, `showKey`, ...) are
// fixture-local; each stack's seed maps them to its own ids. `createdAtMs`
// orders rows deterministically. Users: "chief" creates everything; the
// harness's guests join "Lanterns" (and others) through their invite codes.
import { rng, pick } from "./rng.mjs";

export const USERS = { chief: { name: "Seed Chief" } };
export const SHOWS = 30;
export const TASKS_PER_SHOW = 100; // 3,000 tasks in all
export const BIG_SHOW = "show-0"; // "The Late Lanterns", the one the harness opens

const STATUSES = ["todo", "doing", "done", "blocked"];
const VERBS = ["Load in", "Line check", "Patch", "Soundcheck", "Tape down", "Focus", "Rig", "Strike"];
const THINGS = ["backline", "monitors", "drum riser", "DI boxes", "lighting truss", "FOH desk", "merch table", "cable run"];

export function stagePlanFixture() {
  const r = rng(1);
  const t0 = Date.UTC(2026, 10, 1);
  let clock = 0;
  const at = () => t0 + 1000 * clock++;
  const shows = [];
  const invites = [];
  const tasks = [];
  const comments = [];
  for (let i = 0; i < SHOWS; i++) {
    const key = `show-${i}`;
    shows.push({
      key,
      name: i === 0 ? "The Late Lanterns: album launch" : `Show ${i + 1}`,
      venue: pick(r, ["The Roundhouse", "Paradiso", "Bowery Ballroom", "Lido", "Fabrique"]),
      date: `2026-12-${String((i % 28) + 1).padStart(2, "0")}`,
      doors: "19:00",
      createdAtMs: at(),
    });
    invites.push({ showKey: key, code: `invite-${i}-${Math.floor(r() * 1e9).toString(36)}` });
    for (let j = 0; j < TASKS_PER_SHOW; j++) {
      const taskKey = `${key}-task-${j}`;
      tasks.push({
        key: taskKey,
        showKey: key,
        title: `${pick(r, VERBS)} ${pick(r, THINGS)} #${j + 1}`,
        status: pick(r, STATUSES),
        rank: j,
        createdAtMs: at(),
      });
      if (j % 10 === 0) comments.push({ taskKey, body: `Check with the venue about task ${j + 1}.`, createdAtMs: at() });
    }
  }
  return { users: USERS, shows, invites, tasks, comments };
}
