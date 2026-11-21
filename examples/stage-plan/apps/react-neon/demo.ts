import type { TaskStatus } from "./schema.js";

export const STATUS_LABELS: Record<TaskStatus, string> = {
  todo: "To do",
  doing: "In progress",
  done: "Done",
  blocked: "Blocked",
};

/** The demo show every new account starts with (see src/model/seed.ts). */
export const DEMO_SHOW = {
  name: "The Late Lanterns: album launch",
  venue: "Old Pump House",
  date: "2026-11-14",
  doors: "19:30",
};
export const DEMO_TASKS: { title: string; status: TaskStatus; mine?: boolean; notes?: string }[] = [
  { title: "Load-in", status: "done", mine: true, notes: "Dock opens at 14:00. Four cases and the drum riser." },
  { title: "Confirm stage plot with the venue", status: "done" },
  { title: "Set the backline", status: "doing", mine: true },
  { title: "Line check", status: "doing", notes: "16 inputs. Vocals on channels 1 to 3." },
  { title: "Soundcheck with the band", status: "todo", mine: true },
  { title: "Print setlists and tape them down", status: "todo" },
  { title: "Walk-in playlist on the house system", status: "todo" },
  { title: "Hazer approval from the venue", status: "blocked", notes: "Waiting on the fire officer." },
];
export const DEMO_COMMENT = "Channel 7 crackles. Swapping the DI box before soundcheck.";
