// Synthetic offline public-API receipt for #2979. Build with esbuild into
// packages/jazz-tools/target/composer-repro/ so external packages resolve there.
import { performance } from "node:perf_hooks";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NapiDb } from "jazz-napi";
import { schema as s } from "../../packages/jazz-tools/src/schema-namespace.js";
import { createJazzContext } from "../../packages/jazz-tools/src/backend/create-jazz-context.js";

const ticks: { start: number; duration: number }[] = [];
const wakes: { time: number; urgency: string }[] = [];
const originalTick = NapiDb.prototype.tick;
NapiDb.prototype.tick = function () {
  const start = performance.now();
  try {
    return originalTick.call(this);
  } finally {
    ticks.push({ start, duration: performance.now() - start });
  }
};
const originalScheduler = NapiDb.prototype.setTickScheduler;
NapiDb.prototype.setTickScheduler = function (callback) {
  return originalScheduler.call(this, (...args) => {
    const urgency = typeof args[0] === "string" ? args[0] : args[1];
    wakes.push({ time: performance.now(), urgency });
    callback(...args);
  });
};

const app = s.defineApp({
  drafts: s.table({ text: s.string() }, {}),
  messages: s.table({ channel: s.string(), text: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  for (const table of [policy.drafts, policy.messages]) {
    table.allowRead.always();
    table.allowInsert.always();
    table.allowUpdate.always();
    table.allowDelete.always();
  }
});
const path = mkdtempSync(join(tmpdir(), "jazz-node-startup-"));
const context = createJazzContext({
  appId: "00000000-0000-0000-0000-000000002979",
  app,
  permissions,
  driver: { type: "persistent", dataPath: path },
});
const db = context.forSession({
  issuer: "https://issuer.example",
  user_id: "synthetic-user",
  claims: {},
  account_id: "00000000-0000-0000-0000-000000000001",
  authMode: "external",
});
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const handles: (() => void)[] = [];
const beats: number[] = [];
const phases: Record<string, number> = {};
let heartbeat: ReturnType<typeof setInterval> | undefined;
try {
  const draft = db.insert(app.drafts, { text: "" }).value;
  for (let i = 0; i < 1000; i++)
    db.insert(app.messages, { channel: `channel-${i}`, text: "message" });
  await sleep(1000);
  phases.startup = performance.now();
  heartbeat = setInterval(() => beats.push(performance.now()), 1);
  let initial = 0;
  let observed = "";
  handles.push(
    db.subscribe(app.drafts.where({ id: draft.id }), (rows) => {
      initial++;
      observed = rows[0]?.text ?? "";
    }),
  );
  for (let i = 0; i < 500; i++)
    handles.push(
      db.subscribe(app.messages.where({ channel: `unrelated-${i}` }), () => {
        initial++;
      }),
    );
  while (initial < 501) await sleep(1);
  phases.initialCallbacks = performance.now();
  await sleep(1000);
  phases.settled = performance.now();
  const writes = [];
  const schedule = performance.now();
  for (let i = 1; i <= 30; i++) {
    const target = schedule + i * 10;
    await sleep(Math.max(0, target - performance.now()));
    const start = performance.now();
    const text = "x".repeat(i);
    db.update(app.drafts, draft.id, { text });
    writes.push({
      start,
      lateness: start - target,
      duration: performance.now() - start,
      synchronous: observed === text,
    });
  }
  phases.finished = performance.now();
  console.log(
    JSON.stringify({
      node: process.version,
      path,
      phases,
      ticks,
      wakes,
      beats,
      writes,
      finalText: observed,
    }),
  );
} finally {
  if (heartbeat) clearInterval(heartbeat);
  for (const handle of handles) handle();
  await context.shutdown();
}
