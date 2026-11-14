import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";

// The format can't express every app yet (SPEC 10.10). Such an app must
// start as on .62: the writer's "not supported … yet" is a warning here.
vi.mock("jazz-napi", async (original) => ({
  ...(await original<typeof import("jazz-napi")>()),
  writeAppArtifact: () => {
    throw new Error(
      "jazz.permissions.json: tables.notes.read: a relation join is not supported in jazz.permissions.json yet",
    );
  },
}));

const { schema: s } = await import("../index.js");
const { appMigrations } = await import("../testing/client-migrations.js");
const { createJazz, memory } = await import("./create-jazz.js");

const previousCwd = process.cwd();
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  process.chdir(previousCwd);
  vi.restoreAllMocks();
  while (cleanups.length > 0) await cleanups.pop()!();
});

it("warns and starts when the app uses something the files can't express yet", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-app-files-unsupported-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  process.chdir(dir);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const definition = { notes: s.table({ body: s.string() }, {}) };
  const app = s.defineApp(definition, { migrations: await appMigrations(definition) });
  const permissions = s.definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
  });
  const jazz = createJazz({ app, permissions, store: memory() });
  cleanups.push(() => jazz.close());
  await expect(jazz.ready()).resolves.toBeTruthy();
  expect(warn).toHaveBeenCalledWith(
    expect.stringMatching(
      /jazz\.app\.json and jazz\.permissions\.json were not written: .*not supported in jazz\.permissions\.json yet/,
    ),
  );
});
