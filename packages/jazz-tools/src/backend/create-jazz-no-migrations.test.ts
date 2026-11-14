import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { appArtifact, appPermissions } from "../testing/app-artifact.js";
import { createJazz, memory, remote, sqlite, vercel } from "./create-jazz.js";

const definition = { notes: s.table({ body: s.string() }, {}) };
const none = s.defineApp(definition, { migrations: "none" });
const permissions = s.definePermissions(none, ({ policy }) => {
  policy.notes.allowRead.always();
});

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});
async function tempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "jazz-no-migrations-"));
  dirs.push(dir);
  return dir;
}

/** The fix the refusal names, step by step. */
function expectNamesTheFix(error: unknown, kind: string): void {
  expect(error).toBeInstanceOf(Error);
  const message = (error as Error).message;
  expect(message).toContain('defineApp(schema, { migrations: "none" })');
  expect(message).toContain(`${kind}() store`);
  expect(message).toContain("npx jazz-tools validate");
  expect(message).toContain('import artifact from "./jazz.app.json" with { type: "json" };');
  expect(message).toContain("pass `{ artifact }` to defineApp");
  expect(message).not.toContain("migrations/index.ts");
  expect(message).toContain('"none" is only for memory() stores and tests');
}

function refusal(open: () => unknown): unknown {
  try {
    open();
  } catch (error) {
    return error;
  }
  throw new Error("createJazz did not refuse the app");
}

describe('createJazz with an app on { migrations: "none" }', () => {
  it("refuses a sqlite() store when called, before the store opens", async () => {
    const dir = await tempDir();
    const error = refusal(() =>
      createJazz({ app: none, permissions, store: sqlite({ path: join(dir, "store") }) }),
    );
    expectNamesTheFix(error, "sqlite");
    // Nothing was created on disk.
    expect(await readdir(dir)).toEqual([]);
  });

  it("refuses a remote() store", () => {
    const error = refusal(() =>
      createJazz({
        app: none,
        permissions,
        store: remote({ url: "http://127.0.0.1:1", appId: "app", secret: "secret" }),
      }),
    );
    expectNamesTheFix(error, "remote");
  });

  it("refuses a vercel() store, even one whose local store is memory()", () => {
    for (const store of [
      vercel({ namespace: "test" }),
      vercel({ namespace: "test", local: memory() }),
    ]) {
      expectNamesTheFix(
        refusal(() => createJazz({ app: none, permissions, store })),
        "vercel",
      );
    }
  });

  it('refuses an app slice of a sliceable app on { migrations: "none" }', () => {
    const sliceable = s.defineSliceableApp(definition, { migrations: "none" });
    const error = refusal(() =>
      createJazz({
        app: sliceable.slice("notes"),
        permissions,
        store: remote({ url: "http://127.0.0.1:1", appId: "app", secret: "secret" }),
      }),
    );
    expectNamesTheFix(error, "remote");
  });

  it("allows a memory() store", async () => {
    const jazz = createJazz({ app: none, permissions, store: memory() });
    await jazz.close();
  });

  it("allows a sqlite() store once the app ships its jazz.app.json", async () => {
    const dir = await tempDir();
    const shipped = s.defineApp(definition, {
      artifact: await appArtifact(definition),
    });
    const jazz = createJazz({
      app: shipped,
      // An app that ships its jazz.app.json takes its jazz.permissions.json.
      permissions: await appPermissions(shipped, permissions),
      store: sqlite({ path: join(dir, "store") }),
    });
    await jazz.close();
  });
});
