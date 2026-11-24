/**
 * A deployed backend reaches Jazz's loader (jazz-napi's `loadBackendApp`)
 * through createJazz's own static `jazz-napi` import, never through the
 * tooling fallback's variable specifier. Bundlers and file tracers can't
 * follow that one: in a Next.js app on pnpm deployed to Vercel it doesn't
 * resolve from the server chunk, and every `/jazz` route answered 503 with
 * "Cannot find package 'jazz-napi'" (adopter gate B65-1).
 */
import { afterAll, describe, expect, it, vi } from "vitest";
import { enginePermissions, replaceCodegenFallbackForTests } from "../dev/codegen-napi.js";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { createJazz } from "./create-jazz.js";
import { memory } from "./native-provider.js";

const notes: TestAppFile = { format: 1, tables: { notes: { columns: { body: "string" } } } };
const permissions = { format: 1, tables: { notes: { read: "true", insert: "true" } } };
const app = testApp(notes);

// The specifier fails to resolve, as from a Next.js server chunk on Vercel.
const unresolvable = vi.fn(async (specifier: string) => {
  throw new Error(
    `Cannot find package '${specifier}' imported from /var/task/.next/server/chunks/1.js`,
  );
});
const restore = replaceCodegenFallbackForTests(unresolvable);
afterAll(restore);

describe("createJazz where the bare jazz-napi specifier doesn't resolve", () => {
  // Runs first: nothing has handed over a static import yet.
  it("is what the tooling fallback runs into", async () => {
    await expect(enginePermissions(app, permissions)).rejects.toThrow(
      "Cannot find package 'jazz-napi'",
    );
    unresolvable.mockClear();
  });

  it("starts, loads jazz.permissions.json and serves, through its static jazz-napi import", async () => {
    const jazz = createJazz({ app, permissions, store: memory() });
    try {
      await jazz.ready();
      const { value } = jazz.db.insert(app.notes, { body: "hi" });
      expect(value.body).toBe("hi");
    } finally {
      await jazz.close();
    }
    expect(unresolvable).not.toHaveBeenCalled();
  });
});
