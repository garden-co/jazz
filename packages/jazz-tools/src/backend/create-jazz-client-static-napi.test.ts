/**
 * A backend client (`createJazzClient`) checks its jazz.permissions.json with
 * Jazz's loader through its own static `jazz-napi` import, never through the
 * tooling fallback's variable specifier, which doesn't resolve from a
 * Next.js server chunk on Vercel with pnpm (adopter gate B65-1). Its own
 * file: the handed-over import is process-wide, and nothing else here may
 * hand it over first (create-jazz-static-napi.test.ts covers createJazz).
 * Tracked in jazz#5668.
 */
import { afterAll, describe, expect, it, vi } from "vitest";
import { enginePermissions, replaceCodegenFallbackForTests } from "../dev/codegen-napi.js";
import { testApp, type TestAppFile } from "../testing/test-app.js";
import { createJazzClient } from "./create-jazz-session.js";

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

describe("createJazzClient where the bare jazz-napi specifier doesn't resolve", () => {
  // Runs first: nothing has handed over a static import yet.
  it("is what the tooling fallback runs into", async () => {
    await expect(enginePermissions(app, permissions)).rejects.toThrow(
      "Cannot find package 'jazz-napi'",
    );
    unresolvable.mockClear();
  });

  it("loads jazz.permissions.json and opens, through its static jazz-napi import", async () => {
    // No server: a device account opens locally and syncs once it can.
    const session = await createJazzClient({
      appId: "static-napi-client",
      app,
      permissions,
      serverUrl: "http://127.0.0.1:9/jazz",
      driver: { type: "memory" },
      initial: "device",
    });
    try {
      const { value } = session.getSnapshot().client!.db.insert(app.notes, { body: "hi" });
      expect(value.body).toBe("hi");
    } finally {
      await session.close({ timeoutMs: 0 });
    }
    expect(unresolvable).not.toHaveBeenCalled();
  });
});
