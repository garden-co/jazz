import { Effect } from "effect";
import { Jazz, type JazzDb } from "jazz-tools/effect/backend";
import { describe, expect, it } from "vitest";
import { clearCompletedTodos } from "./jazz-api";

describe("clearCompletedTodos", () => {
  it("deletes every completed todo the caller can read and reports the count", async () => {
    const reads: unknown[] = [];
    const deleted: unknown[] = [];
    const jazz = {
      all: (_query: unknown, options: unknown) => {
        reads.push(options);
        return Effect.succeed([
          { id: "a", title: "Done", done: true },
          { id: "b", title: "Also done", done: true },
        ]);
      },
      delete: (_table: unknown, id: string, options: unknown) => {
        deleted.push({ id, options });
        return Effect.void;
      },
    } as unknown as JazzDb;

    const cleared = await Effect.runPromise(
      clearCompletedTodos.pipe(Effect.provideService(Jazz, jazz)),
    );

    expect(cleared).toBe(2);
    expect(reads).toEqual([{ tier: "remote" }]);
    expect(deleted).toEqual([
      { id: "a", options: { wait: "global" } },
      { id: "b", options: { wait: "global" } },
    ]);
  });
});
