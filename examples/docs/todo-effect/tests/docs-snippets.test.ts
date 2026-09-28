import { Effect, Layer, Option } from "effect";
import { createAccountManager } from "jazz-tools";
import { Jazz } from "jazz-tools/effect";
import { describe, expect, it } from "vitest";
import { app } from "../schema.js";
import {
  canCreateTodo,
  clearNullableTodoFields,
  groupTodoWrites,
  readTodoPage,
  readTodosOneshot,
  restoreDeletedTodo,
  whereOperatorExamples,
} from "../src/docs-snippets.js";

/** A local-first account on an in-memory database, with no server. */
async function localJazzLayer(appId: string): Promise<Layer.Layer<Jazz, unknown>> {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl: "http://127.0.0.1:1",
    store: {
      read: async () => stored,
      update: async (transform) => {
        stored = transform(stored);
      },
    },
  });
  return Jazz.layer({ appId, account: accounts.createLocalFirst(), driver: { type: "memory" } });
}

const todo = (title: string, done = false) =>
  Jazz.use((jazz) => jazz.insert(app.todos, { title, done }));

describe("Effect docs snippets", { timeout: 30_000 }, () => {
  it("run their reads and writes against a local database", async () => {
    const result = await Effect.gen(function* () {
      const milk = yield* todo("buy milk");
      const done = yield* todo("done already", true);

      const open = yield* readTodosOneshot;
      const page = yield* readTodoPage(1, 1);
      const operators = yield* whereOperatorExamples;
      yield* clearNullableTodoFields(milk.id);

      const committedId = yield* groupTodoWrites(milk.id).pipe(
        // No server here, so skip the global durability wait the snippet asks for.
        Effect.timeoutOption("50 millis"),
      );
      const restored = yield* restoreDeletedTodo(done.id);
      const advice = yield* canCreateTodo("new");
      const afterMilk = yield* Jazz.use((jazz) => jazz.one(app.todos.where({ id: milk.id })));

      return { open, page, operators, committedId, restored, advice, afterMilk };
    }).pipe(Effect.provide(await localJazzLayer("effect-docs-snippets")), Effect.runPromise);

    expect(result.open.map((row) => row.title)).toEqual(["buy milk"]);
    expect(result.page).toHaveLength(1);
    expect(result.operators.matches.map((row) => row.title)).toEqual(["buy milk"]);
    expect(Option.getOrThrow(result.afterMilk).done).toBe(true);
    expect(result.restored).toMatchObject({ title: "Restored task", done: false });
    // No permissions are loaded locally, so Jazz cannot give a definite answer.
    expect(result.advice).toBe("unknown");
  });
});
