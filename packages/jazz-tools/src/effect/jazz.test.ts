import { Cause, Data, Effect, Exit, Fiber, Layer, Option, Stream } from "effect";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { Jazz, JazzError } from "./index.js";

const app = s.defineApp({
  todos: s.table({ title: s.string(), done: s.boolean() }, {}),
});

class Oops extends Data.TaggedError("Oops")<{}> {}

async function jazzLayer(name: string): Promise<Layer.Layer<Jazz, JazzError>> {
  return Jazz.layer({
    ...(await localAccountConfig(`effect-${name}`)),
    driver: { type: "memory" },
  });
}

function run<A, E>(
  layer: Layer.Layer<Jazz, JazzError>,
  program: Effect.Effect<A, E, Jazz>,
): Promise<A> {
  return Effect.runPromise(program.pipe(Effect.provide(layer)));
}

/**
 * A layer whose Db counts its open core subscriptions, so tests can observe
 * that a stream removes its subscription.
 */
async function trackedJazzLayer(name: string) {
  let open = 0;
  const layer = Layer.effect(
    Jazz,
    Effect.acquireRelease(
      Effect.promise(async () =>
        createDb({ ...(await localAccountConfig(`effect-${name}`)), driver: { type: "memory" } }),
      ),
      (db) => Effect.promise(() => db.shutdown()),
    ).pipe(
      Effect.map((db) => {
        const subscribe = db.subscribe.bind(db);
        db.subscribe = ((...args: Parameters<typeof subscribe>) => {
          const unsubscribe = subscribe(...args);
          open++;
          let closed = false;
          return () => {
            if (!closed) open--;
            closed = true;
            unsubscribe();
          };
        }) as typeof db.subscribe;
        return Jazz.fromDb(db);
      }),
    ),
  );
  return { layer, openSubscriptions: () => open };
}

const titles = (rows: ReadonlyArray<{ title: string }>) => rows.map((row) => row.title).sort();

// The first test pays for loading the native runtime, which can exceed
// vitest's default 5 s when other suites load theirs concurrently.
describe("Jazz Effect service", { timeout: 30_000 }, () => {
  it("writes and reads rows", async () => {
    const result = await run(
      await jazzLayer("crud"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        const milk = yield* jazz.insert(app.todos, { title: "milk", done: false });
        const eggs = yield* jazz.insert(
          app.todos,
          { title: "eggs", done: false },
          { wait: "local" },
        );
        yield* jazz.update(app.todos, milk.id, { done: true });
        yield* jazz.delete(app.todos, eggs.id);
        return {
          all: yield* jazz.all(app.todos),
          done: yield* jazz.one(app.todos.where({ done: true })),
          missing: yield* jazz.one(app.todos.where({ title: "eggs" })),
          milk,
        };
      }),
    );

    expect(result.all).toEqual([{ id: result.milk.id, title: "milk", done: true }]);
    expect(result.done).toEqual(Option.some({ id: result.milk.id, title: "milk", done: true }));
    expect(result.missing).toEqual(Option.none());
  });

  it("fails with JazzError when the core rejects a write", async () => {
    const exit = await run(
      await jazzLayer("invalid-write"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        // Plain JavaScript can omit a required column.
        return yield* jazz.insert(app.todos, { title: "no done" } as never);
      }).pipe(Effect.exit),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
    expect(error).toBeInstanceOf(JazzError);
    expect(error).toMatchObject({ _tag: "JazzError", operation: "insert" });
  });

  it("describes causes that are not Errors", () => {
    const bare = Object.assign(Object.create(null), { code: "E_BARE" });
    expect(new JazzError({ operation: "insert", cause: bare }).message).toBe(
      'Jazz insert failed: {"code":"E_BARE"}',
    );
    expect(new JazzError({ operation: "all", cause: { message: "closed" } }).message).toBe(
      "Jazz all failed: closed",
    );
  });

  it("streams the complete result on subscription and after each change", async () => {
    const { layer, openSubscriptions } = await trackedJazzLayer("stream");
    const result = await run(
      layer,
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        const snapshots = yield* jazz.stream(app.todos.where({ done: false })).pipe(
          Stream.tap((rows) =>
            // Write once the initial (empty) snapshot has arrived, so the stream
            // observes the change rather than a later initial result.
            rows.length === 0
              ? jazz.insert(app.todos, { title: "milk", done: false })
              : Effect.void,
          ),
          Stream.map(titles),
          Stream.filter((rows) => rows.length > 0),
          Stream.take(1),
          Stream.runCollect,
        );
        const subscriptionsAfter = openSubscriptions();
        return { snapshots, subscriptionsAfter };
      }),
    );

    expect(result.snapshots).toEqual([["milk"]]);
    // Ending the stream removes the core subscription.
    expect(result.subscriptionsAfter).toBe(0);
  });

  it("removes the subscription when the consuming fiber is interrupted", async () => {
    const { layer, openSubscriptions } = await trackedJazzLayer("stream-interrupt");
    const counts = await run(
      layer,
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        const fiber = yield* jazz.stream(app.todos).pipe(Stream.runDrain, Effect.forkChild);
        yield* Effect.yieldNow;
        yield* Effect.sleep("20 millis");
        const during = openSubscriptions();
        yield* Fiber.interrupt(fiber);
        return { during, after: openSubscriptions() };
      }),
    );

    expect(counts).toEqual({ during: 1, after: 0 });
  });

  it("commits a transaction when its effect succeeds", async () => {
    const rows = await run(
      await jazzLayer("tx-commit"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        const returned = yield* jazz.transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.insert(app.todos, { title: "a", done: false });
            yield* tx.insert(app.todos, { title: "b", done: false });
            return "committed";
          }),
        );
        expect(returned).toBe("committed");
        return yield* jazz.all(app.todos);
      }),
    );

    expect(titles(rows)).toEqual(["a", "b"]);
  });

  it("rolls a transaction back and keeps the typed error when its effect fails", async () => {
    const result = await run(
      await jazzLayer("tx-fail"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        const failure = yield* jazz
          .transaction((tx) =>
            Effect.gen(function* () {
              yield* tx.insert(app.todos, { title: "discarded", done: false });
              return yield* new Oops();
            }),
          )
          .pipe(Effect.flip);
        return { failure, rows: yield* jazz.all(app.todos) };
      }),
    );

    expect(result.failure).toBeInstanceOf(Oops);
    expect(result.rows).toEqual([]);
  });

  it("rolls a transaction back when it is interrupted", async () => {
    const rows = await run(
      await jazzLayer("tx-interrupt"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        const fiber = yield* jazz
          .transaction((tx) =>
            Effect.gen(function* () {
              yield* tx.insert(app.todos, { title: "discarded", done: false });
              return yield* Effect.never;
            }),
          )
          .pipe(Effect.forkChild);
        yield* Effect.sleep("20 millis");
        yield* Fiber.interrupt(fiber);
        return yield* jazz.all(app.todos);
      }),
    );

    expect(rows).toEqual([]);
  });

  it("reads its own writes inside a transaction", async () => {
    const seen = await run(
      await jazzLayer("tx-read"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        return yield* jazz.transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.insert(app.todos, { title: "pending", done: false });
            return yield* tx.all(app.todos);
          }),
        );
      }),
    );

    expect(titles(seen)).toEqual(["pending"]);
  });

  it("shuts the database down when the layer's scope closes", async () => {
    const db = await run(
      await jazzLayer("shutdown"),
      Jazz.useSync((jazz) => jazz.db),
    );

    await expect(db.all(app.todos)).rejects.toThrow();
  });

  it("reports the current auth state first", async () => {
    const state = await run(
      await jazzLayer("auth-state"),
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        return yield* jazz.authState.pipe(Stream.runHead);
      }),
    );

    expect(Option.isSome(state)).toBe(true);
  });
});
