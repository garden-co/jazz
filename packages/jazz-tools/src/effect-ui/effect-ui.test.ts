import { Cause, Chunk, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { describe, expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import type { JazzClient } from "../web/registered-jazz-client.js";
import type { JazzSession as CoreSession, JazzSessionSnapshot } from "../session/jazz-session.js";
import { Jazz, JazzError, JazzWriteRejected } from "../effect/index.js";
import { JazzNotSignedIn, JazzSession, JazzSessionState, liveQuery, sessionState } from "./index.js";

const app = testApp({
  tables: { todos: { columns: { title: "string", done: "boolean" } } },
});

type Msg =
  | { readonly _tag: "Rows"; readonly titles: ReadonlyArray<string> }
  | { readonly _tag: "Failed"; readonly error: JazzError };

/** A Jazz layer over a fresh in-memory Db that counts its open subscriptions. */
function trackedJazz(name: string) {
  let open = 0;
  const layer = Layer.effect(
    Jazz,
    Effect.acquireRelease(
      Effect.promise(async () =>
        createDb({ ...(await localAccountConfig(`effect-ui-${name}`)), driver: { type: "memory" } }),
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

const todosBy = (done: boolean) =>
  liveQuery("todos", done, {
    query: (done) => app.todos.where({ done }),
    onRows: (rows): Msg => ({ _tag: "Rows", titles: rows.map((row) => row.title).sort() }),
    onError: (error): Msg => ({ _tag: "Failed", error }),
  });

describe("liveQuery", { timeout: 30_000 }, () => {
  it("is keyed on its name and deps", () => {
    expect(todosBy(true).key).toBe(todosBy(true).key);
    expect(todosBy(true).key).not.toBe(todosBy(false).key);
    expect(todosBy(true).name).toBe("todos");
    expect(todosBy(true).deps).toEqual([true]);
  });

  it("delivers the current result, then each change, as messages", async () => {
    const { layer } = trackedJazz("rows");
    const messages = await Effect.runPromise(
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        yield* jazz.insert(app.todos, { title: "milk", done: false });
        const fiber = yield* todosBy(false).stream.pipe(
          Stream.filter((msg) => msg._tag !== "Rows" || msg.titles.length !== 1),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.sleep("20 millis");
        yield* jazz.insert(app.todos, { title: "eggs", done: false });
        yield* jazz.insert(app.todos, { title: "done already", done: true });
        return Chunk.toReadonlyArray(yield* Fiber.join(fiber));
      }).pipe(Effect.provide(layer)),
    );
    expect(messages).toEqual([{ _tag: "Rows", titles: ["eggs", "milk"] }]);
  });

  it("removes its Jazz subscription when its scope closes", async () => {
    const { layer, openSubscriptions } = trackedJazz("scope");
    const during = await Effect.runPromise(
      Effect.gen(function* () {
        yield* todosBy(true).stream.pipe(Stream.take(1), Stream.runDrain);
        // The stream ended after its first message: nothing stays open.
        const afterTake = openSubscriptions();
        const fiber = yield* todosBy(true).stream.pipe(Stream.runDrain, Effect.forkChild);
        yield* Effect.sleep("20 millis");
        const running = openSubscriptions();
        yield* Fiber.interrupt(fiber);
        return { afterTake, running, afterInterrupt: openSubscriptions() };
      }).pipe(Effect.provide(layer)),
    );
    expect(during).toEqual({ afterTake: 0, running: 1, afterInterrupt: 0 });
  });

  it("turns a failed subscription into its error message", async () => {
    const { layer } = trackedJazz("error");
    const broken = liveQuery("broken", null, {
      // A table the schema doesn't have: the core refuses the subscription.
      query: () => ({ ...app.todos, _table: "missing" }) as typeof app.todos,
      onRows: (): Msg => ({ _tag: "Rows", titles: [] }),
      onError: (error): Msg => ({ _tag: "Failed", error }),
    });
    const messages = await Effect.runPromise(
      broken.stream.pipe(Stream.runCollect, Effect.provide(layer)),
    );
    const [message] = Chunk.toReadonlyArray(messages);
    expect(Chunk.size(messages)).toBe(1);
    expect(message?._tag).toBe("Failed");
    expect(message?._tag === "Failed" && message.error).toBeInstanceOf(JazzError);
  });
});

/** A core session whose snapshot the test sets. */
function fakeSession(initial: JazzSessionSnapshot<JazzClient>) {
  let snapshot = initial;
  const listeners = new Set<() => void>();
  const calls: string[] = [];
  const op = (name: string) => async () => {
    calls.push(name);
  };
  const session = {
    getSnapshot: () => snapshot,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    refresh: op("refresh"),
    createDeviceAccount: op("createDeviceAccount"),
    signOut: op("signOut"),
    retry: op("retry"),
  } as unknown as CoreSession<JazzClient>;
  return {
    session,
    calls,
    listeners,
    set(next: JazzSessionSnapshot<JazzClient>) {
      snapshot = next;
      for (const listener of listeners) listener();
    },
  };
}

const user = {
  accountId: "account-1",
  kind: "backend" as const,
  identity: { issuer: "test", subject: "user-1" },
  claims: {},
};

describe("JazzSession", () => {
  it("streams its state as Schema values, skipping repeats", async () => {
    const fake = fakeSession({ status: "starting", user: null, localFirst: false });
    const states = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* sessionState((state) => state).stream.pipe(
          Stream.take(3),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.sleep("1 millis");
        fake.set({ status: "starting", user: null, localFirst: false });
        fake.set({ status: "signed-out", user: null, localFirst: false });
        fake.set({
          status: "ready",
          user,
          localFirst: false,
          client: { db: {} } as JazzClient,
          error: new Error("offline"),
        });
        return Chunk.toReadonlyArray(yield* Fiber.join(fiber));
      }).pipe(Effect.provide(JazzSession.layerSession(fake.session))),
    );
    expect(states).toEqual([
      { _tag: "Starting" },
      { _tag: "SignedOut" },
      { _tag: "Ready", accountId: "account-1", kind: "backend", error: "offline" },
    ]);
    for (const state of states) expect(Schema.is(JazzSessionState)(state)).toBe(true);
    expect(fake.listeners.size).toBe(0);
  });

  it("provides Jazz only while someone is signed in", async () => {
    const fake = fakeSession({ status: "signed-out", user: null, localFirst: false });
    const build = Effect.gen(function* () {
      return yield* Jazz;
    }).pipe(
      Effect.provide(JazzSession.jazz.pipe(Layer.provide(JazzSession.layerSession(fake.session)))),
    );

    const signedOut = await Effect.runPromise(Effect.exit(build));
    expect(Exit.isFailure(signedOut) && Cause.squash(signedOut.cause)).toEqual(
      new JazzNotSignedIn({ status: "signed-out" }),
    );

    const db = { marker: true };
    fake.set({ status: "ready", user, localFirst: false, client: { db } as unknown as JazzClient });
    const jazz = await Effect.runPromise(build);
    expect(jazz.db).toBe(db);
  });

  it("runs the session's operations as Effects", async () => {
    const fake = fakeSession({ status: "signed-out", user: null, localFirst: false });
    await Effect.runPromise(
      Effect.gen(function* () {
        const session = yield* JazzSession;
        yield* session.refresh;
        yield* session.signOut;
      }).pipe(Effect.provide(JazzSession.layerSession(fake.session))),
    );
    expect(fake.calls).toEqual(["refresh", "signOut"]);
  });
});

describe("errors as Schemas", () => {
  it("encode to JSON and decode back, so they can travel in messages", () => {
    const Errors = Schema.Union([JazzError, JazzWriteRejected, JazzNotSignedIn]);
    const codec = Schema.toCodecJson(Errors);
    const rejected = new JazzWriteRejected({ transactionId: "tx", code: "denied", reason: "nope" });
    const failed = new JazzError({ operation: "insert", cause: new Error("boom") });

    const roundTrip = (error: typeof Errors.Type) =>
      Schema.decodeUnknownSync(codec)(
        JSON.parse(JSON.stringify(Schema.encodeSync(codec)(error))),
      );

    expect(roundTrip(rejected)).toEqual(rejected);
    const decoded = roundTrip(failed);
    expect(decoded).toBeInstanceOf(JazzError);
    expect(decoded.message).toBe("Jazz insert failed: boom");
  });
});
