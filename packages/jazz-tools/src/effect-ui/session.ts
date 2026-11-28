import { Subscription } from "@effect/ui";
import { Context, Effect, Layer, Queue, Schema, Stream } from "effect";
import { createJazzClient, type JazzClientConfig } from "../session/create-jazz-session.js";
import type { JazzClient } from "../web/registered-jazz-client.js";
import type { JazzSession as CoreSession, JazzSessionSnapshot } from "../session/jazz-session.js";
import { Jazz } from "../effect/jazz.js";
import { JazzError, toJazzError } from "../effect/errors.js";

/**
 * Who is signed in, as a Schema, so it can travel in a message and be
 * logged and replayed like any other.
 *
 * - `Starting`: the session is opening.
 * - `Ready`: `accountId` is signed in and `Jazz` can be provided for it.
 *   `error` is set when an operation failed but left the client usable.
 * - `SignedOut`: no one is signed in; `error` may say why.
 * - `Transitioning`: signing in, out, or switching users.
 * - `Failed`: there is no usable client; `JazzSession.retry` tries again.
 * - `Closed`: the session's scope closed.
 */
export const JazzSessionState = Schema.Union([
  Schema.TaggedStruct("Starting", {}),
  Schema.TaggedStruct("Ready", {
    accountId: Schema.String,
    kind: Schema.Literals(["device", "backend"]),
    error: Schema.optionalKey(Schema.String),
  }),
  Schema.TaggedStruct("SignedOut", { error: Schema.optionalKey(Schema.String) }),
  Schema.TaggedStruct("Transitioning", {}),
  Schema.TaggedStruct("Failed", { error: Schema.String }),
  Schema.TaggedStruct("Closed", {}),
]);
export type JazzSessionState = typeof JazzSessionState.Type;

/**
 * `Jazz` was asked for while no one is signed in (or while the session
 * switches users). Key the component that provides it on the signed-in
 * account, so it mounts only once someone is.
 */
export class JazzNotSignedIn extends Schema.TaggedError<JazzNotSignedIn>()("JazzNotSignedIn", {
  /** The session's state then. */
  status: Schema.String,
}) {
  override get message(): string {
    return `Jazz has no signed-in user (session is ${this.status})`;
  }
}

export interface JazzSessionService {
  /** The underlying session, for APIs this binding does not wrap. */
  readonly session: CoreSession<JazzClient>;
  /** The current state, then every change to it. */
  readonly state: Stream.Stream<JazzSessionState>;
  /**
   * Your own auth changed (after a sign-in, sign-out or account switch):
   * Jazz asks your backend who is signed in now and follows its answer.
   */
  readonly refresh: Effect.Effect<void, JazzError>;
  /** Sign in as a new device account, which lives on this device only. */
  readonly createDeviceAccount: Effect.Effect<void, JazzError>;
  /** Sign out of Jazz (your own auth's sign-out is separate). */
  readonly signOut: Effect.Effect<void, JazzError>;
  /** Try again after a failure. */
  readonly retry: Effect.Effect<void, JazzError>;
}

const toState = (snapshot: JazzSessionSnapshot<JazzClient>): JazzSessionState => {
  const error = snapshot.error?.message;
  switch (snapshot.status) {
    case "starting":
      return { _tag: "Starting" };
    case "ready":
      return snapshot.user
        ? {
            _tag: "Ready",
            accountId: snapshot.user.accountId,
            kind: snapshot.user.kind,
            ...(error === undefined ? {} : { error }),
          }
        : // A ready client with no user is one that is about to switch.
          { _tag: "Transitioning" };
    case "signed-out":
      return { _tag: "SignedOut", ...(error === undefined ? {} : { error }) };
    case "transitioning":
      return { _tag: "Transitioning" };
    case "error":
      return { _tag: "Failed", error: error ?? "Jazz could not open" };
    case "closed":
      return { _tag: "Closed" };
  }
};

const call = (operation: string, run: () => Promise<void>): Effect.Effect<void, JazzError> =>
  Effect.tryPromise({ try: run, catch: toJazzError(operation) });

const makeService = (session: CoreSession<JazzClient>): JazzSessionService => ({
  session,
  state: Stream.callback<JazzSessionState>((queue) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        const emit = () => Queue.offerUnsafe(queue, toState(session.getSnapshot()));
        emit();
        return session.subscribe(emit);
      }),
      (stop) => Effect.sync(stop),
    ),
  ).pipe(Stream.changesWith((a, b) => Schema.toEquivalence(JazzSessionState)(a, b))),
  refresh: call("refresh", () => session.refresh()),
  createDeviceAccount: call("createDeviceAccount", () => session.createDeviceAccount()),
  signOut: call("signOut", () => session.signOut()),
  retry: call("retry", () => session.retry()),
});

/**
 * A Jazz session (who is signed in on this device) as an Effect service,
 * for apps whose user signs in and out. Provide it once, at the root, so it
 * outlives every screen.
 *
 * @example
 * ```ts
 * const Main = BrowserHost.mount(App, {}, root).pipe(
 *   Layer.provide(JazzSession.layer({ serverUrl: "/jazz" })),
 * );
 * Layer.launch(Main).pipe(Effect.runFork);
 * ```
 */
export class JazzSession extends Context.Service<JazzSession, JazzSessionService>()(
  "jazz-tools/effect-ui/JazzSession",
) {
  /** Open a session (`createJazzClient(config)`) and close it with the layer's scope. */
  static readonly layer = (config: JazzClientConfig): Layer.Layer<JazzSession, JazzError> =>
    Layer.effect(
      JazzSession,
      Effect.map(
        Effect.acquireRelease(
          Effect.tryPromise({ try: () => createJazzClient(config), catch: toJazzError("open") }),
          (session) => Effect.ignore(Effect.tryPromise(() => session.close())),
        ),
        makeService,
      ),
    );

  /** Provide {@link JazzSession} from a session the caller owns (and closes). */
  static readonly layerSession = (session: CoreSession<JazzClient>): Layer.Layer<JazzSession> =>
    Layer.succeed(JazzSession, makeService(session));

  /**
   * `Jazz` for the user signed in when the layer is built. The session
   * replaces its Db when someone signs in or out, so provide this to a
   * component keyed on the account (`Component.provide(Planner, { key,
   * layer: () => JazzSession.jazz })`): a new user mounts a new instance
   * with a new layer. Fails with {@link JazzNotSignedIn} while no one is.
   */
  static readonly jazz: Layer.Layer<Jazz, JazzNotSignedIn, JazzSession> = Layer.effect(
    Jazz,
    Effect.gen(function* () {
      const { session } = yield* JazzSession;
      const snapshot = session.getSnapshot();
      if (snapshot.status !== "ready" || !snapshot.client || !snapshot.user)
        return yield* new JazzNotSignedIn({ status: snapshot.status });
      return Jazz.fromDb(snapshot.client.db);
    }),
  );
}

/**
 * The session's state as a Subscription: list it in the root component's
 * `subscriptions` and render each screen from the state it delivers.
 *
 * @example
 * ```ts
 * subscriptions: () => [sessionState((state) => Message.ChangedSession({ state }))]
 * ```
 */
export const sessionState = <Msg>(
  toMessage: (state: JazzSessionState) => Msg,
): Subscription.Subscription<Msg, JazzSession> =>
  Subscription.make(
    "jazz-session",
    Stream.unwrap(
      Effect.gen(function* () {
        return (yield* JazzSession).state;
      }),
    ).pipe(Stream.map(toMessage)),
  );
