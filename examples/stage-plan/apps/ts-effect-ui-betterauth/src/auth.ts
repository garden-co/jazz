import { createAuthClient } from "better-auth/client";
import { Context, Effect, Layer, Queue, Redacted, Schema, Stream } from "effect";

/** Whether Better Auth has a session on this origin (`/api/auth`). */
export const AuthState = Schema.Literals(["pending", "signed-in", "signed-out"]);
export type AuthState = typeof AuthState.Type;

export class AuthFailed extends Schema.TaggedError<AuthFailed>()("AuthFailed", {
  message: Schema.String,
}) {}

export interface Credentials {
  readonly email: string;
  readonly password: Redacted.Redacted<string>;
}

/**
 * Better Auth as an Effect service, so components reach it through
 * Subscriptions and Commands, and tests replace it with a Layer.
 */
export class Auth extends Context.Service<
  Auth,
  {
    readonly state: Stream.Stream<AuthState>;
    readonly signIn: (credentials: Credentials) => Effect.Effect<void, AuthFailed>;
    readonly signUp: (
      credentials: Credentials & { readonly name: string },
    ) => Effect.Effect<void, AuthFailed>;
    readonly signOut: Effect.Effect<void, AuthFailed>;
  }
>()("stage-plan/Auth") {
  static readonly layer = Layer.sync(Auth, () => {
    const client = createAuthClient();
    const atom = client.$store.atoms.session!;
    const read = (): AuthState => {
      const session = atom.get() as { isPending: boolean; data: unknown };
      return session.isPending ? "pending" : session.data ? "signed-in" : "signed-out";
    };
    // Better Auth answers `{ data, error }` rather than rejecting.
    type Answer = { readonly error: { readonly message?: string | undefined } | null };
    const attempt = (run: () => Promise<unknown>) =>
      Effect.tryPromise({
        try: () => run() as Promise<Answer>,
        catch: (cause) => new AuthFailed({ message: String(cause) }),
      }).pipe(
        Effect.flatMap(({ error }) =>
          error
            ? Effect.fail(new AuthFailed({ message: error.message ?? "Sign-in failed" }))
            : Effect.void,
        ),
      );
    return Auth.of({
      state: Stream.callback<AuthState>((queue) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            Queue.offerUnsafe(queue, read());
            return atom.subscribe(() => Queue.offerUnsafe(queue, read()));
          }),
          (stop) => Effect.sync(stop),
        ),
      ).pipe(Stream.changes),
      signIn: ({ email, password }) =>
        attempt(() => client.signIn.email({ email, password: Redacted.value(password) })),
      signUp: ({ name, email, password }) =>
        attempt(() => client.signUp.email({ name, email, password: Redacted.value(password) })),
      signOut: attempt(() => client.signOut()),
    });
  });
}
