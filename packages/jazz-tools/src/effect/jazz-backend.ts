import { Context, Effect, Layer } from "effect";
import { HttpServerRequest } from "effect/http";
import type { AccountHandle } from "../accounts/state.js";
import {
  createJazzSession,
  type JazzClient as BackendClient,
  type JazzSessionConfig,
} from "../backend/create-jazz-session.js";
import type { BackendRequestOptions } from "../backend/request-auth.js";
import type { RequestLike } from "../runtime/client.js";
import type { Db } from "../runtime/db.js";
import type { JazzSession } from "../session/state.js";
import { JazzError, toJazzError } from "./errors.js";
import { Jazz, type JazzDb } from "./jazz.js";

export interface JazzBackendService {
  /** The underlying backend session, for account actions this binding does not wrap. */
  readonly session: JazzSession<BackendClient>;
  /** The currently selected backend client. Fails when the session is not ready. */
  readonly client: Effect.Effect<BackendClient, JazzError>;
  /**
   * The backend's own database, acting with the backend's authority rather
   * than a user's. Prefer {@link forRequest} in request handlers.
   */
  readonly authority: Effect.Effect<JazzDb, JazzError>;
  /** Verify a request's credentials and act as that user, with their permissions. */
  forRequest(
    request: RequestLike,
    options?: BackendRequestOptions,
  ): Effect.Effect<JazzDb, JazzError>;
  /** Act as an admitted account, with its permissions. */
  forAccount(account: AccountHandle): Effect.Effect<JazzDb, JazzError>;
  /** Keep backend permissions while recording the request's verified user as author. */
  withAttributionForRequest(request: RequestLike): Effect.Effect<JazzDb, JazzError>;
  /** Keep backend permissions while recording `account` as author. */
  withAttribution(account: AccountHandle): Effect.Effect<JazzDb, JazzError>;
}

/**
 * A Node backend's Jazz session. It deliberately does not provide
 * {@link Jazz} itself: handlers choose whose permissions they act with, most
 * often the requesting user's through {@link JazzBackend.forRequest}.
 *
 * @example
 * ```ts
 * const handler = Effect.gen(function* () {
 *   const jazz = yield* Jazz;
 *   return yield* jazz.all(app.todos);
 * }).pipe(JazzBackend.forCurrentRequest());
 * ```
 */
export class JazzBackend extends Context.Service<JazzBackend, JazzBackendService>()(
  "jazz-tools/effect/JazzBackend",
) {
  /** Wrap an existing backend session. The caller keeps ownership of it. */
  static readonly fromSession = (session: JazzSession<BackendClient>): JazzBackendService =>
    makeJazzBackend(session);

  /** Provide {@link JazzBackend} from a backend session the caller owns. */
  static readonly layerSession = (session: JazzSession<BackendClient>): Layer.Layer<JazzBackend> =>
    Layer.succeed(JazzBackend, makeJazzBackend(session));

  /**
   * Open a backend session for the layer's lifetime and close it when the
   * layer's scope closes. The layer fails if the session does not open ready,
   * for example when `initial` backend admission is rejected.
   */
  static readonly layer = (config: JazzSessionConfig): Layer.Layer<JazzBackend, JazzError> =>
    Layer.effect(
      JazzBackend,
      Effect.gen(function* () {
        const session = yield* Effect.acquireRelease(
          Effect.tryPromise({ try: () => createJazzSession(config), catch: toJazzError("open") }),
          (session) => Effect.ignore(Effect.tryPromise(() => session.close())),
        );
        const backend = makeJazzBackend(session);
        yield* backend.client;
        return backend;
      }),
    );

  /** Provide {@link Jazz} to `self` as the user who sent `request`. */
  static readonly forRequest =
    (request: RequestLike, options?: BackendRequestOptions) =>
    <A, E, R>(
      self: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | JazzError, Exclude<R, Jazz> | JazzBackend> =>
      Effect.provideServiceEffect(
        self,
        Jazz,
        JazzBackend.use((backend) => backend.forRequest(request, options)),
      );

  /**
   * Provide {@link Jazz} to `self` as the user who sent the current
   * `HttpServerRequest`, for handlers built with `effect/http`.
   */
  static readonly forCurrentRequest =
    (options?: BackendRequestOptions) =>
    <A, E, R>(
      self: Effect.Effect<A, E, R>,
    ): Effect.Effect<
      A,
      E | JazzError,
      Exclude<R, Jazz> | JazzBackend | HttpServerRequest.HttpServerRequest
    > =>
      Effect.provideServiceEffect(
        self,
        Jazz,
        HttpServerRequest.HttpServerRequest.use((request) =>
          JazzBackend.use((backend) => backend.forRequest(request, options)),
        ),
      );

  /** Provide {@link Jazz} to `self` with the backend's own authority. */
  static readonly asAuthority = <A, E, R>(
    self: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | JazzError, Exclude<R, Jazz> | JazzBackend> =>
    Effect.provideServiceEffect(
      self,
      Jazz,
      JazzBackend.use((backend) => backend.authority),
    );
}

function makeJazzBackend(session: JazzSession<BackendClient>): JazzBackendService {
  const client = Effect.suspend(() => {
    const snapshot = session.getSnapshot();
    return snapshot.status === "ready" && snapshot.client
      ? Effect.succeed(snapshot.client)
      : Effect.fail(
          new JazzError({
            operation: "backend",
            cause:
              snapshot.error ?? new Error(`Jazz backend session is ${snapshot.status}, not ready`),
          }),
        );
  });
  const scoped = (operation: string, open: (client: BackendClient) => Promise<Db>) =>
    Effect.flatMap(client, (client) =>
      Effect.tryPromise({ try: () => open(client), catch: toJazzError(operation) }).pipe(
        Effect.map(Jazz.fromDb),
      ),
    );
  return {
    session,
    client,
    authority: Effect.map(client, (client) => Jazz.fromDb(client.db)),
    forRequest: (request, options) =>
      scoped("forRequest", (client) => client.forRequest(request, options)),
    forAccount: (account) => scoped("forAccount", (client) => client.forAccount(account)),
    withAttributionForRequest: (request) =>
      scoped("withAttributionForRequest", (client) => client.withAttributionForRequest(request)),
    withAttribution: (account) =>
      scoped("withAttribution", (client) => client.withAttribution(account)),
  };
}
