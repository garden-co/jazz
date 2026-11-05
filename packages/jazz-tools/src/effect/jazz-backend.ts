import { Context, Effect, Layer } from "effect";
import { HttpServerRequest } from "effect/http";
import {
  createJazz,
  type CreateJazzOptions,
  type Jazz as AppJazz,
} from "../backend/create-jazz.js";
import type { Db } from "../runtime/db.js";
import { JazzError, toJazzError } from "./errors.js";
import { Jazz, type JazzDb } from "./jazz.js";

export interface JazzBackendService {
  /** The app's Jazz (`createJazz`), for what this binding does not wrap: serving `/jazz`, `linkIdentity`. */
  readonly jazz: AppJazz;
  /**
   * The backend's own database, acting with the backend's authority rather
   * than a user's. Prefer {@link forRequest} in request handlers.
   */
  readonly authority: Effect.Effect<JazzDb>;
  /** Act as the user your `authenticate` names for `request`, with their permissions. */
  forRequest(request: Request): Effect.Effect<JazzDb, JazzError>;
  /**
   * Keep backend permissions while recording the request's signed-in user as
   * author. Your permissions are not checked (see `createJazz`'s `withAttribution`).
   */
  withAttribution(request: Request): Effect.Effect<JazzDb, JazzError>;
}

/**
 * The app's Jazz in an Effect backend. It deliberately does not provide
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
  /** Wrap an app's Jazz (`createJazz`). The caller keeps ownership of it. */
  static readonly fromJazz = (jazz: AppJazz): JazzBackendService => makeJazzBackend(jazz);

  /** Provide {@link JazzBackend} from an app's Jazz the caller owns. */
  static readonly layerJazz = (jazz: AppJazz): Layer.Layer<JazzBackend> =>
    Layer.succeed(JazzBackend, makeJazzBackend(jazz));

  /**
   * Run the app's Jazz (`createJazz`) for the layer's lifetime and close it
   * when the layer's scope closes.
   */
  static readonly layer = (options: CreateJazzOptions): Layer.Layer<JazzBackend, JazzError> =>
    Layer.effect(
      JazzBackend,
      Effect.gen(function* () {
        const jazz = yield* Effect.acquireRelease(
          Effect.tryPromise({
            // createJazz opens on first use; the layer opens it up front so
            // a store that cannot open fails the layer, not a later request.
            try: async () => {
              const jazz = createJazz(options);
              try {
                await jazz.ready();
              } catch (error) {
                await jazz.close().catch(() => undefined);
                throw error;
              }
              return jazz;
            },
            catch: toJazzError("open"),
          }),
          (jazz) => Effect.ignore(Effect.tryPromise(() => jazz.close())),
        );
        return makeJazzBackend(jazz);
      }),
    );

  /** Provide {@link Jazz} to `self` as the user who sent `request`. */
  static readonly forRequest =
    (request: Request) =>
    <A, E, R>(
      self: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | JazzError, Exclude<R, Jazz> | JazzBackend> =>
      Effect.provideServiceEffect(
        self,
        Jazz,
        JazzBackend.use((backend) => backend.forRequest(request)),
      );

  /**
   * Provide {@link Jazz} to `self` as the user who sent the current
   * `HttpServerRequest`, for handlers built with `effect/http`.
   */
  static readonly forCurrentRequest =
    () =>
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
          HttpServerRequest.toWeb(request).pipe(
            Effect.mapError(toJazzError("forRequest")),
            Effect.flatMap((web) => JazzBackend.use((backend) => backend.forRequest(web))),
          ),
        ),
      );

  /** Provide {@link Jazz} to `self` with the backend's own authority. */
  static readonly asAuthority = <A, E, R>(
    self: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, Exclude<R, Jazz> | JazzBackend> =>
    Effect.provideServiceEffect(
      self,
      Jazz,
      JazzBackend.use((backend) => backend.authority),
    );
}

function makeJazzBackend(jazz: AppJazz): JazzBackendService {
  const scoped = (operation: string, open: () => Promise<Db>) =>
    Effect.tryPromise({ try: open, catch: toJazzError(operation) }).pipe(Effect.map(Jazz.fromDb));
  return {
    jazz,
    authority: Effect.sync(() => Jazz.fromDb(jazz.db)),
    forRequest: (request) => scoped("forRequest", () => jazz.forRequest(request)),
    withAttribution: (request) => scoped("withAttribution", () => jazz.withAttribution(request)),
  };
}
