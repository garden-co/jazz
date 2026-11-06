import { Context, Effect, Layer } from "effect";
import { HttpServerRequest } from "effect/http";
import {
  createJazz,
  type CreateJazzOptions,
  type Jazz as AppJazz,
} from "../backend/create-jazz.js";
import { isNoJazzAccountError } from "../backend/no-account-error.js";
import type { Db } from "../runtime/db.js";
import { JazzError, JazzNoAccount, toJazzError } from "./errors.js";
import { Jazz, type JazzDb } from "./jazz.js";

export interface JazzBackendService {
  /** The app's Jazz (`createJazz`), for what this binding does not wrap: serving `/jazz`, `linkIdentity`. */
  readonly jazz: AppJazz;
  /**
   * The backend's own database, acting with the backend's authority rather
   * than a user's. Prefer {@link forReqUser} in request handlers.
   */
  readonly authority: Effect.Effect<JazzDb>;
  /**
   * Act as the user your `authenticate` names for `request`, with their
   * permissions. Fails with {@link JazzNoAccount} when that user has no Jazz
   * account yet.
   */
  forReqUser(request: Request): Effect.Effect<JazzDb, JazzError | JazzNoAccount>;
  /**
   * Keep backend permissions while recording the request's signed-in user as
   * author. Your permissions are not checked (see `createJazz`'s `withAttribution`).
   * Fails with {@link JazzNoAccount} when that user has no Jazz account yet.
   */
  withAttribution(request: Request): Effect.Effect<JazzDb, JazzError | JazzNoAccount>;
  /**
   * Create the Jazz account of `userId` (as your `authenticate` names them),
   * or return the one they have (see `createJazz`'s `createAccountFor`).
   */
  createAccountFor(userId: string): Effect.Effect<string, JazzError>;
}

/**
 * The app's Jazz in an Effect backend. It deliberately does not provide
 * {@link Jazz} itself: handlers choose whose permissions they act with, most
 * often the requesting user's through {@link JazzBackend.forReqUser}.
 *
 * @example
 * ```ts
 * const handler = Effect.gen(function* () {
 *   const jazz = yield* Jazz;
 *   return yield* jazz.all(app.todos);
 * }).pipe(JazzBackend.forCurrentReqUser());
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
  static readonly forReqUser =
    (request: Request) =>
    <A, E, R>(
      self: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | JazzError | JazzNoAccount, Exclude<R, Jazz> | JazzBackend> =>
      Effect.provideServiceEffect(
        self,
        Jazz,
        JazzBackend.use((backend) => backend.forReqUser(request)),
      );

  /**
   * Provide {@link Jazz} to `self` as the user who sent the current
   * `HttpServerRequest`, for handlers built with `effect/http`.
   */
  static readonly forCurrentReqUser =
    () =>
    <A, E, R>(
      self: Effect.Effect<A, E, R>,
    ): Effect.Effect<
      A,
      E | JazzError | JazzNoAccount,
      Exclude<R, Jazz> | JazzBackend | HttpServerRequest.HttpServerRequest
    > =>
      Effect.provideServiceEffect(
        self,
        Jazz,
        HttpServerRequest.HttpServerRequest.use((request) =>
          HttpServerRequest.toWeb(request).pipe(
            Effect.mapError(toJazzError("forReqUser")),
            Effect.flatMap((web) => JazzBackend.use((backend) => backend.forReqUser(web))),
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
    Effect.tryPromise({
      try: open,
      catch: (cause): JazzError | JazzNoAccount =>
        isNoJazzAccountError(cause)
          ? new JazzNoAccount({ userId: cause.userId, operation })
          : toJazzError(operation)(cause),
    }).pipe(Effect.map(Jazz.fromDb));
  return {
    jazz,
    authority: Effect.sync(() => Jazz.fromDb(jazz.db)),
    forReqUser: (request) => scoped("forReqUser", () => jazz.forReqUser(request)),
    withAttribution: (request) => scoped("withAttribution", () => jazz.withAttribution(request)),
    createAccountFor: (userId) =>
      Effect.tryPromise({
        try: () => jazz.createAccountFor(userId),
        catch: toJazzError("createAccountFor"),
      }),
  };
}
