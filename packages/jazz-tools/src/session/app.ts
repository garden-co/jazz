import {
  connectAuthProvider,
  type AuthProviderConnection,
  type AuthProviderState,
} from "./auth-provider.js";
import { connectBetterAuth, type BetterAuthClient } from "./better-auth.js";
import {
  attachJazzSessionConsumer,
  type JazzSession,
  type JazzSessionActions,
  type JazzSessionSnapshot,
  type JazzSessionConsumer,
} from "./state.js";

export type JazzAuth =
  | { readonly kind: "better-auth"; readonly client: BetterAuthClient }
  | ({ readonly kind: "jwt"; getToken(): Promise<string>; logout(): unknown } & AuthProviderState);
export const betterAuth = (client: BetterAuthClient): JazzAuth => ({ kind: "better-auth", client });
/** The logout callback must clear provider state and eventually publish key: null. */
export const jwtAuth = (options: Omit<Extract<JazzAuth, { kind: "jwt" }>, "kind">): JazzAuth => ({
  kind: "jwt",
  ...options,
});
export interface JazzAppSnapshot<Client> {
  readonly status: "starting" | "signed-out" | "transitioning" | "ready" | "error";
  readonly client?: Client;
  readonly account?: JazzSessionSnapshot<Client>["account"];
  readonly error?: Error;
  /** Action retries repeat a failed enrollment; session retries only recover the selected client. */
  readonly recovery?: "action" | "session";
}
export interface JazzApp<Client> {
  getSnapshot(): JazzAppSnapshot<Client>;
  subscribe(listener: () => void): () => void;
  start(): Promise<void>;
  updateAuth(auth: JazzAuth | undefined): void;
  retry(): Promise<void>;
  logout(): Promise<void>;
  dispose(): Promise<void>;
  readonly sessionActions: JazzSessionActions;
  /** Acknowledge only after the rendered view has detached its old subscriptions. */
  attachConsumer(): { acknowledge(snapshot: JazzAppSnapshot<Client>): void; release(): void };
}
const asError = (cause: unknown) => (cause instanceof Error ? cause : new Error(String(cause)));

/** Host-independent owner. Configuration is captured once; auth state may update in place. */
export function createJazzAppOwner<Config, Client>(
  config: Config & { auth?: JazzAuth },
  factory: (config: Config) => Promise<JazzSession<Client>>,
  options: { start?: boolean } = {},
): JazzApp<Client> {
  let auth = config.auth;
  let session: JazzSession<Client> | undefined;
  let connection:
    | (Omit<AuthProviderConnection, "update" | "logout"> & {
        logout(action?: () => unknown): Promise<void>;
      })
    | undefined;
  let jwtConnection: AuthProviderConnection | undefined;
  let jwtSource: { value: Extract<JazzAuth, { kind: "jwt" }> } | undefined;
  let unsubscribeSession: (() => void) | undefined;
  let unsubscribeConnection: (() => void) | undefined;
  let pending: Promise<void> | undefined;
  let disposal: Promise<void> | undefined;
  let disposed = false;
  let failure: Error | undefined;
  let manualRetry: (() => Promise<void>) | undefined;
  let actionGeneration = 0;
  let manualActionPending = false;
  let pendingLogouts = 0;
  let retrying: Promise<void> | undefined;
  let snapshot: JazzAppSnapshot<Client> = Object.freeze({ status: "starting" });
  const versions = new WeakMap<object, JazzSessionSnapshot<Client>>();
  const listeners = new Set<() => void>();
  const consumers = new Set<{ lease?: JazzSessionConsumer<Client> }>();
  function publish(force = false) {
    if (disposed && !force) return;
    const current = session?.getSnapshot();
    const provider = connection?.getSnapshot();
    const error = failure ?? provider?.error ?? current?.error;
    const ready = !disposed && current?.status === "ready" && (!auth || provider?.ready);
    const status: JazzAppSnapshot<Client>["status"] = error
      ? "error"
      : !current
        ? "starting"
        : ready
          ? "ready"
          : current.status === "signed-out" && (!auth || provider?.ready)
            ? "signed-out"
            : "transitioning";
    const recovery: JazzAppSnapshot<Client>["recovery"] = error
      ? (current?.recovery ??
        (current && (current.status === "ready" || current.status === "signed-out")
          ? "action"
          : "session"))
      : undefined;
    const next = {
      status,
      recovery,
      client: status === "ready" ? current?.client : undefined,
      account: current?.account,
      error,
    };
    // Keep the raw snapshot version even when a provider hides a still-ready client.
    if (
      snapshot.status === next.status &&
      snapshot.client === next.client &&
      snapshot.account === next.account &&
      snapshot.error === next.error &&
      snapshot.recovery === next.recovery &&
      (!current || versions.get(snapshot) === current)
    )
      return;
    snapshot = Object.freeze(next);
    if (current) versions.set(snapshot, current);
    for (const listener of [...listeners]) {
      try {
        listener();
      } catch (cause) {
        console.error("Jazz app observer failed", cause);
      }
    }
  }
  function disconnect() {
    unsubscribeConnection?.();
    unsubscribeConnection = undefined;
    connection?.dispose();
    connection = undefined;
    jwtConnection = undefined;
    jwtSource = undefined;
  }
  function connect() {
    if (!session || !auth || disposed) return;
    if (auth.kind === "better-auth") connection = connectBetterAuth(session, auth.client);
    else {
      const source = { value: auth };
      jwtSource = source;
      jwtConnection = connectAuthProvider(session, { getToken: () => source.value.getToken() });
      connection = jwtConnection;
      jwtConnection.update(auth);
    }
    unsubscribeConnection = connection.subscribe(publish);
  }
  const requireSession = () => {
    if (disposed) throw new Error("Jazz app is disposed");
    if (!session) throw new Error("Jazz app is starting; retry initialization first");
    return session;
  };
  const sessionActions = {} as JazzSessionActions;
  for (const name of [
    "createLocalFirst",
    "restoreLocalFirst",
    "becomeBackend",
    "registerJWT",
    "loginJWT",
    "loginOrRegisterJWT",
    "linkJWT",
  ] as const) {
    (sessionActions as any)[name] = async (...args: unknown[]) => {
      if (auth) throw new Error("Manual session actions require an app without managed auth");
      const current = requireSession();
      if (manualActionPending || pendingLogouts)
        throw new Error("A Jazz session operation is already pending");
      const generation = ++actionGeneration;
      const invoke = async () => {
        if (disposed || generation !== actionGeneration)
          throw new Error("Jazz app action was superseded");
        if (manualActionPending || pendingLogouts)
          throw new Error("A Jazz session operation is already pending");
        manualActionPending = true;
        manualRetry = undefined;
        failure = undefined;
        try {
          await (current[name] as (...args: unknown[]) => Promise<void>)(...args);
          if (generation === actionGeneration) manualRetry = undefined;
        } catch (cause) {
          const failed = current.getSnapshot();
          if (
            generation === actionGeneration &&
            failed.error &&
            (failed.recovery === "action" ||
              failed.status === "ready" ||
              failed.status === "signed-out")
          )
            manualRetry = invoke;
          throw cause;
        } finally {
          manualActionPending = false;
          publish();
        }
      };
      return invoke();
    };
  }
  const app: JazzApp<Client> = {
    getSnapshot: () => snapshot,
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    start() {
      if (disposed) return Promise.reject(new Error("Jazz app is disposed"));
      if (pending) return pending;
      if (session) return Promise.resolve();
      failure = undefined;
      publish();
      const task = Promise.resolve()
        .then(() => factory(config))
        .then(async (created) => {
          if (disposed) {
            await created.close();
            return;
          }
          session = created;
          for (const consumer of consumers) consumer.lease = attachJazzSessionConsumer(created);
          unsubscribeSession = created.subscribe(() => publish(disposed));
          connect();
          publish();
        })
        .catch((cause) => {
          failure = asError(cause);
          publish();
          throw cause;
        })
        .finally(() => {
          if (pending === task) pending = undefined;
        });
      pending = task;
      return task;
    },
    updateAuth(next) {
      if (disposed) return;
      const same =
        auth?.kind === next?.kind &&
        (auth?.kind !== "better-auth" ||
          (next?.kind === "better-auth" && auth.client === next.client));
      auth = next;
      if (session) failure = undefined;
      if (same && next?.kind === "jwt") {
        if (jwtSource) jwtSource.value = next;
        jwtConnection?.update(next);
      } else if (!same) {
        disconnect();
        connect();
      }
      publish();
    },
    retry() {
      if (disposed) return Promise.reject(new Error("Jazz app is disposed"));
      if (!session) return app.start();
      if (retrying) return retrying;
      const current = requireSession();
      const generation = actionGeneration;
      failure = undefined;
      const task = Promise.resolve()
        .then(async () => {
          if (disposed || generation !== actionGeneration) return;
          try {
            if (connection) await connection.retry();
            else if (manualRetry && snapshot.recovery === "action") await manualRetry();
            else {
              await current.retry();
              manualRetry = undefined;
            }
          } catch (cause) {
            if (generation === actionGeneration) failure = asError(cause);
            throw cause;
          } finally {
            publish();
          }
        })
        .finally(() => {
          if (retrying === task) retrying = undefined;
        });
      retrying = task;
      return task;
    },
    async logout() {
      const current = requireSession();
      ++pendingLogouts;
      const generation = ++actionGeneration;
      manualRetry = undefined;
      failure = undefined;
      try {
        if (jwtConnection) {
          const leaving = auth;
          await jwtConnection.logout(() =>
            leaving?.kind === "jwt" ? leaving.logout() : undefined,
          );
        } else if (connection) await connection.logout();
        else await current.logout();
        if (generation === actionGeneration) manualRetry = undefined;
      } catch (cause) {
        if (!connection && generation === actionGeneration) manualRetry = app.logout;
        if (generation === actionGeneration) failure = asError(cause);
        throw cause;
      } finally {
        --pendingLogouts;
        publish();
      }
    },
    dispose() {
      if (disposal) return disposal;
      disposed = true;
      ++actionGeneration;
      manualRetry = undefined;
      disconnect();
      publish(true);
      disposal = (async () => {
        try {
          await pending;
          await session?.close();
        } catch (cause) {
          failure = asError(cause);
          publish(true);
          throw cause;
        } finally {
          unsubscribeSession?.();
          for (const consumer of consumers) consumer.lease?.release();
          consumers.clear();
          listeners.clear();
        }
      })();
      return disposal;
    },
    sessionActions,
    attachConsumer() {
      if (disposed) throw new Error("Jazz app is disposed");
      const consumer = { lease: session ? attachJazzSessionConsumer(session) : undefined };
      consumers.add(consumer);
      return {
        acknowledge(value) {
          const raw = versions.get(value);
          if (raw) consumer.lease?.acknowledge(raw);
        },
        release() {
          consumer.lease?.release();
          consumers.delete(consumer);
        },
      };
    },
  };
  sessionActions.retry = app.retry;
  sessionActions.logout = app.logout;
  sessionActions.close = app.dispose;
  if (options.start !== false) void app.start().catch(() => {});
  return app;
}
