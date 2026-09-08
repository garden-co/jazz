import type { JazzSession } from "./state.js";

/** A stable provider session identifier, never a JWT. Pending means initial hydration. */
export interface AuthProviderState {
  key: string | null;
  isPending?: boolean;
  error?: Error;
}
export interface AuthProviderSnapshot {
  key?: string | null;
  /** True only after Jazz has admitted the current provider session. */
  ready: boolean;
  isPending: boolean;
  error?: Error;
}
export interface AuthProviderConnection {
  getSnapshot(): AuthProviderSnapshot;
  subscribe(listener: () => void): () => void;
  update(state: AuthProviderState): void;
  retry(): Promise<void>;
  /** Flush Jazz before revoking the provider credential. Retry repeats failed logout. */
  logout(signOut: () => unknown): Promise<void>;
  /** Stop observing. The caller still owns and closes its JazzSession. */
  dispose(): void;
}
const owners = new WeakMap<object, AuthProviderConnection>();
const releases = new WeakMap<object, Promise<void>>();
const asError = (cause: unknown) => (cause instanceof Error ? cause : new Error(String(cause)));

/** Reconcile a provider with an externally owned Jazz session.
 * Signup, login and restoration atomically login-or-register. For linking an
 * existing local-first account, use manual session.linkJWT without this connector.
 */
export function connectAuthProvider<Client>(
  session: JazzSession<Client>,
  options: {
    getToken(): Promise<string>;
  },
): AuthProviderConnection {
  if (owners.has(session)) throw new Error("Jazz session already has an auth provider connection");
  const predecessor = releases.get(session);
  let desired: AuthProviderState = { key: null, isPending: true };
  let snapshot: AuthProviderSnapshot = { ready: false, isPending: true };
  let admitted: string | null | undefined;
  let attempted: string | null | undefined;
  let generation = 0;
  let disposed = false;
  let running: Promise<void> | undefined;
  let signOut: (() => unknown) | undefined;
  let failure: Error | undefined;
  const listeners = new Set<() => void>();
  function publish() {
    if (disposed) return;
    const current = session.getSnapshot();
    const error = failure ?? desired.error ?? current.error;
    const ready =
      !signOut &&
      !desired.isPending &&
      !error &&
      admitted === desired.key &&
      (desired.key === null ? current.status === "signed-out" : current.status === "ready");
    const isPending = !!desired.isPending || !!running || (!error && admitted !== desired.key);
    if (
      snapshot.key === desired.key &&
      snapshot.ready === ready &&
      snapshot.isPending === isPending &&
      snapshot.error === error
    )
      return;
    snapshot = { key: desired.key, ready, isPending, error };
    for (const listener of [...listeners]) listener();
  }
  function reconcile(): Promise<void> {
    if (disposed || desired.isPending || desired.error || signOut) {
      publish();
      return Promise.resolve();
    }
    if (running) return running;
    if (attempted === desired.key) {
      publish();
      return Promise.resolve();
    }
    const key = desired.key;
    const token = generation;
    attempted = key;
    admitted = undefined;
    failure = undefined;
    let established = false;
    const operation = Promise.resolve().then(async () => {
      await predecessor;
      if (disposed || token !== generation) return;
      if (key === null) await session.logout();
      else
        await session.loginOrRegisterJWT({
          async getToken() {
            if (
              (!established && (disposed || token !== generation)) ||
              desired.key !== key ||
              desired.isPending
            )
              throw new Error("Auth provider session changed");
            const jwt = await options.getToken();
            if (
              (!established && (disposed || token !== generation)) ||
              desired.key !== key ||
              desired.isPending
            )
              throw new Error("Auth provider session changed while fetching a token");
            return jwt;
          },
        });
      established = true;
      if (!disposed && token === generation) admitted = key;
    });
    const task = operation
      .catch((cause) => {
        if (!disposed && token === generation) failure = asError(cause);
      })
      .finally(() => {
        if (running !== task) return;
        running = undefined;
        publish();
        if (!disposed && token !== generation) void reconcile();
      });
    running = task;
    publish();
    return task;
  }
  const unsubscribe = session.subscribe(publish);
  const connection: AuthProviderConnection = {
    getSnapshot: () => snapshot,
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    update(next) {
      if (disposed) return;
      if (desired.key !== next.key || !!desired.isPending !== !!next.isPending) {
        ++generation;
        attempted = undefined;
        failure = undefined;
      }
      desired = next;
      publish();
      void reconcile();
    },
    async retry() {
      if (signOut) {
        await connection.logout(signOut);
        return;
      }
      await running;
      attempted = undefined;
      failure = undefined;
      await reconcile();
    },
    async logout(action) {
      if (disposed) throw new Error("Auth provider connection is disposed");
      if (running && signOut) {
        await running;
        if (failure) throw failure;
        return;
      }
      signOut = action;
      ++generation;
      admitted = undefined;
      failure = undefined;
      const previous = running;
      const leavingKey = desired.key;
      const operation = Promise.resolve().then(async () => {
        await previous;
        await session.logout();
        await action();
        signOut = undefined;
        // Providers may emit null after signOut resolves. Do not re-admit stale state.
        attempted = desired.key === leavingKey || desired.key === null ? desired.key : undefined;
        if (desired.key === null) admitted = null;
      });
      running = operation
        .catch((cause) => {
          failure = asError(cause);
        })
        .finally(() => {
          running = undefined;
          publish();
          if (!signOut && attempted !== desired.key) void reconcile();
        });
      publish();
      await running;
      if (failure) throw failure;
    },
    dispose() {
      if (running) releases.set(session, running);
      disposed = true;
      ++generation;
      unsubscribe();
      listeners.clear();
      if (owners.get(session) === connection) owners.delete(session);
    },
  };
  owners.set(session, connection);
  return connection;
}
