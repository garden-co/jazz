import { settleAccountSelection } from "../accounts/selection-durability.js";
import type { AccountHandle, AccountManager } from "../accounts/state.js";
import type { JWTAuth, BackendAuth } from "../accounts/enrollment.js";
import {
  GracefulShutdownSyncError,
  SharedClientShutdownError,
} from "../runtime/graceful-shutdown-error.js";

export interface SessionClient {
  shutdown(options?: { waitForSync?: boolean }): Promise<void>;
}
export type JazzSessionOperation =
  | "createLocalFirst"
  | "restoreLocalFirst"
  | "becomeBackend"
  | "registerJWT"
  | "loginJWT"
  | "linkJWT"
  | "logout"
  | "retry";
export interface JazzSessionSnapshot<Client> {
  readonly status: "ready" | "signed-out" | "transitioning" | "error" | "closed";
  readonly account?: AccountHandle;
  readonly client?: Client;
  readonly pending?: JazzSessionOperation;
  readonly error?: Error;
}
export interface JazzSessionActions {
  createLocalFirst(): Promise<void>;
  restoreLocalFirst(secret: string): Promise<void>;
  becomeBackend(auth: BackendAuth): Promise<void>;
  registerJWT(auth: JWTAuth): Promise<void>;
  loginJWT(auth: JWTAuth): Promise<void>;
  linkJWT(auth: JWTAuth): Promise<void>;
  logout(): Promise<void>;
  retry(): Promise<void>;
  close(): Promise<void>;
}
export interface JazzSession<Client> extends JazzSessionActions {
  getSnapshot(): JazzSessionSnapshot<Client>;
  subscribe(listener: () => void): () => void;
}
export interface JazzSessionConsumer<Client> {
  acknowledge(snapshot: JazzSessionSnapshot<Client>): void;
  release(): void;
}
const consumers = new WeakMap<object, () => JazzSessionConsumer<unknown>>();
/** @internal Framework commit acknowledgement; never required by imperative callers. */
export function attachJazzSessionConsumer<Client>(
  session: JazzSession<Client>,
): JazzSessionConsumer<Client> {
  const attach = consumers.get(session);
  if (!attach) throw new Error("Unknown Jazz session");
  return attach() as JazzSessionConsumer<Client>;
}
const asError = (cause: unknown): Error =>
  cause instanceof Error ? cause : new Error(String(cause));

/** @internal Hosts prepare accounts and supply their client adapter once. */
export async function createJazzSessionOwner<Client extends SessionClient>(options: {
  accounts: AccountManager<JWTAuth>;
  openClient(account: AccountHandle): Promise<Client>;
  initial?: "local-first" | BackendAuth;
}): Promise<JazzSession<Client>> {
  const { accounts, openClient } = options;
  let selected = accounts.getLoggedIn();
  if (!selected && options.initial === "local-first") selected = accounts.createLocalFirst();
  if (typeof options.initial === "object") selected = await accounts.becomeBackend(options.initial);
  await settleAccountSelection(accounts);
  let client = selected ? await openClient(selected) : undefined;
  let snapshot: JazzSessionSnapshot<Client> = Object.freeze({
    status: client ? "ready" : "signed-out",
    account: selected,
    client,
  });
  const listeners = new Set<() => void>();
  const leases = new Set<{ waiting?: () => void }>();
  const versions = new WeakMap<object, number>();
  let version = 0;
  versions.set(snapshot, version);
  let generation = 0;
  let busy: Promise<void> | undefined;
  let closing: Promise<void> | undefined;
  let loggingOut: Promise<void> | undefined;
  let closed = false;
  const publish = (next: JazzSessionSnapshot<Client>) => {
    snapshot = Object.freeze(next);
    versions.set(snapshot, ++version);
    for (const listener of [...listeners]) {
      try {
        listener();
      } catch (error) {
        console.error("Jazz session observer failed", error);
      }
    }
  };
  const detach = (next: JazzSessionSnapshot<Client>) => {
    const waits = [...leases].map(
      (lease) =>
        new Promise<void>((resolve) => {
          // A later transition (logout/close) also releases an earlier waiter.
          const previous = lease.waiting;
          lease.waiting = () => {
            previous?.();
            resolve();
          };
        }),
    );
    publish(next);
    return { done: Promise.all(waits).then(() => {}) };
  };
  const superseded = () => new Error("Jazz session operation was superseded");
  const perform = async (
    operation: JazzSessionOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
    token: number,
  ) => {
    if (token !== generation) throw superseded();
    const previous = selected;
    await detach({ status: "transitioning", account: selected, pending: operation }).done;
    if (token !== generation) throw superseded();
    try {
      if (client) {
        await client.shutdown({ waitForSync: true });
        client = undefined;
      }
    } catch (cause) {
      if (token === generation) {
        const usable =
          cause instanceof GracefulShutdownSyncError || cause instanceof SharedClientShutdownError;
        // Unknown teardown failures retain the client privately as a shutdown
        // barrier. Never publish a potentially stopped runtime or enroll over it.
        publish({
          status: usable ? "ready" : "error",
          account: selected,
          client: usable ? client : undefined,
          error: asError(cause),
        });
      }
      throw cause;
    }
    if (token !== generation) throw superseded();
    try {
      selected = await mutate();
    } catch (cause) {
      if (token !== generation) throw cause;
      selected = previous;
      try {
        client = previous ? await openClient(previous) : undefined;
      } catch (startup) {
        if (token === generation)
          publish({ status: "error", account: selected, error: asError(startup) });
        throw cause;
      }
      if (token === generation)
        publish({
          status: client ? "ready" : "signed-out",
          account: selected,
          client,
          error: asError(cause),
        });
      throw cause;
    }
    if (token !== generation) throw superseded();
    try {
      // The registry operation has already succeeded. A local persistence
      // failure must not restore a now-invalid previous handle or repeat linking.
      await settleAccountSelection(accounts, operation === "retry");
      if (token !== generation) throw superseded();
      client = selected ? await openClient(selected) : undefined;
    } catch (cause) {
      if (token === generation)
        publish({ status: "error", account: selected, error: asError(cause) });
      throw cause;
    }
    if (token !== generation) throw superseded();
    publish({ status: client ? "ready" : "signed-out", account: selected, client });
  };
  const run = (
    operation: JazzSessionOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
  ): Promise<void> => {
    if (closed) return Promise.reject(new Error("Jazz session is closed"));
    if (busy || loggingOut)
      return Promise.reject(new Error("A Jazz session operation is already pending"));
    const token = ++generation;
    const task = Promise.resolve().then(() => perform(operation, mutate, token));
    busy = task;
    void task
      .finally(() => {
        if (busy === task) busy = undefined;
      })
      .catch(() => {});
    return task;
  };
  const session: JazzSession<Client> = {
    getSnapshot: () => snapshot,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    createLocalFirst: () => run("createLocalFirst", () => accounts.createLocalFirst()),
    restoreLocalFirst: (secret) =>
      run("restoreLocalFirst", () => accounts.restoreLocalFirst(secret)),
    becomeBackend: (auth) => run("becomeBackend", () => accounts.becomeBackend(auth)),
    registerJWT: (auth) => run("registerJWT", () => accounts.registerJWT(auth)),
    loginJWT: (auth) => run("loginJWT", () => accounts.loginJWT(auth)),
    linkJWT: (auth) => run("linkJWT", () => accounts.linkJWT(auth)),
    retry: () => run("retry", () => selected),
    logout: () => {
      if (closed) return Promise.reject(new Error("Jazz session is closed"));
      if (loggingOut) return loggingOut;
      const token = ++generation;
      const pending = busy;
      const task = Promise.resolve().then(async () => {
        await barrier.done;
        await pending?.catch(() => {});
        if (token !== generation) throw superseded();
        await perform(
          "logout",
          () => {
            accounts.logout();
            return undefined;
          },
          token,
        );
      });
      loggingOut = task;
      const barrier = detach({ status: "transitioning", account: selected, pending: "logout" });
      void task
        .finally(() => {
          if (loggingOut === task) loggingOut = undefined;
        })
        .catch(() => {});
      return task;
    },
    close: () => {
      if (closing) return closing;
      closed = true;
      ++generation;
      const pending = busy;
      const logout = loggingOut;
      closing = Promise.resolve().then(async () => {
        await barrier.done;
        await pending?.catch(() => {});
        await logout?.catch(() => {});
        const old = client;
        client = undefined;
        await old?.shutdown();
      });
      const barrier = detach({ status: "closed" });
      return closing;
    },
  };
  consumers.set(session, () => {
    const lease: { waiting?: () => void } = {};
    leases.add(lease);
    return {
      acknowledge: (observed) => {
        if (!observed.client && (versions.get(observed) ?? -1) >= version) {
          lease.waiting?.();
          lease.waiting = undefined;
        }
      },
      release: () => {
        leases.delete(lease);
        lease.waiting?.();
        lease.waiting = undefined;
      },
    };
  });
  return Object.freeze(session);
}
