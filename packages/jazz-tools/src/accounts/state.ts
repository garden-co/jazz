import type { BackendAuth } from "./enrollment.js";
import { createListeners, toError } from "../runtime/util.js";
/** Framework-independent account selection. Credentials live in handles, never snapshots. */
export interface AccountIdentity {
  readonly issuer: string;
  readonly subject: string;
}

declare const accountHandleBrand: unique symbol;
/** Created only by account enrollment helpers; not a serializable credential. */
export interface AccountHandle {
  readonly [accountHandleBrand]: true;
  readonly id: string;
  readonly identity: AccountIdentity;
}

export type AccountOperation =
  | "becomeBackend"
  | "loginWithBackend"
  | "linkWithBackend"
  | "registerJWT"
  | "loginJWT"
  | "loginOrRegisterJWT";
export interface AccountSnapshot {
  readonly account: AccountHandle | undefined;
  readonly pending: AccountOperation | undefined;
  readonly error: Error | undefined;
}

/** @internal Host-owned persistence of actual account-selection transitions. */
export interface AccountSelectionDurability {
  /** Queue this selection before returning; a synchronous failure is retried explicitly. */
  selectionChanged(account: AccountHandle | undefined): void;
  settle(retry: boolean): Promise<void>;
}

/** @internal Enrollment boundary implemented by first-party account adapters. */
export interface AccountEnrollment<Auth> {
  createLocalFirst(): AccountHandle;
  restoreLocalFirst?(secret: string): AccountHandle;
  logout?(): void;
  becomeBackend?(auth: BackendAuth): Promise<AccountHandle>;
  loginWithBackend?(): Promise<AccountHandle>;
  /** Link the selected local-first guest to the user the backend names. */
  linkWithBackend?(account: AccountHandle): Promise<AccountHandle>;
  /** @internal Test fixtures: enroll with a JWT the test's core trusts. */
  registerJWT?(auth: Auth): Promise<AccountHandle>;
  /** @internal */
  loginJWT?(auth: Auth): Promise<AccountHandle>;
  /** @internal */
  loginOrRegisterJWT?(auth: Auth): Promise<AccountHandle>;
}

export class AccountOperationSuperseded extends Error {
  constructor() {
    super("Account selection changed while this operation was pending");
    this.name = "AccountOperationSuperseded";
  }
}

/** Shared state machine. Each SSR request must own its own instance. */
export class AccountManager<Auth> {
  private snapshot: AccountSnapshot = Object.freeze({
    account: undefined,
    pending: undefined,
    error: undefined,
  });
  private generation = 0;
  // Pending/error-only publications and retries retain the same selection token.
  private selectionDurabilityAttempt: { error?: Error } = {};
  private listeners = createListeners("Account state observer failed");

  /** @internal Public factories supply the verified enrollment adapter. */
  constructor(
    private readonly enrollment: AccountEnrollment<Auth>,
    initialAccount?: AccountHandle,
    private readonly selectionDurability?: AccountSelectionDurability,
  ) {
    if (initialAccount)
      this.snapshot = Object.freeze({
        account: initialAccount,
        pending: undefined,
        error: undefined,
      });
  }

  readonly getSnapshot = (): AccountSnapshot => this.snapshot;
  readonly subscribe = this.listeners.subscribe;

  /** @internal Await host persistence before opening the selected account's client. */
  settleSelection(retry = false): Promise<void> {
    const attempt = this.selectionDurabilityAttempt;
    if (retry && attempt.error) {
      this.queueSelectionPersistence();
      this.listeners.notify();
      if (attempt !== this.selectionDurabilityAttempt)
        return Promise.reject(new AccountOperationSuperseded());
    }
    if (attempt.error) return Promise.reject(attempt.error);
    return this.selectionDurability?.settle(retry) ?? Promise.resolve();
  }

  /** @internal Surface host storage failure without changing the selected account. */
  reportPersistenceError(cause: unknown): void {
    const error = toError(cause);
    this.publish({ ...this.snapshot, error });
  }

  getLoggedIn(): AccountHandle | undefined {
    return this.snapshot.account;
  }

  createLocalFirst(): AccountHandle {
    const account = this.enrollment.createLocalFirst();
    this.generation++;
    this.publish({ account, pending: undefined, error: undefined });
    return account;
  }

  /** Restore a local signing root outside any context, after its ordinary shutdown. */
  restoreLocalFirst(secret: string): AccountHandle {
    if (!this.enrollment.restoreLocalFirst)
      throw new Error("Local account recovery is unavailable");
    const account = this.enrollment.restoreLocalFirst(secret);
    this.generation++;
    this.publish({ account, pending: undefined, error: undefined });
    return account;
  }

  loginWithBackend(): Promise<AccountHandle> {
    return this.run("loginWithBackend", () => {
      if (!this.enrollment.loginWithBackend)
        throw new Error("Backend sign-in is unavailable on this host");
      return this.enrollment.loginWithBackend();
    });
  }

  linkWithBackend(): Promise<AccountHandle> {
    const account = this.snapshot.account;
    return this.run("linkWithBackend", () => {
      if (!account) throw new Error("Linking requires a logged-in account");
      if (!this.enrollment.linkWithBackend)
        throw new Error("Backend sign-in is unavailable on this host");
      return this.enrollment.linkWithBackend(account);
    });
  }

  /**
   * @internal Test fixtures only: enroll with a JWT the test's core trusts.
   * Apps sign in through their backend or stay local-first.
   */
  registerJWT(auth: Auth): Promise<AccountHandle> {
    return this.enrollJWT("registerJWT", auth);
  }

  /** @internal Test fixtures only. */
  loginJWT(auth: Auth): Promise<AccountHandle> {
    return this.enrollJWT("loginJWT", auth);
  }

  /** @internal Test fixtures only. */
  loginOrRegisterJWT(auth: Auth): Promise<AccountHandle> {
    return this.enrollJWT("loginOrRegisterJWT", auth);
  }

  private enrollJWT(
    operation: "registerJWT" | "loginJWT" | "loginOrRegisterJWT",
    auth: Auth,
  ): Promise<AccountHandle> {
    return this.run(operation, () => {
      const enroll = this.enrollment[operation];
      if (!enroll) throw new Error("JWT enrollment is unavailable on this host");
      return enroll.call(this.enrollment, auth);
    });
  }

  becomeBackend(auth: BackendAuth): Promise<AccountHandle> {
    return this.run("becomeBackend", () => {
      if (!this.enrollment.becomeBackend)
        throw new Error("Backend accounts are unavailable on this host");
      return this.enrollment.becomeBackend(auth);
    });
  }

  logout(): void {
    this.generation++;
    this.enrollment.logout?.();
    this.publish({ account: undefined, pending: undefined, error: undefined });
  }

  private async run(
    operation: AccountOperation,
    enroll: () => Promise<AccountHandle>,
  ): Promise<AccountHandle> {
    const generation = ++this.generation;
    this.publish({ ...this.snapshot, pending: operation, error: undefined });
    try {
      if (generation !== this.generation) throw new AccountOperationSuperseded();
      const account = await enroll();
      if (generation !== this.generation) throw new AccountOperationSuperseded();
      this.publish({ account, pending: undefined, error: undefined });
      if (generation !== this.generation) throw new AccountOperationSuperseded();
      return account;
    } catch (cause) {
      const error = toError(cause);
      if (generation === this.generation) {
        this.publish({ ...this.snapshot, pending: undefined, error });
      }
      throw error;
    }
  }

  private publish(snapshot: AccountSnapshot): void {
    const previousAccount = this.snapshot.account;
    this.snapshot = Object.freeze(snapshot);
    if (snapshot.account !== previousAccount) {
      this.selectionDurabilityAttempt = {};
      this.queueSelectionPersistence();
    }
    if (this.selectionDurabilityAttempt.error && this.snapshot.error === undefined) {
      this.snapshot = Object.freeze({
        ...this.snapshot,
        error: this.selectionDurabilityAttempt.error,
      });
    }
    this.listeners.notify();
  }

  private queueSelectionPersistence(): void {
    const attempt = this.selectionDurabilityAttempt;
    const previousError = attempt.error;
    try {
      this.selectionDurability?.selectionChanged(this.snapshot.account);
      if (attempt !== this.selectionDurabilityAttempt) return;
      attempt.error = undefined;
      if (previousError && this.snapshot.error === previousError)
        this.snapshot = Object.freeze({ ...this.snapshot, error: undefined });
    } catch (cause) {
      // A re-entrant selection owns its own outcome; an older hook cannot poison it.
      if (attempt !== this.selectionDurabilityAttempt) return;
      attempt.error = toError(cause);
      this.snapshot = Object.freeze({ ...this.snapshot, error: attempt.error });
    }
  }
}
