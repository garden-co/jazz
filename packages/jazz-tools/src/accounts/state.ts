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
  | "registerJWT"
  | "loginJWT"
  | "loginOrRegisterJWT"
  | "linkJWT"
  | "becomeBackend"
  | "loginWithBackend";
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
  registerJWT(auth: Auth): Promise<AccountHandle>;
  loginJWT(auth: Auth): Promise<AccountHandle>;
  loginOrRegisterJWT(auth: Auth): Promise<AccountHandle>;
  linkJWT(account: AccountHandle, auth: Auth): Promise<AccountHandle>;
  /**
   * Re-admit the selected account in place. Resolves to that same handle with
   * a fresh credential bound, or undefined when the provider identity or the
   * registry assignment changed and an ordinary transition is required.
   */
  revalidateJWT?(
    account: AccountHandle,
    operation: "loginJWT" | "loginOrRegisterJWT",
    auth: Auth,
    isCurrent?: () => boolean,
  ): Promise<AccountHandle | undefined>;
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

  becomeBackend(auth: BackendAuth): Promise<AccountHandle> {
    return this.run("becomeBackend", () => {
      if (!this.enrollment.becomeBackend)
        throw new Error("Backend accounts are unavailable on this host");
      return this.enrollment.becomeBackend(auth);
    });
  }

  registerJWT(auth: Auth): Promise<AccountHandle> {
    return this.run("registerJWT", () => this.enrollment.registerJWT(auth));
  }

  /**
   * Log in with a provider JWT. On a manager that retains the selected
   * account across reloads, logging in again as that account's identity
   * re-admits it in place: the same handle is returned and contexts already
   * open for it adopt the fresh credential. Until then a retained account's
   * context has no credential and does not sync.
   */
  loginJWT(auth: Auth): Promise<AccountHandle> {
    return this.login("loginJWT", auth);
  }

  /** Like {@link loginJWT}, registering an account when the identity has none. */
  loginOrRegisterJWT(auth: Auth): Promise<AccountHandle> {
    return this.login("loginOrRegisterJWT", auth);
  }

  private login(operation: "loginJWT" | "loginOrRegisterJWT", auth: Auth): Promise<AccountHandle> {
    if (!this.snapshot.account || !this.enrollment.revalidateJWT)
      return this.enrollJWT(operation, auth);
    const revalidation = this.revalidateJWT(operation, auth);
    // Any account operation that starts meanwhile (a logout) supersedes this one.
    const generation = this.generation;
    return revalidation.then((same) => {
      if (same) return same;
      if (generation !== this.generation) throw new AccountOperationSuperseded();
      return this.enrollJWT(operation, auth);
    });
  }

  /**
   * @internal The ordinary login: always enrolls a fresh handle. Session hosts
   * call it after their own in-place attempt declined.
   */
  enrollJWT(operation: "loginJWT" | "loginOrRegisterJWT", auth: Auth): Promise<AccountHandle> {
    return this.run(operation, () => this.enrollment[operation](auth));
  }

  linkJWT(auth: Auth): Promise<AccountHandle> {
    const account = this.snapshot.account;
    return this.run("linkJWT", () => {
      if (!account) throw new Error("Linking requires a logged-in account");
      return this.enrollment.linkJWT(account, auth);
    });
  }

  /** @internal Whether a same-identity login re-admits the selected account in place. */
  get revalidatesInPlace(): boolean {
    return this.enrollment.revalidateJWT !== undefined;
  }

  /**
   * @internal Revalidate the selected account without a transition. Resolves
   * to the same handle once the registry re-admits it, or undefined when the
   * caller must run an ordinary transition instead.
   */
  async revalidateJWT(
    operation: "loginJWT" | "loginOrRegisterJWT",
    auth: Auth,
    /** The caller still wants this outcome; checked again just before binding. */
    isCurrent: () => boolean = () => true,
  ): Promise<AccountHandle | undefined> {
    const account = this.snapshot.account;
    const revalidate = this.enrollment.revalidateJWT;
    if (!account || !revalidate) return undefined;
    const generation = ++this.generation;
    this.publish({ ...this.snapshot, pending: operation, error: undefined });
    try {
      const outcome = await revalidate.call(
        this.enrollment,
        account,
        operation,
        auth,
        () => generation === this.generation && isCurrent(),
      );
      if (generation !== this.generation) throw new AccountOperationSuperseded();
      this.publish({ account, pending: undefined, error: undefined });
      return outcome === account ? account : undefined;
    } catch (cause) {
      const error = toError(cause);
      if (generation === this.generation) {
        this.publish({ ...this.snapshot, pending: undefined, error });
      }
      throw error;
    }
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
