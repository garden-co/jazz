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

export type AccountOperation = "registerJWT" | "loginJWT" | "linkJWT";
export interface AccountSnapshot {
  readonly account: AccountHandle | undefined;
  readonly pending: AccountOperation | undefined;
  readonly error: Error | undefined;
}

/** @internal Enrollment boundary implemented by first-party account adapters. */
export interface AccountEnrollment<Auth> {
  createLocalFirst(): AccountHandle;
  logout?(): void;
  registerJWT(auth: Auth): Promise<AccountHandle>;
  loginJWT(auth: Auth): Promise<AccountHandle>;
  linkJWT(account: AccountHandle, auth: Auth): Promise<AccountHandle>;
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
  private listeners = new Set<() => void>();

  /** @internal Public factories supply the verified enrollment adapter. */
  constructor(
    private readonly enrollment: AccountEnrollment<Auth>,
    initialAccount?: AccountHandle,
  ) {
    if (initialAccount)
      this.snapshot = Object.freeze({
        account: initialAccount,
        pending: undefined,
        error: undefined,
      });
  }

  readonly getSnapshot = (): AccountSnapshot => this.snapshot;
  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  /** @internal Surface host storage failure without changing the selected account. */
  reportPersistenceError(cause: unknown): void {
    const error = cause instanceof Error ? cause : new Error(String(cause));
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

  registerJWT(auth: Auth): Promise<AccountHandle> {
    return this.run("registerJWT", () => this.enrollment.registerJWT(auth));
  }

  loginJWT(auth: Auth): Promise<AccountHandle> {
    return this.run("loginJWT", () => this.enrollment.loginJWT(auth));
  }

  linkJWT(auth: Auth): Promise<AccountHandle> {
    const account = this.snapshot.account;
    return this.run("linkJWT", () => {
      if (!account) throw new Error("Linking requires a logged-in account");
      return this.enrollment.linkJWT(account, auth);
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
      const error = cause instanceof Error ? cause : new Error(String(cause));
      if (generation === this.generation) {
        this.publish({ ...this.snapshot, pending: undefined, error });
      }
      throw error;
    }
  }

  private publish(snapshot: AccountSnapshot): void {
    this.snapshot = Object.freeze(snapshot);
    for (const listener of [...this.listeners]) {
      try {
        listener();
      } catch (error) {
        console.error("Account state observer failed", error);
      }
    }
  }
}
