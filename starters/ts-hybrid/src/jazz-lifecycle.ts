import type { AccountHandle, AccountManager, Db } from "jazz-tools";

type Manager = AccountManager<{ getToken(): Promise<string> }>;

/** The application, rather than account snapshot timing, owns context replacement. */
export class JazzLifecycle {
  private client: Db | undefined;
  private chain = Promise.resolve();
  private booted = false;
  private publish: (db: Db | undefined) => void = () => {};

  constructor(
    readonly accounts: Manager,
    private readonly open: (account: AccountHandle) => Promise<Db>,
  ) {}

  onClientChange(publish: (db: Db | undefined) => void): void {
    this.publish = publish;
  }

  getClient(): Db {
    if (!this.client) throw new Error("Jazz client is not ready");
    return this.client;
  }

  attach(selectInitial: () => Promise<void>): Promise<void> {
    if (!this.booted) {
      this.booted = true;
      return this.enqueue(async () => {
        await selectInitial();
        await this.openSelected();
      });
    }
    return this.enqueue(() => this.openSelected());
  }

  transition(action: (accounts: Manager) => Promise<unknown> | unknown): Promise<void> {
    return this.enqueue(async () => {
      if (this.client) {
        await this.client.shutdown({ waitForSync: true });
        this.client = undefined;
        this.publish(undefined);
      }
      try {
        await action(this.accounts);
      } finally {
        await this.openSelected();
      }
    });
  }

  close(): Promise<void> {
    return this.enqueue(async () => {
      if (!this.client) return;
      await this.client.shutdown();
      this.client = undefined;
      this.publish(undefined);
    });
  }

  private openSelected(): Promise<void> {
    const account = this.accounts.getLoggedIn();
    if (!account) return Promise.resolve();
    return this.open(account).then((client) => {
      this.client = client;
      this.publish(client);
    });
  }

  private enqueue(operation: () => Promise<void>): Promise<void> {
    const task = this.chain.then(operation);
    this.chain = task.catch(() => {});
    return task;
  }
}
