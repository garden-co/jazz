import type { AccountHandle, AccountManager, JWTAuth } from "jazz-tools/react";
import type { JazzClient } from "jazz-tools/react";

/** Owns the selected account client independently from React's provider tree. */
export class JazzLifecycle {
  private client: JazzClient | undefined;
  private chain = Promise.resolve();

  constructor(
    private readonly accounts: AccountManager<JWTAuth>,
    private readonly open: (account: AccountHandle) => Promise<JazzClient>,
    private readonly publish: (client: JazzClient | undefined) => void,
  ) {}

  attach(isCurrent: () => boolean = () => true) {
    return this.enqueue(() => this.openSelected(isCurrent));
  }

  transition(
    action: (accounts: AccountManager<JWTAuth>) => Promise<unknown> | unknown,
    isCurrent: () => boolean = () => true,
  ) {
    return this.enqueue(async () => {
      if (!isCurrent()) return;
      await this.closeCurrent(true);
      try {
        await action(this.accounts);
      } finally {
        if (isCurrent()) await this.openSelected(isCurrent);
      }
    });
  }

  close() {
    return this.enqueue(() => this.closeCurrent(false));
  }

  private async closeCurrent(waitForSync: boolean) {
    if (!this.client) return;
    if (waitForSync) await this.client.shutdown({ waitForSync: true });
    else await this.client.shutdown();
    this.client = undefined;
    this.publish(undefined);
  }

  private async openSelected(isCurrent: () => boolean) {
    if (!isCurrent()) return;
    const account = this.accounts.getLoggedIn();
    if (!account) return;
    const next = await this.open(account);
    if (!isCurrent()) {
      await this.disposeStale(next);
      return;
    }
    this.client = next;
    this.publish(next);
  }

  private async disposeStale(client: JazzClient) {
    // A never-published client has no app writes and no retry owner. Retire
    // its lease immediately rather than waiting for an offline sync barrier.
    await client.shutdown();
  }

  private enqueue(operation: () => Promise<void>) {
    const task = this.chain.then(operation);
    this.chain = task.catch(() => {});
    return task;
  }
}
