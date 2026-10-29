import type { AccountHandle, AccountManager, JWTAuth } from "jazz-tools/react";
import type { JazzClient } from "jazz-tools/react";

/** Owns the selected account client independently from React's provider tree. */
export class JazzLifecycle {
  private client: JazzClient | undefined;
  /** The account the open client belongs to. */
  private account: AccountHandle | undefined;
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

  /**
   * Run an account operation while the open client keeps running. A login as
   * the open account's own identity leaves that account selected: its client
   * then adopts the fresh credential in place. Any other outcome closes the
   * client (syncing it first) and opens the newly selected account.
   */
  revalidate(
    action: (accounts: AccountManager<JWTAuth>) => Promise<unknown> | unknown,
    isCurrent: () => boolean = () => true,
  ) {
    return this.enqueue(async () => {
      if (!isCurrent()) return;
      try {
        await action(this.accounts);
      } finally {
        if (isCurrent() && this.accounts.getLoggedIn() !== this.account) {
          await this.closeCurrent(true);
          await this.openSelected(isCurrent);
        }
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
    this.account = undefined;
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
    this.account = account;
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
