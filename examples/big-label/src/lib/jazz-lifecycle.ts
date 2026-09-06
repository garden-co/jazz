import type { AccountHandle, AccountManager, JWTAuth } from "jazz-tools/react";
import type { JazzClient } from "jazz-tools/react";

/**
 * Owns the one browser client selected by an account manager. Framework
 * providers only observe this client; they never decide when it is retired.
 */
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
        // A failed account operation can have changed the selected handle.
        // Reopen it so the retry UI never strands the user without a client.
        if (isCurrent()) await this.openSelected(isCurrent);
      }
    });
  }

  close() {
    return this.enqueue(() => this.closeCurrent(false));
  }

  private async closeCurrent(waitForSync: boolean) {
    if (!this.client) return;
    // Keep the published client if syncing fails: it is still the selected,
    // usable account and a later retry may close it successfully.
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
      // The external session changed while the account opened. Do not publish
      // the stale client.
      await this.disposeStale(next);
      return;
    }
    this.client = next;
    this.publish(next);
  }

  private async disposeStale(client: JazzClient) {
    // A never-published client has no app writes and no retry owner, so it
    // must not wait behind an offline sync barrier during stale retirement.
    await client.shutdown();
  }

  private enqueue(operation: () => Promise<void>) {
    const task = this.chain.then(operation);
    // Keep the queue usable after a failed close/open/auth transition while
    // returning the original rejection to the caller that can render Retry.
    this.chain = task.catch(() => {});
    return task;
  }
}
