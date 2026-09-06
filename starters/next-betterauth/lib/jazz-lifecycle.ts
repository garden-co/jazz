"use client";
import type { AccountHandle, AccountManager } from "jazz-tools";
import type { JazzClient } from "jazz-tools/react";
type Manager = AccountManager<{ getToken(): Promise<string> }>;
export class JazzLifecycle {
  private client: JazzClient | undefined;
  private chain = Promise.resolve();
  private booted = false;
  constructor(
    readonly accounts: Manager,
    private readonly open: (account: AccountHandle) => Promise<JazzClient>,
    private readonly publish: (client: JazzClient | undefined) => void,
  ) {}
  attach(selectInitial: () => Promise<void>): Promise<void> {
    if (this.booted)
      return this.enqueue(async () => {
        if (!this.client) await this.openSelected();
      });
    this.booted = true;
    return this.enqueue(async () => {
      await selectInitial();
      await this.openSelected();
    });
  }
  transition(
    action: (accounts: Manager) => Promise<unknown> | unknown,
    isCurrent: () => boolean = () => true,
    reopenOnFailure = true,
  ): Promise<void> {
    return this.enqueue(async () => {
      if (!isCurrent()) return;
      if (this.client) {
        await this.client.shutdown({ waitForSync: true });
        this.client = undefined;
        this.publish(undefined);
      }
      let completed = false;
      try {
        await action(this.accounts);
        completed = true;
      } finally {
        if (isCurrent() && (completed || reopenOnFailure)) await this.openSelected(isCurrent);
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
  private async openSelected(isCurrent: () => boolean = () => true) {
    const account = this.accounts.getLoggedIn();
    if (!account) return;
    const client = await this.open(account);
    if (!isCurrent()) {
      await client.shutdown();
      return;
    }
    this.client = client;
    this.publish(client);
  }
  private enqueue(operation: () => Promise<void>): Promise<void> {
    const task = this.chain.then(operation);
    this.chain = task.catch(() => {});
    return task;
  }
}
