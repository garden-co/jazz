"use client";

import type { AccountHandle, AccountManager } from "jazz-tools";
import type { JazzClient } from "jazz-tools/react";

type Manager = AccountManager<{ getToken(): Promise<string> }>;

/** Serializes client ownership; manager snapshots are deliberately not lifecycle commands. */
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
