import { getContext, setContext } from "svelte";
import type { AccountHandle, AccountManager } from "jazz-tools";
import type { JazzClient } from "jazz-tools/svelte";

type Manager = AccountManager<{ getToken(): Promise<string> }>;
const key = Symbol("jazz-lifecycle");

/** Serializes the one provider-owned Jazz client. */
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

  private openSelected(isCurrent: () => boolean = () => true): Promise<void> {
    const account = this.accounts.getLoggedIn();
    if (!account) return Promise.resolve();
    return this.open(account).then(async (client) => {
      if (!isCurrent()) {
        await client.shutdown();
        return;
      }
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

export interface JazzLifecycleApi {
  transition: JazzLifecycle["transition"];
  authenticate(
    enroll: boolean,
    request: () => Promise<{ error?: { message?: string | null } | null }>,
  ): Promise<void>;
  reportFailure(cause: unknown): void;
}
export function setJazzLifecycle(lifecycle: JazzLifecycleApi) {
  setContext(key, lifecycle);
}
export function getJazzLifecycle() {
  const lifecycle = getContext<JazzLifecycleApi>(key);
  if (!lifecycle) throw new Error("Jazz lifecycle is not ready");
  return lifecycle;
}
