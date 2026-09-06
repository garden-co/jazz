import type { AccountHandle, AccountManager } from "jazz-tools";
import type { JazzClient } from "jazz-tools/react";

type Accounts = AccountManager<{ getToken(): Promise<string> }>;

/** Serializes the app-owned Jazz client around external-auth transitions. */
export class JazzLifecycle {
  private client: JazzClient | undefined;
  private activePrincipal: string | undefined;
  private activeSessionId: string | undefined;
  private chain = Promise.resolve();
  private closed = false;
  private reconciliation: Promise<void> | undefined;
  private reconcilingPrincipal: string | undefined;
  private requestedSessionId: string | undefined;

  constructor(
    private readonly accounts: Accounts,
    private readonly open: (account: AccountHandle) => Promise<JazzClient>,
    private readonly publish: (client: JazzClient | undefined) => void,
  ) {}

  reconcile(
    principal: string,
    sessionId: string,
    enroll: (accounts: Accounts) => Promise<unknown>,
  ): Promise<void> {
    // React development effects are mounted, cleaned up, and mounted again.
    // A new reconciliation owns the lifecycle again; a real unmount has no
    // subsequent reconciliation and leaves this set by close().
    this.closed = false;
    this.requestedSessionId = sessionId;
    if (this.reconciliation && this.reconcilingPrincipal === sessionId) return this.reconciliation;
    const reconciliation = this.enqueue(async () => {
      const selected = this.accounts.getLoggedIn();
      if (
        this.client &&
        selected?.identity.subject === principal &&
        this.activeSessionId === sessionId
      )
        return;

      await this.closeCurrent(true);
      if (selected && selected.identity.subject !== principal) this.accounts.logout();

      const current = this.accounts.getLoggedIn();
      // A Better Auth session is an authentication boundary even if its `sub`
      // repeats: loginJWT validates and selects the issuer/sub handle anew.
      if (current?.identity.subject !== principal || this.activeSessionId !== sessionId)
        await enroll(this.accounts);
      if (this.requestedSessionId !== sessionId) return;
      await this.openSelected(principal, sessionId);
    });
    this.reconciliation = reconciliation;
    this.reconcilingPrincipal = sessionId;
    void reconciliation.then(
      () => this.clearReconciliation(reconciliation),
      () => this.clearReconciliation(reconciliation),
    );
    return reconciliation;
  }

  transition(action: (accounts: Accounts) => Promise<unknown> | unknown): Promise<void> {
    return this.enqueue(async () => {
      await this.closeCurrent(true);
      await action(this.accounts);
    });
  }

  close(): Promise<void> {
    this.closed = true;
    return this.enqueue(() => this.closeCurrent(false));
  }

  isCurrent(principal: string, sessionId: string): boolean {
    return this.activePrincipal === principal && this.activeSessionId === sessionId;
  }

  private async openSelected(principal: string, sessionId: string): Promise<void> {
    const account = this.accounts.getLoggedIn();
    if (!account || account.identity.subject !== principal) {
      throw new Error("Jazz selected an account for a different signed-in user.");
    }
    const client = await this.open(account);
    if (this.closed || this.requestedSessionId !== sessionId) {
      await client.shutdown();
      return;
    }
    this.client = client;
    this.activePrincipal = principal;
    this.activeSessionId = sessionId;
    this.publish(client);
  }

  private async closeCurrent(waitForSync: boolean): Promise<void> {
    if (!this.client) return;
    await this.client.shutdown(waitForSync ? { waitForSync: true } : undefined);
    this.client = undefined;
    this.activePrincipal = undefined;
    this.activeSessionId = undefined;
    this.publish(undefined);
  }

  private enqueue(operation: () => Promise<void>): Promise<void> {
    const task = this.chain.then(operation);
    this.chain = task.catch(() => {});
    return task;
  }

  private clearReconciliation(reconciliation: Promise<void>): void {
    if (this.reconciliation === reconciliation) {
      this.reconciliation = undefined;
      this.reconcilingPrincipal = undefined;
    }
  }
}
