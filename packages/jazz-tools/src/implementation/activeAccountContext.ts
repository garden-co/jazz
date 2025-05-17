import type { Account } from "../internal.js";

class ActiveAccountContext {
  private activeAccount: Account | null = null;
  private guestMode: boolean = false;

  set(account: Account | null) {
    this.activeAccount = account;
    this.guestMode = false;
  }

  setGuestMode() {
    this.activeAccount = null;
    this.guestMode = true;
  }

  maybeGet() {
    return this.activeAccount;
  }

  get() {
    if (!this.activeAccount) {
      if (this.guestMode) {
        throw new Error(
          "Something that expects a full active account was called in guest mode.",
        );
      }

      throw new Error("No active account");
    }

    return this.activeAccount;
  }
}

export type { ActiveAccountContext };

export const activeAccountContext = new ActiveAccountContext();
