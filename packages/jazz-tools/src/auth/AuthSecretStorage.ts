import { AgentSecret } from "cojson";
import type { Account } from "../coValues/account.js";
import type { ID } from "../internal.js";
import { AuthCredentials } from "../types.js";
import KvStoreContext from "./KvStoreContext.js";

const STORAGE_KEY = "jazz-logged-in-secret";

export type AuthSetPayload = {
  accountID: ID<Account>;
  secretSeed?: Uint8Array;
  accountSecret: AgentSecret;
  provider:
    | "anonymous"
    | "clerk"
    | "betterauth"
    | "demo"
    | "passkey"
    | "passphrase"
    | string;
};

export class AuthSecretStorage {
  private listeners: Set<(isAuthenticated: boolean) => void>;
  public isAuthenticated: boolean;

  constructor() {
    this.listeners = new Set();
    this.isAuthenticated = false;
  }

  async migrate() {
    const kvStore = KvStoreContext.getInstance().getStorage();

    if (!(await kvStore.get(STORAGE_KEY))) {
      const demoAuthSecret = await kvStore.get("demo-auth-logged-in-secret");
      if (demoAuthSecret) {
        const parsed = JSON.parse(demoAuthSecret);
        await kvStore.set(
          STORAGE_KEY,
          JSON.stringify({
            accountID: parsed.accountID,
            accountSecret: parsed.accountSecret,
            provider: "demo",
          }),
        );
        await kvStore.delete("demo-auth-logged-in-secret");
      }

      const clerkAuthSecret = await kvStore.get("jazz-clerk-auth");
      if (clerkAuthSecret) {
        const parsed = JSON.parse(clerkAuthSecret);
        await kvStore.set(
          STORAGE_KEY,
          JSON.stringify({
            accountID: parsed.accountID,
            accountSecret: parsed.secret,
            provider: "clerk",
          }),
        );
        await kvStore.delete("jazz-clerk-auth");
      }
    }

    const value = await kvStore.get(STORAGE_KEY);

    if (value) {
      const parsed = JSON.parse(value);

      if ("secret" in parsed) {
        await kvStore.set(
          STORAGE_KEY,
          JSON.stringify({
            accountID: parsed.accountID,
            secretSeed: parsed.secretSeed,
            accountSecret: parsed.secret,
            provider: parsed.provider,
          }),
        );
      }
    }
  }

  async get(): Promise<AuthCredentials | null> {
    const kvStore = KvStoreContext.getInstance().getStorage();
    const data = await kvStore.get(STORAGE_KEY);

    if (!data) return null;

    const parsed = JSON.parse(data);

    if (!parsed.accountID || !parsed.accountSecret) {
      throw new Error("Invalid auth secret storage data");
    }

    return {
      accountID: parsed.accountID,
      secretSeed: parsed.secretSeed
        ? new Uint8Array(parsed.secretSeed)
        : undefined,
      accountSecret: parsed.accountSecret,
      provider: parsed.provider,
    };
  }

  async setWithoutNotify(payload: AuthSetPayload) {
    const kvStore = KvStoreContext.getInstance().getStorage();
    await kvStore.set(
      STORAGE_KEY,
      JSON.stringify({
        accountID: payload.accountID,
        secretSeed: payload.secretSeed
          ? Array.from(payload.secretSeed)
          : undefined,
        accountSecret: payload.accountSecret,
        provider: payload.provider,
      }),
    );
  }

  async set(payload: AuthSetPayload) {
    this.setWithoutNotify(payload);
    this.emitUpdate(payload);
  }

  getIsAuthenticated(data: AuthCredentials | null): boolean {
    if (!data) return false;
    return data.provider !== "anonymous";
  }

  onUpdate(handler: (isAuthenticated: boolean) => void) {
    this.listeners.add(handler);
    return () => {
      this.listeners.delete(handler);
    };
  }

  emitUpdate(data: AuthCredentials | null) {
    const isAuthenticated = this.getIsAuthenticated(data);

    if (this.isAuthenticated === isAuthenticated) return;

    this.isAuthenticated = isAuthenticated;
    for (const listener of this.listeners) {
      listener(this.isAuthenticated);
    }
  }

  async clearWithoutNotify() {
    const kvStore = KvStoreContext.getInstance().getStorage();
    await kvStore.delete(STORAGE_KEY);
  }

  async clear() {
    await this.clearWithoutNotify();
    this.emitUpdate(null);
  }
}
