import { AgentSecret } from "cojson";
import { Account, ID } from "jazz-tools";

const STORAGE_KEY = "jazz-logged-in-secret";

export type AuthCredentials =
  | {
      accountID: ID<Account>;
      secretSeed?: Uint8Array;
      accountSecret: AgentSecret;
      isAnonymous: false;
    }
  | {
      accountID: ID<Account>;
      secretSeed: Uint8Array;
      accountSecret: AgentSecret;
      isAnonymous: true;
    };

export type AuthSetPayload =
  | {
      accountID: ID<Account>;
      secretSeed: Uint8Array;
      accountSecret: AgentSecret;
      isAnonymous?: boolean;
    }
  | {
      accountID: ID<Account>;
      accountSecret: AgentSecret;
    };

export class AuthSecretStorage {
  static migrate() {
    if (!localStorage[STORAGE_KEY]) {
      const demoAuthSecret = localStorage["demo-auth-logged-in-secret"];
      if (demoAuthSecret) {
        localStorage[STORAGE_KEY] = demoAuthSecret;
        delete localStorage["demo-auth-logged-in-secret"];
      }

      const clerkAuthSecret = localStorage["jazz-clerk-auth"];
      if (clerkAuthSecret) {
        localStorage[STORAGE_KEY] = clerkAuthSecret;
        delete localStorage["jazz-clerk-auth"];
      }
    }
  }

  static get(): AuthCredentials | null {
    const data = localStorage.getItem(STORAGE_KEY);

    if (!data) return null;

    const parsed = JSON.parse(data);

    if (parsed.secretSeed) {
      const secretSeed = new Uint8Array(parsed.secretSeed);

      return {
        accountID: parsed.accountID,
        secretSeed,
        accountSecret: parsed.accountSecret,
        isAnonymous: Boolean(parsed.isAnonymous),
      };
    }

    if (parsed.accountSecret) {
      return {
        accountID: parsed.accountID,
        accountSecret: parsed.accountSecret,
        isAnonymous: false,
      };
    }

    throw new Error("Invalid auth secret storage data");
  }

  static set(payload: AuthSetPayload) {
    if ("secretSeed" in payload) {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          accountID: payload.accountID,
          secretSeed: Array.from(payload.secretSeed),
          accountSecret: payload.accountSecret,
          isAnonymous: payload.isAnonymous,
        }),
      );
      this.emitUpdate();
      return;
    }

    if ("accountSecret" in payload) {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          accountID: payload.accountID,
          accountSecret: payload.accountSecret,
        }),
      );
      this.emitUpdate();
      return;
    }

    throw new Error("Invalid auth secret storage data");
  }

  static isAnonymous() {
    const data = localStorage.getItem(STORAGE_KEY);

    if (!data) return false;

    const parsed = JSON.parse(data);

    return Boolean(parsed.isAnonymous);
  }

  static onUpdate(handler: () => void) {
    window.addEventListener("jazz-auth-update", handler);
    return () => window.removeEventListener("jazz-auth-update", handler);
  }

  static emitUpdate() {
    window.dispatchEvent(new Event("jazz-auth-update"));
  }

  static clear() {
    localStorage.removeItem(STORAGE_KEY);
    this.emitUpdate();
  }
}
