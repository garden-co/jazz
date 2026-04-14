import { getItemAsync, setItemAsync, deleteItemAsync } from "expo-secure-store";
import { getRandomBytes } from "expo-crypto";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

const DEFAULT_KEY = "jazz-auth-secret";

export interface ExpoAuthSecretStoreOptions {
  key?: string;
  secureStore?: {
    getItemAsync(key: string): Promise<string | null>;
    setItemAsync(key: string, value: string): Promise<void>;
    deleteItemAsync(key: string): Promise<void>;
  };
}

export class ExpoAuthSecretStore implements AuthSecretStore {
  private static defaultInstance: ExpoAuthSecretStore | null = null;
  private readonly key: string;
  private readonly store: {
    getItemAsync(key: string): Promise<string | null>;
    setItemAsync(key: string, value: string): Promise<void>;
    deleteItemAsync(key: string): Promise<void>;
  };
  private cachedPromise: Promise<string> | null = null;

  constructor(options: ExpoAuthSecretStoreOptions = {}) {
    this.key = options.key ?? DEFAULT_KEY;
    this.store = options.secureStore ?? { getItemAsync, setItemAsync, deleteItemAsync };
  }

  private static getDefault(): ExpoAuthSecretStore {
    if (!ExpoAuthSecretStore.defaultInstance) {
      ExpoAuthSecretStore.defaultInstance = new ExpoAuthSecretStore();
    }
    return ExpoAuthSecretStore.defaultInstance;
  }

  async loadSecret(): Promise<string | null> {
    return this.store.getItemAsync(this.key);
  }

  async saveSecret(secret: string): Promise<void> {
    await this.store.setItemAsync(this.key, secret);
  }

  async clearSecret(): Promise<void> {
    await this.store.deleteItemAsync(this.key);
    this.cachedPromise = null;
  }

  getOrCreateSecret(): Promise<string> {
    if (!this.cachedPromise) {
      this.cachedPromise = this._getOrCreate();
    }
    return this.cachedPromise;
  }

  private async _getOrCreate(): Promise<string> {
    const existing = await this.store.getItemAsync(this.key);
    if (existing) return existing;
    const secret = generateExpoAuthSecret();
    await this.store.setItemAsync(this.key, secret);
    return secret;
  }

  static loadSecret(): Promise<string | null> {
    return ExpoAuthSecretStore.getDefault().loadSecret();
  }

  static saveSecret(secret: string): Promise<void> {
    return ExpoAuthSecretStore.getDefault().saveSecret(secret);
  }

  static clearSecret(): Promise<void> {
    return ExpoAuthSecretStore.getDefault().clearSecret();
  }

  static getOrCreateSecret(): Promise<string> {
    return ExpoAuthSecretStore.getDefault().getOrCreateSecret();
  }
}

function generateExpoAuthSecret(): string {
  const bytes = getRandomBytes(32);
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
