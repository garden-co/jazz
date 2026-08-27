import { getRandomBytes } from "expo-crypto";
import { deleteItemAsync, getItemAsync, setItemAsync } from "expo-secure-store";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";
import {
  authSecretStorageKey,
  formatAuthSecret,
  parseAuthSecret,
  type AuthSecretScope,
} from "../runtime/auth-secret-codec.js";

export interface ExpoSecureStoreLike {
  getItemAsync(key: string): Promise<string | null>;
  setItemAsync(key: string, value: string): Promise<void>;
  deleteItemAsync(key: string): Promise<void>;
}

export interface ExpoAuthSecretStoreOptions extends AuthSecretScope {
  /** Explicit physical SecureStore key. Prefer appId/profile for portable scoping. */
  key?: string;
  /** Override SecureStore backend for tests and host adapters. */
  secureStore?: ExpoSecureStoreLike;
}

function resolveExpoAuthSecretKey(options: ExpoAuthSecretStoreOptions = {}): string {
  if (options.key) {
    return options.key;
  }

  return authSecretStorageKey(options);
}

export class ExpoAuthSecretStore implements AuthSecretStore {
  private static globalInstances = new Map<string, ExpoAuthSecretStore>();
  private static storageScopedInstances = new WeakMap<
    ExpoSecureStoreLike,
    Map<string, ExpoAuthSecretStore>
  >();
  private readonly key: string;
  private readonly store: ExpoSecureStoreLike;
  private cachedPromise: Promise<string> | null = null;

  constructor(options: ExpoAuthSecretStoreOptions = {}) {
    this.key = resolveExpoAuthSecretKey(options);
    this.store = options.secureStore ?? { getItemAsync, setItemAsync, deleteItemAsync };
  }

  static getDefault(options: ExpoAuthSecretStoreOptions = {}): ExpoAuthSecretStore {
    const key = resolveExpoAuthSecretKey(options);

    if (options.secureStore) {
      let instances = ExpoAuthSecretStore.storageScopedInstances.get(options.secureStore);
      if (!instances) {
        instances = new Map<string, ExpoAuthSecretStore>();
        ExpoAuthSecretStore.storageScopedInstances.set(options.secureStore, instances);
      }

      let instance = instances.get(key);
      if (!instance) {
        instance = new ExpoAuthSecretStore(options);
        instances.set(key, instance);
      }
      return instance;
    }

    let instance = ExpoAuthSecretStore.globalInstances.get(key);
    if (!instance) {
      instance = new ExpoAuthSecretStore(options);
      ExpoAuthSecretStore.globalInstances.set(key, instance);
    }
    return instance;
  }

  async loadSecret(): Promise<string | null> {
    const secret = await this.store.getItemAsync(this.key);
    if (secret === null) return null;
    parseAuthSecret(secret);
    return secret;
  }

  async saveSecret(secret: string): Promise<void> {
    parseAuthSecret(secret);
    await this.store.setItemAsync(this.key, secret);
    this.cachedPromise = Promise.resolve(secret);
  }

  async clearSecret(): Promise<void> {
    await this.store.deleteItemAsync(this.key);
    this.cachedPromise = null;
  }

  getOrCreateSecret(): Promise<string> {
    if (!this.cachedPromise) {
      this.cachedPromise = this.getOrCreateSecretInternal();
    }
    return this.cachedPromise;
  }

  private async getOrCreateSecretInternal(): Promise<string> {
    const existing = await this.store.getItemAsync(this.key);
    if (existing !== null) {
      parseAuthSecret(existing);
      return existing;
    }
    const secret = generateExpoAuthSecret();
    await this.store.setItemAsync(this.key, secret);
    return secret;
  }

  static loadSecret(options: ExpoAuthSecretStoreOptions = {}): Promise<string | null> {
    return ExpoAuthSecretStore.getDefault(options).loadSecret();
  }

  static saveSecret(secret: string, options: ExpoAuthSecretStoreOptions = {}): Promise<void> {
    return ExpoAuthSecretStore.getDefault(options).saveSecret(secret);
  }

  static clearSecret(options: ExpoAuthSecretStoreOptions = {}): Promise<void> {
    return ExpoAuthSecretStore.getDefault(options).clearSecret();
  }

  static getOrCreateSecret(options: ExpoAuthSecretStoreOptions = {}): Promise<string> {
    return ExpoAuthSecretStore.getDefault(options).getOrCreateSecret();
  }
}

export const expoAuthSecretStore: AuthSecretStore = ExpoAuthSecretStore.getDefault();

function generateExpoAuthSecret(): string {
  const bytes = getRandomBytes(32);
  return formatAuthSecret(bytes);
}
