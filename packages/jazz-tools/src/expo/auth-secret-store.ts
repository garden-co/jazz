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

/** State shared by every adapter instance addressing one SecureStore key. */
type SecretOperationState = {
  cachedPromise: Promise<string> | null;
  operationTail: Promise<void>;
};

function newSecretOperationState(): SecretOperationState {
  return { cachedPromise: null, operationTail: Promise.resolve() };
}

function resolveExpoAuthSecretKey(options: ExpoAuthSecretStoreOptions = {}): string {
  if (options.key) {
    return options.key;
  }

  return authSecretStorageKey(options);
}

export class ExpoAuthSecretStore implements AuthSecretStore {
  private static globalInstances = new Map<string, ExpoAuthSecretStore>();
  private static globalOperationStates = new Map<string, SecretOperationState>();
  private static storageScopedInstances = new WeakMap<
    ExpoSecureStoreLike,
    Map<string, ExpoAuthSecretStore>
  >();
  private static storageScopedOperationStates = new WeakMap<
    ExpoSecureStoreLike,
    Map<string, SecretOperationState>
  >();
  private readonly key: string;
  private readonly store: ExpoSecureStoreLike;
  private readonly operationState: SecretOperationState;

  constructor(options: ExpoAuthSecretStoreOptions = {}) {
    this.key = resolveExpoAuthSecretKey(options);
    this.store = options.secureStore ?? { getItemAsync, setItemAsync, deleteItemAsync };
    this.operationState = ExpoAuthSecretStore.getOperationState(this.key, options.secureStore);
  }

  private static getOperationState(
    key: string,
    secureStore: ExpoSecureStoreLike | undefined,
  ): SecretOperationState {
    if (!secureStore) {
      let state = ExpoAuthSecretStore.globalOperationStates.get(key);
      if (!state) {
        state = newSecretOperationState();
        ExpoAuthSecretStore.globalOperationStates.set(key, state);
      }
      return state;
    }

    let states = ExpoAuthSecretStore.storageScopedOperationStates.get(secureStore);
    if (!states) {
      states = new Map<string, SecretOperationState>();
      ExpoAuthSecretStore.storageScopedOperationStates.set(secureStore, states);
    }
    let state = states.get(key);
    if (!state) {
      state = newSecretOperationState();
      states.set(key, state);
    }
    return state;
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

  loadSecret(): Promise<string | null> {
    return this.serialize(async () => {
      const secret = await this.store.getItemAsync(this.key);
      if (secret === null) return null;
      parseAuthSecret(secret);
      return secret;
    });
  }

  saveSecret(secret: string): Promise<void> {
    parseAuthSecret(secret);
    const saving = this.serialize(() => this.store.setItemAsync(this.key, secret));
    const cached = saving.then(() => secret);
    this.operationState.cachedPromise = cached;
    void cached.catch(() => {
      if (this.operationState.cachedPromise === cached) {
        this.operationState.cachedPromise = null;
      }
    });
    return saving;
  }

  clearSecret(): Promise<void> {
    this.operationState.cachedPromise = null;
    return this.serialize(() => this.store.deleteItemAsync(this.key));
  }

  getOrCreateSecret(): Promise<string> {
    if (!this.operationState.cachedPromise) {
      const pending = this.serialize(() => this.getOrCreateSecretInternal());
      this.operationState.cachedPromise = pending;
      void pending.catch(() => {
        if (this.operationState.cachedPromise === pending) {
          this.operationState.cachedPromise = null;
        }
      });
    }
    return this.operationState.cachedPromise;
  }

  private serialize<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.operationState.operationTail.then(operation);
    this.operationState.operationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
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
