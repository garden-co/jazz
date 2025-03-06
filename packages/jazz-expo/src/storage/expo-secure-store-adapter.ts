import * as SecureStore from "expo-secure-store";
import type { KvStore } from "jazz-react-native-core";

export class ExpoSecureStoreAdapter implements KvStore {
  get(key: string): Promise<string | null> {
    return SecureStore.getItemAsync(key, {
      requireAuthentication: false,
      keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
    });
  }

  async set(key: string, value: string): Promise<void> {
    return SecureStore.setItemAsync(key, value, {
      requireAuthentication: false,
      keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
    });
  }

  async delete(key: string): Promise<void> {
    return SecureStore.deleteItemAsync(key, {
      requireAuthentication: false,
      keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
    });
  }

  async clearAll(): Promise<void> {
    throw new Error("Not implemented");
  }
}
