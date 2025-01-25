import { KvStore } from "jazz-react-native";
import { MMKV } from "react-native-mmkv";

const storage = new MMKV({
  id: "e2e_rn_cli.default",
});

export class MMKVStore implements KvStore {
  get(key: string): Promise<string | null> {
    if (!key) {
      throw new Error("Key is required");
    }
    return Promise.resolve(storage.getString(key) || null);
  }

  set(key: string, value: string): Promise<void> {
    if (!key) {
      throw new Error("Key is required");
    }
    storage.set(key, value);
    return Promise.resolve();
  }

  delete(key: string): Promise<void> {
    if (!key) {
      throw new Error("Key is required");
    }
    storage.delete(key);
    return Promise.resolve();
  }

  clearAll(): Promise<void> {
    storage.clearAll();
    return Promise.resolve();
  }
}
