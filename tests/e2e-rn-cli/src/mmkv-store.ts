import { KvStore } from "jazz-react-native";
import { MMKV } from "react-native-mmkv";

const storage = new MMKV({
  id: "e2e_rn_cli.default",
});

export class MMKVStore implements KvStore {
  async get(key: string): Promise<string | null> {
    return storage.getString(key) ?? null;
  }

  async set(key: string, value: string): Promise<void> {
    return storage.set(key, value);
  }

  async delete(key: string): Promise<void> {
    return storage.delete(key);
  }

  async clearAll(): Promise<void> {
    return storage.clearAll();
  }
}
