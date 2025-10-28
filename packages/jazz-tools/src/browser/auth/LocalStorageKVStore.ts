import { KvStore } from "jazz-tools";

export class LocalStorageKVStore implements KvStore {
  constructor() {}

  async get(key: string) {
    return localStorage.getItem(key);
  }

  async set(key: string, value: string) {
    localStorage.setItem(key, value);
  }

  async delete(key: string) {
    localStorage.removeItem(key);
  }

  async clearAll() {
    localStorage.clear();
  }
}
