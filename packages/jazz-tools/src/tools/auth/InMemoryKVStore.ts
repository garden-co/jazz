import { KvStore } from "./KvStoreContext.js";

export class InMemoryKVStore implements KvStore {
  private store: Record<string, string> = {};

  async get(key: string) {
    const data = this.store[key];

    if (!data) return null;

    return data;
  }

  async set(key: string, value: string) {
    this.store[key] = value;
  }

  async delete(key: string) {
    delete this.store[key];
  }

  async clearAll() {
    this.store = {};
  }
}
