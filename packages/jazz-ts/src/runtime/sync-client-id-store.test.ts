import { afterEach, describe, expect, it } from "vitest";
import { withResolvedSyncClientId } from "./sync-client-id-store.js";

class MemoryStorage implements Storage {
  private data = new Map<string, string>();

  get length(): number {
    return this.data.size;
  }

  clear(): void {
    this.data.clear();
  }

  getItem(key: string): string | null {
    return this.data.has(key) ? this.data.get(key)! : null;
  }

  key(index: number): string | null {
    return [...this.data.keys()][index] ?? null;
  }

  removeItem(key: string): void {
    this.data.delete(key);
  }

  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
}

const originalLocalStorage = (globalThis as any).localStorage;

describe("withResolvedSyncClientId", () => {
  afterEach(() => {
    (globalThis as any).localStorage = originalLocalStorage;
  });

  it("keeps a valid provided client ID", () => {
    const config = withResolvedSyncClientId({
      appId: "test-app",
      clientId: "550e8400-e29b-41d4-a716-446655440000",
    });

    expect(config.clientId).toBe("550e8400-e29b-41d4-a716-446655440000");
  });

  it("persists and reuses generated ID in browser storage", () => {
    (globalThis as any).localStorage = new MemoryStorage();

    const first = withResolvedSyncClientId({
      appId: "test-app",
      env: "dev",
      userBranch: "main",
      serverUrl: "https://example.test",
    });
    const second = withResolvedSyncClientId({
      appId: "test-app",
      env: "dev",
      userBranch: "main",
      serverUrl: "https://example.test",
    });

    expect(first.clientId).toBeDefined();
    expect(second.clientId).toBe(first.clientId);
  });

  it("regenerates when stored ID is invalid", () => {
    const storage = new MemoryStorage();
    (globalThis as any).localStorage = storage;

    const key = "jazz:sync-client-id:test-app:dev:main:https://example.test";
    storage.setItem(key, "not-a-uuid");

    const resolved = withResolvedSyncClientId({
      appId: "test-app",
      env: "dev",
      userBranch: "main",
      serverUrl: "https://example.test/path",
    });

    expect(resolved.clientId).toBeDefined();
    expect(resolved.clientId).not.toBe("not-a-uuid");
  });
});
