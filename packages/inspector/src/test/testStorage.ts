import { vi } from "vitest";

// Node can expose an undefined localStorage under Vitest depending on runtime flags.
// Use one deterministic in-memory Storage implementation for inspector tests.
class TestStorage implements Storage {
  private values = new Map<string, string>();

  get length(): number {
    return this.values.size;
  }

  clear(): void {
    this.values.clear();
  }

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  key(index: number): string | null {
    return Array.from(this.values.keys())[index] ?? null;
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

export const testStorage = new TestStorage();

export function installTestStorage(): void {
  vi.stubGlobal("localStorage", testStorage);
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: testStorage,
  });
}
