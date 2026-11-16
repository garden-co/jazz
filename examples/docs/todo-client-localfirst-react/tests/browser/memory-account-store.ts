import type { AccountStore } from "jazz-tools";

/**
 * Where a test session keeps its device account: one store per mount gives
 * each mount its own account; sharing a store reopens the same one.
 */
export function memoryAccountStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}
