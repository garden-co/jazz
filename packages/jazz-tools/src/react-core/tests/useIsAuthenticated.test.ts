import { AuthSecretStorage, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { useIsAuthenticated } from "../hooks";
// @vitest-environment happy-dom
import { renderHook } from "./testUtils";

// Initialize KV store for tests
KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("useIsAuthenticated", () => {
  let authSecretStorage: AuthSecretStorage;

  beforeEach(async () => {
    // Clear storage and create new instance for each test
    KvStoreContext.getInstance().getStorage().clearAll();
    authSecretStorage = new AuthSecretStorage();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  it("should return false when no credentials exist", () => {
    const { result } = renderHook(() => useIsAuthenticated());
    expect(result.current).toBe(false);
  });

  it("should return true when valid credentials exist", async () => {
    const { result } = renderHook(() => useIsAuthenticated(), {
      isAuthenticated: true,
    });
    expect(result.current).toBe(true);
  });
});
