// @vitest-environment happy-dom

import { AuthSecretStorage, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { useIsAuthenticated } from "../auth/useIsAuthenticated.js";
import { withJazzTestSetup } from "./testUtils.js";

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
    const [result] = withJazzTestSetup(() => useIsAuthenticated());
    expect(result.value).toBe(false);
  });

  it("should return true when valid credentials exist", async () => {
    const [result] = withJazzTestSetup(() => useIsAuthenticated(), {
      isAuthenticated: true,
    });
    expect(result.value).toBe(true);
  });
});
