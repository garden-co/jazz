// @vitest-environment happy-dom

import { KvStoreContext } from "jazz-tools";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useDemoAuth } from "../auth/useDemoAuth.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

describe("useDemoAuth", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  afterEach(() => {
    KvStoreContext.getInstance().getStorage().clearAll();
  });

  it("throws error when using guest account", async () => {
    const guestAccount = await createJazzTestGuest();

    expect(() =>
      withJazzTestSetup(() => useDemoAuth(), {
        account: guestAccount,
      }),
    ).toThrow();
  });

  it("should show signed in state with authenticated account", async () => {
    const [result] = withJazzTestSetup(() => useDemoAuth(), {
      isAuthenticated: true,
    });

    expect(result.value.state).toBe("signedIn");
  });

  it("should update existing users list", async () => {
    const testUsername = "testuser";
    const [result] = withJazzTestSetup(() => useDemoAuth());

    await result.value.signUp(testUsername);

    expect(result.value.existingUsers).toContain(testUsername);
  });

  it("should handle login for existing user", async () => {
    const testUsername = "testuser";

    const [result] = withJazzTestSetup(() => useDemoAuth());

    await result.value.signUp(testUsername);

    const [newResult] = withJazzTestSetup(() => useDemoAuth());

    // Then try to log in
    await newResult.value.logIn(testUsername);

    expect(newResult.value.state).toBe("signedIn");
  });

  it("should handle signup for new user", async () => {
    const testUsername = "newuser";

    const [result] = withJazzTestSetup(() => useDemoAuth());

    await result.value.signUp(testUsername);

    expect(result.value.state).toBe("signedIn");
  });
});
