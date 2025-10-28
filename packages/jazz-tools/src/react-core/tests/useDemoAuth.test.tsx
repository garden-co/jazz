// @vitest-environment happy-dom

import { KvStoreContext } from "jazz-tools";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useDemoAuth } from "../auth/DemoAuth";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing";
import { act, renderHook } from "./testUtils";

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
      renderHook(() => useDemoAuth(), {
        account: guestAccount,
      }),
    ).toThrow();
  });

  it("should show signed in state with authenticated account", async () => {
    const { result } = renderHook(() => useDemoAuth(), {
      isAuthenticated: true,
    });

    expect(result.current.state).toBe("signedIn");
  });

  it("should update existing users list", async () => {
    const testUsername = "testuser";
    const { result } = renderHook(() => useDemoAuth());

    await act(async () => {
      await result.current.signUp(testUsername);
    });

    expect(result.current.existingUsers).toContain(testUsername);
  });

  it("should handle login for existing user", async () => {
    const testUsername = "testuser";

    const { result } = renderHook(() => useDemoAuth());

    await act(async () => {
      await result.current.signUp(testUsername);
    });

    const { result: newResult } = renderHook(() => useDemoAuth());

    // Then try to log in
    await act(async () => {
      await newResult.current.logIn(testUsername);
    });

    expect(newResult.current.state).toBe("signedIn");
  });

  it("should handle signup for new user", async () => {
    const testUsername = "newuser";

    const { result } = renderHook(() => useDemoAuth());

    await act(async () => {
      await result.current.signUp(testUsername);
    });

    expect(result.current.state).toBe("signedIn");
  });
});
