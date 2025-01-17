// @vitest-environment happy-dom
import { act, renderHook } from "@testing-library/react";
import { PureJSCrypto } from "cojson/crypto";
import { Account, ID } from "jazz-tools";
import { beforeEach, describe, expect, it } from "vitest";
import { useOnboardingAuth } from "../auth/AnonymousAuth";

const STORAGE_KEY = "jazz-logged-in-secret";

beforeEach(() => {
  localStorage.removeItem(STORAGE_KEY);
});

const crypto = await PureJSCrypto.create();

describe("useOnboardingAuth", () => {
  it("should initialize with loading state", () => {
    const { result } = renderHook(() => useOnboardingAuth());

    expect(result.current[1]).toEqual({
      state: "loading",
      errors: [],
    });
  });

  it("should use provided username", () => {
    const customName = "Custom User";
    const { result } = renderHook(() =>
      useOnboardingAuth({ defaultUserName: customName }),
    );

    expect(result.current[0].defaultUserName).toBe(customName);
  });

  it("should use default username if none provided", () => {
    const { result } = renderHook(() => useOnboardingAuth());

    expect(result.current[0].defaultUserName).toBe("Anonymous user");
  });

  it("should transition to signedIn state after successful auth", async () => {
    const { result } = renderHook(() => useOnboardingAuth());

    // Start the auth process
    await act(async () => {
      const authResult = await result.current[0].start(crypto);
      authResult.onSuccess();
    });

    expect(result.current[1].state).toBe("signedIn");
    expect(result.current[1].errors).toEqual([]);
  });

  it("should handle logout", async () => {
    const { result } = renderHook(() => useOnboardingAuth());

    // Sign in first
    await act(async () => {
      const authResult = await result.current[0].start(crypto);
      if (authResult.type === "new") {
        await authResult.saveCredentials({
          accountID: "test-account-id" as ID<Account>,
          secret:
            "test-secret" as `sealerSecret_z${string}/signerSecret_z${string}`,
        });
        authResult.onSuccess();
      }
    });

    // Then logout
    await act(async () => {
      if (result.current[1].state === "signedIn") {
        result.current[1].logOut();
      }
    });

    const { result: result2 } = renderHook(() => useOnboardingAuth());

    await act(async () => {
      const authResult = await result2.current[0].start(crypto);
      expect(authResult.type).toBe("new");
    });
  });

  it("should persist auth state across hook re-renders", async () => {
    const { result, rerender } = renderHook(() => useOnboardingAuth());

    // Sign in
    await act(async () => {
      const authResult = await result.current[0].start(crypto);
      if (authResult.type === "new") {
        await authResult.saveCredentials({
          accountID: "test-account-id" as ID<Account>,
          secret:
            "test-secret" as `sealerSecret_z${string}/signerSecret_z${string}`,
        });
        authResult.onSuccess();
      }
    });

    // Re-render the hook
    rerender();

    expect(result.current[1].state).toBe("signedIn");
  });

  it("should maintain auth state in localStorage", async () => {
    const { result } = renderHook(() => useOnboardingAuth());

    // Sign in
    await act(async () => {
      const authResult = await result.current[0].start(crypto);
      if (authResult.type === "new") {
        await authResult.saveCredentials({
          accountID: "test-account-id" as ID<Account>,
          secret:
            "test-secret" as `sealerSecret_z${string}/signerSecret_z${string}`,
        });
        authResult.onSuccess();
      }
    });

    // Verify localStorage has the auth data
    const { result: result2 } = renderHook(() => useOnboardingAuth());

    await act(async () => {
      const authResult = await result2.current[0].start(crypto);
      expect(authResult.type).toBe("existing");
      authResult.onSuccess();
    });

    expect(result.current[1].state).toBe("signedIn");
    expect(result.current[1].errors).toEqual([]);
  });

  it("should handle errors", async () => {
    const { result } = renderHook(() => useOnboardingAuth());

    await act(async () => {
      const authResult = await result.current[0].start(crypto);
      authResult.onError("test-error");
    });

    expect(result.current[1].state).toBe("loading");
    expect(result.current[1].errors).toEqual(["test-error"]);
  });
});
