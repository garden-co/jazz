import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useLocalFirstAuth } from "./use-local-first-auth.js";

describe("react/useLocalFirstAuth", () => {
  it("accepts browser secret-store options to isolate storage keys", async () => {
    const aliceKey = `auth:alice`;
    const bobKey = `auth:bob`;
    const { result: alice } = renderHook(() =>
      useLocalFirstAuth({ authSecretStorageKey: aliceKey }),
    );
    const { result: bob } = renderHook(() => useLocalFirstAuth({ authSecretStorageKey: bobKey }));

    await waitFor(() => {
      expect(alice.current.isLoading).toBe(false);
      expect(bob.current.isLoading).toBe(false);
    });

    await act(async () => {
      await alice.current.login("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");
    });

    await waitFor(() => expect(alice.current.secret).toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    expect(localStorage.getItem(aliceKey)).toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");
    expect(localStorage.getItem(bobKey)).not.toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");
    expect(bob.current.secret).not.toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");
  });
});
