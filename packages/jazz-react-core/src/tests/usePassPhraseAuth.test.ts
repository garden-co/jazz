// @vitest-environment happy-dom

import { mnemonicToEntropy } from "@scure/bip39";
import { AuthSecretStorage, KvStoreContext } from "jazz-tools";
import { testWordlist } from "jazz-tools/src/tests/fixtures.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { usePassphraseAuth } from "../auth/PassphraseAuth";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing";
import { act, renderHook, waitFor } from "./testUtils";

describe("usePassphraseAuth", () => {
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
      renderHook(() => usePassphraseAuth({ wordlist: testWordlist }), {
        account: guestAccount,
      }),
    ).toThrow();
  });

  it("should show signed in state with authenticated account", async () => {
    const { result } = renderHook(
      () => usePassphraseAuth({ wordlist: testWordlist }),
      {
        isAuthenticated: true,
      },
    );

    expect(result.current.state).toBe("signedIn");
  });

  it("should sign up with the current credentials", async () => {
    const { result } = renderHook(() =>
      usePassphraseAuth({ wordlist: testWordlist }),
    );

    const authSecretStorage = new AuthSecretStorage();
    const credentialsBefore = await authSecretStorage.get();

    let passphrase = "";

    await act(async () => {
      passphrase = await result.current.signUp();
    });

    expect(result.current.state).toBe("signedIn");
    expect(await authSecretStorage.get()).toEqual({
      ...credentialsBefore,
      provider: "passphrase",
    });
    expect(mnemonicToEntropy(passphrase, testWordlist)).toEqual(
      credentialsBefore?.secretSeed,
    );
  });

  it("should log in with the previous passphrase", async () => {
    const { result } = renderHook(() =>
      usePassphraseAuth({ wordlist: testWordlist }),
    );

    const authSecretStorage = new AuthSecretStorage();
    const credentialsBefore = await authSecretStorage.get();

    let passphrase = "";

    await act(async () => {
      passphrase = await result.current.signUp();
    });

    await act(async () => {
      await result.current.logIn(passphrase);
    });

    expect(result.current.state).toBe("signedIn");
    expect(await authSecretStorage.get()).toMatchObject({
      ...credentialsBefore,
      provider: "passphrase",
    });
  });

  it("should return the current account passphrase", async () => {
    const { result } = renderHook(() =>
      usePassphraseAuth({ wordlist: testWordlist }),
    );

    await waitFor(() => result.current.passphrase !== "");

    const passphrase = result.current.passphrase;

    expect(await result.current.signUp()).toBe(passphrase);
  });
});
