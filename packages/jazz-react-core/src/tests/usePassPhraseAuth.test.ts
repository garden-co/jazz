// @vitest-environment happy-dom

import { mnemonicToEntropy } from "@scure/bip39";
import { AuthSecretStorage, KvStoreContext } from "jazz-tools";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { usePassphraseAuth } from "../auth/PassphraseAuth";
import { useAccount } from "../hooks";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing";
import { testWordlist } from "./fixtures.js";
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

  it("should be able to logout after sign up", async () => {
    const account = await createJazzTestAccount({});

    const accounts: string[] = [];
    const updates: { state: string; accountIndex: number }[] = [];

    const { result } = renderHook(
      () => {
        const passphraseAuth = usePassphraseAuth({ wordlist: testWordlist });
        const account = useAccount();

        if (!accounts.includes(account.me.id)) {
          accounts.push(account.me.id);
        }

        updates.push({
          state: passphraseAuth.state,
          accountIndex: accounts.indexOf(account.me.id),
        });

        return { passphraseAuth, account };
      },
      {
        account,
        isAuthenticated: false,
      },
    );

    expect(result.current?.passphraseAuth.state).toBe("anonymous");
    expect(result.current?.account?.me).toBeDefined();

    const id = result.current?.account?.me?.id;

    await act(async () => {
      await result.current?.passphraseAuth.signUp();
    });

    expect(result.current?.passphraseAuth.state).toBe("signedIn");
    expect(result.current?.account?.me.id).toBe(id);

    await act(async () => {
      await result.current?.account?.logOut();
    });

    expect(result.current?.passphraseAuth.state).toBe("anonymous");
    expect(result.current?.account?.me.id).not.toBe(id);

    expect(updates).toMatchInlineSnapshot(`
      [
        {
          "accountIndex": 0,
          "state": "anonymous",
        },
        {
          "accountIndex": 0,
          "state": "anonymous",
        },
        {
          "accountIndex": 0,
          "state": "signedIn",
        },
        {
          "accountIndex": 1,
          "state": "anonymous",
        },
      ]
    `);
  });
});
