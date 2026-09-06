import { generateAuthSecret } from "../runtime/auth-secret-store.js";
import { describe, expect, it, vi } from "vitest";
import {
  accountToken,
  exportLocalFirstSecret,
  createAccountManagerWithRuntime,
  onAccountInvalidated,
} from "./enrollment.js";

import {
  admitAccountConfig,
  assertAccountConfig,
  copyAccountConfigAdmission,
} from "./config-capability.js";

const registry = "https://core.example/apps/test/accounts";
const identity = { issuer: "https://issuer.example", subject: "alice" };
const id = "00000000-0000-4000-8000-000000000001";
function token(subject = identity.subject) {
  return `e30.${btoa(JSON.stringify({ iss: identity.issuer, sub: subject }))}.signature`;
}
function setup(
  fetcher: typeof fetch = vi.fn(
    async () => new Response(JSON.stringify({ account: id, identity })),
  ),
) {
  return createAccountManagerWithRuntime({
    registry,
    fetch: fetcher,
    localFirst: { create: () => ({ accountId: id, identity, auth: token() }) },
  });
}

it("retains the supplied recovery root independently of a factory echo", () => {
  const secret = generateAuthSecret();
  const unrelated = generateAuthSecret();
  const local = {
    accountId: id,
    identity: { issuer: "urn:jazz:local-first", subject: "local" },
    auth: "unused",
    secret: unrelated,
  };
  const manager = createAccountManagerWithRuntime({
    registry,
    localFirst: { create: () => local, restore: () => local },
    restoredLocalFirstSecret: secret,
  });
  expect(exportLocalFirstSecret(manager.getLoggedIn()!)).toBe(secret);
  expect(exportLocalFirstSecret(manager.restoreLocalFirst(secret))).toBe(secret);
});

it("does not export provider credentials as a local recovery root", async () => {
  const manager = setup();
  const account = await manager.registerJWT(token());
  expect(() => exportLocalFirstSecret(account)).toThrow(/recovery_unavailable/);
});

describe("opaque account credentials", () => {
  it("binds normalized context admission to both account and registry", () => {
    const handle = setup().createLocalFirst();
    const config = { accountId: id, accountRegistryAuthority: registry };
    admitAccountConfig(config, handle);
    const normalized = { ...config };
    expect(() => assertAccountConfig(normalized)).toThrow();
    copyAccountConfigAdmission(config, normalized);
    expect(() => assertAccountConfig(normalized)).not.toThrow();
    expect(() =>
      copyAccountConfigAdmission(config, {
        ...config,
        accountRegistryAuthority: "https://other.example",
      }),
    ).toThrow();
    normalized.accountRegistryAuthority = "https://other.example";
    expect(() => assertAccountConfig(normalized)).toThrow();
    expect(() => admitAccountConfig(normalized, handle)).toThrow();
  });

  it("rejects forged handles and handles from another registry", async () => {
    const manager = setup();
    const handle = manager.createLocalFirst();
    await expect(accountToken({ ...handle }, registry)).rejects.toMatchObject({
      code: "invalid_account_handle",
    });
    await expect(accountToken(handle, `${registry}-other`)).rejects.toMatchObject({
      code: "invalid_account_handle",
    });
    await expect(accountToken(handle, registry)).resolves.toBe(token());
  });

  it("revokes all credentials before notifying contexts, even when cleanup throws", async () => {
    const manager = setup();
    const first = manager.createLocalFirst();
    const second = manager.createLocalFirst();
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const closed = vi.fn();
    onAccountInvalidated(first, () => {
      throw new Error("cleanup");
    });
    onAccountInvalidated(second, closed);
    try {
      manager.logout();
      expect(closed).toHaveBeenCalledOnce();
      expect(manager.getLoggedIn()).toBeUndefined();
      await expect(accountToken(first, registry)).rejects.toMatchObject({
        code: "invalid_account_handle",
      });
      await expect(accountToken(second, registry)).rejects.toMatchObject({
        code: "invalid_account_handle",
      });
    } finally {
      error.mockRestore();
    }
  });

  it("does not admit a response arriving after logout", async () => {
    let finish!: (response: Response) => void;
    const fetcher = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          finish = resolve;
        }),
    );
    const manager = setup(fetcher);
    const pending = manager.loginJWT(token());
    await vi.waitFor(() => expect(fetcher).toHaveBeenCalledOnce());
    manager.logout();
    finish(new Response(JSON.stringify({ account: id, identity })));
    await expect(pending).rejects.toMatchObject({ code: "account_logged_out" });
    expect(manager.getLoggedIn()).toBeUndefined();
  });

  it("bounds a hung credential callback and permits a later retry", async () => {
    vi.useFakeTimers();
    try {
      let late!: (token: string) => void;
      const getToken = vi
        .fn()
        .mockResolvedValueOnce(token())
        .mockImplementationOnce(
          () =>
            new Promise<string>((resolve) => {
              late = resolve;
            }),
        )
        .mockResolvedValueOnce(token());
      const manager = setup();
      const handle = await manager.loginJWT({ getToken });
      const timedOut = expect(accountToken(handle, registry)).rejects.toMatchObject({
        code: "credential_refresh_timeout",
      });
      await vi.advanceTimersByTimeAsync(30_000);
      await timedOut;
      // Completion of the abandoned attempt cannot replace this fresh result.
      late(token("someone-else"));
      await expect(accountToken(handle, registry)).resolves.toBe(token());
      expect(manager.getLoggedIn()).toBe(handle);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects a refreshed token with a different identity", async () => {
    let current = token();
    const manager = setup();
    const handle = await manager.loginJWT({ getToken: async () => current });
    current = token("bob");
    await expect(accountToken(handle, registry)).rejects.toMatchObject({
      code: "credential_identity_changed",
    });
  });
});
