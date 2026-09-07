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

it("keeps backend authority opaque, scoped, and revocable", async () => {
  const { getBackendAuth, accountRegistry } = await import("./enrollment.js");
  const manager = createAccountManagerWithRuntime({
    registry,
    localFirst: { create: () => ({ accountId: id, identity, auth: token() }) },
    backend: { admitBackend: async () => ({ nodeId: id }) },
  });
  const backend = await manager.becomeBackend({ backendSecret: "private-backend-credential" });
  expect(backend.id).toBe("00000000-0000-0000-0000-000000000000");
  expect(backend.identity).toEqual({ issuer: "urn:jazz:system", subject: id });
  expect(JSON.stringify(manager.getSnapshot())).not.toContain("private-backend-credential");
  expect(getBackendAuth(backend, registry)).toEqual({
    backendSecret: "private-backend-credential",
    nodeId: id,
  });
  expect(() => getBackendAuth({ ...backend }, registry)).toThrow(/invalid_account_handle/);
  expect(() => getBackendAuth(backend, registry + "other")).toThrow(/invalid_account_handle/);
  await expect(accountToken(backend, registry)).rejects.toThrow(
    /backend_account_requires_backend_host/,
  );
  await expect(manager.linkJWT(token())).rejects.toThrow(/backend_account_requires_backend_host/);
  const invalidated = vi.fn();
  onAccountInvalidated(backend, invalidated);
  manager.createLocalFirst();
  manager.logout();
  expect(invalidated).toHaveBeenCalledOnce();
  expect(() => accountRegistry(backend)).toThrow(/invalid_account_handle/);
});

it("rejects unsupported backend hosts and fences late backend admission on logout", async () => {
  await expect(setup().becomeBackend({ backendSecret: "secret" })).rejects.toThrow(
    /backend_host_unavailable/,
  );
  let finish!: (value: { nodeId: string }) => void;
  const manager = createAccountManagerWithRuntime({
    registry,
    localFirst: { create: () => ({ accountId: id, identity, auth: token() }) },
    backend: {
      admitBackend: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
  });
  const pending = manager.becomeBackend({ backendSecret: "secret" });
  manager.logout();
  finish({ nodeId: id });
  await expect(pending).rejects.toThrow(/account_logged_out/);
  expect(manager.getLoggedIn()).toBeUndefined();
});

it("rejects SYSTEM identities through every ordinary JWT enrollment path", async () => {
  const fetcher = vi.fn<typeof fetch>();
  const manager = setup(fetcher);
  const forged = `e30.${btoa(JSON.stringify({ iss: "urn:jazz:system", sub: id }))}.signature`;
  await expect(manager.registerJWT(forged)).rejects.toThrow(/external_identity_required/);
  await expect(manager.loginJWT(forged)).rejects.toThrow(/external_identity_required/);
  manager.createLocalFirst();
  await expect(manager.linkJWT(forged)).rejects.toThrow(/external_identity_required/);
  expect(fetcher).not.toHaveBeenCalled();
});
