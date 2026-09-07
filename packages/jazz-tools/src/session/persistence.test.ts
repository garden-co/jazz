import { it, expect, vi } from "vitest";
import { prepareAccountManager } from "../accounts/persistence.js";
import { accountToken } from "../accounts/enrollment.js";
import { createJazzSessionOwner } from "./state.js";

it("logout waits for durable selection and retry clears it without restoring an invalid handle", async () => {
  let value: string | null = null;
  let fail = false;
  const failure = new Error("storage full");
  const registry = "https://core.example/apps/test/accounts";
  const store = {
    read: async () => value,
    update: async (transform: (current: string | null) => string) => {
      if (fail) throw failure;
      value = transform(value);
    },
  };
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    store,
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`,
  });
  const old = accounts.createLocalFirst();
  await accountToken(old, registry);
  const openClient = vi.fn(async () => ({ shutdown: async () => {} }));
  const session = await createJazzSessionOwner({ accounts, openClient });
  fail = true;
  await expect(session.logout()).rejects.toBe(failure);
  expect(session.getSnapshot()).toMatchObject({ status: "error", error: failure });
  expect(session.getSnapshot().account).toBeUndefined();
  expect(session.getSnapshot().client).toBeUndefined();
  expect(JSON.parse(value!).selected).toBe(0);
  expect(openClient).toHaveBeenCalledTimes(1);
  fail = false;
  await session.retry();
  expect(session.getSnapshot().status).toBe("signed-out");
  expect(JSON.parse(value!).selected).toBeNull();
  expect(openClient).toHaveBeenCalledTimes(1);
  await session.close();
});

it("retry durably retains the exact newly selected root before opening its client", async () => {
  let value: string | null = null;
  let fail = true;
  const failure = new Error("storage unavailable");
  const registry = "https://core.example/apps/test/accounts";
  const accounts = await prepareAccountManager({
    appId: "test",
    registry,
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`,
    store: {
      read: async () => value,
      update: async (transform) => {
        if (fail) throw failure;
        value = transform(value);
      },
    },
  });
  const openClient = vi.fn(async (account) => {
    await accountToken(account, registry);
    return { shutdown: async () => {} };
  });
  const session = await createJazzSessionOwner({ accounts, openClient });
  await expect(session.createLocalFirst()).rejects.toBe(failure);
  const selected = session.getSnapshot().account;
  expect(selected).toBeDefined();
  expect(openClient).not.toHaveBeenCalled();
  expect(value).toBeNull();
  fail = false;
  await session.retry();
  expect(session.getSnapshot().status).toBe("ready");
  expect(session.getSnapshot().account).toBe(selected);
  expect(openClient).toHaveBeenCalledExactlyOnceWith(selected);
  expect(JSON.parse(value!).roots).toHaveLength(1);
  await session.close();
});

it("backend initialization and transitions retain roots but never persist backend selection or credentials", async () => {
  let value: string | null = null;
  let fail = false;
  const registry = "https://core.example/apps/test/accounts";
  const nodeId = "00000000-0000-4000-8000-000000000001";
  const admitBackend = vi.fn(async () => ({ nodeId }));
  const config = {
    appId: "test",
    registry,
    backend: { admitBackend },
    mintToken: () =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: nodeId }))}.sig`,
    store: {
      read: async () => value,
      update: async (transform: (current: string | null) => string) => {
        if (fail) throw new Error("storage unavailable");
        value = transform(value);
      },
    },
  };
  const accounts = await prepareAccountManager(config);
  const local = accounts.createLocalFirst();
  await accountToken(local, registry);
  const shutdown = vi.fn(async () => {});
  const openClient = vi.fn(async () => ({ shutdown }));
  const session = await createJazzSessionOwner({
    accounts,
    openClient,
    initial: { backendSecret: "ephemeral-secret" },
  });
  expect(session.getSnapshot().account?.identity).toEqual({
    issuer: "urn:jazz:system",
    subject: nodeId,
  });
  expect(JSON.parse(value!).roots).toHaveLength(1);
  expect(JSON.parse(value!).selected).toBeNull();
  expect(value).not.toContain("ephemeral-secret");
  expect((await prepareAccountManager(config)).getLoggedIn()).toBeUndefined();
  fail = true;
  await expect(session.becomeBackend({ backendSecret: "replacement-secret" })).rejects.toThrow(
    "storage unavailable",
  );
  expect(shutdown).toHaveBeenCalledWith({ waitForSync: true });
  expect(session.getSnapshot().status).toBe("error");
  const selected = session.getSnapshot().account;
  fail = false;
  await session.retry();
  expect(session.getSnapshot().account).toBe(selected);
  expect(admitBackend).toHaveBeenCalledTimes(2);
  expect(value).not.toContain("replacement-secret");
  await session.close();
});
