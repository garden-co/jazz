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
