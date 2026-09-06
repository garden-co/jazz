import { expect, it, vi } from "vitest";
import { prepareAccountManager } from "./persistence.js";
import { accountToken } from "./enrollment.js";

const registry = "https://core.example/apps/test/accounts";
// Controlled crypto boundary: these tests prove storage ordering and recovery;
// native signature/subject derivation is covered by the Rust identity corpus.
const mintToken = () =>
  `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: "00000000-0000-4000-8000-000000000001" }))}.sig`;

it("restores local selection and retains its root after logout", async () => {
  let value: string | null = null;
  const store = {
    async read() {
      return value;
    },
    async write(next: string) {
      value = next;
    },
  };
  const options = { appId: "test", registry, store, mintToken };
  const first = await prepareAccountManager(options);
  const handle = first.createLocalFirst();
  await accountToken(handle, registry);
  const restarted = await prepareAccountManager(options);
  expect(restarted.getLoggedIn()?.id).toBe(handle.id);
  const root = JSON.parse(value!).roots[0];
  restarted.logout();
  await vi.waitFor(() => expect(JSON.parse(value!).selected).toBeNull());
  const loggedOut = await prepareAccountManager(options);
  expect(loggedOut.getLoggedIn()).toBeUndefined();
  expect(JSON.parse(value!).roots).toEqual([root]);
});

it("does not release a local credential before asynchronous root persistence finishes", async () => {
  let finish!: () => void;
  const write = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      }),
  );
  const manager = await prepareAccountManager({
    appId: "test",
    registry,
    mintToken,
    store: {
      async read() {
        return null;
      },
      write,
    },
  });
  const handle = manager.createLocalFirst();
  const released = vi.fn();
  const pending = accountToken(handle, registry).then(released);
  await vi.waitFor(() => expect(write).toHaveBeenCalledOnce());
  expect(released).not.toHaveBeenCalled();
  finish();
  await pending;
  expect(released).toHaveBeenCalledOnce();
});
