import { expect, it } from "vitest";
import { createKeyStore } from "../../src/account-store.js";

it("serialises overlapping key updates across connections and survives reopening", async () => {
  const scope = {
    registry: `https://registry.test/${crypto.randomUUID()}`,
    env: "dev",
    accountId: crypto.randomUUID(),
  };
  const first = createKeyStore(scope);
  const second = createKeyStore(scope);
  await Promise.all(
    Array.from({ length: 24 }, (_, index) =>
      (index % 2 ? first : second).update((value) =>
        JSON.stringify([...JSON.parse(value ?? "[]"), index]),
      ),
    ),
  );
  const reopened = createKeyStore(scope);
  expect(JSON.parse((await reopened.read())!).sort((a: number, b: number) => a - b)).toEqual(
    Array.from({ length: 24 }, (_, index) => index),
  );
  await expect(
    reopened.update(() => {
      throw new Error("transform refused");
    }),
  ).rejects.toThrow("transform refused");
  expect(await reopened.read()).toBe(await first.read());
  expect(await createKeyStore({ ...scope, env: "production" }).read()).toBeNull();
  expect(await createKeyStore({ ...scope, accountId: crypto.randomUUID() }).read()).toBeNull();
  expect(await createKeyStore({ ...scope, registry: `${scope.registry}/other` }).read()).toBeNull();
});
