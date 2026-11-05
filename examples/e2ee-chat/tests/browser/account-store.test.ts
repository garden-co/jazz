import { expect, it } from "vitest";
import { resolveE2eeStore } from "../../../../packages/jazz-tools/src/e2ee/browser-key-store.js";

it("uses the default SDK store for overlapping updates and survives reopening", async () => {
  const scope = {
    registry: `https://registry.test/apps/${crypto.randomUUID()}/accounts`,
    env: "dev",
    accountId: crypto.randomUUID(),
  };
  const first = resolveE2eeStore(undefined, scope);
  const second = resolveE2eeStore(undefined, scope);
  await Promise.all(
    Array.from({ length: 24 }, (_, index) =>
      (index % 2 ? first : second).update((value) =>
        JSON.stringify([...JSON.parse(value ?? "[]"), index]),
      ),
    ),
  );
  const reopened = resolveE2eeStore(undefined, scope);
  expect(JSON.parse((await reopened.read())!).sort((a: number, b: number) => a - b)).toEqual(
    Array.from({ length: 24 }, (_, index) => index),
  );
  const beforeFailedUpdate = await reopened.read();
  await expect(
    reopened.update(() => {
      throw new Error("transform refused");
    }),
  ).rejects.toThrow("transform refused");
  expect(await reopened.read()).toBe(beforeFailedUpdate);
  expect(await resolveE2eeStore(undefined, { ...scope, env: "production" }).read()).toBeNull();
  expect(
    await resolveE2eeStore(undefined, { ...scope, accountId: crypto.randomUUID() }).read(),
  ).toBeNull();
  expect(
    await resolveE2eeStore(undefined, { ...scope, registry: `${scope.registry}/other` }).read(),
  ).toBeNull();
});
