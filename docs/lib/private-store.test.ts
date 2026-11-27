import { randomBytes } from "node:crypto";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
// The getting-started page includes this sample. Its `jazz-tools/backend`
// import resolves to `private-store-sample-backend.ts` here.
import { directoryStore } from "../../examples/docs/e2ee-ts/src/directory-store";

async function tempStore(onCleanup: (cleanup: () => Promise<void>) => void) {
  const root = await mkdtemp(join(tmpdir(), "docs-private-store-"));
  onCleanup(() => rm(root, { force: true, recursive: true }));
  return join(root, "store");
}

test("the directory store lists saved names ending in .tmp and reserves metadata names", async ({
  onTestFinished,
}) => {
  const dir = await tempStore(onTestFinished);
  const store = directoryStore(dir, randomBytes(32));
  const incarnation = await store.openScope("device");
  const bytes = Uint8Array.of(1, 2, 3);
  expect(await store.save("device", incarnation, "note.tmp", bytes, null)).toBe(1n);
  await expect(store.save("device", incarnation, ".document.tmp", bytes, null)).rejects.toThrow(
    "Invalid document name",
  );
  expect(await store.list("device")).toEqual(["note.tmp"]);
  expect(await store.load("device", "note.tmp")).toEqual({ bytes, revision: 1n });
});

test.skipIf(process.platform === "win32")(
  "scope and document modes exclude other users",
  async ({ onTestFinished }) => {
    const dir = await tempStore(onTestFinished);
    const store = directoryStore(dir, randomBytes(32));
    const incarnation = await store.openScope("device");
    await store.save("device", incarnation, "keys", Uint8Array.of(1), null);

    const scope = join(dir, "device");
    expect((await stat(scope)).mode & 0o777).toBe(0o700);
    expect((await stat(join(scope, ".incarnation"))).mode & 0o777).toBe(0o600);
    expect((await stat(join(scope, "keys"))).mode & 0o777).toBe(0o600);
  },
);

test("only a store with the supplied wrapping key can open a protected secret", async ({
  onTestFinished,
}) => {
  const dir = await tempStore(onTestFinished);
  const machineKey = randomBytes(32);
  const secret = randomBytes(32);
  const blob = await directoryStore(dir, machineKey).protect(secret);

  expect(Buffer.from(blob)).not.toEqual(secret);
  expect(Buffer.from(await directoryStore(dir, machineKey).unprotect(blob))).toEqual(secret);
  await expect(directoryStore(dir, randomBytes(32)).unprotect(blob)).rejects.toThrow();
});
