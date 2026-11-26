import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect, test } from "vitest";
import type { PrivateStore } from "../../packages/jazz-tools/src/encryption/private-store";

const doc = new URL("../content/docs/e2ee/getting-started.mdx", import.meta.url);
const errors = new URL(
  "../../packages/jazz-tools/src/encryption/private-store-errors.ts",
  import.meta.url,
);

async function extractSample(onCleanup: (cleanup: () => Promise<void>) => void) {
  const text = await readFile(doc, "utf8");
  const sample = [...text.matchAll(/```ts\n([\s\S]*?)\n```/g)]
    .map((block) => block[1])
    .find((code) => code?.includes("export function directoryStore"));
  if (!sample) throw new Error("Private store example is missing");
  const root = await mkdtemp(join(tmpdir(), "docs-private-store-"));
  onCleanup(() => rm(root, { force: true, recursive: true }));
  const module = join(root, "sample.mts");
  await writeFile(module, sample.replace('from "jazz-tools/backend"', `from "${errors.href}"`));
  const { directoryStore }: { directoryStore: (dir: string, machineKey: Buffer) => PrivateStore } =
    await import(/* @vite-ignore */ pathToFileURL(module).href);
  return { directoryStore, dir: join(root, "store") };
}

test("the directory store lists saved names ending in .tmp and reserves metadata names", async ({
  onTestFinished,
}) => {
  const { directoryStore, dir } = await extractSample(onTestFinished);
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
    const { directoryStore, dir } = await extractSample(onTestFinished);
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
  const { directoryStore, dir } = await extractSample(onTestFinished);
  const machineKey = randomBytes(32);
  const secret = randomBytes(32);
  const blob = await directoryStore(dir, machineKey).protect(secret);

  expect(Buffer.from(blob)).not.toEqual(secret);
  expect(Buffer.from(await directoryStore(dir, machineKey).unprotect(blob))).toEqual(secret);
  await expect(directoryStore(dir, randomBytes(32)).unprotect(blob)).rejects.toThrow();
});
